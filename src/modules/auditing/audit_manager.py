"""
Audit Manager - Sistema de auditoría y registro en PostgreSQL.
Registra pipelines, ejecuciones y validaciones para monitoreo.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from uuid import uuid4
import psycopg2
from psycopg2.extras import RealDictCursor
import json

logger = logging.getLogger(__name__)


class AuditManager:
    """
    Gestor de auditoría para registro de pipelines y ejecuciones.
    Almacena toda la información en PostgreSQL para trazabilidad.
    """
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Args:
            connection_config: Configuración de conexión PostgreSQL
                {host, port, database, user, password}
        """
        self.config = connection_config
        self.connection = None
        
    def connect(self) -> bool:
        """Establecer conexión con PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            logger.info("Connected to audit database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to audit database: {e}")
            return False
    
    def register_pipeline(
        self,
        name: str,
        description: str,
        config: Dict[str, Any]
    ) -> Optional[str]:
        """
        Registrar un nuevo pipeline en la base de datos.
        
        Args:
            name: Nombre del pipeline
            description: Descripción
            config: Configuración completa del pipeline
            
        Returns:
            UUID del pipeline registrado o None si falla
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.pipelines 
                    (name, description, config)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name) DO UPDATE 
                    SET description = EXCLUDED.description,
                        config = EXCLUDED.config,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (name, description, json.dumps(config)))
                
                pipeline_id = cursor.fetchone()[0]
                self.connection.commit()
                
                logger.info(f"Pipeline registered: {name} (ID: {pipeline_id})")
                return str(pipeline_id)
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to register pipeline: {e}")
            return None
    
    def start_execution(
        self,
        pipeline_id: str,
        execution_type: str = "manual"
    ) -> Optional[str]:
        """
        Iniciar registro de una ejecución de pipeline.
        
        Args:
            pipeline_id: UUID del pipeline
            execution_type: Tipo de ejecución (manual, scheduled, triggered)
            
        Returns:
            UUID de la ejecución o None si falla
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.executions 
                    (pipeline_id, status, start_time, metrics)
                    VALUES (%s, 'running', CURRENT_TIMESTAMP, %s)
                    RETURNING id
                """, (pipeline_id, json.dumps({"execution_type": execution_type})))
                
                execution_id = cursor.fetchone()[0]
                self.connection.commit()
                
                logger.info(f"Execution started: {execution_id}")
                return str(execution_id)
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to start execution: {e}")
            return None
    
    def complete_execution(
        self,
        execution_id: str,
        status: str,
        records_processed: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Completar registro de ejecución.
        
        Args:
            execution_id: UUID de la ejecución
            status: Estado final (completed, failed, cancelled)
            records_processed: Registros procesados exitosamente
            records_failed: Registros que fallaron
            error_message: Mensaje de error si aplica
            metrics: Métricas adicionales de la ejecución
            
        Returns:
            True si se actualizó correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE pipeline.executions
                    SET status = %s,
                        end_time = CURRENT_TIMESTAMP,
                        records_processed = %s,
                        records_failed = %s,
                        error_message = %s,
                        metrics = %s
                    WHERE id = %s
                """, (
                    status,
                    records_processed,
                    records_failed,
                    error_message,
                    json.dumps(metrics or {}),
                    execution_id
                ))
                
                self.connection.commit()
                logger.info(f"Execution completed: {execution_id} - {status}")
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to complete execution: {e}")
            return False
    
    def log_validation_result(
        self,
        execution_id: str,
        rule_name: str,
        rule_type: str,
        passed: bool,
        failed_count: int = 0,
        failure_details: Optional[List[Dict]] = None
    ) -> bool:
        """
        Registrar resultado de una regla de validación.
        
        Args:
            execution_id: UUID de la ejecución
            rule_name: Nombre de la regla
            rule_type: Tipo de regla (schema, quality, custom)
            passed: Si la regla pasó o no
            failed_count: Número de registros que fallaron
            failure_details: Detalles de fallos (lista de dicts)
            
        Returns:
            True si se registró correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.validation_results
                    (execution_id, rule_name, rule_type, passed, failed_count, failure_details)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    execution_id,
                    rule_name,
                    rule_type,
                    passed,
                    failed_count,
                    json.dumps(failure_details or [])
                ))
                
                self.connection.commit()
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to log validation result: {e}")
            return False
    
    def get_validation_results(self, execution_id: str) -> List[Dict[str, Any]]:
        """
        Obtener todos los resultados de validación para una ejecución.
        
        Args:
            execution_id: UUID de la ejecución
            
        Returns:
            Lista de resultados de validación
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        rule_name,
                        rule_type,
                        passed,
                        failed_count,
                        failure_details,
                        timestamp
                    FROM pipeline.validation_results
                    WHERE execution_id = %s
                    ORDER BY timestamp ASC
                """, (execution_id,))
                
                results = []
                for row in cursor.fetchall():
                    # failure_details ya es un objeto JSON/dict de PostgreSQL
                    failure_details = row[4] if row[4] else []
                    if isinstance(failure_details, str):
                        failure_details = json.loads(failure_details)
                    
                    results.append({
                        'rule_name': row[0],
                        'rule_type': row[1],
                        'passed': row[2],
                        'failed_count': row[3],
                        'failure_details': failure_details,
                        'timestamp': row[5].isoformat() if row[5] else None
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to get validation results: {e}")
            return []
    
    def log_audit_event(
        self,
        execution_id: Optional[str],
        level: str,
        module: str,
        event: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Registrar un evento de auditoría.
        
        Args:
            execution_id: UUID de la ejecución (opcional)
            level: Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            module: Módulo que genera el evento
            event: Descripción del evento
            context: Contexto adicional
            
        Returns:
            True si se registró correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.audit_logs
                    (execution_id, level, module, event, context)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    execution_id,
                    level,
                    module,
                    event,
                    json.dumps(context or {})
                ))
                
                self.connection.commit()
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to log audit event: {e}")
            return False
    
    def get_pipeline_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Obtener información de un pipeline por nombre.
        
        Args:
            name: Nombre del pipeline
            
        Returns:
            Diccionario con información del pipeline o None
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM pipeline.pipelines
                    WHERE name = %s
                """, (name,))
                
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            logger.error(f"Failed to get pipeline: {e}")
            return None
    
    def get_recent_executions(
        self,
        pipeline_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Obtener ejecuciones recientes de un pipeline.
        
        Args:
            pipeline_id: UUID del pipeline
            limit: Número máximo de ejecuciones a retornar
            
        Returns:
            Lista de ejecuciones
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT * FROM pipeline.executions
                    WHERE pipeline_id = %s
                    ORDER BY start_time DESC
                    LIMIT %s
                """, (pipeline_id, limit))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to get executions: {e}")
            return []
    
    def log_security_result(
        self,
        execution_id: str,
        attack_type: str,
        start_time: datetime,
        end_time: datetime,
        attempts_total: int,
        attempts_detected: int,
        attempts_blocked: int,
        attempts_successful: int,
        mttd_avg_ms: float,
        vulnerabilities: List[str],
        security_score: float
    ) -> bool:
        """
        Registrar resultado de simulación de ataque.
        
        Args:
            execution_id: UUID de la ejecución
            attack_type: Tipo de ataque
            start_time: Hora de inicio
            end_time: Hora de fin
            attempts_total: Total de intentos
            attempts_detected: Intentos detectados
            attempts_blocked: Intentos bloqueados
            attempts_successful: Intentos exitosos
            mttd_avg_ms: Tiempo promedio de detección
            vulnerabilities: Lista de vulnerabilidades encontradas
            security_score: Puntaje de seguridad (0-100)
            
        Returns:
            True si se registró correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO security.simulation_results
                    (execution_id, attack_type, start_time, end_time, 
                     attempts_total, attempts_detected, attempts_blocked, attempts_successful,
                     mttd_avg_ms, vulnerabilities, security_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    execution_id,
                    attack_type,
                    start_time,
                    end_time,
                    attempts_total,
                    attempts_detected,
                    attempts_blocked,
                    attempts_successful,
                    mttd_avg_ms,
                    json.dumps(vulnerabilities),
                    security_score
                ))
                
                self.connection.commit()
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to log security result: {e}")
            return False
    
    def close(self):
        """Cerrar conexión a la base de datos"""
        if self.connection:
            self.connection.close()
            logger.info("Audit database connection closed")
