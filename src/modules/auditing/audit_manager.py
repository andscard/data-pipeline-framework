"""
Audit Manager - Sistema de auditoría y registro en PostgreSQL.
Registra pipelines, ejecuciones y validaciones para monitoreo.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from uuid import uuid4
import uuid
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
        config: Dict[str, Any],
        version: str = "1.0.0",
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Registrar un nuevo pipeline en la base de datos.
        
        Args:
            name: Nombre del pipeline
            description: Descripción
            config: Configuración completa del pipeline
            version: Versión del pipeline (semantic versioning)
            owner: Usuario o equipo responsable
            tags: Etiquetas para clasificación
            
        Returns:
            UUID del pipeline registrado o None si falla
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.pipelines 
                    (name, description, config, version, owner, tags)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE 
                    SET description = EXCLUDED.description,
                        config = EXCLUDED.config,
                        version = EXCLUDED.version,
                        owner = EXCLUDED.owner,
                        tags = EXCLUDED.tags,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (name, description, json.dumps(config), version, owner, tags or []))
                
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
        execution_type: str = "manual",
        triggered_by: Optional[str] = None,
        environment: str = "development"
    ) -> Optional[str]:
        """
        Iniciar registro de una ejecución de pipeline.
        
        Args:
            pipeline_id: UUID del pipeline
            execution_type: Tipo de ejecución (manual, scheduled, triggered, api)
            triggered_by: Usuario o sistema que inició la ejecución
            environment: Entorno (development, staging, production)
            
        Returns:
            UUID de la ejecución o None si falla
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.executions 
                    (pipeline_id, status, start_time, execution_type, triggered_by, environment, metrics)
                    VALUES (%s, 'running', CURRENT_TIMESTAMP, %s, %s, %s, %s)
                    RETURNING id
                """, (pipeline_id, execution_type, triggered_by, environment, json.dumps({})))
                
                execution_id = cursor.fetchone()[0]
                self.connection.commit()
                
                # Incrementar contador de ejecuciones en el pipeline
                cursor.execute("""
                    UPDATE pipeline.pipelines 
                    SET run_count = run_count + 1
                    WHERE id = %s
                """, (pipeline_id,))
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
        metrics: Optional[Dict[str, Any]] = None,
        stages_summary: Optional[List[Dict]] = None,
        quality_score: Optional[float] = None,
        health_status: Optional[str] = None,
        total_errors: int = 0,
        total_warnings: int = 0,
        report_path: Optional[str] = None,
        executive_report_path: Optional[str] = None
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
            stages_summary: Resumen de etapas ejecutadas
            quality_score: Puntaje de calidad (0-100)
            health_status: Estado de salud (HEALTHY, WARNING, CRITICAL, FAILED)
            total_errors: Número total de errores
            total_warnings: Número total de warnings
            report_path: Ruta del reporte técnico HTML generado
            executive_report_path: Ruta del reporte ejecutivo HTML generado
            
        Returns:
            True si se actualizó correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE pipeline.executions
                    SET status = %s,
                        end_time = CURRENT_TIMESTAMP,
                        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)),
                        records_processed = %s,
                        records_failed = %s,
                        error_message = %s,
                        metrics = %s,
                        stages_summary = %s,
                        quality_score = %s,
                        health_status = %s,
                        total_errors = %s,
                        total_warnings = %s,
                        report_path = %s,
                        executive_report_path = %s
                    WHERE id = %s
                """, (
                    status,
                    records_processed,
                    records_failed,
                    error_message,
                    json.dumps(metrics or {}),
                    json.dumps(stages_summary or []),
                    quality_score,
                    health_status,
                    total_errors,
                    total_warnings,
                    report_path,
                    executive_report_path,
                    execution_id
                ))
                
                # Si fue exitosa, actualizar last_run_at en pipeline
                if status == 'completed':
                    cursor.execute("""
                        UPDATE pipeline.pipelines
                        SET last_run_at = CURRENT_TIMESTAMP
                        WHERE id = (SELECT pipeline_id FROM pipeline.executions WHERE id = %s)
                    """, (execution_id,))
                
                self.connection.commit()
                logger.info(f"Execution completed: {execution_id} - {status}")
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to complete execution: {e}")
            return False
    
    def start_stage(
        self,
        execution_id: str,
        stage_name: str
    ) -> Optional[str]:
        """
        Iniciar tracking de un stage del pipeline.
        
        Args:
            execution_id: UUID de la ejecución padre
            stage_name: Nombre del stage (INGESTION, VALIDATION, TRANSFORMATION, OUTPUT)
            
        Returns:
            UUID del stage_execution creado, None si hubo error
        """
        stage_order_map = {
            'INGESTION': 1,
            'VALIDATION': 2,
            'TRANSFORMATION': 3,
            'OUTPUT': 4
        }
        
        try:
            with self.connection.cursor() as cursor:
                stage_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO pipeline.stage_executions (
                        id, execution_id, stage_name, stage_order, status, start_time
                    ) VALUES (%s, %s, %s, %s, 'running', CURRENT_TIMESTAMP)
                    ON CONFLICT (execution_id, stage_name) 
                    DO UPDATE SET 
                        start_time = CURRENT_TIMESTAMP,
                        status = 'running',
                        end_time = NULL
                    RETURNING id
                """, (
                    stage_id,
                    execution_id,
                    stage_name,
                    stage_order_map.get(stage_name, 0)
                ))
                
                result = cursor.fetchone()
                self.connection.commit()
                
                stage_id = result[0] if result else stage_id
                logger.debug(f"Stage started: {stage_name} ({stage_id})")
                return stage_id
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to start stage {stage_name}: {e}")
            return None
    
    def complete_stage(
        self,
        execution_id: str,
        stage_name: str,
        status: str = 'completed',
        records_in: int = 0,
        records_out: int = 0,
        records_failed: int = 0,
        memory_usage_mb: Optional[float] = None,
        cpu_usage_percent: Optional[float] = None,
        error_count: int = 0,
        warning_count: int = 0,
        error_details: Optional[List[str]] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Completar tracking de un stage con métricas finales.
        
        Args:
            execution_id: UUID de la ejecución padre
            stage_name: Nombre del stage
            status: Estado final (completed, failed, skipped)
            records_in: Registros de entrada
            records_out: Registros de salida
            records_failed: Registros fallidos
            memory_usage_mb: Uso de memoria en MB
            cpu_usage_percent: Uso promedio de CPU
            error_count: Número de errores
            warning_count: Número de warnings
            error_details: Lista de mensajes de error
            metrics: Métricas adicionales (quality_score, validations_passed, etc)
            
        Returns:
            True si se actualizó correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE pipeline.stage_executions
                    SET status = %s,
                        end_time = CURRENT_TIMESTAMP,
                        duration_seconds = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)),
                        records_in = %s,
                        records_out = %s,
                        records_failed = %s,
                        memory_usage_mb = %s,
                        cpu_usage_percent = %s,
                        error_count = %s,
                        warning_count = %s,
                        error_details = %s,
                        metrics = %s
                    WHERE execution_id = %s AND stage_name = %s
                """, (
                    status,
                    records_in,
                    records_out,
                    records_failed,
                    memory_usage_mb,
                    cpu_usage_percent,
                    error_count,
                    warning_count,
                    json.dumps(error_details or []),
                    json.dumps(metrics or {}),
                    execution_id,
                    stage_name
                ))
                
                self.connection.commit()
                logger.debug(f"Stage completed: {stage_name} - {status}")
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to complete stage {stage_name}: {e}")
            return False
    
    def log_validation_result(
        self,
        execution_id: str,
        rule_name: str,
        rule_type: str,
        passed: bool,
        failed_count: int = 0,
        failure_details: Optional[List[Dict]] = None,
        dataset_name: Optional[str] = None,
        suite_name: Optional[str] = None,
        total_records: int = 0,
        severity: str = "error",
        expectation_type: Optional[str] = None
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
            dataset_name: Nombre del dataset validado
            suite_name: Nombre del suite de validación
            total_records: Total de registros validados
            severity: Severidad (critical, error, warning, info)
            expectation_type: Tipo de expectativa GE
            
        Returns:
            True si se registró correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.validation_results
                    (execution_id, rule_name, rule_type, passed, failed_count, failure_details,
                     dataset_name, suite_name, total_records, severity, expectation_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    execution_id,
                    rule_name,
                    rule_type,
                    passed,
                    failed_count,
                    json.dumps(failure_details or []),
                    dataset_name,
                    suite_name,
                    total_records,
                    severity,
                    expectation_type
                ))
                
                self.connection.commit()
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to log validation result: {e}")
            return False
    
    def log_validation_summary(
        self,
        execution_id: str,
        suite_name: str,
        dataset_name: str,
        total_validations: int,
        passed_validations: int,
        failed_validations: int,
        quality_score: float,
        total_records: int = 0,
        execution_time_ms: Optional[int] = None
    ) -> bool:
        """
        Registrar resumen agregado de validaciones por suite.
        Esta tabla reemplaza el registro individual de validaciones pasadas,
        reduciendo drásticamente el número de inserts en BD.
        
        Args:
            execution_id: UUID de la ejecución
            suite_name: Nombre del suite de validación
            dataset_name: Nombre del dataset validado
            total_validations: Total de validaciones ejecutadas
            passed_validations: Validaciones que pasaron
            failed_validations: Validaciones que fallaron
            quality_score: Score de calidad (0-100)
            total_records: Total de registros validados
            execution_time_ms: Tiempo de ejecución del suite en milisegundos
            
        Returns:
            True si se registró correctamente
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline.validation_summary
                    (execution_id, suite_name, dataset_name, total_validations,
                     passed_validations, failed_validations, quality_score,
                     total_records, execution_time_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (execution_id, suite_name, dataset_name)
                    DO UPDATE SET
                        total_validations = EXCLUDED.total_validations,
                        passed_validations = EXCLUDED.passed_validations,
                        failed_validations = EXCLUDED.failed_validations,
                        quality_score = EXCLUDED.quality_score,
                        total_records = EXCLUDED.total_records,
                        execution_time_ms = EXCLUDED.execution_time_ms,
                        timestamp = CURRENT_TIMESTAMP
                """, (
                    execution_id,
                    suite_name,
                    dataset_name,
                    total_validations,
                    passed_validations,
                    failed_validations,
                    quality_score,
                    total_records,
                    execution_time_ms
                ))
                
                self.connection.commit()
                return True
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Failed to log validation summary: {e}")
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
    
    def close(self):
        """Cerrar conexión a la base de datos"""
        if self.connection:
            self.connection.close()
            logger.info("Audit database connection closed")
