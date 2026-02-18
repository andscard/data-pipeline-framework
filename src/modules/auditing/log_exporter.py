"""
Log Exporter - Exporta métricas y logs de pipelines a CSV.
Cruza todas las tablas de auditoría para generar reportes útiles.
"""

import logging
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import json

logger = logging.getLogger(__name__)


class LogExporter:
    """Exportador de logs de auditoría a archivos CSV."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Inicializar exportador de logs.
        
        Args:
            db_config: Configuración de conexión a PostgreSQL
        """
        self.db_config = db_config
        self.connection = None
    
    def connect(self):
        """Conectar a base de datos de auditoría."""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            logger.info("Connected to audit database for export")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self):
        """Cerrar conexión."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def export_all(self, pipeline_name: str, output_dir: Path, execution_id: Optional[str] = None) -> Dict[str, str]:
        """
        Exportar todos los logs del pipeline a múltiples CSVs.
        
        Args:
            pipeline_name: Nombre del pipeline a exportar
            output_dir: Directorio donde guardar los CSVs
            execution_id: (Opcional) Filtrar por UUID de ejecución específica
            
        Returns:
            Diccionario con rutas de archivos generados
        """
        if not self.connection:
            self.connect()
        
        # Crear directorio si no existe
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Timestamp para los archivos
        # Si es una ejecución específica, usamos el timestamp actual para el nombre del archivo
        # aunque el contenido sea filtrado
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_pipeline_name = pipeline_name.replace(" ", "_").replace("/", "_")
        
        exported_files = {}
        
        # 1. Resumen de ejecuciones
        file_path = output_dir / f"{safe_pipeline_name}_executions_{timestamp}.csv"
        self._export_executions_summary(pipeline_name, file_path, execution_id)
        exported_files['executions_summary'] = str(file_path)
        
        # 2. Performance por stage
        file_path = output_dir / f"{safe_pipeline_name}_stages_{timestamp}.csv"
        self._export_stages_performance(pipeline_name, file_path, execution_id)
        exported_files['stages_performance'] = str(file_path)
        
        # 3. Métricas de calidad de validación
        file_path = output_dir / f"{safe_pipeline_name}_validation_quality_{timestamp}.csv"
        self._export_validation_quality(pipeline_name, file_path, execution_id)
        exported_files['validation_quality'] = str(file_path)
        
        # 4. Detalle de validaciones fallidas
        file_path = output_dir / f"{safe_pipeline_name}_validation_failures_{timestamp}.csv"
        self._export_validation_failures(pipeline_name, file_path, execution_id)
        exported_files['validation_failures'] = str(file_path)
        
        # 5. Timeline de ejecuciones
        file_path = output_dir / f"{safe_pipeline_name}_timeline_{timestamp}.csv"
        self._export_timeline(pipeline_name, file_path, execution_id)
        exported_files['timeline'] = str(file_path)
        
        # 6. Análisis de errores
        file_path = output_dir / f"{safe_pipeline_name}_errors_{timestamp}.csv"
        self._export_errors_analysis(pipeline_name, file_path, execution_id)
        exported_files['errors_analysis'] = str(file_path)
        
        return exported_files
    
    def _export_executions_summary(self, pipeline_name: str, output_path: Path, execution_id: Optional[str] = None):
        """Exportar resumen de todas las ejecuciones del pipeline."""
        query = """
            SELECT 
                e.id as execution_id,
                p.name as pipeline_name,
                e.status,
                e.execution_type,
                e.triggered_by,
                e.environment,
                TO_CHAR(e.start_time, 'YYYY-MM-DD HH24:MI:SS') as start_time,
                TO_CHAR(e.end_time, 'YYYY-MM-DD HH24:MI:SS') as end_time,
                ROUND(e.duration_seconds::numeric, 3) as duration_seconds,
                e.records_processed,
                e.records_failed,
                ROUND(e.quality_score::numeric, 2) as quality_score,
                e.health_status,
                e.total_errors,
                e.total_warnings,
                e.report_path,
                e.executive_report_path
            FROM pipeline.executions e
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            WHERE p.name = %s
        """
        
        params = [pipeline_name]
        if execution_id:
            query += " AND e.id = %s"
            params.append(execution_id)
            
        query += " ORDER BY e.start_time DESC;"
        
        self._execute_query_to_csv(query, tuple(params), output_path)
        logger.info(f"✓ Exported executions summary: {output_path.name}")
    
    def _export_stages_performance(self, pipeline_name: str, output_path: Path, execution_id: Optional[str] = None):
        """Exportar performance detallada por stage."""
        query = """
            SELECT 
                e.id as execution_id,
                TO_CHAR(e.start_time, 'YYYY-MM-DD HH24:MI:SS') as execution_date,
                e.status as execution_status,
                s.stage_name,
                s.stage_order,
                s.status as stage_status,
                ROUND(s.duration_seconds::numeric, 3) as duration_seconds,
                s.records_in,
                s.records_out,
                s.records_failed,
                ROUND((s.records_out::float / NULLIF(s.records_in, 0) * 100)::numeric, 2) as success_rate_pct,
                s.memory_usage_mb,
                s.cpu_usage_percent,
                s.error_count,
                s.warning_count,
                s.metrics->>'quality_score' as quality_score,
                s.metrics->>'validations_passed' as validations_passed,
                s.metrics->>'validations_failed' as validations_failed
            FROM pipeline.stage_executions s
            JOIN pipeline.executions e ON s.execution_id = e.id
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            WHERE p.name = %s
        """
        
        params = [pipeline_name]
        if execution_id:
            query += " AND e.id = %s"
            params.append(execution_id)
            
        query += " ORDER BY e.start_time DESC, s.stage_order;"
        
        self._execute_query_to_csv(query, tuple(params), output_path)
        logger.info(f"✓ Exported stages performance: {output_path.name}")
    
    def _export_validation_quality(self, pipeline_name: str, output_path: Path, execution_id: Optional[str] = None):
        """Exportar métricas de calidad por suite de validación."""
        query = """
            SELECT 
                e.id as execution_id,
                TO_CHAR(e.start_time, 'YYYY-MM-DD HH24:MI:SS') as execution_date,
                e.status as execution_status,
                ROUND(e.quality_score::numeric, 2) as overall_quality_score,
                vs.suite_name,
                vs.dataset_name,
                vs.total_validations,
                vs.passed_validations,
                vs.failed_validations,
                ROUND(vs.quality_score::numeric, 2) as suite_quality_score,
                vs.total_records,
                ROUND(vs.execution_time_ms::numeric, 2) as execution_time_ms,
                ROUND((vs.failed_validations::float / vs.total_validations * 100)::numeric, 2) as failure_rate_pct
            FROM pipeline.validation_summary vs
            JOIN pipeline.executions e ON vs.execution_id = e.id
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            WHERE p.name = %s
        """
        
        params = [pipeline_name]
        if execution_id:
            query += " AND e.id = %s"
            params.append(execution_id)
            
        query += " ORDER BY e.start_time DESC, vs.suite_name, vs.dataset_name;"
        
        self._execute_query_to_csv(query, tuple(params), output_path)
        logger.info(f"✓ Exported validation quality: {output_path.name}")
    
    def _export_validation_failures(self, pipeline_name: str, output_path: Path, execution_id: Optional[str] = None):
        """Exportar detalle de validaciones fallidas."""
        query = """
            SELECT 
                e.id as execution_id,
                TO_CHAR(e.start_time, 'YYYY-MM-DD HH24:MI:SS') as execution_date,
                vr.suite_name,
                vr.dataset_name,
                vr.rule_name,
                vr.rule_type,
                vr.expectation_type,
                vr.passed,
                vr.severity,
                vr.total_records,
                vr.failed_count,
                ROUND((vr.failed_count::float / NULLIF(vr.total_records, 0) * 100)::numeric, 2) as failure_rate_pct,
                vr.failure_details
            FROM pipeline.validation_results vr
            JOIN pipeline.executions e ON vr.execution_id = e.id
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            WHERE p.name = %s
              AND vr.passed = false
        """
        
        params = [pipeline_name]
        if execution_id:
            query += " AND e.id = %s"
            params.append(execution_id)
            
        query += " ORDER BY e.start_time DESC, vr.suite_name, vr.severity DESC, vr.failed_count DESC;"
        
        self._execute_query_to_csv(query, tuple(params), output_path)
        logger.info(f"✓ Exported validation failures: {output_path.name}")
    
    def _export_timeline(self, pipeline_name: str, output_path: Path, execution_id: Optional[str] = None):
        """Exportar timeline con eventos importantes (simplificado)."""
        query = """
            SELECT 
                e.id as execution_id,
                TO_CHAR(e.start_time, 'YYYY-MM-DD HH24:MI:SS') as start_time,
                TO_CHAR(e.end_time, 'YYYY-MM-DD HH24:MI:SS') as end_time,
                ROUND(e.duration_seconds::numeric, 3) as duration_seconds,
                e.status,
                e.execution_type,
                e.triggered_by,
                e.environment,
                ROUND(e.quality_score::numeric, 2) as quality_score,
                e.health_status,
                e.total_errors,
                e.total_warnings,
                e.records_processed,
                e.records_failed
            FROM pipeline.executions e
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            WHERE p.name = %s
        """
        
        params = [pipeline_name]
        if execution_id:
            query += " AND e.id = %s"
            params.append(execution_id)
            
        query += " ORDER BY e.start_time DESC;"
        
        self._execute_query_to_csv(query, tuple(params), output_path)
        logger.info(f"✓ Exported timeline: {output_path.name}")
    
    def _export_errors_analysis(self, pipeline_name: str, output_path: Path, execution_id: Optional[str] = None):
        """Exportar análisis de errores y warnings."""
        query = """
            SELECT 
                e.id as execution_id,
                TO_CHAR(e.start_time, 'YYYY-MM-DD HH24:MI:SS') as execution_date,
                e.status,
                e.total_errors,
                e.total_warnings,
                e.error_message as execution_error,
                s.stage_name,
                s.error_count as stage_errors,
                s.warning_count as stage_warnings,
                s.error_details as stage_error_details
            FROM pipeline.executions e
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            LEFT JOIN pipeline.stage_executions s ON e.id = s.execution_id
            WHERE p.name = %s
              AND (e.total_errors > 0 OR e.total_warnings > 0 OR s.error_count > 0 OR s.warning_count > 0)
        """
        
        params = [pipeline_name]
        if execution_id:
            query += " AND e.id = %s"
            params.append(execution_id)
            
        query += " ORDER BY e.start_time DESC, s.stage_order;"
        
        self._execute_query_to_csv(query, tuple(params), output_path)
        logger.info(f"✓ Exported errors analysis: {output_path.name}")
    
    def _execute_query_to_csv(self, query: str, params: tuple, output_path: Path):
        """
        Ejecutar query y guardar resultados en CSV.
        
        Args:
            query: Query SQL a ejecutar
            params: Parámetros para el query
            output_path: Ruta del archivo CSV de salida
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                if not rows:
                    # Crear archivo vacío con headers
                    with open(output_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['No data found for this pipeline'])
                    return
                
                # Obtener nombres de columnas
                column_names = rows[0].keys()
                
                # Escribir CSV
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=column_names)
                    writer.writeheader()
                    
                    for row in rows:
                        # Convertir valores a strings, manejando JSONs
                        clean_row = {}
                        for key, value in row.items():
                            if value is None:
                                clean_row[key] = ''
                            elif isinstance(value, (dict, list)):
                                clean_row[key] = json.dumps(value)
                            else:
                                clean_row[key] = value
                        writer.writerow(clean_row)
                
                logger.debug(f"Exported {len(rows)} rows to {output_path}")
                
        except Exception as e:
            logger.error(f"Failed to export query to CSV: {e}")
            raise
    
    def get_available_pipelines(self) -> List[str]:
        """
        Obtener lista de pipelines disponibles en la BD.
        
        Returns:
            Lista de nombres de pipelines
        """
        if not self.connection:
            self.connect()
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT name FROM pipeline.pipelines ORDER BY name;")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get available pipelines: {e}")
            return []
