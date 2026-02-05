"""
Pipeline Executor - Orquestador principal del framework.

NUEVA ARQUITECTURA (3 ETAPAS):
1. Ingestion: Carga de datos desde fuentes
2. Validation: Validación de esquema, calidad y detección de ataques/anomalías
3. Transformation: Transformaciones sin agregaciones

NOTA: La infección de datos ahora ocurre ANTES del pipeline usando el módulo
      data_infection. Security testing se realiza durante la validación.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import logging
import yaml
import os

from src.modules.ingestion.multi_source_loader import MultiSourceLoader
from src.modules.validation.pandera_validator import PanderaValidator
from src.modules.validation.ge_validator import GreatExpectationsValidator
from src.modules.transformation import DataTransformer
from src.modules.auditing import AuditManager
from src.modules.monitoring import MonitoringCollector
from src.modules.reporting.html_generator_v2 import HTMLReportGenerator
from src.modules.ingestion.config import POSTGRES_CONFIG
from src.config import config
# Security detection moved to validation stage
# Data infection happens PRE-PIPELINE using data_infection module

logger = logging.getLogger(__name__)


class ExecutionResult:
    """Resultado de ejecución de pipeline."""
    
    def __init__(self, pipeline_name: str, execution_id: str):
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id
        self.status = "pending"
        self.start_time = datetime.now()
        self.end_time = None
        self.duration_seconds = 0
        self.records_processed = 0
        self.records_failed = 0
        self.errors = []
        self.report_path = None
        self.monitoring_summary = None  # Métricas de MonitoringCollector
        
    def complete(self, status: str = "completed"):
        """Completar ejecución."""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = status


class PipelineExecutor:
    """
    Ejecutor de pipelines de datos.
    
    NUEVA ARQUITECTURA - 3 ETAPAS:
      1. Ingestion: Carga datos (limpios o pre-infectados)
      2. Validation: Detecta problemas, anomalías, ataques OWASP Top 10
      3. Transformation: Limpia y transforma
    
    CAMBIO IMPORTANTE:
      - Security testing ya NO está en el pipeline
      - Los datos se infectan ANTES con el módulo data_infection
      - Validation detecta los ataques durante el pipeline
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Args:
            name: Nombre del pipeline
            config: Configuración completa del YAML
        """
        self.name = name
        self.config = config
        self.pipeline_id = None
        self.execution_id = None
        
        # Datasets en memoria
        self.datasets = {}
        
        # Módulos
        self.loader = MultiSourceLoader()
        self.audit = AuditManager(POSTGRES_CONFIG)
        self.monitoring = None  # Se inicializa en execute() con execution_id
        
        # Conectar auditoría
        self.audit.connect()
    
    def register(self) -> Optional[str]:
        """
        Registrar pipeline en la base de datos de auditoría.
        
        Returns:
            UUID del pipeline registrado
        """
        pipeline_config = self.config.get('pipeline', {})
        pipeline_name = pipeline_config.get('name', self.name)
        pipeline_desc = pipeline_config.get('description', '')
        
        self.pipeline_id = self.audit.register_pipeline(
            name=pipeline_name,
            description=pipeline_desc,
            config=self.config,
            version=pipeline_config.get('version', '1.0.0'),
            owner=pipeline_config.get('owner'),
            tags=pipeline_config.get('tags', [])
        )
        
        return self.pipeline_id
    
    @classmethod
    def load(cls, name: str) -> Optional['PipelineExecutor']:
        """
        Cargar pipeline existente desde la base de datos.
        
        Args:
            name: Nombre del pipeline
            
        Returns:
            Instancia de PipelineExecutor o None si no existe
        """
        audit = AuditManager(POSTGRES_CONFIG)
        if not audit.connect():
            return None
        
        pipeline_data = audit.get_pipeline_by_name(name)
        audit.close()
        
        if not pipeline_data:
            return None
        
        # Reconstruir configuración desde config
        config = pipeline_data.get('config', {})
        executor = cls(name, config)
        executor.pipeline_id = str(pipeline_data['id'])
        
        return executor
    
    def execute(self, dry_run: bool = False) -> ExecutionResult:
        """
        Ejecutar pipeline completo.
        
        Args:
            dry_run: Si True, simula ejecución sin procesar datos
            
        Returns:
            ExecutionResult con resultados de ejecución
        """
        # Auto-registrar pipeline si no existe
        if not self.pipeline_id:
            logger.info("Pipeline no registrado, auto-registrando...")
            self.register()
        
        # Iniciar registro de ejecución en auditoría
        self.execution_id = self.audit.start_execution(
            pipeline_id=self.pipeline_id,
            execution_type="manual"
        )
        
        # Inicializar MonitoringCollector para métricas en tiempo real
        self.monitoring = MonitoringCollector(
            execution_id=self.execution_id,
            pipeline_name=self.name
        )
        
        result = ExecutionResult(self.name, self.execution_id)
        
        try:
            if dry_run:
                result.complete("completed")
                return result
            
            # NUEVA ARQUITECTURA: 3 ETAPAS
            # (Los datos ya pueden venir infectados desde data_infection module)
            
            # ETAPA 1: INGESTION
            self._execute_ingestion(result)
            
            # ETAPA 2: VALIDATION (detecta ataques y problemas de calidad)
            self._execute_validation(result)
            
            # ETAPA 3: TRANSFORMATION
            self._execute_transformation(result)
            
            # ETAPA 4: OUTPUT (opcional)
            if 'output' in self.config:
                self._execute_outputs(result)
            
            # Completar ejecución exitosa
            result.complete("completed")
            
            # Finalizar monitoring y obtener resumen
            self.monitoring.finalize()
            monitoring_summary = self.monitoring.get_summary()
            
            # Generar reporte HTML con datos detallados
            try:
                # Recopilar datos de auditoría para el reporte
                audit_data = {
                    'execution_id': self.execution_id,
                    'pipeline_id': self.pipeline_id,
                    'validation_results': self.audit.get_validation_results(self.execution_id)
                }
                
                report_generator = HTMLReportGenerator()
                report_path = report_generator.generate_report(
                    monitoring_summary=monitoring_summary,
                    audit_data=audit_data
                )
                result.report_path = report_path
                logger.info(f"✓ Reporte HTML: {report_path}")
            except Exception as e:
                logger.warning(f"No se pudo generar reporte HTML: {e}")
            
            self.audit.complete_execution(
                execution_id=self.execution_id,
                status="completed",
                records_processed=result.records_processed,
                records_failed=result.records_failed,
                stages_summary=[{
                    'stage_name': stage.stage_name,
                    'status': stage.status.value,
                    'duration_seconds': stage.duration_seconds,
                    'records_input': stage.records_input,
                    'records_output': stage.records_output,
                    'records_failed': stage.records_failed,
                    'quality_score': stage.quality_score,
                    'validations_passed': stage.validations_passed,
                    'validations_failed': stage.validations_failed
                } for stage in self.monitoring.metrics.stages],
                quality_score=monitoring_summary.get('overall_quality_score'),
                report_path=str(result.report_path) if result.report_path else None,
                metrics={
                    'health_status': self.monitoring.get_health_status().value
                }
            )
            
            # Guardar resumen de monitoring en result para reporte HTML
            result.monitoring_summary = monitoring_summary
            
        except Exception as e:
            result.errors.append(str(e))
            result.complete("failed")
            
            # Finalizar monitoring incluso en caso de error
            if self.monitoring:
                self.monitoring.finalize()
                result.monitoring_summary = self.monitoring.get_summary()
            
            self.audit.complete_execution(
                execution_id=self.execution_id,
                status="failed",
                records_processed=result.records_processed,
                records_failed=result.records_failed,
                error_message=str(e)
            )
        
        finally:
            self.audit.close()
        
        return result
    
    def _execute_ingestion(self, result: ExecutionResult):
        """Ejecutar etapa de ingestion."""
        sources = self.config.get('ingestion', {}).get('sources', [])
        
        if not sources:
            return
        
        with self.monitoring.track_stage("INGESTION") as stage:
            total_records = 0
            
            for source_config in sources:
                source_name = source_config.get('name', 'unnamed')
                output_dataset = source_config.get('output_dataset')
                
                try:
                    df = self.loader.load_from_config(source_config)
                    
                    if output_dataset:
                        self.datasets[output_dataset] = df
                        total_records += len(df)
                        result.records_processed += len(df)
                    
                    self.audit.log_audit_event(
                        execution_id=self.execution_id,
                        level="INFO",
                        module="ingestion",
                        event=f"Source loaded: {source_name}",
                        context={"rows": len(df), "dataset": output_dataset}
                    )
                    
                except Exception as e:
                    result.errors.append(f"Ingestion error in {source_name}: {e}")
                    stage.add_error(f"Source '{source_name}': {e}")
                    
                    self.audit.log_audit_event(
                        execution_id=self.execution_id,
                        level="ERROR",
                        module="ingestion",
                        event=f"Source failed: {source_name}",
                        context={"error": str(e)}
                    )
            
            # Establecer métricas de la etapa
            stage.set_records(input=0, output=total_records, failed=0)
    
    # SECURITY STAGE REMOVED
    # Security testing ahora se hace PRE-PIPELINE usando data_infection module
    # La detección de ataques ocurre en _execute_validation()
    
    def _execute_validation(self, result: ExecutionResult):
        """
        Ejecutar etapa de validation.
        
        Esta etapa ahora incluye:
          1. Validación de esquema (Pandera)
          2. Validación de calidad (Great Expectations)
          3. Detección de ataques OWASP Top 10 (si datos fueron pre-infectados)
        """
        with self.monitoring.track_stage("VALIDATION") as stage:
            # Support both 'quality' (legacy) and 'validation' (new) config keys
            validation_config = self.config.get('validation', self.config.get('quality', {}))
            
            total_validations = 0
            passed_validations = 0
            failed_validations = 0
            total_records_validated = 0
            
            # Validación de esquema con Pandera
            schema_validations = validation_config.get('schema_validation', [])
            for schema_config in schema_validations:
                passed, failed, records = self._run_pandera_validation(schema_config, result)
                total_validations += passed + failed
                passed_validations += passed
                failed_validations += failed
                total_records_validated += records
            
            # Validación de calidad con Great Expectations
            expectations = validation_config.get('expectations', [])
            for expectation_config in expectations:
                passed, failed, records = self._run_ge_validation(expectation_config, result)
                total_validations += passed + failed
                passed_validations += passed
                failed_validations += failed
                total_records_validated += records
            
            # Calcular score de calidad
            if total_validations > 0:
                quality_score = (passed_validations / total_validations) * 100
                stage.set_quality_metrics(
                    score=quality_score,
                    passed=passed_validations,
                    failed=failed_validations
                )
                
                # Establecer conteo de registros validados
                stage.set_records(input=total_records_validated, output=total_records_validated, failed=0)
                
                logger.info(f"\n[VALIDATION] Quality Score: {quality_score:.1f}%")
                logger.info(f"  Passed: {passed_validations}/{total_validations}")
                logger.info(f"  Failed: {failed_validations}/{total_validations}")
                logger.info(f"  Records validated: {total_records_validated:,}")
    
    def _run_pandera_validation(self, validation_config: Dict[str, Any], result: ExecutionResult) -> tuple[int, int, int]:
        """Ejecutar validación con Pandera.
        
        Returns:
            tuple: (validaciones_pasadas, validaciones_fallidas, registros_validados)
        """
        validation_name = validation_config.get('name', 'unnamed')
        input_dataset = validation_config.get('input_dataset')
        output_dataset = validation_config.get('output_dataset')
        
        if input_dataset not in self.datasets:
            return (0, 0, 0)
        
        df = self.datasets[input_dataset]
        record_count = len(df)
        
        try:
            validator = PanderaValidator(validation_config.get('schema', {}))
            validated_df, validation_result = validator.validate(df)
            
            # Guardar resultado
            if output_dataset:
                self.datasets[output_dataset] = validated_df
            
            # Registrar en auditoría
            passed = validation_result.get('passed', False)
            failed_count = validation_result.get('failed_count', 0)
            
            self.audit.log_validation_result(
                execution_id=self.execution_id,
                rule_name=validation_name,
                rule_type="pandera_schema",
                passed=passed,
                failed_count=failed_count,
                failure_details=validation_result.get('failures', []),
                dataset_name=dataset_name,
                total_records=record_count,
                severity="error" if not passed else "info"
            )
            
            # Retornar métricas
            if passed:
                return (1, 0, record_count)
            else:
                return (0, 1, record_count)
            
        except Exception as e:
            result.errors.append(f"Pandera validation error in {validation_name}: {e}")
            return (0, 1, record_count)
    
    def _run_ge_validation(self, expectation_config: Dict[str, Any], result: ExecutionResult) -> tuple[int, int, int]:
        """Ejecutar validación con Great Expectations.
        
        Returns:
            tuple: (validaciones_pasadas, validaciones_fallidas, registros_validados)
        """
        dataset_name = expectation_config.get('dataset', 'unknown')
        suite_name = expectation_config.get('suite_name', 'unnamed_suite')
        expectations_list = expectation_config.get('expectations', [])
        
        if dataset_name not in self.datasets:
            logger.warning(f"Dataset '{dataset_name}' no encontrado para validación GE")
            return (0, 0, 0)
        
        df = self.datasets[dataset_name]
        record_count = len(df)
        
        try:
            # Crear validador GE
            validator = GreatExpectationsValidator()
            
            # Construir suite desde configuración
            validator.build_expectation_suite_from_config(suite_name, expectations_list)
            
            # Ejecutar validación
            validation_result = validator.validate(df, suite_name, dataset_name)
            
            # Extraer métricas
            passed_count = validation_result.get('passed_expectations', 0)
            failed_count = validation_result.get('failed_expectations', 0)
            success = validation_result.get('success', False)
            
            # Registrar cada expectativa individualmente en auditoría
            failed_details = validation_result.get('failed_details', [])
            
            # Primero, registrar las expectativas fallidas
            for failed_detail in failed_details:
                self.audit.log_validation_result(
                    execution_id=self.execution_id,
                    rule_name=suite_name,
                    rule_type="great_expectations",
                    passed=False,
                    failed_count=1,
                    failure_details=[failed_detail],
                    dataset_name=dataset_name,
                    suite_name=suite_name,
                    total_records=record_count,
                    severity="critical" if 'security' in suite_name.lower() else "error",
                    expectation_type=failed_detail.get('expectation_type')
                )
            
            # Luego, registrar las expectativas que pasaron (sin detalles)
            for _ in range(passed_count):
                self.audit.log_validation_result(
                    execution_id=self.execution_id,
                    rule_name=suite_name,
                    rule_type="great_expectations",
                    passed=True,
                    failed_count=0,
                    failure_details=[],
                    dataset_name=dataset_name,
                    suite_name=suite_name,
                    total_records=record_count,
                    severity="info"
                )
            
            # Log de seguridad si es suite de detección
            if 'security' in suite_name.lower() or 'seguridad' in suite_name.lower():
                if failed_count > 0:
                    logger.warning(f"⚠️  VULNERABILITIES DETECTED: {failed_count} security issues found")
                    for detail in validation_result.get('failed_details', [])[:5]:  # Primeras 5
                        logger.warning(f"   - {detail.get('expectation_type')}: {detail.get('kwargs')}")
                else:
                    logger.info(f"✓ Security validation passed: No vulnerabilities detected")
            
            return (passed_count, failed_count, record_count)
            
        except Exception as e:
            logger.error(f"GE validation error in {suite_name}: {e}")
            result.errors.append(f"GE validation error in {suite_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return (0, len(expectations_list), record_count)
    
    def _execute_transformation(self, result: ExecutionResult):
        """Ejecutar etapa de transformation."""
        transformations = self.config.get('transformation', [])
        
        if not transformations:
            return
        
        with self.monitoring.track_stage("TRANSFORMATION") as stage:
            total_records = 0
            
            for transform_config in transformations:
                transform_name = transform_config.get('name', 'unnamed')
                input_dataset = transform_config.get('input_dataset')
                output_dataset = transform_config.get('output_dataset')
                operations = transform_config.get('operations', [])
                
                if input_dataset not in self.datasets:
                    continue
                
                df = self.datasets[input_dataset]
                
                try:
                    logger.info(f"  Transformation: {transform_name}")
                    
                    transformer = DataTransformer(transform_name)
                    for operation in operations:
                        df = transformer.apply_operation(df, operation)
                    
                    if output_dataset:
                        self.datasets[output_dataset] = df
                        total_records += len(df)
                    
                    logger.info(f"    Applied {len(operations)} operations")
                    
                    # Registrar en auditoría
                    self.audit.log_audit_event(
                        execution_id=self.execution_id,
                        level="INFO",
                        module="transformation",
                        event=f"Transformation applied: {transform_name}",
                        context={"operations": len(operations), "rows": len(df)}
                    )
                    
                except Exception as e:
                    logger.error(f"    Transformation failed: {e}")
                    result.errors.append(f"Transformation error in {transform_name}: {e}")
                    stage.add_error(f"Transform '{transform_name}': {e}")
            
            # Establecer métricas de la etapa
            stage.set_records(input=total_records, output=total_records, failed=0)
        
        logger.info("Transformation completed")
    
    def _execute_outputs(self, result: ExecutionResult):
        """Guardar datasets en destinos configurados (archivos o PostgreSQL)."""
        outputs = self.config.get('output', [])
        
        if not outputs:
            return
        
        logger.info("\n[ETAPA 4: OUTPUT]")
        logger.info(f"Guardando {len(outputs)} outputs...")
        
        with self.monitoring.track_stage("OUTPUT") as stage:
            total_records = 0
            
            for output_config in outputs:
                output_name = output_config.get('name', 'unnamed')
                input_dataset = output_config.get('input_dataset')
                output_type = output_config.get('type')
                
                if input_dataset not in self.datasets:
                    logger.warning(f"  Output '{output_name}': Dataset '{input_dataset}' no encontrado")
                    stage.add_warning(f"Dataset '{input_dataset}' not found for output '{output_name}'")
                    continue
                
                df = self.datasets[input_dataset]
                total_records += len(df)
                
                try:
                    logger.info(f"  Output: {output_name} ({output_type})")
                    
                    if output_type in ['csv', 'excel', 'parquet']:
                        # Guardar en archivo
                        path = output_config.get('path')
                        if not path:
                            raise ValueError(f"Output tipo '{output_type}' requiere 'path'")
                        
                        from pathlib import Path
                        output_path = Path(path)
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        if output_type == 'csv':
                            df.to_csv(output_path, index=False)
                        elif output_type == 'excel':
                            df.to_excel(output_path, index=False)
                        elif output_type == 'parquet':
                            df.to_parquet(output_path, index=False)
                        
                        logger.info(f"    Guardado en: {output_path}")
                        logger.info(f"    Registros: {len(df)}")
                    
                    elif output_type == 'postgres':
                        # Guardar en PostgreSQL
                        table = output_config.get('table')
                        schema = output_config.get('schema', 'public')
                        if_exists = output_config.get('if_exists', 'replace')  # 'replace', 'append', 'fail'
                        
                        if not table:
                            raise ValueError("Output tipo 'postgres' requiere 'table'")
                        
                        from sqlalchemy import create_engine, text, pool
                        from src.modules.ingestion.config import POSTGRES_CONFIG
                        
                        db_url = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@" \
                                 f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
                        
                        engine = create_engine(db_url, poolclass=pool.NullPool)
                        
                        try:
                            # Crear schema si no existe
                            with engine.begin() as conn:
                                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                            
                            # Guardar DataFrame
                            df.to_sql(
                                name=table,
                                con=engine,
                                schema=schema,
                                if_exists=if_exists,
                                index=False,
                                method='multi',
                                chunksize=1000
                            )
                            
                            # Verificar registros insertados
                            with engine.begin() as conn:
                                result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
                                row_count = result.scalar()
                            
                            logger.info(f"    Guardado en: {schema}.{table}")
                            logger.info(f"    Registros: {row_count}")
                            logger.info(f"    Modo: {if_exists}")
                        
                        except Exception as e:
                            logger.error(f"    Error guardando en PostgreSQL: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                            raise
                        
                        finally:
                            engine.dispose()
                    
                    else:
                        logger.warning(f"    Tipo de output no soportado: {output_type}")
                        stage.add_warning(f"Unsupported output type: {output_type}")
                    
                    # Registrar en auditoría
                    self.audit.log_audit_event(
                        execution_id=self.execution_id,
                        level="INFO",
                        module="output",
                        event=f"Output saved: {output_name}",
                        context={
                            "type": output_type,
                            "rows": len(df),
                            "columns": len(df.columns)
                        }
                    )
                
                except Exception as e:
                    logger.error(f"    Output failed: {e}")
                    result.errors.append(f"Output error in {output_name}: {e}")
                    stage.add_error(f"Output '{output_name}': {e}")
            
            # Establecer métricas de la etapa
            stage.set_records(input=total_records, output=total_records, failed=0)
