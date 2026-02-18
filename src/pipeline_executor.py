"""
Pipeline Executor - Orquestador principal del framework.
"""

from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import logging
import pickle
import json
import traceback
from sqlalchemy import create_engine, text, pool
from src.modules.ingestion.multi_source_loader import MultiSourceLoader
from src.modules.validation.ge_validator import GreatExpectationsValidator
from src.modules.validation.schema_validator import convert_simple_validation_to_ge
from src.modules.transformation import DataTransformer
from src.modules.auditing import AuditManager
from src.modules.monitoring import MonitoringCollector
from src.modules.reporting.html_generator import HTMLReportGenerator
from src.modules.reporting.executive_report import ExecutiveReportGenerator
from src.modules.ingestion.config import POSTGRES_CONFIG
from src.modules.validation.exceptions import QualityThresholdError
from src.config import Config

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
        self.exception = None  # Store the actual exception object
        self.report_path = None
        self.monitoring_summary = None  # Métricas de MonitoringCollector
        
    def fail(self, error: str):
        """Marcar como fallida."""
        self.status = "failed"
        self.errors.append(error)
        self.complete("failed")

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
        
        self.state_dir = Config.DATA_DIR / "pipeline_state" / name
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Módulos
        self.loader = MultiSourceLoader()
        self.audit = AuditManager(POSTGRES_CONFIG)
        self.monitoring = None  # Se inicializa en execute() con execution_id
        self._previous_metrics = None  # Métricas de etapas previas
        self._previous_validation_results = []  # Validation results de etapa de validación
        
        # Conectar auditoría
        self.audit.connect()
    
    def _save_state(self):
        """Guardar estado de datasets, métricas y validation results para ejecución de etapas individuales."""
        try:
            state_file = self.state_dir / "datasets.pkl"
            metrics_file = self.state_dir / "metrics.pkl"
            validation_file = self.state_dir / "validation_results.pkl"
            context_file = self.state_dir / "context.json"

            # Guardar contexto de ejecución (ID y timestamp)
            context = {
                "execution_id": self.execution_id,
                "timestamp": getattr(self, 'execution_timestamp', datetime.now().strftime("%Y-%m-%d"))
            }
            with open(context_file, 'w') as f:
                json.dump(context, f)
            
            # Guardar datasets
            with open(state_file, 'wb') as f:
                pickle.dump(self.datasets, f)
            
            # Guardar métricas acumuladas del monitoring
            if self.monitoring:
                with open(metrics_file, 'wb') as f:
                    pickle.dump(self.monitoring.metrics, f)
                logger.info(f"✓ Estado guardado: {len(self.datasets)} datasets + métricas de {len(self.monitoring.metrics.stages)} etapas")
            else:
                logger.info(f"✓ Estado guardado: {len(self.datasets)} datasets")
            
            # Guardar validation results si existen (solo en etapa de validación)
            if self.execution_id:
                validation_results = self.audit.get_validation_results(self.execution_id)
                if validation_results:
                    with open(validation_file, 'wb') as f:
                        pickle.dump(validation_results, f)
                    logger.info(f"✓ Validation results guardados: {len(validation_results)} resultados")
        except Exception as e:
            logger.warning(f"No se pudo guardar estado: {e}")
    
    def _load_state(self):
        """Cargar estado de datasets, métricas y validation results desde ejecución previa."""
        try:
            state_file = self.state_dir / "datasets.pkl"
            metrics_file = self.state_dir / "metrics.pkl"
            validation_file = self.state_dir / "validation_results.pkl"
            context_file = self.state_dir / "context.json"
            execution_file = self.state_dir / "execution_id.txt"

            # Cargar contexto (preferir JSON nuevo, fallback a TXT antiguo)
            if context_file.exists():
                with open(context_file, 'r') as f:
                    context = json.load(f)
                    self.execution_id = context.get("execution_id")
                    self.execution_timestamp = context.get("timestamp")
                logger.info(f"✓ Contexto restaurado: ID={self.execution_id}, Timestamp={self.execution_timestamp}")
            elif execution_file.exists():
                with open(execution_file, 'r') as f:
                    self.execution_id = f.read().strip()
                # Si venimos de versión vieja sin timestamp guardado, se generará uno nuevo en setup (no ideal, pero fallback)
                logger.info(f"✓ Execution ID restaurado (legacy): {self.execution_id}")
            else:
                logger.warning(f"No se encontró contexto previo")
            
            # Cargar datasets
            if state_file.exists():
                with open(state_file, 'rb') as f:
                    self.datasets = pickle.load(f)
                logger.info(f"✓ Datasets restaurados: {len(self.datasets)} datasets")
            else:
                logger.warning(f"No se encontró estado previo de datasets")
            
            # Cargar métricas previas si existen
            if metrics_file.exists():
                with open(metrics_file, 'rb') as f:
                    self._previous_metrics = pickle.load(f)
                logger.info(f"✓ Métricas restauradas: {len(self._previous_metrics.stages)} etapas previas")
            else:
                self._previous_metrics = None
                logger.info("No se encontraron métricas previas (primera etapa)")
            
            # Cargar validation results previos si existen
            if validation_file.exists():
                with open(validation_file, 'rb') as f:
                    self._previous_validation_results = pickle.load(f)
                logger.info(f"✓ Validation results restaurados: {len(self._previous_validation_results)} resultados")
            else:
                self._previous_validation_results = []
        except Exception as e:
            logger.warning(f"No se pudo cargar estado: {e}")
            self._previous_metrics = None
            self._previous_validation_results = []
    
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
    
    def _setup_artifacts_structure(self):
        """Crear estructura de carpetas para artifacts de la ejecución."""
        
        # timestamp_guid para carpeta única (reutilizar si es continuación)
        timestamp = getattr(self, 'execution_timestamp', None)
        if not timestamp:
            timestamp = datetime.now().strftime("%Y-%m-%d")
            self.execution_timestamp = timestamp
            
        execution_folder_name = f"{timestamp}_{self.execution_id}"
        
        # Base unificada: artifacts/{pipeline_name}/executions/{execution_id}/
        self.artifacts_base_path = Config.ARTIFACTS_DIR / self.name / "executions" / execution_folder_name
        self.reports_path = self.artifacts_base_path / "reports"
        self.logs_path = self.artifacts_base_path / "logs"
        
        # Crear directorios
        self.reports_path.mkdir(parents=True, exist_ok=True)
        self.logs_path.mkdir(parents=True, exist_ok=True)
            
    def execute(self, dry_run: bool = False, stage: Optional[str] = None) -> ExecutionResult:
        """
        Ejecutar pipeline completo o una etapa individual.
        
        Args:
            dry_run: Si True, simula ejecución sin procesar datos
            stage: Etapa específica a ejecutar ('ingestion', 'validation', 'transformation', 'output')
                   Si None, ejecuta todas las etapas
            
        Returns:
            ExecutionResult con resultados de ejecución
        """
        if not self.pipeline_id:
            self.register()
        
        # Determinar si debemos continuar una ejecución existente
        is_continuation = stage and stage != 'ingestion'
        
        if is_continuation:
            # Intentar cargar execution_id previo
            self._load_state()
            
            if not self.execution_id:
                # Si no hay ID previo, crear una nueva ejecución (fallback)
                logger.warning(f"No se encontró execution ID previo para etapa {stage}, iniciando nueva ejecución")
                self.execution_id = self.audit.start_execution(
                    pipeline_id=self.pipeline_id,
                    execution_type="manual"
                )
            else:
                logger.info(f"Continuando ejecución existente: {self.execution_id} para etapa {stage}")
        else:
            # Nueva ejecución (ingestion o pipeline completo)
            self.execution_id = self.audit.start_execution(
                pipeline_id=self.pipeline_id,
                execution_type="manual"
            )
        
        # Configurar estructura de artifacts
        self._setup_artifacts_structure()
        
        # Obtener thresholds de la configuración si existen
        thresholds = self.config.get('thresholds') or self.config.get('quality_thresholds')
        
        self.monitoring = MonitoringCollector(
            execution_id=self.execution_id,
            pipeline_name=self.name,
            thresholds=thresholds
        )
        
        # Restaurar métricas previas si es necesario
        # Si es continuation, ya se cargaron en _load_state, ahora solo las aplicamos
        if is_continuation and self._previous_metrics:
            for stage_name, stage_metrics in self._previous_metrics.stages.items():
                self.monitoring.metrics.add_stage_metrics(stage_metrics)
            logger.info(f"✓ Métricas restauradas al monitor: {len(self._previous_metrics.stages)} etapas previas")
        
        result = ExecutionResult(self.name, self.execution_id)
        
        try:
            if dry_run:
                result.complete("completed")
                return result
            
            if not stage or stage == 'ingestion':
                self._execute_ingestion(result)
                # Siempre guardar estado después de ingestión
                self._save_state()
            
            if not stage or stage == 'validation':
                self._execute_validation(result)
                if stage == 'validation':
                    self._save_state()
            
            if not stage or stage == 'transformation':
                self._execute_transformation(result)
                if stage == 'transformation':
                    self._save_state()
            
            if not stage or stage == 'output':
                if 'output' in self.config:
                    self._execute_outputs(result)
                if stage == 'output':
                    self._save_state()
            
            self.monitoring.finalize()
            monitoring_summary = self.monitoring.get_summary()
            
            # Solo marcar como completado en DB si es output o pipeline completo
            if not stage or stage == 'output':
                health_status = self.monitoring.get_health_status()
                execution_status = "completed"
                result.complete(execution_status)
                self.audit.complete_execution(
                    execution_id=self.execution_id,
                    status=execution_status,
                    executive_report_path=str(self.reports_path / "executive_report.html")
                )
                self._generate_executive_report(monitoring_summary)
            else:
                # Para etapas intermedias, solo actualizamos result localmente pero no cerramos la ejecución en DB
                result.complete("in_progress")
            
            if stage == 'validation':
                self._generate_validation_report(monitoring_summary)

            return result

        except Exception as e:
            logger.error(f"Error en ejecución: {e}")
            if self.execution_id:
                try:
                    self.audit.fail_execution(self.execution_id, str(e))
                except:
                    pass
            result.fail(str(e))
            raise
        finally:
            self.audit.close()
    
    def _execute_ingestion(self, result: ExecutionResult):
        """Ejecutar etapa de ingestion."""
        sources = self.config.get('ingestion', {}).get('sources', [])
        
        if not sources:
            return
        
        with self.monitoring.track_stage("INGESTION") as stage:
            # Iniciar tracking en BD
            self.audit.start_stage(
                execution_id=self.execution_id,
                stage_name="INGESTION"
            )
            
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
            # Input es 0 porque lee desde source externo
            stage.set_records(input=0, output=total_records, failed=0)
            
            # Completar tracking en BD con métricas
            self.audit.complete_stage(
                execution_id=self.execution_id,
                stage_name="INGESTION",
                status='completed' if len(stage.errors) == 0 else 'failed',
                records_in=0,
                records_out=stage.records_output,
                records_failed=stage.records_failed,
                error_count=len(stage.errors),
                warning_count=len(stage.warnings),
                error_details=stage.errors
            )
    
    def _execute_validation(self, result: ExecutionResult):
        """
        Ejecutar etapa de validation.
        
        Esta etapa ahora incluye:
          1. Validación de esquema y calidad (Great Expectations)
          2. Detección de ataques OWASP Top 10 (si datos fueron pre-infectados)
        
        Soporta dos formatos de configuración:
          a) Sintaxis simplificada (schema + custom_validations) -> Convertida a GE
          b) Sintaxis legacy (expectations directas)
        """
        with self.monitoring.track_stage("VALIDATION") as stage:
            # Iniciar tracking en BD
            self.audit.start_stage(
                execution_id=self.execution_id,
                stage_name="VALIDATION"
            )
            
            # Support both 'quality' (legacy) and 'validation' (new) config keys
            validation_config = self.config.get('validation', self.config.get('quality', {}))
            
            if 'schema' in validation_config:
                logger.info("Detected simplified schema validation syntax - converting to GE expectations")
                converted_config = convert_simple_validation_to_ge(validation_config)
                validation_config = {
                    **validation_config,
                    'expectations': converted_config.get('expectations', [])
                }
            
            total_validations = 0
            passed_validations = 0
            failed_validations = 0
            total_records_validated = 0
            
            # Estructura para trackear estadísticas por dataset
            dataset_stats = {}
            
            # Validación de calidad con Great Expectations
            expectations = validation_config.get('expectations', [])
            for expectation_config in expectations:
                passed, failed, records = self._run_ge_validation(expectation_config, result)
                total_validations += passed + failed
                passed_validations += passed
                failed_validations += failed
                total_records_validated += records

                # Track per dataset
                ds_name = expectation_config.get('dataset', 'unknown')
                if ds_name not in dataset_stats: dataset_stats[ds_name] = {'passed': 0, 'total': 0}
                dataset_stats[ds_name]['passed'] += passed
                dataset_stats[ds_name]['total'] += (passed + failed)
            
            # Calcular score de calidad
            quality_score = 0
            if total_validations > 0:
                quality_score = (passed_validations / total_validations) * 100
            
            stage.set_quality_metrics(
                score=quality_score,
                passed=passed_validations,
                failed=failed_validations
            )
            
            # Contar registros únicos validados (no multiplicar por número de suites)
            unique_records = sum(len(self.datasets.get(name, [])) for name in self.datasets.keys())
            stage.set_records(input=unique_records, output=unique_records, failed=0)
            
            logger.info(f"\n[VALIDATION] Quality Score: {quality_score:.1f}%")
            logger.info(f"  Passed: {passed_validations}/{total_validations}")
            logger.info(f"  Failed: {failed_validations}/{total_validations}")
            logger.info(f"  Records validated: {unique_records:,}")

            # --- VALIDACIÓN DE UMBRALES POR DATASET ---
            # Verificar si algún dataset individual no cumple su umbral específico
            
            # Obtener configuración de schemas (si existe)
            schemas_config = validation_config.get('schema', {})
            global_threshold = validation_config.get('min_quality_score') or validation_config.get('quality_threshold')

            validation_errors = []

            for ds_name, stats in dataset_stats.items():
                if stats['total'] == 0: continue
                
                ds_score = (stats['passed'] / stats['total']) * 100
                
                # Buscar umbral específico para este dataset
                ds_config = schemas_config.get(ds_name, {})
                # Prioridad: 1. Dataset specific, 2. Global, 3. Default safe (0 si no hay nada definido)
                ds_threshold = ds_config.get('quality_threshold') or global_threshold
                
                if ds_threshold is not None:
                    # Normalizar a 0-100
                    if ds_threshold <= 1.0: ds_threshold *= 100
                    
                    if ds_score < ds_threshold:
                        msg = f"Dataset '{ds_name}' Quality Score ({ds_score:.1f}%) is below configured threshold ({ds_threshold:.1f}%)"
                        logger.error(f"❌ {msg}")
                        validation_errors.append(msg)
                    else:
                        logger.info(f"✓ Dataset '{ds_name}' passed quality check ({ds_score:.1f}% >= {ds_threshold:.1f}%)")
            
            # Registrar errores de umbral en el stage ANTES de generar reporte
            # Esto asegura que el MonitoringCollector marque el status como 'failed'
            if validation_errors:
                for err in validation_errors:
                    stage.add_error(err)
            
            # GENERAR REPORTE ANTES DE FALLAR
            # Es crítico generar el reporte HTML incluso si el pipeline va a fallar por calidad
            # para que el usuario pueda ver QUÉ falló.
            try:
                self.monitoring.finalize() # Asegurar que métricas estén listas
                monitoring_summary = self.monitoring.get_summary()
                report_path = self._generate_validation_report(monitoring_summary)
                if report_path:
                    logger.info(f"✓ Reporte de Validación generado exitosamente: {report_path}")
            except Exception as e:
                logger.error(f"❌ Error crítico generando reporte: {e}")
            
            if validation_errors:
                # Fail the pipeline immediately AFTER generating the report
                raise QualityThresholdError(f"Pipeline failed due to quality thresholds: {'; '.join(validation_errors)}")
            
            # Completar tracking en BD con métricas
            self.audit.complete_stage(
                execution_id=self.execution_id,
                stage_name="VALIDATION",
                status='completed' if len(stage.errors) == 0 else 'failed',
                records_in=stage.records_input,
                records_out=stage.records_output,
                records_failed=stage.records_failed,
                error_count=len(stage.errors),
                warning_count=len(stage.warnings),
                error_details=stage.errors,
                metrics={
                    'quality_score': stage.quality_score,
                    'validations_passed': stage.validations_passed,
                    'validations_failed': stage.validations_failed
                }
            )
    
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
            failed_details = validation_result.get('failed_details', [])
            
            # Registrar SUMMARY de la suite (1 registro con totales)
            self.audit.log_validation_result(
                execution_id=self.execution_id,
                rule_name=suite_name,
                rule_type="great_expectations",
                passed=success,
                failed_count=failed_count,
                failure_details=failed_details,
                dataset_name=dataset_name,
                suite_name=suite_name,
                total_records=record_count,
                severity="critical" if 'security' in suite_name.lower() else "error",
                expectation_type=f"{passed_count}_passed_{failed_count}_failed"
            )
            
            # NUEVO: Registrar resumen agregado de validaciones (reduce inserts 95%+)
            # En lugar de insertar 1 fila por cada validación pasada, insertamos UN resumen
            if passed_count + failed_count > 0:
                quality_score = (passed_count / (passed_count + failed_count)) * 100
                self.audit.log_validation_summary(
                    execution_id=self.execution_id,
                    suite_name=suite_name,
                    dataset_name=dataset_name,
                    total_validations=passed_count + failed_count,
                    passed_validations=passed_count,
                    failed_validations=failed_count,
                    quality_score=quality_score,
                    total_records=record_count
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
            logger.error(traceback.format_exc())
            return (0, len(expectations_list), record_count)
    
    def _execute_transformation(self, result: ExecutionResult):
        """Ejecutar etapa de transformation."""
        transformations = self.config.get('transformation', [])
        
        if not transformations:
            return
        
        with self.monitoring.track_stage("TRANSFORMATION") as stage:
            # Iniciar tracking en BD
            self.audit.start_stage(
                execution_id=self.execution_id,
                stage_name="TRANSFORMATION"
            )
            
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
            
            # Completar tracking en BD con métricas
            self.audit.complete_stage(
                execution_id=self.execution_id,
                stage_name="TRANSFORMATION",
                status='completed' if len(stage.errors) == 0 else 'failed',
                records_in=stage.records_input,
                records_out=stage.records_output,
                records_failed=stage.records_failed,
                error_count=len(stage.errors),
                warning_count=len(stage.warnings),
                error_details=stage.errors
            )
        
        logger.info("Transformation completed")
    
    def _generate_validation_report(self, monitoring_summary: dict):
        """Generar reporte técnico de validación."""
        try:
            logger.info("Generando reporte de validación...")
            validation_results = self.audit.get_validation_results(self.execution_id)
            validation_summary = self.audit.get_validation_summary(self.execution_id)
            
            # --- Extraer Quality Thresholds del Config ---
            quality_thresholds = {}
            validation_config = self.config.get('validation', {}).get('schema', {})
            for dataset_name, dataset_config in validation_config.items():
                if isinstance(dataset_config, dict):
                    threshold = dataset_config.get('quality_threshold')
                    if threshold is not None:
                        quality_thresholds[dataset_name] = threshold
            
            audit_data = {
                'execution_id': self.execution_id,
                'pipeline_id': self.pipeline_id,
                'validation_results': validation_results,
                'validation_summary': validation_summary,
                'quality_thresholds': quality_thresholds # Pasar thresholds al reporte
            }
            
            report_path_target = self.reports_path / "validation_report.html"

            report_generator = HTMLReportGenerator()
            report_path = report_generator.generate_report(
                monitoring_summary=monitoring_summary,
                audit_data=audit_data,
                output_path=report_path_target
            )
            return report_path
            
        except Exception as e:
            logger.error(f"No se pudo generar reporte de validación: {e}")
            return None
    
    def _generate_executive_report(self, monitoring_summary: dict):
        """Generar reporte ejecutivo."""
        try:
            # Use artifacts report path
            report_path_target = self.reports_path / "executive_report.html"

            exec_generator = ExecutiveReportGenerator()
            exec_report_path = exec_generator.generate_report(
                monitoring_summary=monitoring_summary,
                output_path=report_path_target
            )
            logger.info(f"✓ Reporte Ejecutivo: {exec_report_path}")
            
        except Exception as e:
            logger.warning(f"No se pudo generar reporte ejecutivo: {e}")
    
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
