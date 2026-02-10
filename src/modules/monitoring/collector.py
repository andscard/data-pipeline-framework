"""
MonitoringCollector - Captura de métricas operativas en tiempo real.

Este módulo NO persiste en base de datos. Todas las métricas viven en RAM
durante la ejecución del pipeline y se exportan al finalizar.

Uso:
    monitoring = MonitoringCollector(execution_id="uuid-123")
    
    with monitoring.track_stage("INGESTION") as stage:
        # Tu código limpio aquí
        df = loader.load_data()
        stage.set_records(input=0, output=len(df), failed=0)
    
    health = monitoring.get_health_status()
    report = monitoring.generate_summary()
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional
from contextlib import contextmanager
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class StageStatus(Enum):
    """Estados posibles de una etapa"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class HealthStatus(Enum):
    """Estados de salud del pipeline"""
    HEALTHY = "healthy"      # Verde: 95-100% éxito
    WARNING = "warning"      # Amarillo: 80-95% éxito
    CRITICAL = "critical"    # Rojo: <80% éxito
    FAILED = "failed"        # Rojo: Falló completamente


@dataclass
class StageMetrics:
    """
    Métricas de una etapa específica del pipeline.
    Captura tiempo de ejecución, registros procesados y errores.
    """
    stage_name: str
    status: StageStatus = StageStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    
    # Contadores de registros
    records_input: int = 0
    records_output: int = 0
    records_failed: int = 0
    
    # Errores y warnings
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Métricas de calidad (para etapa VALIDATION)
    quality_score: Optional[float] = None  # 0-100
    validations_passed: int = 0
    validations_failed: int = 0
    
    def start(self):
        """Iniciar medición de la etapa"""
        self.status = StageStatus.RUNNING
        self.start_time = datetime.now()
        logger.debug(f"Stage '{self.stage_name}' started")
    
    def complete(self, success: bool = True):
        """Completar medición de la etapa"""
        self.end_time = datetime.now()
        if self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        if success:
            self.status = StageStatus.COMPLETED
        else:
            self.status = StageStatus.FAILED
        
        logger.debug(f"Stage '{self.stage_name}' completed: {self.status.value} in {self.duration_seconds:.2f}s")
    
    def set_records(self, input: int = 0, output: int = 0, failed: int = 0):
        """Establecer contadores de registros"""
        self.records_input = input
        self.records_output = output
        self.records_failed = failed
    
    def add_error(self, error: str):
        """Agregar un error"""
        self.errors.append(error)
        logger.error(f"[{self.stage_name}] {error}")
    
    def add_warning(self, warning: str):
        """Agregar un warning"""
        self.warnings.append(warning)
        logger.warning(f"[{self.stage_name}] {warning}")
    
    def set_quality_metrics(self, score: float, passed: int, failed: int):
        """Establecer métricas de calidad (solo para VALIDATION)"""
        self.quality_score = score
        self.validations_passed = passed
        self.validations_failed = failed
    
    def get_success_rate(self) -> float:
        """Calcular tasa de éxito de registros procesados"""
        total = self.records_input
        if total == 0:
            return 100.0
        return (self.records_output / total) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para serialización"""
        return {
            'stage_name': self.stage_name,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': round(self.duration_seconds, 2),
            'records_input': self.records_input,
            'records_output': self.records_output,
            'records_failed': self.records_failed,
            'success_rate': round(self.get_success_rate(), 2),
            'errors': self.errors,
            'warnings': self.warnings,
            'quality_score': round(self.quality_score, 2) if self.quality_score else None,
            'validations_passed': self.validations_passed,
            'validations_failed': self.validations_failed
        }


@dataclass
class ExecutionMetrics:
    """
    Métricas globales de toda la ejecución del pipeline.
    Resume el estado de todas las etapas.
    """
    execution_id: str
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration: float = 0.0
    
    stages: Dict[str, StageMetrics] = field(default_factory=dict)
    
    # Contadores globales
    total_records_processed: int = 0
    total_records_failed: int = 0
    total_errors: int = 0
    total_warnings: int = 0
    
    # Métricas de calidad global
    overall_quality_score: Optional[float] = None
    
    def add_stage_metrics(self, stage: StageMetrics):
        """Agregar métricas de una etapa"""
        self.stages[stage.stage_name] = stage
        
        # Actualizar contadores globales
        # CAMBIO: Solo contar registros de INGESTION para evitar duplicados
        if stage.stage_name == "INGESTION":
            self.total_records_processed = stage.records_output
        
        self.total_records_failed += stage.records_failed
        self.total_errors += len(stage.errors)
        self.total_warnings += len(stage.warnings)
    
    def finalize(self):
        """Finalizar métricas de ejecución"""
        self.end_time = datetime.now()
        self.total_duration = (self.end_time - self.start_time).total_seconds()
        
        # Calcular score de calidad global
        validation_stages = [s for s in self.stages.values() if s.quality_score is not None]
        if validation_stages:
            self.overall_quality_score = sum(s.quality_score for s in validation_stages) / len(validation_stages)
    
    def get_health_status(self) -> HealthStatus:
        """
        Determinar estado de salud basado en métricas.
        
        Returns:
            HealthStatus (HEALTHY, WARNING, CRITICAL, FAILED)
        """
        # Si hay alguna etapa fallida -> FAILED
        if any(s.status == StageStatus.FAILED for s in self.stages.values()):
            return HealthStatus.FAILED
        
        # Calcular tasa de éxito global
        if self.total_records_processed == 0:
            return HealthStatus.HEALTHY
        
        success_rate = ((self.total_records_processed - self.total_records_failed) / 
                       self.total_records_processed * 100)
        
        if success_rate >= 95:
            return HealthStatus.HEALTHY
        elif success_rate >= 80:
            return HealthStatus.WARNING
        else:
            return HealthStatus.CRITICAL
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir a diccionario para serialización"""
        return {
            'execution_id': self.execution_id,
            'pipeline_name': self.pipeline_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_duration': round(self.total_duration, 2),
            'health_status': self.get_health_status().value,
            'total_records_processed': self.total_records_processed,
            'total_records_failed': self.total_records_failed,
            'total_errors': self.total_errors,
            'total_warnings': self.total_warnings,
            'overall_quality_score': round(self.overall_quality_score, 2) if self.overall_quality_score else None,
            'stages': {name: stage.to_dict() for name, stage in self.stages.items()}
        }


class MonitoringCollector:
    """
    Colector de métricas operativas en tiempo real.
    
    Responsabilidades:
    - Capturar tiempos de ejecución por etapa
    - Contar registros procesados/fallidos
    - Calcular tasas de éxito y calidad
    - Determinar estado de salud del pipeline
    
    NO responsable de:
    - Persistir en base de datos (eso es AuditManager)
    - Trazabilidad legal (eso es AuditManager)
    - Logs detallados (eso es logging estándar)
    """
    
    def __init__(self, execution_id: str, pipeline_name: str):
        """
        Args:
            execution_id: ID único de la ejecución
            pipeline_name: Nombre del pipeline
        """
        self.metrics = ExecutionMetrics(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            start_time=datetime.now()
        )
        self._current_stage: Optional[StageMetrics] = None
    
    @contextmanager
    def track_stage(self, stage_name: str):
        """
        Context manager para instrumentar una etapa del pipeline.
        
        Usage:
            with monitoring.track_stage("INGESTION") as stage:
                df = load_data()
                stage.set_records(output=len(df))
        
        Args:
            stage_name: Nombre de la etapa (INGESTION, VALIDATION, TRANSFORMATION, OUTPUT)
        
        Yields:
            StageMetrics: Objeto para registrar métricas de la etapa
        """
        stage = StageMetrics(stage_name=stage_name)
        stage.start()
        self._current_stage = stage
        
        try:
            yield stage
            if stage_name == "VALIDATION" and stage.quality_score is not None:
                success = stage.quality_score >= 75.0
            else:
                success = True
            stage.complete(success=success)
        except Exception as e:
            stage.add_error(str(e))
            stage.complete(success=False)
            raise
        finally:
            self.metrics.add_stage_metrics(stage)
            self._current_stage = None
    
    def record_validation_metrics(
        self,
        validator_name: str,
        quality_score: float,
        passed: int,
        failed: int
    ):
        """
        Registrar métricas de validación.
        
        Args:
            validator_name: Nombre del validador (Pandera, GE)
            quality_score: Score de calidad (0-100)
            passed: Validaciones pasadas
            failed: Validaciones fallidas
        """
        if self._current_stage and self._current_stage.stage_name == "VALIDATION":
            self._current_stage.set_quality_metrics(quality_score, passed, failed)
            logger.info(f"[{validator_name}] Quality: {quality_score:.1f}% ({passed}/{passed+failed} passed)")
    
    def finalize(self):
        """Finalizar colección de métricas"""
        self.metrics.finalize()
        
        health = self.metrics.get_health_status()
        logger.info(f"Pipeline execution completed: {health.value}")
        logger.info(f"Total duration: {self.metrics.total_duration:.2f}s")
        logger.info(f"Records processed: {self.metrics.total_records_processed}")
        if self.metrics.overall_quality_score:
            logger.info(f"Overall quality: {self.metrics.overall_quality_score:.1f}%")
    
    def get_health_status(self) -> HealthStatus:
        """Obtener estado de salud actual"""
        return self.metrics.get_health_status()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Obtener resumen completo de métricas.
        
        Returns:
            Diccionario con todas las métricas
        """
        return self.metrics.to_dict()
    
    def get_stage_metrics(self, stage_name: str) -> Optional[StageMetrics]:
        """Obtener métricas de una etapa específica"""
        return self.metrics.stages.get(stage_name)
    
    def has_errors(self) -> bool:
        """Verificar si hubo errores en alguna etapa"""
        return self.metrics.total_errors > 0
    
    def has_warnings(self) -> bool:
        """Verificar si hubo warnings en alguna etapa"""
        return self.metrics.total_warnings > 0
