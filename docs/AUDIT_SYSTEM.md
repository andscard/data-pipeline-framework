# Sistema de Auditoría

Sistema de auditoría basado en PostgreSQL que proporciona seguimiento integral de ejecución, métricas de calidad y monitoreo de rendimiento.

## Visión General

El sistema de auditoría captura:
- Historial de ejecución de pipelines
- Resultados de validación y quality scores
- Métricas de rendimiento a nivel de stage
- Seguimiento de health status
- Logs de errores y advertencias

**Schema:** `pipeline`  
**Tablas:** 6 tablas principales  
**Almacenamiento:** PostgreSQL vía contenedor Docker

## Tablas de Base de Datos

### 1. pipelines

Registro de pipelines que almacena configuración y metadata.

```sql
CREATE TABLE pipeline.pipelines (
    id UUID PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    config JSONB,
    version VARCHAR(50),
    owner VARCHAR(255),
    tags TEXT[],
    is_active BOOLEAN DEFAULT true,
    run_count INTEGER DEFAULT 0,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

**Campos Clave:**
- `name`: Identificador de pipeline (único)
- `config`: Configuración YAML completa almacenada como JSONB
- `run_count`: Total de ejecuciones exitosas
- `last_run_at`: Timestamp de última ejecución exitosa
- `is_active`: Habilitar/deshabilitar pipeline sin eliminarlo

**Índices:**
- `idx_name` (UNIQUE)
- `idx_is_active`
- `idx_created_at`
- `idx_owner`
- `idx_tags` (GIN)

### 2. executions

Historial de ejecuciones con métricas de calidad y health status.

```sql
CREATE TABLE pipeline.executions (
    id UUID PRIMARY KEY,
    pipeline_id UUID REFERENCES pipelines(id),
    status VARCHAR(50) CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    execution_type VARCHAR(50) CHECK (execution_type IN ('manual', 'scheduled', 'triggered', 'api')),
    triggered_by VARCHAR(255),
    environment VARCHAR(50) CHECK (environment IN ('development', 'staging', 'production')),
    start_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMPTZ,
    duration_seconds NUMERIC(10,3),
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metrics JSONB DEFAULT '{}',
    stages_summary JSONB DEFAULT '{}',
    quality_score NUMERIC(5,2),
    health_status VARCHAR(50) CHECK (health_status IN ('healthy', 'warning', 'critical', 'failed')),
    total_errors INTEGER DEFAULT 0,
    total_warnings INTEGER DEFAULT 0,
    report_path TEXT,
    executive_report_path TEXT
);
```

**Campos Clave:**
- `status`: Estado de ejecución (pending/running/completed/failed/cancelled)
- `quality_score`: Métrica de calidad general (0-100)
- `health_status`: Evaluación de salud de ejecución
  - `healthy`: quality_score ≥ 95, errors = 0
  - `warning`: quality_score ≥ 80 o warnings > 0
  - `critical`: quality_score < 80 o errors > 0
  - `failed`: status = 'failed'
- `total_errors`: Contador de fallos críticos de validación
- `total_warnings`: Contador de problemas de nivel advertencia
- `records_processed`: Registros procesados exitosamente
- `records_failed`: Validación fallida o transformación

**Índices:**
- `idx_pipeline_id`
- `idx_status`
- `idx_start_time`
- `idx_quality_score`
- `idx_health_status`
- `idx_execution_type`
- `idx_environment`

### 3. validation_summary

Métricas de validación agregadas por ejecución.

```sql
CREATE TABLE pipeline.validation_summary (
    id UUID PRIMARY KEY,
    execution_id UUID UNIQUE REFERENCES executions(id),
    total_validations INTEGER NOT NULL,
    passed_validations INTEGER NOT NULL,
    failed_validations INTEGER NOT NULL,
    warning_validations INTEGER DEFAULT 0,
    critical_failures INTEGER DEFAULT 0,
    total_records_checked INTEGER NOT NULL,
    quality_score NUMERIC(5,2),
    summary_metrics JSONB DEFAULT '{}'
);
```

**Propósito:**
Reduce el tamaño de la tabla validation_results en un 95% almacenando métricas agregadas en lugar de validaciones individuales aprobadas.

**Campos Clave:**
- `total_validations`: Total de verificaciones ejecutadas
- `passed_validations`: Verificaciones exitosas
- `failed_validations`: Verificaciones fallidas
- `critical_failures`: Fallos de alta severidad
- `quality_score`: (passed / total) × 100

**Beneficios:**
- Query único para vista general de calidad de ejecución
- Huella de almacenamiento mínima
- Queries de agregación rápidas

### 4. validation_results

Resultados detallados solo para validaciones fallidas.

```sql
CREATE TABLE pipeline.validation_results (
    id UUID PRIMARY KEY,
    execution_id UUID REFERENCES executions(id),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(100) CHECK (rule_type IN ('schema', 'quality', 'custom', 'great_expectations', 'pandera_schema')),
    dataset_name VARCHAR(255) NOT NULL,
    suite_name VARCHAR(255),
    expectation_type VARCHAR(255),
    passed BOOLEAN NOT NULL,
    failed_count INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    severity VARCHAR(50) CHECK (severity IN ('critical', 'error', 'warning', 'info')),
    failure_details JSONB,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

**Optimización de Almacenamiento:**
Solo almacena validaciones fallidas (passed = false). Validaciones aprobadas se agregan en validation_summary.

**Campos Clave:**
- `expectation_type`: Nombre de expectation de Great Expectations
- `failed_count`: Número de registros que fallaron
- `total_records`: Total de registros verificados
- `failure_details`: JSONB con información de fallo específica
- `severity`: Clasificación de severidad de fallo

**Índices:**
- `idx_execution_id`
- `idx_passed` (para queries de solo fallidos)
- `idx_dataset_name`
- `idx_suite_name`
- `idx_severity`
- `idx_rule_type`
- `idx_expectation_type`

### 5. stage_executions

Seguimiento granular para cada stage del pipeline.

```sql
CREATE TABLE pipeline.stage_executions (
    id UUID PRIMARY KEY,
    execution_id UUID REFERENCES executions(id),
    stage_name VARCHAR(50) CHECK (stage_name IN ('INGESTION', 'VALIDATION', 'TRANSFORMATION', 'OUTPUT')),
    stage_order INTEGER NOT NULL,
    status VARCHAR(50) CHECK (status IN ('running', 'completed', 'failed', 'skipped')),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_seconds NUMERIC(10,3),
    records_in INTEGER DEFAULT 0,
    records_out INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    memory_usage_mb NUMERIC(10,2),
    cpu_usage_percent NUMERIC(5,2),
    error_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    error_details JSONB DEFAULT '[]',
    metrics JSONB DEFAULT '{}',
    CONSTRAINT stage_executions_unique_stage UNIQUE (execution_id, stage_name)
);
```

**Propósito:**
Seguimiento de rendimiento e identificación de cuellos de botella a nivel de stage.

**Campos Clave:**
- `stage_name`: Stage del pipeline (INGESTION/VALIDATION/TRANSFORMATION/OUTPUT)
- `stage_order`: Secuencia de ejecución (1-4)
- `duration_seconds`: Tiempo de ejecución del stage
- `records_in`/`records_out`: Flujo de datos a través del stage
- `metrics`: Métricas específicas del stage almacenadas como JSONB
  - VALIDATION: `quality_score`, `validations_passed`, `validations_failed`
  - INGESTION: `sources_loaded`, `bytes_read`
  - TRANSFORMATION: `operations_applied`

**Índices:**
- `idx_execution_id`
- `idx_stage_name`
- `idx_status`
- `idx_duration`
- `idx_stage_order`

**Uso:**
```sql
-- Identificar stages más lentos
SELECT stage_name, AVG(duration_seconds) as avg_duration
FROM pipeline.stage_executions
WHERE status = 'completed'
GROUP BY stage_name
ORDER BY avg_duration DESC;
```

### 6. audit_logs

Logs de eventos detallados para debugging y cumplimiento.

```sql
CREATE TABLE pipeline.audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID REFERENCES executions(id) ON DELETE SET NULL,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(20) NOT NULL DEFAULT 'INFO' CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    module VARCHAR(100) NOT NULL,
    event VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(100),
    context JSONB DEFAULT '{}'
);
```

**Propósito:**
Logging estructurado para auditoría, debugging y análisis de eventos del framework.

**Campos Clave:**
- `execution_id`: Referencia a la ejecución (nullable para eventos no relacionados a ejecuciones)
- `level`: Nivel de severidad del log (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- `module`: Módulo del framework que generó el log (ej: 'ingestion', 'validation')
- `event`: Tipo de evento (ej: 'pipeline_started', 'connector_initialized')
- `correlation_id`: ID para correlacionar eventos relacionados
- `context`: Metadatos adicionales en formato JSON (usuario, ambiente, etc.)

**Índices:**
- `idx_execution_id`
- `idx_level`
- `idx_module`
- `idx_timestamp`
- `idx_correlation_id`
- `idx_context` (GIN index para búsquedas en JSONB)

**Uso:**
```sql
-- Obtener logs de error de una ejecución específica
SELECT timestamp, module, event, context
FROM pipeline.audit_logs
WHERE execution_id = 'a1b2c3d4-5678-90ab-cdef-1234567890ab'
  AND level IN ('ERROR', 'CRITICAL')
ORDER BY timestamp DESC;
```

## Uso de API

### Registrar Ejecución de Pipeline

```python
from src.modules.auditing.audit_manager import AuditManager
from src.config import POSTGRES_CONFIG

audit = AuditManager(POSTGRES_CONFIG)
audit.connect()

# Registrar pipeline
pipeline_id = audit.register_pipeline(
    name="CustomerDataPipeline",
    description="Procesamiento de datos de clientes",
    config=yaml_config,
    version="1.0.0",
    owner="data_team"
)

# Iniciar ejecución
execution_id = audit.start_execution(
    pipeline_id=pipeline_id,
    execution_type="manual",
    triggered_by="john.doe",
    environment="production"
)

# Rastrear stage
audit.start_stage(execution_id, "INGESTION")
# ... realizar ingesta ...
audit.complete_stage(
    execution_id=execution_id,
    stage_name="INGESTION",
    status="completed",
    records_in=0,
    records_out=10000,
    error_count=0
)

# Registrar validaciones
audit.record_validation_summary(
    execution_id=execution_id,
    total_validations=45,
    passed_validations=41,
    failed_validations=4,
    total_records_checked=10000,
    quality_score=91.11
)

# Completar ejecución
audit.complete_execution(
    execution_id=execution_id,
    status="completed",
    records_processed=9500,
    records_failed=500,
    quality_score=91.11,
    health_status="warning",
    total_errors=4,
    total_warnings=2,
    report_path="reports/execution_12345.html"
)
```

### Seguimiento de Stages

```python
# Iniciar stage
stage_id = audit.start_stage(
    execution_id=execution_id,
    stage_name="VALIDATION"
)

# Completar con métricas
audit.complete_stage(
    execution_id=execution_id,
    stage_name="VALIDATION",
    status="completed",
    records_in=10000,
    records_out=10000,
    records_failed=0,
    error_count=4,
    warning_count=2,
    metrics={
        "quality_score": 91.11,
        "validations_passed": 41,
        "validations_failed": 4
    }
)
```

## Consultar Datos de Auditoría

### Historial de Ejecuciones

```sql
-- Ejecuciones recientes con métricas de calidad
SELECT 
    e.id,
    p.name as pipeline_name,
    e.start_time,
    e.duration_seconds,
    e.status,
    e.health_status,
    e.quality_score,
    e.records_processed,
    e.total_errors,
    e.total_warnings
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
ORDER BY e.start_time DESC
LIMIT 10;
```

### Análisis de Rendimiento de Stages

```sql
-- Duraciones promedio de stages
SELECT 
    stage_name,
    COUNT(*) as executions,
    ROUND(AVG(duration_seconds)::numeric, 3) as avg_duration,
    ROUND(MIN(duration_seconds)::numeric, 3) as min_duration,
    ROUND(MAX(duration_seconds)::numeric, 3) as max_duration,
    ROUND((AVG(records_out)::float / NULLIF(AVG(records_in), 0) * 100)::numeric, 2) as avg_success_rate
FROM pipeline.stage_executions
WHERE status = 'completed'
GROUP BY stage_name
ORDER BY avg_duration DESC;
```

### Tendencias de Calidad

```sql
-- Tendencia de quality score a lo largo del tiempo
SELECT 
    DATE_TRUNC('day', e.start_time) as day,
    p.name as pipeline,
    COUNT(*) as executions,
    ROUND(AVG(e.quality_score)::numeric, 2) as avg_quality,
    SUM(e.total_errors) as total_errors,
    SUM(e.total_warnings) as total_warnings
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.start_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', e.start_time), p.name
ORDER BY day DESC, pipeline;
```

### Resumen de Validaciones Fallidas

```sql
-- Fallos de validación más comunes
SELECT 
    expectation_type,
    dataset_name,
    COUNT(*) as failure_count,
    ROUND(AVG(failed_count)::numeric, 2) as avg_failed_records,
    MAX(severity) as max_severity
FROM pipeline.validation_results
WHERE passed = false
GROUP BY expectation_type, dataset_name
ORDER BY failure_count DESC
LIMIT 20;
```

## Lógica de Health Status

Health status calculado basándose en quality score y contadores de errores:

```python
if status == "failed":
    health_status = "failed"
elif total_errors > 0 or quality_score < 80:
    health_status = "critical"
elif total_warnings > 0 or quality_score < 95:
    health_status = "warning"
else:
    health_status = "healthy"
```

**Umbrales:**
- **healthy**: quality_score ≥ 95%, errors = 0, warnings = 0
- **warning**: 80% ≤ quality_score < 95% O warnings > 0
- **critical**: quality_score < 80% O errors > 0
- **failed**: Ejecución de pipeline falló

## Mantenimiento

### Monitoreo de Tamaño de Base de Datos

```sql
-- Tamaños de tablas
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'pipeline'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Limpieza de Datos Antiguos

```sql
-- Eliminar ejecuciones de más de 90 días
DELETE FROM pipeline.executions
WHERE start_time < CURRENT_DATE - INTERVAL '90 days';

-- Cascade elimina validation_results, stage_executions, audit_logs
```

### Vacuum y Analyze

```bash
# Mantenimiento regular
docker exec framework_postgres psql -U admin -d data_framework -c "VACUUM ANALYZE pipeline.executions;"
docker exec framework_postgres psql -U admin -d data_framework -c "VACUUM ANALYZE pipeline.validation_results;"
```

## Exportar Métricas

Usar el comando CLI `export-logs` para extraer datos de auditoría a CSV:

```bash
data-framework export-logs -n CustomerDataPipeline -o logs/
```

Genera 6 archivos CSV:
- `executions_summary.csv` - Historial de ejecuciones
- `stages_performance.csv` - Métricas de stages
- `validation_quality.csv` - Quality scores por suite
- `validation_failures.csv` - Detalles de validaciones fallidas
- `timeline.csv` - Timeline de ejecuciones
- `errors_analysis.csv` - Patrones de errores

Ver [EXPORT_LOGS.md](EXPORT_LOGS.md) para detalles.

## Mejores Prácticas

1. **Mantenimiento de Índices**: Ejecutar VACUUM ANALYZE semanalmente en tablas de alta escritura
2. **Retención de Datos**: Archivar ejecuciones de más de 90 días a almacenamiento separado
3. **Monitoreo**: Configurar alertas en health_status = 'critical' o 'failed'
4. **Rendimiento**: Usar stage_executions para identificar cuellos de botella
5. **Tendencias de Calidad**: Rastrear quality_score a lo largo del tiempo para detectar degradación
6. **Particionamiento**: Considerar particionar tabla executions por mes para despliegues grandes
