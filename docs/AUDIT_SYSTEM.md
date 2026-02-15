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

Métricas de validación agregadas por suite y dataset.

```sql
CREATE TABLE pipeline.validation_summary (
    id UUID PRIMARY KEY,
    execution_id UUID REFERENCES executions(id),
    suite_name VARCHAR(255) NOT NULL,
    dataset_name VARCHAR(255) NOT NULL,
    total_validations INTEGER NOT NULL,
    passed_validations INTEGER NOT NULL,
    failed_validations INTEGER NOT NULL,
    quality_score NUMERIC(5,2) NOT NULL,
    total_records INTEGER DEFAULT 0,
    execution_time_ms INTEGER,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT validation_summary_unique UNIQUE (execution_id, suite_name, dataset_name)
);
```

**Propósito:**
Almacena métricas agregadas por suite de validación, reduciendo la necesidad de consultar resultados individuales.

**Campos Clave:**
- `suite_name`: Nombre del grupo de validaciones (ej. "Schema Checks")
- `dataset_name`: Dataset validado
- `quality_score`: Porcentaje de éxito (0-100)
- `execution_time_ms`: Tiempo de ejecución en milisegundos

### 4. validation_results

Resultados detallados de validaciones.

```sql
CREATE TABLE pipeline.validation_results (
    id UUID PRIMARY KEY,
    execution_id UUID REFERENCES executions(id),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(100) NOT NULL,
    dataset_name VARCHAR(255),
    suite_name VARCHAR(255),
    expectation_type VARCHAR(255),
    passed BOOLEAN NOT NULL,
    failed_count INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    severity VARCHAR(50) DEFAULT 'error',
    failure_details JSONB DEFAULT '[]',
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

**Optimización:**
Se recomienda almacenar solo las validaciones fallidas para ahorrar espacio, aunque el esquema permite almacenar todas.

**Campos Clave:**
- `rule_name`: Nombre legible de la regla
- `rule_type`: Origen de la regla (schema, quality, custom)
- `failed_count`: Registros que no cumplieron la regla
- `failure_details`: JSON con muestras de datos fallidos
- `severity`: critical, error, warning, info

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

## Uso de API y Consultas

Ejemplos de cómo interactuar con el sistema de auditoría.

### Consultas Comunes

#### Historial Reciente
```sql
SELECT 
    p.name, 
    e.start_time, 
    e.status, 
    e.quality_score, 
    e.duration_seconds
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
ORDER BY e.start_time DESC
LIMIT 10;
```

#### Rendimiento por Stage
```sql
SELECT 
    stage_name,
    COUNT(*) as executions,
    ROUND(AVG(duration_seconds)::numeric, 3) as avg_sec
FROM pipeline.stage_executions
WHERE status = 'completed'
GROUP BY stage_name
ORDER BY avg_sec DESC;
```

#### Validaciones con más Fallos
```sql
SELECT 
    rule_name,
    COUNT(*) as failures
FROM pipeline.validation_results
WHERE passed = false
GROUP BY rule_name
ORDER BY failures DESC
LIMIT 5;
```

## Mantenimiento

### Limpieza de Datos
El script `scripts/utils_db.py` incluye utilidades de mantenimiento, o vía SQL:

```sql
-- Eliminar ejecuciones antiguas (ej: > 90 días)
DELETE FROM pipeline.executions
WHERE start_time < CURRENT_DATE - INTERVAL '90 days';
-- El borrado en cascada limpia tablas relacionadas
```
