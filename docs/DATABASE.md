# Gestión de Base de Datos - Data Pipeline Framework

Guía completa para administrar y consultar la base de datos `data_framework` del framework.

---

## 📋 Contenidos

1. [Herramienta db_utils.py](#herramienta-db_utilspy)
2. [Comandos INFO](#comandos-info---consultar-información)
3. [Comandos MAINTENANCE](#comandos-maintenance---mantenimiento)
4. [Esquema de Base de Datos](#esquema-de-base-de-datos)
5. [Queries SQL Útiles](#queries-sql-útiles)
6. [Troubleshooting](#troubleshooting)

---

## Herramienta db_utils.py

**Ubicación:** `scripts/db_utils.py`

Herramienta de línea de comandos para administrar la base de datos del framework sin necesidad de escribir SQL manualmente.

### Sintaxis General

```bash
python scripts/db_utils.py <comando> [opciones]
```

### Ayuda

```bash
python scripts/db_utils.py --help
```

---

## Comandos INFO - Consultar Información

### `status` - Ver Estado de Tablas

Muestra el estado de todas las tablas con número de registros y tamaño en disco.

```bash
python scripts/db_utils.py status
```

**Salida:**
```
================================================================================
  ESTADO DE LA BASE DE DATOS - data_framework
================================================================================

  Schema: PIPELINE
  ----------------------------------------------------------------------------
  [EMPTY]  audit_logs                                   0 rows  |     72 kB
  [OK]     executions                                   4 rows  |     80 kB
  [OK]     pipelines                                    1 rows  |     80 kB
  [OK]     stage_executions                            12 rows  |     80 kB
  [OK]     validation_results                          33 rows  |     80 kB
  [OK]     validation_summary                           4 rows  |     80 kB


  Schema: SAMPLE_DATA
  ----------------------------------------------------------------------------
  [OK]     customers                               10,000 rows  |   2144 kB
  [OK]     customers_infected                      10,000 rows  |   2200 kB
  [OK]     transactions                            50,000 rows  |   5120 kB
  [OK]     transactions_infected                   50,000 rows  |   5200 kB

================================================================================
  TOTAL: 120,054 registros
================================================================================
```

**Interpretación:**
- `[OK]` - Tabla con datos
- `[EMPTY]` - Tabla vacía
- Tamaños en KB/MB ayudan a identificar crecimiento

---

### `pipelines` - Listar Pipelines Registrados

Muestra todos los pipelines registrados en el framework con sus detalles.

```bash
python scripts/db_utils.py pipelines
```

**Salida:**
```
================================================================================
  PIPELINES REGISTRADOS
================================================================================

  Pipeline: CustomerDataPipeline
  ----------------------------------------------------------------------------
    ID:           a1b2c3d4-5678-90ab-cdef-1234567890ab
    Version:      1.0.0
    Owner:        data_team
    Estado:       Activo
    Ejecuciones:  4
    Última ejec.: 2024-02-04 15:30:22
    Creado:       2024-02-01 10:00:00

  Pipeline: DataQualityPipeline
  ----------------------------------------------------------------------------
    ID:           f9e8d7c6-b5a4-3210-fedc-ba0987654321
    Version:      2.1.0
    Owner:        qa_team
    Estado:       Activo
    Ejecuciones:  12
    Última ejec.: 2024-02-04 16:45:10
    Creado:       2024-01-15 09:30:00

================================================================================
```

**Información mostrada:**
- **ID**: UUID único del pipeline
- **Version**: Versión del pipeline
- **Owner**: Responsable del pipeline
- **Estado**: Activo / Inactivo
- **Ejecuciones**: Número total de runs
- **Última ejec.**: Timestamp de último run

---

### `executions` - Ver Últimas Ejecuciones

Muestra el historial de ejecuciones con métricas clave.

**Sintaxis:**
```bash
python scripts/db_utils.py executions                # Últimas 10 (default)
python scripts/db_utils.py executions -l 20          # Últimas 20
python scripts/db_utils.py executions --limit 50     # Últimas 50
```

**Salida:**
```
================================================================================
  ÚLTIMAS 10 EJECUCIONES
================================================================================

  [OK] CustomerDataPipeline
  ----------------------------------------------------------------------------
    ID:           a1b2c3d4-5678-90ab-cdef-1234567890ab
    Tipo:         manual
    Ambiente:     development
    Ejecutado por: user@example.com
    Duración:     8.61s
    Calidad:      97.5/100
    Registros:    60,000
    Inicio:       2024-02-04 15:30:22
    Fin:          2024-02-04 15:30:31

  [FAIL] DataQualityPipeline
  ----------------------------------------------------------------------------
    ID:           f9e8d7c6-b5a4-3210-fedc-ba0987654321
    Tipo:         scheduled
    Ambiente:     production
    Ejecutado por: airflow_scheduler
    Duración:     N/A
    Calidad:      N/A
    Registros:    0
    Inicio:       2024-02-04 14:20:10
    Fin:          En progreso

  [RUN] SecurityTestPipeline
  ----------------------------------------------------------------------------
    ID:           1234abcd-ef56-7890-ghij-klmnopqrstuv
    Tipo:         triggered
    Ambiente:     staging
    Ejecutado por: webhook_api
    Duración:     N/A
    Calidad:      N/A
    Registros:    0
    Inicio:       2024-02-04 16:45:10
    Fin:          En progreso

================================================================================
```

**Estados posibles:**
- `[OK]` - completed
- `[FAIL]` - failed
- `[RUN]` - running
- `[PEND]` - pending

**Métricas clave:**
- **Tipo**: manual, scheduled, triggered, api
- **Ambiente**: development, staging, production
- **Duración**: Tiempo total de ejecución
- **Calidad**: Quality score 0-100
- **Registros**: Total procesado

---

### `validations` - Ver Resultados de Validaciones

Muestra los resultados de validaciones por pipeline.

**Sintaxis:**
```bash
python scripts/db_utils.py validations                      # Todas
python scripts/db_utils.py validations -l 50                # Últimas 50
python scripts/db_utils.py validations -s failed            # Solo fallidas
python scripts/db_utils.py validations -s passed            # Solo exitosas
python scripts/db_utils.py validations --status failed -l 30  # 30 fallidas
```

**Salida:**
```
================================================================================
  VALIDACIONES (FAILED) - Últimas 20
================================================================================

  Pipeline: CustomerDataPipeline
  ----------------------------------------------------------------------------
    [FAIL] [ERROR]     expect_column_values_to_be_unique
        Dataset: raw_customers | Tipo: expect_column_values_to_be_unique
        Registros: 10,000 | Fallidos: 15 (0.2%)
        Fecha: 2024-02-04 15:30:25

    [FAIL] [CRITICAL]  sql_injection_detection
        Dataset: raw_customers | Tipo: expect_column_values_to_not_match_regex
        Registros: 10,000 | Fallidos: 3 (0.0%)
        Fecha: 2024-02-04 15:30:26

    [FAIL] [WARNING]   expect_column_mean_to_be_between
        Dataset: raw_customers | Tipo: expect_column_mean_to_be_between
        Registros: 10,000 | Fallidos: 0 (0.0%)
        Fecha: 2024-02-04 15:30:27

  Pipeline: DataQualityPipeline
  ----------------------------------------------------------------------------
    [FAIL] [ERROR]     expect_column_values_to_not_be_null
        Dataset: transactions | Tipo: expect_column_values_to_not_be_null
        Registros: 50,000 | Fallidos: 120 (0.2%)
        Fecha: 2024-02-04 14:20:15

================================================================================
```

**Severidades:**
- `[CRITICAL]` - Vulnerabilidades de seguridad críticas
- `[ERROR]` - Violaciones de calidad serias
- `[WARNING]` - Advertencias menores
- `[INFO]` - Informativas

**Filtros disponibles:**
- `-s passed` - Solo validaciones exitosas
- `-s failed` - Solo validaciones fallidas
- `-l N` - Limitar resultados

---

### `stats` - Estadísticas del Framework

Muestra estadísticas completas del framework con métricas agregadas.

```bash
python scripts/db_utils.py stats
```

**Salida:**
```
================================================================================
  ESTADÍSTICAS DEL FRAMEWORK
================================================================================

  GENERAL
  ----------------------------------------------------------------------------
    Pipelines registrados:     3
    Total ejecuciones:         47
    - Exitosas:                42
    - Fallidas:                5
    - Tasa de éxito:           89.4%

    Total validaciones:        1,551
    - Fallidas:                87

    Duración promedio:         12.3s
    Calidad promedio:          94.2/100

  ACTIVIDAD (Últimos 7 días)
  ----------------------------------------------------------------------------
    2024-02-04:  12 ejecuciones (11 exitosas)
    2024-02-03:  8 ejecuciones (8 exitosas)
    2024-02-02:  15 ejecuciones (13 exitosas)
    2024-02-01:  7 ejecuciones (6 exitosas)
    2024-01-31:  3 ejecuciones (3 exitosas)
    2024-01-30:  2 ejecuciones (1 exitosas)

================================================================================
```

**Métricas incluidas:**
- Total de pipelines, ejecuciones, validaciones
- Tasa de éxito global
- Duración y calidad promedio
- Actividad diaria (últimos 7 días)

---

## Comandos MAINTENANCE - Mantenimiento

### `clean-samples` - Limpiar Datos de Ejemplo

Elimina solo los datos de ejemplo (schema `sample_data`), manteniendo intacta toda la auditoría.

```bash
python scripts/db_utils.py clean-samples
```

**Prompt interactivo:**
```
================================================================================
  LIMPIEZA DE DATOS DE EJEMPLO
================================================================================

  [INFO] Esto eliminará solo sample_data.* (datos sintéticos)
  [INFO] Las tablas pipeline.* NO se tocarán

  ¿Continuar? (y/n): y

  Limpiando datos...
  [OK] sample_data.customers limpiada
  [OK] sample_data.customers_infected limpiada
  [OK] sample_data.transactions limpiada
  [OK] sample_data.transactions_infected limpiada

  [SUCCESS] Limpieza completada
  [TIP] Regenera datos: python scripts/generate_sample_data.py -c 10000 -t 50000
```

**Tablas afectadas:**
- ✅ `sample_data.customers` - Eliminada
- ✅ `sample_data.customers_infected` - Eliminada
- ✅ `sample_data.transactions` - Eliminada
- ✅ `sample_data.transactions_infected` - Eliminada
- ❌ `pipeline.*` - **NO se toca**

**Casos de uso:**
- Reiniciar datos de prueba desde cero
- Liberar espacio en disco
- Preparar ambiente para nueva carga

---

### `clean-old` - Limpiar Ejecuciones Antiguas

Elimina ejecuciones antiguas manteniendo solo las recientes (default: últimos 30 días).

**Sintaxis:**
```bash
python scripts/db_utils.py clean-old                   # >30 días (default)
python scripts/db_utils.py clean-old -d 60             # >60 días
python scripts/db_utils.py clean-old --days 90         # >90 días
```

**Prompt interactivo:**
```
================================================================================
  LIMPIEZA DE EJECUCIONES ANTIGUAS (>30 días)
================================================================================

  [INFO] Se eliminarán 184 ejecuciones antiguas
  [INFO] Los registros de validaciones asociados también se eliminarán

  ¿Continuar? (y/n): y

  [SUCCESS] 184 ejecuciones eliminadas
  [INFO] Las ejecuciones recientes (<30 días) se mantienen intactas
```

**Tablas afectadas:**
- ✅ `pipeline.executions` - Registros antiguos eliminados
- ✅ `pipeline.stage_executions` - Stages asociados eliminados (CASCADE)
- ✅ `pipeline.validation_results` - Resultados asociados eliminados (CASCADE)
- ✅ `pipeline.validation_summary` - Resúmenes asociados eliminados (CASCADE)
- ✅ `pipeline.audit_logs` - Logs asociados eliminados (SET NULL)

**Casos de uso:**
- Limitar crecimiento de base de datos
- Mantener solo auditoría reciente
- Cumplir políticas de retención de datos

---

### `vacuum` - Optimizar Base de Datos

Ejecuta `VACUUM ANALYZE` en todas las tablas para optimizar rendimiento y liberar espacio.

```bash
python scripts/db_utils.py vacuum
```

**Salida:**
```
================================================================================
  OPTIMIZACIÓN DE BASE DE DATOS
================================================================================

  [INFO] Ejecutando VACUUM ANALYZE en todas las tablas...
  [INFO] Esto puede tardar unos momentos...

  Schema: pipeline
    [OK] pipeline.audit_logs
    [OK] pipeline.executions
    [OK] pipeline.pipelines
    [OK] pipeline.stage_executions
    [OK] pipeline.validation_results
    [OK] pipeline.validation_summary

  Schema: sample_data
    [OK] sample_data.customers
    [OK] sample_data.customers_infected
    [OK] sample_data.transactions
    [OK] sample_data.transactions_infected

  [SUCCESS] Optimización completada
```

**Qué hace VACUUM:**
- Libera espacio de registros eliminados
- Actualiza estadísticas de consultas
- Mejora rendimiento de queries
- Reorganiza índices

**Cuándo ejecutar:**
- Después de eliminar muchos registros
- Si las queries están lentas
- Mantenimiento mensual
- Después de `clean-old`

---

## Esquema de Base de Datos

### Schema: `pipeline`

#### Tabla: `pipelines`

Registro maestro de pipelines configurados.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID | Identificador único (PK) |
| `name` | VARCHAR(255) | Nombre del pipeline (UNIQUE) |
| `description` | TEXT | Descripción del pipeline |
| `config` | JSONB | Configuración YAML en JSON |
| `version` | VARCHAR(50) | Versión del pipeline (default: 1.0.0) |
| `owner` | VARCHAR(255) | Responsable del pipeline |
| `tags` | TEXT[] | Etiquetas para categorización |
| `is_active` | BOOLEAN | Indicador activo/inactivo |
| `run_count` | INTEGER | Contador de ejecuciones |
| `last_run_at` | TIMESTAMP | Timestamp de última ejecución |
| `created_at` | TIMESTAMP | Fecha de creación |
| `updated_at` | TIMESTAMP | Fecha de última actualización |

**Índices:**
- `pipelines_pkey` (PRIMARY KEY) - id
- `pipelines_name_key` (UNIQUE) - name
- `idx_pipelines_name` - name
- `idx_pipelines_owner` - owner
- `idx_pipelines_is_active` - is_active
- `idx_pipelines_last_run_at` - last_run_at DESC
- `idx_pipelines_tags` (GIN) - tags

---

#### Tabla: `executions`

Historial de todas las ejecuciones de pipelines.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID | Identificador único (PK) |
| `pipeline_id` | UUID | FK a pipelines.id |
| `status` | VARCHAR(50) | Estado: pending, running, completed, failed, cancelled |
| `execution_type` | VARCHAR(50) | Tipo: manual, scheduled, triggered, api |
| `triggered_by` | VARCHAR(255) | Usuario o sistema que ejecutó |
| `environment` | VARCHAR(50) | Ambiente: development, staging, production |
| `start_time` | TIMESTAMP | Inicio de ejecución |
| `end_time` | TIMESTAMP | Fin de ejecución |
| `duration_seconds` | NUMERIC(10,3) | Duración total en segundos |
| `records_processed` | INTEGER | Total de registros procesados |
| `records_failed` | INTEGER | Registros con errores |
| `error_message` | TEXT | Mensaje de error si falló |
| `metrics` | JSONB | Métricas adicionales en JSON |
| `stages_summary` | JSONB | Resumen de stages ejecutados |
| `quality_score` | NUMERIC(5,2) | Score de calidad 0-100 |
| `report_path` | TEXT | Ruta al reporte HTML generado |

**Índices:**
- `executions_pkey` (PRIMARY KEY) - id
- `idx_executions_pipeline_id` - pipeline_id
- `idx_executions_status` - status
- `idx_executions_start_time` - start_time DESC
- `idx_executions_execution_type` - execution_type
- `idx_executions_environment` - environment
- `idx_executions_triggered_by` - triggered_by
- `idx_executions_duration` - duration_seconds DESC
- `idx_executions_quality_score` - quality_score DESC

**Constraints:**
- `executions_status_valid` - CHECK (status IN ('pending','running','completed','failed','cancelled'))
- `executions_execution_type_valid` - CHECK (execution_type IN ('manual','scheduled','triggered','api'))
- `executions_environment_valid` - CHECK (environment IN ('development','staging','production'))
- `executions_end_after_start` - CHECK (end_time IS NULL OR end_time >= start_time)

---

#### Tabla: `validation_results`

Resultados detallados de validaciones de calidad y seguridad.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID | Identificador único (PK) |
| `execution_id` | UUID | FK a executions.id |
| `rule_name` | VARCHAR(255) | Nombre de la regla de validación |
| `rule_type` | VARCHAR(100) | Tipo de validación (GE, Pandera) |
| `dataset_name` | VARCHAR(255) | Nombre del dataset validado |
| `suite_name` | VARCHAR(255) | Suite de validación (security, quality, etc.) |
| `expectation_type` | VARCHAR(255) | Tipo de expectativa de GE |
| `passed` | BOOLEAN | ¿Validación pasó? |
| `failed_count` | INTEGER | Número de registros que fallaron |
| `total_records` | INTEGER | Total de registros evaluados |
| `severity` | VARCHAR(50) | Severidad: critical, error, warning, info |
| `failure_details` | JSONB | Detalles de fallos en JSON |
| `timestamp` | TIMESTAMP | Timestamp de validación |

**Índices:**
- `validation_results_pkey` (PRIMARY KEY) - id
- `idx_validation_results_execution_id` - execution_id
- `idx_validation_results_rule_name` - rule_name
- `idx_validation_results_passed` - passed
- `idx_validation_results_severity` - severity
- `idx_validation_results_dataset_name` - dataset_name
- `idx_validation_results_suite_name` - suite_name
- `idx_validation_results_expectation_type` - expectation_type
- `idx_validation_results_timestamp` - timestamp DESC

**Constraints:**
- `validation_results_severity_valid` - CHECK (severity IN ('critical','error','warning','info'))
- `validation_results_failed_count_positive` - CHECK (failed_count >= 0)

---

#### Tabla: `audit_logs`

Logs de auditoría detallados de eventos del framework.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID | Identificador único (PK) |
| `execution_id` | UUID | FK a executions.id (nullable) |
| `timestamp` | TIMESTAMP | Timestamp del evento |
| `level` | VARCHAR(20) | Nivel: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `module` | VARCHAR(100) | Módulo del framework |
| `event` | VARCHAR(255) | Tipo de evento |
| `correlation_id` | VARCHAR(100) | ID de correlación |
| `context` | JSONB | Contexto adicional en JSON |

**Índices:**
- `audit_logs_pkey` (PRIMARY KEY) - id
- `idx_audit_logs_execution_id` - execution_id
- `idx_audit_logs_level` - level
- `idx_audit_logs_module` - module
- `idx_audit_logs_timestamp` - timestamp DESC
- `idx_audit_logs_correlation_id` - correlation_id
- `idx_audit_logs_context` - context (GIN index)

---

### Schema: `sample_data`

Este schema contiene datos sintéticos de prueba generados por `generate_sample_data.py` y versiones infectadas con vulnerabilidades para testing de seguridad.

#### Tabla: `customers`

Datos sintéticos de clientes para pruebas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `customer_id` | TEXT | ID único del cliente (PK) |
| `name` | TEXT | Nombre completo |
| `email` | TEXT | Email |
| `phone` | TEXT | Teléfono |
| `address` | TEXT | Dirección completa |
| `registration_date` | TIMESTAMP | Fecha de registro |
| `last_login` | TIMESTAMP | Última sesión |
| `account_status` | TEXT | Estado: active, inactive, suspended |
| `lifetime_value` | DOUBLE PRECISION | Valor total del cliente |
| `user_comment` | TEXT | Comentarios del usuario |
| `website` | TEXT | Sitio web |
| `ip_address` | TEXT | Dirección IP |
| `credit_card_last4` | TEXT | Últimos 4 dígitos de tarjeta |
| `age` | BIGINT | Edad |
| `postal_code` | TEXT | Código postal |

---

#### Tabla: `customers_infected`

Versiones de datos de clientes con ataques de inyección inyectados para testing de seguridad.

**Estructura:** Igual que `customers` pero con datos maliciosos inyectados (SQL injection, XSS, command injection, etc.)

**Propósito:** Testing del sistema de validación de seguridad (OWASP Top 10).

---

#### Tabla: `transactions`

Datos sintéticos de transacciones para pruebas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `transaction_id` | TEXT | ID único de transacción (PK) |
| `customer_id` | TEXT | FK a customers |
| `amount` | DOUBLE PRECISION | Monto de transacción |
| `transaction_date` | TIMESTAMP | Fecha de transacción |
| `status` | TEXT | Estado: completed, pending, failed |
| `payment_method` | TEXT | Método de pago |
| `description` | TEXT | Descripción |

---

#### Tabla: `transactions_infected`

Versiones de transacciones con datos maliciosos para testing.

**Estructura:** Igual que `transactions` pero con payloads de ataque inyectados.

**Propósito:** Validar detección de ataques en campos transaccionales.

---

## Queries SQL Útiles

### Consultas Básicas

#### 1. Últimas 10 Ejecuciones

```sql
SELECT 
    e.id,
    p.name as pipeline_name,
    e.status,
    e.execution_type,
    e.environment,
    e.duration_seconds,
    e.quality_score,
    e.records_processed,
    e.start_time,
    e.end_time
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
ORDER BY e.start_time DESC
LIMIT 10;
```

#### 2. Count de Ejecuciones por Pipeline

```sql
SELECT 
    p.name,
    p.owner,
    COUNT(e.id) as total_executions,
    COUNT(CASE WHEN e.status = 'completed' THEN 1 END) as successful,
    COUNT(CASE WHEN e.status = 'failed' THEN 1 END) as failed,
    ROUND(AVG(e.duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(e.quality_score), 2) as avg_quality_score
FROM pipeline.pipelines p
LEFT JOIN pipeline.executions e ON p.id = e.pipeline_id
GROUP BY p.name, p.owner
ORDER BY total_executions DESC;
```

#### 3. Validaciones Fallidas por Pipeline

```sql
SELECT 
    p.name as pipeline_name,
    v.rule_name,
    v.severity,
    COUNT(*) as times_failed,
    SUM(v.failed_count) as total_records_failed,
    MAX(v.timestamp) as last_occurrence
FROM pipeline.validation_results v
JOIN pipeline.executions e ON v.execution_id = e.id
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE v.passed = false
GROUP BY p.name, v.rule_name, v.severity
ORDER BY times_failed DESC, severity;
```

---

### Análisis de Calidad

#### 4. Trend de Quality Score

```sql
SELECT 
    DATE(e.start_time) as date,
    p.name as pipeline_name,
    COUNT(e.id) as executions,
    ROUND(AVG(e.quality_score), 2) as avg_quality_score,
    MIN(e.quality_score) as min_quality_score,
    MAX(e.quality_score) as max_quality_score
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.start_time >= CURRENT_DATE - INTERVAL '30 days'
  AND e.status = 'completed'
GROUP BY DATE(e.start_time), p.name
ORDER BY date DESC, pipeline_name;
```

#### 5. Validaciones con Mayor Tasa de Fallo

```sql
SELECT 
    v.rule_name,
    v.expectation_type,
    v.severity,
    COUNT(*) as total_checks,
    COUNT(CASE WHEN v.passed = false THEN 1 END) as failed_checks,
    ROUND(
        COUNT(CASE WHEN v.passed = false THEN 1 END)::numeric / COUNT(*)::numeric * 100, 
        2
    ) as failure_rate_pct,
    SUM(v.failed_count) as total_records_affected
FROM pipeline.validation_results v
GROUP BY v.rule_name, v.expectation_type, v.severity
HAVING COUNT(*) >= 10  -- Al menos 10 checks
ORDER BY failure_rate_pct DESC
LIMIT 20;
```

---

### Análisis de Seguridad

#### 6. Vulnerabilidades de Seguridad Detectadas

```sql
SELECT 
    e.start_time,
    p.name as pipeline_name,
    v.rule_name,
    v.dataset_name,
    v.failed_count,
    v.total_records,
    ROUND(v.failed_count::numeric / v.total_records::numeric * 100, 2) as infection_rate_pct,
    v.failure_details::jsonb->0->'kwargs'->>'column' as affected_column,
    v.failure_details::jsonb->0->'kwargs'->>'regex' as attack_pattern
FROM pipeline.validation_results v
JOIN pipeline.executions e ON v.execution_id = e.id
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE v.suite_name = 'security_detection_suite'
  AND v.passed = false
  AND v.severity IN ('critical', 'error')
ORDER BY e.start_time DESC, v.failed_count DESC;
```

#### 7. Columnas Más Atacadas

```sql
SELECT 
    v.failure_details::jsonb->0->'kwargs'->>'column' as column_name,
    COUNT(*) as attack_detections,
    COUNT(DISTINCT e.id) as affected_executions,
    SUM(v.failed_count) as total_malicious_records,
    ARRAY_AGG(DISTINCT v.rule_name) as attack_types
FROM pipeline.validation_results v
JOIN pipeline.executions e ON v.execution_id = e.id
WHERE v.suite_name = 'security_detection_suite'
  AND v.passed = false
  AND v.failure_details::jsonb->0->'kwargs'->>'column' IS NOT NULL
GROUP BY v.failure_details::jsonb->0->'kwargs'->>'column'
ORDER BY attack_detections DESC
LIMIT 10;
```

---

### Análisis de Performance

#### 8. Pipelines Más Lentos

```sql
SELECT 
    p.name,
    p.owner,
    COUNT(e.id) as executions,
    ROUND(AVG(e.duration_seconds), 2) as avg_seconds,
    ROUND(MIN(e.duration_seconds), 2) as min_seconds,
    ROUND(MAX(e.duration_seconds), 2) as max_seconds,
    ROUND(STDDEV(e.duration_seconds), 2) as stddev_seconds,
    ROUND(AVG(e.records_processed), 0) as avg_records,
    ROUND(AVG(e.records_processed / NULLIF(e.duration_seconds, 0)), 0) as avg_records_per_second
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.status = 'completed'
  AND e.duration_seconds IS NOT NULL
GROUP BY p.name, p.owner
HAVING COUNT(e.id) >= 3
ORDER BY avg_seconds DESC
LIMIT 10;
```

#### 9. Stages Summary - Breakdown de Duración

```sql
SELECT 
    p.name as pipeline_name,
    e.id as execution_id,
    e.start_time,
    jsonb_array_elements(e.stages_summary) ->> 'stage_name' as stage_name,
    (jsonb_array_elements(e.stages_summary) ->> 'duration')::numeric as stage_duration_seconds,
    jsonb_array_elements(e.stages_summary) ->> 'status' as stage_status,
    (jsonb_array_elements(e.stages_summary) ->> 'records')::integer as stage_records
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.stages_summary IS NOT NULL
  AND jsonb_array_length(e.stages_summary) > 0
ORDER BY e.start_time DESC, stage_duration_seconds DESC
LIMIT 50;
```

---

### Reportes Ejecutivos

#### 10. Dashboard Diario - Métricas del día

```sql
SELECT 
    COUNT(DISTINCT p.id) as active_pipelines,
    COUNT(e.id) as total_executions,
    COUNT(CASE WHEN e.status = 'completed' THEN 1 END) as successful_executions,
    COUNT(CASE WHEN e.status = 'failed' THEN 1 END) as failed_executions,
    ROUND(
        COUNT(CASE WHEN e.status = 'completed' THEN 1 END)::numeric / 
        NULLIF(COUNT(e.id), 0)::numeric * 100, 
        2
    ) as success_rate_pct,
    ROUND(AVG(CASE WHEN e.status = 'completed' THEN e.duration_seconds END), 2) as avg_duration_seconds,
    ROUND(AVG(CASE WHEN e.status = 'completed' THEN e.quality_score END), 2) as avg_quality_score,
    SUM(CASE WHEN e.status = 'completed' THEN e.records_processed ELSE 0 END) as total_records_processed,
    COUNT(DISTINCT CASE WHEN v.passed = false AND v.severity = 'critical' THEN v.id END) as critical_issues
FROM pipeline.pipelines p
LEFT JOIN pipeline.executions e ON p.id = e.pipeline_id 
    AND DATE(e.start_time) = CURRENT_DATE
LEFT JOIN pipeline.validation_results v ON e.id = v.execution_id;
```

#### 11. Top 5 Pipelines por Volumen de Datos

```sql
SELECT 
    p.name,
    p.owner,
    COUNT(e.id) as total_runs,
    SUM(e.records_processed) as total_records_processed,
    ROUND(AVG(e.records_processed), 0) as avg_records_per_run,
    ROUND(AVG(e.duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(e.quality_score), 2) as avg_quality_score
FROM pipeline.pipelines p
JOIN pipeline.executions e ON p.id = e.pipeline_id
WHERE e.status = 'completed'
  AND e.start_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.name, p.owner
ORDER BY total_records_processed DESC
LIMIT 5;
```

---

## Solución de Problemas

### Problema: No puedo conectarme a la base de datos

**Síntomas:**
```
[ERROR] No se pudo conectar a la base de datos
Host: localhost:5432
Database: data_framework
```

**Solución:**
```bash
# 1. Verificar que PostgreSQL está corriendo
docker ps | findstr postgres

# 2. Si no está corriendo, iniciarlo (ver README.md para instalación)
python scripts/db_utils.py status

# 3. Verificar logs si hay errores
docker logs framework_postgres
```

**Nota:** Para configuración inicial de PostgreSQL, ver [README.md - Instalación](../README.md#instalación-rápida-automatizada)

---

### Problema: Tablas vacías después de ejecutar pipeline

**Síntomas:**
```
[EMPTY]  executions                                   0 rows
[EMPTY]  validation_results                           0 rows
```

**Causas posibles:**
1. Pipeline nunca se ejecutó
2. Error durante ejecución
3. Datos se limpiaron

**Solución:**
```bash
# 1. Ejecutar pipeline
data-framework run pipeline -c examples/pipelines/data_pipeline.yml

# 2. Verificar estado nuevamente
python scripts/db_utils.py status

# 3. Ver logs del contenedor
docker logs framework_postgres
```

---

### Problema: Base de datos crece demasiado

**Síntomas:**
- Disco lleno
- Queries lentas
- Advertencias de espacio

**Solución:**
```bash
# 1. Ver tamaños actuales
python scripts/db_utils.py status

# 2. Limpiar ejecuciones viejas (>60 días)
python scripts/db_utils.py clean-old -d 60

# 3. Optimizar base de datos
python scripts/db_utils.py vacuum

# 4. Verificar mejora
python scripts/db_utils.py status
```

---

### Problema: Queries muy lentas

**Síntomas:**
- db_utils.py tarda mucho
- Reportes lentos
- Timeout en queries

**Diagnóstico:**
```sql
-- Ver tablas más grandes
SELECT 
    schemaname,
    tablename,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    n_live_tup as rows
FROM pg_stat_user_tables
ORDER BY size_bytes DESC;

-- Ver índices no utilizados
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_relation_size(indexrelid) DESC;
```

**Solución:**
```bash
# 1. Limpiar datos antiguos
python scripts/db_utils.py clean-old -d 30

# 2. Ejecutar VACUUM
python scripts/db_utils.py vacuum

# 3. Reiniciar PostgreSQL
docker restart framework_postgres

# 4. Verificar mejora
python scripts/db_utils.py stats
```

---

### Problema: Error "relation does not exist"

**Síntomas:**
```
psycopg2.errors.UndefinedTable: relation "pipeline.executions" does not exist
```

**Causa:**
- Base de datos no inicializada
- Schema no creado

**Solución:**
```bash
# 1. Reinicializar base de datos
docker exec -i framework_postgres psql -U admin -d data_framework < scripts/init_db.sql

# 2. Verificar que se creó
python scripts/db_utils.py status
```

**Nota:** Para reinstalación completa de PostgreSQL, ver [README.md - Instalación](../README.md#instalación-rápida-automatizada) o usar `setup.ps1`

---

## Mejores Prácticas

### 1. Mantenimiento Regular

```bash
# Cada semana:
python scripts/db_utils.py stats  # Revisar métricas

# Cada mes:
python scripts/db_utils.py clean-old -d 60  # Limpiar >60 días
python scripts/db_utils.py vacuum            # Optimizar

# Cada trimestre:
python scripts/db_utils.py clean-old -d 90  # Retención más agresiva
```

### 2. Monitoreo

```bash
# Verificar health diariamente
python scripts/db_utils.py status

# Revisar ejecuciones recientes
python scripts/db_utils.py executions -l 20

# Alertas de validaciones fallidas
python scripts/db_utils.py validations -s failed -l 50
```

### 3. Backups

```bash
# Backup completo (ejecutar diariamente)
docker exec framework_postgres pg_dump -U admin data_framework > backup_$(date +%Y%m%d).sql

# Backup solo schema (sin datos)
docker exec framework_postgres pg_dump -U admin -s data_framework > schema_backup.sql

# Restaurar backup
docker exec -i framework_postgres psql -U admin -d data_framework < backup_20240204.sql
```

### 4. Seguridad

```bash
# Cambiar password de PostgreSQL
docker exec -it framework_postgres psql -U admin -c "ALTER USER admin WITH PASSWORD 'new_secure_password';"

# Actualizar .env
POSTGRES_PASSWORD=new_secure_password

# Reiniciar servicios
docker-compose restart postgres
```

---

## Comandos Rápidos (Cheat Sheet)

```bash
# Estado general
python scripts/db_utils.py status

# Estadísticas
python scripts/db_utils.py stats

# Últimas 20 ejecuciones
python scripts/db_utils.py executions -l 20

# Validaciones fallidas
python scripts/db_utils.py validations -s failed

# Pipelines registrados
python scripts/db_utils.py pipelines

# Limpiar datos de ejemplo
python scripts/db_utils.py clean-samples

# Limpiar ejecuciones viejas (>60 días)
python scripts/db_utils.py clean-old -d 60

# Optimizar BD
python scripts/db_utils.py vacuum

# Help completo
python scripts/db_utils.py --help
```

---

**Última actualización:** Febrero 2026  
**Versión:** 2.0.0
