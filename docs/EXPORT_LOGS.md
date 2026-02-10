# Exportación de Logs

Exporta datos de auditoría de pipeline a archivos CSV para análisis, reportes y monitoreo.

## Comando

```bash
data-framework export-logs -n <pipeline_name> [-o <output_dir>]
```

**Opciones:**
- `-n, --name` (requerido): Nombre del pipeline a exportar
- `-o, --output` (opcional): Directorio de salida (predeterminado: `logs/`)

**Ejemplo:**
```bash
data-framework export-logs -n CustomerTransactionPipeline -o analysis/
```

## Archivos Generados

Se generan seis archivos CSV por pipeline:

1. **executions_summary.csv** - Historial de ejecución y métricas de calidad
2. **stages_performance.csv** - Datos de rendimiento a nivel de stage
3. **validation_quality.csv** - Quality scores por suite y dataset
4. **validation_failures.csv** - Registros detallados de fallos de validación
5. **timeline.csv** - Timeline cronológico de ejecución
6. **errors_analysis.csv** - Agregación de errores y advertencias

Los archivos se nombran: `{PipelineName}_{type}_{timestamp}.csv`

## Especificaciones de Archivos

### 1. executions_summary.csv

Historial completo de ejecución con métricas de calidad.

**Columnas (17 en total):**
- `execution_id` - Identificador UUID
- `pipeline_name` - Nombre del pipeline
- `status` - completed, failed, cancelled
- `start_time`, `end_time` - Timestamps de ejecución
- `duration_seconds` - Tiempo total de ejecución
- `records_processed`, `records_failed` - Contadores de registros
- `quality_score` - Porcentaje de calidad (0-100)
- `health_status` - healthy, warning, critical, failed
- `total_errors`, `total_warnings` - Contadores de problemas
- `execution_type` - manual, scheduled, triggered, api
- `triggered_by` - Identificador de usuario o sistema
- `environment` - development, staging, production
- `report_path`, `executive_report_path` - Rutas a reportes HTML

**Casos de Uso:**
- Análisis de historial de ejecuciones
- Monitoreo de tendencias de calidad
- Seguimiento de frecuencia de fallos
- Comparación entre entornos

**Ejemplo de Query:**
```sql
SELECT 
    DATE_TRUNC('day', start_time) as day,
    AVG(quality_score) as avg_quality,
    COUNT(*) as executions,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures
FROM executions_summary
GROUP BY DATE_TRUNC('day', start_time)
ORDER BY day DESC;
```

### 2. stages_performance.csv

Métricas de rendimiento a nivel de stage para identificación de cuellos de botella.

**Columnas (18 en total):**
- `execution_id`, `execution_date` - Referencia de ejecución
- `pipeline_name` - Identificador del pipeline
- `stage_name` - INGESTION, VALIDATION, TRANSFORMATION, OUTPUT
- `stage_order` - Secuencia de ejecución (1-4)
- `status` - running, completed, failed, skipped
- `duration_seconds` - Tiempo de ejecución del stage
- `records_in`, `records_out`, `records_failed` - Flujo de datos
- `success_rate_pct` - Porcentaje de registros exitosos
- `memory_usage_mb`, `cpu_usage_percent` - Utilización de recursos
- `error_count`, `warning_count` - Contadores de problemas
- `quality_score`, `validations_passed`, `validations_failed` - Métricas del stage VALIDATION

**Casos de Uso:**
- Identificación de cuellos de botella
- Análisis de throughput
- Detección de fallos de stage
- Optimización de rendimiento

**Análisis:**
```python
import pandas as pd

stages = pd.read_csv('stages_performance.csv')

# Identificar stage más lento
avg_duration = stages.groupby('stage_name')['duration_seconds'].mean()
print("Duración promedio por stage:")
print(avg_duration.sort_values(ascending=False))

# Calcular throughput
stages['throughput'] = stages['records_out'] / stages['duration_seconds']
print("\nThroughput (registros/seg) por stage:")
print(stages.groupby('stage_name')['throughput'].mean())
```

### 3. validation_quality.csv

Métricas de calidad agregadas por suite de validación y dataset.

**Columnas (13 en total):**
- `execution_id`, `execution_date` - Referencia de ejecución
- `overall_quality_score` - Quality score a nivel de pipeline
- `suite_name` - Identificador de suite de validación
- `dataset_name` - Dataset objetivo
- `total_validations`, `passed_validations`, `failed_validations` - Contadores de validación
- `suite_quality_score` - Quality score específico de suite
- `total_records` - Registros validados
- `total_errors`, `total_warnings` - Contadores de problemas
- `execution_time_ms` - Tiempo de ejecución de suite
- `failure_rate_pct` - Porcentaje de fallo

**Casos de Uso:**
- Análisis de efectividad de suite
- Comparación de calidad de dataset
- Detección de tendencias de calidad
- Priorización de correcciones

**Umbrales de Severidad:**
- `failure_rate_pct > 50%` - Crítico: acción inmediata requerida
- `suite_quality_score < 70%` - Advertencia: necesita atención
- `suite_quality_score ≥ 95%` - Saludable: solo monitorear

### 4. validation_failures.csv

Registros detallados de fallos para análisis de causa raíz.

**Columnas (14 en total):**
- `execution_id`, `execution_date` - Referencia de ejecución
- `suite_name`, `dataset_name` - Contexto de validación
- `rule_name` - Identificador de regla de validación
- `rule_type` - Categoría de validación
- `expectation_type` - Expectation de Great Expectations
- `severity` - critical, error, warning, info
- `total_records`, `failed_count` - Estadísticas de fallo
- `failure_rate_pct` - Porcentaje de registros fallidos
- `failure_details` - JSON con información específica de fallo

**Casos de Uso:**
- Análisis de causa raíz
- Detección de patrones de fallo
- Planificación de remediación
- Priorización basada en severidad

**Flujo de Análisis:**
1. Filtrar por `severity = 'critical'` para atención inmediata
2. Ordenar por `failure_rate_pct DESC` para priorizar problemas de alto impacto
3. Parsear JSON de `failure_details` para contexto específico de fallo
4. Agrupar por `rule_name` para detectar reglas consistentemente fallidas

**Registro de Ejemplo:**
```csv
suite_name,rule_name,severity,failed_count,failure_rate_pct
04_Seguridad_OWASP,expect_no_sql_injection,critical,234,11.7
```

### 5. timeline.csv

Timeline cronológico de ejecución para visualización de tendencias.

**Columnas (11 en total):**
- `execution_id` - Identificador UUID
- `start_time`, `end_time` - Ventana de ejecución
- `duration_seconds` - Tiempo de ejecución
- `status` - Estado de ejecución
- `execution_type`, `triggered_by`, `environment` - Contexto
- `quality_score`, `health_status` - Métricas de calidad
- `total_errors`, `total_warnings` - Contadores de problemas

**Casos de Uso:**
- Visualización de tendencias históricas
- Detección de patrones temporales
- Análisis de frecuencia de ejecución
- Monitoreo de degradación de calidad

**Visualización:**
```python
import matplotlib.pyplot as plt
import pandas as pd

timeline = pd.read_csv('timeline.csv')
timeline['start_time'] = pd.to_datetime(timeline['start_time'])

plt.figure(figsize=(12, 6))
plt.plot(timeline['start_time'], timeline['quality_score'])
plt.axhline(y=95, color='g', linestyle='--', label='Umbral saludable')
plt.axhline(y=80, color='y', linestyle='--', label='Umbral advertencia')
plt.xlabel('Tiempo de Ejecución')
plt.ylabel('Quality Score (%)')
plt.title('Tendencia de Quality Score')
plt.legend()
plt.show()
```

### 6. errors_analysis.csv

Agregación de errores y advertencias por ejecución y stage.

**Columnas (10 en total):**
- `execution_id`, `execution_date` - Referencia de ejecución
- `status` - Estado de ejecución
- `total_errors`, `total_warnings` - Contadores a nivel de pipeline
- `execution_error` - Mensaje de error general
- `stage_name` - Stage que generó errores
- `stage_errors`, `stage_warnings` - Contadores a nivel de stage
- `stage_error_details` - JSON con información de error específica del stage

**Nota:** Contiene solo ejecuciones con errores o advertencias. Archivo vacío indica que no hay problemas.

**Casos de Uso:**
- Análisis de distribución de errores
- Patrones de fallo de stage
- Seguimiento de evolución de errores
- Troubleshooting de fallos

## Ejemplos de Uso

### Exportación Básica

```bash
data-framework export-logs -n CustomerTransactionPipeline
```

Salida:
```
Exporting logs for pipeline: CustomerTransactionPipeline
Output directory: logs

✓ executions_summary: 15 rows
✓ stages_performance: 60 rows
✓ validation_quality: 180 rows
✓ validation_failures: 234 rows
✓ timeline: 15 rows
✓ errors_analysis: 3 rows

All files saved in: /path/to/logs
```

### Directorio de Salida Personalizado

```bash
data-framework export-logs -n MyPipeline -o exports/2024-02
```

### Listar Pipelines Disponibles

```bash
data-framework export-logs -n InvalidName
```

Salida:
```
❌ Pipeline 'InvalidName' not found

Available pipelines:
  - CustomerTransactionPipeline
  - DataQualityPipeline
  - ETLPipeline
```

## Flujos de Análisis

### 1. Identificar Cuellos de Botella

**Objetivo:** Encontrar stages que consumen tiempo excesivo.

**Pasos:**
1. Abrir `stages_performance.csv`
2. Ordenar por `duration_seconds DESC`
3. Calcular porcentaje: `(stage_duration / sum(all_stages)) × 100`
4. Stages >40% del tiempo total son cuellos de botella

**Ejemplo:**
```python
stages = pd.read_csv('stages_performance.csv')
total_time = stages['duration_seconds'].sum()
stages['pct_of_total'] = (stages['duration_seconds'] / total_time) * 100
print(stages[stages['pct_of_total'] > 40][['stage_name', 'duration_seconds', 'pct_of_total']])
```

### 2. Detección de Degradación de Calidad

**Objetivo:** Identificar tendencias decrecientes de calidad.

**Pasos:**
1. Abrir `executions_summary.csv`
2. Graficar `quality_score` vs `start_time`
3. Calcular promedio móvil (ventana=7)
4. Alertar si la pendiente de la tendencia < -0.5% por día

**Ejemplo:**
```python
execs = pd.read_csv('executions_summary.csv')
execs['start_time'] = pd.to_datetime(execs['start_time'])
execs = execs.sort_values('start_time')

# Promedio móvil de 7 días
execs['quality_ma'] = execs['quality_score'].rolling(7).mean()

# Detectar degradación
recent = execs.tail(14)
if recent['quality_ma'].iloc[-1] < recent['quality_ma'].iloc[0] - 5:
    print("⚠️ Degradación de calidad detectada!")
```

### 3. Análisis de Causa Raíz

**Objetivo:** Entender por qué fallan las validaciones.

**Pasos:**
1. Abrir `validation_failures.csv`
2. Filtrar por `severity = 'critical'`
3. Ordenar por `failure_rate_pct DESC`
4. Parsear JSON de `failure_details`
5. Correlacionar con cambios de datos

**Ejemplo:**
```python
failures = pd.read_csv('validation_failures.csv')
critical = failures[failures['severity'] == 'critical']
top_failures = critical.nlargest(10, 'failure_rate_pct')

print("Top 10 Fallos Críticos:")
print(top_failures[['rule_name', 'dataset_name', 'failed_count', 'failure_rate_pct']])

# Parsear detalles de fallo
import json
for idx, row in top_failures.iterrows():
    details = json.loads(row['failure_details'])
    print(f"\n{row['rule_name']}: {details}")
```

### 4. Optimización de Rendimiento

**Objetivo:** Calcular y mejorar throughput.

**Pasos:**
1. Abrir `stages_performance.csv`
2. Calcular: `throughput = records_out / duration_seconds`
3. Identificar stages con throughput < umbral
4. Perfilar y optimizar código lento

**Ejemplo:**
```python
stages = pd.read_csv('stages_performance.csv')
stages['throughput'] = stages['records_out'] / stages['duration_seconds']

# Stages procesando <1000 registros/seg
slow = stages[stages['throughput'] < 1000]
print("Stages lentos (< 1k registros/seg):")
print(slow[['stage_name', 'throughput', 'duration_seconds']])
```

## Formato de Archivo

**Codificación:** UTF-8  
**Delimitador:** `,` (coma)  
**Encabezado:** Primera fila contiene nombres de columnas  
**Valores NULL:** Cadena vacía `""`  
**Columnas JSON:** Escapadas y formateadas como string  
**Fin de línea:** CRLF (`\r\n`) en Windows, LF (`\n`) en Unix

## Herramientas

**Hojas de Cálculo:**
- Microsoft Excel
- Google Sheets
- LibreOffice Calc

**Programación:**
- Python: pandas, matplotlib
- R: readr, ggplot2
- SQL: Importar a PostgreSQL/SQLite

**Plataformas BI:**
- Power BI
- Tableau
- Looker

## Automatización

### Exportaciones Diarias

**PowerShell:**
```powershell
$pipelines = @("Pipeline1", "Pipeline2", "Pipeline3")
$date = Get-Date -Format "yyyyMMdd"
$outputDir = "exports\daily\$date"

New-Item -ItemType Directory -Path $outputDir -Force

foreach ($pipeline in $pipelines) {
    data-framework export-logs -n $pipeline -o $outputDir
}
```

**Bash:**
```bash
#!/bin/bash
pipelines=("Pipeline1" "Pipeline2" "Pipeline3")
date=$(date +%Y%m%d)
output_dir="exports/daily/$date"

mkdir -p "$output_dir"

for pipeline in "${pipelines[@]}"; do
    data-framework export-logs -n "$pipeline" -o "$output_dir"
done
```

### Tarea Programada (Windows)

```powershell
$action = New-ScheduledTaskAction -Execute "data-framework" -Argument "export-logs -n MyPipeline"
$trigger = New-ScheduledTaskTrigger -Daily -At "02:00AM"
Register-ScheduledTask -TaskName "PipelineExport" -Action $action -Trigger $trigger
```

### Cron Job (Linux)

```bash
# Exportar diariamente a las 2 AM
0 2 * * * /path/to/data-framework export-logs -n MyPipeline -o /exports/$(date +\%Y\%m\%d)
```

## Solución de Problemas

### Pipeline No Encontrado

**Síntoma:** `Pipeline 'MyPipeline' not found`

**Solución:**
1. Verificar que el nombre del pipeline sea exacto (case-sensitive)
2. Ejecutar sin flag `-n` para ver pipelines disponibles
3. Asegurar que el pipeline se ha ejecutado al menos una vez

### No Hay Datos Exportados

**Síntoma:** Todos los archivos CSV tienen 0 filas

**Causa:** El pipeline aún no se ha ejecutado.

**Solución:**
```bash
# Ejecutar pipeline primero
data-framework run pipeline -c examples/my_pipeline.yml

# Luego exportar
data-framework export-logs -n MyPipeline
```

### errors_analysis.csv Vacío

**Esto es normal.** El archivo contiene solo ejecuciones con errores. Vacío indica que todas las ejecuciones tuvieron éxito.

### Error de Conexión a Base de Datos

**Síntoma:** `psycopg2.OperationalError`

**Solución:**
```bash
# Verificar contenedor PostgreSQL
docker ps | grep postgres

# Iniciar si no está corriendo
docker-compose up -d postgres
```

## Detalles Técnicos

**Fuente de Datos:** Schema `pipeline` de PostgreSQL  
**Tablas Consultadas:**
- executions
- stage_executions
- validation_summary
- validation_results
- pipelines

**Rendimiento de Queries:**
- Columnas indexadas usadas para filtrado
- Ejecución de query: <1s para 1000 ejecuciones
- Velocidad de exportación: ~50k filas/segundo

**Timestamp:** El timestamp del archivo refleja el tiempo de exportación, no tiempos de ejecución. Usar columnas `start_time` o `execution_date` para análisis temporal.

**Datos Históricos:** Exporta todas las ejecuciones históricas del pipeline. Sin límite de filas.

## Documentación Relacionada

- [AUDIT_SYSTEM.md](AUDIT_SYSTEM.md) - Schema y tablas de base de datos de auditoría
- [CLI_REFERENCE.md](CLI_REFERENCE.md) - Referencia de comandos CLI
- [ARCHITECTURE.md](ARCHITECTURE.md) - Visión general de arquitectura del sistema
