# Referencia CLI

Interfaz de línea de comandos para el Data Pipeline Framework.

## Instalación

Comando global disponible después de ejecutar `setup.ps1`:

```bash
data-framework <command> [options]
```

## Comandos

### run pipeline

Ejecuta un data pipeline desde configuración YAML.

```bash
data-framework run pipeline -c <config_file>
```

**Opciones:**
- `-c, --config` **(requerido)**: Ruta al archivo de configuración YAML del pipeline

**Ejemplo:**
```bash
data-framework run pipeline -c examples/pipelines/data_pipeline.yml
```

**Salida:**
```
Starting pipeline: CustomerDataPipeline
================================================================================
Stage: INGESTION
  ✓ Loaded 10,000 records from CSV

Stage: VALIDATION
  ✓ Executed 45 validations
  ✓ Quality Score: 91.11%
  ⚠ 4 validation failures detected

Stage: TRANSFORMATION
  ✓ Filtered 500 records
  ✓ Created 2 derived columns

Stage: OUTPUT
  ✓ Exported to CSV: data/output/customers.csv
  ✓ Exported to PostgreSQL: sample_data.customers

================================================================================
Execution completed
  Status: completed
  Duration: 8.61s
  Health Status: warning
  Quality Score: 91.11%
  Records Processed: 9,500
  Records Failed: 500
  Report: reports/execution_a1b2c3d4_20260210_153045.html
  Executive Report: reports/execution_a1b2c3d4_20260210_153045_summary.html
================================================================================
```

**Códigos de Salida:**
- `0`: Éxito
- `1`: Ejecución de pipeline falló
- `2`: Archivo de configuración no encontrado
- `3`: Sintaxis YAML inválida

**Relacionado:**
- [ARCHITECTURE.md](ARCHITECTURE.md#pipeline-executor) - Flujo de ejecución del pipeline
- [examples/pipelines/data_pipeline.yml](../examples/pipelines/data_pipeline.yml) - Ejemplo de configuración

---

### export-logs

Exporta logs de auditoría del pipeline y métricas a archivos CSV.

```bash
data-framework export-logs -n <pipeline_name> [-o <output_dir>]
```

**Opciones:**
- `-n, --name` **(requerido)**: Nombre del pipeline a exportar
- `-o, --output` (opcional): Directorio de salida (predeterminado: `logs/`)

**Ejemplo:**
```bash
data-framework export-logs -n CustomerDataPipeline -o analysis/
```

**Salida:**
```
Exporting logs for pipeline: CustomerDataPipeline
Output directory: analysis/

Exporting data...
✓ executions_summary: 15 rows, 12,450 bytes
✓ stages_performance: 60 rows, 18,320 bytes
✓ validation_quality: 180 rows, 45,600 bytes
✓ validation_failures: 234 rows, 156,780 bytes
✓ timeline: 15 rows, 3,240 bytes
✓ errors_analysis: 12 rows, 2,890 bytes

All files saved in: C:\...\analysis

Files generated:
  - CustomerDataPipeline_executions_20260210_153045.csv
  - CustomerDataPipeline_stages_20260210_153045.csv
  - CustomerDataPipeline_validation_quality_20260210_153045.csv
  - CustomerDataPipeline_validation_failures_20260210_153045.csv
  - CustomerDataPipeline_timeline_20260210_153045.csv
  - CustomerDataPipeline_errors_20260210_153045.csv

Next steps:
  1. Open CSV files with Excel, Google Sheets, or pandas
  2. Analyze validation failures to identify quality issues
  3. Review stages performance to optimize slow stages
  4. Check timeline to understand execution flow
```

**Archivos Generados:**

1. **executions_summary.csv** - Historial de ejecuciones con métricas de calidad
2. **stages_performance.csv** - Datos de rendimiento a nivel de stage
3. **validation_quality.csv** - Quality scores por suite y dataset
4. **validation_failures.csv** - Fallos de validación detallados
5. **timeline.csv** - Timeline de ejecución
6. **errors_analysis.csv** - Patrones de errores y análisis

**Códigos de Salida:**
- `0`: Exportación exitosa
- `1`: Pipeline no encontrado
- `2`: Error de conexión a base de datos
- `3`: Directorio de salida no escribible

**Casos de Uso:**
- Análisis histórico de ejecuciones de pipeline
- Análisis de tendencias de calidad
- Optimización de rendimiento
- Reportes de cumplimiento
- Debugging de fallos de validación

**Relacionado:**
- [EXPORT_LOGS.md](EXPORT_LOGS.md) - Documentación detallada de formato de archivos
- [AUDIT_SYSTEM.md](AUDIT_SYSTEM.md) - Schema de base de datos de auditoría

---

### infect

Inyecta vulnerabilidades en datasets limpios para testing de seguridad.

```bash
data-framework infect -c <config_file>
```

**Opciones:**
- `-c, --config` **(requerido)**: Ruta al archivo YAML de configuración de infección

**Ejemplo:**
```bash
data-framework infect -c examples/infection_config.yml
```

**Salida:**
```
Loading clean data: data/samples/customers.csv
Loaded 10,000 records

Injecting vulnerabilities...
  ✓ SQL Injection: 100 records (1.0%)
  ✓ XSS Attacks: 100 records (1.0%)
  ✓ Command Injection: 100 records (1.0%)
  ✓ NoSQL Injection: 100 records (1.0%)
  ✓ Path Traversal: 100 records (1.0%)
  ✓ Data Leakage (SSN): 100 records (1.0%)
  ✓ Outliers: 100 records (1.0%)
  ✓ Missing Data: 100 records (1.0%)
  ✓ Duplicates: 50 records (0.5%)
  ✓ Format Corruption: 100 records (1.0%)

Total infections: 850 records (8.5%)

Output saved: data/output/customers_infected.csv
Infection report: data/output/infection_report.json
```

**Formato de Configuración:**

```yaml
infection:
  input_file: "data/samples/customers.csv"
  output_file: "data/output/customers_infected.csv"
  
  attacks:
    - type: "sql_injection"
      rate: 0.01           # 1% de registros
      columns: ["name", "address"]
      severity: "critical"
    
    - type: "xss"
      rate: 0.01
      columns: ["name", "email"]
      severity: "critical"
    
    - type: "data_leakage"
      rate: 0.01
      columns: ["notes"]
      patterns: ["ssn", "credit_card"]
      severity: "critical"
```

**Tipos de Ataque Soportados:**
- `sql_injection` - Patrones de SQL injection
- `xss` - Payloads de cross-site scripting
- `command_injection` - Inyección de comandos shell
- `nosql_injection` - Manipulación de queries NoSQL
- `path_traversal` - Ataques de directory traversal
- `data_leakage` - Exposición de PII (SSN, tarjetas de crédito, API keys)
- `outliers` - Anomalías estadísticas
- `missing_data` - Valores null estratégicos
- `duplicates` - Registros duplicados exactos
- `format_corruption` - Formatos de datos inválidos

**Códigos de Salida:**
- `0`: Infección exitosa
- `1`: Archivo de entrada no encontrado
- `2`: Configuración inválida
- `3`: Error de escritura

**Flujo de Trabajo:**
```bash
# 1. Generar datos limpios
python scripts/generate_sample_data.py

# 2. Infectar con vulnerabilidades
data-framework infect -c examples/infection_config.yml

# 3. Ejecutar pipeline de validación
data-framework run pipeline -c examples/pipelines/data_pipeline.yml

# 4. Revisar hallazgos de seguridad en reporte
start reports\execution_*.html
```

**Relacionado:**
- [examples/infection_config.yml](../examples/infection_config.yml) - Ejemplo de configuración
- [VALIDATION_SYSTEM.md](VALIDATION_SYSTEM.md#security-owasp) - Validaciones de seguridad

---

## Opciones Globales

Disponibles para todos los comandos:

```bash
data-framework <command> --help      # Mostrar ayuda específica del comando
data-framework --version             # Mostrar versión del framework
```

## Archivos de Configuración

### Configuración de Pipeline

**Ubicación:** `examples/pipelines/data_pipeline.yml`

**Estructura:**
```yaml
pipeline:
  name: string
  description: string

ingestion:
  sources: [...]

validation:
  great_expectations: [...]

transformation:
  steps: [...]

outputs: [...]
```

Ver [examples/pipelines/data_pipeline.yml](../examples/pipelines/data_pipeline.yml) para ejemplo completo.

### Configuración de Infección

**Ubicación:** `examples/infection_config.yml`

**Estructura:**
```yaml
infection:
  input_file: string
  output_file: string
  attacks: [...]
```

Ver [examples/infection_config.yml](../examples/infection_config.yml) para ejemplo completo.

## Variables de Entorno

Variables de entorno opcionales para configuración:

```bash
# Conexión PostgreSQL
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=data_framework
export DB_USER=admin
export DB_PASSWORD=admin123

# Rutas de salida
export REPORTS_DIR=reports
export LOGS_DIR=logs
export DATA_DIR=data
```

Los valores predeterminados están definidos en `src/config.py`.

## Referencia de Códigos de Salida

| Código | Significado |
|------|-------------|
| 0 | Éxito |
| 1 | Error general |
| 2 | Archivo no encontrado |
| 3 | Configuración inválida |
| 4 | Error de conexión a base de datos |
| 5 | Error de validación |

## Solución de Problemas

### Comando No Encontrado

**Error:** `data-framework : The term 'data-framework' is not recognized`

**Solución:**
```powershell
# Recargar perfil de PowerShell
. $PROFILE

# Verificar que la función existe
Get-Command data-framework
```

### Error de Conexión a Base de Datos

**Error:** `psycopg2.OperationalError: could not connect to server`

**Solución:**
```bash
# Verificar contenedor PostgreSQL
docker ps | findstr postgres

# Iniciar contenedor si no está corriendo
docker-compose up -d postgres

# Verificar conexión
docker exec framework_postgres psql -U admin -d data_framework -c "SELECT 1;"
```

### Configuración de Pipeline Inválida

**Error:** `yaml.scanner.ScannerError: mapping values are not allowed here`

**Solución:**
- Validar sintaxis YAML con validador en línea
- Verificar indentación (usar espacios, no tabs)
- Asegurar citado apropiado de caracteres especiales

### Exportación Sin Datos

**Salida:** `✓ executions_summary: 0 rows`

**Solución:**
```bash
# Ejecutar pipeline primero para generar datos
data-framework run pipeline -c examples/pipelines/data_pipeline.yml

# Luego exportar
data-framework export-logs -n CustomerDataPipeline
```

## Uso Avanzado

### Procesamiento por Lotes

```bash
# Procesar múltiples pipelines
for config in examples/*.yml; do
    data-framework run pipeline -c "$config"
done
```

### Reportes Automatizados

```bash
# Exportación diaria de todos los pipelines
$pipelines = @("Pipeline1", "Pipeline2", "Pipeline3")
$date = Get-Date -Format "yyyyMMdd"

foreach ($pipeline in $pipelines) {
    data-framework export-logs -n $pipeline -o "reports\daily\$date"
}
```

### Encadenamiento de Pipelines

```bash
# Ejecutar múltiples stages
data-framework run pipeline -c stage1.yml
data-framework run pipeline -c stage2.yml
data-framework run pipeline -c stage3.yml

# Exportar resultados combinados
data-framework export-logs -n CombinedPipeline
```

## Método Tradicional

Si no se usa el comando global, activar entorno virtual:

```powershell
# Windows
.venv\Scripts\Activate.ps1

# Ejecutar comando
data-framework run pipeline -c examples/pipelines/data_pipeline.yml
```

```bash
# Linux/Mac
source .venv/bin/activate

# Ejecutar comando
data-framework run pipeline -c examples/pipelines/data_pipeline.yml
```
