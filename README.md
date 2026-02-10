# Data Pipeline Framework

Framework production-ready para construir pipelines de datos con validación integral, escaneo de seguridad, seguimiento de auditoría y monitoreo de calidad.

## Características

**Procesamiento de Datos:**
- Ingesta multi-origen (CSV, JSON, PostgreSQL, datos sintéticos)
- Transformación y enriquecimiento de datos
- Salida multi-formato (CSV, Excel, Parquet, PostgreSQL)

**Calidad y Seguridad:**
- 40+ tipos de validación semántica
- Detección de vulnerabilidades OWASP Top 10+
- Verificaciones de cumplimiento (GDPR, PCI-DSS, HIPAA, SOC2)
- Detección de outliers estadísticos
- Validación cross-field

**Auditoría y Monitoreo:**
- Sistema de auditoría PostgreSQL con 6 tablas
- Seguimiento de rendimiento a nivel de stage
- Cálculo de quality score
- Monitoreo de health status
- Exportación CSV para análisis histórico

**Reportes:**
- Reportes HTML profesionales
- Resúmenes ejecutivos
- Resultados de validación detallados
- Vulnerabilidades de seguridad
- Métricas de rendimiento

## Inicio Rápido

### Instalación

```powershell
# Windows: Ejecutar setup automatizado
.\setup.ps1
```

**El setup realiza:**
- Verifica dependencias (Python 3.10+, Docker)
- Instala paquetes Python
- Inicia contenedor PostgreSQL
- Inicializa schema de base de datos
- Genera datos de ejemplo
- Configura comando CLI global

**Duración:** 2-3 minutos

### Activar Comando Global

```powershell
# Recargar perfil de PowerShell
. $PROFILE

# Verificar instalación
data-framework --help
```

### Ejecutar Tu Primer Pipeline

```bash
# Ejecutar pipeline de ejemplo
data-framework run pipeline -c examples/complete_pipeline.yml

# Ver resultados
start reports\execution_*.html
```

## Comandos CLI

### run pipeline

Ejecuta data pipeline desde configuración YAML.

```bash
data-framework run pipeline -c <config_file>
```

**Stages del pipeline:**
1. **INGESTION** - Carga datos de múltiples fuentes
2. **VALIDATION** - Verificaciones de calidad y escaneo de seguridad
3. **TRANSFORMATION** - Limpieza y enriquecimiento de datos
4. **OUTPUT** - Exporta a múltiples formatos

### export-logs

Exporta métricas de auditoría a archivos CSV.

```bash
data-framework export-logs -n <pipeline_name> [-o <output_dir>]
```

**Genera 6 archivos CSV:**
- `executions_summary.csv` - Historial de ejecuciones
- `stages_performance.csv` - Métricas de stages
- `validation_quality.csv` - Quality scores por suite
- `validation_failures.csv` - Registros detallados de fallos
- `timeline.csv` - Timeline cronológico de ejecuciones
- `errors_analysis.csv` - Agregación de errores

**Casos de uso:**
- Optimización de rendimiento
- Análisis de tendencias de calidad
- Análisis de causa raíz de fallos
- Reportes de cumplimiento

### infect

Inyecta vulnerabilidades para testing de seguridad.

```bash
data-framework infect -c examples/infection_config.yml
```

**Inyecta 10 tipos de ataques:**
- SQL injection
- Ataques XSS
- Command injection
- NoSQL injection
- Path traversal
- Data leakage (SSN, tarjetas de crédito, API keys)
- Outliers estadísticos
- Datos faltantes
- Registros duplicados
- Corrupción de formato

## Arquitectura

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐     ┌────────────┐
│  Ingestion  │────▶│  Validation  │────▶│ Transformation  │────▶│   Output   │
└─────────────┘     └──────────────┘     └─────────────────┘     └────────────┘
       │                    │                      │                     │
       └────────────────────┴──────────────────────┴─────────────────────┘
                                      │
                            ┌─────────▼─────────┐
                            │  Audit System     │
                            │  (PostgreSQL)     │
                            └───────────────────┘
```

**Componentes principales:**
- **Pipeline Executor** - Orquestación de stages y manejo de errores
- **Ingestion Module** - Conectores multi-origen (CSV, JSON, PostgreSQL)
- **Validation Module** - Verificaciones de calidad, seguridad y cumplimiento
- **Transformation Module** - Filtrado y enriquecimiento de datos
- **Audit System** - Seguimiento PostgreSQL con 6 tablas
- **Reporting Module** - Generación de reportes HTML profesionales

Ver [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) para detalles.

## Sistema de Auditoría

Sistema de auditoría basado en PostgreSQL que rastrea:
- Historial de ejecución de pipelines
- Métricas de rendimiento a nivel de stage
- Resultados de validación y quality scores
- Monitoreo de health status
- Logs de errores y advertencias

**Schema de base de datos: `pipeline`**

**Tablas:**
1. `pipelines` - Registro de pipelines
2. `executions` - Historial de ejecuciones con métricas de calidad
3. `validation_summary` - Métricas de validación agregadas
4. `validation_results` - Fallos de validación detallados
5. `stage_executions` - Seguimiento granular de stages
6. `audit_logs` - Logs de eventos

**Seguimiento de health status:**
- `healthy`: quality_score ≥ 95%, sin errores
- `warning`: quality_score ≥ 80% o advertencias presentes
- `critical`: quality_score < 80% o errores presentes
- `failed`: ejecución de pipeline falló

Ver [docs/AUDIT_SYSTEM.md](docs/AUDIT_SYSTEM.md) para schema y API.

## Sistema de Validación

Validación production-ready que cubre:

**Calidad de Datos:**
- Completitud (required, mostly, null checks)
- Unicidad (unique, compound unique)
- Precisión (formatos, patrones, rangos)
- Consistencia (validación cross-field)
- Outliers estadísticos (Z-score, quantiles)

**Seguridad (OWASP Top 10+):**
- SQL injection, NoSQL injection, LDAP injection
- XSS (cross-site scripting)
- Command injection, YAML injection, template injection
- Path traversal, file inclusion
- XXE (XML external entity), SSRF (server-side request forgery)
- Data leakage (API keys, private keys, JWT tokens, passwords)

**Cumplimiento:**
- GDPR (detección y protección de PII)
- PCI-DSS (seguridad de datos de tarjetas de crédito)
- HIPAA (protección de registros médicos)
- SOC2 (prevención de fuga de información de debug)

**40+ Tipos Semánticos:**
- Identificadores: `uuid`, `id`
- Contacto: `email`, `phone_es`, `phone_us`, `phone`
- PII: `ssn`, `dni`, `passport`
- Financiero: `credit_card`, `iban`, `currency`, `amount`
- Dirección: `postal_code_es`, `postal_code_us`, `country_code`
- Web: `url`, `url_secure`, `ipv4`, `ipv6`, `domain`
- Texto: `name`, `text`, `slug`, `alphanumeric`, `enum`
- Numérico: `integer`, `numeric`, `percentage`, `latitude`, `longitude`
- Fecha: `date`, `datetime`, `timestamp`, `year`
- Booleano: `boolean`

Ver [docs/VALIDATION_SYSTEM.md](docs/VALIDATION_SYSTEM.md) para referencia completa.

## Configuración

Configuración de pipeline usando YAML:

```yaml
pipeline:
  name: "CustomerDataPipeline"
  description: "Procesamiento de datos de clientes con validación"

ingestion:
  sources:
    - name: "customers_csv"
      type: "csv"
      path: "data/samples/customers.csv"
      output_dataset: "raw_customers"

validation:
  great_expectations:
    - dataset: "raw_customers"
      suites:
        - name: "01_Schema_Validation"
          expectations:
            - expectation_type: "expect_table_columns_to_match_set"
              column_set: ["id", "name", "email", "phone"]
        
        - name: "02_Data_Quality"
          expectations:
            - expectation_type: "expect_column_values_to_not_be_null"
              column: "name"
            - expectation_type: "expect_column_values_to_be_unique"
              column: "email"
        
        - name: "03_Security_OWASP"
          expectations:
            - expectation_type: "expect_column_values_to_not_match_regex"
              column: "name"
              regex: "(?i)(\\bOR\\b.*=.*|;.*DROP|<script)"
              severity: "critical"

transformation:
  steps:
    - name: "filter_active"
      dataset: "raw_customers"
      output_dataset: "active_customers"
      operations:
        - type: "filter"
          condition: "status == 'active'"

outputs:
  - name: "export_csv"
    type: "csv"
    dataset: "active_customers"
    path: "data/output/customers_active.csv"
  
  - name: "save_postgres"
    type: "postgres"
    dataset: "active_customers"
    table: "sample_data.customers_processed"
    mode: "replace"
```

Ver [examples/complete_pipeline.yml](examples/complete_pipeline.yml) para ejemplo completo.

## Gestión de Base de Datos

### Verificación de Estado

```bash
# Ver estado de tablas
python scripts/db_utils.py status
```

**Salida:**
```
================================================================================
  DATABASE STATUS - data_framework
================================================================================

  Schema: PIPELINE
  ----------------------------------------------------------------------------
  [OK]     pipelines                                1 rows  |     80 kB
  [OK]     executions                               4 rows  |     80 kB
  [OK]     validation_results                      33 rows  |     80 kB
  [OK]     validation_summary                       4 rows  |     72 kB
  [OK]     stage_executions                        12 rows  |     72 kB
  [EMPTY]  audit_logs                               0 rows  |     72 kB

  Schema: SAMPLE_DATA
  ----------------------------------------------------------------------------
  [OK]     customers                           10,000 rows  |   2144 kB
  [OK]     customers_processed                  8,038 rows  |   1856 kB

================================================================================
  TOTAL: 18,092 registros
================================================================================
```

### Comandos Comunes

```bash
# Ver ejecuciones de pipeline
python scripts/db_utils.py executions

# Ver pipelines registrados
python scripts/db_utils.py pipelines

# Ver estadísticas de base de datos
python scripts/db_utils.py stats

# Limpiar datos de ejemplo
python scripts/db_utils.py clean-samples
```

Ver [docs/DATABASE.md](docs/DATABASE.md) para referencia completa.

## Rendimiento

**Ejecución típica (10K registros):**
- INGESTION: 0.3-0.6s
- VALIDATION: 1.5-12s (80-95% del tiempo total)
- TRANSFORMATION: 0.1-0.5s
- OUTPUT: 0.2-0.5s

**Huella de memoria:** <100MB

**Optimización:**
- Reducir cantidad de suites de validación para ejecución más rápida
- Usar muestreo para datasets grandes
- Habilitar modo batch para escrituras de auditoría
- Indexar columnas de auditoría consultadas frecuentemente

## Estructura del Proyecto

```
data-pipeline-framework/
├── src/
│   ├── cli.py                      # Punto de entrada CLI
│   ├── pipeline_executor.py        # Orquestación del pipeline
│   ├── config.py                   # Configuración
│   └── modules/
│       ├── data_infection/         # Inyección de vulnerabilidades
│       ├── ingestion/              # Conectores de datos
│       │   └── connectors/         # CSV, JSON, PostgreSQL
│       ├── validation/             # Validación de calidad
│       ├── transformation/         # Transformación de datos
│       ├── auditing/               # Sistema de auditoría
│       │   ├── audit_manager.py   # API de auditoría
│       │   └── log_exporter.py    # Exportación CSV
│       ├── monitoring/             # Recolección de métricas
│       └── reporting/              # Reportes HTML
├── docs/
│   ├── ARCHITECTURE.md             # Arquitectura del sistema
│   ├── AUDIT_SYSTEM.md             # Schema de base de datos de auditoría
│   ├── CLI_REFERENCE.md            # Referencia de comandos
│   ├── DATABASE.md                 # Gestión de base de datos
│   ├── EXPORT_LOGS.md              # Guía de exportación de logs
│   └── VALIDATION_SYSTEM.md        # Tipos de validación
├── examples/
│   ├── complete_pipeline.yml       # Ejemplo de pipeline completo
│   └── infection_config.yml        # Configuración de inyección de ataques
├── scripts/
│   ├── init_db.sql                 # Inicialización de base de datos
│   ├── db_utils.py                 # Utilidades de base de datos
│   └── generate_sample_data.py     # Generador de datos de ejemplo
├── data/
│   ├── samples/                    # Datasets de ejemplo
│   └── output/                     # Resultados de procesamiento
├── reports/                        # Reportes HTML
├── logs/                           # Logs CSV exportados
├── docker-compose.yml              # Contenedor PostgreSQL
└── requirements.txt                # Dependencias Python
```

## Flujos de Trabajo

### Flujo Básico (Datos Limpios)

```bash
# 1. Generar datos de ejemplo
python scripts/generate_sample_data.py -c 10000 -t 50000

# 2. Ejecutar pipeline
data-framework run pipeline -c examples/complete_pipeline.yml

# 3. Ver reporte HTML
start reports\execution_*.html
```

**Esperado:** Quality score 100%, 0 vulnerabilidades

### Flujo de Testing de Seguridad

```bash
# 1. Generar datos limpios
python scripts/generate_sample_data.py

# 2. Inyectar vulnerabilidades
data-framework infect -c examples/infection_config.yml

# 3. Actualizar configuración del pipeline para usar datos infectados
# Editar examples/complete_pipeline.yml:
#   path: "data/output/customers_infected.csv"

# 4. Ejecutar pipeline de validación
data-framework run pipeline -c examples/complete_pipeline.yml

# 5. Revisar hallazgos de seguridad
start reports\execution_*.html
```

**Esperado:** Quality score ~23%, 3+ vulnerabilidades detectadas

### Flujo de Análisis de Rendimiento

```bash
# 1. Ejecutar pipeline
data-framework run pipeline -c examples/complete_pipeline.yml

# 2. Exportar métricas
data-framework export-logs -n CustomerDataPipeline -o analysis/

# 3. Analizar cuellos de botella
# Abrir analysis/stages_performance.csv
# Ordenar por duration_seconds DESC para identificar stages más lentos
```

## Instalación Manual

Alternativa al `setup.ps1` automatizado:

### Prerrequisitos

```bash
# Verificar Python 3.10+
python --version

# Verificar Docker
docker --version
docker ps
```

### Instalar Dependencias

```bash
pip install -r requirements.txt
```

### Iniciar PostgreSQL

```bash
# Iniciar contenedor
docker-compose up -d postgres

# Inicializar schema
docker exec -i framework_postgres psql -U admin -d data_framework < scripts/init_db.sql

# Verificar
python scripts/db_utils.py status
```

### Generar Datos de Ejemplo

```bash
python scripts/generate_sample_data.py -c 10000 -t 50000
```

**Salida:**
- `data/samples/customers.csv` (10,000 registros)
- `data/samples/transactions.csv` (50,000 registros)
- PostgreSQL: `sample_data.customers` (10,000 registros)

## Solución de Problemas

### Comando No Encontrado

**Error:** `data-framework : The term 'data-framework' is not recognized`

**Solución:**
```powershell
# Recargar perfil de PowerShell
. $PROFILE

# O usar método tradicional
.venv\Scripts\Activate.ps1
python -m src.cli run pipeline -c examples/pipeline.yml
```

### Error de Conexión a Base de Datos

**Error:** `psycopg2.OperationalError: could not connect to server`

**Solución:**
```bash
# Verificar estado del contenedor
docker ps | findstr postgres

# Iniciar si no está corriendo
docker-compose up -d postgres

# Ver logs
docker logs framework_postgres
```

### Error de Configuración del Pipeline

**Error:** `yaml.scanner.ScannerError: mapping values are not allowed here`

**Solución:**
- Validar sintaxis YAML
- Verificar indentación (usar espacios, no tabs)
- Entrecomillar caracteres especiales
- Referirse a [examples/complete_pipeline.yml](examples/complete_pipeline.yml)

### Sin Datos de Exportación

**Salida:** `✓ executions_summary: 0 rows`

**Solución:**
```bash
# Ejecutar pipeline primero
data-framework run pipeline -c examples/complete_pipeline.yml

# Luego exportar
data-framework export-logs -n CustomerDataPipeline
```

## Documentación

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Arquitectura del sistema y flujo de datos
- **[AUDIT_SYSTEM.md](docs/AUDIT_SYSTEM.md)** - Schema de base de datos y API de auditoría
- **[CLI_REFERENCE.md](docs/CLI_REFERENCE.md)** - Referencia completa de comandos
- **[DATABASE.md](docs/DATABASE.md)** - Gestión de base de datos y queries
- **[EXPORT_LOGS.md](docs/EXPORT_LOGS.md)** - Guía de exportación y análisis de logs
- **[VALIDATION_SYSTEM.md](docs/VALIDATION_SYSTEM.md)** - Tipos de validación y configuración

## Requisitos

- Python 3.10+
- Docker (para PostgreSQL)
- 512MB RAM mínimo
- 1GB espacio en disco
