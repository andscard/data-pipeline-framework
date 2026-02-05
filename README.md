# Data Pipeline Framework v2.0

Framework modular para pipelines de datos con **validación de calidad**, **detección de vulnerabilidades de seguridad** y **reportes HTML profesionales**.

---

## � Instalación Rápida (Automatizada)

**Setup completo en un solo comando:**

```powershell
# Ejecutar script de setup automático (Windows PowerShell)
.\setup.ps1
```

Este script automáticamente:
- ✅ Verifica dependencias (Python, Docker)
- ✅ Instala paquetes de Python
- ✅ Inicia PostgreSQL en Docker
- ✅ Crea la base de datos `data_framework`
- ✅ Inicializa todas las tablas
- ✅ Genera datos de ejemplo (opcional)

**Duración:** ~2-3 minutos

---

## 🗄️ Gestión de Base de Datos

Después de la instalación, usa `db_utils.py` para gestionar la base de datos:

### Comandos Esenciales

```bash
# Ver estado de todas las tablas
python scripts/db_utils.py status

# Ver estadísticas del framework
python scripts/db_utils.py stats

# Ver últimas 10 ejecuciones
python scripts/db_utils.py executions

# Ver pipelines registrados
python scripts/db_utils.py pipelines

# Limpiar datos de ejemplo (mantiene auditoría)
python scripts/db_utils.py clean-samples

# Ver todas las opciones
python scripts/db_utils.py --help
```

**📚 Documentación completa:** [docs/DATABASE.md](docs/DATABASE.md) (gestión avanzada, queries SQL, schema)

---

## 📖 Instalación Manual (Paso a Paso)

Si prefieres instalación manual o estás en Linux/Mac:

### Paso 1: Verificar Dependencias

```bash
# Verificar Python 3.10+
python --version

# Verificar Docker
docker --version
docker ps  # Debe estar corriendo
```

### Paso 2: Instalar Dependencias Python

```bash
pip install -r requirements.txt
```

### Paso 3: Iniciar Base de Datos

```bash
# Iniciar PostgreSQL con Docker
docker-compose up -d postgres

# Esperar 10 segundos a que esté listo
# Windows:
timeout /t 10
# Linux/Mac:
sleep 10

# Inicializar tablas automáticamente
docker exec -i framework_postgres psql -U admin -d data_framework < scripts/init_db.sql
```

**Verificar instalación:**
```bash
# Ver estado de la base de datos
python scripts/db_utils.py status

# Debe mostrar: 6 tablas en schemas pipeline y sample_data (vacías)
```

### Paso 4: Generar Datos de Ejemplo

```bash
# Generar datos sintéticos
python scripts/generate_sample_data.py -c 10000 -t 50000

# -c 10000  : 10,000 clientes
# -t 50000  : 50,000 transacciones
```

**Salida esperada:**
```
✓ data/samples/customers.csv          (10,000 registros)
✓ data/samples/transactions.csv       (50,000 transacciones)
✓ sample_data.customers en PostgreSQL (10,000 registros)
```

---

## 🎯 Ejecución de Pipeline

### Primera Ejecución

```bash
# Ejecutar pipeline de ejemplo
python -m src.cli run pipeline -c examples/complete_pipeline.yml
```

**El pipeline ejecuta 4 etapas:**

1. **INGESTION** → Carga datos desde múltiples fuentes
   - PostgreSQL: `sample_data.customers` (10,000 registros)
   - CSV: `transactions.csv` (50,000 registros)

2. **VALIDATION** → 9 suites de validación con 33 expectativas
   - Suite 01: Estructura básica (3 expectativas)
   - Suite 02: Identificadores únicos (5 expectativas)
   - Suite 03: Formatos personales (6 expectativas)
   - Suite 04: Valores de negocio (4 expectativas)
   - Suite 05: Fechas (2 expectativas)
   - Suite 06: Calidad de datos (4 expectativas)
   - Suite 07: Seguridad - inyección (3 expectativas)
   - Suite 08: Seguridad - datos sensibles (2 expectativas)
   - Suite 09: Reglas de negocio (4 expectativas)

3. **TRANSFORMATION** → Limpia y transforma datos
   - Filtra registros activos
   - Crea segmentación de clientes
   - Normaliza nombres

4. **OUTPUT** → Genera archivos en múltiples formatos
   - CSV: `data/output/active_customers.csv`
   - Excel: `data/output/customers_enriched.xlsx`
   - Parquet: `data/output/customers_enriched.parquet`
   - PostgreSQL: `sample_data.customers_processed`

**Salida esperada:**
```
Execution completed
  Status: completed
  Duration: 8.61s
  Records: 60,000
  Report: reports/execution_XXXXX_YYYYMMDD_HHMMSS.html
```

### Ver Resultados

```bash
# Ver estado de la base de datos
python scripts/db_utils.py status

# Debe mostrar:
# - pipeline.executions: 1 registro (tu ejecución)
# - pipeline.validation_results: 33 registros (una por expectativa)
# - sample_data.customers_processed: ~8,000 registros (filtrados)
```

### Abrir Reporte HTML

- Localización: `reports/execution_XXXXX_YYYYMMDD_HHMMSS.html`
- El reporte incluye:
  - ✅ Resumen ejecutivo con quality score
  - 📊 Tabla de validaciones por suite
  - ❌ Detalles de validaciones fallidas
  - 📈 Métricas de rendimiento por etapa
Status: completed
Duration: ~2.5s
Quality Score: 100%
Vulnerabilities: 0
Report: reports/execution_XXXXX_YYYYMMDD_HHMMSS.html
```

**Si obtienes error "CSV file not found":**
```bash
# Genera los datos primero:
python scripts/generate_samples.py

# (Opcional) Infecta los datos:
python -m src.cli infect -c examples/infection_config.yml

# Luego ejecuta el pipeline:
python -m src.cli run pipeline -c examples/complete_pipeline.yml
```

---

### Paso 5: Ver Reportes HTML

```bash
# 5.1 Abrir el reporte más reciente
start reports\execution_*.html
```

**El reporte incluye:**
- ✅ Executive Summary (Status, Duration, Records, Quality Score)
- 📊 Validation Results (Passed/Failed por suite)
- 🔍 Tabla detallada de fallos (con columnas, patrones y porcentajes)
- 🚨 Vulnerabilidades detectadas con severidad
- ⚙️ Pipeline stages con métricas

---

## 🗄️ Base de Datos PostgreSQL

### Schemas Principales

- **`pipeline`** - Auditoría y métricas
  - `pipelines` - Registro de pipelines
  - `executions` - Historial de ejecuciones
  - `validation_results` - Resultados de validaciones
  - `audit_logs` - Logs detallados

- **`sample_data`** - Datos procesados
  - `customers` - Datos de ejemplo
  - `customers_processed` - Output del pipeline

> **📚 Gestión avanzada:** Para comandos de administración, queries SQL y troubleshooting, ver [docs/DATABASE.md](docs/DATABASE.md)

---

## 🎯 Workflows Comunes

### A. Workflow Básico (Datos Limpios)

Prueba el pipeline con datos sin vulnerabilidades:

```bash
# 1. Generar datos
python scripts/generate_samples.py

# 2. Ejecutar pipeline (usa data/samples/customers.csv por defecto)
python -m src.cli run pipeline -c examples/complete_pipeline.yml

# 3. Ver reporte
start reports\execution_*.html
```

**Resultado esperado:** Quality Score 100%, 0 vulnerabilidades

---

### B. Workflow con Testing de Seguridad (Datos Infectados)

Prueba la detección de vulnerabilidades OWASP:

```bash
# 1. Generar datos limpios
python scripts/generate_samples.py

# 2. Infectar datos con vulnerabilidades
python -m src.cli infect -c examples/infection_config.yml

# 3. Cambiar el pipeline para usar datos infectados
# Editar examples/complete_pipeline.yml línea 20:
#   path: "data/output/customers_infected.csv"

# 4. Ejecutar pipeline
python -m src.cli run pipeline -c examples/complete_pipeline.yml

# 5. Ver reporte con vulnerabilidades detectadas
start reports\execution_*.html
```

**Resultado esperado:** Quality Score ~23%, 3+ vulnerabilidades detectadas

---

### C. Análisis de Resultados en Base de Datos

```bash
# Ver últimas ejecuciones
docker exec -i framework_postgres psql -U admin -d data_framework -c "
  SELECT 
    pipeline_id,
    status,
    start_time,
    duration_seconds,
    records_processed,
    quality_score
  FROM pipeline.executions 
  ORDER BY start_time DESC 
  LIMIT 5;"

# Ver vulnerabilidades detectadas
docker exec -i framework_postgres psql -U admin -d data_framework -c "
  SELECT 
    rule_name,
    passed,
    failed_count,
    timestamp
  FROM pipeline.validation_results
  WHERE rule_name = 'security_detection_suite'
  ORDER BY timestamp DESC
  LIMIT 10;"
```

---
start reports\execution_*.html

# Para re-generar datos desde cero:
python scripts/generate_samples.py
# (El script genera tanto datos limpios como infectados)
```

### C. Comparar Resultados (Antes vs Después de Infección)

```bash
# 1. Ejecutar con datos limpios
python -m src.cli run pipeline -c examples/complete_pipeline.yml

# 2. Infectar datos
python -m src.modules.data_infection.infector data/samples/customers.csv data/output/customers_infected.csv

# 3. Ejecutar nuevamente con datos infectados
python -m src.cli run pipeline -c examples/complete_pipeline.yml

# 4. Comparar reportes en carpeta reports/
```

---

## 🆕 Arquitectura v2.0

**Cambio fundamental**: Infección de datos ocurre **ANTES** del pipeline

```
┌─────────────────────────────────────────────────┐
│ PASO 0: PRE-PIPELINE (Opcional)                │
│  python -m src.modules.data_infection.infector │
│  → Genera datos infectados con vulnerabilidades│
└─────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────┐
│ PASO 1-3: PIPELINE                              │
│  1. Ingestion   → Carga datos                  │
│  2. Validation  → Detecta ataques + calidad    │
│  3. Transformation → Limpia y transforma       │
└─────────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────────┐
│ REPORTES Y AUDITORÍA                            │
│  • HTML Report (visual, profesional)           │
│  • PostgreSQL (auditoría persistente)          │
│  • Outputs múltiples (CSV, Excel, Parquet)     │
└─────────────────────────────────────────────────┘
```

---

## 📊 Componentes del Framework

### 1. Ingestion Module
Carga datos desde múltiples fuentes:
- ✅ CSV
- ✅ Excel (.xlsx)
- ✅ Parquet
- ✅ PostgreSQL
- ✅ JSON

### 2. Validation Module
Valida calidad y detecta vulnerabilidades:
- **Pandera**: Validación de esquema
- **Great Expectations**: 39 expectativas en 4 suites
  - `basic_quality_suite`: 18 checks de calidad
  - `security_detection_suite`: 16 checks de seguridad (OWASP)
  - `anomaly_detection_suite`: 9 checks de anomalías
  - `business_rules_suite`: 7 reglas de negocio

### 3. Transformation Module
Transforma y limpia datos:
- Filter (condiciones SQL-like)
- Derive (nuevas columnas calculadas)
- Transform (str.upper(), str.title(), etc.)
- Clean (trim, lowercase, remove_nulls)

### 4. Auditing Module
Persiste resultados en PostgreSQL:
- Registro de ejecuciones
- Resultados de validación detallados
- Métricas de monitoreo
- Failure details en formato JSON

### 5. Reporting Module
Genera reportes HTML profesionales:
- Diseño corporativo limpio
- Executive summary
- Validation results con tablas detalladas
- Severity indicators (Critical/High/Medium)
- Pipeline stages metrics

---

## 🔧 Configuración de Pipeline (YAML)

```yaml
pipeline:
  name: "CustomerDataPipeline"
  description: "Pipeline completo con validación de seguridad"

ingestion:
  sources:
    - name: "customers_csv"
      type: "csv"
      path: "data/output/customers_infected.csv"
      output_dataset: "raw_customers"

validation:
  great_expectations:
    - dataset: "raw_customers"
      suites:
        - name: "basic_quality_suite"
          expectations:
            - expectation_type: "expect_column_values_to_not_be_null"
              column: "customer_id"
            - expectation_type: "expect_column_values_to_be_unique"
              column: "email"
        
        - name: "security_detection_suite"
          expectations:
            - expectation_type: "expect_column_values_to_not_match_regex"
              column: "name"
              regex: "(?:PRIVATE.*KEY|api.*key|secret|token|password)"

transformation:
  steps:
    - name: "filter_active"
      dataset: "raw_customers"
      output_dataset: "active_customers"
      operations:
        - type: "filter"
          condition: "account_status == 'active'"
    
    - name: "enrich_data"
      dataset: "active_customers"
      output_dataset: "enriched_customers"
      operations:
        - type: "derive"
          new_column: "customer_segment"
          expression: "'VIP' if lifetime_value > 10000 else 'Regular'"
        - type: "transform"
          column: "name"
          expression: "str.title()"

outputs:
  - name: "export_csv"
    type: "csv"
    dataset: "enriched_customers"
    path: "data/output/active_customers.csv"
  
  - name: "export_excel"
    type: "excel"
    dataset: "enriched_customers"
    path: "data/output/customers_enriched.xlsx"
  
  - name: "save_postgres"
    type: "postgres"
    dataset: "enriched_customers"
    table: "sample_data.customers_processed"
    mode: "replace"
```

Ver ejemplo completo: [examples/complete_pipeline.yml](examples/complete_pipeline.yml)

---

## 🦠 Módulo de Infección

Simula 10 tipos de ataques basados en OWASP Top 10:

| Ataque | Ejemplo | Detección |
|--------|---------|-----------|
| Data Poisoning | `<script>alert('XSS')</script>` | XSS regex patterns |
| SQL Injection | `' OR '1'='1` | SQL keywords detection |
| Command Injection | `; rm -rf /` | Shell operators |
| NoSQL Injection | `{"$ne": null}` | NoSQL operators |
| Path Traversal | `../../etc/passwd` | Path manipulation |
| Data Leakage | SSN: `123-45-6789` | PII pattern matching |
| Outliers | Valores 1000x normales | Statistical analysis |
| Missing Data | Strategic nulls | Completeness checks |
| Duplicates | Exact duplicates | Uniqueness validation |
| Format Corruption | Invalid encoding | Format validation |

**Uso:**
```bash
python -m src.modules.data_infection.infector <input.csv> <output_infected.csv> [--infection-rate 0.1]
```

---

## 💻 Comandos CLI Disponibles

```bash
# EJECUTAR PIPELINE
python -m src.cli run pipeline -c <config.yml>     # Ejecutar desde config YAML

# EJEMPLO REAL:
python -m src.cli run pipeline -c examples/complete_pipeline.yml

# GENERAR DATOS DE EJEMPLO
python scripts/generate_samples.py                 # Genera datos limpios y infectados

# VERIFICAR INSTALACIÓN
python scripts/test_connections.py                 # (si existe) Test de conexiones
```
- Valida esquema (Pandera)
- Valida calidad (Great Expectations)
- **Detecta ataques** OWASP Top 10

### 3. Transformation
Limpia y transforma datos

### Configuración (YAML)

```yaml
pipeline:
  name: "MyPipeline"
  schedule: "@daily"

ingestion:
  sources:
    - name: "customers"
      type: "csv"
      path: "data/output/customers_infected.csv"
      output_dataset: "raw_customers"

validation:
  schema_validation:
    - dataset: "raw_customers"
      schema_path: "schemas/customers_schema.py"

transformation:
  steps:
    - type: "filter"
      dataset: "raw_customers"
      condition: "status == 'active'"
```

Ver: [examples/complete_pipeline.yml](examples/complete_pipeline.yml)

---

## 🗄️ Base de Datos PostgreSQL

### Esquemas y Tablas

```sql
-- pipeline: Registro de pipelines y ejecuciones
pipeline.pipelines           -- Definición de pipelines
pipeline.executions          -- Historial de ejecuciones
pipeline.validation_results  -- Resultados detallados de validaciones

**📚 Documentación completa:** [docs/DATABASE.md](docs/DATABASE.md)
- Todos los comandos de db_utils.py con ejemplos
- Esquema completo de tablas
- 11+ queries SQL útiles para análisis
- Troubleshooting de base de datos

---

## 📁 Estructura del Proyecto

```
data-pipeline-framework/
├── src/
│   ├── cli.py                          # ✨ CLI principal
│   ├── pipeline_executor.py            # Orquestador (3 etapas)
│   ├── config.py                       # Configuración global
│   └── modules/
│       ├── data_infection/             # ✨ Módulo de infección
│       │   ├── infector.py            # Inyección de ataques
│       │   └── attack_types.py        # 10 tipos de ataques
│       ├── ingestion/                  # Conectores de datos
│       │   ├── multi_source_loader.py # Loader principal
│       │   └── connectors/
│       │       ├── csv_connector.py
│       │       ├── postgres_connector.py
│       │       └── json_connector.py
│       ├── validation/                 # Validación de calidad
│       │   ├── pandera_validator.py   # Schema validation
│       │   └── ge_validator.py        # Great Expectations
│       ├── transformation/             # Transformaciones
│       │   └── transformer.py
│       ├── auditing/                   # Auditoría PostgreSQL
│       │   └── audit_manager.py
│       ├── monitoring/                 # Métricas en memoria
│       │   └── collector.py
│       └── reporting/                  # ✨ Reportes HTML
│           └── html_generator_v2.py   # Diseño profesional
│
├── examples/
│   └── complete_pipeline.yml           # ✨ Ejemplo completo
│
├── scripts/
│   ├── init_db.sql                     # Schema PostgreSQL
│   └── generate_samples.py             # ✨ Genera datos sintéticos (limpios + infectados)
│
├── data/
│   ├── samples/                        # Datos de ejemplo
│   │   ├── customers.csv              # 1000 registros limpios
│   │   └── transactions.csv           # 5000 transacciones
│   └── output/                         # Datos procesados
│       ├── customers_infected.csv     # Con vulnerabilidades
│       ├── active_customers.csv       # Filtrados
│       ├── customers_enriched.xlsx    # Enriquecidos
│       └── customers_enriched.parquet # Optimizados
│
├── reports/                            # ✨ Reportes HTML generados
│   └── execution_XXXXX_YYYYMMDD.html
│
├── docker-compose.yml                  # PostgreSQL container
├── requirements.txt                    # Dependencias Python
└── README.md                          # 📖 Esta guía
```

---

## 🎯 Características Principales

### ✅ Ingestion
- Múltiples fuentes (CSV, Excel, Parquet, PostgreSQL, JSON)
- Configuración YAML
- Validación de accesibilidad

### ✅ Validation
- **Great Expectations**: 39 expectativas en 4 suites
- **Pandera**: Validación de esquema
- Detección de vulnerabilidades OWASP Top 10
- Quality score automático

### ✅ Transformation
- Filter (SQL-like conditions)
- Derive (nuevas columnas calculadas)
- Transform (string operations)
- Clean (normalización)

### ✅ Security Detection
- 16 patrones de detección en Great Expectations
- SQL Injection (detecta: `' OR '1'='1`, `--`, `union`, `select`)
- XSS (detecta: `<script>`, `javascript:`, `onerror`)
- Command Injection (detecta: `|`, `&&`, `;`, backticks)
- NoSQL Injection (detecta: `$ne`, `$gt`, `$where`)
- LDAP Injection (detecta: `*`, `(`, `)`)
- XML External Entities (detecta: `<!CDATA`, `<!DOCTYPE`)
- Path Traversal (detecta: `../`, `..\`)
- Data Leakage (detecta: SSN patterns, API keys, passwords, secrets)

### ✅ Reporting
- HTML profesional con diseño corporativo
- Executive summary
- Validation results detallados
- Severity indicators
- Pipeline stages metrics

### ✅ Auditing
- PostgreSQL persistente
- Historial completo de ejecuciones
- Resultados de validación con JSON details
- Queries útiles para análisis

---

## 🚀 Performance

| Métrica | Valor |
|---------|-------|
| Ejecución completa | ~2.5s |
| Ingestion (1030 records) | ~0.3s |
| Validation (39 checks) | ~1.5s |
| Transformation | ~0.2s |
| Output (4 formatos) | ~0.5s |
| Memory footprint | < 100MB |

---

## 🛠️ Troubleshooting

### Error: "Connection refused" PostgreSQL
```bash
# Verificar que el contenedor está corriendo
docker ps | findstr postgres

# Ver logs
docker logs framework_postgres
```

**Solución:** Si PostgreSQL no está corriendo, ver [Instalación Rápida](#instalación-rápida-automatizada) para iniciar servicios.


### Error: "File not found" al ejecutar pipeline
```bash
# Verificar rutas en el YAML
# Las rutas deben ser relativas al directorio raíz del proyecto

# Generar datos si faltan
python scripts/generate_sample_data.py
```

### Error: "Great Expectations validation failed"
```bash
# Ver detalles en el log
# Los warnings son normales si hay problemas en los datos

# Ver reporte HTML para análisis visual
start reports\execution_*.html
```

### Pipeline muy lento
```bash
# Reducir número de validaciones en el YAML
# Usar datos de muestra más pequeños
# Verificar que PostgreSQL no está sobrecargado
```

---

## 🆘 Soporte y Troubleshooting

**Issues comunes:**
1. PostgreSQL connection → Ver [docs/DATABASE.md - Troubleshooting](docs/DATABASE.md#troubleshooting)
2. File not found → Generar datos sintéticos primero
3. Validation warnings → Son esperados con datos infectados
4. Base de datos lenta → Ver [docs/DATABASE.md - Optimización](docs/DATABASE.md#troubleshooting)

**Recursos útiles:**
- Ver logs completos en terminal
- Revisar reportes HTML generados en `reports/`
- Consultar [docs/DATABASE.md](docs/DATABASE.md) para gestión de base de datos
- Revisar [docs/GUIA_COMPLETA.md](docs/GUIA_COMPLETA.md) para arquitectura

**Documentación adicional:**
- **DATABASE.md**: Gestión completa de base de datos
- **GUIA_COMPLETA.md**: Arquitectura y uso avanzado
- **Great Expectations**: https://docs.greatexpectations.io/
- **Pandera**: https://pandera.readthedocs.io/
- **OWASP Top 10**: https://owasp.org/www-project-top-ten/

---

**Versión**: 2.0.0  
**Framework**: Data Pipeline con Security Testing  
**Última actualización**: Febrero 2026

