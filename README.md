# Data Pipeline Framework v2.0

Framework modular para pipelines de datos con **validación de calidad**, **detección de vulnerabilidades de seguridad** y **reportes HTML profesionales**.

---

## 📖 Guía Completa Paso a Paso

### Paso 1: Instalación Inicial

```bash
# 1.1 Instalar dependencias de Python
pip install -r requirements.txt

# 1.2 Iniciar PostgreSQL con Docker
docker-compose up -d postgres

# 1.3 Esperar a que PostgreSQL esté listo (5-10 segundos)
timeout /t 10

# 1.4 Inicializar base de datos
docker exec -i framework_postgres psql -U admin -d pipeline_db < scripts/init_db.sql
```

**Verificar instalación:**
```bash
# Verificar que PostgreSQL está corriendo
docker ps | findstr postgres

# Verificar tablas creadas
docker exec -i framework_postgres psql -U admin -d pipeline_db -c "\dt pipeline.*"
```

---

### Paso 2: Generar Datos Sintéticos

```bash
# 2.1 Generar datos limpios de ejemplo (1000 registros)
python scripts/generate_sample_data.py
```

**Salida esperada:**
```
data/samples/customers.csv       (1000 registros limpios)
data/samples/transactions.csv    (5000 transacciones)
```

---

### Paso 3: (Opcional) Infectar Datos con Vulnerabilidades

Este paso simula datos con problemas de seguridad y calidad para testing:

```bash
# 3.1 Infectar datos con 10 tipos de ataques OWASP
python -m src.modules.data_infection.infector data/samples/customers.csv data/output/customers_infected.csv

# 3.2 Ver reporte de infección
type data\output\infection_report.txt
```

**Tipos de ataques inyectados:**
- SQL Injection (`' OR '1'='1`)
- XSS (`<script>alert('XSS')</script>`)
- Command Injection (`; rm -rf /`)
- NoSQL Injection (`{"$ne": null}`)
- Path Traversal (`../../etc/passwd`)
- Data Leakage (SSN, API keys, passwords)
- Outliers (valores 1000x normales)
- Missing Data (nulls estratégicos)
- Duplicates (registros duplicados)
- Format Corruption (encoding corrupto)

---

### Paso 4: Ejecutar Pipeline Completo

```bash
# 4.1 Ejecutar pipeline con datos infectados
python -m src.cli run pipeline -c examples/complete_pipeline.yml
```

**El pipeline ejecutará 3 etapas:**

1. **INGESTION** → Carga datos desde CSV
2. **VALIDATION** → Detecta vulnerabilidades y valida calidad  
   - 4 suites: Quality, Security, Anomaly, Business Rules
   - 39 expectativas configuradas
3. **TRANSFORMATION** → Filtra y transforma datos limpios

**Salida esperada:**
```
Pipeline: CustomerDataPipeline
Status: completed
Duration: ~2.5s
Quality Score: 23.1%
Vulnerabilities: 3 detected
Report: reports/execution_XXXXX_YYYYMMDD_HHMMSS.html
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

### Paso 6: Consultar Base de Datos de Auditoría

```bash
# 6.1 Ver últimas ejecuciones
docker exec -i framework_postgres psql -U admin -d pipeline_db -c "
  SELECT execution_id, pipeline_name, status, 
         duration_seconds, start_time 
  FROM pipeline.executions 
  ORDER BY start_time DESC LIMIT 5;"

# 6.2 Ver validaciones fallidas de última ejecución
docker exec -i framework_postgres psql -U admin -d pipeline_db -c "
  SELECT rule_name, rule_type, failed_count, passed 
  FROM pipeline.validation_results 
  WHERE execution_id = (
    SELECT execution_id FROM pipeline.executions 
    ORDER BY start_time DESC LIMIT 1
  )
  ORDER BY failed_count DESC;"

# 6.3 Ver vulnerabilidades de seguridad detectadas
docker exec -i framework_postgres psql -U admin -d pipeline_db -c "
  SELECT rule_name, failed_count, 
         failure_details::jsonb->0->'kwargs'->>'column' as column,
         failure_details::jsonb->0->'kwargs'->>'regex' as pattern
  FROM pipeline.validation_results 
  WHERE rule_name = 'security_detection_suite' 
        AND passed = false
  ORDER BY timestamp DESC LIMIT 10;"
```

---

### Paso 7: Ver Datos Procesados

Los datos limpios y transformados se guardan en múltiples formatos:

```bash
# 7.1 CSV procesado
type data\output\active_customers.csv

# 7.2 Excel enriquecido
start data\output\customers_enriched.xlsx

# 7.3 Parquet optimizado
# (usar pandas o herramientas de análisis)

# 7.4 PostgreSQL
docker exec -i framework_postgres psql -U admin -d pipeline_db -c "
  SELECT * FROM sample_data.customers_processed LIMIT 10;"
```

---

## � Workflows Comunes

### A. Pipeline con Datos Limpios (Testing Normal)

```bash
# Generar datos → Ejecutar pipeline → Ver reporte
python scripts/generate_sample_data.py
python -m src.cli run pipeline -c examples/complete_pipeline.yml
start reports\execution_*.html
```

### B. Pipeline con Security Testing (Datos Infectados)

```bash
# Generar → Infectar → Ejecutar → Analizar vulnerabilidades
python scripts/generate_sample_data.py
python -m src.modules.data_infection.infector data/samples/customers.csv data/output/customers_infected.csv
python -m src.cli run pipeline -c examples/complete_pipeline.yml
start reports\execution_*.html
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
python -m src.cli run pipeline -c <config.yml>     # Ejecutar desde config
python -m src.cli run pipeline -n <pipeline_name>  # Ejecutar pipeline registrado

# GESTIÓN DE PIPELINES
python -m src.cli list pipelines                   # Listar todos los pipelines
python -m src.cli list pipelines --status active   # Filtrar por status

# VALIDACIÓN DE CONFIGURACIÓN
python -m src.cli validate config -p <config.yml>  # Validar YAML antes de ejecutar

# INFECCIÓN DE DATOS
python -m src.modules.data_infection.infector <input> <output> [--infection-rate 0.1]
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

-- sample_data: Datos procesados
sample_data.customers_processed  -- Output final del pipeline
```

### Queries Útiles

```sql
-- 1. Ver últimas 10 ejecuciones con métricas
SELECT 
    execution_id,
    pipeline_name,
    status,
    duration_seconds,
    records_processed,
    quality_score,
    start_time
FROM pipeline.executions
ORDER BY start_time DESC
LIMIT 10;

-- 2. Validaciones fallidas por tipo
SELECT 
    rule_name,
    rule_type,
    COUNT(*) as total_failures,
    SUM(failed_count) as total_records_affected
FROM pipeline.validation_results
WHERE passed = false
GROUP BY rule_name, rule_type
ORDER BY total_failures DESC;

-- 3. Vulnerabilidades de seguridad por columna
SELECT 
    failure_details::jsonb->0->'kwargs'->>'column' as column_name,
    failure_details::jsonb->0->'kwargs'->>'regex' as attack_pattern,
    COUNT(*) as detections
FROM pipeline.validation_results
WHERE rule_name = 'security_detection_suite' 
      AND passed = false
GROUP BY column_name, attack_pattern
ORDER BY detections DESC;

-- 4. Quality score trend (últimas 30 ejecuciones)
SELECT 
    execution_id,
    pipeline_name,
    quality_score,
    start_time::date as execution_date
FROM pipeline.executions
WHERE pipeline_name = 'CustomerDataPipeline'
ORDER BY start_time DESC
LIMIT 30;

-- 5. Pipelines más lentos (promedio)
SELECT 
    pipeline_name,
    COUNT(*) as executions,
    AVG(duration_seconds) as avg_seconds,
    MIN(duration_seconds) as min_seconds,
    MAX(duration_seconds) as max_seconds
FROM pipeline.executions
WHERE status = 'completed'
GROUP BY pipeline_name
ORDER BY avg_seconds DESC;
```

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
│   ├── generate_sample_data.py         # Datos sintéticos
│   └── test_connections.py             # Test de conexiones
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
- 16 patrones de detección
- SQL Injection
- XSS (Cross-Site Scripting)
- Command Injection
- NoSQL Injection
- LDAP Injection
- XML External Entities
- Path Traversal
- Data Leakage (PII, secrets)

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

# Si no está corriendo, iniciar
docker-compose up -d postgres

# Ver logs
docker logs framework_postgres
```

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

## 📖 Recursos Adicionales

- **Great Expectations Docs**: https://docs.greatexpectations.io/
- **Pandera Docs**: https://pandera.readthedocs.io/
- **OWASP Top 10**: https://owasp.org/www-project-top-ten/
- **PostgreSQL Docs**: https://www.postgresql.org/docs/

---

## 🆘 Soporte

**Issues comunes:**
1. PostgreSQL connection → Verificar docker-compose
2. File not found → Generar datos sintéticos primero
3. Validation warnings → Son esperados con datos infectados

**Para más ayuda:**
- Ver logs completos en terminal
- Revisar reportes HTML generados
- Consultar base de datos PostgreSQL

---

**Versión**: 2.0.0  
**Framework**: Data Pipeline con Security Testing  
**Última actualización**: Febrero 2026

