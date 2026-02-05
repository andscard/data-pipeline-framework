# Data Pipeline Framework

Framework modular para pipelines de datos con validación de calidad, detección de vulnerabilidades de seguridad y reportes HTML profesionales.

---

## Instalación

```powershell
# Ejecutar script de instalación automática (Windows)
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

**📚 Documentación completa:** [docs/DATABASE.md](docs/DATABASE.md) (gestión, queries SQL, schema)

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

# Crear tablas
docker exec -i framework_postgres psql -U admin -d data_framework < scripts/init_db.sql
```

**Verificar instalación:**
```bash
# Ver estado de la base de datos
python scripts/db_utils.py status

# Debe mostrar: 4 tablas en schema pipeline (vacías)
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

## 🎯 Ejecución de Pipeline de Ejemplo


```bash
# Ejecutar pipeline de ejemplo
python -m src.cli run pipeline -c examples/complete_pipeline.yml
```

**El pipeline ejecuta:**

1. **INGESTION** - Carga datos desde múltiples fuentes (CSV, Excel, Parquet, PostgreSQL, JSON)

2. **VALIDATION** - Sistema de validación production-ready completo
   - **40+ tipos semánticos**: uuid, email, phone, ssn, credit_card, iban, ipv4, url, name, text, enum, date, etc.
   - **Seguridad OWASP Top 10+**: SQL injection, XSS, Command injection, NoSQL, LDAP, XPath, YAML, Template (SSTI), CSV injection, XXE, SSRF, Path traversal, File inclusion, CRLF injection
   - **Data Leakage**: API keys (AWS, GitHub, Google, Slack), Private keys (RSA, SSH), JWT tokens, Passwords, Connection strings
   - **Compliance**: GDPR (PII detection), PCI-DSS (credit card protection), HIPAA (medical records), SOC2 (debug leakage)
   - **Calidad de datos**: Outliers estadísticos (Z-score), distribuciones, quantiles, anti-patterns (test values, placeholders)
   - **Integridad**: Cross-field validation (date ranges, conditional required, sum equals, unique combinations)
   - **Reglas de negocio**: Minimum records, percentage in category, monotonic trends, referential integrity

3. **TRANSFORMATION** - Filtrado, limpieza y enriquecimiento de datos

4. **OUTPUT** - Exportación en múltiples formatos (CSV, Excel, Parquet, PostgreSQL)

> **💡 Validación Simplificada:** En lugar de escribir expectativas verbosas, define tipos de columna (`email`, `phone_es`, `credit_card`, etc.) y el framework genera automáticamente 100+ validaciones de calidad, seguridad y compliance. **80% menos código**, **10x más validaciones**. Ver [docs/VALIDATION_TYPES.md](docs/VALIDATION_TYPES.md) para los 40+ tipos disponibles.

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



```bash
# Abrir el reporte más reciente
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

## 💻 Comandos CLI

```bash
# Ejecutar pipeline desde configuración YAML
python -m src.cli run pipeline -c examples/complete_pipeline.yml

# Infectar datos con vulnerabilidades para testing
python -m src.cli infect -c examples/infection_config.yml

# Generar datos sintéticos
python scripts/generate_sample_data.py -c 10000 -t 50000
```

---

## 📁 Estructura del Proyecto

```
data-pipeline-framework/
├── src/
│   ├── cli.py                          # CLI principal
│   ├── pipeline_executor.py            # Orquestador de pipeline
│   ├── config.py                       # Configuración global
│   └── modules/
│       ├── data_infection/             # Módulo de infección
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
│       └── reporting/                  # Reportes HTML
│           └── html_generator.py      # Generador de reportes
│
├── examples/
│   └── complete_pipeline.yml           # Configuración completa
│
├── scripts/
│   ├── init_db.sql                     # Schema PostgreSQL
│   ├── db_utils.py                     # Utilidades de base de datos
│   └── generate_sample_data.py         # Generador de datos sintéticos
│
├── data/
│   ├── samples/                        # Datos de ejemplo
│   │   ├── customers.csv
│   │   └── transactions.csv
│   └── output/                         # Datos procesados
│       ├── customers_infected.csv
│       ├── active_customers.csv
│       ├── customers_enriched.xlsx
│       └── customers_enriched.parquet
│
├── reports/                            # Reportes HTML generados
│   └── execution_XXXXX_YYYYMMDD.html
│
├── docker-compose.yml                  # PostgreSQL container
├── requirements.txt                    # Dependencias Python
└── README.md                          # 📖 Esta guía
```

---

## 🎯 Características

### Ingestion
- Conectores para múltiples fuentes (CSV, Excel, Parquet, PostgreSQL, JSON)
- Configuración mediante YAML

### Validation
- Great Expectations: 39 expectativas en 4 suites
- Pandera: Validación de esquema
- Detección de vulnerabilidades OWASP Top 10
- Cálculo automático de quality score

### Security Detection
- SQL Injection
- XSS (Cross-Site Scripting)
- Command Injection
- NoSQL Injection
- LDAP Injection
- XML External Entities
- Path Traversal
- Data Leakage (SSN, API keys, passwords)

### Transformation
- Filtrado con condiciones SQL-like
- Creación de columnas derivadas
- Operaciones de string
- Normalización de datos

### Reporting
- Reportes HTML profesionales
- Executive summary con métricas clave
- Detalles de validaciones
- Indicadores de severidad

### Auditing
- Persistencia en PostgreSQL
- Historial completo de ejecuciones
- Resultados de validación detallados

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

**Solución:** Ejecutar `setup.ps1` o iniciar manualmente con `docker-compose up -d postgres`

### Error: "File not found" al ejecutar pipeline
Verificar que las rutas en el YAML son relativas al directorio raíz. Generar datos si faltan:
```bash
python scripts/generate_sample_data.py
```

### Error: "Great Expectations validation failed"
Los warnings son esperados cuando hay problemas en los datos. Ver reporte HTML para análisis visual:
```bash
start reports\execution_*.html
```

### Pipeline muy lento
- Reducir número de validaciones en el YAML
- Usar datos de muestra más pequeños
- Verificar que PostgreSQL no está sobrecargado

### Problemas con base de datos
Ver [docs/DATABASE.md](docs/DATABASE.md) para troubleshooting específico y optimización.

---

## 📚 Documentación

- **[docs/DATABASE.md](docs/DATABASE.md)** - Gestión de base de datos, queries SQL, troubleshooting
- **[docs/VALIDATION_TYPES.md](docs/VALIDATION_TYPES.md)** - Tipos de validación simplificada (uuid, email, phone, etc.)
- **[Great Expectations](https://docs.greatexpectations.io/)** - Documentación oficial de validaciones
- **[Pandera](https://pandera.readthedocs.io/)** - Documentación oficial de schemas
- **[OWASP Top 10](https://owasp.org/www-project-top-ten/)** - Vulnerabilidades de seguridad

