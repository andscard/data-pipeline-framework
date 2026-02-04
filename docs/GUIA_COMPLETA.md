# Data Pipeline Framework v2.0 - Guía Completa

## 📋 Contenido

1. [Instalación Rápida](#instalación-rápida)
2. [Arquitectura v2.0](#arquitectura-v20)
3. [Módulo de Infección](#módulo-de-infección-de-datos)
4. [Pipeline (3 Etapas)](#pipeline-3-etapas)
5. [Integración con Airflow](#integración-con-airflow)
6. [Ejemplos de Uso](#ejemplos-de-uso)
7. [Base de Datos y Auditoría](#base-de-datos-y-auditoría)

---

## 🚀 Instalación Rápida

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar PostgreSQL (Docker)
docker-compose up -d postgres

# 3. Inicializar base de datos
docker exec -i framework_postgres psql -U admin -d pipeline_db < scripts/init_db.sql

# 4. Probar instalación
python scripts/demo_new_architecture.py
```

---

## 🏗️ Arquitectura v2.0

### Cambio Fundamental

**❌ Antes (v1.x)**: Security testing dentro del pipeline (4 etapas)

**✅ Ahora (v2.0)**: Infección de datos PRE-PIPELINE + Pipeline simplificado (3 etapas)

### Flujo Completo

```
┌──────────────────────────────────────────────────────┐
│ PASO 0: PRE-PIPELINE (Opcional)                     │
│                                                      │
│  Datos Limpios → [Data Infection] → Datos Infectados│
│                   (OWASP Top 10)                     │
│                                                      │
│  Comando: python -m src.cli infect -c config.yml    │
└──────────────────────────────────────────────────────┘
                      ↓
┌──────────────────────────────────────────────────────┐
│ PASO 1-3: PIPELINE                                   │
│                                                      │
│  1. INGESTION      → Carga datos                    │
│  2. VALIDATION     → Detecta ataques + calidad      │
│  3. TRANSFORMATION → Limpia y transforma            │
│                                                      │
│  Comando: python -m src.cli run pipeline -n NAME    │
└──────────────────────────────────────────────────────┘
                      ↓
┌──────────────────────────────────────────────────────┐
│ RESULTADOS                                           │
│                                                      │
│  • Datos procesados en destinos configurados        │
│  • Métricas en PostgreSQL (pipeline.executions)     │
│  • Validaciones en validation.validation_results    │
│  • Logs completos en pipeline.audit_logs            │
└──────────────────────────────────────────────────────┘
```

---

## 🦠 Módulo de Infección de Datos

### ¿Por qué infectar datos?

Para **simular ataques reales** y validar que el pipeline detecta:
- Injection payloads (SQL, NoSQL, Command)
- Data poisoning (valores maliciosos)
- Outliers extremos
- Data leakage (SSN, passwords)
- Format corruption
- Y más... (10 tipos basados en OWASP Top 10)

### 10 Tipos de Ataques

| # | Tipo | Descripción | Ejemplo |
|---|------|-------------|---------|
| 1 | `data_poisoning` | Valores maliciosos | `<script>alert('XSS')</script>` |
| 2 | `schema_manipulation` | Cambio de tipos | `"INVALID"` en columna numérica |
| 3 | `injection` | SQL/NoSQL injection | `' OR '1'='1` |
| 4 | `missing_data` | Nulls estratégicos | Nulls en campos críticos |
| 5 | `outliers` | Valores extremos | 999999999 en `amount` |
| 6 | `duplicates` | Filas duplicadas | Duplicar registros completos |
| 7 | `data_leakage` | Info sensible | `SSN: 123-45-6789` |
| 8 | `inconsistency` | Formatos mixtos | `2024-01-01` y `01/01/2024` |
| 9 | `format_corruption` | Encoding corrupto | Null bytes, `\x00` |
| 10 | `timing` | Timestamps falsos | Fechas futuras (2099-12-31) |

### Configuración (YAML)

```yaml
# examples/infection_config.yml
input:
  path: "data/samples/customers.csv"

output:
  path: "data/output/customers_infected.csv"

random_seed: 42
global_rate: 0.10  # 10% de filas afectadas

attacks:
  - type: data_poisoning
    columns: ['email', 'name']
    rate: 0.05
  
  - type: injection
    columns: ['address']
    rate: 0.02
  
  - type: outliers
    columns: ['lifetime_value']
    rate: 0.10
```

### Uso

```bash
# CLI
python -m src.cli infect -c examples/infection_config.yml

# Ver reporte
cat data/output/infection_report.json
```

```python
# Python API
from src.modules.data_infection import DataInfector

infector = DataInfector.from_yaml('examples/infection_config.yml')
infected_df = infector.run()

report = infector.generate_report()
print(f"Attacks: {report['attacks_successful']}/{report['attacks_applied']}")
```

---

## 🔄 Pipeline (3 Etapas)

### Etapa 1: Ingestion

**Propósito**: Cargar datos desde múltiples fuentes

**Conectores disponibles**:
- CSV, Excel, Parquet, TXT
- PostgreSQL
- JSON

```yaml
ingestion:
  sources:
    - name: "customers"
      type: "csv"
      path: "data/output/customers_infected.csv"  # Datos pre-infectados
      output_dataset: "raw_customers"
```

### Etapa 2: Validation

**Propósito**: 
1. Validar esquema (Pandera)
2. Validar calidad (Great Expectations)
3. **NUEVO**: Detectar ataques OWASP Top 10

```yaml
validation:
  # Validación de esquema
  schema_validation:
    - dataset: "raw_customers"
      schema_path: "schemas/customers_schema.py"
  
  # Validación de calidad
  expectations:
    - dataset: "raw_customers"
      suite_name: "customer_quality"
      expectations:
        - type: "expect_column_values_to_not_be_null"
          column: "customer_id"
```

**TODO**: Implementar detectores de ataques en esta etapa

### Etapa 3: Transformation

**Propósito**: Limpiar y transformar datos

```yaml
transformation:
  steps:
    - type: "filter"
      dataset: "raw_customers"
      condition: "status == 'active'"
    
    - type: "derive"
      dataset: "raw_customers"
      new_column: "full_name"
      expression: "first_name + ' ' + last_name"
```

---

## ✈️ Integración con Airflow

### ¿Por qué Airflow?

Airflow proporciona:
- **Orquestación**: Scheduling automático (@daily, @hourly, cron)
- **Visualización**: UI para ver ejecuciones y logs
- **Alertas**: Email on failure/retry
- **Monitoreo**: Métricas y duración de tasks
- **Reintento automático**: Retry con backoff exponencial

### Generar DAG

```bash
# El DAG se genera automáticamente al crear pipeline
python -m src.cli create pipeline \
  -n CustomerPipeline \
  -p examples/complete_pipeline.yml \
  --generate-airflow
```

**Salida**: `airflow/dags/customer_pipeline_v2_dag.py`

### Estructura del DAG

```python
# DAG generado automáticamente
with DAG(
    dag_id='customer_data_pipeline_v2',
    schedule_interval='@daily',
    default_args={
        'owner': 'data_team',
        'retries': 2,
        'email_on_failure': True
    }
) as dag:
    
    # Task único que ejecuta las 3 etapas
    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
        execution_timeout=timedelta(hours=1)
    )
```

### Configuración en YAML

```yaml
airflow:
  dag_id: "my_pipeline_v2"
  
  default_args:
    owner: "data_team"
    retries: 2
    retry_delay_minutes: 5
    email_on_failure: true
```

### Iniciar Airflow

```bash
# 1. Inicializar Airflow (primera vez)
cd airflow
airflow db init
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# 2. Iniciar servicios
airflow webserver -p 8080  # Terminal 1
airflow scheduler             # Terminal 2

# 3. Acceder a UI
# http://localhost:8080
# Usuario: admin | Password: admin
```

### Ver Resultados en Airflow UI

1. **Grid View**: Timeline de ejecuciones
2. **Graph View**: Flujo del DAG (1 task en v2.0)
3. **Logs**: Ver salida completa del pipeline
4. **XCom**: Ver resultados (execution_id, records_processed, etc.)

### Monitoreo Avanzado

```python
# Los resultados están en XCom:
def downstream_task(**context):
    ti = context['task_instance']
    
    execution_id = ti.xcom_pull(task_ids='run_pipeline', key='execution_id')
    status = ti.xcom_pull(task_ids='run_pipeline', key='status')
    records = ti.xcom_pull(task_ids='run_pipeline', key='records_processed')
    
    print(f"Pipeline {execution_id}: {status} - {records} records")
```

---

## 💡 Ejemplos de Uso

### Ejemplo 1: Pipeline Simple

```bash
# 1. Generar datos de muestra
python scripts/generate_sample_data.py

# 2. Crear y ejecutar pipeline
python -m src.cli create pipeline \
  -n SimplePipeline \
  -p examples/complete_pipeline.yml

python -m src.cli run pipeline -n SimplePipeline
```

### Ejemplo 2: Testing de Seguridad

```bash
# 1. Generar datos limpios
python scripts/generate_sample_data.py

# 2. Infectar datos
python -m src.cli infect -c examples/infection_config.yml

# 3. Ejecutar pipeline (detecta ataques)
# Modificar complete_pipeline.yml:
#   ingestion.sources[0].path: "data/output/customers_infected.csv"

python -m src.cli run pipeline -n SecurityTestPipeline

# 4. Ver resultados
psql -U admin -d pipeline_db -c "
  SELECT check_name, status, COUNT(*)
  FROM validation.validation_results
  WHERE execution_id = (
    SELECT execution_id 
    FROM pipeline.executions 
    ORDER BY start_time DESC 
    LIMIT 1
  )
  GROUP BY check_name, status;
"
```

### Ejemplo 3: Orquestación con Airflow

```bash
# 1. Crear pipeline con Airflow
python -m src.cli create pipeline \
  -n ProductionPipeline \
  -p examples/complete_pipeline.yml \
  --generate-airflow

# 2. Verificar DAG generado
ls airflow/dags/

# 3. Iniciar Airflow
cd airflow
airflow webserver -p 8080 &
airflow scheduler &

# 4. En UI (http://localhost:8080):
#    - Activar DAG "customer_data_pipeline_v2"
#    - Trigger manual run
#    - Ver logs y resultados
```

### Ejemplo 4: Demo Completo

```bash
# Script que ejecuta todo el flujo
python scripts/demo_new_architecture.py

# Salida:
# ✓ Genera 1000 customers limpios
# ✓ Infecta con 10 tipos de ataques
# ✓ Compara limpios vs infectados
# ✓ Muestra estadísticas
```

---

## 🗄️ Base de Datos y Auditoría

### Esquemas de PostgreSQL

```sql
-- 1. pipeline: Registro de pipelines y ejecuciones
SELECT * FROM pipeline.pipelines;
SELECT * FROM pipeline.executions ORDER BY start_time DESC LIMIT 10;
SELECT * FROM pipeline.audit_logs WHERE execution_id = '<id>';

-- 2. validation: Resultados de validación
SELECT * FROM validation.validation_results 
WHERE execution_id = '<id>' AND status = 'failed';

-- 3. security: Simulaciones de ataques (legacy, aún en BD)
SELECT * FROM security.attack_scenarios;
SELECT * FROM security.simulation_results;

-- 4. monitoring: Métricas del sistema
SELECT * FROM monitoring.pipeline_metrics 
WHERE metric_name = 'execution_duration'
ORDER BY timestamp DESC LIMIT 10;
```

### Queries Útiles

```sql
-- Últimas 10 ejecuciones
SELECT 
  execution_id,
  pipeline_id,
  status,
  records_processed,
  records_failed,
  ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2) as duration_seconds
FROM pipeline.executions
ORDER BY start_time DESC
LIMIT 10;

-- Validaciones fallidas
SELECT 
  check_name,
  status,
  error_message,
  COUNT(*) as failures
FROM validation.validation_results
WHERE status = 'failed'
GROUP BY check_name, status, error_message;

-- Pipelines más lentos
SELECT 
  p.name,
  AVG(EXTRACT(EPOCH FROM (e.end_time - e.start_time))) as avg_duration_seconds
FROM pipeline.pipelines p
JOIN pipeline.executions e ON p.pipeline_id = e.pipeline_id
GROUP BY p.name
ORDER BY avg_duration_seconds DESC;
```

---

## 📁 Estructura del Proyecto

```
data-pipeline-framework/
├── src/
│   ├── cli.py                         # CLI principal
│   ├── pipeline_executor.py           # Orquestador (3 etapas)
│   ├── airflow_generator.py           # Generador de DAGs
│   ├── config_validator.py            # Validador de YAML
│   └── modules/
│       ├── data_infection/            # ✨ NUEVO v2.0
│       │   ├── attack_types.py        # 10 ataques OWASP
│       │   └── infector.py            # Motor de infección
│       ├── ingestion/
│       │   ├── connectors/            # CSV, Excel, Postgres, etc.
│       │   └── synthetic_generator.py # Generador de datos
│       ├── validation/
│       │   ├── pandera_validator.py
│       │   └── ge_validator.py
│       ├── transformation/
│       │   └── transformer.py
│       └── auditing/
│           └── audit_manager.py
│
├── airflow/
│   ├── dags/                          # DAGs generados
│   ├── airflow.cfg                    # Configuración Airflow
│   └── webserver_config.py
│
├── examples/
│   ├── complete_pipeline.yml          # ✨ Ejemplo completo v2.0
│   └── infection_config.yml           # ✨ Config de infección
│
├── scripts/
│   ├── init_db.sql                    # Schema de PostgreSQL
│   ├── generate_sample_data.py        # Genera datos sintéticos
│   └── demo_new_architecture.py       # ✨ Demo completo v2.0
│
├── docs/
│   └── GUIA_COMPLETA.md              # Este documento
│
├── data/
│   ├── samples/                       # Datos de ejemplo
│   └── output/                        # Datos infectados/procesados
│
└── docker-compose.yml                 # PostgreSQL + Airflow (futuro)
```

---

## 🎯 Roadmap

### Completado ✅
- [x] Módulo de infección (10 ataques OWASP Top 10)
- [x] Arquitectura v2.0 (3 etapas)
- [x] CLI mejorado con comando `infect`
- [x] Generador de Airflow actualizado
- [x] Documentación consolidada

### En Desarrollo 🚧
- [ ] **Detectores de ataques en Validation stage**
  - [ ] Detector de injection payloads (regex)
  - [ ] Detector de outliers extremos (Z-score)
  - [ ] Detector de data leakage (SSN, credit cards)
  - [ ] Detector de format corruption
- [ ] **Dashboard de seguridad** (Grafana/Streamlit)
- [ ] **Docker Compose completo** (PostgreSQL + Airflow)

### Futuro 🔮
- [ ] Más tipos de ataques (drift detection, model poisoning)
- [ ] Integración con CI/CD (GitHub Actions)
- [ ] API REST para ejecución remota
- [ ] Soporte para más destinos (S3, BigQuery, Snowflake)

---

## 🆘 Troubleshooting

### Error: "Pipeline not found"

```bash
# Listar pipelines registrados
python -m src.cli list pipelines

# Crear pipeline si no existe
python -m src.cli create pipeline -n MyPipeline -p config.yml
```

### Error: PostgreSQL connection failed

```bash
# Verificar que PostgreSQL está corriendo
docker ps | grep postgres

# Iniciar PostgreSQL
docker-compose up -d postgres

# Verificar conexión
docker exec -it framework_postgres psql -U admin -d pipeline_db -c "SELECT 1;"
```

### Airflow DAG no aparece en UI

```bash
# Verificar que el DAG existe
ls airflow/dags/

# Verificar permisos
chmod +x airflow/dags/*.py

# Refrescar DAGs en UI
# O reiniciar scheduler:
pkill -f "airflow scheduler"
airflow scheduler &
```

---

## 📞 Contacto y Soporte

- **Documentación**: Ver `docs/` para detalles técnicos
- **Ejemplos**: Ver `examples/` para configs de ejemplo
- **Issues**: Reportar bugs o sugerencias

---

**Versión**: 2.0.0  
**Última actualización**: Febrero 2026  
**Framework**: Data Pipeline Framework con Security Testing
