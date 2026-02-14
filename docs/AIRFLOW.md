# Apache Airflow Integration

El framework incluye integración con Apache Airflow para orquestar y programar la ejecución de pipelines de datos.

## Descripción

Apache Airflow es un orquestador de workflows que permite:
- 📅 **Programar pipelines** para ejecución automática (diaria, horaria, etc.)
- 🔍 **Monitorear ejecuciones** en tiempo real desde una interfaz web
- 🔄 **Re-ejecutar tareas fallidas** sin repetir todo el pipeline
- 📊 **Ver historial completo** de ejecuciones con logs detallados

## Arquitectura

### Componentes

```
┌─────────────────────────────────────────────────────────┐
│  Airflow Webserver (Puerto 8080)                       │
│  - Interfaz web para monitoreo                          │
│  - Trigger manual de DAGs                               │
│  - Visualización de logs                                │
└─────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│  Airflow Scheduler                                       │
│  - Detecta nuevos DAGs en examples/pipelines/*.yml      │
│  - Programa ejecuciones según schedule                  │
│  - Ejecuta tareas usando comando data-framework         │
└─────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────┐
│  PostgreSQL                                              │
│  - Metadata de Airflow (schema: airflow)                │
│  - Audit data del framework (schema: pipeline)          │
└─────────────────────────────────────────────────────────┘
```

### DAG Factory

El archivo [`airflow/dags/pipeline_dag_factory.py`](../airflow/dags/pipeline_dag_factory.py) escanea automáticamente el directorio `examples/pipelines/` y crea un DAG de Airflow por cada archivo `.yml` que encuentre.

**Pipeline con 5 Tareas:**

Cada pipeline se descompone en 5 tareas ejecutables independientemente:

```
1_ingestion → 2_validation → 3_transformation → 4_output → 5_export_logs
                                                                    ↑
                                                            (trigger: all_done)
```

- **1_ingestion**: Carga datos desde las fuentes configuradas
- **2_validation**: Ejecuta validaciones de calidad y seguridad
- **3_transformation**: Aplica transformaciones y enriquecimientos
- **4_output**: Escribe resultados en destinos configurados
- **5_export_logs**: Exporta logs y métricas (se ejecuta siempre, incluso si fallan tareas previas)

**Beneficio**: Si falla la validación, puedes corregir el problema y re-ejecutar solo desde `2_validation` sin repetir la ingestion.

## Inicio

### Levantar Airflow

```powershell
# Iniciar framework + Airflow
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml up -d

# Ver logs
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml logs -f
```

**Tiempo de inicio**: ~30 segundos

### Acceder a la UI

1. Abrir: http://localhost:8080
2. Login:
   - **Usuario**: `admin`
   - **Password**: `admin`

### Verificar DAGs

Los pipelines configurados en `examples/pipelines/*.yml` aparecerán automáticamente como DAGs en la interfaz.

**Ejemplo**:
```yaml
# examples/pipelines/data_pipeline.yml
pipeline:
  name: "CustomerPipeline"
  schedule: "@daily"  # Se ejecuta diariamente a medianoche
```

Genera el DAG: `customer_pipeline` visible en http://localhost:8080

## Uso

### Ejecutar Pipeline Manual

**Desde la UI:**
1. Ir a http://localhost:8080
2. Localizar el DAG (ej. `customer_transaction_pipeline`)
3. Click en "▶️" (Trigger DAG)
4. Monitorear en la vista "Graph" o "Grid"

**Desde API:**
```powershell
$headers = @{Authorization=("Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin")))}
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/dags/customer_transaction_pipeline/dagRuns" -Method POST -Headers $headers -ContentType "application/json" -Body '{}'
```

### Ver Logs de Tarea

**UI:**
1. Click en el cuadro de la tarea (ej. `1_ingestion`)
2. Click en "Log"

**API:**
```powershell
# Ver logs de tarea específica
docker logs framework_airflow_scheduler | Select-String "1_ingestion"
```

### Re-ejecutar Tarea Fallida

Si una tarea falla, puedes corregir el problema y re-ejecutar solo esa tarea:

1. Click en el cuadro de la tarea fallida
2. Click en "Clear"
3. Confirmar - la tarea se re-ejecutará automáticamente

**Importante**: Las tareas mantienen estado. Si re-ejecutas `2_validation`, usará los datos cargados por `1_ingestion` (guardados en `data/pipeline_state/<pipeline_name>/`).

### Detener Airflow

```powershell
# Detener servicios de Airflow (mantiene PostgreSQL)
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml stop airflow-scheduler airflow-webserver

# Detener todo (incluyendo PostgreSQL)
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml down
```

## Configuración de Pipelines

### Schedule Expressions

En el archivo YAML del pipeline:

```yaml
pipeline:
  name: "MyPipeline"
  schedule: "@daily"  # Cron expression o preset
```

**Presets disponibles:**
- `@once` - Una sola vez
- `@hourly` - Cada hora
- `@daily` - Diariamente a medianoche
- `@weekly` - Semanalmente los domingos
- `@monthly` - Mensualmente el día 1
- `@yearly` - Anualmente el 1 de enero

**Cron expressions:**
- `"0 */4 * * *"` - Cada 4 horas
- `"0 9 * * 1-5"` - Lunes a viernes a las 9am
- `"0 0 * * 0"` - Domingos a medianoche

**Sin schedule (manual):**
```yaml
pipeline:
  name: "MyPipeline"
  schedule: null  # Solo ejecución manual
```

### Configuración Avanzada

Puedes agregar configuración de Airflow en el YAML del pipeline:

```yaml
pipeline:
  name: "MyPipeline"
  schedule: "@daily"

airflow:
  default_args:
    owner: "data_team"
    retries: 2
    retry_delay_minutes: 5
    email: ["alerts@company.com"]
    email_on_failure: true
```

## Variables de Entorno

Las tareas de Airflow ejecutan el comando `data-framework` con estas variables de entorno:

```bash
# Configuradas automáticamente en docker-compose.airflow.yml
POSTGRES_HOST=postgres       # Nombre del servicio Docker
POSTGRES_PORT=5432           # Puerto interno (no el mapeado 5433)
POSTGRES_DB=data_framework
POSTGRES_USER=admin
POSTGRES_PASSWORD=<desde .env>
PYTHONPATH=/opt/airflow/framework
PATH=/home/airflow/.local/bin:/usr/local/bin:...
```

**Nota**: Desde contenedores Docker, PostgreSQL es accesible en `postgres:5432`. Desde el host (Windows), es `localhost:5433`.

## Troubleshooting

### DAGs no aparecen en la UI

**Causa**: El scheduler puede tardar hasta 30 segundos en detectar cambios.

**Solución**:
```powershell
# Reiniciar scheduler para forzar detección
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml restart airflow-scheduler

# Esperar 30 segundos
Start-Sleep -Seconds 30
```

### Tarea falla con "connection refused"

**Error**: `connection to server at "localhost", port 5433 failed: Connection refused`

**Causa**: Configuración incorrecta de host PostgreSQL en entorno Docker.

**Solución**: Las variables de entorno en `docker-compose.airflow.yml` deben usar:
```yaml
POSTGRES_HOST: postgres  # ✓ Correcto (nombre de servicio)
POSTGRES_HOST: localhost # ✗ Incorrecto (solo funciona desde host)
```

### Tarea falla con "bash not found"

**Causa**: Variables de entorno `PATH` no configuradas correctamente.

**Solución**: Verificar que `TASK_ENV` en [`pipeline_dag_factory.py`](../airflow/dags/pipeline_dag_factory.py) incluya:
```python
'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:...'
```

### Comando "data-framework" no encontrado

**Causa**: Script wrapper no instalado o sin permisos.

**Solución**:
```powershell
# Verificar que el script existe
docker exec framework_airflow_scheduler ls -la /home/airflow/.local/bin/data-framework

# Debe mostrar: -rwxr-xr-x (permisos de ejecución)

# Si no existe, reiniciar servicios
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml restart
```

### Ver logs detallados del scheduler

```powershell
# Logs del scheduler (detección de DAGs, ejecución de tareas)
docker logs framework_airflow_scheduler -f

# Logs del webserver (UI, API)
docker logs framework_airflow_webserver -f

# Filtrar por DAG específico
docker logs framework_airflow_scheduler | Select-String "customer_transaction"
```

## Comandos Útiles

```powershell
# Estado de contenedores
docker ps --filter "name=framework"

# Reiniciar solo scheduler (detectar cambios en DAGs)
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml restart airflow-scheduler

# Limpiar metadata de Airflow (reset completo)
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml down
docker volume rm framework_postgres_data  # ⚠️ BORRA TODOS LOS DATOS
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml up -d

# Ver DAGs detectados
docker exec framework_airflow_scheduler airflow dags list

# Trigger DAG desde CLI
docker exec framework_airflow_scheduler airflow dags trigger customer_transaction_pipeline
```

## Recursos

- **Airflow UI**: http://localhost:8080
- **API Docs**: http://localhost:8080/api/v1/ui/
- **DAG Factory**: [`airflow/dags/pipeline_dag_factory.py`](../airflow/dags/pipeline_dag_factory.py)
- **Docker Compose**: [`docker-compose.airflow.yml`](../docker-compose.airflow.yml)
- **Pipeline Configs**: [`examples/pipelines/`](../examples/pipelines/)
