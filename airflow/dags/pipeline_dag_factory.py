"""
DAG Factory - Data Pipeline Framework + Airflow Integration
============================================================

Este módulo genera dinámicamente DAGs de Airflow a partir de las 
configuraciones YAML de pipelines del framework.

Características:
- 🔄 Auto-descubrimiento de pipelines en examples/
- 📅 Scheduling desde configuración YAML
- 🔁 Retries configurables desde YAML  
- 📊 Integración con sistema de auditoría existente
- 🚫 No duplica procesos - usa el CLI existente

Funcionamiento:
1. Escanea directorio examples/ buscando *.yml
2. Lee metadata del pipeline (name, schedule, retries)
3. Crea un DAG por cada pipeline
4. El DAG ejecuta: data-framework run pipeline -c <config>

Configuración YAML:
```yaml
pipeline:
  name: "MyPipeline"
  description: "Pipeline description"
  schedule: "@daily"  # Cron expression o preset de Airflow
  
airflow:  # Opcional
  dag_id: "custom_dag_id"  # Default: pipeline name
  default_args:
    owner: "data_team"
    email: ["alerts@company.com"]
    email_on_failure: true
    email_on_retry: false
    retries: 2
    retry_delay_minutes: 5
```

Schedules soportados:
- @once, @hourly, @daily, @weekly, @monthly, @yearly
- Cron: "0 */4 * * *" (cada 4 horas)
- None: Solo ejecución manual

Autor: Data Team
Versión: 1.0.0
"""

import pendulum
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import yaml

# Airflow imports
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.pipeline_executor import PipelineExecutor
from src.modules.validation.exceptions import QualityThresholdError

import json
from src.modules.auditing.log_exporter import LogExporter
from src.modules.ingestion.config import POSTGRES_CONFIG

# Logger setup
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Path al framework (montado en /opt/airflow/framework)
FRAMEWORK_PATH = Path("/opt/airflow/framework")
if str(FRAMEWORK_PATH) not in sys.path:
    sys.path.append(str(FRAMEWORK_PATH))
    
# Actualizar paths relativos para que funcionen dentro de Airflow
STATE_DIR = FRAMEWORK_PATH / "data" / "pipeline_state"
ARTIFACTS_DIR = FRAMEWORK_PATH / "artifacts"

PIPELINES_DIR = FRAMEWORK_PATH / "examples" / "pipelines"
CLI_COMMAND = "data-framework"  # Ejecución del CLI dentro del contenedor de Airflow

# Environment variables para BashOperator
# Las variables PostgreSQL se heredan de docker-compose.airflow.yml
TASK_ENV = {
    'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/sbin:/bin',
    'PYTHONPATH': '/opt/airflow/framework',
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB'),
    'POSTGRES_USER': os.getenv('POSTGRES_USER'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
}

# Defaults de Airflow
DEFAULT_ARGS = {
    'owner': 'data_framework',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Presets de schedule
SCHEDULE_PRESETS = {
    '@once': None,
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@yearly': '0 0 1 1 *',
    None: None,
}


def load_pipeline_config(yaml_path: Path) -> Optional[Dict[str, Any]]:
    """
    Carga configuración de pipeline desde archivo YAML.
    
    Args:
        yaml_path: Path al archivo de configuración
        
    Returns:
        Dict con configuración o None si hay error
    """
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Validar que tenga sección pipeline
        if 'pipeline' not in config:
            logger.warning(f"⚠️  {yaml_path.name}: No tiene sección 'pipeline', ignorando")
            return None
            
        return config
    except Exception as e:
        logger.error(f"❌ Error cargando {yaml_path.name}: {e}")
        return None


def parse_schedule(schedule: Optional[str]) -> Optional[str]:
    """
    Convierte schedule del YAML a formato Airflow.
    
    Args:
        schedule: Schedule del YAML (@daily, cron, None)
        
    Returns:
        Schedule compatible con Airflow
    """
    if schedule is None or schedule.lower() == 'none':
        return None
    
    # Si es un preset, convertir
    if schedule in SCHEDULE_PRESETS:
        return SCHEDULE_PRESETS[schedule]
    
    # Si es cron expression, retornar tal cual
    return schedule


def parse_airflow_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrae configuración específica de Airflow del YAML.
    
    Args:
        config: Configuración completa del pipeline
        
    Returns:
        Dict con configuración de Airflow
    """
    airflow_config = config.get('airflow', {})
    default_args = airflow_config.get('default_args', {})
    
    # Convertir retry_delay_minutes a timedelta
    if 'retry_delay_minutes' in default_args:
        minutes = default_args.pop('retry_delay_minutes')
        default_args['retry_delay'] = timedelta(minutes=minutes)
    
    # Merge con defaults
    merged_args = DEFAULT_ARGS.copy()
    merged_args.update(default_args)
    
    return {
        'dag_id': airflow_config.get('dag_id'),
        'default_args': merged_args,
    }


def execute_pipeline_stage(pipeline_name: str, config_path: str, stage: str):
    """
    Función ejecutable por PythonOperator para correr una etapa específica.
    """
    
    logger.info(f"🚀 Iniciando etapa {stage} para: {pipeline_name}")
    
    try:
        # Cargar configuración desde el path absoluto
        # Nota: config_path viene como relativo 'examples/...', ajustamos a absoluto
        abs_config_path = FRAMEWORK_PATH / config_path
        
        with open(abs_config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        # Ejecutar etapa
        executor = PipelineExecutor(pipeline_name, config)
        executor.execute(stage=stage)
        logger.info(f"✅ Etapa {stage} finalizada exitosamente")
        
    except QualityThresholdError as e:
        logger.error(f"❌ FALLO DE CALIDAD IRRECUPERABLE: {e}")
        # Esto detendrá los reintentos automáticos
        raise e
        
    except Exception as e:
        logger.exception(f"❌ Error en etapa {stage}: {e}")
        raise e

def execute_export_logs_for_pipeline(pipeline_name: str):
    """
    Exportar logs de la ejecución actual del pipeline hacia su carpeta de artifacts unificada.
    Lee el contexto de ejecución para determinar la carpeta correcta.
    """
    logger.info(f"📤 Iniciando exportación de logs para: {pipeline_name}")
    
    try:
        # Determinar execution_id desde el contexto guardado
        context_file = STATE_DIR / pipeline_name / "context.json"
        
        execution_id = None
        timestamp = None
        
        if context_file.exists():
            with open(context_file, 'r') as f:
                context = json.load(f)
                execution_id = context.get("execution_id")
                timestamp = context.get("timestamp")
        
        if not execution_id or not timestamp:
            logger.warning(f"⚠️ No se encontró contexto de ejecución para {pipeline_name}, usando fallback")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = ARTIFACTS_DIR / pipeline_name / "exports" / f"airflow_fallback_{timestamp}"
        else:
            execution_folder_name = f"{timestamp}_{execution_id}"
            output_dir = ARTIFACTS_DIR / pipeline_name / "executions" / execution_folder_name / "logs"
        
        print(f"  Contexto encontrado: execution_id={execution_id}, timestamp={timestamp}")
        print(f"  Exportando logs a: {output_dir}")
        logger.info(f"  Contexto encontrado: execution_id={execution_id}, timestamp={timestamp}")
        logger.info(f"  Exportando logs a: {output_dir}")

        logger.info(f"📂 Destino de logs: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Ejecutar exportación usando LogExporter directamente
        exporter = LogExporter(POSTGRES_CONFIG)
        exporter.connect()
        
        # Verificar que el pipeline existe
        available_pipelines = exporter.get_available_pipelines()
        
        if pipeline_name in available_pipelines:
            # Exportar logs filtrando por execution_id si es posible, o todo el pipeline
            exported_files = exporter.export_all(pipeline_name, output_dir, execution_id=execution_id)
            
            for file_type, file_path in exported_files.items():
                logger.info(f"  - {Path(file_path).name}")
                
            logger.info("✅ Exportación completada exitosamente")
        else:
            logger.warning(f"⚠️ Pipeline '{pipeline_name}' no encontrado en BD para exportar logs")
            
        exporter.close()

    except Exception as e:
        logger.exception(f"❌ Error exportando logs: {e}")
        raise e


def create_pipeline_dag(yaml_path: Path) -> Optional[DAG]:
    """
    Crea un DAG de Airflow para un pipeline del framework.
    
    Args:
        yaml_path: Path al archivo de configuración YAML
        
    Returns:
        DAG de Airflow o None si hay error
    """
    # Cargar configuración
    config = load_pipeline_config(yaml_path)
    if not config:
        return None
    
    pipeline_config = config['pipeline']
    pipeline_name = pipeline_config.get('name', yaml_path.stem)
    description = pipeline_config.get('description', f'Pipeline: {pipeline_name}')
    schedule = parse_schedule(pipeline_config.get('schedule'))
    
    # Extraer configuración de Airflow
    airflow_config = parse_airflow_config(config)
    dag_id = airflow_config['dag_id'] or f"pipeline_{pipeline_name.lower().replace(' ', '_')}"
    default_args = airflow_config['default_args']
    
    # Crear DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule=schedule,
        start_date=pendulum.today('UTC').add(days=-1), 
        catchup=False,
        tags=['data-framework', 'pipeline'],
    )
    
    config_path = f"examples/pipelines/{yaml_path.name}"
    
    # ========================================================================
    # TAREAS SEPARADAS POR ETAPA DEL PIPELINE
    # ========================================================================
    
    # 1. Ingestion
    ingestion_task = PythonOperator(
        task_id='1_ingestion',
        python_callable=execute_pipeline_stage,
        op_kwargs={
            'pipeline_name': pipeline_name,
            'config_path': config_path,
            'stage': 'ingestion'
        },
        dag=dag,
    )
    
    # 2. Validation
    validation_task = PythonOperator(
        task_id='2_validation',
        python_callable=execute_pipeline_stage,
        op_kwargs={
            'pipeline_name': pipeline_name,
            'config_path': config_path,
            'stage': 'validation'
        },
        dag=dag,
    )
    
    # 3. Transformation
    transformation_task = PythonOperator(
        task_id='3_transformation',
        python_callable=execute_pipeline_stage,
        op_kwargs={
            'pipeline_name': pipeline_name,
            'config_path': config_path,
            'stage': 'transformation'
        },
        dag=dag,
    )
    
    # 4. Output
    output_task = PythonOperator(
        task_id='4_output',
        python_callable=execute_pipeline_stage,
        op_kwargs={
            'pipeline_name': pipeline_name,
            'config_path': config_path,
            'stage': 'output'
        },
        dag=dag,
    )
    
    # 5. Export Logs
    export_logs = PythonOperator(
        task_id='5_export_logs',
        python_callable=execute_export_logs_for_pipeline,
        op_kwargs={
            'pipeline_name': pipeline_name,
        },
        dag=dag,
        trigger_rule='all_done',
    )
    
    # ========================================================================
    # DEFINIR FLUJO DE DEPENDENCIAS
    # ========================================================================
    # Cada etapa depende de la anterior (flujo lineal)
    ingestion_task >> validation_task >> transformation_task >> output_task >> export_logs
    
    logger.info(f"✅ DAG creado: {dag_id} (Schedule: {schedule or 'Manual'})")
    
    return dag


# ============================================================================
# DAG GENERATION
# ============================================================================

def discover_and_create_dags() -> Dict[str, DAG]:
    """
    Descubre pipelines en examples/ y crea DAGs para cada uno.
    
    Returns:
        Dict con DAGs creados {dag_id: DAG}
    """
    dags = {}
    
    if not PIPELINES_DIR.exists():
        logger.warning(f"⚠️  Directorio de pipelines no encontrado: {PIPELINES_DIR}")
        return dags
    
    logger.info(f"🔍 Escaneando pipelines en: {PIPELINES_DIR}")
    
    # Buscar archivos YAML
    yaml_files = list(PIPELINES_DIR.glob("*.yml")) + list(PIPELINES_DIR.glob("*.yaml"))
    
    if not yaml_files:
        logger.warning("⚠️  No se encontraron archivos de configuración (.yml/.yaml)")
        return dags
    
    logger.info(f"📋 Encontrados {len(yaml_files)} archivos de configuración")
    
    # Crear DAG por cada pipeline
    for yaml_path in yaml_files:
        logger.debug(f"🔨 Procesando: {yaml_path.name}")
        
        dag = create_pipeline_dag(yaml_path)
        if dag:
            dags[dag.dag_id] = dag
    
    logger.info(f"✅ Total de DAGs creados: {len(dags)}")
    
    return dags


# ============================================================================
# EXECUTION
# ============================================================================

# Generar DAGs automáticamente
generated_dags = discover_and_create_dags()

# Log summary
if generated_dags:
    logger.info(f"📊 DAGs disponibles en Airflow: {list(generated_dags.keys())}")

# Exponer DAGs al namespace de Airflow
globals().update(generated_dags)
