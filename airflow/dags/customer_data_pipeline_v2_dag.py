
"""
DAG generado automáticamente por Data Pipeline Framework v2.0
Pipeline: CustomerDataPipeline
Descripción: Pipeline de procesamiento de datos de clientes con validación de seguridad
Generado: 2026-02-04 00:20:47.754318

ARQUITECTURA v2.0 (3 ETAPAS):
  1. Ingestion: Carga datos (limpios o pre-infectados)
  2. Validation: Valida calidad + detecta ataques OWASP Top 10
  3. Transformation: Limpia y transforma

NOTA: Para testing de seguridad, infectar datos ANTES del pipeline:
      python -m src.cli infect -c examples/infection_config.yml
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
from pathlib import Path

# Agregar src al path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline_executor import PipelineExecutor


# ============================================
# DEFAULT ARGS
# ============================================
default_args = {

    'owner': 'data_team',

    'depends_on_past': False,

    'email_on_failure': True,

    'email_on_retry': False,

    'retries': 2,

    'retry_delay': 0:05:00,

    'start_date': days_ago(1),
}


# ============================================
# FUNCIONES DE TAREAS - ARQUITECTURA v2.0
# ============================================

def run_pipeline(**context):
    """
    Ejecutar pipeline completo (3 etapas: Ingestion → Validation → Transformation)
    
    Este task ejecuta el pipeline completo usando PipelineExecutor.execute()
    que ya implementa las 3 etapas de la arquitectura v2.0.
    """
    executor = PipelineExecutor.load('CustomerDataPipeline')
    
    if not executor:
        raise ValueError(f"Pipeline 'CustomerDataPipeline' not found in database")
    
    # Ejecutar pipeline completo
    result = executor.execute()
    
    # Guardar resultados en XCom
    context['task_instance'].xcom_push(key='execution_id', value=result.execution_id)
    context['task_instance'].xcom_push(key='status', value=result.status)
    context['task_instance'].xcom_push(key='records_processed', value=result.records_processed)
    context['task_instance'].xcom_push(key='records_failed', value=result.records_failed)
    context['task_instance'].xcom_push(key='duration_seconds', value=result.duration_seconds)
    
    # Fallar si el pipeline no completó exitosamente
    if result.status != 'completed':
        raise RuntimeError(f"Pipeline failed with status: {result.status}")
    
    return {
        'execution_id': result.execution_id,
        'status': result.status,
        'records_processed': result.records_processed,
        'records_failed': result.records_failed,
        'duration_seconds': result.duration_seconds
    }


# ============================================
# DAG DEFINITION
# ============================================
with DAG(
    dag_id='customer_data_pipeline_v2',
    default_args=default_args,
    description='Pipeline de procesamiento de datos de clientes con validación de seguridad',
    schedule_interval='@daily',
    catchup=False,
    tags=['customers', 'v2.0', 'production'],
) as dag:
    
    # Task único que ejecuta el pipeline completo (3 etapas)
    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
        doc_md="""
        ### Pipeline Executor (3 Etapas)
        
        Este task ejecuta el pipeline completo:
        1. **Ingestion**: Carga datos desde fuentes configuradas
        2. **Validation**: Valida calidad y detecta ataques OWASP Top 10
        3. **Transformation**: Aplica transformaciones configuradas
        
        **Resultados disponibles en XCom**:
        - `execution_id`: UUID de la ejecución
        - `status`: Estado final (completed/failed)
        - `records_processed`: Registros procesados exitosamente
        - `records_failed`: Registros con errores
        - `duration_seconds`: Duración total en segundos
        
        **Ver detalles en la base de datos**:
        ```sql
        SELECT * FROM pipeline.executions 
        WHERE execution_id = '<execution_id>';
        
        SELECT * FROM validation.validation_results
        WHERE execution_id = '<execution_id>';
        ```
        """
    )
    
    # El pipeline se ejecuta como un solo task
    # Para mayor granularidad, consultar los logs en pipeline.audit_logs