# Exportación de Logs - Data Pipeline Framework

El módulo de exportación de logs (`data-framework export-logs`) permite extraer la información de auditoría almacenada en la base de datos a archivos CSV estructurados.

Esta funcionalidad es esencial para:
*   Generar reportes históricos sin conocimientos de SQL.
*   Alimentar herramientas de BI (Excel, PowerBI, Tableau).
*   Archivar resultados de auditoría para cumplimiento normativo.

---

## Diferencias de Ejecución: CLI vs Airflow

Es importante distinguir cómo se genera esta información dependiendo del entorno de ejecución:

### 1. Ejecución Manual (CLI)
Cuando ejecutas un pipeline manualmente con `run pipeline`, **los logs NO se exportan automáticamente a archivos CSV**.
*   Los datos se guardan en la base de datos PostgreSQL (`pipeline.executions`, etc.).
*   Debes ejecutar explícitamente el comando `export-logs` post-ejecución si requieres los archivos.
*   **Comportamiento**: Exporta **todo el historial** de ejecuciones del pipeline a la carpeta `artifacts/<PipelineName>/exports/<timestamp>`.

```bash
# Paso 1: Ejecutar pipeline (guarda en DB)
data-framework run pipeline -c config.yml

# Paso 2: Exportar logs a CSV (opcional)
data-framework export-logs -n MyPipeline
```

### 2. Ejecución Orquestada (Airflow)
En el entorno de Airflow, el DAG incluye una tarea dedicada `5_export_logs` al final.
*   Esta tarea se ejecuta **automáticamente** en cada corrida.
*   Utiliza el archivo de estado `data/pipeline_state/context.json` para identificar la ejecución actual.
*   **Comportamiento**: Exporta **únicamente los logs de la ejecución actual** (scoped export).
*   **Ubicación**: Los archivos se guardan en `artifacts/<PipelineName>/executions/<Current_Execution_ID>/logs`.

---

## Uso del Comando

```bash
data-framework export-logs -n <pipeline_name> [-o <output_dir>]
```

**Parámetros:**
*   `-n, --name` (Requerido): Nombre del pipeline a consultar.
*   `-o, --output` (Opcional): Directorio de destino con los CSV resultantes (por defecto: `logs/`).

**Ejemplo:**
```bash
data-framework export-logs -n CustomerTransactionPipeline -o analysis/2026-Q1
```

---

## Archivos Generados

Al ejecutar el comando, se generan 6 archivos CSV con prefijo `{PipelineName}_{Timestamp}`:

### 1. Resumen de Ejecuciones (`_executions_summary.csv`)
Visión general de cada corrida del pipeline.
*   **Identificación:** `execution_id`, `pipeline_name`, `environment`
*   **Tiempo:** `start_time`, `end_time`, `duration_seconds`
*   **Estado:** `status` (completed, failed), `execution_type`
*   **Métricas:** `records_processed`, `records_failed`, `quality_score` (0-100)
*   **Salud:** `health_status`, `total_errors`, `total_warnings`

### 2. Rendimiento por Etapa (`_stages_performance.csv`)
Detalle granular de cada etapa (`INGESTION`, `VALIDATION`, `TRANSFORMATION`, `OUTPUT`).
*   **Clave:** `execution_id`, `stage_name`, `stage_order`
*   **Métricas:** `duration_seconds`, `records_in`, `records_out`
*   **Recursos:** `memory_usage_mb`, `cpu_usage_percent`
*   **Calidad:** `success_rate_pct`, `error_count`

### 3. Calidad de Datos (`_validation_quality.csv`)
Resultados agregados por suite de validación (Great Expectations).
*   **Contexto:** `execution_id`, `suite_name`, `dataset_name`
*   **Resultados:** `total_validations`, `passed_validations`, `failed_validations`
*   **Score:** `suite_quality_score`, `failure_rate_pct`

### 4. Fallos de Validación (`_validation_failures.csv`)
Detalle fila por fila de las reglas que fallaron. Útil para análisis de causa raíz.
*   **Regla:** `rule_name`, `rule_type`, `expectation_type`
*   **Impacto:** `severity` (critical, error, warning), `failed_count`, `total_records`
*   **Detalle:** `failure_details` (JSON con valores incorrectos)

### 5. Línea de Tiempo (`_timeline.csv`)
Secuencia cronológica simplificada para visualización de tendencias.
*   **Columnas:** `start_time`, `duration_seconds`, `status`, `quality_score`, `total_errors`, `records_processed`, `records_failed`

### 6. Análisis de Errores (`_errors_analysis.csv`)
Consolidado de errores técnicos y de negocio. Solo incluye ejecuciones con problemas.
*   **Nivel Ejecución:** `execution_error`, `total_errors`
*   **Nivel Stage:** `stage_name`, `stage_errors`, `stage_error_details` (JSON)

---

## Flujos de Trabajo Comunes

### Análisis de Tendencias en Excel
1.  Exportar logs: `data-framework export-logs -n MiPipeline`
2.  Abrir `_executions_summary.csv`.
3.  Crear gráfico de línea con `start_time` en Eje X y `duration_seconds` en Eje Y para ver degradación de rendimiento.
4.  Crear gráfico de línea con `quality_score` para monitorear la salud de los datos.

### Auditoría de Fallos Críticos
1.  Abrir `_validation_failures.csv`.
2.  Filtrar por `severity = 'critical'`.
3.  Revisar la columna `failure_details` para entender qué datos específicos están rompiendo las reglas de negocio.
