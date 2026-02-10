# Arquitectura

## Visión General del Sistema

Framework modular de data pipeline para ingestar, validar, transformar y exportar datos con auditoría integral y monitoreo de calidad.

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

## Componentes Principales

### 1. Pipeline Executor
**Archivo:** `src/pipeline_executor.py`

Orquesta la ejecución del pipeline a través de cuatro stages:
- **INGESTION**: Carga datos de múltiples fuentes
- **VALIDATION**: Verificaciones de calidad y escaneo de seguridad
- **TRANSFORMATION**: Limpieza y enriquecimiento de datos
- **OUTPUT**: Exporta a múltiples formatos

Responsabilidades:
- Coordinación de stages
- Manejo de errores
- Recolección de métricas
- Integración con auditoría

### 2. Ingestion Module
**Path:** `src/modules/ingestion/`

Cargador de datos multi-origen con conectores para:
- CSV (`csv_connector.py`)
- JSON (`json_connector.py`)
- PostgreSQL (`postgres_connector.py`)
- Generación de datos sintéticos (`synthetic_generator.py`)

Capacidades:
- Detección automática de schema
- Carga incremental
- Muestreo de datos
- Conversión de formatos

### 3. Validation Module
**Path:** `src/modules/validation/`

Sistema de validación production-ready:
- 40+ tipos semánticos (uuid, email, phone, credit_card)
- Patrones de seguridad OWASP Top 10+ (SQL injection, XSS, command injection)
- Verificaciones de cumplimiento (GDPR, PCI-DSS, HIPAA)
- Detección de outliers estadísticos
- Validación cross-field

Implementación:
- `ge_validator.py`: Integración con Great Expectations
- `pandera_validator.py`: Validación de schema

### 4. Transformation Module
**Path:** `src/modules/transformation/`

Motor de transformación de datos que soporta:
- Filtrado (condiciones tipo SQL)
- Columnas derivadas (expresiones Python)
- Conversiones de tipo
- Operaciones de string
- Agregaciones

### 5. Audit System
**Path:** `src/modules/auditing/`

**Archivo:** `audit_manager.py`

Sistema de auditoría basado en PostgreSQL con 6 tablas:
1. `pipelines` - Registro de pipelines
2. `executions` - Historial de ejecuciones
3. `validation_summary` - Métricas de validación agregadas
4. `validation_results` - Resultados de validación detallados
5. `stage_executions` - Seguimiento granular de stages
6. `audit_logs` - Logs de eventos detallados

Características:
- Recolección de métricas en tiempo real
- Seguimiento de health status (healthy/warning/critical/failed)
- Reportes ejecutivos y técnicos
- Seguimiento de rendimiento a nivel de stage

Ver [AUDIT_SYSTEM.md](AUDIT_SYSTEM.md) para detalles.

### 6. Monitoring Collector
**Path:** `src/modules/monitoring/`

**Archivo:** `collector.py`

Agregación de métricas en memoria:
- Seguimiento de duración de stages
- Contadores de registros (input/output/failed)
- Recolección de errores y advertencias
- Cálculo de quality score

### 7. Reporting Module
**Path:** `src/modules/reporting/`

**Archivo:** `html_generator.py`

Genera reportes HTML profesionales:
- Resumen ejecutivo (status, duración, quality score)
- Resultados de validación por suite
- Tabla de validaciones fallidas con severidad
- Métricas de rendimiento de stages
- Vulnerabilidades de seguridad detectadas

## Flujo de Datos

### Flujo de Ejecución

```
1. Cargar Config YAML
   ↓
2. Inicializar Pipeline
   - Registrar en DB de auditoría
   - Crear registro de ejecución
   - Iniciar monitoreo
   ↓
3. Stage INGESTION
   - Cargar datos de fuentes
   - Seguimiento: duración, registros cargados
   ↓
4. Stage VALIDATION
   - Ejecutar suites de validación
   - Cálculo de score
   - Escaneo de seguridad
   - Seguimiento: quality_score, validaciones passed/failed
   ↓
5. Stage TRANSFORMATION
   - Aplicar filtros y transformaciones
   - Seguimiento: registros in/out
   ↓
6. Stage OUTPUT
   - Exportar a destinos
   - Seguimiento: duración, bytes escritos
   ↓
7. Finalizar
   - Actualizar status de ejecución
   - Generar reportes
   - Cerrar registros de auditoría
```

### Flujo de Datos de Auditoría

```
Pipeline Execution
   │
   ├─▶ Start Execution
   │     └─▶ INSERT INTO executions
   │
   ├─▶ For Each Stage
   │     ├─▶ start_stage()
   │     │     └─▶ INSERT INTO stage_executions
   │     │
   │     └─▶ complete_stage()
   │           └─▶ UPDATE stage_executions
   │                 (duration, records, metrics)
   │
   ├─▶ Validation Results
   │     ├─▶ INSERT INTO validation_results (detallado)
   │     └─▶ INSERT INTO validation_summary (agregado)
   │
   └─▶ Complete Execution
         └─▶ UPDATE executions
               (status, health_status, quality_score)
```

## Schema de Base de Datos

### Schema: `pipeline`

**Tablas:**
- `pipelines` - Definiciones de pipelines
- `executions` - Historial de ejecuciones con métricas de calidad
- `validation_summary` - Métricas de validación agregadas por ejecución
- `validation_results` - Resultados de validación detallados
- `stage_executions` - Seguimiento de rendimiento a nivel de stage
- `audit_logs` - Logs de eventos

Relaciones clave:
```
pipelines (1) ──< (N) executions
executions (1) ──< (N) validation_results
executions (1) ──< (1) validation_summary
executions (1) ──< (N) stage_executions
```

Ver [DATABASE_SCHEMA.md](DATABASE.md#schema-de-base-de-datos) para schema completo.

## Configuración

### Estructura YAML

```yaml
pipeline:
  name: string              # Identificador de pipeline
  description: string       # Descripción de propósito

ingestion:
  sources:                  # Lista de fuentes de datos
    - name: string
      type: csv|json|postgres
      path: string
      output_dataset: string

validation:
  great_expectations:       # Suites de validación
    - dataset: string
      suites:
        - name: string
          expectations: []

transformation:
  steps:                    # Pipeline de transformación
    - name: string
      dataset: string
      output_dataset: string
      operations: []

outputs:                    # Destinos de exportación
  - name: string
    type: csv|excel|postgres|parquet
    dataset: string
    path: string
```

Ver [examples/complete_pipeline.yml](../examples/complete_pipeline.yml) para ejemplo completo.

## Arquitectura CLI

**Punto de entrada:** `src/cli.py`

Comandos:
- `run pipeline` - Ejecutar pipeline desde YAML
- `export-logs` - Exportar métricas de auditoría a CSV
- `infect` - Inyectar vulnerabilidades para testing

Ver [CLI_REFERENCE.md](CLI_REFERENCE.md) para detalles de comandos.

## Puntos de Extensión

### Agregar Nuevos Conectores

1. Crear conector en `src/modules/ingestion/connectors/`
2. Implementar interfaz `BaseConnector`
3. Registrar en `multi_source_loader.py`

### Agregar Tipos de Validación

1. Definir tipo en config de validación
2. Agregar patrones en `ge_validator.py`
3. Actualizar [VALIDATION_SYSTEM.md](VALIDATION_SYSTEM.md)

### Agregar Operaciones de Transformación

1. Implementar operación en `transformer.py`
2. Agregar tipo de operación al schema de config
3. Actualizar documentación

## Características de Rendimiento

**Ejecución Típica (10K registros):**
- INGESTION: 0.3-0.6s
- VALIDATION: 1.5-12s (depende de cantidad de suites)
- TRANSFORMATION: 0.1-0.5s
- OUTPUT: 0.2-0.5s

**Cuellos de botella:**
- Stage VALIDATION consume típicamente 80-95% del tiempo de ejecución
- Operaciones de Great Expectations son intensivas en cómputo
- Escrituras de auditoría PostgreSQL son asíncronas

**Optimización:**
- Reducir cantidad de suites de validación
- Usar muestreo para datasets grandes
- Habilitar modo batch para escrituras de auditoría
- Indexar columnas de auditoría consultadas frecuentemente

## Consideraciones de Despliegue

**Requisitos:**
- Python 3.10+
- PostgreSQL 13+ (contenedor Docker incluido)
- 512MB RAM mínimo
- 1GB espacio en disco (DB de auditoría crece con ejecuciones)

**Setup de Producción:**
1. Desplegar PostgreSQL separadamente (almacenamiento persistente)
2. Configurar conexión en `src/config.py`
3. Ejecutar `scripts/init_db.sql` para inicializar schema
4. Configurar monitoreo para crecimiento de DB de auditoría
5. Programar exportaciones de logs para análisis histórico

**Monitoreo:**
- Usar comando `export-logs` para extracción de métricas
- Monitorear tabla `stage_executions` para tendencias de rendimiento
- Configurar alertas en campo `health_status`
- Rastrear `validation_summary.quality_score` a lo largo del tiempo
