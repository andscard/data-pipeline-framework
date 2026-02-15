# Testing de Seguridad y Caos

El Data Pipeline Framework incluye un módulo avanzado de **Ingeniería del Caos y Seguridad** diseñado para probar la resiliencia de los pipelines y la efectividad de las reglas de validación.

En lugar de esperar a que ocurran errores reales o ataques, este módulo (`data-framework infect`) permite inyectar proactivamente vulnerabilidades, datos corruptos y patrones maliciosos en los datasets de prueba.

---

## Concepto: "Data Infection"

El proceso de "infección de datos" sigue estos principios:
1.  **Datos Limpios como Base:** Se parte de datos sintéticos válidos (`sample_data`).
2.  **Inyección Controlada:** Se aplican transformaciones maliciosas configurables (ataques).
3.  **Verificación de Defensa:** Se ejecuta el pipeline normal para asegurar que las reglas de calidad (`great_expectations`) detecten y bloqueen los datos infectados.

---

## Catálogo de Ataques

El framework implementa ataques basados en **OWASP Top 10** y problemas comunes de calidad de datos. Los ataques se definen en `src/modules/data_infection/attack_types.py`.

### 1. Inyección (Injection Attacks)

Simula intentos de explotación de vulnerabilidades en sistemas downstream (bases de datos, logs, web).

| Ataque | Tipo (`type`) | Descripción | Ejemplo Inyectado |
|--------|---------------|-------------|-------------------|
| **Generic Injection** | `injection` | Inyecta un mix aleatorio de SQL, NoSQL y Command Injection. | `' OR '1'='1`, `; rm -rf /` |
| **XSS / HTML** | `xss` | Inyecta scripts y tags HTML peligrosos. | `<script>alert(1)</script>` |
| **Path Traversal** | `path_traversal` | Intentos de acceder a archivos del sistema. | `../../../etc/passwd` |

### 2. Privacidad y Fugas (Data Leakage)

Verifica si el sistema detecta información sensible expuesta incorrectamente (PII, secretos).

| Ataque | Tipo (`type`) | Descripción | Ejemplo |
|--------|---------------|-------------|---------|
| **Data Leakage** | `data_leakage` | Fuga genérica de información sensible (PII, Credenciales). | `SSN: 123...`, `API_KEY: sk...` |

### 3. Redes y SSRF

Simula peticiones maliciosas que intentan acceder a recursos internos.

| Ataque | Tipo (`type`) | Descripción | Ejemplo |
|--------|---------------|-------------|---------|
| **SSRF** | `ssrf` | URLs apuntando a IPs privadas o metadata cloud. | `http://169.254.169.254/latest` |

### 4. Integridad y Lógica de Negocio

Problemas de calidad que afectan la lógica del negocio o reglas relacionales.

| Ataque | Tipo (`type`) | Descripción | Ejemplo |
|--------|---------------|-------------|---------|
| **Data Poisoning** | `data_poisoning` | Valores estadísticamente válidos pero maliciosos. | Precios alterados sutilmente |
| **Negative Amounts** | `negative_value` | Valores negativos en campos estrictamente positivos. | Saldo: `-100.50` |
| **Orphaned References** | `orphaned_reference` | IDs que no existen en tablas maestras. | `NONEXISTENT-UUID-1234` |
| **Cross-field Violation** | `cross_field_violation` | Incoherencia lógica entre dos columnas. | `registro` (2025) > `login` (2020) |

### 5. Manipulación de Esquema y Tipos

Prueba la robustez del pipeline ante cambios inesperados en la estructura de datos.

| Ataque | Tipo (`type`) | Descripción | Ejemplo |
|--------|---------------|-------------|---------|
| **Schema Manipulation** | `schema_manipulation` | Cambia el tipo de dato de una columna (ej: número a string). | `"INVALID"` en campo numérico |
| **Null Injection** | `missing_data` | Introduce valores nulos en campos obligatorios. | `NULL` / `None` |
| **Format Corruption** | `format_corruption` | Rompe formatos específicos con bytes nulos o basura. | `\x00\x00`, `` |
| **Inconsistency** | `inconsistency` | Variaciones de formato en el mismo campo. | `2024-01-01` vs `01/01/24` |

### 6. Anomalías Estadísticas y Temporales

| Ataque | Tipo (`type`) | Descripción | Ejemplo |
|--------|---------------|-------------|---------|
| **Outliers** | `outliers` | Valores extremos (100x desviación estándar). | Edad: `250` |
| **Duplicates** | `duplicates` | Duplicación exacta de filas. | Filas idénticas |
| **Timing / Temporal** | `timing` | Timestamps en el futuro lejano o pasado. | `2099-12-31` |


---

## Configuración de Ataques

La infección se controla mediante un archivo YAML (ej: `examples/infection_config.yml`).

### Estructura del Archivo

```yaml
# Configuración Global
global:
  default_infection_rate: 0.10  # 10% de registros afectados por defecto
  mode: "mixed"                 # "mixed" combina múltiples ataques

# Destinos (dónde guardar los datos infectados)
output_targets:
  csv:
    enabled: true
    path: "data/samples/"
  postgres:
    enabled: true
    schema: "sample_data"

# Definición de Datasets
datasets:
  customers:
    input: "data/samples/customers.csv"
    output:
      csv_file: "customers_infected.csv"
      postgres_table: "customers_infected"
    
    # Lista de Ataques
    attacks:
      - name: "Random Injection Mix"
        type: "injection"
        target_columns: ["name", "address"]
        rate: 0.05

      - name: "Format Corruption"
        type: "format_corruption"
        target_columns: ["email"]
        rate: 0.10

      - name: "Edad Negativa"
        type: "negative_value"
        target_columns: ["age"]
        rate: 0.02
```

---

## Ejecución

### Comando CLI

Para ejecutar la infección y generar los datos corruptos:

```bash
data-framework infect -c examples/infection_config.yml
```

### Opciones

*   `-c, --config`: Ruta al archivo YAML de configuración (Requerido).
*   `--dry-run`: Simula la infección y muestra qué ataques se aplicarían sin modificar archivos.

### Salida del Proceso

El comando genera:

1.  **Archivos de Datos Infectados:** CSVs o tablas en PostgreSQL (ej: `customers_infected.csv`) listos para ser consumidos por el pipeline.
2.  **Reporte de Infección (JSON):** Un archivo detallado (`infection_report.json`) con métricas de qué filas fueron modificadas y cómo.

---

## Flujo de Trabajo Recomendado

1.  **Generar Datos Limpios:**
    ```bash
    python scripts/generate_sample_data.py
    ```
2.  **Infectar Datos (Chaos Engineering):**
    ```bash
    data-framework infect -c examples/infection_config.yml
    ```
3.  **Ejecutar Pipeline:**
    Configura tu pipeline para leer los datasets infectados (ej: `data/samples/customers_infected.csv`).
    ```bash
    data-framework run pipeline -c examples/pipelines/security_test_pipeline.yml
    ```
4.  **Verificar Detección:**
    Consulta los reportes de validación para confirmar que el sistema bloqueó los ataques.
    ```bash
    data-framework export-logs -n MyPipeline
    # Revisar validation_failures.csv
    ```
