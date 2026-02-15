# Gestión de Base de Datos - Data Pipeline Framework

Guía completa para administrar la base de datos `data_framework` utilizando las herramientas del framework.

---

## 📋 Contenidos

1. [Herramienta db_utils.py](#herramienta-db_utilspy)
2. [Comandos INFO](#comandos-info---consultar-información)
3. [Comandos MAINTENANCE](#comandos-maintenance---mantenimiento)
4. [Esquema de Base de Datos](#esquema-de-base-de-datos)
5. [Troubleshooting](#troubleshooting)

> **Nota:** Para consultas SQL avanzadas y reportes manuales, ver [SQL_RECIPES.md](SQL_RECIPES.md).

---

## Herramienta db_utils.py

**Ubicación:** `scripts/db_utils.py`

Herramienta de línea de comandos para administrar la base de datos del framework sin necesidad de escribir SQL manualmente.

### Sintaxis General

```bash
python scripts/db_utils.py <comando> [opciones]
```

### Ayuda

```bash
python scripts/db_utils.py --help
```

---

## Comandos INFO - Consultar Información

### `status` - Ver Estado de Tablas

Muestra el número de registros y tamaño en disco de todas las tablas en los esquemas `pipeline` y `sample_data`.

```bash
python scripts/db_utils.py status
```

**Salida Ejemplo:**
```
================================================================================
  ESTADO DE LA BASE DE DATOS - data_framework
================================================================================

  Schema: PIPELINE
  ----------------------------------------------------------------------------
  [OK]     executions                                  12 rows  |     80 kB
  [OK]     pipelines                                    1 rows  |     80 kB
  [OK]     stage_executions                            36 rows  |     80 kB
  [OK]     validation_results                          90 rows  |     80 kB
  [OK]     validation_summary                          12 rows  |     80 kB

  Schema: SAMPLE_DATA
  ----------------------------------------------------------------------------
  [OK]     customers                                1,000 rows  |   2144 kB
  [OK]     transactions                             5,000 rows  |   5120 kB
```

### `pipelines` - Listar Pipelines Registrados

Muestra la configuración y estado de los pipelines (ID, Versión, Owner, Estado).

```bash
python scripts/db_utils.py pipelines
```

### `executions` - Ver Historial

Muestra las últimas ejecuciones con sus métricas clave (duración, calidad, registros procesados).

```bash
python scripts/db_utils.py executions -l 10
```

### `validations` - Ver Resultados de Calidad

Muestra los resultados detallados de las reglas de validación (Great Expectations).

```bash
# Ver solo validaciones fallidas
python scripts/db_utils.py validations -s failed

# Ver últimas 50 validaciones
python scripts/db_utils.py validations -l 50
```

### `stats` - Métricas Globales

Resumen estadístico del uso del framework (Tasa de éxito, duración promedio, actividad diaria).

```bash
python scripts/db_utils.py stats
```

---

## Comandos MAINTENANCE - Mantenimiento

### `clean-samples` - Limpiar Datos de Prueba

Elimina los datos generados en el esquema `sample_data` (ej: customers, transactions) para liberar espacio o reiniciar pruebas. **No afecta la auditoría del pipeline.**

```bash
python scripts/db_utils.py clean-samples
```

### `clean-old` - Purgar Historial Antiguo

Elimina ejecuciones y logs de auditoría más antiguos que X días.

```bash
python scripts/db_utils.py clean-old -d 60  # Borrar > 60 días
```

### `vacuum` - Optimizar Rendimiento

Ejecuta `VACUUM ANALYZE` en PostgreSQL para recuperar espacio físico y actualizar las estadísticas del optimizador de consultas. Recomendado después de borrar muchos datos.

```bash
python scripts/db_utils.py vacuum
```

---

## Esquema de Base de Datos

El framework utiliza dos esquemas principales en la base de datos `data_framework`.

### 1. Schema: `pipeline` (Metadata y Auditoría)

Contiene toda la información operativa del framework. Estas tablas se crean automáticamente con `scripts/init_db.sql`.

| Tabla | Descripción | Columnas Clave |
|-------|-------------|----------------|
| **`pipelines`** | Catálogo de pipelines configurados. | `id`, `name`, `config`, `version`, `is_active` |
| **`executions`** | Historial de cada ejecución. | `id`, `status`, `duration_seconds`, `quality_score`, `metrics` |
| **`stage_executions`** | Detalle granular por etapa. | `execution_id`, `stage_name`, `status`, `duration_seconds` |
| **`validation_results`** | Resultados fila por fila. | `execution_id`, `rule_name`, `passed`, `failed_count`, `severity` |
| **`validation_summary`** | Resumen agregado por suite. | `execution_id`, `suite_name`, `passed_validations`, `quality_score` |
| **`audit_logs`** | Logs de eventos del sistema. | `timestamp`, `level`, `module`, `event`, `context` |

### 2. Schema: `sample_data` (Datos de Prueba)

Contiene los datos sintéticos generados por `scripts/generate_sample_data.py`.

#### Tabla: `customers`
Datos maestros de clientes.
*   **Identidad:** `customer_id`, `name`, `email`, `phone`, `address`, `postal_code`
*   **Actividad:** `registration_date`, `last_login`, `account_status`, `lifetime_value`, `user_comment`
*   **Técnico:** `website`, `ip_address`, `credit_card_last4`, `age`

#### Tabla: `transactions`
Datos transaccionales vinculados a clientes.
*   **Detalle:** `transaction_id`, `customer_id`, `timestamp`, `amount`, `description`
*   **Clasificación:** `category`, `status`, `merchant_name`, `payment_method`
*   **Técnico:** `ip_address`

> **Nota:** Si se ejecuta el módulo de infección de datos, aparecerán tablas adicionales como `customers_infected` y `transactions_infected` con la misma estructura pero conteniendo datos maliciosos.

---

## Troubleshooting

### No puedo conectarme a la base de datos

*   **Error**: `Connection refused` o `psycopg2.OperationalError`.
*   **Solución**:
    1.  Verificar que el contenedor Docker esté corriendo: `docker ps`.
    2.  Si conectas desde el **host** (tu máquina, DBeaver, PowerBI), usa el puerto **5433**.
    3.  Si conectas desde **dentro de Docker** (otros contenedores), usa el puerto **5432**.

### Las tablas están vacías

*   **Síntoma**: `[EMPTY]` en el comando `status`.
*   **Causa**: El pipeline no se ha ejecutado o falló antes de escribir.
*   **Solución**:
    1.  Ejecuta el pipeline: `data-framework run pipeline`.
    2.  Verifica logs: `docker logs framework_postgres`.

### Error "relation does not exist"

*   **Síntoma**: `psycopg2.errors.UndefinedTable`.
*   **Causa**: La base de datos no se ha inicializado o el esquema no existe.
*   **Solución**: Ejecuta el script de inicialización (es idempotente).
    ```bash
    # Usando Docker
    docker exec -i framework_postgres psql -U admin -d data_framework < scripts/init_db.sql
    ```
