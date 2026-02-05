# Framework Modular de Validación de Datos - Capa de Abstracción

## 🎯 Resumen

Esta capa de abstracción simplifica la configuración de validaciones de datos en pipelines, reduciendo **~80-90% del código necesario** mientras mantiene toda la potencia de Great Expectations.

### ¿Por qué usar esta abstracción?

**ANTES (Great Expectations directo):**
```yaml
- expectation_type: "expect_column_values_to_not_be_null"
  column: "customer_id"
- expectation_type: "expect_column_values_to_be_unique"
  column: "customer_id"
- expectation_type: "expect_column_values_to_match_regex"
  column: "customer_id"
  regex: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
```

**DESPUÉS (con abstracción):**
```yaml
customer_id:
  type: uuid
  required: true
  unique: true
```

---

## 📦 Instalación

### Requisitos

- Python 3.8+
- Great Expectations
- PyYAML

### Instalación de dependencias

```bash
pip install great-expectations pyyaml pandas
```

### Estructura de archivos

```
proyecto/
├── validation/
│   ├── __init__.py
│   ├── type_mappings.py          # Mapeo de tipos semánticos
│   └── validation_mapper.py      # Mapper principal
├── examples/
│   ├── simplified_pipeline.yml   # Ejemplo de configuración
│   └── complete_pipeline.yml     # Configuración verbosa (comparación)
├── example_usage.py              # Scripts de ejemplo
├── DOCUMENTATION.md              # Documentación completa
└── README.md                     # Este archivo
```

---

## 🚀 Inicio Rápido

### 1. Crear configuración simplificada

Crea un archivo `my_pipeline.yml`:

```yaml
validation:
  default_level: strict
  security:
    enabled: true
    checks:
      sql_injection: true
      xss: true
  
  schemas:
    customers:
      table_rules:
        row_count:
          min: 1
          max: 1000000
      
      columns:
        customer_id:
          type: uuid
          required: true
          unique: true
        
        name:
          type: person_name
          required: true
        
        email:
          type: email
          required: true
          unique: 0.95
        
        phone:
          type: phone_es
          nullable: 0.1
        
        status:
          type: enum
          values: [active, suspended, inactive]
          required: true
        
        lifetime_value:
          type: currency
          min: 0
          max: 100000
          mean_range: [1000, 50000]
      
      business_rules:
        - name: "minimum_active_customers"
          description: "Al menos 50% activos"
          metric: percentage_where
          column: status
          value: active
          threshold: ">= 50%"
          severity: critical
```

### 2. Usar el mapper

```python
from validation.validation_mapper import ValidationMapper
import yaml

# Cargar configuración
with open('my_pipeline.yml', 'r') as f:
    config = yaml.safe_load(f)

# Crear mapper
mapper = ValidationMapper(config)

# Generar suite de Great Expectations
suite = mapper.generate_great_expectations_suite('customers')

# Usar con Great Expectations
import great_expectations as gx

context = gx.get_context()
context.add_expectation_suite(
    expectation_suite_name=suite['expectation_suite_name'],
    expectations=suite['expectations']
)

# Validar datos
batch = context.get_batch(
    datasource_name="my_datasource",
    data_asset_name="customers"
)

results = context.run_validation_operator(
    "action_list_operator",
    assets_to_validate=[batch],
    run_id="manual_run"
)
```

### 3. Ejecutar ejemplos

```bash
python example_usage.py
```

---

## 📚 Tipos de Datos Disponibles

### Identificadores
- `uuid` - UUID v4 con validaciones automáticas
- `id` - Identificador numérico único

### Datos Personales
- `person_name` - Nombre con anti-injection
- `email` - Email con formato y unicidad
- `phone_es` - Teléfono español (+34)
- `phone_international` - Teléfono internacional

### Texto
- `text` - Texto genérico
- `text_short` - Texto corto (≤255 chars)
- `text_long` - Texto largo

### Numéricos
- `integer` - Entero
- `float` - Decimal
- `currency` - Valor monetario (≥0)
- `percentage` - Porcentaje (0-100)

### Temporales
- `datetime` - Fecha y hora
- `date` - Solo fecha
- `timestamp` - Unix timestamp

### Categóricos
- `enum` - Conjunto predefinido de valores
- `boolean` - Booleano

### Especiales
- `url` - URL válida
- `ip_address` - IP v4
- `json` - JSON parseable

**Ver [DOCUMENTATION.md](DOCUMENTATION.md) para detalles completos.**

---

## 🔒 Validaciones de Seguridad

Activa validaciones de seguridad globalmente:

```yaml
validation:
  security:
    enabled: true
    checks:
      sql_injection: true           # Detecta SQL injection
      xss: true                     # Detecta XSS
      nosql_injection: true         # Detecta NoSQL injection
      command_injection: true       # Detecta command injection
      path_traversal: true          # Detecta path traversal
      sensitive_data_exposure: true # Detecta tarjetas, API keys
      ssn_exposure: true            # Detecta SSN/DNI
```

Las validaciones se aplican automáticamente a columnas de texto.

---

## 📊 Reglas de Negocio

### Porcentaje de filas

```yaml
business_rules:
  - name: "minimum_active"
    metric: percentage_where
    column: status
    value: active
    threshold: ">= 50%"
```

### Condicionales

```yaml
business_rules:
  - name: "high_value_active"
    when: "lifetime_value > 20000"
    expect:
      status: [active]
```

### Validaciones temporales

```yaml
business_rules:
  - name: "registration_before_login"
    condition: "registration_date <= last_login"
    applies_to_rows: "last_login IS NOT NULL"
```

---

## 🎨 Características Avanzadas

### Templates Predefinidos

Usa templates para configuraciones comunes:

```yaml
validation:
  schemas:
    my_customers:
      template: "customer_data_standard_v1"
      
      custom_columns:
        loyalty_points:
          type: integer
          min: 0
```

Templates disponibles:
- `customer_data_standard_v1`
- `transaction_data_standard_v1`

### Detección de Outliers

```yaml
lifetime_value:
  type: currency
  outlier_detection:
    enabled: true
    method: quantile
    quantiles: [0.01, 0.99]
    expected_ranges:
      - [0, 200]
      - [8000, 12000]
```

### Niveles de Validación

```yaml
validation:
  default_level: strict  # basic | standard | strict | paranoid
```

| Nivel | Incluye |
|-------|---------|
| `basic` | Tipos, nulls |
| `standard` | + Formatos, rangos |
| `strict` | + Reglas de negocio, outliers |
| `paranoid` | + Todas las validaciones de seguridad |

---

## 📈 Beneficios

### ✅ Reducción de Código
- **~80-90% menos líneas** de configuración
- Configuraciones más legibles y mantenibles

### ✅ Menos Errores
- No necesitas conocer la sintaxis exacta de Great Expectations
- Tipos semánticos previenen configuraciones incorrectas

### ✅ Mejor Productividad
- Configuración más rápida
- Menos tiempo depurando sintaxis

### ✅ Estandarización
- Templates reutilizables
- Nomenclatura consistente

### ✅ Seguridad Integrada
- Validaciones de seguridad automáticas
- Detección de injection attacks

---

## 🔧 Integración con Pipelines

### Con Apache Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from validation.validation_mapper import ValidationMapper

def validate_data(**context):
    mapper = ValidationMapper(pipeline_config)
    suite = mapper.generate_great_expectations_suite('customers')
    # Ejecutar validación...
    
with DAG('data_validation_dag') as dag:
    validate_task = PythonOperator(
        task_id='validate_customers',
        python_callable=validate_data
    )
```

### Con Databricks

```python
# En notebook de Databricks
%pip install great-expectations pyyaml

from validation.validation_mapper import ValidationMapper
import yaml

# Cargar config
config = yaml.safe_load(dbutils.fs.head("/configs/pipeline.yml"))

# Generar validaciones
mapper = ValidationMapper(config)
suite = mapper.generate_great_expectations_suite('customers')

# Validar DataFrame de Spark
df = spark.table("raw_customers")
# ... aplicar validaciones
```

---

## 🧪 Testing

Ejecuta los tests de ejemplo:

```bash
python example_usage.py
```

Esto ejecutará 6 ejemplos que demuestran:
1. Uso básico
2. Reglas de negocio
3. Validaciones de seguridad
4. Carga desde YAML
5. Comparación de verbosidad
6. Tipos disponibles

---

## 📖 Documentación Adicional

- **[DOCUMENTATION.md](DOCUMENTATION.md)** - Documentación completa de tipos y características
- **[simplified_pipeline.yml](examples/simplified_pipeline.yml)** - Ejemplo completo de configuración
- **[complete_pipeline.yml](examples/complete_pipeline.yml)** - Configuración verbosa (para comparar)

---

## 🤝 Contribuir

### Agregar nuevos tipos de datos

Edita `validation/type_mappings.py`:

```python
TYPE_MAPPINGS['mi_nuevo_tipo'] = {
    'description': 'Descripción del tipo',
    'expectations': [
        {
            'expectation_type': 'expect_column_...',
            'kwargs': {...},
            'meta': {'description': '...'}
        }
    ]
}
```

### Agregar nuevas validaciones de seguridad

Edita `validation/type_mappings.py`:

```python
SECURITY_CHECKS['mi_check'] = {
    'description': 'Descripción del check',
    'expectation_type': 'expect_column_values_to_not_match_regex',
    'regex': r'mi_patron_regex',
    'severity': 'critical'
}
```

---

## 📝 Licencia

Este código forma parte del TFM "Framework Modular para Validación, Monitoreo y Seguridad en Pipelines de Datos".

---

## 👨‍💻 Autor

Desarrollado como parte del Trabajo de Fin de Máster en Ingeniería de Software.

---

## 🙋 FAQ

### ¿Puedo mezclar sintaxis simplificada con expectativas manuales?

Sí, usa `custom_expectations`:

```yaml
customer_id:
  type: uuid
  custom_expectations:
    - expectation_type: "mi_expectativa_personalizada"
      kwargs: {...}
```

### ¿Cómo debugging si algo falla?

El mapper genera metadata descriptiva en cada expectativa. Revisa:

```python
suite = mapper.generate_great_expectations_suite('customers')
for exp in suite['expectations']:
    print(exp['meta']['description'])
```

### ¿Funciona con Spark DataFrames?

Sí, Great Expectations soporta Spark. Genera la suite y aplícala a tu Spark DF.

### ¿Puedo exportar a formato Great Expectations nativo?

Sí, la función `generate_great_expectations_suite()` devuelve formato compatible:

```python
import json

suite = mapper.generate_great_expectations_suite('customers')
with open('suite.json', 'w') as f:
    json.dump(suite, f, indent=2)
```

---

## 📞 Soporte

Para preguntas o issues, consulta la documentación o revisa los ejemplos en `example_usage.py`.

---

**¡Empieza a validar tus datos de forma más simple y efectiva! 🚀**
