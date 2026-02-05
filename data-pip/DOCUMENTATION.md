# Documentación: Tipos de Datos y Validaciones

## Tabla de Contenidos
1. [Introducción](#introducción)
2. [Tipos de Datos Disponibles](#tipos-de-datos-disponibles)
3. [Modificadores de Validación](#modificadores-de-validación)
4. [Validaciones de Seguridad](#validaciones-de-seguridad)
5. [Reglas de Negocio](#reglas-de-negocio)
6. [Ejemplos Completos](#ejemplos-completos)

---

## Introducción

Este framework proporciona una capa de abstracción sobre Great Expectations que permite definir validaciones de datos usando una sintaxis simplificada y user-friendly.

En lugar de escribir:
```yaml
- expectation_type: "expect_column_values_to_not_be_null"
  column: "customer_id"
- expectation_type: "expect_column_values_to_be_unique"
  column: "customer_id"
- expectation_type: "expect_column_values_to_match_regex"
  column: "customer_id"
  regex: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
```

Simplemente escribes:
```yaml
customer_id:
  type: uuid
  required: true
  unique: true
```

---

## Tipos de Datos Disponibles

### 1. Identificadores

#### `uuid`
Identificador único universal en formato UUID v4.

**Validaciones automáticas:**
- ✓ No nulo
- ✓ Único
- ✓ Formato regex UUID válido

**Ejemplo:**
```yaml
customer_id:
  type: uuid
  required: true
  unique: true
```

#### `id`
Identificador numérico entero.

**Validaciones automáticas:**
- ✓ No nulo
- ✓ Único
- ✓ Tipo entero

**Ejemplo:**
```yaml
order_id:
  type: id
  required: true
```

---

### 2. Datos Personales

#### `person_name`
Nombre de persona con validaciones anti-injection.

**Validaciones automáticas:**
- ✓ No nulo
- ✓ Longitud entre 3-100 caracteres
- ✓ Sin SQL injection / XSS
- ✓ Sin datos sensibles expuestos

**Ejemplo:**
```yaml
full_name:
  type: person_name
  required: true
  min_length: 2    # Sobrescribe el default de 3
  max_length: 150  # Sobrescribe el default de 100
```

#### `email`
Dirección de correo electrónico.

**Validaciones automáticas:**
- ✓ No nulo
- ✓ Formato email válido (regex)
- ✓ Longitud 5-100 caracteres
- ✓ Sin emails de testing (test@, dummy@, etc.)
- ✓ Sin NoSQL injection

**Ejemplo:**
```yaml
email:
  type: email
  required: true
  unique: 0.95  # Al menos 95% únicos
```

#### `phone_es`
Teléfono con formato español (+34).

**Validaciones automáticas:**
- ✓ Comienza con +34 (90% de los casos)
- ✓ Sin números dummy (000, 111, 999)

**Ejemplo:**
```yaml
phone:
  type: phone_es
  nullable: 0.1  # Permite 10% nulos
```

#### `phone_international`
Teléfono internacional genérico.

**Validaciones automáticas:**
- ✓ Formato internacional válido (+X...)

**Ejemplo:**
```yaml
contact_phone:
  type: phone_international
```

---

### 3. Tipos de Texto

#### `text`
Texto genérico.

**Validaciones automáticas:**
- ✓ Tipo string

**Modificadores opcionales:**
- `min_length`: Longitud mínima
- `max_length`: Longitud máxima
- `security: true`: Aplica validaciones de seguridad

**Ejemplo:**
```yaml
address:
  type: text
  min_length: 10
  max_length: 200
  security: true  # Detecta injection attacks
```

#### `text_short`
Texto corto (máximo 255 caracteres).

**Ejemplo:**
```yaml
city:
  type: text_short
  required: true
```

#### `text_long`
Texto largo sin límite específico.

**Ejemplo:**
```yaml
description:
  type: text_long
  nullable: 0.2
```

---

### 4. Tipos Numéricos

#### `integer`
Número entero.

**Ejemplo:**
```yaml
age:
  type: integer
  min: 18
  max: 120
```

#### `float`
Número decimal.

**Ejemplo:**
```yaml
rating:
  type: float
  min: 0.0
  max: 5.0
```

#### `currency`
Valor monetario (positivo por defecto).

**Validaciones automáticas:**
- ✓ Tipo float
- ✓ Valor >= 0

**Ejemplo:**
```yaml
lifetime_value:
  type: currency
  min: 0
  max: 100000
  mean_range: [1000, 50000]
  outlier_detection:
    enabled: true
    method: quantile
    quantiles: [0.01, 0.99]
    expected_ranges:
      - [0, 100]
      - [80000, 150000]
```

#### `percentage`
Porcentaje (0-100).

**Validaciones automáticas:**
- ✓ Tipo float
- ✓ Rango 0-100

**Ejemplo:**
```yaml
discount_rate:
  type: percentage
```

---

### 5. Tipos Temporales

#### `datetime`
Fecha y hora.

**Ejemplo:**
```yaml
created_at:
  type: datetime
  required: true
  nullable: 0.01
```

#### `date`
Solo fecha (sin hora).

**Ejemplo:**
```yaml
birth_date:
  type: date
  required: true
```

#### `timestamp`
Timestamp Unix (epoch).

**Validaciones automáticas:**
- ✓ Tipo entero
- ✓ Rango válido (0 a 2147483647)

**Ejemplo:**
```yaml
event_timestamp:
  type: timestamp
```

---

### 6. Tipos Categóricos

#### `enum`
Valor de un conjunto predefinido.

**Requiere:** `values` (lista de valores permitidos)

**Ejemplo:**
```yaml
account_status:
  type: enum
  values: [active, suspended, inactive, closed]
  required: true
```

#### `boolean`
Valor booleano.

**Validaciones automáticas:**
- ✓ Solo acepta: true, false, 0, 1, 'true', 'false', etc.

**Ejemplo:**
```yaml
is_verified:
  type: boolean
  required: true
```

---

### 7. Tipos Especiales

#### `url`
URL válida.

**Validaciones automáticas:**
- ✓ Formato URL http/https

**Ejemplo:**
```yaml
website:
  type: url
  nullable: 0.3
```

#### `ip_address`
Dirección IP v4.

**Validaciones automáticas:**
- ✓ Formato IP v4 válido

**Ejemplo:**
```yaml
client_ip:
  type: ip_address
```

#### `json`
Cadena JSON válida.

**Validaciones automáticas:**
- ✓ JSON parseable

**Ejemplo:**
```yaml
metadata:
  type: json
  nullable: 0.5
```

---

## Modificadores de Validación

### Modificadores Comunes

| Modificador | Descripción | Tipos Aplicables | Ejemplo |
|------------|-------------|------------------|---------|
| `required` | Columna no puede ser nula | Todos | `required: true` |
| `unique` | Valores únicos (bool o threshold) | Todos | `unique: true` o `unique: 0.95` |
| `nullable` | % máximo de nulos permitido | Todos | `nullable: 0.1` (10%) |
| `min_length` | Longitud mínima de texto | text, email, etc. | `min_length: 5` |
| `max_length` | Longitud máxima de texto | text, email, etc. | `max_length: 100` |
| `min` | Valor mínimo numérico | integer, float, currency | `min: 0` |
| `max` | Valor máximo numérico | integer, float, currency | `max: 10000` |
| `mean_range` | Rango esperado del promedio | integer, float, currency | `mean_range: [100, 500]` |
| `values` | Lista de valores permitidos | enum | `values: [A, B, C]` |
| `description` | Descripción de la columna | Todos | `description: "ID único del cliente"` |

### Detección de Outliers

```yaml
lifetime_value:
  type: currency
  outlier_detection:
    enabled: true
    method: quantile
    quantiles: [0.01, 0.99]
    expected_ranges:
      - [0, 200]      # Q1% debe estar en 0-200
      - [8000, 12000] # Q99% debe estar en 8000-12000
```

---

## Validaciones de Seguridad

### Configuración Global

```yaml
validation:
  security:
    enabled: true
    checks:
      sql_injection: true        # Detecta: ', --, ;, UNION, SELECT, etc.
      xss: true                  # Detecta: <script>, javascript:, etc.
      nosql_injection: true      # Detecta: $ne, $gt, $where, etc.
      command_injection: true    # Detecta: ;, &&, ||, backticks
      path_traversal: true       # Detecta: ../, ..\
      sensitive_data_exposure: true  # Detecta: tarjetas, API keys, passwords
      ssn_exposure: true         # Detecta: SSN, DNI
```

Las validaciones de seguridad se aplican automáticamente a todas las columnas de tipo texto.

### Por Columna Individual

```yaml
user_input:
  type: text
  security: true  # Aplica TODAS las validaciones de seguridad
```

---

## Reglas de Negocio

### Tipo 1: Percentage Where

Valida que un porcentaje de filas cumpla una condición.

```yaml
business_rules:
  - name: "minimum_active_customers"
    description: "Al menos 50% de clientes deben estar activos"
    metric: percentage_where
    column: account_status
    value: active
    threshold: ">= 50%"
    severity: critical
```

### Tipo 2: Conditional

Valida que cuando se cumple una condición, otras columnas tengan valores específicos.

```yaml
business_rules:
  - name: "high_value_customers_active"
    description: "Clientes con alto valor deben estar activos"
    when: "lifetime_value > 20000"
    expect:
      account_status: [active]
    severity: critical
```

### Tipo 3: Temporal/Lógica

Valida relaciones lógicas entre columnas.

```yaml
business_rules:
  - name: "registration_before_login"
    description: "Fecha de registro debe ser anterior al login"
    condition: "registration_date <= last_login"
    applies_to_rows: "last_login IS NOT NULL"
    severity: error
```

---

## Ejemplos Completos

### Ejemplo 1: Dataset de Clientes Básico

```yaml
validation:
  default_level: standard
  
  schemas:
    customers:
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
          unique: 0.98
        
        status:
          type: enum
          values: [active, inactive]
          required: true
```

### Ejemplo 2: Dataset con Seguridad Estricta

```yaml
validation:
  default_level: paranoid
  security:
    enabled: true
    checks:
      sql_injection: true
      xss: true
      sensitive_data_exposure: true
  
  schemas:
    user_inputs:
      columns:
        user_id:
          type: uuid
        
        comment:
          type: text_long
          max_length: 5000
          # Security checks se aplican automáticamente
```

### Ejemplo 3: Dataset con Reglas de Negocio Complejas

```yaml
validation:
  schemas:
    transactions:
      columns:
        transaction_id:
          type: uuid
          required: true
          unique: true
        
        amount:
          type: currency
          min: 0
          max: 1000000
          mean_range: [50, 5000]
          outlier_detection:
            enabled: true
            method: quantile
            quantiles: [0.01, 0.99]
            expected_ranges:
              - [0, 10]
              - [50000, 100000]
        
        status:
          type: enum
          values: [pending, completed, failed, refunded]
          required: true
        
        timestamp:
          type: datetime
          required: true
      
      business_rules:
        - name: "high_completion_rate"
          description: "Al menos 80% de transacciones completadas"
          metric: percentage_where
          column: status
          value: completed
          threshold: ">= 80%"
          severity: warning
        
        - name: "large_transactions_manual_review"
          description: "Transacciones >50000 deben estar en revisión"
          when: "amount > 50000"
          expect:
            status: [pending, completed]
          severity: critical
```

### Ejemplo 4: Usando Templates

```yaml
validation:
  schemas:
    new_customers:
      template: "customer_data_standard_v1"
      
      # Solo agregar columnas adicionales
      custom_columns:
        loyalty_tier:
          type: enum
          values: [bronze, silver, gold, platinum]
          required: true
```

---

## Niveles de Validación

Puedes configurar el nivel de rigurosidad global:

```yaml
validation:
  default_level: strict  # basic | standard | strict | paranoid
```

| Nivel | Incluye |
|-------|---------|
| `basic` | Tipos de datos, valores nulos |
| `standard` | + Formatos, rangos |
| `strict` | + Reglas de negocio, detección de outliers |
| `paranoid` | + Todas las validaciones de seguridad |

---

## Comparación: Antes vs Después

### ANTES (verboso, ~50 líneas):
```yaml
expectations:
  - expectation_type: "expect_column_values_to_not_be_null"
    column: "customer_id"
  - expectation_type: "expect_column_values_to_be_unique"
    column: "customer_id"
  - expectation_type: "expect_column_values_to_match_regex"
    column: "customer_id"
    regex: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  - expectation_type: "expect_column_values_to_not_be_null"
    column: "email"
  - expectation_type: "expect_column_values_to_match_regex"
    column: "email"
    regex: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
  # ... 40 líneas más
```

### DESPUÉS (simplificado, ~10 líneas):
```yaml
validation:
  level: strict
  security: true
  
  columns:
    customer_id:
      type: uuid
      required: true
    
    email:
      type: email
      required: true
```

**Reducción: ~80% menos código** 🎯
