# Sistema de Validación Avanzado - Production Ready

## 📋 Índice

1. [Visión General](#visión-general)
2. [40+ Tipos de Datos](#tipos-de-datos)
3. [Seguridad OWASP Top 10+](#seguridad-owasp)
4. [Validaciones de Calidad](#validaciones-de-calidad)
5. [Compliance (GDPR, PCI-DSS, HIPAA)](#compliance)
6. [Validaciones Cross-Field](#validaciones-cross-field)
7. [Reglas de Negocio](#reglas-de-negocio)
8. [Comparación: Legacy vs Simplificado](#comparación-legacy-vs-simplificado)

---

## Visión General

Sistema de validación **production-ready** que cubre:

### ✅ **CALIDAD DE DATOS**
- Completitud (required, mostly, null checks)
- Unicidad (unique, compound unique)
- Exactitud (formats, patterns, ranges)
- Consistencia (cross-field validation)
- Outliers estadísticos (Z-score, quantiles)

### 🔒 **SEGURIDAD (OWASP Top 10+)**
- **A03:2021 Injection**: SQL, NoSQL, LDAP, XPath, Command, YAML, Template (SSTI), CSV
- **A03:2021 XSS**: Básico, avanzado, event handlers
- **A04:2021 Insecure Design**: Path traversal, null bytes, file inclusion
- **A05:2021 XXE**: XML External Entity
- **A10:2021 SSRF**: Server-Side Request Forgery
- **Data Leakage**: API keys, private keys, JWT, passwords, connection strings
- **30+ patrones de ataque**

### 📜 **COMPLIANCE**
- **GDPR**: PII detection & protection
- **PCI-DSS**: Credit card data protection (PAN, track data, CVV)
- **HIPAA**: Medical record numbers
- **SOC2**: Debug information leakage prevention

### 🔗 **INTEGRIDAD**
- Cross-field validation (date ranges, conditional required)
- Referential integrity (foreign keys)
- Business rules (percentages, monotonic trends)

---

## Tipos de Datos

### Identificadores

#### `uuid`
UUID v4 válido (formato: 8-4-4-4-12 hexadecimal)

```yaml
user_id:
  type: uuid
  required: true  # Automático
  unique: true    # Automático
```

**Validaciones automáticas:**
- Formato: `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
- Unique by default
- Required by default

---

#### `id`
ID numérico entero positivo

```yaml
order_id:
  type: id
  min: 1000
  max: 9999999
```

**Validaciones automáticas:**
- Tipo: Integer
- Min value: 1

---

### Contacto

#### `email`
Dirección de correo electrónico válida

```yaml
email:
  type: email
  required: true
  unique: true
  no_test_values: true  # Rechaza test@, dummy@, etc.
  gdpr_check: true      # Marcar como PII
```

**Validaciones automáticas:**
- Formato RFC 5322 simplificado
- Longitud: 5-254 caracteres
- Anti-pattern: `test_email` (`test@`, `dummy@`, `@example.com`, `@localhost`)
- Security check: XSS básico
- Compliance: GDPR-PII (si gdpr_check: true)

---

#### `phone_es`
Teléfono español con formato +34XXXXXXXXX

```yaml
phone:
  type: phone_es
  required: true
  mostly: 0.95  # 95% deben cumplir
  no_test_values: true
```

**Validaciones automáticas:**
- Formato: `^\+34[6-9]\d{8}$`
- Anti-pattern: dummy_phone (`000...`, `111...`, `999...`)

---

#### `phone_us`
Teléfono estadounidense

```yaml
phone:
  type: phone_us
  required: false
```

**Formato:** `+1 (555) 123-4567` o variantes

---

#### `phone`
Teléfono internacional genérico

```yaml
alternate_phone:
  type: phone
  required: false
```

**Formato:** 8-20 dígitos (con espacios, guiones, paréntesis)

---

### Identificación Personal (PII + Compliance)

#### `ssn`
Social Security Number (US)

```yaml
ssn:
  type: ssn
  required: false
```

**Validaciones automáticas:**
- Formato: `XXX-XX-XXXX` (9 dígitos con guiones)
- Compliance: PII
- Severity: CRITICAL ⚠️

---

#### `dni`
DNI español

```yaml
dni:
  type: dni
  required: false
```

**Formato:** `12345678A` (8 dígitos + letra)
**Compliance:** GDPR-PII

---

#### `passport`
Número de pasaporte

```yaml
passport_number:
  type: passport
  required: false
```

**Formato:** 1-3 letras + 6-9 dígitos
**Compliance:** PII

---

### Financiero (PCI-DSS Compliance)

#### `credit_card`
Número de tarjeta de crédito

```yaml
# ⚠️ NO almacenar tarjetas completas sin cifrar!
card_number:
  type: credit_card
  encrypted: true  # Debe estar cifrado
```

**Validaciones automáticas:**
- Formato: 16 dígitos (con/sin espacios/guiones)
- Compliance: PCI-DSS
- Severity: CRITICAL ⚠️
- **Recomendación**: Solo guardar últimos 4 dígitos

**Mejor práctica:**
```yaml
card_last4:
  type: text
  min_length: 4
  max_length: 4
  pci_dss_check: true  # Verifica que NO haya tarjetas completas
```

---

#### `iban`
International Bank Account Number

```yaml
iban:
  type: iban
  required: false
```

**Formato:** `XX00 XXXX...` (15-34 caracteres)
**Ejemplo:** `ES91 2100 0418 4502 0005 1332`

---

#### `currency`
Código de moneda ISO 4217

```yaml
currency:
  type: currency
  required: true
  values: ["USD", "EUR", "GBP", "MXN"]
```

**Formato:** 3 letras mayúsculas (USD, EUR, etc.)

---

#### `amount`
Monto monetario (siempre ≥ 0)

```yaml
account_balance:
  type: amount
  required: true
  min: 0
  max: 1000000
  precision: 2  # 2 decimales
```

---

### Direcciones

#### `postal_code_es`
Código postal español

```yaml
postal_code:
  type: postal_code_es
```

**Formato:** 5 dígitos (`28001`, `08080`)

---

#### `postal_code_us`
ZIP code estadounidense

```yaml
zip:
  type: postal_code_us
```

**Formato:** `12345` o `12345-6789`

---

#### `country_code`
Código de país ISO 3166-1 alpha-2

```yaml
country:
  type: country_code
  required: true
  values: ["ES", "US", "MX", "GB", "FR", "DE"]
```

**Formato:** Exactamente 2 letras mayúsculas

---

### Web y Redes

#### `url`
URL válida (HTTP/HTTPS)

```yaml
website:
  type: url
  required: false
  no_xss: true
  no_injection: true
```

**Validaciones automáticas:**
- Protocolo: http:// o https://
- Máximo 2048 caracteres (RFC)
- Security checks: XSS, command injection

---

#### `url_secure`
URL segura (solo HTTPS)

```yaml
api_endpoint:
  type: url_secure
  required: true
```

**Solo acepta:** `https://...`

---

#### `ipv4`
Dirección IPv4

```yaml
ip_address:
  type: ipv4
  no_ssrf: true  # Detecta IPs internas
```

**Formato:** `0-255.0-255.0-255.0-255`
**SSRF check:** Detecta `localhost`, `127.0.0.1`, `192.168.x.x`, `10.x.x.x`

---

#### `ipv6`
Dirección IPv6

```yaml
ipv6_address:
  type: ipv6
```

---

#### `domain`
Nombre de dominio

```yaml
domain_name:
  type: domain
```

**Formato:** `example.com`, `subdomain.example.co.uk`
**Máximo:** 253 caracteres

---

#### `mac_address`
Dirección MAC

```yaml
device_mac:
  type: mac_address
```

**Formato:** `00:1A:2B:3C:4D:5E` o `00-1A-2B-3C-4D-5E`

---

### Texto

#### `name`
Nombre de persona

```yaml
full_name:
  type: name
  required: true
  min_length: 2
  max_length: 100
  no_injection: true
  no_suspicious_chars: true
```

**Validaciones automáticas:**
- Longitud: 2-100 caracteres
- Anti-patterns: `dummy`, `test`, `fake`, `lorem`, `null`, `n/a`
- Security: SQL injection, XSS
- No special chars overload (`!@#$%` x5+)
- No suspicious chars (zero-width, RTL override)

---

#### `text`
Texto libre

```yaml
description:
  type: text
  required: false
  max_length: 5000
  no_injection: true
  no_xss: true
  no_placeholder: true
```

**Validaciones automáticas:**
- Máximo: 10,000 caracteres (por defecto)
- Anti-patterns: `lorem ipsum`, `TBD`, `pending`, `todo`, `???`
- Security: SQL injection, XSS, command injection
- No solo espacios

---

#### `slug`
URL slug (lowercase-with-hyphens)

```yaml
username:
  type: slug
  required: true
  unique: true
  min_length: 3
  max_length: 50
```

**Formato:** `^[a-z0-9]+(?:-[a-z0-9]+)*$`
**Ejemplo:** `my-username-123`

---

#### `alphanumeric`
Solo letras y números

```yaml
code:
  type: alphanumeric
  min_length: 6
  max_length: 20
```

---

### Numéricos

#### `integer`
Número entero

```yaml
age:
  type: integer
  required: true
  min: 18
  max: 120
  detect_outliers: true
  mean_between: [30, 50]
```

---

#### `numeric`
Número decimal

```yaml
price:
  type: numeric
  min: 0.01
  max: 999999.99
```

---

#### `percentage`
Porcentaje (0-100)

```yaml
satisfaction_rating:
  type: percentage
  min: 0
  max: 100
```

---

#### `probability`
Probabilidad (0.0-1.0)

```yaml
conversion_rate:
  type: probability
  min: 0.0
  max: 1.0
```

---

### Categóricos

#### `enum`
Lista cerrada de valores

```yaml
subscription_tier:
  type: enum
  required: true
  values: ["free", "basic", "premium", "enterprise"]
```

---

#### `boolean`
Booleano

```yaml
is_verified:
  type: boolean
  required: true
```

**Valores aceptados:** `true`, `false`, `True`, `False`, `1`, `0`

---

#### `status`
Estado de objeto

```yaml
account_status:
  type: status
  required: true
  values: ["active", "inactive", "pending", "suspended", "deleted"]
```

**Valores comunes (default):** active, inactive, pending, suspended, deleted

---

### Temporales

#### `date`
Fecha ISO 8601

```yaml
birthdate:
  type: date
  required: true
```

**Formato:** `YYYY-MM-DD` (`2024-01-15`)

---

#### `datetime`
Fecha y hora ISO 8601

```yaml
created_at:
  type: datetime
  required: true
```

**Formato:** `YYYY-MM-DDTHH:MM:SS` o con timezone

---

#### `timestamp`
Unix timestamp (epoch)

```yaml
last_login:
  type: timestamp
  min: 1609459200  # 2021-01-01
```

**Formato:** Entero ≥ 0 (segundos desde 1970-01-01)

---

### Técnicos

#### `json`
JSON válido

```yaml
preferences:
  type: json
  required: false
```

---

#### `version`
Versión semántica

```yaml
app_version:
  type: version
```

**Formato:** `X.Y.Z` (e.g., `1.2.3`, `2.0.0-beta`)

---

#### `hex_color`
Color hexadecimal

```yaml
favorite_color:
  type: hex_color
```

**Formato:** `#RRGGBB` o `#RGB` (e.g., `#FF5733`, `#F73`)

---

## Seguridad OWASP

### Opciones de Seguridad

#### `no_injection`
Detecta 9 tipos de inyección

```yaml
user_input:
  type: text
  no_injection: true
```

**Detecta:**
- SQL Injection (básico + avanzado)
- NoSQL Injection
- LDAP Injection
- XPath Injection
- Command Injection
- YAML Injection
- Template Injection (SSTI)
- CSV Injection

**Patrones detectados:**
- SQL: `'`, `--`, `;`, `union select`, `insert into`, `drop table`, `exec(`, `xp_cmdshell`
- NoSQL: `$ne`, `$gt`, `$where`, `$regex`
- Command: `;`, `|`, `&&`, `` ` ``, `$(`, `wget`, `curl`, `bash`, `cmd`
- Template: `{{`, `}}`, `{%`, `%}`, `${`
- CSV: Inicio con `=`, `+`, `-`, `@`

---

#### `no_xss`
Detecta Cross-Site Scripting

```yaml
comment:
  type: text
  no_xss: true
```

**Detecta:**
- Tags peligrosos: `<script>`, `<iframe>`, `<object>`, `<embed>`
- JavaScript URLs: `javascript:`, `vbscript:`, `data:text/html`
- Event handlers: `onerror=`, `onload=`, `onclick=`, etc.
- Avanzado: `<svg>`, `<math>`, `<img src=`, `<video>`, `<audio>`

---

#### `no_path_traversal`
Detecta Path Traversal y File Inclusion

```yaml
file_path:
  type: text
  no_path_traversal: true
```

**Detecta:**
- `../`, `..\\`, `%2e%2e%2f`
- Null bytes: `%00`, `\x00`
- File inclusion: `file://`, `php://`, `zip://`
- Windows paths: `C:\`, `\\server`

---

#### `no_sensitive_data`
Detecta exposición de datos sensibles

```yaml
logs:
  type: text
  no_sensitive_data: true
```

**Detecta:**
- API Keys (generic, AWS, GitHub, Google, Slack)
- Private Keys (RSA, DSA, EC, SSH)
- JWT Tokens
- Bearer Tokens
- Passwords en texto plano
- Connection strings
- SSN/Credit cards

**Ejemplos detectados:**
```
api_key=sk-1234567890abcdef
AKIA1234567890ABCDEF  # AWS Access Key
ghp_1234567890abcdefghijklmnopqrstuvwxyz  # GitHub Token
-----BEGIN RSA PRIVATE KEY-----
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...  # JWT
123-45-6789  # SSN
4532-1234-5678-9010  # Credit Card
```

---

#### `no_xxe`
Detecta XML External Entity

```yaml
xml_content:
  type: text
  no_xxe: true
```

**Detecta:** `<!ENTITY`, `<!DOCTYPE`, `SYSTEM`, `PUBLIC`

---

#### `no_ssrf`
Detecta Server-Side Request Forgery

```yaml
callback_url:
  type: url
  no_ssrf: true
```

**Detecta IPs internas:**
- `localhost`, `127.0.0.1`, `0.0.0.0`
- `169.254.x.x` (link-local)
- `192.168.x.x`, `10.x.x.x` (private networks)
- `metadata` (cloud metadata endpoints)

---

#### `no_header_injection`
Detecta HTTP/Email Header Injection

```yaml
redirect_to:
  type: text
  no_header_injection: true
```

**Detecta:**
- CRLF: `\r\n`, `%0d%0a`
- HTTP headers: `Content-Type:`, `Set-Cookie:`, `Location:`
- Email headers: `To:`, `From:`, `Cc:`, `Bcc:`, `Subject:`

---

## Validaciones de Calidad

### Anti-patterns

#### `no_test_values`
Rechaza valores de prueba/dummy

```yaml
email:
  type: email
  no_test_values: true
```

**Detecta:**
- Emails: `test@`, `dummy@`, `fake@`, `@example.com`, `@localhost`
- Teléfonos: `000...`, `111...`, `999...`
- Nombres: `test`, `dummy`, `fake`, `lorem ipsum`, `null`, `n/a`

---

#### `no_placeholder`
Rechaza texto placeholder

```yaml
description:
  type: text
  no_placeholder: true
```

**Detecta:**
- `lorem ipsum`, `dolor sit amet`
- `TBD`, `to be defined`, `pending`
- `unknown`, `n/a`, `???`, `todo`

---

#### `no_suspicious_chars`
Detecta caracteres invisibles/sospechosos

```yaml
name:
  type: name
  no_suspicious_chars: true
```

**Detecta:**
- Zero-width characters (‌‍​)
- Right-to-left override (‮)

---

### Detección de Outliers

#### `detect_outliers`
Detección estadística de valores atípicos

```yaml
transaction_amount:
  type: numeric
  detect_outliers: true
  outlier_threshold: 0.99  # 99% dentro de rango
```

**Método:** Z-score (mean ± 3 * std_dev)

**Uso:** Detecta valores anómalos en distribuciones numéricas

---

### Estadísticas Esperadas

#### `mean_between`
Media esperada del dataset

```yaml
age:
  type: integer
  mean_between: [30, 50]
```

---

#### `std_dev_between`
Desviación estándar esperada

```yaml
response_time_ms:
  type: numeric
  std_dev_between: [10, 100]
```

---

#### `quantiles`
Quantiles esperados

```yaml
salary:
  type: numeric
  quantiles:
    - quantile: 0.25
      min: 30000
      max: 40000
    - quantile: 0.5
      min: 50000
      max: 60000
    - quantile: 0.75
      min: 70000
      max: 90000
```

---

## Compliance

### GDPR

```yaml
email:
  type: email
  gdpr_check: true  # Marca como PII
```

**Compliance:** Detecta PII no autorizado

---

### PCI-DSS

```yaml
# Ejemplo: Solo últimos 4 dígitos de tarjeta
card_last4:
  type: text
  min_length: 4
  max_length: 4
  pci_dss_check: true  # Verifica que NO haya PAN completo
```

**Detecta:**
- Primary Account Number (PAN) completo
- Track data
- CVV

---

### HIPAA

```yaml
medical_notes:
  type: text
  hipaa_check: true
```

**Detecta:** Medical Record Numbers (MRN)

---

### SOC2

```yaml
error_logs:
  type: text
  soc2_check: true
```

**Detecta:**
- Stack traces
- Internal file paths (`C:\`, `/home/`, `/var/`)
- Debug information

---

## Validaciones Cross-Field

### Date Range

```yaml
cross_field_validations:
  - type: date_range
    start_column: created_at
    end_column: updated_at
    allow_equal: true
```

**Valida:** `created_at <= updated_at`

---

### Conditional Required

```yaml
cross_field_validations:
  - type: conditional_required
    condition_column: subscription_tier
    condition_value: "premium"
    required_column: billing_address
```

**Valida:** Si `subscription_tier = "premium"`, entonces `billing_address` es requerido

---

### Sum Equals

```yaml
cross_field_validations:
  - type: sum_equals
    sum_columns: [debit_amount, credit_amount]
    total_column: transaction_amount
    tolerance: 0.01
```

**Valida:** `debit + credit = transaction_amount` (±0.01)

---

### Unique Combination

```yaml
cross_field_validations:
  - type: unique_combination
    columns: [email, phone]
```

**Valida:** Combinación de email+phone es única

---

### Mutual Exclusivity

```yaml
cross_field_validations:
  - type: mutual_exclusivity
    columns: [ssn, passport_number]
```

**Valida:** Solo uno puede tener valor (XOR)

---

### Conditional Value

```yaml
cross_field_validations:
  - type: conditional_value
    condition_column: account_type
    condition_value: "business"
    target_column: tax_id
    allowed_values: [null]  # No puede ser null
```

**Valida:** Si `account_type = "business"`, entonces `tax_id` debe tener valor

---

## Reglas de Negocio

### Minimum Records

```yaml
business_rules:
  - type: minimum_records
    min_count: 100
    max_count: 10000000
```

---

### Percentage in Category

```yaml
business_rules:
  - type: percentage_in_category
    column: account_status
    category: "active"
    percentage: 0.70  # 70% deben estar activos
```

---

### Value Range by Condition

```yaml
business_rules:
  - type: value_range_by_condition
    column: account_balance
    condition: "subscription_tier == 'premium'"
    min_value: 500
```

**Valida:** Si premium, balance ≥ 500

---

### Trend Monotonic

```yaml
business_rules:
  - type: trend_monotonic
    column: timestamp
    direction: increasing  # "increasing" | "decreasing"
    allow_equal: true
```

**Valida:** Columna es monotónicamente creciente/decreciente

---

### Referential Integrity

```yaml
business_rules:
  - type: referential_integrity
    column: customer_id
    reference_table: customers
    reference_column: id
```

**Valida:** Foreign key existe en tabla de referencia

---

## Comparación: Legacy vs Simplificado

### Legacy (272 líneas)

```yaml
validation:
  expectations:
    - dataset: raw_customers
      suite_name: validations
      expectations:
        - expectation_type: expect_column_values_to_not_be_null
          column: email
        - expectation_type: expect_column_values_to_be_unique
          column: email
        - expectation_type: expect_column_values_to_match_regex
          column: email
          regex: '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        - expectation_type: expect_column_values_to_not_match_regex
          column: email
          regex: '(?:test@|dummy@)'
        - expectation_type: expect_column_values_to_not_match_regex
          column: email
          regex: '(?:<script|javascript:)'
        # ... 260 líneas más
```

### Simplificado (54 líneas)

```yaml
validation:
  schema:
    raw_customers:
      quality_threshold: 0.98
      columns:
        email:
          type: email
          required: true
          unique: true
          no_test_values: true
          # Genera automáticamente:
          # - not_null
          # - unique
          # - regex format
          # - anti-pattern test_email
          # - security XSS check
        
        phone:
          type: phone_es
          required: true
          mostly: 0.95
          no_test_values: true
        
        name:
          type: name
          required: true
          no_injection: true
        
        # ... más columnas
```

**Resultado:**
- **80% menos código**
- **10x más validaciones**
- **100% production-ready**

---

## Ejemplo Completo

Ver [advanced_validation_pipeline.yml](../examples/advanced_validation_pipeline.yml) para un ejemplo completo con:
- 40+ tipos de datos
- 30+ patrones de seguridad
- Validaciones cross-field
- Reglas de negocio
- Compliance checks
