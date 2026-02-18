# Sistema de Validación

Sistema de validación production-ready con 40+ tipos semánticos, patrones de seguridad OWASP Top 10+ y verificaciones de cumplimiento.

## Visión General

**Capacidades:**
- Validación de calidad de datos (completitud, unicidad, precisión, consistencia)
- Detección de vulnerabilidades de seguridad (SQL injection, XSS, command injection, data leakage)
- Verificaciones de cumplimiento (GDPR, PCI-DSS, HIPAA, SOC2)
- Detección de outliers estadísticos
- Validación cross-field
- Aplicación de reglas de negocio

**Implementación:** Integraciones con Great Expectations

## Categorías de Validación

### 1. Identificadores

| Tipo | Descripción | Formato |
|------|-------------|--------|
| `uuid` | UUID v4 | `8-4-4-4-12` hexadecimal |
| `id` | Integer ID | Entero positivo |

**Ejemplo:**
```yaml
user_id:
  type: uuid
  required: true
  unique: true
```

### 2. Información de Contacto

| Tipo | Descripción | Formato |
|------|-------------|--------|
| `email` | Dirección email | RFC 5322 |
| `phone_es` | Teléfono español | `+34XXXXXXXXX` |
| `phone_us` | Teléfono US | `+1 (XXX) XXX-XXXX` |
| `phone` | Internacional | 8-20 dígitos |

**Ejemplo:**
```yaml
email:
  type: email
  required: true
  unique: true
  no_test_values: true  # Rechazar test@, dummy@
  gdpr_check: true      # Marcar como PII
```

**Validaciones automáticas:**
- Cumplimiento de formato
- Anti-patrones (valores de test, datos dummy)
- Detección XSS
- Marcado PII GDPR

### 3. Identificación Personal (PII)

| Tipo | Descripción | Cumplimiento |
|------|-------------|-------------|
| `ssn` | Número de Seguro Social US | PII |
| `dni` | DNI nacional español | GDPR |
| `passport` | Número de pasaporte | PII |

**Ejemplo:**
```yaml
ssn:
  type: ssn
  required: false
  # La validación verificará que NO existan patrones de SSN visibles
```

**Seguridad:**
- Severidad: CRITICAL
- Almacenamiento: Encriptación requerida
- Acceso: Auditoría registrada

### 4. Datos Financieros (PCI-DSS)

| Type | Description | Compliance |
|------|-------------|------------|
| `credit_card` | Card number | PCI-DSS |
| `iban` | International bank account | Financial |
| `currency` | ISO 4217 code | Standard |
| `amount` | Monetary amount | Standard |

**Example:**
```yaml
card_number:
  type: credit_card
  encrypted: true  # Required for PCI-DSS
```

**Best Practice:**
```yaml
# Store only last 4 digits
card_last4:
  type: text
  min_length: 4
  max_length: 4
  pci_dss_check: true  # Verify no full cards present
```

### 5. Componentes de Dirección

| Type | Description | Format |
|------|-------------|--------|
| `postal_code_es` | Código postal español | 5 dígitos |
| `postal_code_us` | Código ZIP de EE.UU. | `XXXXX` o `XXXXX-XXXX` |
| `country_code` | ISO 3166-1 alpha-2 | 2 letras |

### 6. Web y Red

| Type | Description | Security Checks |
|------|-------------|-----------------|
| `url` | URL HTTP/HTTPS | XSS, injection |
| `url_secure` | Solo HTTPS | XSS, injection |
| `ipv4` | Dirección IPv4 | Detección SSRF |
| `ipv6` | Dirección IPv6 | Detección SSRF |
| `domain` | Nombre de dominio | Validación de formato |
| `mac_address` | Dirección MAC | Validación de formato |

**Ejemplo:**
```yaml
api_endpoint:
  type: url_secure  # Solo HTTPS
  required: true
  no_ssrf: true     # Rechazar IPs internas
```

**Detección SSRF:**
- Bloquea: `localhost`, `127.0.0.1`, `192.168.x.x`, `10.x.x.x`, `172.16-31.x.x`

### 7. Tipos de Texto

| Type | Description | Validations |
|------|-------------|-------------|
| `name` | Nombre de persona | Longitud, anti-patrones, injection |
| `text` | Texto libre | Longitud, injection, XSS |
| `slug` | Slug de URL | Minúsculas, solo guiones |
| `alphanumeric` | Letras + números | Sin caracteres especiales |
| `enum` | Valores fijos | Lista blanca |

**Ejemplo:**
```yaml
full_name:
  type: name
  required: true
  min_length: 2
  max_length: 100
  # Seguridad automática habilitada por defecto (según security_level):
  # - SQL, NoSQL, Command Injection
  # - XSS Patterns
  # - Suspicious Characters
```

**Anti-Patrones Detectados:**
- Valores de prueba: `test`, `dummy`, `fake`, `lorem`
- Marcadores: `TBD`, `N/A`, `???`, `pending`
- Sobrecarga de caracteres especiales: `!@#$%` repetido 5+ veces

### 8. Tipos Numéricos

| Type | Description | Validation |
|------|-------------|------------|
| `integer` | Número entero | Rango, outliers |
| `numeric` | Decimal | Rango, precisión |
| `percentage` | Porcentaje (0-100) | Rango estricto |
| `probability` | 0.0 - 1.0 | Rango estricto |
| `amount` | Moneda | Precisión 2 decimales |

**Ejemplo:**
```yaml
age:
  type: integer
  required: true
  min: 18
  max: 120
  detect_outliers: true     # Z-score, IQR
  mean_between: [30, 50]    # Rango de media esperado
```

### 9. Fecha y Hora

| Type | Description | Format |
|------|-------------|--------|
| `date` | Solo fecha | ISO 8601 |
| `datetime` | Fecha + hora | ISO 8601 |
| `timestamp` | Unix timestamp | Segundos enteros |
| `year` | Año | 4 dígitos |

**Ejemplo:**
```yaml
birth_date:
  type: date
  required: true
  min: "1900-01-01"
  max: "2010-12-31"  # Debe tener 14+ años
  format: "%Y-%m-%d"
```

### 10. Boolean

| Type | Description | Values |
|------|-------------|--------|
| `boolean` | Verdadero/falso | `true`, `false`, `0`, `1`, `yes`, `no` |

## Validaciones de Seguridad (OWASP Top 10+)

### Ataques de Inyección

Patrones detectados automáticamente:

**SQL Injection:**
- `' OR '1'='1`
- `'; DROP TABLE--`
- `UNION SELECT`
- `exec(`, `xp_cmdshell`

**NoSQL Injection:**
- `{"$ne": null}`
- `{"$gt": ""}`
- `[$regex]`

**LDAP Injection:**
- `*)(uid=*`
- `admin*)(|(password=*`

**XPath Injection:**
- `' or 1=1 or ''='`
- `//*[@id='xyz']`

**Command Injection:**
- `; rm -rf /`
- `| cat /etc/passwd`
- `& del C:\`

**YAML Injection:**
- `!!python/object/apply`
- `!!python/object/new`

**Template Injection (SSTI):**
- `{{7*7}}`
- `${7*7}`
- `<%= 7*7 %>`

**CSV Injection:**
- `=1+1`
- `@SUM(A1:A10)`
- `+cmd|'/c calc'!A1`

**XSS (Cross-Site Scripting):**
- `<script>alert('XSS')</script>`
- `<img src=x onerror=alert(1)>`
- `javascript:alert(1)`
- Manejadores de eventos: `onload=`, `onerror=`, `onclick=`

### Detección de Fuga de Datos

Escanea automáticamente datos sensibles en campos de texto libre:

**API Keys:**
- AWS: `AKIA[0-9A-Z]{16}`
- GitHub: `ghp_[a-zA-Z0-9]{36}`
- Google: `AIza[0-9A-Za-z-_]{35}`
- Slack: `xoxb-[0-9]{10,13}-[0-9]{10,13}-[a-zA-Z0-9]{24}`

**Private Keys:**
- RSA: `-----BEGIN RSA PRIVATE KEY-----`
- SSH: `-----BEGIN OPENSSH PRIVATE KEY-----`

**Tokens:**
- JWT: `eyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*`
- Bearer: `Bearer [a-zA-Z0-9_-]+`

**Passwords:**
- `password=`, `pwd=`, `pass=`

**Connection Strings:**
- `mongodb://`, `postgresql://`, `mysql://`

### Seguridad de Rutas

**Path Traversal:**
- `../../../etc/passwd`
- `..\..\..\windows\system32`
- `....//....//`

**File Inclusion:**
- `file:///etc/passwd`
- `php://filter/`

**Null Bytes:**
- `\x00`, `%00`

**CRLF Injection:**
- `\r\n`, `%0d%0a`

### Seguridad de Red

**XXE (XML External Entity):**
- `<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>`

**SSRF (Server-Side Request Forgery):**
- IPs internas: `127.0.0.1`, `localhost`, `0.0.0.0`
- Redes privadas: `192.168.x.x`, `10.x.x.x`, `172.16-31.x.x`
- Metadata en la nube: `169.254.169.254`

## Verificaciones de Cumplimiento

### GDPR (Detección de PII)

Marca automáticamente campos PII:
- Direcciones de email
- Números de teléfono
- SSN, DNI, números de pasaporte
- Nombres
- Direcciones
- Direcciones IP

**Acciones:**
- Marcar columnas como PII
- Requerir encriptación en reposo
- Auditar logs de acceso
- Implementar derecho al olvido

### PCI-DSS (Seguridad de Tarjetas de Pago)

**Requisitos:**
- Números de tarjetas de crédito: Encriptados en reposo
- Códigos CVV: Nunca almacenados
- Fechas de expiración: Aseguradas
- Track data: Prohibido

**Validación:**
- Detectar números completos de tarjetas en campos de texto
- Marcar almacenamiento sin encriptar
- Severidad: CRITICAL

### HIPAA (Datos de Salud)

**Protected Health Information (PHI):**
- Números de historias médicas
- Números de beneficiarios de planes de salud
- Identificadores de pacientes

**Acciones:**
- Marcar como PHI
- Requerir encriptación
- Auditar todo acceso

### SOC2 (Controles de Seguridad)

**Fuga de Información de Debug:**
- Stack traces
- Rutas internas
- Schema de base de datos
- Variables de entorno
- Mensajes de error con datos sensibles

## Validaciones de Calidad

### Completitud

```yaml
required: true          # No nulo
mostly: 0.95            # 95% no-nulo permitido
allow_nulls: false      # Modo estricto
```

### Unicidad

```yaml
unique: true            # Todos los valores únicos
mostly_unique: 0.99     # 99% único permitido
compound_unique: ["col1", "col2"]  # Unicidad multi-columna
```

### Validación de Rango

```yaml
min: 0
max: 100
between: [18, 65]
not_between: [0, 0.001]  # Excluir cerca de cero
```

### Outliers Estadísticos

```yaml
detect_outliers: true
z_score_threshold: 3      # Desviaciones estándar
iqr_multiplier: 1.5       # Rango intercuartílico
quantile_threshold: 0.99  # Top/bottom 1%
```

### Conjuntos de Valores

```yaml
values: ["active", "inactive", "pending"]  # Lista blanca
not_values: ["deleted", "archived"]        # Lista negra
```

### Coincidencia de Patrones

```yaml
regex: "^[A-Z]{3}-\d{6}$"      # Formato personalizado
not_regex: "(?i)test|dummy"     # Anti-patrones
```

## Validación Cross-Field

### Rangos de Fecha

```yaml
# Validation suite
cross_field:
  - type: date_range
    start_column: start_date
    end_column: end_date
    allow_same_day: false
```

### Requisitos Condicionales

```yaml
cross_field:
  - type: conditional_required
    if_column: has_children
    if_value: true
    then_required: ["num_children", "child_ages"]
```

### Validación de Suma

```yaml
cross_field:
  - type: sum_equals
    columns: ["rent", "utilities", "food"]
    equals_column: total_expenses
    tolerance: 0.01  # Permitir redondeo de $0.01
```

### Integridad Referencial

```yaml
cross_field:
  - type: foreign_key
    column: customer_id
    reference_table: customers
    reference_column: id
```

## Reglas de Negocio

### Registros Mínimos

```yaml
business_rules:
  - type: minimum_records
    min_count: 1000
```

### Distribución por Categoría

```yaml
business_rules:
  - type: percentage_in_category
    column: status
    category: "active"
    percentage: 0.70  # Espera ~70% de registros en esta categoría
```

### Rangos Condicionales

```yaml
business_rules:
  - type: value_range_by_condition
    column: amount
    condition: "payment_method == 'crypto'"
    min_value: 50
    max_value: 100000
```

### Tendencia Monotónica

```yaml
business_rules:
  - type: trend_monotonic
    column: transaction_id
    direction: increasing   # increasing | decreasing
    allow_equal: true
```

## Ejemplo de Configuración

```yaml
validation:
  schema:
    raw_customers:
      security_level: strict
      quality_threshold: 0.95
      
      columns:
        customer_id:
          type: uuid
          required: true
          unique: true
        
        email:
          type: email
          required: true
          unique: true
        
        age:
          type: integer
          min: 18
          max: 100
      
      business_rules:
        - type: percentage_in_category
          column: account_status
          category: "active"
          percentage: 0.85
```

## Niveles de Severidad

| Level | Description | Action |
|-------|-------------|--------|
| `critical` | Vulnerabilidad de seguridad o violación de cumplimiento | Registrar incidencia crítica y afectar Score |
| `error` | Problema de calidad de datos que afecta la lógica de negocio | Afectar Score de Calidad |
| `warning` | Problema menor de calidad | Registrar advertencia |
| `info` | Solo informativo | Solo registrar |

## Reportes de Validación

Los reportes incluyen:
- Resumen ejecutivo (quality score, conteos pass/fail)
- Fallos detallados por suite
- Vulnerabilidades de seguridad detectadas
- Violaciones de cumplimiento
- Recomendaciones de remediación

**Formato:** HTML con tablas ordenables e indicadores de severidad

## Documentación Relacionada

- [ARCHITECTURE.md](ARCHITECTURE.md) - Arquitectura del módulo de validación
- [AUDIT_SYSTEM.md](AUDIT_SYSTEM.md) - Almacenamiento de resultados de validación
- [CLI_REFERENCE.md](CLI_REFERENCE.md) - Ejecución de pipelines de validación
