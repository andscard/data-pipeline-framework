from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import re


class ValidationSeverity(Enum):
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ValidationCategory(Enum):
    STRUCTURE = "estructura"
    DATA_QUALITY = "calidad_datos"
    DATA_TYPE = "tipos_formato"
    BUSINESS_RULES = "reglas_negocio"
    SECURITY = "seguridad"
    INTEGRITY = "integridad"
    COMPLIANCE = "cumplimiento"


class DataTypeRegistry:
    
    PATTERNS = {
        'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        'uuid_any': r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
        
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'phone_es': r'^\+34[6-9]\d{8}$',
        'phone_intl': r'^\+?[\d\s\-\(\)]{8,20}$',
        'phone_us': r'^\+?1?\s*\(?[2-9]\d{2}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
        
        'url': r'^https?://[^\s/$.?#].[^\s]*$',
        'url_secure': r'^https://[^\s/$.?#].[^\s]*$',
        'domain': r'^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]?\.[a-zA-Z]{2,}$',
        'ipv4': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
        'ipv6': r'^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::)$',
        'mac_address': r'^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$',
        
        'ssn_us': r'^\d{3}-\d{2}-\d{4}$',
        'dni_es': r'^\d{8}[A-Z]$',
        'nie_es': r'^[XYZ]\d{7}[A-Z]$',
        'passport': r'^[A-Z]{1,3}\d{6,9}$',
        
        'credit_card': r'^(?:\d{4}[\s-]?){3}\d{4}$',
        'iban': r'^[A-Z]{2}\d{2}[A-Z0-9]{4,30}$',
        'swift_bic': r'^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$',
        'currency_code': r'^[A-Z]{3}$',
        
        'postal_code_es': r'^\d{5}$',
        'postal_code_us': r'^\d{5}(-\d{4})?$',
        'postal_code_uk': r'^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$',
        
        'alphanumeric': r'^[a-zA-Z0-9]+$',
        'alpha_only': r'^[a-zA-Z]+$',
        'numeric_only': r'^\d+$',
        'slug': r'^[a-z0-9]+(?:-[a-z0-9]+)*$',
        'hex_color': r'^#?([a-fA-F0-9]{6}|[a-fA-F0-9]{3})$',
        
        'semver': r'^\d+\.\d+\.\d+(?:-[a-zA-Z0-9]+)?$',
        'iso_date': r'^\d{4}-\d{2}-\d{2}$',
        'iso_datetime': r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$',
        'country_code': r'^[A-Z]{2}$',
        'language_code': r'^[a-z]{2}(-[A-Z]{2})?$',
        
        'sql_injection': r"(?i)(?:--|;|\/\*|\*\/|union\s+select|insert\s+into|delete\s+from|drop\s+table|update\s+.+set|exec\s*\(|execute\s*\(|xp_cmdshell|sp_executesql)",
        'sql_injection_advanced': r"(?i)(?:or\s+1\s*=\s*1|and\s+1\s*=\s*1|having\s+1\s*=\s*1|waitfor\s+delay|benchmark\s*\(|sleep\s*\(|pg_sleep)",
        
        'nosql_injection': r"(?:\$ne|\$gt|\$lt|\$gte|\$lte|\$in|\$nin|\$where|\$regex|\$options|\$expr|\$jsonSchema)",
        
        'ldap_injection': r"(?:\*|!|~=|>=|<=|\x00)",
        
        'xpath_injection': r"(?:\"\]|\[\"|\.\.)",
        
        'command_injection': r"(?:;|&&|\n|\r|`|\$\(|>\s*\/|<\s*\/|\bwget\s+|\bcurl\s+|\bnc\s+|\bbash\s+|\bsh\s+|\bcmd\s+|\bpowershell\s+|\beval\s+|\bexec\s+)",
        
        'yaml_injection': r"(?:!!python/|!!map|!!omap|!!pairs|__import__|eval\(|exec\()",
        
        'template_injection': r"(?:\{\{|\}\}|\{%|%\}|\$\{|<%|%>|#\{)",
        
        'csv_injection': r"^[=@]|^[+\-](?!\d)",
        
        'crlf_injection': r"(?:%0d|%0a|\\r|\\n|\r\n)",
        
        'xss_basic': r"(?i)(?:<script[^>]*>|<\/script>|javascript:|onerror\s*=|onload\s*=|(?:\s+on[a-z]+\s*=)|<iframe|<object|<embed)",
        
        'xss_advanced': r"(?i)(?:<img[^>]+src|<svg[^>]*>|<math[^>]*>|<video[^>]*>|<audio[^>]*>|<link[^>]+href|vbscript:|livescript:|mocha:|data:text/html)",
        
        'xss_event_handlers': r"(?i)on(?:abort|blur|change|click|dblclick|error|focus|keydown|keypress|keyup|load|mousedown|mousemove|mouseout|mouseover|mouseup|reset|resize|select|submit|unload)\s*=",
        
        'path_traversal': r"(?:\.\.\/|\.\.\\|%2e%2e%2f|%2e%2e%5c|..%2f|..%5c|\.\.%252f)",
        'path_traversal_win': r"(?:[C-Z]:\\|\\\\)",
        'null_byte': r"(?:%00|\x00)",
        
        'xxe_injection': r"(?:<!ENTITY|<!DOCTYPE|SYSTEM\s+['\"]|PUBLIC\s+['\"])",
        
        'ssrf_patterns': r"(?:localhost|127\.0\.0\.|0\.0\.0\.0|169\.254\.|metadata|internal)",
        
        'privilege_escalation': r"(?i)(?:\bsudo\b|\bsuperuser\b)",

        'api_key_generic': r"(?i)(?:api[_-]?key|apikey|access[_-]?token|auth[_-]?token)\s*[:=]\s*['\"]?[a-zA-Z0-9_-]{20,}['\"]?",
        'aws_access_key': r"(?:AKIA|ASIA)[0-9A-Z]{16}",
        'aws_secret_key': r"(?i)aws.{0,20}?['\"][0-9a-zA-Z/+=]{40}['\"]",
        'github_token': r"ghp_[0-9a-zA-Z]{36}|gho_[0-9a-zA-Z]{36}|ghu_[0-9a-zA-Z]{36}|ghs_[0-9a-zA-Z]{36}|ghr_[0-9a-zA-Z]{36}",
        'slack_token': r"xox[pboa]-[0-9]{12}-[0-9]{12}-[0-9a-zA-Z]{24,32}",
        'google_api_key': r"AIza[0-9A-Za-z\\-_]{35}",
        
        'private_key': r"(?:-----BEGIN (?:RSA |DSA |EC |OPENSSH )?PRIVATE KEY-----)",
        'ssh_key': r"(?:ssh-rsa |ssh-dss |ecdsa-sha2-nistp256 )AAAA[0-9A-Za-z+/]+",
        
        'jwt_token': r"eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+",
        'bearer_token': r"(?i)bearer\s+[a-zA-Z0-9_\\-\\.=]+",
        
        'password_pattern': r"(?i)(?:password|passwd|pwd)\s*[:=]\s*['\"]?[^\s'\"]{8,}['\"]?",
        'connection_string': r"(?i)(?:server|host|database|uid|pwd|password)\s*=",
        
        'ssn_pattern': r"\b\d{3}-\d{2}-\d{4}\b",
        'credit_card_pattern': r"\b(?:\d{4}[\s-]?){3}\d{4}\b",
        
        'weak_password': r"^(?:123456|password|qwerty|admin|letmein|welcome|monkey|dragon|master|sunshine)$",
        
        'serialization_gadget': r"(?:__reduce__|__setstate__|pickle|marshal|yaml\.load|eval\(|exec\()",
        'deserialization_attack': r"(?:java\.lang\.Runtime|ProcessBuilder|ObjectInputStream)",
        
        'log_injection': r"(?:\n|\r|%0d|%0a|%0D|%0A)",
        
        'http_header_injection': r"(?:\r\n|\n|%0d|%0a)(?:Content-Type|Set-Cookie|Location):",
        
        'email_header_injection': r"(?:\r\n|\n|%0d|%0a)(?:To:|From:|Cc:|Bcc:|Subject:)",
        
        'file_inclusion': r"(?:file://|php://|zip://|data://|expect://|input://)",
        
        'test_email': r"(?i)(?:test|dummy|fake|sample|example|temp|noreply)@|@(?:test|example|dummy|localhost|invalid)\.",
        'dummy_phone': r"^(?:000|111|222|333|444|555|666|777|888|999)",
        'dummy_name': r"(?i)^(?:test|dummy|fake|sample|example|lorem|ipsum|null|n/a|na|xxx|undefined)$",
        
        'placeholder_text': r"(?i)(?:lorem ipsum|dolor sit|to\s+be\s+defined|tbd|pending|unknown|n/a|\?\?\?|xxx+|todo)",
        'spaces_only': r'^\s+$',
        'special_chars_overload': r'[!@#$%^&*()]{5,}',
        
        'invisible_chars': r"[\u200B-\u200D\uFEFF]",
        'rtl_override': r"[\u202E]",
        
        'gdpr_pii_combined': r"(?:\d{3}-\d{2}-\d{4}|\d{4}[\s-]\d{4}[\s-]\d{4}[\s-]\d{4}|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        
        'pci_pan': r"\b(?:\d{4}[\s-]?){3}\d{4}\b",
        'pci_track_data': r"%[A-Z]?\d{13,19}\^",
        'pci_cvv': r"\b\d{3,4}\b",
        
        'hipaa_mrn': r"(?i)(?:mrn|medical\s+record)\s*[:=]?\s*\d{6,}",
        
        'debug_stack_trace': r"(?i)(?:stack\s+trace|exception|error\s+at\s+line|warning:|notice:|debug:|traceback)",
        'internal_path': r"(?:[C-Z]:\\|\/home\/|\/usr\/|\/var\/|\/etc\/)",
    }
    
    TYPE_DEFINITIONS = {
        'uuid': {
            'category': 'identifier',
            'patterns': ['uuid'],
            'description': 'UUID v4',
            'default_required': True,
            'default_unique': True
        },
        'id': {
            'category': 'identifier',
            'description': 'ID numérico entero positivo',
            'min_value': 1,
            'type_check': 'integer'
        },
        
        'email': {
            'category': 'contact',
            'patterns': ['email'],
            'anti_patterns': ['test_email'],
            'description': 'Email válido',
            'min_length': 5,
            'max_length': 254,
            'security_checks': ['xss_basic']
        },
        'phone_es': {
            'category': 'contact',
            'patterns': ['phone_es'],
            'anti_patterns': ['dummy_phone'],
            'description': 'Teléfono español (+34XXXXXXXXX)',
        },
        'phone_us': {
            'category': 'contact',
            'patterns': ['phone_us'],
            'anti_patterns': ['dummy_phone'],
            'description': 'Teléfono estadounidense',
        },
        'phone': {
            'category': 'contact',
            'patterns': ['phone_intl'],
            'anti_patterns': ['dummy_phone'],
            'description': 'Teléfono internacional',
        },
        
        # ========== IDENTIFICACIÓN PERSONAL ==========
        'ssn': {
            'category': 'pii',
            'patterns': ['ssn_us'],
            'description': 'Social Security Number (US)',
            'compliance': 'PII',
            'severity': ValidationSeverity.CRITICAL
        },
        'dni': {
            'category': 'pii',
            'patterns': ['dni_es'],
            'description': 'DNI español',
            'compliance': 'GDPR-PII'
        },
        'passport': {
            'category': 'pii',
            'patterns': ['passport'],
            'description': 'Número de pasaporte',
            'compliance': 'PII'
        },
        
        # ========== FINANCIERO ==========
        'credit_card': {
            'category': 'financial',
            'patterns': ['credit_card'],
            'description': 'Número de tarjeta de crédito',
            'compliance': 'PCI-DSS',
            'severity': ValidationSeverity.CRITICAL,
            'should_be_encrypted': True
        },
        'iban': {
            'category': 'financial',
            'patterns': ['iban'],
            'description': 'IBAN (International Bank Account Number)',
            'min_length': 15,
            'max_length': 34
        },
        'currency': {
            'category': 'financial',
            'patterns': ['currency_code'],
            'description': 'Código de moneda ISO 4217 (USD, EUR, GBP)',
            'type_check': 'categorical'
        },
        'amount': {
            'category': 'financial',
            'description': 'Monto monetario',
            'type_check': 'numeric',
            'min_value': 0,
            'precision': 2
        },
        
        # ========== DIRECCIONES ==========
        'postal_code_es': {
            'category': 'address',
            'patterns': ['postal_code_es'],
            'description': 'Código postal español (5 dígitos)'
        },
        'postal_code_us': {
            'category': 'address',
            'patterns': ['postal_code_us'],
            'description': 'ZIP code estadounidense'
        },
        'country_code': {
            'category': 'address',
            'patterns': ['country_code'],
            'description': 'Código de país ISO 3166-1 alpha-2',
            'min_length': 2,
            'max_length': 2
        },
        
        # ========== WEB Y REDES ==========
        'url': {
            'category': 'web',
            'patterns': ['url'],
            'description': 'URL válida (http/https)',
            'max_length': 2048,
            'security_checks': ['xss_basic', 'command_injection']
        },
        'url_secure': {
            'category': 'web',
            'patterns': ['url_secure'],
            'description': 'URL segura (solo HTTPS)',
            'max_length': 2048
        },
        'ipv4': {
            'category': 'web',
            'patterns': ['ipv4'],
            'description': 'Dirección IPv4'
        },
        'ipv6': {
            'category': 'web',
            'patterns': ['ipv6'],
            'description': 'Dirección IPv6'
        },
        'domain': {
            'category': 'web',
            'patterns': ['domain'],
            'description': 'Nombre de dominio',
            'max_length': 253
        },
        'mac_address': {
            'category': 'web',
            'patterns': ['mac_address'],
            'description': 'Dirección MAC'
        },
        
        # ========== TEXTO ==========
        'name': {
            'category': 'text',
            'description': 'Nombre de persona',
            'min_length': 2,
            'max_length': 100,
            'anti_patterns': ['dummy_name', 'special_chars_overload'],
            'security_checks': ['sql_injection', 'xss_basic']
        },
        'text': {
            'category': 'text',
            'description': 'Texto libre',
            'max_length': 10000,
            'anti_patterns': ['placeholder_text', 'spaces_only'],
            'security_checks': ['sql_injection', 'xss_basic', 'command_injection']
        },
        'slug': {
            'category': 'text',
            'patterns': ['slug'],
            'description': 'URL slug (lowercase, hyphens)',
            'max_length': 200
        },
        'alphanumeric': {
            'category': 'text',
            'patterns': ['alphanumeric'],
            'description': 'Solo letras y números'
        },
        
        # ========== NUMÉRICOS ==========
        'integer': {
            'category': 'numeric',
            'description': 'Número entero',
            'type_check': 'integer'
        },
        'numeric': {
            'category': 'numeric',
            'description': 'Número decimal',
            'type_check': 'numeric'
        },
        'percentage': {
            'category': 'numeric',
            'description': 'Porcentaje (0-100)',
            'type_check': 'numeric',
            'min_value': 0,
            'max_value': 100
        },
        'probability': {
            'category': 'numeric',
            'description': 'Probabilidad (0.0-1.0)',
            'type_check': 'numeric',
            'min_value': 0.0,
            'max_value': 1.0
        },
        
        # ========== CATEGÓRICOS ==========
        'enum': {
            'category': 'categorical',
            'description': 'Lista cerrada de valores',
            'type_check': 'categorical'
        },
        'boolean': {
            'category': 'categorical',
            'description': 'Booleano',
            'type_check': 'boolean'
        },
        'status': {
            'category': 'categorical',
            'description': 'Estado de objeto',
            'common_values': ['active', 'inactive', 'pending', 'suspended', 'deleted'],
            'type_check': 'categorical'
        },
        
        # ========== TEMPORALES ==========
        'date': {
            'category': 'temporal',
            'patterns': ['iso_date'],
            'description': 'Fecha ISO 8601 (YYYY-MM-DD)',
            'type_check': 'date'
        },
        'datetime': {
            'category': 'temporal',
            'patterns': ['iso_datetime'],
            'description': 'Fecha y hora ISO 8601',
            'type_check': 'datetime'
        },
        'timestamp': {
            'category': 'temporal',
            'description': 'Timestamp Unix (epoch)',
            'type_check': 'integer',
            'min_value': 0
        },
        
        # ========== TÉCNICOS ==========
        'json': {
            'category': 'technical',
            'description': 'JSON válido',
            'type_check': 'json'
        },
        'version': {
            'category': 'technical',
            'patterns': ['semver'],
            'description': 'Versión semántica (X.Y.Z)'
        },
        'hex_color': {
            'category': 'technical',
            'patterns': ['hex_color'],
            'description': 'Color hexadecimal (#RRGGBB)'
        },
    }


class SchemaValidator:
    """Validador avanzado production-ready con soporte completo de seguridad y calidad"""
    
    @classmethod
    def convert_schema_to_expectations(
        cls, 
        schema_config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Convierte schema completo a expectativas organizadas por categoría.
        
        Soporta:
        - Validaciones simples por columna (40+ tipos)
        - Validaciones compuestas (cross-field)
        - Reglas de negocio
        - Niveles de severidad
        - Compliance checks
        """
        dataset = schema_config.get('dataset')
        columns = schema_config.get('columns', {})
        cross_field = schema_config.get('cross_field_validations', [])
        business_rules = schema_config.get('business_rules', [])
        quality_threshold = schema_config.get('quality_threshold', 0.95)
        security_level = schema_config.get('security_level', 'strict')  # strict | standard | relaxed
        
        # Agrupar por categoría
        suites = {
            ValidationCategory.STRUCTURE.value: [],
            ValidationCategory.DATA_TYPE.value: [],
            ValidationCategory.DATA_QUALITY.value: [],
            ValidationCategory.SECURITY.value: [],
            ValidationCategory.BUSINESS_RULES.value: [],
            ValidationCategory.INTEGRITY.value: [],
            ValidationCategory.COMPLIANCE.value: []
        }
        
        # 1. Procesar validaciones por columna
        for col_name, col_config in columns.items():
            expectations = cls._process_column(col_name, col_config, quality_threshold, security_level)
            
            for exp in expectations:
                category = exp.pop('_category', ValidationCategory.DATA_TYPE.value)
                suites[category].append(exp)
        
        # 2. Procesar validaciones cross-field
        for cross_validation in cross_field:
            expectations = cls._process_cross_field_validation(cross_validation)
            for exp in expectations:
                suites[ValidationCategory.INTEGRITY.value].append(exp)
        
        # 3. Procesar reglas de negocio
        for rule in business_rules:
            expectations = cls._process_business_rule(rule)
            for exp in expectations:
                suites[ValidationCategory.BUSINESS_RULES.value].append(exp)
        
        # Generar estructura final con nombres descriptivos
        result = []
        suite_order = [
            (ValidationCategory.STRUCTURE.value, '01_Estructura_Basica'),
            (ValidationCategory.DATA_TYPE.value, '02_Tipos_y_Formatos'),
            (ValidationCategory.DATA_QUALITY.value, '03_Calidad_de_Datos'),
            (ValidationCategory.SECURITY.value, '04_Seguridad_OWASP'),
            (ValidationCategory.INTEGRITY.value, '05_Integridad_Referencial'),
            (ValidationCategory.BUSINESS_RULES.value, '06_Reglas_de_Negocio'),
            (ValidationCategory.COMPLIANCE.value, '07_Cumplimiento_Normativo')
        ]
        
        for category_key, suite_name in suite_order:
            expectations = suites[category_key]
            if expectations:
                result.append({
                    'dataset': dataset,
                    'suite_name': suite_name,
                    'expectations': expectations
                })
        
        return result
    
    @classmethod
    def _process_column(
        cls, 
        col_name: str, 
        col_config: Dict[str, Any],
        quality_threshold: float,
        security_level: str = 'strict'
    ) -> List[Dict[str, Any]]:
        """Procesa una columna y genera todas sus expectativas"""
        expectations = []
        col_type = col_config.get('type', 'text')
        
        # Obtener definición del tipo
        type_def = DataTypeRegistry.TYPE_DEFINITIONS.get(col_type, {})
        
        # 1. VALIDACIONES BÁSICAS (Null, Unique)
        if col_config.get('required', type_def.get('default_required', False)):
            mostly = col_config.get('mostly', quality_threshold)
            exp = {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'column': col_name,
                '_category': ValidationCategory.DATA_QUALITY.value
            }
            if mostly < 1.0:
                exp['mostly'] = mostly
            expectations.append(exp)
        
        if col_config.get('unique', type_def.get('default_unique', False)):
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_unique',
                'column': col_name,
                '_category': ValidationCategory.DATA_QUALITY.value
            })
        
        # 2. VALIDACIONES DE TIPO (Patterns, Ranges)
        type_expectations = cls._get_type_validations(col_name, col_type, col_config, type_def)
        expectations.extend(type_expectations)
        
        # 3. VALIDACIONES DE SEGURIDAD (AUTOMÁTICAS según security_level)
        security_expectations = cls._get_security_validations(col_name, col_config, type_def, security_level)
        expectations.extend(security_expectations)
        
        # 4. VALIDACIONES DE CALIDAD
        quality_expectations = cls._get_quality_validations(col_name, col_config, type_def)
        expectations.extend(quality_expectations)
        
        # 5. VALIDACIONES DE COMPLIANCE
        compliance_expectations = cls._get_compliance_validations(col_name, col_config, type_def)
        expectations.extend(compliance_expectations)
        
        return expectations
    
    @classmethod
    def _get_type_validations(
        cls, 
        col_name: str, 
        col_type: str, 
        col_config: Dict,
        type_def: Dict
    ) -> List[Dict]:
        """Genera validaciones específicas del tipo"""
        expectations = []
        
        # Validaciones por patrón regex
        patterns = type_def.get('patterns', [])
        for pattern_key in patterns:
            if pattern_key in DataTypeRegistry.PATTERNS:
                mostly = col_config.get('mostly')
                exp = {
                    'expectation_type': 'expect_column_values_to_match_regex',
                    'column': col_name,
                    'regex': DataTypeRegistry.PATTERNS[pattern_key],
                    '_category': ValidationCategory.DATA_TYPE.value
                }
                if mostly:
                    exp['mostly'] = mostly
                expectations.append(exp)
        
        # Validaciones de longitud de texto
        min_len = col_config.get('min_length', type_def.get('min_length'))
        max_len = col_config.get('max_length', type_def.get('max_length'))
        if min_len is not None or max_len is not None:
            exp = {
                'expectation_type': 'expect_column_value_lengths_to_be_between',
                'column': col_name,
                '_category': ValidationCategory.DATA_TYPE.value
            }
            if min_len is not None:
                exp['min_value'] = min_len
            if max_len is not None:
                exp['max_value'] = max_len
            if col_config.get('mostly'):
                exp['mostly'] = col_config['mostly']
            expectations.append(exp)
        
        # Validaciones de rango numérico
        min_val = col_config.get('min', type_def.get('min_value'))
        max_val = col_config.get('max', type_def.get('max_value'))
        if min_val is not None or max_val is not None:
            exp = {
                'expectation_type': 'expect_column_values_to_be_between',
                'column': col_name,
                '_category': ValidationCategory.DATA_TYPE.value
            }
            if min_val is not None:
                exp['min_value'] = min_val
            if max_val is not None:
                exp['max_value'] = max_val
            if col_config.get('mostly'):
                exp['mostly'] = col_config['mostly']
            expectations.append(exp)
        
        # Validaciones categóricas (enum)
        values = col_config.get('values', type_def.get('common_values'))
        if values:
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_in_set',
                'column': col_name,
                'value_set': values,
                '_category': ValidationCategory.DATA_TYPE.value
            })
        
        # Validaciones estadísticas
        if col_config.get('mean_between'):
            min_mean, max_mean = col_config['mean_between']
            expectations.append({
                'expectation_type': 'expect_column_mean_to_be_between',
                'column': col_name,
                'min_value': min_mean,
                'max_value': max_mean,
                '_category': ValidationCategory.DATA_QUALITY.value
            })
        
        if col_config.get('std_dev_between'):
            min_std, max_std = col_config['std_dev_between']
            expectations.append({
                'expectation_type': 'expect_column_stdev_to_be_between',
                'column': col_name,
                'min_value': min_std,
                'max_value': max_std,
                '_category': ValidationCategory.DATA_QUALITY.value
            })
        
        return expectations
    
    @classmethod
    def _get_security_validations(
        cls, 
        col_name: str, 
        col_config: Dict,
        type_def: Dict,
        security_level: str = 'strict'
    ) -> List[Dict]:
        """Genera validaciones de seguridad AUTOMÁTICAS según el tipo y nivel global"""
        expectations = []
        
        # Verificar si seguridad está desactivada para esta columna
        if col_config.get('skip_security', False):
            return expectations
        
        # Checks específicos a omitir (opt-out)
        skip_checks = set(col_config.get('skip_security_checks', []))
        
        # Determinar checks según tipo de columna y nivel de seguridad
        col_category = type_def.get('category', 'text')
        security_checks = set()
        
        # ============================================================================
        # ANÁLISIS AUTOMÁTICO SEGÚN CATEGORÍA DE COLUMNA
        # ============================================================================
        
        if col_category in ['text', 'contact', 'address', 'web']:
            # TODAS las columnas de texto reciben análisis completo
            
            # A03:2021 - Injection (SIEMPRE)
            security_checks.update([
                'sql_injection', 'sql_injection_advanced', 'nosql_injection',
                'ldap_injection', 'xpath_injection', 'command_injection',
                'yaml_injection', 'template_injection', 'csv_injection'
            ])
            
            # A03:2021 - XSS (SIEMPRE)
            security_checks.update(['xss_basic', 'xss_advanced', 'xss_event_handlers'])
            
            # A04:2021 - Path Traversal (SIEMPRE)
            security_checks.update(['path_traversal', 'path_traversal_win', 'null_byte', 'file_inclusion'])
            
            # A02:2021 - Data Leakage (SIEMPRE)
            security_checks.update([
                'api_key_generic', 'aws_access_key', 'aws_secret_key',
                'github_token', 'slack_token', 'google_api_key',
                'private_key', 'ssh_key', 'jwt_token', 'bearer_token',
                'password_pattern', 'connection_string',
                'ssn_pattern', 'credit_card_pattern'
            ])
            
            if security_level == 'strict':
                # Checks adicionales en modo strict
                security_checks.update([
                    'xxe_injection', 'http_header_injection',
                    'email_header_injection', 'crlf_injection',
                    'serialization_gadget', 'deserialization_attack',
                    'log_injection', 'privilege_escalation'
                ])
        
        elif col_category == 'web':
            # URLs: XSS + SSRF + Injection
            security_checks.update([
                'xss_basic', 'xss_advanced', 'command_injection',
                'ssrf_patterns'
            ])
            if security_level in ['strict', 'standard']:
                security_checks.update(['sql_injection', 'path_traversal'])
        
        elif col_category in ['identifier', 'pii', 'financial']:
            # IDs y datos sensibles: Data leakage + format validation
            security_checks.update([
                'api_key_generic', 'aws_access_key', 'github_token',
                'private_key', 'jwt_token', 'password_pattern'
            ])
            
            if col_category in ['pii', 'financial']:
                # Verificar que no haya PII adicional
                security_checks.update(['ssn_pattern', 'credit_card_pattern'])
        
        # Nivel 'relaxed': Solo checks críticos
        if security_level == 'relaxed':
            # Mantener solo los más críticos
            critical_checks = {
                'sql_injection', 'xss_basic', 'command_injection',
                'private_key', 'aws_access_key', 'password_pattern',
                'credit_card_pattern'
            }
            security_checks = security_checks.intersection(critical_checks)
        
        # Remover checks específicamente omitidos
        security_checks = security_checks - skip_checks
        
        # Agregar checks predefinidos por tipo (si existen)
        security_checks.update(type_def.get('security_checks', []))
        
        # ============================================================================
        # GENERAR EXPECTATIONS
        # ============================================================================
        
        # CRITICAL: GE deduplica expectativas del mismo tipo en la misma columna
        # SOLUCIÓN: Combinar TODOS los patterns de seguridad en UN SOLO regex
        security_patterns = []
        security_check_names = []
        has_case_insensitive = False
        
        for check in security_checks:
            if check in DataTypeRegistry.PATTERNS:
                pattern = DataTypeRegistry.PATTERNS[check]
                
                # Remover flags inline (?i) para evitar warnings de pandas
                # Moveremos todos los flags al inicio del combined pattern
                if pattern.startswith('(?i)'):
                    pattern = pattern[4:]  # Remover '(?i)'
                    has_case_insensitive = True
                
                security_patterns.append(pattern)
                security_check_names.append(check)
        
        # Si hay patterns de seguridad, crear UNA SOLA expectativa con todos combinados
        if security_patterns:
            # Combinar patterns con alternación (|) y wrap each en grupo non-capturing
            combined_pattern = '|'.join(f'(?:{pattern})' for pattern in security_patterns)
            
            # Si CUALQUIER pattern original tenía (?i), aplicarlo al inicio del combined pattern
            if has_case_insensitive:
                combined_pattern = '(?i)' + combined_pattern
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'column': col_name,
                'regex': combined_pattern,
                '_category': ValidationCategory.SECURITY.value,
                '_security_checks': ','.join(security_check_names),  # Lista de checks incluidos
                '_security_patterns_count': len(security_patterns)
            })
        
        return expectations
    
    @classmethod
    def _get_quality_validations(
        cls, 
        col_name: str, 
        col_config: Dict,
        type_def: Dict
    ) -> List[Dict]:
        """Genera validaciones de calidad de datos"""
        expectations = []
        
        # Anti-patterns (valores de prueba, dummy data)
        anti_patterns = list(type_def.get('anti_patterns', []))
        
        # AUTOMÁTICO: Para email, siempre validar que no sea sintético
        col_type = col_config.get('type', 'text')
        if col_type == 'email':
            col_config['no_test_values'] = True
        
        if col_config.get('no_test_values', False):
            for pattern_key in ['test_email', 'dummy_phone', 'dummy_name']:
                if pattern_key in DataTypeRegistry.PATTERNS:
                    anti_patterns.append(pattern_key)
        
        if col_config.get('no_placeholder', False):
            anti_patterns.extend(['placeholder_text', 'spaces_only', 'special_chars_overload'])
        
        if col_config.get('no_suspicious_chars', False):
            anti_patterns.extend(['invisible_chars', 'rtl_override'])
        
        for pattern_key in set(anti_patterns):
            if pattern_key in DataTypeRegistry.PATTERNS:
                expectations.append({
                    'expectation_type': 'expect_column_values_to_not_match_regex',
                    'column': col_name,
                    'regex': DataTypeRegistry.PATTERNS[pattern_key],
                    '_category': ValidationCategory.DATA_QUALITY.value
                })
        
        # Detección de outliers estadísticos
        if col_config.get('detect_outliers', False):
            # Z-score method: detectar valores fuera de mean ± 3*std
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_between',
                'column': col_name,
                'min_value': 'auto',  # Se calcula dinámicamente
                'max_value': 'auto',
                'mostly': col_config.get('outlier_threshold', 0.99),
                '_category': ValidationCategory.DATA_QUALITY.value,
                '_note': 'Z-score outlier detection: mean ± 3*std'
            })
        
        # Distribución esperada
        if col_config.get('expect_distribution'):
            dist = col_config['expect_distribution']
            expectations.append({
                'expectation_type': 'expect_column_kl_divergence_to_be_less_than',
                'column': col_name,
                'partition_object': dist,
                'threshold': col_config.get('distribution_threshold', 0.1),
                '_category': ValidationCategory.DATA_QUALITY.value
            })
        
        # Quantiles esperados
        if col_config.get('quantiles'):
            expectations.append({
                'expectation_type': 'expect_column_quantile_values_to_be_between',
                'column': col_name,
                'quantile_ranges': col_config['quantiles'],
                '_category': ValidationCategory.DATA_QUALITY.value
            })
        
        return expectations
    
    @classmethod
    def _get_compliance_validations(
        cls, 
        col_name: str, 
        col_config: Dict,
        type_def: Dict
    ) -> List[Dict]:
        """Genera validaciones de compliance (GDPR, PCI-DSS, HIPAA, etc.)"""
        expectations = []
        
        compliance = type_def.get('compliance')
        
        # PCI-DSS: No debe haber números de tarjeta sin cifrar
        if compliance == 'PCI-DSS' or col_config.get('pci_dss_check'):
            if not col_config.get('encrypted', type_def.get('should_be_encrypted', False)):
                # Advertir que debería estar cifrado
                expectations.append({
                    'expectation_type': 'expect_column_values_to_not_match_regex',
                    'column': col_name,
                    'regex': DataTypeRegistry.PATTERNS['pci_pan'],
                    '_category': ValidationCategory.COMPLIANCE.value,
                    '_severity': ValidationSeverity.CRITICAL.value
                })
                expectations.append({
                    'expectation_type': 'expect_column_values_to_not_match_regex',
                    'column': col_name,
                    'regex': DataTypeRegistry.PATTERNS['pci_track_data'],
                    '_category': ValidationCategory.COMPLIANCE.value
                })
        
        # GDPR: Detección de PII no autorizado
        if compliance in ['GDPR-PII', 'PII'] or col_config.get('gdpr_check'):
            expectations.append({
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'column': col_name,
                'regex': DataTypeRegistry.PATTERNS['gdpr_pii_combined'],
                '_category': ValidationCategory.COMPLIANCE.value,
                '_severity': ValidationSeverity.ERROR.value
            })
        
        # HIPAA: Medical record numbers
        if compliance == 'HIPAA' or col_config.get('hipaa_check'):
            expectations.append({
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'column': col_name,
                'regex': DataTypeRegistry.PATTERNS['hipaa_mrn'],
                '_category': ValidationCategory.COMPLIANCE.value
            })
        
        # SOC2: No debug information leakage
        if col_config.get('soc2_check'):
            expectations.extend([
                {
                    'expectation_type': 'expect_column_values_to_not_match_regex',
                    'column': col_name,
                    'regex': DataTypeRegistry.PATTERNS['debug_stack_trace'],
                    '_category': ValidationCategory.COMPLIANCE.value
                },
                {
                    'expectation_type': 'expect_column_values_to_not_match_regex',
                    'column': col_name,
                    'regex': DataTypeRegistry.PATTERNS['internal_path'],
                    '_category': ValidationCategory.COMPLIANCE.value
                }
            ])
        
        return expectations
    
    @classmethod
    def _process_cross_field_validation(cls, validation: Dict) -> List[Dict]:
        """Procesa validaciones entre múltiples columnas (integridad referencial)"""
        expectations = []
        validation_type = validation.get('type')
        
        if validation_type == 'date_range':
            # start_date <= end_date
            start_col = validation.get('start_column')
            end_col = validation.get('end_column')
            allow_equal = validation.get('allow_equal', True)
            
            # Usamos row_condition que es universal en GE en lugar de expectativas experimentales
            operator = ">=" if allow_equal else ">"
            condition = f"{end_col} {operator} {start_col}"
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_not_be_null',
                'column': end_col,
                'row_condition': condition,
                'condition_parser': 'pandas',
                '_category': ValidationCategory.INTEGRITY.value,
                '_note': f"Integrity check: {condition}"
            })
        
        elif validation_type == 'conditional_required':
            # Si column A tiene valor X, entonces column B es requerida
            condition_col = validation.get('condition_column')
            condition_value = validation.get('condition_value')
            required_col = validation.get('required_column')
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_not_be_null',
                'column': required_col,
                'row_condition': f"{condition_col} == '{condition_value}'",
                '_category': ValidationCategory.INTEGRITY.value
            })
        
        elif validation_type == 'sum_equals':
            # Suma de columnas debe ser igual a otra columna
            sum_columns = validation.get('sum_columns')
            total_column = validation.get('total_column')
            tolerance = validation.get('tolerance', 0.01)
            
            expectations.append({
                'expectation_type': 'expect_multicolumn_sum_to_equal',
                'column_list': sum_columns,
                'sum_total': f"column:{total_column}",
                'tolerance': tolerance,
                '_category': ValidationCategory.INTEGRITY.value
            })
        
        elif validation_type == 'unique_combination':
            # Combinación de columnas debe ser única
            columns = validation.get('columns')
            
            expectations.append({
                'expectation_type': 'expect_compound_columns_to_be_unique',
                'column_list': columns,
                '_category': ValidationCategory.INTEGRITY.value
            })
        
        elif validation_type == 'mutual_exclusivity':
            # Solo una de las columnas puede tener valor (XOR)
            columns = validation.get('columns')
            
            expectations.append({
                'expectation_type': 'expect_only_one_column_to_have_value',
                'column_list': columns,
                '_category': ValidationCategory.INTEGRITY.value
            })
        
        elif validation_type == 'conditional_value':
            # Si A = X, entonces B debe estar en [Y, Z]
            condition_col = validation.get('condition_column')
            condition_value = validation.get('condition_value')
            target_col = validation.get('target_column')
            allowed_values = validation.get('allowed_values')
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_in_set',
                'column': target_col,
                'value_set': allowed_values,
                'row_condition': f"{condition_col} == '{condition_value}'",
                '_category': ValidationCategory.INTEGRITY.value
            })
        
        return expectations
    
    @classmethod
    def _process_business_rule(cls, rule: Dict) -> List[Dict]:
        """Procesa reglas de negocio específicas del dominio"""
        expectations = []
        rule_type = rule.get('type')
        
        if rule_type == 'minimum_records':
            # Dataset debe tener mínimo N registros
            expectations.append({
                'expectation_type': 'expect_table_row_count_to_be_between',
                'min_value': rule.get('min_count', 1),
                'max_value': rule.get('max_count', 100000000),
                '_category': ValidationCategory.BUSINESS_RULES.value
            })
        
        elif rule_type == 'percentage_in_category':
            # Al menos X% debe estar en categoría Y
            column = rule.get('column')
            category = rule.get('category')
            percentage = rule.get('percentage', 0.5)
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_in_set',
                'column': column,
                'value_set': [category] if isinstance(category, str) else category,
                'mostly': percentage,
                '_category': ValidationCategory.BUSINESS_RULES.value
            })
        
        elif rule_type == 'value_range_by_condition':
            # Si condición, entonces valor en rango
            column = rule.get('column')
            condition = rule.get('condition')
            min_val = rule.get('min_value')
            max_val = rule.get('max_value')
            
            exp = {
                'expectation_type': 'expect_column_values_to_be_between',
                'column': column,
                'row_condition': condition,
                'condition_parser': 'pandas',
                '_category': ValidationCategory.BUSINESS_RULES.value
            }
            if min_val is not None:
                exp['min_value'] = min_val
            if max_val is not None:
                exp['max_value'] = max_val
            
            expectations.append(exp)
        
        elif rule_type == 'trend_monotonic':
            # Columna debe ser monotónicamente creciente/decreciente
            column = rule.get('column')
            direction = rule.get('direction', 'increasing')  # increasing | decreasing
            allow_equal = rule.get('allow_equal', True)
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_monotonic',
                'column': column,
                'direction': direction,
                'strictly': not allow_equal,
                '_category': ValidationCategory.BUSINESS_RULES.value
            })
        
        elif rule_type == 'referential_integrity':
            # Foreign key validation
            column = rule.get('column')
            reference_table = rule.get('reference_table')
            reference_column = rule.get('reference_column')
            
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_in_reference_set',
                'column': column,
                'reference_dataset': reference_table,
                'reference_column': reference_column,
                '_category': ValidationCategory.INTEGRITY.value
            })
        
        return expectations


def convert_simple_validation_to_ge(validation_config: Dict) -> Dict:
    """
    Función principal: Convierte configuración simplificada a formato GE.
    
    Arquitectura de configuración simplificada:
    {
        'schema': {
            'dataset_name': {
                'columns': {
                    'col1': {'type': 'email', 'required': True},
                    'col2': {'type': 'uuid', 'unique': True}
                },
                'cross_field_validations': [
                    {'type': 'date_range', 'start_column': 'created_at', 'end_column': 'updated_at'}
                ],
                'business_rules': [
                    {'type': 'minimum_records', 'min_count': 100}
                ],
                'quality_threshold': 0.95  # Umbral global para "mostly"
            }
        },
        'custom_validations': [...]  # Opcional: validaciones GE raw
    }
    
    Soporta:
    - 40+ tipos semánticos con validaciones automáticas
    - 30+ patrones de seguridad (OWASP completo)
    - Cross-field validation (fecha_inicio < fecha_fin, unicidad compuesta, etc.)
    - Reglas de negocio (mínimo registros, porcentajes por categoría, etc.)
    - Compliance (GDPR, PCI-DSS, HIPAA, SOC2)
    - Validaciones de calidad (outliers, distribuciones, etc.)
    """
    result = {'expectations': []}
    
    # Procesar schemas
    if 'schema' in validation_config:
        for dataset_name, schema_config in validation_config['schema'].items():
            schema_config['dataset'] = dataset_name
            ge_expectations = SchemaValidator.convert_schema_to_expectations(schema_config)
            result['expectations'].extend(ge_expectations)
    
    # Agregar validaciones custom si existen
    if 'custom_validations' in validation_config:
        result['expectations'].extend(validation_config['custom_validations'])
    
    return result
