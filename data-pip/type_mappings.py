"""
validation/type_mappings.py
============================================================================
Type Mappings - Semantic Data Types to Great Expectations
============================================================================
Este módulo define el mapeo entre tipos de datos semánticos user-friendly
y las expectativas correspondientes de Great Expectations.

Cada tipo semántico se expande automáticamente en múltiples expectativas
de validación, reduciendo la configuración necesaria por parte del usuario.
============================================================================
"""

from typing import Dict, List, Any, Optional


# ============================================================================
# MAPEO DE TIPOS DE DATOS SEMÁNTICOS
# ============================================================================

TYPE_MAPPINGS: Dict[str, Dict[str, Any]] = {
    # ========================================================================
    # IDENTIFICADORES
    # ========================================================================
    'uuid': {
        'description': 'Identificador único universal (UUID v4)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'meta': {'description': 'UUID no puede ser nulo'}
            },
            {
                'expectation_type': 'expect_column_values_to_be_unique',
                'meta': {'description': 'UUID debe ser único'}
            },
            {
                'expectation_type': 'expect_column_values_to_match_regex',
                'kwargs': {
                    'regex': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                },
                'meta': {'description': 'UUID debe cumplir formato estándar'}
            }
        ]
    },
    
    'id': {
        'description': 'Identificador numérico único',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'meta': {'description': 'ID no puede ser nulo'}
            },
            {
                'expectation_type': 'expect_column_values_to_be_unique',
                'meta': {'description': 'ID debe ser único'}
            },
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'int'},
                'meta': {'description': 'ID debe ser entero'}
            }
        ]
    },
    
    # ========================================================================
    # DATOS PERSONALES
    # ========================================================================
    'person_name': {
        'description': 'Nombre de persona (con validación anti-injection)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'meta': {'description': 'Nombre no puede ser nulo'}
            },
            {
                'expectation_type': 'expect_column_value_lengths_to_be_between',
                'kwargs': {'min_value': 3, 'max_value': 100},
                'meta': {'description': 'Nombre debe tener longitud razonable'}
            },
            {
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'kwargs': {
                    'regex': r"(?:'|--|;|union|select|drop|delete|insert|update|<script|javascript:|<img|onerror)"
                },
                'meta': {'description': 'Nombre no debe contener código malicioso (SQL injection/XSS)'}
            },
            {
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'kwargs': {
                    'regex': r'(?:\d{4}[\s-]\d{4}[\s-]\d{4}[\s-]\d{4}|api[_-]?key|secret|token|password|PRIVATE.*KEY)'
                },
                'meta': {'description': 'Nombre no debe contener datos sensibles'}
            }
        ]
    },
    
    'email': {
        'description': 'Dirección de correo electrónico',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'meta': {'description': 'Email no puede ser nulo'}
            },
            {
                'expectation_type': 'expect_column_values_to_match_regex',
                'kwargs': {
                    'regex': r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
                },
                'meta': {'description': 'Email debe tener formato válido'}
            },
            {
                'expectation_type': 'expect_column_value_lengths_to_be_between',
                'kwargs': {'min_value': 5, 'max_value': 100},
                'meta': {'description': 'Email debe tener longitud razonable'}
            },
            {
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'kwargs': {
                    'regex': r'(?:test@|dummy@|fake@|example\.com|localhost)'
                },
                'meta': {'description': 'Email no debe ser placeholder/testing'}
            },
            {
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'kwargs': {
                    'regex': r'(?:\$ne|\$gt|\$where|\$regex)'
                },
                'meta': {'description': 'Email no debe contener NoSQL injection'}
            }
        ]
    },
    
    'phone_es': {
        'description': 'Número de teléfono español (+34)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_match_regex',
                'kwargs': {
                    'regex': r'^\+34',
                    'mostly': 0.9
                },
                'meta': {'description': 'Teléfono debe comenzar con +34'}
            },
            {
                'expectation_type': 'expect_column_values_to_not_match_regex',
                'kwargs': {
                    'regex': r'^(?:000|111|999)'
                },
                'meta': {'description': 'Teléfono no debe ser número dummy'}
            }
        ]
    },
    
    'phone_international': {
        'description': 'Número de teléfono internacional',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_match_regex',
                'kwargs': {
                    'regex': r'^\+[1-9]\d{1,14}$',
                    'mostly': 0.9
                },
                'meta': {'description': 'Teléfono debe tener formato internacional válido'}
            }
        ]
    },
    
    # ========================================================================
    # TIPOS DE TEXTO
    # ========================================================================
    'text': {
        'description': 'Texto genérico (con opción de validación de seguridad)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'str'},
                'meta': {'description': 'Debe ser texto'}
            }
        ]
    },
    
    'text_short': {
        'description': 'Texto corto (hasta 255 caracteres)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'str'},
                'meta': {'description': 'Debe ser texto'}
            },
            {
                'expectation_type': 'expect_column_value_lengths_to_be_between',
                'kwargs': {'min_value': 1, 'max_value': 255},
                'meta': {'description': 'Texto debe tener máximo 255 caracteres'}
            }
        ]
    },
    
    'text_long': {
        'description': 'Texto largo (sin límite específico)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'str'},
                'meta': {'description': 'Debe ser texto'}
            }
        ]
    },
    
    # ========================================================================
    # TIPOS NUMÉRICOS
    # ========================================================================
    'integer': {
        'description': 'Número entero',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'int'},
                'meta': {'description': 'Debe ser número entero'}
            }
        ]
    },
    
    'float': {
        'description': 'Número decimal',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'float'},
                'meta': {'description': 'Debe ser número decimal'}
            }
        ]
    },
    
    'currency': {
        'description': 'Valor monetario (positivo por defecto)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'float'},
                'meta': {'description': 'Debe ser número decimal'}
            },
            {
                'expectation_type': 'expect_column_values_to_be_between',
                'kwargs': {'min_value': 0, 'max_value': None},
                'meta': {'description': 'Valor monetario debe ser no negativo'}
            }
        ]
    },
    
    'percentage': {
        'description': 'Porcentaje (0-100)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'float'},
                'meta': {'description': 'Debe ser número decimal'}
            },
            {
                'expectation_type': 'expect_column_values_to_be_between',
                'kwargs': {'min_value': 0, 'max_value': 100},
                'meta': {'description': 'Porcentaje debe estar entre 0 y 100'}
            }
        ]
    },
    
    # ========================================================================
    # TIPOS TEMPORALES
    # ========================================================================
    'datetime': {
        'description': 'Fecha y hora',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'datetime'},
                'meta': {'description': 'Debe ser fecha/hora válida'}
            }
        ]
    },
    
    'date': {
        'description': 'Solo fecha (sin hora)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'date'},
                'meta': {'description': 'Debe ser fecha válida'}
            }
        ]
    },
    
    'timestamp': {
        'description': 'Timestamp Unix (epoch)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_of_type',
                'kwargs': {'type_': 'int'},
                'meta': {'description': 'Timestamp debe ser entero'}
            },
            {
                'expectation_type': 'expect_column_values_to_be_between',
                'kwargs': {'min_value': 0, 'max_value': 2147483647},  # Max Unix timestamp 32-bit
                'meta': {'description': 'Timestamp debe estar en rango válido'}
            }
        ]
    },
    
    # ========================================================================
    # TIPOS CATEGÓRICOS
    # ========================================================================
    'enum': {
        'description': 'Valor de un conjunto predefinido',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_not_be_null',
                'meta': {'description': 'Valor enumerado no puede ser nulo'}
            },
            # La lista de valores se agrega dinámicamente desde el config
        ]
    },
    
    'boolean': {
        'description': 'Valor booleano (true/false)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_in_set',
                'kwargs': {'value_set': [True, False, 0, 1, 'true', 'false', 'True', 'False']},
                'meta': {'description': 'Debe ser valor booleano'}
            }
        ]
    },
    
    # ========================================================================
    # TIPOS ESPECIALES
    # ========================================================================
    'url': {
        'description': 'URL válida',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_match_regex',
                'kwargs': {
                    'regex': r'^https?://[^\s/$.?#].[^\s]*$'
                },
                'meta': {'description': 'Debe ser URL válida'}
            }
        ]
    },
    
    'ip_address': {
        'description': 'Dirección IP (v4)',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_match_regex',
                'kwargs': {
                    'regex': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
                },
                'meta': {'description': 'Debe ser dirección IP v4 válida'}
            }
        ]
    },
    
    'json': {
        'description': 'Cadena JSON válida',
        'expectations': [
            {
                'expectation_type': 'expect_column_values_to_be_json_parseable',
                'meta': {'description': 'Debe ser JSON válido'}
            }
        ]
    },
}


# ============================================================================
# VALIDACIONES DE SEGURIDAD
# ============================================================================

SECURITY_CHECKS: Dict[str, Dict[str, Any]] = {
    'sql_injection': {
        'description': 'Detecta intentos de SQL Injection',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:'|--|;|union|select|drop|delete|insert|update|exec|execute)",
        'severity': 'critical'
    },
    
    'xss': {
        'description': 'Detecta intentos de Cross-Site Scripting (XSS)',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:<script|javascript:|alert\(|<iframe|onerror|onload|eval\()",
        'severity': 'critical'
    },
    
    'nosql_injection': {
        'description': 'Detecta intentos de NoSQL Injection',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:\$ne|\$gt|\$lt|\$gte|\$lte|\$where|\$regex|\$in|\$nin)",
        'severity': 'critical'
    },
    
    'command_injection': {
        'description': 'Detecta intentos de Command Injection',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:;\s*(?:rm|cat|ls|wget|curl)|&&|\|\||`|\$\()",
        'severity': 'critical'
    },
    
    'path_traversal': {
        'description': 'Detecta intentos de Path Traversal',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:\.\./|\.\.\\|%2e%2e%2f|%2e%2e/)",
        'severity': 'high'
    },
    
    'sensitive_data_exposure': {
        'description': 'Detecta exposición de datos sensibles (tarjetas, API keys, passwords)',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}|api[_-]?key|secret|token|password|PRIVATE.*KEY|BEGIN.*PRIVATE)",
        'severity': 'critical'
    },
    
    'ssn_exposure': {
        'description': 'Detecta números de seguridad social / DNI',
        'expectation_type': 'expect_column_values_to_not_match_regex',
        'regex': r"(?:\d{3}-\d{2}-\d{4}|\d{9}|[0-9]{8}[A-Z])",  # SSN US / DNI España
        'severity': 'critical'
    },
}


# ============================================================================
# NIVELES DE VALIDACIÓN
# ============================================================================

VALIDATION_LEVELS: Dict[str, List[str]] = {
    'basic': [
        'type_validation',
        'null_checks'
    ],
    
    'standard': [
        'type_validation',
        'null_checks',
        'format_validation',
        'range_validation'
    ],
    
    'strict': [
        'type_validation',
        'null_checks',
        'format_validation',
        'range_validation',
        'business_rules',
        'outlier_detection'
    ],
    
    'paranoid': [
        'type_validation',
        'null_checks',
        'format_validation',
        'range_validation',
        'business_rules',
        'outlier_detection',
        'security_checks'
    ]
}


# ============================================================================
# TEMPLATES PREDEFINIDOS
# ============================================================================

SCHEMA_TEMPLATES: Dict[str, Dict[str, Any]] = {
    'customer_data_standard_v1': {
        'description': 'Template estándar para datos de clientes',
        'columns': {
            'customer_id': {'type': 'uuid', 'required': True, 'unique': True},
            'name': {'type': 'person_name', 'required': True},
            'email': {'type': 'email', 'required': True, 'unique': 0.95},
            'phone': {'type': 'phone_international', 'nullable': 0.1},
            'created_at': {'type': 'datetime', 'required': True},
            'updated_at': {'type': 'datetime', 'required': True},
        }
    },
    
    'transaction_data_standard_v1': {
        'description': 'Template estándar para transacciones',
        'columns': {
            'transaction_id': {'type': 'uuid', 'required': True, 'unique': True},
            'customer_id': {'type': 'uuid', 'required': True},
            'amount': {'type': 'currency', 'required': True, 'min': 0},
            'currency': {'type': 'enum', 'values': ['USD', 'EUR', 'GBP']},
            'status': {'type': 'enum', 'values': ['pending', 'completed', 'failed']},
            'timestamp': {'type': 'datetime', 'required': True},
        }
    },
}
