"""
Tests para Schema Validator - Validación Simplificada
"""

import pytest
from src.modules.validation.schema_validator import (
    SchemaValidator,
    convert_simple_validation_to_ge
)


class TestSchemaValidator:
    """Tests para conversión de schema simplificado a GE expectations"""
    
    def test_uuid_type_conversion(self):
        """Test conversión de tipo UUID"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'customer_id': {
                    'type': 'uuid',
                    'required': True,
                    'unique': True
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        
        # Debe generar al menos 3 expectativas (required, unique, regex)
        assert len(result) > 0
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Verificar que contiene las expectativas esperadas
        exp_types = [exp['expectation_type'] for exp in all_expectations]
        assert 'expect_column_values_to_not_be_null' in exp_types
        assert 'expect_column_values_to_be_unique' in exp_types
        assert 'expect_column_values_to_match_regex' in exp_types
    
    def test_email_type_conversion(self):
        """Test conversión de tipo email"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'email': {
                    'type': 'email',
                    'required': True,
                    'no_test_values': True
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Debe incluir validación de formato email y detección de test values
        exp_types = [exp['expectation_type'] for exp in all_expectations]
        assert 'expect_column_values_to_not_be_null' in exp_types
        assert 'expect_column_values_to_match_regex' in exp_types
        
        # Verificar que hay una expectativa de no match para test values
        regex_expectations = [
            exp for exp in all_expectations 
            if exp['expectation_type'] == 'expect_column_values_to_not_match_regex'
        ]
        assert len(regex_expectations) > 0
    
    def test_numeric_type_with_range(self):
        """Test conversión de tipo numeric con rangos"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'lifetime_value': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 10000,
                    'mean_between': [1000, 5000]
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Debe incluir validación de rango y media
        exp_types = [exp['expectation_type'] for exp in all_expectations]
        assert 'expect_column_values_to_be_between' in exp_types
        assert 'expect_column_mean_to_be_between' in exp_types
        
        # Verificar valores
        range_exp = [
            exp for exp in all_expectations 
            if exp['expectation_type'] == 'expect_column_values_to_be_between'
        ][0]
        assert range_exp['min_value'] == 0
        assert range_exp['max_value'] == 10000
    
    def test_enum_type_conversion(self):
        """Test conversión de tipo enum"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'status': {
                    'type': 'enum',
                    'values': ['active', 'inactive', 'suspended'],
                    'required': True
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Debe incluir validación de conjunto
        exp_types = [exp['expectation_type'] for exp in all_expectations]
        assert 'expect_column_values_to_be_in_set' in exp_types
        
        # Verificar valores del enum
        enum_exp = [
            exp for exp in all_expectations 
            if exp['expectation_type'] == 'expect_column_values_to_be_in_set'
        ][0]
        assert set(enum_exp['value_set']) == {'active', 'inactive', 'suspended'}
    
    def test_injection_detection(self):
        """Test detección de inyección"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'name': {
                    'type': 'name',
                    'no_injection': True
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Debe incluir 3 checks de inyección (SQL, XSS, Command)
        not_match_expectations = [
            exp for exp in all_expectations 
            if exp['expectation_type'] == 'expect_column_values_to_not_match_regex'
        ]
        assert len(not_match_expectations) >= 3
    
    def test_phone_es_type(self):
        """Test tipo teléfono español"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'phone': {
                    'type': 'phone_es',
                    'mostly': 0.9
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Debe incluir validación de regex con mostly
        regex_exp = [
            exp for exp in all_expectations 
            if exp['expectation_type'] == 'expect_column_values_to_match_regex'
        ][0]
        assert 'mostly' in regex_exp
        assert regex_exp['mostly'] == 0.9
    
    def test_complete_conversion(self):
        """Test conversión completa con schema + custom validations"""
        validation_config = {
            'schema': {
                'raw_customers': {
                    'columns': {
                        'customer_id': {
                            'type': 'uuid',
                            'required': True,
                            'unique': True
                        },
                        'email': {
                            'type': 'email',
                            'required': True
                        }
                    }
                }
            },
            'custom_validations': [
                {
                    'dataset': 'raw_customers',
                    'suite_name': 'custom_suite',
                    'expectations': [
                        {
                            'expectation_type': 'expect_table_row_count_to_be_between',
                            'min_value': 1,
                            'max_value': 1000000
                        }
                    ]
                }
            ]
        }
        
        result = convert_simple_validation_to_ge(validation_config)
        
        # Debe contener expectations del schema + custom
        assert 'expectations' in result
        assert len(result['expectations']) > 1
        
        # Verificar que contiene las custom validations
        datasets = [exp['dataset'] for exp in result['expectations']]
        assert 'raw_customers' in datasets
        
        # Verificar que contiene la custom expectation
        all_suite_names = [exp['suite_name'] for exp in result['expectations']]
        assert 'custom_suite' in all_suite_names
    
    def test_text_type_with_length(self):
        """Test tipo text con longitudes"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'address': {
                    'type': 'text',
                    'min_length': 10,
                    'max_length': 200,
                    'mostly': 0.9
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        all_expectations = []
        for suite in result:
            all_expectations.extend(suite['expectations'])
        
        # Debe incluir validación de longitud
        length_exp = [
            exp for exp in all_expectations 
            if exp['expectation_type'] == 'expect_column_value_lengths_to_be_between'
        ]
        assert len(length_exp) > 0
        assert length_exp[0]['min_value'] == 10
        assert length_exp[0]['max_value'] == 200
        assert length_exp[0].get('mostly') == 0.9
    
    def test_suite_classification(self):
        """Test clasificación de expectativas en suites"""
        schema_config = {
            'dataset': 'test_data',
            'columns': {
                'customer_id': {
                    'type': 'uuid',
                    'required': True,
                    'unique': True
                },
                'name': {
                    'type': 'name',
                    'no_injection': True
                }
            }
        }
        
        result = SchemaValidator.convert_schema_to_expectations(schema_config)
        
        # Debe tener múltiples suites
        assert len(result) > 1
        
        # Verificar nombres de suites
        suite_names = [suite['suite_name'] for suite in result]
        assert any('formato' in name or 'tipos' in name for name in suite_names)
        assert any('seguridad' in name or 'security' in name for name in suite_names)


class TestSchemaValidatorPatterns:
    """Tests para patrones regex de SchemaValidator"""
    
    def test_uuid_pattern(self):
        """Test patrón UUID"""
        import re
        pattern = SchemaValidator.PATTERNS['uuid']
        
        # UUIDs válidos
        assert re.match(pattern, '550e8400-e29b-41d4-a716-446655440000')
        assert re.match(pattern, 'a1b2c3d4-5678-90ab-cdef-1234567890ab')
        
        # UUIDs inválidos
        assert not re.match(pattern, '550e8400-e29b-41d4-a716')  # Incompleto
        assert not re.match(pattern, 'not-a-uuid')
    
    def test_email_pattern(self):
        """Test patrón email"""
        import re
        pattern = SchemaValidator.PATTERNS['email']
        
        # Emails válidos
        assert re.match(pattern, 'user@example.com')
        assert re.match(pattern, 'test.user+tag@domain.co.uk')
        
        # Emails inválidos
        assert not re.match(pattern, 'invalid.email')
        assert not re.match(pattern, '@example.com')
    
    def test_phone_es_pattern(self):
        """Test patrón teléfono español"""
        import re
        pattern = SchemaValidator.PATTERNS['phone_es']
        
        # Teléfonos válidos
        assert re.match(pattern, '+34612345678')
        assert re.match(pattern, '+34912345678')
        
        # Teléfonos inválidos
        assert not re.match(pattern, '612345678')  # Sin +34
        assert not re.match(pattern, '+34512345678')  # No empieza con 6-9
    
    def test_sql_injection_pattern(self):
        """Test patrón SQL injection"""
        import re
        pattern = SchemaValidator.PATTERNS['sql_injection']
        
        # Debe detectar injection
        assert re.search(pattern, "' OR '1'='1")
        assert re.search(pattern, "admin'--")
        assert re.search(pattern, "1; DROP TABLE users")
        
        # No debe dar falsos positivos
        assert not re.search(pattern, "John O'Connor")  # Apóstrofe legítimo


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
