"""
example_usage.py
============================================================================
Ejemplo de uso del ValidationMapper
============================================================================
Este script demuestra cómo usar el ValidationMapper para convertir
configuraciones simplificadas en expectativas de Great Expectations.
============================================================================
"""

import json
import yaml
from pathlib import Path

# Importar el mapper (ajustar path según tu estructura)
# from validation.validation_mapper import ValidationMapper, load_and_map_validation_config
# from validation.type_mappings import TYPE_MAPPINGS, SECURITY_CHECKS

# Para este ejemplo, asumimos que los módulos están en el mismo directorio
import sys
sys.path.append('.')


def example_1_basic_usage():
    """
    Ejemplo 1: Uso básico - cargar configuración y generar expectativas
    """
    print("\n" + "="*80)
    print("EJEMPLO 1: USO BÁSICO")
    print("="*80)
    
    # Configuración simplificada
    config = {
        'validation': {
            'default_level': 'strict',
            'security': {
                'enabled': True,
                'checks': {
                    'sql_injection': True,
                    'xss': True,
                    'sensitive_data_exposure': True
                }
            },
            'schemas': {
                'customers': {
                    'columns': {
                        'customer_id': {
                            'type': 'uuid',
                            'required': True,
                            'unique': True
                        },
                        'name': {
                            'type': 'person_name',
                            'required': True
                        },
                        'email': {
                            'type': 'email',
                            'required': True,
                            'unique': 0.95
                        },
                        'status': {
                            'type': 'enum',
                            'values': ['active', 'inactive'],
                            'required': True
                        }
                    }
                }
            }
        }
    }
    
    # Crear mapper
    from validation_mapper import ValidationMapper
    mapper = ValidationMapper(config)
    
    # Generar expectativas
    expectations = mapper.map_schema('customers', config['validation']['schemas']['customers'])
    
    print(f"\n✓ Generadas {len(expectations)} expectativas")
    print("\nPrimeras 3 expectativas:")
    for i, exp in enumerate(expectations[:3], 1):
        print(f"\n{i}. {exp['expectation_type']}")
        print(f"   Columna: {exp.get('kwargs', {}).get('column', 'N/A')}")
        print(f"   Descripción: {exp.get('meta', {}).get('description', 'N/A')}")


def example_2_business_rules():
    """
    Ejemplo 2: Reglas de negocio
    """
    print("\n" + "="*80)
    print("EJEMPLO 2: REGLAS DE NEGOCIO")
    print("="*80)
    
    config = {
        'validation': {
            'schemas': {
                'transactions': {
                    'columns': {
                        'amount': {
                            'type': 'currency',
                            'min': 0,
                            'max': 100000
                        },
                        'status': {
                            'type': 'enum',
                            'values': ['pending', 'completed', 'failed'],
                            'required': True
                        }
                    },
                    'business_rules': [
                        {
                            'name': 'minimum_completion_rate',
                            'description': 'Al menos 70% completadas',
                            'metric': 'percentage_where',
                            'column': 'status',
                            'value': 'completed',
                            'threshold': '>= 70%',
                            'severity': 'warning'
                        },
                        {
                            'name': 'high_value_validation',
                            'description': 'Montos altos deben estar completados',
                            'when': 'amount > 50000',
                            'expect': {
                                'status': ['completed']
                            },
                            'severity': 'critical'
                        }
                    ]
                }
            }
        }
    }
    
    from validation_mapper import ValidationMapper
    mapper = ValidationMapper(config)
    
    expectations = mapper.map_schema('transactions', config['validation']['schemas']['transactions'])
    
    # Filtrar reglas de negocio
    business_rule_expectations = [
        exp for exp in expectations 
        if 'business_rule' in exp.get('meta', {})
    ]
    
    print(f"\n✓ Generadas {len(business_rule_expectations)} reglas de negocio")
    for exp in business_rule_expectations:
        print(f"\n• {exp.get('meta', {}).get('business_rule')}")
        print(f"  Tipo: {exp['expectation_type']}")
        print(f"  Severidad: {exp.get('meta', {}).get('severity')}")


def example_3_security_validations():
    """
    Ejemplo 3: Validaciones de seguridad
    """
    print("\n" + "="*80)
    print("EJEMPLO 3: VALIDACIONES DE SEGURIDAD")
    print("="*80)
    
    config = {
        'validation': {
            'security': {
                'enabled': True,
                'checks': {
                    'sql_injection': True,
                    'xss': True,
                    'command_injection': True,
                    'sensitive_data_exposure': True
                }
            },
            'schemas': {
                'user_inputs': {
                    'columns': {
                        'comment': {
                            'type': 'text',
                            'max_length': 1000
                        },
                        'address': {
                            'type': 'text',
                            'max_length': 200
                        }
                    }
                }
            }
        }
    }
    
    from validation_mapper import ValidationMapper
    mapper = ValidationMapper(config)
    
    expectations = mapper.map_schema('user_inputs', config['validation']['schemas']['user_inputs'])
    
    # Filtrar validaciones de seguridad
    security_expectations = [
        exp for exp in expectations 
        if 'security_check' in exp.get('meta', {})
    ]
    
    print(f"\n✓ Generadas {len(security_expectations)} validaciones de seguridad")
    
    # Agrupar por tipo de check
    from collections import defaultdict
    by_check = defaultdict(int)
    for exp in security_expectations:
        check_type = exp['meta']['security_check']
        by_check[check_type] += 1
    
    print("\nValidaciones por tipo:")
    for check_type, count in by_check.items():
        print(f"  • {check_type}: {count} validaciones")


def example_4_full_pipeline_config():
    """
    Ejemplo 4: Cargar desde archivo YAML y generar suite completa
    """
    print("\n" + "="*80)
    print("EJEMPLO 4: CONFIGURACIÓN COMPLETA DESDE YAML")
    print("="*80)
    
    # Cargar configuración desde archivo
    yaml_path = 'simplified_pipeline.yml'
    
    if not Path(yaml_path).exists():
        print(f"\n⚠ Archivo {yaml_path} no encontrado. Saltando este ejemplo.")
        return
    
    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    from validation_mapper import ValidationMapper
    mapper = ValidationMapper(config)
    
    # Generar suite completa para el dataset 'raw_customers'
    suite = mapper.generate_great_expectations_suite('raw_customers')
    
    print(f"\n✓ Suite generada: {suite['expectation_suite_name']}")
    print(f"  Total de expectativas: {len(suite['expectations'])}")
    
    # Estadísticas
    expectation_types = {}
    for exp in suite['expectations']:
        exp_type = exp['expectation_type']
        expectation_types[exp_type] = expectation_types.get(exp_type, 0) + 1
    
    print("\nDistribución de expectativas:")
    for exp_type, count in sorted(expectation_types.items(), key=lambda x: -x[1])[:10]:
        # Simplificar nombre para display
        simple_name = exp_type.replace('expect_column_', '').replace('expect_', '')
        print(f"  • {simple_name}: {count}")
    
    # Guardar suite en JSON
    output_path = 'generated_suite.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(suite, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Suite guardada en: {output_path}")


def example_5_compare_verbosity():
    """
    Ejemplo 5: Comparación de verbosidad (antes vs después)
    """
    print("\n" + "="*80)
    print("EJEMPLO 5: COMPARACIÓN DE VERBOSIDAD")
    print("="*80)
    
    # Configuración SIMPLIFICADA (nueva sintaxis)
    simplified_config = """
validation:
  level: strict
  security: true
  
  schemas:
    customers:
      columns:
        customer_id:
          type: uuid
          required: true
        
        email:
          type: email
          required: true
          unique: 0.95
        
        status:
          type: enum
          values: [active, inactive]
"""
    
    # Configuración VERBOSA (sintaxis antigua)
    verbose_config = """
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
  - expectation_type: "expect_column_proportion_of_unique_values_to_be_between"
    column: "email"
    min_value: 0.95
    max_value: 1.0
  - expectation_type: "expect_column_values_to_match_regex"
    column: "email"
    regex: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"
  - expectation_type: "expect_column_values_to_not_match_regex"
    column: "email"
    regex: "(?:test@|dummy@|fake@|example\\\\.com|localhost)"
  - expectation_type: "expect_column_values_to_not_be_null"
    column: "status"
  - expectation_type: "expect_column_values_to_be_in_set"
    column: "status"
    value_set: ["active", "inactive"]
  # + validaciones de seguridad (30+ líneas más)...
"""
    
    simplified_lines = len(simplified_config.strip().split('\n'))
    verbose_lines = len(verbose_config.strip().split('\n'))
    
    reduction = ((verbose_lines - simplified_lines) / verbose_lines) * 100
    
    print(f"\n📊 MÉTRICAS DE SIMPLIFICACIÓN:")
    print(f"  • Configuración simplificada: {simplified_lines} líneas")
    print(f"  • Configuración verbosa: {verbose_lines} líneas")
    print(f"  • Reducción: {reduction:.1f}%")
    print(f"\n✨ Resultado: ~{reduction:.0f}% menos código, 100% más legible!")
    
    print("\n" + "─"*80)
    print("CONFIGURACIÓN SIMPLIFICADA:")
    print("─"*80)
    print(simplified_config)
    
    print("\n" + "─"*80)
    print("CONFIGURACIÓN VERBOSA (parcial):")
    print("─"*80)
    print(verbose_config)


def example_6_available_types():
    """
    Ejemplo 6: Mostrar todos los tipos disponibles
    """
    print("\n" + "="*80)
    print("EJEMPLO 6: TIPOS DE DATOS DISPONIBLES")
    print("="*80)
    
    from type_mappings import TYPE_MAPPINGS
    
    categories = {
        'Identificadores': ['uuid', 'id'],
        'Datos Personales': ['person_name', 'email', 'phone_es', 'phone_international'],
        'Texto': ['text', 'text_short', 'text_long'],
        'Numéricos': ['integer', 'float', 'currency', 'percentage'],
        'Temporales': ['datetime', 'date', 'timestamp'],
        'Categóricos': ['enum', 'boolean'],
        'Especiales': ['url', 'ip_address', 'json']
    }
    
    print("\n📚 TIPOS DISPONIBLES POR CATEGORÍA:\n")
    
    for category, types in categories.items():
        print(f"\n{category}:")
        for dtype in types:
            if dtype in TYPE_MAPPINGS:
                desc = TYPE_MAPPINGS[dtype]['description']
                num_validations = len(TYPE_MAPPINGS[dtype]['expectations'])
                print(f"  • {dtype:20} - {desc} ({num_validations} validaciones)")
    
    print(f"\n✓ Total: {len(TYPE_MAPPINGS)} tipos de datos disponibles")


def main():
    """Ejecutar todos los ejemplos"""
    print("\n" + "="*80)
    print("EJEMPLOS DE USO - VALIDATION MAPPER")
    print("="*80)
    
    try:
        example_1_basic_usage()
    except Exception as e:
        print(f"\n❌ Error en ejemplo 1: {e}")
    
    try:
        example_2_business_rules()
    except Exception as e:
        print(f"\n❌ Error en ejemplo 2: {e}")
    
    try:
        example_3_security_validations()
    except Exception as e:
        print(f"\n❌ Error en ejemplo 3: {e}")
    
    try:
        example_4_full_pipeline_config()
    except Exception as e:
        print(f"\n❌ Error en ejemplo 4: {e}")
    
    try:
        example_5_compare_verbosity()
    except Exception as e:
        print(f"\n❌ Error en ejemplo 5: {e}")
    
    try:
        example_6_available_types()
    except Exception as e:
        print(f"\n❌ Error en ejemplo 6: {e}")
    
    print("\n" + "="*80)
    print("✓ EJEMPLOS COMPLETADOS")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
