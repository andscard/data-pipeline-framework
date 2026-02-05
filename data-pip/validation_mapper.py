"""
validation/validation_mapper.py
============================================================================
Validation Mapper - Converts simplified YAML config to Great Expectations
============================================================================
Este módulo es el core de la capa de abstracción. Toma la configuración
simplificada del usuario y la traduce a expectativas de Great Expectations.

Funciones principales:
- Mapear tipos semánticos a expectativas específicas
- Aplicar validaciones de seguridad según configuración
- Expandir reglas de negocio a expectativas complejas
- Generar suites de validación completas
============================================================================
"""

import re
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
import yaml

from .type_mappings import (
    TYPE_MAPPINGS,
    SECURITY_CHECKS,
    VALIDATION_LEVELS,
    SCHEMA_TEMPLATES
)


# ============================================================================
# DATACLASSES PARA CONFIGURACIÓN
# ============================================================================

@dataclass
class ColumnConfig:
    """Configuración de una columna individual"""
    name: str
    type: str
    required: bool = False
    unique: Union[bool, float] = False
    nullable: float = 0.0
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min: Optional[float] = None
    max: Optional[float] = None
    mean_range: Optional[List[float]] = None
    values: Optional[List[Any]] = None  # Para enums
    outlier_detection: Optional[Dict[str, Any]] = None
    description: str = ""
    custom_expectations: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TableConfig:
    """Configuración a nivel de tabla"""
    row_count: Optional[Dict[str, int]] = None
    column_count: Optional[int] = None
    column_order: Optional[List[str]] = None
    unique_combinations: Optional[List[Dict[str, Any]]] = None


@dataclass
class BusinessRule:
    """Regla de negocio personalizada"""
    name: str
    description: str
    rule_type: str  # 'percentage_where', 'conditional', 'temporal'
    column: Optional[str] = None
    value: Optional[Any] = None
    threshold: Optional[str] = None
    when: Optional[str] = None
    expect: Optional[Dict[str, Any]] = None
    condition: Optional[str] = None
    severity: str = "warning"


# ============================================================================
# CLASE PRINCIPAL: ValidationMapper
# ============================================================================

class ValidationMapper:
    """
    Clase principal que mapea configuración simplificada a Great Expectations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa el mapper con la configuración del pipeline.
        
        Args:
            config: Diccionario con la configuración completa del pipeline
        """
        self.config = config
        self.validation_config = config.get('validation', {})
        self.default_level = self.validation_config.get('default_level', 'standard')
        self.security_config = self.validation_config.get('security', {})
        
    def map_all_schemas(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Mapea todos los schemas definidos en la configuración.
        
        Returns:
            Diccionario con {dataset_name: [expectations]}
        """
        schemas_config = self.validation_config.get('schemas', {})
        result = {}
        
        for dataset_name, schema_config in schemas_config.items():
            expectations = self.map_schema(dataset_name, schema_config)
            result[dataset_name] = expectations
            
        return result
    
    def map_schema(self, dataset_name: str, schema_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mapea un schema individual a expectativas de Great Expectations.
        
        Args:
            dataset_name: Nombre del dataset
            schema_config: Configuración del schema
            
        Returns:
            Lista de expectativas de Great Expectations
        """
        expectations = []
        
        # 1. Aplicar template si existe
        template_name = schema_config.get('template')
        if template_name and template_name in SCHEMA_TEMPLATES:
            template = SCHEMA_TEMPLATES[template_name]
            # Mergear columnas del template con las personalizadas
            base_columns = template.get('columns', {})
            custom_columns = schema_config.get('columns', {})
            columns = {**base_columns, **custom_columns}
        else:
            columns = schema_config.get('columns', {})
        
        # 2. Reglas de tabla
        table_rules = schema_config.get('table_rules', {})
        expectations.extend(self._map_table_rules(table_rules))
        
        # 3. Reglas de columnas
        for column_name, column_config in columns.items():
            column_expectations = self._map_column(column_name, column_config)
            expectations.extend(column_expectations)
        
        # 4. Aplicar validaciones de seguridad si están habilitadas
        if self.security_config.get('enabled', False):
            security_expectations = self._map_security_checks(columns)
            expectations.extend(security_expectations)
        
        # 5. Reglas de negocio
        business_rules = schema_config.get('business_rules', [])
        for rule in business_rules:
            rule_expectations = self._map_business_rule(rule)
            expectations.extend(rule_expectations)
        
        return expectations
    
    def _map_table_rules(self, table_rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Mapea reglas a nivel de tabla."""
        expectations = []
        
        # Row count
        if 'row_count' in table_rules:
            row_count = table_rules['row_count']
            expectations.append({
                'expectation_type': 'expect_table_row_count_to_be_between',
                'kwargs': {
                    'min_value': row_count.get('min', 1),
                    'max_value': row_count.get('max', None)
                },
                'meta': {
                    'description': row_count.get('description', 'Validación de conteo de filas')
                }
            })
        
        # Column count
        if 'column_count' in table_rules:
            expectations.append({
                'expectation_type': 'expect_table_column_count_to_equal',
                'kwargs': {'value': table_rules['column_count']},
                'meta': {'description': 'Validación de número de columnas'}
            })
        
        # Column order
        if 'column_order' in table_rules:
            expectations.append({
                'expectation_type': 'expect_table_columns_to_match_ordered_list',
                'kwargs': {'column_list': table_rules['column_order']},
                'meta': {'description': 'Validación de orden de columnas'}
            })
        
        # Unique combinations
        if 'unique_combinations' in table_rules:
            for combo in table_rules['unique_combinations']:
                expectations.append({
                    'expectation_type': 'expect_compound_columns_to_be_unique',
                    'kwargs': {'column_list': combo['columns']},
                    'meta': {
                        'description': combo.get('description', 'Combinación de columnas debe ser única')
                    }
                })
        
        return expectations
    
    def _map_column(self, column_name: str, column_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mapea una columna individual a expectativas.
        
        Args:
            column_name: Nombre de la columna
            column_config: Configuración de la columna
            
        Returns:
            Lista de expectativas para esta columna
        """
        expectations = []
        column_type = column_config.get('type', 'text')
        
        # 1. Obtener expectativas base del tipo
        if column_type in TYPE_MAPPINGS:
            type_expectations = TYPE_MAPPINGS[column_type]['expectations']
            for exp in type_expectations:
                exp_copy = exp.copy()
                exp_copy['kwargs'] = exp_copy.get('kwargs', {}).copy()
                exp_copy['kwargs']['column'] = column_name
                expectations.append(exp_copy)
        
        # 2. Aplicar modificadores según configuración
        
        # Required / Nullable
        if column_config.get('required', False):
            nullable_threshold = column_config.get('nullable', 0.0)
            if nullable_threshold > 0:
                # Permitir cierto % de nulos
                mostly = 1.0 - nullable_threshold
                expectations.append({
                    'expectation_type': 'expect_column_values_to_not_be_null',
                    'kwargs': {'column': column_name, 'mostly': mostly},
                    'meta': {'description': f'Máximo {nullable_threshold*100}% de valores nulos permitido'}
                })
            else:
                # Ya está incluido en el tipo, verificar que no esté duplicado
                has_null_check = any(
                    exp['expectation_type'] == 'expect_column_values_to_not_be_null' 
                    for exp in expectations
                )
                if not has_null_check:
                    expectations.append({
                        'expectation_type': 'expect_column_values_to_not_be_null',
                        'kwargs': {'column': column_name},
                        'meta': {'description': 'Columna no puede ser nula'}
                    })
        
        # Unique
        unique_value = column_config.get('unique')
        if unique_value is True:
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_unique',
                'kwargs': {'column': column_name},
                'meta': {'description': 'Valores deben ser únicos'}
            })
        elif isinstance(unique_value, (int, float)) and 0 < unique_value < 1:
            expectations.append({
                'expectation_type': 'expect_column_proportion_of_unique_values_to_be_between',
                'kwargs': {
                    'column': column_name,
                    'min_value': unique_value,
                    'max_value': 1.0
                },
                'meta': {'description': f'Al menos {unique_value*100}% de valores únicos'}
            })
        
        # Length constraints
        min_len = column_config.get('min_length')
        max_len = column_config.get('max_length')
        if min_len is not None or max_len is not None:
            expectations.append({
                'expectation_type': 'expect_column_value_lengths_to_be_between',
                'kwargs': {
                    'column': column_name,
                    'min_value': min_len,
                    'max_value': max_len
                },
                'meta': {'description': 'Validación de longitud de texto'}
            })
        
        # Numeric range
        min_val = column_config.get('min')
        max_val = column_config.get('max')
        if min_val is not None or max_val is not None:
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_between',
                'kwargs': {
                    'column': column_name,
                    'min_value': min_val,
                    'max_value': max_val
                },
                'meta': {'description': 'Validación de rango numérico'}
            })
        
        # Mean range
        mean_range = column_config.get('mean_range')
        if mean_range and len(mean_range) == 2:
            expectations.append({
                'expectation_type': 'expect_column_mean_to_be_between',
                'kwargs': {
                    'column': column_name,
                    'min_value': mean_range[0],
                    'max_value': mean_range[1]
                },
                'meta': {'description': 'Validación de promedio esperado'}
            })
        
        # Enum values
        if column_type == 'enum':
            values = column_config.get('values', [])
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_in_set',
                'kwargs': {
                    'column': column_name,
                    'value_set': values
                },
                'meta': {'description': f'Valores permitidos: {values}'}
            })
        
        # Outlier detection
        outlier_config = column_config.get('outlier_detection')
        if outlier_config and outlier_config.get('enabled', False):
            method = outlier_config.get('method', 'quantile')
            if method == 'quantile':
                quantiles = outlier_config.get('quantiles', [0.01, 0.99])
                expected_ranges = outlier_config.get('expected_ranges', [])
                
                quantile_ranges = {
                    'quantiles': quantiles,
                    'value_ranges': expected_ranges
                }
                
                expectations.append({
                    'expectation_type': 'expect_column_quantile_values_to_be_between',
                    'kwargs': {
                        'column': column_name,
                        'quantile_ranges': quantile_ranges
                    },
                    'meta': {'description': 'Detección de outliers por cuantiles'}
                })
        
        return expectations
    
    def _map_security_checks(self, columns: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Aplica validaciones de seguridad a columnas de texto.
        
        Args:
            columns: Diccionario de configuración de columnas
            
        Returns:
            Lista de expectativas de seguridad
        """
        expectations = []
        security_checks = self.security_config.get('checks', {})
        
        # Determinar qué columnas son de texto (susceptibles a injection)
        text_columns = [
            col_name for col_name, col_config in columns.items()
            if col_config.get('type') in ['text', 'text_short', 'text_long', 'person_name', 'email']
        ]
        
        for check_name, enabled in security_checks.items():
            if not enabled or check_name not in SECURITY_CHECKS:
                continue
            
            check_config = SECURITY_CHECKS[check_name]
            
            for column_name in text_columns:
                expectations.append({
                    'expectation_type': check_config['expectation_type'],
                    'kwargs': {
                        'column': column_name,
                        'regex': check_config['regex']
                    },
                    'meta': {
                        'description': check_config['description'],
                        'severity': check_config['severity'],
                        'security_check': check_name
                    }
                })
        
        return expectations
    
    def _map_business_rule(self, rule: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mapea una regla de negocio a expectativas.
        
        Args:
            rule: Configuración de la regla de negocio
            
        Returns:
            Lista de expectativas correspondientes
        """
        expectations = []
        rule_name = rule.get('name', 'unnamed_rule')
        metric = rule.get('metric')
        
        # Tipo 1: Percentage where (e.g., "al menos 50% de clientes activos")
        if metric == 'percentage_where':
            column = rule['column']
            value = rule['value']
            threshold = rule.get('threshold', '>= 50%')
            
            # Parsear threshold
            threshold_match = re.match(r'([<>=]+)\s*(\d+(?:\.\d+)?)%?', threshold)
            if threshold_match:
                operator, percent = threshold_match.groups()
                percent = float(percent) / 100.0
                
                if operator in ['>=', '>']:
                    # Al menos X% deben tener el valor
                    expectations.append({
                        'expectation_type': 'expect_column_values_to_be_in_set',
                        'kwargs': {
                            'column': column,
                            'value_set': [value],
                            'mostly': percent
                        },
                        'meta': {
                            'description': rule.get('description', ''),
                            'business_rule': rule_name,
                            'severity': rule.get('severity', 'warning')
                        }
                    })
                elif operator in ['<=', '<']:
                    # Máximo X% pueden tener el valor
                    expectations.append({
                        'expectation_type': 'expect_column_values_to_not_be_in_set',
                        'kwargs': {
                            'column': column,
                            'value_set': [value],
                            'mostly': 1.0 - percent
                        },
                        'meta': {
                            'description': rule.get('description', ''),
                            'business_rule': rule_name,
                            'severity': rule.get('severity', 'warning')
                        }
                    })
        
        # Tipo 2: Conditional (e.g., "si LTV > 20000, entonces status = active")
        elif 'when' in rule and 'expect' in rule:
            when_condition = rule['when']
            expectations_dict = rule['expect']
            
            for column, expected_values in expectations_dict.items():
                if not isinstance(expected_values, list):
                    expected_values = [expected_values]
                
                expectations.append({
                    'expectation_type': 'expect_column_values_to_be_in_set',
                    'kwargs': {
                        'column': column,
                        'value_set': expected_values,
                        'row_condition': when_condition
                    },
                    'meta': {
                        'description': rule.get('description', ''),
                        'business_rule': rule_name,
                        'severity': rule.get('severity', 'warning')
                    }
                })
        
        # Tipo 3: Generic condition (e.g., "registration_date <= last_login")
        elif 'condition' in rule:
            condition = rule['condition']
            applies_to = rule.get('applies_to_rows')
            
            # Great Expectations no tiene expect_condition directa, 
            # usar SQL o Pandas expression
            expectations.append({
                'expectation_type': 'expect_column_pair_values_to_be_in_set',  # Adaptar según necesidad
                'kwargs': {
                    'column_A': 'registration_date',  # Inferir de la condición
                    'column_B': 'last_login',
                    # Nota: Esto requiere parsing más avanzado de la condición
                },
                'meta': {
                    'description': rule.get('description', ''),
                    'business_rule': rule_name,
                    'severity': rule.get('severity', 'error'),
                    'note': 'Validación temporal - requiere implementación personalizada'
                }
            })
        
        return expectations
    
    def generate_great_expectations_suite(self, dataset_name: str) -> Dict[str, Any]:
        """
        Genera una suite completa de Great Expectations para un dataset.
        
        Args:
            dataset_name: Nombre del dataset
            
        Returns:
            Diccionario con la suite en formato Great Expectations
        """
        schemas = self.validation_config.get('schemas', {})
        if dataset_name not in schemas:
            raise ValueError(f"Dataset '{dataset_name}' no encontrado en configuración")
        
        schema_config = schemas[dataset_name]
        expectations = self.map_schema(dataset_name, schema_config)
        
        suite = {
            'expectation_suite_name': f'{dataset_name}_suite',
            'data_asset_type': 'Dataset',
            'meta': {
                'generated_by': 'ValidationMapper',
                'dataset': dataset_name,
                'validation_level': self.default_level
            },
            'expectations': expectations
        }
        
        return suite


# ============================================================================
# FUNCIÓN HELPER PARA CARGAR Y MAPEAR
# ============================================================================

def load_and_map_validation_config(yaml_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Carga un archivo YAML de configuración y mapea todas las validaciones.
    
    Args:
        yaml_path: Ruta al archivo YAML de configuración
        
    Returns:
        Diccionario con {dataset_name: [expectations]}
    """
    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    mapper = ValidationMapper(config)
    return mapper.map_all_schemas()


def generate_ge_suite_from_config(yaml_path: str, dataset_name: str) -> Dict[str, Any]:
    """
    Genera una suite de Great Expectations desde un archivo de configuración.
    
    Args:
        yaml_path: Ruta al archivo YAML
        dataset_name: Nombre del dataset a procesar
        
    Returns:
        Suite de Great Expectations
    """
    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    mapper = ValidationMapper(config)
    return mapper.generate_great_expectations_suite(dataset_name)
