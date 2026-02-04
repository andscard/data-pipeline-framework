"""
Módulo de validación de esquemas con Pandera

Este módulo proporciona funcionalidades para validar esquemas de DataFrames
utilizando Pandera, con soporte para generación desde configuración YAML.
"""

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PanderaSchemaBuilder:
    """Constructor de esquemas Pandera desde configuración YAML"""
    
    def __init__(self, schema_config: Dict[str, Any]):
        """
        Args:
            schema_config: Configuración del esquema desde YAML
        """
        self.schema_config = schema_config
        self.schema = None
    
    def build(self) -> DataFrameSchema:
        """
        Construir esquema Pandera desde configuración.
        
        Returns:
            DataFrameSchema configurado
        """
        columns = {}
        
        for col_name, col_config in self.schema_config.items():
            columns[col_name] = self._build_column(col_name, col_config)
        
        self.schema = DataFrameSchema(
            columns=columns,
            strict=False,  # Permitir columnas adicionales
            coerce=True    # Intentar coerción de tipos
        )
        
        return self.schema
    
    def _build_column(self, col_name: str, col_config: Dict[str, Any]) -> Column:
        """
        Construir una columna Pandera desde configuración.
        
        Args:
            col_name: Nombre de la columna
            col_config: Configuración de la columna
        
        Returns:
            Column configurada
        """
        # Mapeo de tipos YAML a Pandera
        type_mapping = {
            'string': str,
            'int': int,
            'integer': int,
            'float': float,
            'double': float,
            'bool': bool,
            'boolean': bool,
            'datetime': 'datetime64[ns]',
            'date': 'datetime64[ns]'
        }
        
        dtype = type_mapping.get(col_config.get('type', 'string'), str)
        nullable = col_config.get('nullable', True)
        unique = col_config.get('unique', False)
        
        # Construir checks
        checks = []
        
        # Check de unicidad
        if unique:
            checks.append(Check(lambda s: ~s.duplicated().any(), 
                              error=f"Column '{col_name}' debe ser única"))
        
        # Checks personalizados
        if 'checks' in col_config:
            for check_config in col_config['checks']:
                check = self._build_check(col_name, check_config)
                if check:
                    checks.append(check)
        
        return Column(
            dtype=dtype,
            nullable=nullable,
            checks=checks,
            coerce=True
        )
    
    def _build_check(self, col_name: str, check_config: Dict[str, Any]) -> Optional[Check]:
        """
        Construir un check Pandera desde configuración.
        
        Args:
            col_name: Nombre de la columna
            check_config: Configuración del check
        
        Returns:
            Check configurado o None
        """
        # String length
        if 'str_length' in check_config:
            length_config = check_config['str_length']
            min_val = length_config.get('min_value')
            max_val = length_config.get('max_value')
            
            if min_val and max_val:
                return Check(
                    lambda s: s.str.len().between(min_val, max_val),
                    error=f"Column '{col_name}' debe tener longitud entre {min_val} y {max_val}"
                )
            elif min_val:
                return Check(
                    lambda s: s.str.len() >= min_val,
                    error=f"Column '{col_name}' debe tener longitud mínima de {min_val}"
                )
            elif max_val:
                return Check(
                    lambda s: s.str.len() <= max_val,
                    error=f"Column '{col_name}' debe tener longitud máxima de {max_val}"
                )
        
        # String matches (regex)
        if 'str_matches' in check_config:
            pattern = check_config['str_matches']
            return Check(
                lambda s: s.str.match(pattern),
                error=f"Column '{col_name}' debe coincidir con el patrón: {pattern}"
            )
        
        # Greater than or equal to
        if 'greater_than_or_equal_to' in check_config:
            min_val = check_config['greater_than_or_equal_to']
            return Check.greater_than_or_equal_to(
                min_val,
                error=f"Column '{col_name}' debe ser >= {min_val}"
            )
        
        # Less than
        if 'less_than' in check_config:
            max_val = check_config['less_than']
            return Check.less_than(
                max_val,
                error=f"Column '{col_name}' debe ser < {max_val}"
            )
        
        # In range
        if 'in_range' in check_config:
            range_config = check_config['in_range']
            min_val = range_config.get('min_value')
            max_val = range_config.get('max_value')
            
            # Si son fechas, convertir strings a datetime
            if isinstance(min_val, str):
                min_val = pd.Timestamp(min_val)
            if isinstance(max_val, str):
                max_val = pd.Timestamp(max_val)
            
            return Check.in_range(
                min_val, max_val,
                error=f"Column '{col_name}' debe estar entre {min_val} y {max_val}"
            )
        
        # In set
        if 'in_set' in check_config:
            allowed_values = check_config['in_set']
            return Check.isin(
                allowed_values,
                error=f"Column '{col_name}' debe estar en {allowed_values}"
            )
        
        logger.warning(f"Check no reconocido para columna '{col_name}': {check_config}")
        return None


class PanderaValidator:
    """Validador de DataFrames usando Pandera"""
    
    def __init__(self, schema: DataFrameSchema, source_name: str):
        """
        Args:
            schema: Esquema Pandera para validación
            source_name: Nombre de la fuente de datos
        """
        self.schema = schema
        self.source_name = source_name
        self.validation_results = []
    
    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validar DataFrame contra el esquema.
        
        Args:
            df: DataFrame a validar
        
        Returns:
            Diccionario con resultados de validación
        """
        try:
            # Validar DataFrame
            validated_df = self.schema.validate(df, lazy=True)
            
            result = {
                'source': self.source_name,
                'timestamp': datetime.now().isoformat(),
                'status': 'passed',
                'total_rows': len(df),
                'validated_rows': len(validated_df),
                'errors': [],
                'schema_errors': 0,
                'data_errors': 0
            }
            
            logger.info(f"✓ Validación exitosa para '{self.source_name}': {len(df)} registros")
            
        except pa.errors.SchemaErrors as e:
            # Capturar errores de validación
            error_df = e.failure_cases
            
            schema_errors = error_df[error_df['check'] == 'dtype_coerce_failure']
            data_errors = error_df[error_df['check'] != 'dtype_coerce_failure']
            
            result = {
                'source': self.source_name,
                'timestamp': datetime.now().isoformat(),
                'status': 'failed',
                'total_rows': len(df),
                'validated_rows': len(df) - len(error_df),
                'errors': self._format_errors(error_df),
                'schema_errors': len(schema_errors),
                'data_errors': len(data_errors)
            }
            
            logger.error(f"✗ Validación fallida para '{self.source_name}': "
                        f"{len(error_df)} errores encontrados")
        
        except Exception as e:
            result = {
                'source': self.source_name,
                'timestamp': datetime.now().isoformat(),
                'status': 'error',
                'total_rows': len(df),
                'validated_rows': 0,
                'errors': [str(e)],
                'schema_errors': 0,
                'data_errors': 0
            }
            
            logger.exception(f"Error inesperado al validar '{self.source_name}'")
        
        self.validation_results.append(result)
        return result
    
    def _format_errors(self, error_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Formatear errores de validación para reporte.
        
        Args:
            error_df: DataFrame de errores de Pandera
        
        Returns:
            Lista de errores formateados
        """
        errors = []
        
        for _, row in error_df.iterrows():
            errors.append({
                'column': row['column'],
                'check': row['check'],
                'index': int(row['index']) if pd.notna(row['index']) else None,
                'failure_case': str(row['failure_case']) if pd.notna(row['failure_case']) else None
            })
        
        return errors
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Obtener resumen de validaciones realizadas.
        
        Returns:
            Diccionario con resumen
        """
        if not self.validation_results:
            return {
                'total_validations': 0,
                'passed': 0,
                'failed': 0,
                'error': 0
            }
        
        passed = sum(1 for r in self.validation_results if r['status'] == 'passed')
        failed = sum(1 for r in self.validation_results if r['status'] == 'failed')
        error = sum(1 for r in self.validation_results if r['status'] == 'error')
        
        return {
            'total_validations': len(self.validation_results),
            'passed': passed,
            'failed': failed,
            'error': error,
            'success_rate': (passed / len(self.validation_results) * 100) if self.validation_results else 0
        }
