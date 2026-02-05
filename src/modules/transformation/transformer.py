# src/modules/transformation/transformer.py
"""
Clase principal del módulo de transformación
"""

import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

from .operations import TransformOperation, TransformResult

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transformador principal de datos.
    
    Permite aplicar múltiples transformaciones de forma secuencial.
    
    Ejemplo:
        transformer = DataTransformer()
        
        # Agregar operaciones
        transformer.add_clean_operation("trim_strings", clean_type="trim")
        transformer.add_normalize_operation("normalize_amount", columns=["amount"])
        
        # Aplicar transformaciones
        df_transformed = transformer.transform(df)
    """
    
    def __init__(self, name: str = "default_transformer"):
        self.name = name
        self.operations: List[TransformOperation] = []
        self.last_transform_time: Optional[datetime] = None
        self.last_results: List[TransformResult] = []
        self.transformation_history: List[Dict[str, Any]] = []
        
        logger.info(f"DataTransformer '{name}' initialized")
    
    # ========================================
    # Métodos de conveniencia para operaciones
    # ========================================
    
    def add_clean_operation(
        self,
        name: str,
        clean_type: str = "trim",
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """
        Agregar operación de limpieza.
        
        Args:
            name: Nombre de la operación
            clean_type: Tipo de limpieza (trim, lowercase, remove_nulls, fill_nulls)
            columns: Columnas objetivo
            **kwargs: Parámetros adicionales (fill_value, etc.)
        """
        params = {'type': clean_type, **kwargs}
        
        op = TransformOperation(
            name=name,
            operation_type="clean",
            columns=columns,
            params=params,
            description=f"Clean operation: {clean_type}"
        )
        self.operations.append(op)
        logger.info(f"Added clean operation: {name}")
    
    def add_normalize_operation(
        self,
        name: str,
        columns: Optional[List[str]] = None,
        method: str = "min-max"
    ) -> None:
        """
        Agregar operación de normalización.
        
        Args:
            name: Nombre de la operación
            columns: Columnas a normalizar
            method: Método (min-max, z-score)
        """
        op = TransformOperation(
            name=name,
            operation_type="normalize",
            columns=columns,
            params={'method': method},
            description=f"Normalize using {method}"
        )
        self.operations.append(op)
        logger.info(f"Added normalize operation: {name}")
    
    def add_derive_operation(
        self,
        name: str,
        new_column: str,
        expression: str
    ) -> None:
        """
        Agregar operación de derivación de campo.
        
        Args:
            name: Nombre de la operación
            new_column: Nombre de la nueva columna
            expression: Expresión para calcular el valor
        """
        op = TransformOperation(
            name=name,
            operation_type="derive",
            params={'new_column': new_column, 'expression': expression},
            description=f"Derive {new_column} = {expression}"
        )
        self.operations.append(op)
        logger.info(f"Added derive operation: {name}")
    
    def add_aggregate_operation(
        self,
        name: str,
        group_by: List[str],
        agg_functions: Dict[str, str]
    ) -> None:
        """
        Agregar operación de agregación.
        
        Args:
            name: Nombre de la operación
            group_by: Columnas para agrupar
            agg_functions: Funciones de agregación por columna
        """
        op = TransformOperation(
            name=name,
            operation_type="aggregate",
            params={'group_by': group_by, 'agg_functions': agg_functions},
            description=f"Aggregate by {group_by}"
        )
        self.operations.append(op)
        logger.info(f"Added aggregate operation: {name}")
    
    def add_filter_operation(
        self,
        name: str,
        condition: str
    ) -> None:
        """
        Agregar operación de filtrado.
        
        Args:
            name: Nombre de la operación
            condition: Condición de filtro (expresión pandas query)
        """
        op = TransformOperation(
            name=name,
            operation_type="filter",
            params={'condition': condition},
            description=f"Filter: {condition}"
        )
        self.operations.append(op)
        logger.info(f"Added filter operation: {name}")
    
    def add_custom_operation(self, operation: TransformOperation) -> None:
        """Agregar operación personalizada"""
        self.operations.append(operation)
        logger.info(f"Added custom operation: {operation.name}")
    
    # ========================================
    # Ejecución de transformaciones
    # ========================================
    
    def transform(self, df: pd.DataFrame, track_history: bool = True) -> pd.DataFrame:
        """
        Aplicar todas las transformaciones sobre un DataFrame.
        
        Args:
            df: DataFrame a transformar
            track_history: Si registrar el historial de transformaciones
            
        Returns:
            DataFrame transformado
        """
        logger.info(f"[{self.name}] Starting transformation pipeline on {len(df)} records")
        
        self.last_transform_time = datetime.now()
        self.last_results = []
        
        df_current = df.copy()
        
        for operation in self.operations:
            df_current, result = operation.apply(df_current)
            self.last_results.append(result)
            
            if not result.success:
                logger.warning(f"Operation '{operation.name}' failed: {result.error_message}")
            
            if track_history:
                self.transformation_history.append({
                    'timestamp': result.timestamp,
                    'operation': operation.name,
                    'type': operation.operation_type,
                    'success': result.success,
                    'records_affected': result.records_affected
                })
        
        logger.info(
            f"[{self.name}] Transformation complete: "
            f"{len(self.operations)} operations applied, "
            f"{df.shape} -> {df_current.shape}"
        )
        
        return df_current
    
    def get_summary(self) -> Dict[str, Any]:
        """Obtener resumen de la última transformación"""
        if not self.last_results:
            return {
                'error': 'No transformation results available',
                'last_transformation': None
            }
        
        successful_ops = sum(1 for r in self.last_results if r.success)
        total_records_affected = sum(r.records_affected for r in self.last_results)
        
        return {
            'transformer_name': self.name,
            'total_operations': len(self.last_results),
            'successful_operations': successful_ops,
            'failed_operations': len(self.last_results) - successful_ops,
            'total_records_affected': total_records_affected,
            'last_transformation_time': self.last_transform_time
        }
    
    def clear_operations(self) -> None:
        """Limpiar todas las operaciones"""
        self.operations = []
        logger.info(f"[{self.name}] Cleared all operations")
    
    def print_report(self) -> None:
        """Imprimir reporte en consola"""
        print(f"TRANSFORMATION REPORT - {self.name}")
        
        summary = self.get_summary()
        
        print(f"\nOverview:")
        print(f"  Total Operations: {summary['total_operations']}")
        print(f"  Successful: {summary['successful_operations']} ✓")
        print(f"  Failed: {summary['failed_operations']} ✗")
        print(f"  Records Affected: {summary['total_records_affected']}")
        
        print(f"\nOperations Applied:")
        for result in self.last_results:
            status = "✓" if result.success else "✗"
            print(f"  {status} {result.operation_name} ({result.operation_type})")
            print(f"     Shape: {result.original_shape} -> {result.new_shape}")
            if result.records_affected > 0:
                print(f"     Affected: {result.records_affected} records")
        
    
    def apply_operation(self, df: pd.DataFrame, operation_config: Dict[str, Any]) -> pd.DataFrame:
        """
        Aplicar una operación de transformación desde configuración.
        
        Args:
            df: DataFrame a transformar
            operation_config: Configuración de la operación desde YAML
            
        Returns:
            DataFrame transformado
        """
        op_type = operation_config.get('type')
        
        try:
            if op_type == 'filter':
                # Filtrar filas usando pandas query o eval
                condition = operation_config.get('condition')
                if condition:
                    logger.info(f"      Filter: {condition}")
                    try:
                        # Intentar con query primero (más eficiente)
                        df = df.query(condition).copy()
                    except Exception as e:
                        # Si falla, usar eval con las columnas del df
                        logger.warning(f"      Query failed, using eval: {e}")
                        # Crear namespace con todas las columnas
                        namespace = {col: df[col] for col in df.columns}
                        mask = eval(condition, namespace)
                        df = df[mask].copy()
                    logger.info(f"      Result: {len(df)} rows")
            
            elif op_type == 'derive':
                # Crear nueva columna derivada
                new_column = operation_config.get('new_column')
                expression = operation_config.get('expression')
                
                if new_column and expression:
                    logger.info(f"      Derive: {new_column}")
                    # Importar pandas para usar en eval
                    import pandas as pd
                    import numpy as np
                    
                    # Evaluar expresión con acceso a columnas del DataFrame
                    # Crear diccionario con todas las columnas
                    col_dict = {col: df[col] for col in df.columns}
                    namespace = {'pd': pd, 'np': np, 'df': df, **col_dict}
                    
                    df = df.copy()
                    df[new_column] = eval(expression, namespace)
                    logger.info(f"      Created column: {new_column}")
            
            elif op_type == 'transform':
                # Transformar columna existente
                column = operation_config.get('column')
                expression = operation_config.get('expression')
                
                if column and expression:
                    logger.info(f"      Transform: {column} with {expression}")
                    df = df.copy()
                    # Aplicar método string (ej: str.title())
                    if expression.startswith('str.'):
                        method_name = expression.replace('str.', '').replace('()', '')
                        method = getattr(df[column].str, method_name)
                        df[column] = method()
                    else:
                        # Evaluar expresión general
                        df[column] = df[column].apply(lambda x: eval(expression, {'x': x}))
                    logger.info(f"      Transformed column: {column}")
            
            elif op_type == 'clean':
                # Limpieza de datos
                method = operation_config.get('method', 'trim')
                columns = operation_config.get('columns')
                
                if method == 'trim' and columns:
                    logger.info(f"      Clean (trim): {columns}")
                    for col in columns:
                        if col in df.columns and df[col].dtype == 'object':
                            df[col] = df[col].str.strip()
            
            else:
                logger.warning(f"      Unknown operation type: {op_type}")
        
        except Exception as e:
            logger.error(f"      Operation failed: {e}")
            raise
        
        return df
