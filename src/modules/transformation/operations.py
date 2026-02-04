# src/modules/transformation/operations.py
"""
Operaciones de transformación de datos
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, List
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    """Resultado de una transformación"""
    operation_name: str
    operation_type: str
    success: bool
    records_affected: int = 0
    original_shape: tuple = (0, 0)
    new_shape: tuple = (0, 0)
    timestamp: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None
    changes: List[dict] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convertir a diccionario"""
        return {
            'operation_name': self.operation_name,
            'operation_type': self.operation_type,
            'success': self.success,
            'records_affected': self.records_affected,
            'original_shape': self.original_shape,
            'new_shape': self.new_shape,
            'timestamp': self.timestamp,
            'error_message': self.error_message
        }


@dataclass
class TransformOperation:
    """
    Definición de una operación de transformación.
    
    Ejemplos:
        # Limpieza de espacios
        op = TransformOperation(
            name="trim_strings",
            operation_type="clean",
            function=lambda df: df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        )
        
        # Normalización de columna
        op = TransformOperation(
            name="normalize_amount",
            operation_type="normalize",
            columns=["amount"],
            params={"method": "min-max"}
        )
    """
    name: str
    operation_type: str
    columns: Optional[List[str]] = None
    function: Optional[Callable] = None
    params: dict = field(default_factory=dict)
    description: Optional[str] = None
    track_changes: bool = True
    
    def apply(self, df: pd.DataFrame) -> tuple[pd.DataFrame, TransformResult]:
        """
        Aplicar transformación sobre un DataFrame.
        
        Args:
            df: DataFrame a transformar
            
        Returns:
            Tuple (DataFrame transformado, TransformResult)
        """
        result = TransformResult(
            operation_name=self.name,
            operation_type=self.operation_type,
            success=True,
            original_shape=df.shape
        )
        
        try:
            # Aplicar según tipo de operación
            if self.operation_type == "clean":
                df_transformed = self._apply_clean(df, result)
            
            elif self.operation_type == "normalize":
                df_transformed = self._apply_normalize(df, result)
            
            elif self.operation_type == "derive":
                df_transformed = self._apply_derive(df, result)
            
            elif self.operation_type == "aggregate":
                df_transformed = self._apply_aggregate(df, result)
            
            elif self.operation_type == "filter":
                df_transformed = self._apply_filter(df, result)
            
            elif self.operation_type == "custom" and self.function:
                df_transformed = self.function(df)
                result.records_affected = len(df_transformed)
            
            else:
                raise ValueError(f"Unknown operation type: {self.operation_type}")
            
            result.new_shape = df_transformed.shape
            logger.info(f"Transform '{self.name}' applied: {result.original_shape} -> {result.new_shape}")
            
            return df_transformed, result
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            result.new_shape = df.shape
            logger.error(f"Transform '{self.name}' failed: {e}")
            return df, result
    
    def _apply_clean(self, df: pd.DataFrame, result: TransformResult) -> pd.DataFrame:
        """Operaciones de limpieza"""
        df_clean = df.copy()
        
        clean_type = self.params.get('type', 'trim')
        target_cols = self.columns or df.select_dtypes(include=['object']).columns.tolist()
        
        if clean_type == 'trim':
            # Eliminar espacios en blanco
            for col in target_cols:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )
            result.records_affected = len(df_clean)
        
        elif clean_type == 'lowercase':
            # Convertir a minúsculas
            for col in target_cols:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: x.lower() if isinstance(x, str) else x
                    )
            result.records_affected = len(df_clean)
        
        elif clean_type == 'remove_nulls':
            # Eliminar filas con nulos
            initial_rows = len(df_clean)
            df_clean = df_clean.dropna(subset=target_cols)
            result.records_affected = initial_rows - len(df_clean)
        
        elif clean_type == 'fill_nulls':
            # Rellenar nulos
            fill_value = self.params.get('fill_value', 0)
            df_clean[target_cols] = df_clean[target_cols].fillna(fill_value)
            result.records_affected = len(df_clean)
        
        return df_clean
    
    def _apply_normalize(self, df: pd.DataFrame, result: TransformResult) -> pd.DataFrame:
        """Normalización de datos"""
        df_norm = df.copy()
        
        method = self.params.get('method', 'min-max')
        target_cols = self.columns or df.select_dtypes(include=['number']).columns.tolist()
        
        if method == 'min-max':
            # Normalización Min-Max [0, 1]
            for col in target_cols:
                if col in df_norm.columns:
                    min_val = df_norm[col].min()
                    max_val = df_norm[col].max()
                    if max_val > min_val:
                        df_norm[col] = (df_norm[col] - min_val) / (max_val - min_val)
        
        elif method == 'z-score':
            # Normalización Z-score
            for col in target_cols:
                if col in df_norm.columns:
                    mean = df_norm[col].mean()
                    std = df_norm[col].std()
                    if std > 0:
                        df_norm[col] = (df_norm[col] - mean) / std
        
        result.records_affected = len(df_norm)
        return df_norm
    
    def _apply_derive(self, df: pd.DataFrame, result: TransformResult) -> pd.DataFrame:
        """Derivación de nuevos campos"""
        df_derived = df.copy()
        
        new_column = self.params.get('new_column')
        expression = self.params.get('expression')
        
        if new_column and expression:
            # Evaluar expresión para crear nueva columna
            df_derived[new_column] = df_derived.eval(expression)
            result.records_affected = len(df_derived)
            result.changes.append({
                'type': 'column_added',
                'column': new_column
            })
        
        return df_derived
    
    def _apply_aggregate(self, df: pd.DataFrame, result: TransformResult) -> pd.DataFrame:
        """Agregaciones"""
        group_by = self.params.get('group_by', [])
        agg_functions = self.params.get('agg_functions', {})
        
        if group_by and agg_functions:
            df_agg = df.groupby(group_by).agg(agg_functions).reset_index()
            result.records_affected = len(df) - len(df_agg)
            return df_agg
        
        return df
    
    def _apply_filter(self, df: pd.DataFrame, result: TransformResult) -> pd.DataFrame:
        """Filtrado de registros"""
        condition = self.params.get('condition')
        
        if condition:
            df_filtered = df.query(condition)
            result.records_affected = len(df) - len(df_filtered)
            return df_filtered
        
        return df
