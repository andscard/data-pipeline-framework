"""
Conector para archivos Parquet

Proporciona funcionalidades para leer y escribir archivos Parquet
utilizando pandas y pyarrow.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class ParquetConnector:
    """Conector para archivos Parquet"""
    
    def __init__(self, file_path: str, **options):
        """
        Args:
            file_path: Ruta al archivo Parquet
            **options: Opciones adicionales
        """
        self.file_path = Path(file_path)
        self.options = options
        self.df = None
    
    def connect(self) -> bool:
        """
        Verificar que el archivo existe.
        
        Returns:
            True si el archivo existe
        """
        if not self.file_path.exists():
            logger.error(f"Archivo no encontrado: {self.file_path}")
            return False
        
        logger.info(f"✓ Archivo Parquet encontrado: {self.file_path}")
        return True
    
    def read(self) -> pd.DataFrame:
        """
        Leer datos del archivo Parquet.
        
        Returns:
            DataFrame con los datos leídos
        """
        try:
            # Opciones para read_parquet
            read_options = {
                'engine': self.options.get('engine', 'pyarrow'),
                'columns': self.options.get('columns', None),
                'use_nullable_dtypes': self.options.get('use_nullable_dtypes', False),
            }
            
            # Leer Parquet
            self.df = pd.read_parquet(self.file_path, **read_options)
            
            logger.info(f"✓ Parquet leído: {len(self.df)} filas, {len(self.df.columns)} columnas")
            logger.info(f"  Tamaño del archivo: {self._get_file_size_mb():.2f} MB")
            
            return self.df
            
        except Exception as e:
            logger.error(f"Error al leer Parquet: {e}")
            raise
    
    def write(self, df: pd.DataFrame, output_path: Optional[str] = None, **write_options) -> bool:
        """
        Escribir DataFrame a archivo Parquet.
        
        Args:
            df: DataFrame a escribir
            output_path: Ruta de salida (usa self.file_path si es None)
            **write_options: Opciones adicionales para to_parquet
        
        Returns:
            True si la escritura fue exitosa
        """
        try:
            path = Path(output_path) if output_path else self.file_path
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Opciones para to_parquet
            options = {
                'engine': write_options.get('engine', 'pyarrow'),
                'compression': write_options.get('compression', 'snappy'),
                'index': write_options.get('index', False),
            }
            
            df.to_parquet(path, **options)
            
            logger.info(f"✓ Parquet escrito: {path} ({len(df)} filas)")
            logger.info(f"  Compresión: {options['compression']}")
            logger.info(f"  Tamaño: {self._get_file_size_mb(path):.2f} MB")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al escribir Parquet: {e}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Obtener metadata del archivo Parquet.
        
        Returns:
            Diccionario con metadata
        """
        try:
            import pyarrow.parquet as pq
            
            parquet_file = pq.ParquetFile(self.file_path)
            metadata = parquet_file.metadata
            
            return {
                'num_rows': metadata.num_rows,
                'num_row_groups': metadata.num_row_groups,
                'num_columns': metadata.num_columns,
                'serialized_size': metadata.serialized_size,
                'schema': parquet_file.schema_arrow,
                'created_by': metadata.created_by
            }
            
        except Exception as e:
            logger.error(f"Error al obtener metadata: {e}")
            return {}
    
    def read_partitioned(self, partition_path: str) -> pd.DataFrame:
        """
        Leer dataset Parquet particionado.
        
        Args:
            partition_path: Ruta al directorio con particiones
        
        Returns:
            DataFrame con datos combinados
        """
        try:
            import pyarrow.parquet as pq
            
            dataset = pq.ParquetDataset(partition_path)
            table = dataset.read()
            df = table.to_pandas()
            
            logger.info(f"✓ Dataset particionado leído: {len(df)} filas")
            return df
            
        except Exception as e:
            logger.error(f"Error al leer dataset particionado: {e}")
            raise
    
    def write_partitioned(
        self, 
        df: pd.DataFrame, 
        output_path: str,
        partition_cols: List[str],
        **write_options
    ) -> bool:
        """
        Escribir DataFrame como dataset Parquet particionado.
        
        Args:
            df: DataFrame a escribir
            output_path: Directorio de salida
            partition_cols: Columnas para particionar
            **write_options: Opciones adicionales
        
        Returns:
            True si la escritura fue exitosa
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            Path(output_path).mkdir(parents=True, exist_ok=True)
            
            table = pa.Table.from_pandas(df)
            
            pq.write_to_dataset(
                table,
                root_path=output_path,
                partition_cols=partition_cols,
                compression=write_options.get('compression', 'snappy')
            )
            
            logger.info(f"✓ Dataset particionado escrito: {output_path}")
            logger.info(f"  Particiones: {partition_cols}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error al escribir dataset particionado: {e}")
            raise
    
    def _get_file_size_mb(self, path: Optional[Path] = None) -> float:
        """Obtener tamaño del archivo en MB"""
        file_path = path if path else self.file_path
        if file_path.exists():
            return file_path.stat().st_size / (1024 * 1024)
        return 0.0
    
    def disconnect(self):
        """Cerrar conexión (liberar memoria)"""
        self.df = None
        logger.info("Conexión Parquet cerrada")


def create_parquet_connector(file_path: str, **options) -> ParquetConnector:
    """
    Factory function para crear conector Parquet.
    
    Args:
        file_path: Ruta al archivo Parquet
        **options: Opciones de lectura
    
    Returns:
        ParquetConnector configurado
    """
    return ParquetConnector(file_path, **options)
