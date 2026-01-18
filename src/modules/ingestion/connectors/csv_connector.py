# src/modules/ingestion/connectors/csv_connector.py
"""
Conector para leer datos desde archivos CSV
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
import logging

from ..base import DataSourceConnector

logger = logging.getLogger(__name__)


class CSVConnector(DataSourceConnector):
    """
    Conector para leer datos desde archivos CSV.
    
    Soporta múltiples formatos de CSV, diferentes encodings,
    y opciones avanzadas de pandas.read_csv
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializar conector CSV.
        
        Args:
            config: Debe contener:
                - file_path: Ruta al archivo CSV
                - encoding: Encoding del archivo (default: 'utf-8')
                - delimiter: Delimitador (default: ',')
                - **kwargs: Otros parámetros de pd.read_csv
        """
        super().__init__(config)
        self.file_path = Path(config["file_path"])
        self.encoding = config.get("encoding", "utf-8")
        self.delimiter = config.get("delimiter", ",")
        self.read_options = {
            k: v for k, v in config.items() 
            if k not in ["file_path", "encoding", "delimiter"]
        }
    
    def connect(self) -> bool:
        """Verificar que el archivo existe y es accesible"""
        try:
            if not self.file_path.exists():
                logger.error(f"CSV file not found: {self.file_path}")
                return False
            
            if not self.file_path.is_file():
                logger.error(f"Path is not a file: {self.file_path}")
                return False
            
            # Verificar que es legible
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                f.read(1)
            
            self.connected = True
            logger.info(f"CSV file accessible: {self.file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error accessing CSV file: {e}")
            self.connected = False
            return False
    
    def extract(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Leer datos desde el archivo CSV.
        
        Args:
            query: No usado en CSV (incluido por compatibilidad)
            **kwargs: Parámetros adicionales para pd.read_csv
            
        Returns:
            pd.DataFrame con los datos del CSV
        """
        if not self.connected:
            raise ConnectionError("CSV file not accessible. Call connect() first.")
        
        try:
            logger.info(f"Reading CSV file: {self.file_path}")
            
            # Combinar opciones de configuración con kwargs
            read_params = {**self.read_options, **kwargs}
            
            df = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                **read_params
            )
            
            self.log_extraction(len(df), success=True)
            logger.info(f"Read {len(df)} records, {len(df.columns)} columns from CSV")
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            self.log_extraction(0, success=False)
            raise
    
    def extract_chunked(self, chunksize: int = 10000):
        """
        Leer CSV en chunks (útil para archivos grandes).
        
        Args:
            chunksize: Tamaño de cada chunk
            
        Yields:
            pd.DataFrame: Chunks del archivo
        """
        try:
            logger.info(f"Reading CSV in chunks: {self.file_path}")
            
            for chunk in pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                chunksize=chunksize,
                **self.read_options
            ):
                logger.debug(f"Yielding chunk with {len(chunk)} records")
                yield chunk
                
        except Exception as e:
            logger.error(f"Error reading CSV in chunks: {e}")
            raise
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Obtener información del archivo CSV.
        
        Returns:
            Dict con metadata del archivo
        """
        if not self.file_path.exists():
            return {"error": "File not found"}
        
        import os
        stat = self.file_path.stat()
        
        return {
            "file_path": str(self.file_path),
            "file_name": self.file_path.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "modified_time": stat.st_mtime,
            "encoding": self.encoding,
            "delimiter": self.delimiter
        }
    
    def validate_connection(self) -> bool:
        """Validar que el archivo sigue siendo accesible"""
        return self.file_path.exists() and self.file_path.is_file()
    
    def close(self) -> None:
        """Cerrar conexión (no hace nada en CSV, incluido por compatibilidad)"""
        self.connected = False
        logger.debug("CSV connector closed")


# Factory function
def create_csv_connector(
    file_path: str,
    encoding: str = "utf-8",
    delimiter: str = ",",
    **kwargs
) -> CSVConnector:
    """
    Factory function para crear un conector CSV.
    
    Args:
        file_path: Ruta al archivo CSV
        encoding: Encoding del archivo
        delimiter: Delimitador
        **kwargs: Parámetros adicionales para pd.read_csv
        
    Returns:
        CSVConnector configurado
    """
    config = {
        "file_path": file_path,
        "encoding": encoding,
        "delimiter": delimiter,
        **kwargs
    }
    
    return CSVConnector(config)