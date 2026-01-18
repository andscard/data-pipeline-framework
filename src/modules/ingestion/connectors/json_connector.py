"""
Conector para archivos JSON.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union
import logging

from ..base import DataSourceConnector, DataSourceException

logger = logging.getLogger(__name__)


class JSONConnector(DataSourceConnector):
    """
    Conector para extraer datos desde archivos JSON.
    
    Soporta:
    - JSON Lines (JSONL): cada línea es un objeto JSON
    - JSON Array: archivo con array de objetos
    - JSON Object: archivo con objeto único
    
    Ejemplo de uso:
        config = {
            'name': 'users_json',
            'filepath': 'data/samples/users.json',
            'json_format': 'lines'  # 'lines', 'array', 'object'
        }
        
        with JSONConnector(config) as connector:
            df = connector.extract()
            print(df.head())
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializar conector JSON.
        
        Args:
            config: Debe contener 'filepath'. Opcional: json_format, encoding
        """
        super().__init__(config)
        
        if 'filepath' not in config:
            raise DataSourceException("Configuration must contain 'filepath'")
        
        self.filepath = Path(config['filepath'])
        self.encoding = config.get('encoding', 'utf-8')
        self.json_format = config.get('json_format', 'lines')  # lines, array, object
        self.orient = config.get('orient', 'records')  # Para pd.read_json
    
    def connect(self) -> bool:
        """
        Validar que el archivo existe y es accesible.
        
        Returns:
            bool: True si el archivo existe
            
        Raises:
            DataSourceException: Si el archivo no existe
        """
        if not self.filepath.exists():
            raise DataSourceException(f"File not found: {self.filepath}")
        
        if not self.filepath.is_file():
            raise DataSourceException(f"Path is not a file: {self.filepath}")
        
        # Verificar tamaño del archivo
        file_size_mb = self.filepath.stat().st_size / (1024 * 1024)
        logger.info(
            f"[{self.name}] File found: {self.filepath} ({file_size_mb:.2f} MB)"
        )
        
        self._connection = True
        return True
    
    def extract(
        self,
        columns: Optional[list] = None,
        nrows: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extraer datos desde archivo JSON.
        
        Args:
            columns: Lista de columnas a extraer (solo si aplica)
            nrows: Número máximo de registros a leer
            
        Returns:
            pd.DataFrame: Datos extraídos
            
        Raises:
            DataSourceException: Si la extracción falla
        """
        if not self._connection:
            self.connect()
        
        try:
            logger.info(
                f"[{self.name}] Reading JSON ({self.json_format}): {self.filepath}"
            )
            
            if self.json_format == 'lines':
                # JSON Lines: cada línea es un objeto JSON
                df = pd.read_json(
                    self.filepath,
                    lines=True,
                    encoding=self.encoding,
                    nrows=nrows
                )
            
            elif self.json_format == 'array':
                # JSON Array: [{"col1": val1}, {"col2": val2}]
                df = pd.read_json(
                    self.filepath,
                    orient=self.orient,
                    encoding=self.encoding
                )
                
                if nrows:
                    df = df.head(nrows)
            
            elif self.json_format == 'object':
                # JSON Object: {"key1": {}, "key2": {}}
                with open(self.filepath, 'r', encoding=self.encoding) as f:
                    data = json.load(f)
                
                df = pd.DataFrame.from_dict(data, orient='index')
                
                if nrows:
                    df = df.head(nrows)
            
            else:
                raise DataSourceException(
                    f"Unsupported json_format: {self.json_format}. "
                    "Use 'lines', 'array', or 'object'"
                )
            
            # Filtrar columnas si se especificaron
            if columns:
                available_columns = [col for col in columns if col in df.columns]
                df = df[available_columns]
            
            logger.info(
                f"[{self.name}] Extracted {len(df)} records, "
                f"{len(df.columns)} columns"
            )
            
            # Actualizar metadata
            self._update_metadata(len(df))
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to read JSON file: {str(e)}"
            logger.error(f"[{self.name}] {error_msg}")
            raise DataSourceException(error_msg)
    
    def validate_connection(self) -> bool:
        """
        Validar que el archivo sigue existiendo.
        
        Returns:
            bool: True si el archivo existe
        """
        return self.filepath.exists()
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Obtener información del archivo JSON.
        
        Returns:
            Dict: Información del archivo
        """
        if not self.filepath.exists():
            return {}
        
        stat = self.filepath.stat()
        
        return {
            'filepath': str(self.filepath),
            'size_bytes': stat.st_size,
            'size_mb': stat.st_size / (1024 * 1024),
            'modified_time': stat.st_mtime,
            'encoding': self.encoding,
            'json_format': self.json_format
        }