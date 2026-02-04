"""
Conector para archivos Excel (.xlsx, .xls)

Proporciona funcionalidades para leer y escribir archivos Excel
utilizando pandas y openpyxl.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class ExcelConnector:
    """Conector para archivos Excel"""
    
    def __init__(self, file_path: str, **options):
        """
        Args:
            file_path: Ruta al archivo Excel
            **options: Opciones adicionales para pandas.read_excel
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
        
        logger.info(f"✓ Archivo Excel encontrado: {self.file_path}")
        return True
    
    def read(self) -> pd.DataFrame:
        """
        Leer datos del archivo Excel.
        
        Returns:
            DataFrame con los datos leídos
        """
        try:
            # Opciones comunes para read_excel
            read_options = {
                'sheet_name': self.options.get('sheet_name', 0),
                'header': self.options.get('header', 0),
                'skiprows': self.options.get('skiprows', None),
                'usecols': self.options.get('usecols', None),
                'dtype': self.options.get('dtype', None),
                'na_values': self.options.get('na_values', None),
            }
            
            # Leer Excel
            self.df = pd.read_excel(self.file_path, **read_options)
            
            logger.info(f"✓ Excel leído: {len(self.df)} filas, {len(self.df.columns)} columnas")
            return self.df
            
        except Exception as e:
            logger.error(f"Error al leer Excel: {e}")
            raise
    
    def write(self, df: pd.DataFrame, output_path: Optional[str] = None, **write_options) -> bool:
        """
        Escribir DataFrame a archivo Excel.
        
        Args:
            df: DataFrame a escribir
            output_path: Ruta de salida (usa self.file_path si es None)
            **write_options: Opciones adicionales para to_excel
        
        Returns:
            True si la escritura fue exitosa
        """
        try:
            path = Path(output_path) if output_path else self.file_path
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Opciones comunes para to_excel
            options = {
                'sheet_name': write_options.get('sheet_name', 'Sheet1'),
                'index': write_options.get('index', False),
                'engine': write_options.get('engine', 'openpyxl'),
            }
            
            df.to_excel(path, **options)
            
            logger.info(f"✓ Excel escrito: {path} ({len(df)} filas)")
            return True
            
        except Exception as e:
            logger.error(f"Error al escribir Excel: {e}")
            raise
    
    def get_sheet_names(self) -> List[str]:
        """
        Obtener nombres de todas las hojas del archivo Excel.
        
        Returns:
            Lista de nombres de hojas
        """
        try:
            excel_file = pd.ExcelFile(self.file_path)
            return excel_file.sheet_names
        except Exception as e:
            logger.error(f"Error al obtener nombres de hojas: {e}")
            raise
    
    def disconnect(self):
        """Cerrar conexión (liberar memoria)"""
        self.df = None
        logger.info("Conexión Excel cerrada")


def create_excel_connector(file_path: str, **options) -> ExcelConnector:
    """
    Factory function para crear conector Excel.
    
    Args:
        file_path: Ruta al archivo Excel
        **options: Opciones de lectura
    
    Returns:
        ExcelConnector configurado
    """
    return ExcelConnector(file_path, **options)
