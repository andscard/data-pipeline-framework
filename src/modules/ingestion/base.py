# src/modules/ingestion/base.py
"""
Clase base abstracta para conectores de datos
Implementa el patrón Strategy para diferentes fuentes de datos
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataSourceConnector(ABC):
    """
    Clase base abstracta para todos los conectores de datos.
    
    Cada conector debe implementar los métodos abstractos:
    - connect(): Establecer conexión con la fuente
    - extract(): Extraer datos
    - validate_connection(): Verificar que la conexión funciona
    - close(): Cerrar conexión
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializar conector con configuración.
        
        Args:
            config: Diccionario con parámetros de conexión
        """
        self.config = config
        self.connection = None
        self.connected = False
        self.last_extraction_time = None
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establecer conexión con la fuente de datos.
        
        Returns:
            bool: True si la conexión fue exitosa
        """
        pass
    
    @abstractmethod
    def extract(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Extraer datos de la fuente.
        
        Args:
            query: Query o instrucción para extraer datos
            **kwargs: Parámetros adicionales específicos del conector
            
        Returns:
            pd.DataFrame: Datos extraídos
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validar que la conexión está activa y funcional.
        
        Returns:
            bool: True si la conexión es válida
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Cerrar conexión con la fuente de datos"""
        pass
    
    def __enter__(self):
        """Context manager: entrada"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: salida"""
        self.close()
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Obtener metadata del conector.
        
        Returns:
            Dict con información del conector
        """
        return {
            "connector_type": self.__class__.__name__,
            "connected": self.connected,
            "last_extraction": self.last_extraction_time,
            "config": {k: v for k, v in self.config.items() if "password" not in k.lower()}
        }
    
    def log_extraction(self, records_count: int, success: bool = True) -> None:
        """
        Registrar información de extracción.
        
        Args:
            records_count: Número de registros extraídos
            success: Si la extracción fue exitosa
        """
        self.last_extraction_time = datetime.now()
        
        if success:
            logger.info(
                f"Extraction successful: {records_count} records from {self.__class__.__name__}",
                extra={
                    "connector": self.__class__.__name__,
                    "records": records_count,
                    "timestamp": self.last_extraction_time.isoformat()
                }
            )
        else:
            logger.error(
                f"Extraction failed from {self.__class__.__name__}",
                extra={"connector": self.__class__.__name__}
            )