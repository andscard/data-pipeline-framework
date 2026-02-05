"""
Multi-source Loader

Cargador unificado que soporta múltiples formatos de datos:
- CSV
- Excel (.xlsx, .xls)
- Parquet
- TXT (texto delimitado)
- PostgreSQL

Utiliza los conectores específicos de cada formato.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import os

from .connectors.csv_connector import create_csv_connector
from .connectors.excel_connector import create_excel_connector
from .connectors.parquet_connector import create_parquet_connector
from .connectors.postgres_connector import create_postgres_connector

logger = logging.getLogger(__name__)


class MultiSourceLoader:
    """Cargador multi-formato de datos"""
    
    SUPPORTED_TYPES = ['csv', 'excel', 'parquet', 'txt', 'postgres']
    
    def __init__(self):
        """Inicializar cargador"""
        self.sources = {}
        self.datasets = {}
    
    def load_from_config(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """
        Cargar datos desde configuración YAML.
        
        Args:
            source_config: Configuración de la fuente desde YAML
        
        Returns:
            DataFrame con los datos cargados
        """
        source_name = source_config.get('name')
        source_type = source_config.get('type')
        output_dataset = source_config.get('output_dataset')
        
        if not source_name:
            raise ValueError("La fuente debe tener un 'name'")
        
        if not source_type:
            raise ValueError(f"La fuente '{source_name}' debe tener un 'type'")
        
        if source_type not in self.SUPPORTED_TYPES:
            raise ValueError(
                f"Tipo '{source_type}' no soportado. "
                f"Tipos disponibles: {self.SUPPORTED_TYPES}"
            )
        
        logger.info(f"Loading source: {source_name} (type: {source_type})")
        
        # Expandir variables de entorno
        source_config = self._expand_env_vars(source_config)
        
        # Cargar según tipo
        if source_type == 'csv':
            df = self._load_csv(source_config)
        elif source_type == 'excel':
            df = self._load_excel(source_config)
        elif source_type == 'parquet':
            df = self._load_parquet(source_config)
        elif source_type == 'txt':
            df = self._load_txt(source_config)
        elif source_type == 'postgres':
            df = self._load_postgres(source_config)
        else:
            raise ValueError(f"Tipo no implementado: {source_type}")
        
        # Guardar en registro de datasets
        if output_dataset:
            self.datasets[output_dataset] = df
            logger.info(f"✓ Dataset '{output_dataset}' guardado en memoria")
        
        logger.info(f"✓ Fuente '{source_name}' cargada: {len(df)} filas, {len(df.columns)} columnas")
        
        return df
    
    def _load_csv(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Cargar desde CSV"""
        path = config.get('path')
        options = config.get('options', {})
        
        connector = create_csv_connector(path, **options)
        
        if not connector.connect():
            raise FileNotFoundError(f"Archivo CSV no encontrado: {path}")
        
        return connector.extract()
    
    def _load_excel(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Cargar desde Excel"""
        path = config.get('path')
        options = config.get('options', {})
        
        connector = create_excel_connector(path, **options)
        
        if not connector.connect():
            raise FileNotFoundError(f"Archivo Excel no encontrado: {path}")
        
        return connector.extract()
    
    def _load_parquet(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Cargar desde Parquet"""
        path = config.get('path')
        options = config.get('options', {})
        
        connector = create_parquet_connector(path, **options)
        
        if not connector.connect():
            raise FileNotFoundError(f"Archivo Parquet no encontrado: {path}")
        
        return connector.extract()
    
    def _load_txt(self, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Cargar desde archivo TXT delimitado.
        
        Usa pd.read_csv internamente con delimitador personalizado.
        """
        path = config.get('path')
        options = config.get('options', {})
        
        # Validar que existe el archivo
        if not Path(path).exists():
            raise FileNotFoundError(f"Archivo TXT no encontrado: {path}")
        
        # Parámetros por defecto para TXT
        default_params = {
            'delimiter': '|',  # Pipe por defecto
            'encoding': 'utf-8',
            'header': 0
        }
        
        # Combinar con opciones del usuario
        read_params = {**default_params, **options}
        
        logger.info(f"  Leyendo TXT con delimitador: '{read_params['delimiter']}'")
        
        # Usar pandas read_csv con delimitador personalizado
        df = pd.read_csv(path, **read_params)
        
        return df
    
    def _load_postgres(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Cargar desde PostgreSQL"""
        connection = config.get('connection', {})
        query = config.get('query')
        table = config.get('table')
        
        if not query and not table:
            raise ValueError("Se requiere 'query' o 'table' para PostgreSQL")
        
        # Construir query si solo se especificó tabla
        if table and not query:
            query = f"SELECT * FROM {table}"
        
        connector = create_postgres_connector(
            host=connection.get('host'),
            port=connection.get('port', 5432),
            database=connection.get('database'),
            user=connection.get('user'),
            password=connection.get('password'),
            mode='sqlalchemy'
        )
        
        if not connector.connect():
            raise ConnectionError("No se pudo conectar a PostgreSQL")
        
        return connector.extract(query=query)
    
    def write_to_destination(
        self, 
        df: pd.DataFrame, 
        output_config: Dict[str, Any]
    ) -> bool:
        """
        Escribir DataFrame a destino especificado.
        
        Args:
            df: DataFrame a escribir
            output_config: Configuración del destino
        
        Returns:
            True si la escritura fue exitosa
        """
        output_name = output_config.get('name')
        output_type = output_config.get('type')
        
        logger.info(f"Writing to: {output_name} (type: {output_type})")
        
        # Expandir variables de entorno
        output_config = self._expand_env_vars(output_config)
        
        if output_type == 'csv':
            return self._write_csv(df, output_config)
        elif output_type == 'excel':
            return self._write_excel(df, output_config)
        elif output_type == 'parquet':
            return self._write_parquet(df, output_config)
        elif output_type == 'txt':
            return self._write_txt(df, output_config)
        elif output_type == 'postgres':
            return self._write_postgres(df, output_config)
        else:
            raise ValueError(f"Tipo de output no soportado: {output_type}")
    
    def _write_csv(self, df: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """Escribir a CSV"""
        path = config.get('path')
        options = config.get('options', {})
        
        connector = create_csv_connector(path)
        return connector.write(df, **options)
    
    def _write_excel(self, df: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """Escribir a Excel"""
        path = config.get('path')
        options = config.get('options', {})
        
        connector = create_excel_connector(path)
        return connector.write(df, **options)
    
    def _write_parquet(self, df: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """Escribir a Parquet"""
        path = config.get('path')
        options = config.get('options', {})
        
        connector = create_parquet_connector(path)
        return connector.write(df, **options)
    
    def _write_txt(self, df: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """Escribir a archivo TXT delimitado"""
        path = config.get('path')
        options = config.get('options', {})
        
        # Parámetros por defecto para TXT
        default_params = {
            'sep': '|',
            'encoding': 'utf-8',
            'index': False
        }
        
        # Combinar con opciones del usuario
        write_params = {**default_params, **options}
        
        logger.info(f"  Escribiendo TXT con delimitador: '{write_params['sep']}'")
        
        # Crear directorio si no existe
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        
        # Escribir usando to_csv de pandas
        df.to_csv(path, **write_params)
        
        return True
    
    def _write_postgres(self, df: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """Escribir a PostgreSQL"""
        connection = config.get('connection', {})
        table = config.get('table')
        mode = config.get('mode', 'append')  # append, overwrite, upsert
        
        if not table:
            raise ValueError("Se requiere 'table' para escribir a PostgreSQL")
        
        connector = create_postgres_connector(
            host=connection.get('host'),
            port=connection.get('port', 5432),
            database=connection.get('database'),
            user=connection.get('user'),
            password=connection.get('password'),
            mode='sqlalchemy'
        )
        
        if not connector.connect():
            raise ConnectionError("No se pudo conectar a PostgreSQL")
        
        # Mapear modo a if_exists de pandas
        if_exists_map = {
            'append': 'append',
            'overwrite': 'replace',
            'replace': 'replace'
        }
        
        if_exists = if_exists_map.get(mode, 'append')
        
        return connector.write_dataframe(df, table, if_exists=if_exists)
    
    def _expand_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Expandir variables de entorno en la configuración.
        
        Reemplaza ${VAR_NAME} con el valor de la variable de entorno.
        
        Args:
            config: Configuración con posibles variables
        
        Returns:
            Configuración con variables expandidas
        """
        import re
        import json
        
        # Convertir a JSON string para procesamiento
        config_str = json.dumps(config)
        
        # Encontrar todas las variables ${VAR_NAME}
        pattern = r'\$\{([^}]+)\}'
        
        def replace_var(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
        
        # Reemplazar variables
        config_str = re.sub(pattern, replace_var, config_str)
        
        # Convertir de vuelta a dict
        return json.loads(config_str)
    
    def get_dataset(self, dataset_id: str) -> Optional[pd.DataFrame]:
        """
        Obtener dataset por ID.
        
        Args:
            dataset_id: ID del dataset
        
        Returns:
            DataFrame o None si no existe
        """
        return self.datasets.get(dataset_id)
    
    def list_datasets(self) -> Dict[str, Dict[str, Any]]:
        """
        Listar todos los datasets cargados.
        
        Returns:
            Diccionario con info de cada dataset
        """
        info = {}
        for dataset_id, df in self.datasets.items():
            info[dataset_id] = {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
            }
        return info
    
    def clear_datasets(self):
        """Limpiar todos los datasets de memoria"""
        self.datasets.clear()
        logger.info("✓ Datasets limpiados de memoria")
