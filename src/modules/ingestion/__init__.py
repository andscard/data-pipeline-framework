# src/modules/ingestion/__init__.py
"""
Módulo de Ingesta de Datos

Proporciona conectores para diferentes fuentes de datos y
generación de datos sintéticos para pruebas.
"""

from .base import DataSourceConnector
from .synthetic_generator import SyntheticDataGenerator, create_synthetic_generator

# Importar conectores
from .connectors.postgres_connector import PostgreSQLConnector, create_postgres_connector
from .connectors.csv_connector import CSVConnector, create_csv_connector

__all__ = [
    # Clase base
    'DataSourceConnector',
    
    # Conectores
    'PostgreSQLConnector',
    'CSVConnector',
    
    # Factory functions
    'create_postgres_connector',
    'create_csv_connector',
    
    # Generador sintético
    'SyntheticDataGenerator',
    'create_synthetic_generator'
]

__version__ = '0.1.0'