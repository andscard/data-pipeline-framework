# tests/unit/test_postgres_connector.py
"""
Pruebas unitarias para el conector PostgreSQL
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.modules.ingestion.connectors.postgres_connector import PostgreSQLConnector


@pytest.fixture
def postgres_config():
    """Configuración de prueba para PostgreSQL"""
    return {
        "host": "localhost",
        "port": 5440,
        "database": "pipeline_db",
        "user": "admin",
        "password": "secret_password",
        "mode": "sqlalchemy"
    }


@pytest.fixture
def postgres_connector(postgres_config):
    """Crear instancia del conector"""
    return PostgreSQLConnector(postgres_config)


def test_connector_initialization(postgres_connector, postgres_config):
    """Test: Inicialización correcta del conector"""
    assert postgres_connector.config == postgres_config
    assert postgres_connector.mode == "sqlalchemy"
    assert postgres_connector.connected == False
    assert postgres_connector.engine is None


def test_connect_success(postgres_connector):
    """Test: Conexión exitosa a PostgreSQL"""
    with patch('sqlalchemy.create_engine') as mock_engine:
        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        result = postgres_connector.connect()
        
        assert result == True
        assert postgres_connector.connected == True
        mock_engine.assert_called_once()


def test_connect_failure(postgres_connector):
    """Test: Fallo en la conexión"""
    with patch('sqlalchemy.create_engine', side_effect=Exception("Connection failed")):
        result = postgres_connector.connect()
        
        assert result == False
        assert postgres_connector.connected == False


def test_extract_with_query(postgres_connector):
    """Test: Extracción con query personalizada"""
    postgres_connector.connected = True
    
    mock_df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
    
    with patch('pandas.read_sql_query', return_value=mock_df):
        result = postgres_connector.extract(query="SELECT * FROM test_table")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ['id', 'name']


def test_extract_with_table(postgres_connector):
    """Test: Extracción especificando tabla"""
    postgres_connector.connected = True
    
    mock_df = pd.DataFrame({'id': [1, 2], 'value': [100, 200]})
    
    with patch('pandas.read_sql_query', return_value=mock_df) as mock_read:
        result = postgres_connector.extract(table="test_table")
        
        # Verificar que se generó la query correcta
        call_args = mock_read.call_args
        assert "SELECT * FROM test_table" in call_args[0][0]
        assert len(result) == 2


def test_extract_without_connection_raises_error(postgres_connector):
    """Test: Extraer sin conexión debe lanzar error"""
    postgres_connector.connected = False
    
    with pytest.raises(ConnectionError, match="Not connected"):
        postgres_connector.extract(query="SELECT 1")


def test_extract_without_query_or_table_raises_error(postgres_connector):
    """Test: Extraer sin query ni tabla debe lanzar error"""
    postgres_connector.connected = True
    
    with pytest.raises(ValueError, match="Must provide either"):
        postgres_connector.extract()


def test_validate_connection_success(postgres_connector):
    """Test: Validación de conexión exitosa"""
    postgres_connector.mode = "sqlalchemy"
    postgres_connector.engine = MagicMock()
    mock_conn = MagicMock()
    postgres_connector.engine.connect.return_value.__enter__.return_value = mock_conn
    
    result = postgres_connector.validate_connection()
    
    assert result == True


def test_validate_connection_failure(postgres_connector):
    """Test: Validación de conexión fallida"""
    postgres_connector.mode = "sqlalchemy"
    postgres_connector.engine = MagicMock()
    postgres_connector.engine.connect.side_effect = Exception("Connection lost")
    
    result = postgres_connector.validate_connection()
    
    assert result == False


def test_close_connection(postgres_connector):
    """Test: Cerrar conexión correctamente"""
    postgres_connector.mode = "sqlalchemy"
    postgres_connector.engine = MagicMock()
    postgres_connector.connected = True
    
    postgres_connector.close()
    
    postgres_connector.engine.dispose.assert_called_once()
    assert postgres_connector.connected == False


def test_context_manager(postgres_config):
    """Test: Uso como context manager"""
    with patch('sqlalchemy.create_engine'):
        with PostgreSQLConnector(postgres_config) as connector:
            assert connector.connected == True
        
        assert connector.connected == False


def test_get_metadata(postgres_connector):
    """Test: Obtener metadata del conector"""
    postgres_connector.connected = True
    
    metadata = postgres_connector.get_metadata()
    
    assert metadata['connector_type'] == 'PostgreSQLConnector'
    assert metadata['connected'] == True
    assert 'password' not in str(metadata['config'])  # No debe exponer password