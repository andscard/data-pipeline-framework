# tests/unit/test_csv_connector.py
"""
Pruebas unitarias para el conector CSV
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.modules.ingestion.connectors.csv_connector import CSVConnector


@pytest.fixture
def sample_csv_file():
    """Crear archivo CSV temporal para pruebas"""
    # Crear archivo temporal
    fd, path = tempfile.mkstemp(suffix='.csv')
    
    # Escribir datos de prueba
    with os.fdopen(fd, 'w') as f:
        f.write("id,name,value\n")
        f.write("1,Alice,100\n")
        f.write("2,Bob,200\n")
        f.write("3,Charlie,300\n")
    
    yield path
    
    # Limpiar después de las pruebas
    os.unlink(path)


@pytest.fixture
def csv_config(sample_csv_file):
    """Configuración de prueba para CSV"""
    return {
        "file_path": sample_csv_file,
        "encoding": "utf-8",
        "delimiter": ","
    }


@pytest.fixture
def csv_connector(csv_config):
    """Crear instancia del conector CSV"""
    return CSVConnector(csv_config)


def test_connector_initialization(csv_connector, sample_csv_file):
    """Test: Inicialización correcta del conector"""
    assert csv_connector.file_path == Path(sample_csv_file)
    assert csv_connector.encoding == "utf-8"
    assert csv_connector.delimiter == ","
    assert csv_connector.connected == False


def test_connect_success(csv_connector):
    """Test: Conexión exitosa (verificar archivo existe)"""
    result = csv_connector.connect()
    
    assert result == True
    assert csv_connector.connected == True


def test_connect_file_not_found():
    """Test: Fallo cuando el archivo no existe"""
    config = {
        "file_path": "/ruta/inexistente/archivo.csv",
        "encoding": "utf-8",
        "delimiter": ","
    }
    connector = CSVConnector(config)
    
    result = connector.connect()
    
    assert result == False
    assert connector.connected == False


def test_extract_success(csv_connector):
    """Test: Extracción exitosa de datos"""
    csv_connector.connect()
    
    df = csv_connector.extract()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ['id', 'name', 'value']
    assert df['name'].tolist() == ['Alice', 'Bob', 'Charlie']


def test_extract_without_connection_raises_error(csv_connector):
    """Test: Extraer sin conexión debe lanzar error"""
    csv_connector.connected = False
    
    with pytest.raises(ConnectionError, match="CSV file not accessible"):
        csv_connector.extract()


def test_extract_with_custom_params(sample_csv_file):
    """Test: Extracción con parámetros personalizados"""
    config = {
        "file_path": sample_csv_file,
        "encoding": "utf-8",
        "delimiter": ",",
        "usecols": ["id", "name"]  # Solo leer algunas columnas
    }
    connector = CSVConnector(config)
    connector.connect()
    
    df = connector.extract()
    
    assert len(df.columns) == 2
    assert 'value' not in df.columns


def test_get_file_info(csv_connector):
    """Test: Obtener información del archivo"""
    csv_connector.connect()
    
    info = csv_connector.get_file_info()
    
    assert 'file_path' in info
    assert 'file_name' in info
    assert 'size_bytes' in info
    assert info['encoding'] == 'utf-8'
    assert info['delimiter'] == ','


def test_validate_connection(csv_connector):
    """Test: Validar que el archivo sigue accesible"""
    csv_connector.connect()
    
    result = csv_connector.validate_connection()
    
    assert result == True


def test_context_manager(csv_config):
    """Test: Uso como context manager"""
    with CSVConnector(csv_config) as connector:
        assert connector.connected == True
        df = connector.extract()
        assert len(df) == 3
    
    assert connector.connected == False


def test_extract_chunked(csv_connector):
    """Test: Lectura en chunks"""
    csv_connector.connect()
    
    chunks = list(csv_connector.extract_chunked(chunksize=2))
    
    assert len(chunks) == 2  # 3 registros / 2 por chunk = 2 chunks
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1