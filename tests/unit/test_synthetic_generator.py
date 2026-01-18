# tests/unit/test_synthetic_generator.py
"""
Pruebas unitarias para el generador de datos sintéticos
"""

import pytest
import pandas as pd
import numpy as np

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.modules.ingestion.synthetic_generator import SyntheticDataGenerator


@pytest.fixture
def generator():
    """Crear instancia del generador con semilla fija"""
    return SyntheticDataGenerator(locale='es_ES', seed=42)


def test_generator_initialization(generator):
    """Test: Inicialización correcta del generador"""
    assert generator.locale == 'es_ES'
    assert generator.fake is not None


def test_generate_basic_schema(generator):
    """Test: Generar datos con esquema básico"""
    schema = {
        'id': 'int',
        'name': 'string',
        'active': 'bool'
    }
    
    df = generator.generate(schema, num_records=100)
    
    assert len(df) == 100
    assert len(df.columns) == 3
    assert 'id' in df.columns
    assert 'name' in df.columns
    assert 'active' in df.columns


def test_generate_numeric_types(generator):
    """Test: Generar tipos numéricos"""
    schema = {
        'int_col': 'int',
        'float_col': 'float'
    }
    
    df = generator.generate(schema, num_records=50)
    
    assert df['int_col'].dtype == np.int64
    assert df['float_col'].dtype == np.float64


def test_generate_text_types(generator):
    """Test: Generar tipos de texto"""
    schema = {
        'name': 'name',
        'email': 'email',
        'phone': 'phone'
    }
    
    df = generator.generate(schema, num_records=20)
    
    # Verificar que todos tienen valores
    assert df['name'].notna().all()
    assert df['email'].notna().all()
    
    # Verificar formato de email
    assert all('@' in email for email in df['email'])


def test_generate_temporal_types(generator):
    """Test: Generar tipos temporales"""
    schema = {
        'date': 'date',
        'datetime': 'datetime',
        'timestamp': 'timestamp'
    }
    
    df = generator.generate(schema, num_records=30)
    
    assert df['date'].notna().all()
    assert df['datetime'].notna().all()
    assert df['timestamp'].notna().all()


def test_generate_with_seed_reproducibility():
    """Test: Reproducibilidad con semilla fija"""
    gen1 = SyntheticDataGenerator(seed=123)
    gen2 = SyntheticDataGenerator(seed=123)
    
    schema = {'value': 'int'}
    
    df1 = gen1.generate(schema, num_records=10)
    df2 = gen2.generate(schema, num_records=10)
    
    # Deben ser idénticos con la misma semilla
    assert df1['value'].tolist() == df2['value'].tolist()


def test_inject_nulls(generator):
    """Test: Inyección de valores nulos"""
    schema = {'col1': 'int', 'col2': 'int', 'col3': 'int'}
    df = generator.generate(schema, num_records=100)
    
    # Sin anomalías
    assert df.isnull().sum().sum() == 0
    
    # Con anomalías
    df_with_nulls = generator.inject_anomalies(
        df,
        anomaly_rate=0.1,
        anomaly_types=['nulls']
    )
    
    # Debe haber algunos nulos
    assert df_with_nulls.isnull().sum().sum() > 0


def test_inject_outliers(generator):
    """Test: Inyección de outliers"""
    schema = {'value': 'float'}
    df = generator.generate(schema, num_records=100)
    
    original_max = df['value'].max()
    
    df_with_outliers = generator.inject_anomalies(
        df,
        anomaly_rate=0.05,
        anomaly_types=['outliers']
    )
    
    # Debe haber valores mucho más grandes
    assert df_with_outliers['value'].max() > original_max * 5


def test_inject_duplicates(generator):
    """Test: Inyección de duplicados"""
    schema = {'id': 'int'}
    df = generator.generate(schema, num_records=100)
    
    original_length = len(df)
    
    df_with_duplicates = generator.inject_anomalies(
        df,
        anomaly_rate=0.1,
        anomaly_types=['duplicates']
    )
    
    # Debe haber más registros
    assert len(df_with_duplicates) > original_length


def test_generate_customer_data(generator):
    """Test: Generar datos de clientes predefinidos"""
    df = generator.generate_customer_data(num_customers=50)
    
    assert len(df) == 50
    assert 'customer_id' in df.columns
    assert 'name' in df.columns
    assert 'email' in df.columns
    assert 'lifetime_value' in df.columns


def test_generate_transaction_data(generator):
    """Test: Generar datos de transacciones predefinidos"""
    df = generator.generate_transaction_data(num_transactions=100)
    
    assert len(df) == 100
    assert 'transaction_id' in df.columns
    assert 'amount' in df.columns
    assert 'category' in df.columns
    
    # Verificar categorías válidas
    valid_categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Other']
    assert all(cat in valid_categories for cat in df['category'])