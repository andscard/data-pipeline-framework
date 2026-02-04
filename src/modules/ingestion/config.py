"""
Configuración y constantes para el módulo de ingesta.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5433)),
    'database': os.getenv('POSTGRES_DB', 'pipeline_db'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'secret_password')
}

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', 5540)),
    'password': os.getenv('REDIS_PASSWORD', 'redis_secret'),
    'db': int(os.getenv('REDIS_DB', 0))
}

# File Configuration
DATA_DIR = os.path.join(os.getcwd(), 'data')
SAMPLES_DIR = os.path.join(DATA_DIR, 'samples')
OUTPUT_DIR = os.path.join(DATA_DIR, 'output')

for directory in [DATA_DIR, SAMPLES_DIR, OUTPUT_DIR]:
    os.makedirs(directory, exist_ok=True)

# Connector Settings
DEFAULT_CHUNK_SIZE = 10000
DEFAULT_TIMEOUT = 30
DEFAULT_ENCODING = 'utf-8'

# Synthetic Data Generator
FAKER_LOCALE = 'es_ES'
DEFAULT_SYNTHETIC_RECORDS = 1000
RANDOM_SEED = 42

# Predefined Schemas
PREDEFINED_SCHEMAS = {
    'customers': {
        'customer_id': 'uuid',
        'first_name': 'first_name',
        'last_name': 'last_name',
        'email': 'email',
        'phone': 'phone_number',
        'address': 'address',
        'city': 'city',
        'country': 'country',
        'registration_date': 'date_time',
        'is_active': 'boolean'
    },
    'transactions': {
        'transaction_id': 'uuid',
        'customer_id': 'uuid',
        'amount': 'decimal',
        'currency': 'currency_code',
        'transaction_date': 'date_time',
        'status': 'random_element',
        'description': 'sentence'
    },
    'products': {
        'product_id': 'uuid',
        'name': 'word',
        'category': 'word',
        'price': 'decimal',
        'stock': 'int',
        'description': 'text',
        'created_at': 'date_time'
    }
}

# Limits and Validations
MAX_RECORDS_PER_EXTRACTION = 1000000
MAX_FILE_SIZE_MB = 500