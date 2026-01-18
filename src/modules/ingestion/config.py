"""
Configuración y constantes para el módulo de ingesta.
"""

import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ============================================
# CONFIGURACIÓN DE BASE DE DATOS
# ============================================

# PostgreSQL - USANDO TUS PUERTOS PERSONALIZADOS
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5440)),  # TU PUERTO PERSONALIZADO
    'database': os.getenv('POSTGRES_DB', 'pipeline_db'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'secret_password')
}

# Redis - USANDO TU PUERTO PERSONALIZADO
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', 5540)),  # TU PUERTO PERSONALIZADO
    'password': os.getenv('REDIS_PASSWORD', 'redis_secret'),
    'db': int(os.getenv('REDIS_DB', 0))
}

# ============================================
# CONFIGURACIÓN DE ARCHIVOS
# ============================================

DATA_DIR = os.path.join(os.getcwd(), 'data')
SAMPLES_DIR = os.path.join(DATA_DIR, 'samples')
OUTPUT_DIR = os.path.join(DATA_DIR, 'output')

# Crear directorios si no existen
for directory in [DATA_DIR, SAMPLES_DIR, OUTPUT_DIR]:
    os.makedirs(directory, exist_ok=True)

# ============================================
# CONFIGURACIÓN DE CONECTORES
# ============================================

# Tamaño de chunk para lecturas grandes (registros)
DEFAULT_CHUNK_SIZE = 10000

# Timeout para conexiones (segundos)
DEFAULT_TIMEOUT = 30

# Encoding por defecto para archivos
DEFAULT_ENCODING = 'utf-8'

# ============================================
# CONFIGURACIÓN DE GENERADOR SINTÉTICO
# ============================================

# Locale para Faker (datos en español)
FAKER_LOCALE = 'es_ES'

# Número de registros por defecto
DEFAULT_SYNTHETIC_RECORDS = 1000

# Semilla para reproducibilidad
RANDOM_SEED = 42

# ============================================
# ESQUEMAS PREDEFINIDOS
# ============================================

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

# ============================================
# LÍMITES Y VALIDACIONES
# ============================================

# Máximo de registros por extracción
MAX_RECORDS_PER_EXTRACTION = 1000000

# Tamaño máximo de archivo CSV/JSON (MB)
MAX_FILE_SIZE_MB = 500