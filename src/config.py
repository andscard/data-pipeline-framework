# src/config.py
"""
Configuración centralizada del framework
Lee variables de entorno y proporciona valores por defecto
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)

class Config:
    """Configuración centralizada de la aplicación"""
    
    # Paths del proyecto
    PROJECT_ROOT = project_root
    DATA_DIR = PROJECT_ROOT / "data"
    ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
    
    # PostgreSQL - Base de datos de auditoría
    # Usada por: audit_manager.py, postgres_connector.py
    # Estas variables DEBEN estar definidas en .env
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT")) if os.getenv("POSTGRES_PORT") else None
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    
    # ------------------------------------------------------------------------
    # QUALITY & HEALTH THRESHOLDS (DEFAULTS)
    # ------------------------------------------------------------------------
    # Estos valores se usan como fallback si no se definen en el YAML del pipeline.
    
    # Umbrales de Salud Operativa (% de registros procesados exitosamente)
    # Puede ser sobrescrito en pipeline.yml bajo la sección 'thresholds'
    DEFAULT_HEALTH_OPERATIONAL_HEALTHY = 95.0
    DEFAULT_HEALTH_OPERATIONAL_WARNING = 80.0
    
    # Umbrales de Calidad de Datos (% de reglas de validación pasadas)
    # Puede ser sobrescrito en pipeline.yml bajo la sección 'thresholds'
    DEFAULT_QUALITY_EXCELLENT = 90.0
    DEFAULT_QUALITY_GOOD = 70.0  
    DEFAULT_QUALITY_WARNING = 50.0  # Debajo de esto es POOR/CRITICAL

    # Validar que todas las variables requeridas estén presentes
    _missing_vars = []
    if not POSTGRES_HOST:
        _missing_vars.append("POSTGRES_HOST")
    if not POSTGRES_PORT:
        _missing_vars.append("POSTGRES_PORT")
    if not POSTGRES_DB:
        _missing_vars.append("POSTGRES_DB")
    if not POSTGRES_USER:
        _missing_vars.append("POSTGRES_USER")
    if not POSTGRES_PASSWORD:
        _missing_vars.append("POSTGRES_PASSWORD")
    
    if _missing_vars:
        raise EnvironmentError(
            f"Variables de entorno requeridas NO encontradas en .env: {', '.join(_missing_vars)}\n"
            f"Por favor, asegúrate de que el archivo .env existe y contiene todas las variables necesarias."
        )

# Instancia global de configuración
config = Config()

# Verificar configuración al importar
if __name__ == "__main__":
    print("\n=== CONFIGURACIÓN DEL FRAMEWORK ===\n")
    print(f"PostgreSQL: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
    print(f"Project Root: {config.PROJECT_ROOT}")
