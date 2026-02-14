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
    
    # ==========================================
    # CORE: Configuración del Pipeline de Datos
    # ==========================================
    
    # Paths del proyecto
    PROJECT_ROOT = project_root
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"
    REPORTS_DIR = PROJECT_ROOT / "reports"
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # PostgreSQL - Base de datos de auditoría
    # Usada por: audit_manager.py, postgres_connector.py
    # IMPORTANTE: Estas variables DEBEN estar definidas en .env
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT")) if os.getenv("POSTGRES_PORT") else None
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    
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
            f"❌ Variables de entorno requeridas NO encontradas en .env: {', '.join(_missing_vars)}\n"
            f"Por favor, asegúrate de que el archivo .env existe y contiene todas las variables necesarias."
        )
    
    @property
    def POSTGRES_URL(self):
        """URL de conexión completa para PostgreSQL"""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

# Instancia global de configuración
config = Config()

# Verificar configuración al importar
if __name__ == "__main__":
    print("\n=== CONFIGURACIÓN DEL FRAMEWORK ===\n")
    print(f"PostgreSQL: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
    print(f"PostgreSQL URL: {config.POSTGRES_URL}")
    print(f"Log Level: {config.LOG_LEVEL}")
    print(f"Project Root: {config.PROJECT_ROOT}")
