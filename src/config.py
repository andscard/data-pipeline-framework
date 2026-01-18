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
    
    # Paths
    PROJECT_ROOT = project_root
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"
    REPORTS_DIR = PROJECT_ROOT / "reports"
    
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5440"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "pipeline_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret_password")
    
    @property
    def POSTGRES_URL(self):
        """URL de conexión completa para PostgreSQL"""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "5540"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "redis_secret")
    
    # API
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # JWT
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "insecure_dev_key")
    JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_EXPIRATION_MINUTES = int(os.getenv("JWT_EXPIRATION_MINUTES", "30"))
    
    # Simulation
    SIMULATION_MODE = os.getenv("SIMULATION_MODE", "true").lower() == "true"
    DEFAULT_ATTACK_DELAY_MS = int(os.getenv("DEFAULT_ATTACK_DELAY_MS", "100"))
    MAX_SIMULATION_DURATION_MINUTES = int(os.getenv("MAX_SIMULATION_DURATION_MINUTES", "60"))
    ENABLE_DESTRUCTIVE_ATTACKS = os.getenv("ENABLE_DESTRUCTIVE_ATTACKS", "false").lower() == "true"

# Instancia global de configuración
config = Config()

# Verificar configuración al importar
if __name__ == "__main__":
    print("\n=== CONFIGURACIÓN DEL FRAMEWORK ===\n")
    print(f"PostgreSQL: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
    print(f"Redis: {config.REDIS_HOST}:{config.REDIS_PORT}")
    print(f"API: {config.API_HOST}:{config.API_PORT}")
    print(f"Simulation Mode: {config.SIMULATION_MODE}")
    print(f"\nPostgreSQL URL: {config.POSTGRES_URL}")