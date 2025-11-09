# generate_secrets.py
import secrets
from cryptography.fernet import Fernet

print("=== CLAVES SECRETAS GENERADAS ===\n")
print(f"AIRFLOW_FERNET_KEY={Fernet.generate_key().decode()}")
print(f"AIRFLOW_SECRET_KEY={secrets.token_urlsafe(32)}")
print(f"JWT_SECRET_KEY={secrets.token_urlsafe(32)}")
print(f"POSTGRES_PASSWORD={secrets.token_urlsafe(16)}")
print(f"REDIS_PASSWORD={secrets.token_urlsafe(16)}")
print(f"GRAFANA_ADMIN_PASSWORD={secrets.token_urlsafe(12)}")