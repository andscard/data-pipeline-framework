# scripts/test_connections_safe.py
"""
Script para verificar conectividad a PostgreSQL y Redis
VERSION SIN EMOJIS - Compatible con Windows PowerShell
"""

import os
import sys
from pathlib import Path

# Configurar encoding para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Asegurarse de buscar .env en la raíz del proyecto
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

print(f"[*] Directorio de trabajo: {os.getcwd()}")
print(f"[*] Archivo .env existe: {Path('.env').exists()}\n")

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

# Verificar que se cargaron las variables
print("[INFO] Variables de entorno detectadas:")
print(f"   POSTGRES_DB: {os.getenv('POSTGRES_DB', 'NO ENCONTRADA')}")
print(f"   POSTGRES_USER: {os.getenv('POSTGRES_USER', 'NO ENCONTRADA')}")
print(f"   POSTGRES_PASSWORD: {os.getenv('POSTGRES_PASSWORD', 'NO ENCONTRADA')}")
print(f"   REDIS_PASSWORD: {os.getenv('REDIS_PASSWORD', 'NO ENCONTRADA')}")
#print(f"   POSTGRES_PASSWORD: {'***' if os.getenv('POSTGRES_PASSWORD') else 'NO ENCONTRADA'}")
#print(f"   REDIS_PASSWORD: {'***' if os.getenv('REDIS_PASSWORD') else 'NO ENCONTRADA'}\n")

# Imports de librerías
import psycopg2
from psycopg2 import sql
import redis
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def test_postgres_psycopg2():
    """Probar conexión a PostgreSQL usando psycopg2"""
    print("\n" + "="*60)
    print("[PostgreSQL] PROBANDO CONEXION (psycopg2)")
    print("="*60)
    
    # Obtener credenciales
    db_host = "localhost"
    db_port = 5440
    db_name = os.getenv("POSTGRES_DB", "pipeline_db")
    db_user = os.getenv("POSTGRES_USER", "admin")
    db_password = os.getenv("POSTGRES_PASSWORD", "secret_password")
    
    print(f"[*] Intentando conectar a: {db_user}@{db_host}:{db_port}/{db_name}")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=5
        )
        
        cursor = conn.cursor()
        
        # Query de prueba
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"[OK] Conexion exitosa")
        print(f"[*] Version PostgreSQL: {version[:80]}...")
        
        # Verificar esquemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('pipeline', 'security', 'monitoring')
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()
        print(f"\n[*] Esquemas encontrados:")
        for schema in schemas:
            print(f"   - {schema[0]}")
        
        # Verificar tablas
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'pipeline'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        print(f"\n[*] Tablas en esquema 'pipeline':")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM pipeline.pipelines;")
        count = cursor.fetchone()[0]
        print(f"\n[*] Registros en pipeline.pipelines: {count}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"[ERROR] Error de conexion a PostgreSQL:")
        print(f"   {str(e)}")
        print(f"\n[SUGERENCIAS]")
        print(f"   1. Verifica que PostgreSQL este corriendo: docker-compose ps")
        print(f"   2. Verifica las credenciales en .env")
        print(f"   3. Prueba conectar desde PowerShell:")
        print(f"      docker exec -it framework_postgres psql -U admin -d pipeline_db")
        return False
    except Exception as e:
        print(f"[ERROR] Error inesperado: {type(e).__name__}: {e}")
        return False

def test_postgres_sqlalchemy():
    """Probar conexión a PostgreSQL usando SQLAlchemy"""
    print("\n" + "="*60)
    print("[PostgreSQL] PROBANDO CONEXION (SQLAlchemy)")
    print("="*60)
    
    try:
        # Construir URL de conexión
        db_user = os.getenv('POSTGRES_USER', 'admin')
        db_password = os.getenv('POSTGRES_PASSWORD', 'secret_password')
        db_host = 'localhost'
        db_port = 5440
        db_name = os.getenv('POSTGRES_DB', 'pipeline_db')
        
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        print(f"[*] URL de conexion: postgresql://{db_user}:***@{db_host}:{db_port}/{db_name}")
        
        # Crear engine
        engine = create_engine(db_url, echo=False, connect_args={'connect_timeout': 5})
        
        # Probar conexión
        with engine.connect() as connection:
            result = connection.execute(text("SELECT current_database(), current_user;"))
            row = result.fetchone()
            print(f"[OK] Conexion exitosa")
            print(f"[*] Base de datos actual: {row[0]}")
            print(f"[*] Usuario actual: {row[1]}")
            
            # Query con parámetros
            result = connection.execute(
                text("SELECT name, description FROM pipeline.pipelines WHERE is_active = :active"),
                {"active": True}
            )
            pipelines = result.fetchall()
            
            if pipelines:
                print(f"\n[*] Pipelines activos encontrados:")
                for pipeline in pipelines:
                    print(f"   - {pipeline[0]}: {pipeline[1]}")
            else:
                print(f"\n[*] No hay pipelines activos")
        
        return True
        
    except OperationalError as e:
        print(f"[ERROR] Error de conexion a PostgreSQL:")
        print(f"   {str(e)}")
        return False
    except Exception as e:
        print(f"[ERROR] Error inesperado: {type(e).__name__}: {e}")
        return False

def test_redis():
    """Probar conexión a Redis"""
    print("\n" + "="*60)
    print("[Redis] PROBANDO CONEXION")
    print("="*60)
    
    redis_host = 'localhost'
    redis_port = 7000  # Puerto personalizado
    redis_password = os.getenv("REDIS_PASSWORD", "redis_secret")
    
    print(f"[*] Intentando conectar a: {redis_host}:{redis_port}")
    
    try:
        r = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        
        # Probar conexión
        r.ping()
        print(f"[OK] Conexion exitosa")
        
        # Info del servidor
        info = r.info('server')
        print(f"[*] Version Redis: {info['redis_version']}")
        print(f"[*] Puerto: {redis_port}")
        
        # Operaciones de prueba
        test_key = "framework:test:connection"
        test_value = "Connection test successful!"
        
        r.set(test_key, test_value, ex=60)
        print(f"\n[*] SET: {test_key} = {test_value}")
        
        retrieved = r.get(test_key)
        print(f"[*] GET: {test_key} = {retrieved}")
        
        ttl = r.ttl(test_key)
        print(f"[*] TTL: {ttl} segundos")
        
        keys_count = len(r.keys('*'))
        print(f"\n[*] Total de claves en Redis: {keys_count}")
        
        return True
        
    except redis.ConnectionError as e:
        print(f"[ERROR] Error de conexion a Redis:")
        print(f"   {str(e)}")
        print(f"\n[SUGERENCIAS]")
        print(f"   1. Verifica que Redis este corriendo: docker-compose ps")
        print(f"   2. Verifica el puerto (7000): docker-compose logs redis")
        print(f"   3. Prueba conectar:")
        print(f"      docker exec -it framework_redis redis-cli -a {redis_password}")
        return False
    except redis.TimeoutError as e:
        print(f"[ERROR] Timeout al conectar a Redis:")
        print(f"   {str(e)}")
        print(f"[SUGERENCIAS] Verifica que el puerto 7000 este accesible")
        return False
    except Exception as e:
        print(f"[ERROR] Error inesperado: {type(e).__name__}: {e}")
        return False

def main():
    """Ejecutar todas las pruebas de conexión"""
    print("\n" + "="*60)
    print("VERIFICACION DE CONECTIVIDAD A SERVICIOS")
    print("="*60)
    import datetime
    print(f"Timestamp: {datetime.datetime.now()}\n")
    
    results = {
        'postgres_psycopg2': False,
        'postgres_sqlalchemy': False,
        'redis': False
    }
    
    # Probar conexiones
    results['postgres_psycopg2'] = test_postgres_psycopg2()
    results['postgres_sqlalchemy'] = test_postgres_sqlalchemy()
    results['redis'] = test_redis()
    
    # Resumen
    print("\n" + "="*60)
    print("RESUMEN DE PRUEBAS")
    print("="*60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "[PASS]" if result else "[FAIL]"
        print(f"{test_name:.<40} {status}")
    
    print("-" * 60)
    print(f"Total: {passed}/{total} pruebas pasadas ({passed/total*100:.0f}%)")
    print("="*60 + "\n")
    
    if passed == total:
        print("[OK] TODAS LAS CONEXIONES FUNCIONAN CORRECTAMENTE!")
        print("[OK] Paso 2 completado exitosamente\n")
        sys.exit(0)
    else:
        print("[WARNING] Algunas conexiones fallaron. Revisa los errores arriba.\n")
        sys.exit(1)

if __name__ == "__main__":
    main()