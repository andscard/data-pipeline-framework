# scripts/generate_samples.py
"""
=============================================================================
GENERADOR DE DATOS DE PRUEBA PARA DESARROLLO Y TESTING
=============================================================================

¿QUÉ HACE ESTE SCRIPT?
  Este script genera datos SINTÉTICOS (falsos) para que puedas probar
  el framework sin tener que conectarte a fuentes de datos reales.

¿POR QUÉ ES ÚTIL?
  1. Setup inicial: Cuando instalas el proyecto, necesitas datos para probar
  2. Desarrollo: Puedes probar nuevas features sin afectar datos reales
  3. Testing: Valida que las validaciones de calidad detecten errores

¿QUÉ GENERA?

  A) ARCHIVOS DE DATOS (data/samples/):
     - customers.csv/xlsx/parquet/txt: 1000 clientes ficticios
     - transactions.csv: 5000 transacciones ficticias
     - data_with_anomalies.csv: 500 registros CON ERRORES INTENCIONALES
       * Este archivo tiene nulls, outliers y duplicados A PROPÓSITO
       * Sirve para validar que tu pipeline detecte datos malos

  B) TABLAS EN POSTGRESQL:
     - sample_data.customers: Tabla de ejemplo para ingestar
     - pipeline.pipelines: Pipeline de ejemplo ya configurado
     - pipeline.executions: Historial de ejecución SIMULADA
     - pipeline.validation_results: Resultados de validación SIMULADOS
     - security.attack_scenarios: Escenarios de ataque de ejemplo
     - monitoring.metrics: Métricas de ejemplo para dashboard

¿TIENE VALOR PARA PRODUCCIÓN?
  NO. Estos datos son 100% ficticios.
  
  VALOR REAL:
  - Los DATOS de clientes/transacciones son solo para testing
  - Las TABLAS de auditoría (pipelines, executions, etc.) son las que
    guardarán información REAL cuando ejecutes pipelines con datos reales

¿PUEDO HACER REPORTES CON ESTO?
  SÍ, pero solo para DEMOSTRAR el framework.
  - Puedes consultar pipeline.executions para ver historial
  - Puedes consultar monitoring.metrics para gráficas
  - Puedes consultar security.simulation_results para reportes de seguridad
  
  Cuando ejecutes el pipeline con datos REALES, estas mismas tablas
  guardarán información real y útil para reportes de producción.

IDEMPOTENTE: Puedes ejecutar este script múltiples veces sin problemas.
=============================================================================
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import config
from src.modules.ingestion.synthetic_generator import create_synthetic_generator
from src.modules.ingestion.connectors.postgres_connector import create_postgres_connector
import pandas as pd
import numpy as np

def generate_csv_samples():
    """
    PASO 1: Generar archivos de datos sintéticos
    
    Crea archivos de ejemplo en data/samples/ para que puedas:
    - Probar el pipeline sin datos reales
    - Validar que los conectores (CSV, Excel, Parquet, TXT) funcionan
    - Testear validaciones de calidad con datos que tienen errores
    
    IMPORTANTE: El archivo 'data_with_anomalies.csv' tiene errores
    A PROPÓSITO (nulls, outliers, duplicados) para que puedas verificar
    que tu pipeline detecta datos malos correctamente.
    """
    print("\n[*] Generando archivos de datos sintéticos...")
    
    try:
        samples_dir = config.DATA_DIR / "samples"
        samples_dir.mkdir(parents=True, exist_ok=True)
        
        generator = create_synthetic_generator(seed=42)
        
        # 1. Customers - múltiples formatos para demostrar capacidades
        print("  - Generando customers en múltiples formatos...")
        customers = generator.generate_customer_data(num_customers=1000)
        
        formats_saved = generator.save_to_formats(
            df=customers,
            base_path=samples_dir / "customers",
            formats=['csv', 'excel', 'parquet']
        )
        
        for fmt, success in formats_saved.items():
            status = "✓" if success else "✗"
            print(f"    {status} customers.{fmt}")
        
        # 2. Transactions - transacciones para ejemplos de agregación
        print("  - Generando transactions.csv...")
        transactions = generator.generate_transaction_data(num_transactions=5000)
        transactions.to_csv(samples_dir / "transactions.csv", index=False)
        print(f"    ✓ OK: {len(transactions)} registros")
        
        print(f"\n[✓] Archivos generados en: {samples_dir}")
        print(f"    Total: 4 archivos (customers en 3 formatos + transactions)")
        
        return customers
        
    except Exception as e:
        print(f"\n[✗] ERROR generando archivos: {e}")
        import traceback
        traceback.print_exc()
        return None


def populate_postgres_tables(customers_df=None):
    """
    PASO 2: Poblar tablas de PostgreSQL con datos de ejemplo
    
    Inserta datos SINTÉTICOS en dos tipos de tablas:
    
    A) TABLA DE DATOS (sample_data.customers):
       - Datos ficticios de 1000 clientes
       - Sirve como fuente de datos para probar el pipeline
       - NO tiene valor para producción, es solo para testing
    
    B) TABLAS DE AUDITORÍA (pipeline.*, security.*, monitoring.*):
       - Datos de ejemplo para mostrar cómo funciona el sistema
       - Estas MISMAS tablas guardarán datos REALES cuando ejecutes
         pipelines con fuentes de datos reales
       - SÍ tienen valor: puedes hacer reportes, dashboards, alertas
    
    ¿QUÉ PUEDES HACER CON LAS TABLAS DE AUDITORÍA?
    - pipeline.executions: Ver historial de ejecuciones (cuándo, cuánto tardó, cuántos registros)
    - pipeline.validation_results: Ver qué validaciones pasaron/fallaron
    - security.simulation_results: Ver resultados de ataques simulados
    - monitoring.metrics: Ver métricas para dashboards (Grafana, PowerBI)
    
    Args:
        customers_df: DataFrame de customers (si no se provee, se genera automáticamente)
    """
    print("\n[*] Poblando tablas de PostgreSQL...")
    
    try:
        print("  - Conectando a PostgreSQL...")
        connector = create_postgres_connector(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
            mode="sqlalchemy"
        )
        
        if not connector.connect():
            print("[✗] ERROR: No se pudo conectar a PostgreSQL")
            print("    Verifica: docker-compose up -d postgres")
            return False
        
        print("    ✓ Conexión exitosa")
        
        from sqlalchemy import text
        
        # Crear schema si no existe
        print("  - Creando schema sample_data...")
        with connector.engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS sample_data;"))
        print("    ✓ Schema listo")
        
        # Generar o usar customers existentes
        if customers_df is None:
            print("  - Generando datos de customers...")
            generator = create_synthetic_generator(seed=42)
            customers_df = generator.generate_customer_data(num_customers=1000)
        
        # Guardar en PostgreSQL
        print("  - Insertando tabla sample_data.customers...")
        customers_df.to_sql(
            name='customers',
            schema='sample_data',
            con=connector.engine,
            if_exists='replace',
            index=False
        )
        print(f"    ✓ Tabla creada con {len(customers_df)} registros")
        
        connector.close()
        print("\n[✓] Tabla sample_data.customers poblada correctamente")
        return True
        
    except Exception as e:
        print(f"\n[✗] ERROR poblando PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Ejecutar generación de datos de ejemplo"""
    print("="*70)
    print("GENERACIÓN DE DATOS DE EJEMPLO PARA DESARROLLO Y TESTING")
    print("="*70)
    print("\nEste script genera datos dummy en múltiples formatos:")
    print("  • CSV, Excel, Parquet")
    print("  • Tabla PostgreSQL (sample_data.customers)")
    print()
    
    success_files = False
    success_db = False
    
    try:
        # Generar archivos (CSV, Excel, Parquet)
        customers_df = generate_csv_samples()
        if customers_df is not None:
            success_files = True
        
        # Poblar PostgreSQL
        if populate_postgres_tables(customers_df):
            success_db = True
        
        # Resumen final
        print("\n" + "="*70)
        if success_files and success_db:
            print("[✓] DATOS DE EJEMPLO GENERADOS EXITOSAMENTE")
        elif success_files:
            print("[⚠] ARCHIVOS GENERADOS (PostgreSQL falló)")
        elif success_db:
            print("[⚠] POSTGRESQL POBLADO (Archivos fallaron)")
        else:
            print("[✗] ERROR: No se pudieron generar los datos")
        print("="*70)
        
        if success_files:
            print("\n📁 ARCHIVOS GENERADOS (data/samples/):")
            print("  ✓ customers.csv - 1000 clientes")
            print("  ✓ customers.xlsx - 1000 clientes (Excel)")
            print("  ✓ customers.parquet - 1000 clientes (Parquet)")
            print("  ✓ transactions.csv - 5000 transacciones")
            print("\n  Demostración de capacidades:")
            print("  → Lectura: Pipeline lee CSV, Excel, Parquet, PostgreSQL")
            print("  → Escritura: Pipeline escribe CSV, Excel, Parquet, PostgreSQL")
        
        if success_db:
            print("\n🗄️  TABLA POBLADA EN POSTGRESQL:")
            print("  ✓ sample_data.customers - 1000 clientes")
        
        print("\n📚 Siguiente paso:")
        print("  Ejecutar pipeline: python -m src.cli run pipeline -c examples/complete_pipeline.yml")
        print()
        
        if not (success_files and success_db):
            sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n\n[!] Generación interrumpida por el usuario")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n[✗] ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()