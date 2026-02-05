# scripts/generate_sample_data.py
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
     - customers.csv/parquet/txt: Clientes ficticios en múltiples formatos
     - transactions.csv: Transacciones ficticias

  B) TABLAS EN POSTGRESQL:
     - sample_data.customers: Tabla de ejemplo para ingestar

NOTA: Las tablas de auditoría (pipeline.executions, pipeline.validation_results, etc.)
      se crean automáticamente al ejecutar pipelines. No se generan datos de ejemplo.

¿TIENE VALOR PARA PRODUCCIÓN?
  NO. Los datos generados son 100% ficticios para testing.
  
  Las tablas de auditoría (pipeline.*) SÍ guardarán datos reales cuando
  ejecutes pipelines con fuentes de datos reales.

IDEMPOTENTE: Puedes ejecutar este script múltiples veces sin problemas.
=============================================================================
"""

import sys
import argparse
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import config
from src.modules.ingestion.synthetic_generator import create_synthetic_generator
from src.modules.ingestion.connectors.postgres_connector import create_postgres_connector
import pandas as pd
import numpy as np

def generate_csv_samples(num_customers=1000, num_transactions=5000):
    """
    PASO 1: Generar archivos de datos sintéticos
    
    Crea archivos de ejemplo en data/samples/ para que puedas:
    - Probar el pipeline sin datos reales
    - Validar que los conectores (CSV, Parquet, TXT) funcionan
    - Testing de integración con datos limpios
    
    Args:
        num_customers: Número de clientes a generar (default: 1000)
        num_transactions: Número de transacciones a generar (default: 5000)
    """
    print(f"\n[*] Generando archivos de datos sintéticos ({num_customers:,} clientes, {num_transactions:,} transacciones)...")
    
    try:
        samples_dir = config.DATA_DIR / "samples"
        samples_dir.mkdir(parents=True, exist_ok=True)
        
        generator = create_synthetic_generator(seed=42)
        
        # 1. Customers - usar método completo del generador
        print(f"  - Generando customers (CSV, Parquet, TXT)... [{num_customers:,} registros]")
        customers = generator.generate_customer_data(num_customers=num_customers)
        
        formats_saved = generator.save_to_formats(
            df=customers,
            base_path=samples_dir / "customers",
            formats=['csv', 'parquet', 'txt']
        )
        
        for fmt, success in formats_saved.items():
            status = "✓" if success else "✗"
            print(f"    {status} customers.{fmt}")
        
        # 2. Transactions - transacciones para ejemplos de agregación
        print(f"  - Generando transactions.csv... [{num_transactions:,} registros]")
        transactions = generator.generate_transaction_data(num_transactions=num_transactions)
        transactions.to_csv(samples_dir / "transactions.csv", index=False)
        print(f"    ✓ OK: {len(transactions):,} registros")
        
        print(f"\n[✓] Archivos generados en: {samples_dir}")
        print(f"    Total: 6 archivos (4 formatos de customers + 2 CSVs adicionales)")
        
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
        
        # Nota: Las tablas de auditoría (pipeline.*, security.*, monitoring.*)
        # se poblarán automáticamente cuando ejecutes pipelines REALES.
        # No es necesario insertar datos de ejemplo aquí.
        
        connector.close()
        print("\n[✓] Tablas de PostgreSQL pobladas correctamente")
        return True
        
    except Exception as e:
        print(f"\n[✗] ERROR poblando PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Ejecutar generación de datos de ejemplo"""
    # Parsear argumentos CLI
    parser = argparse.ArgumentParser(
        description='Generar datos sintéticos para testing y desarrollo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Ejemplos de uso:
  %(prog)s                              # Generar 1,000 clientes y 5,000 transacciones (rápido)
  %(prog)s -c 100000 -t 500000          # Generar 100K clientes y 500K transacciones
  %(prog)s -c 1000000 -t 5000000        # Generar 1M clientes y 5M transacciones (lento)
  %(prog)s --skip-db                    # Solo archivos, sin poblar PostgreSQL
  %(prog)s --only-db                    # Solo PostgreSQL, sin archivos CSV
        ''')
    
    parser.add_argument('-c', '--customers', type=int, default=1000,
                        help='Número de clientes a generar (default: 1000)')
    parser.add_argument('-t', '--transactions', type=int, default=5000,
                        help='Número de transacciones a generar (default: 5000)')
    parser.add_argument('--skip-db', action='store_true',
                        help='No poblar tablas de PostgreSQL')
    parser.add_argument('--only-db', action='store_true',
                        help='Solo poblar PostgreSQL, no generar archivos')
    
    args = parser.parse_args()
    
    print("GENERACIÓN DE DATOS DE EJEMPLO PARA DESARROLLO Y TESTING")
    print(f"\n📊 Configuración:")
    print(f"  • Clientes: {args.customers:,}")
    print(f"  • Transacciones: {args.transactions:,}")
    print(f"  • Archivos CSV/Excel/Parquet: {'❌ Omitido' if args.only_db else '✓'}")
    print(f"  • PostgreSQL: {'❌ Omitido' if args.skip_db else '✓'}")
    print()
    
    success_files = False
    success_db = False
    
    try:
        customers_df = None
        
        # Generar archivos (CSV, Excel, Parquet, TXT)
        if not args.only_db:
            customers_df = generate_csv_samples(
                num_customers=args.customers,
                num_transactions=args.transactions
            )
            if customers_df is not None:
                success_files = True
        
        # Poblar PostgreSQL
        if not args.skip_db:
            if populate_postgres_tables(customers_df):
                success_db = True
        
        # Resumen final
        if success_files and success_db:
            print("[✓] DATOS DE EJEMPLO GENERADOS EXITOSAMENTE")
        elif success_files:
            print("[⚠] ARCHIVOS GENERADOS (PostgreSQL falló)")
        elif success_db:
            print("[⚠] POSTGRESQL POBLADO (Archivos fallaron)")
        else:
            print("[✗] ERROR: No se pudieron generar los datos")
        
        if success_files:
            print("\n📁 ARCHIVOS DE PRUEBA GENERADOS (data/samples/):")
            print("  ✓ customers.csv/parquet/txt - Datos ficticios para ingestar")
            print("  ✓ transactions.csv - Transacciones para ejemplos")
        
        if success_db:
            print("\n🗄️  TABLAS POBLADAS EN POSTGRESQL:")
            print("\n  DATOS DE PRUEBA:")
            print(f"    ✓ sample_data.customers - {args.customers:,} clientes sintéticos")
            print("\n  💡 Las tablas de auditoría se poblarán automáticamente al ejecutar pipelines")
            print("     (pipeline.executions, validation_results, metrics, etc.)")
        
        print("\n📚 Siguiente paso:")
        print("  1. Ejecutar setup completo: python scripts/setup_environment.py")
        print("  2. O ejecutar pipeline directamente con estos datos")
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