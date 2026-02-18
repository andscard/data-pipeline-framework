# scripts/generate_sample_data.py
"""
Generador de Datos Sintéticos para Testing y Desarrollo

¿QUÉ HACE ESTE SCRIPT?
  Este script genera datos SINTÉTICOS (falsos) para que puedas probar
  el framework sin tener que conectarte a fuentes de datos reales.

=============================================================================
"""

import sys
import argparse
from pathlib import Path
import traceback
from sqlalchemy import text
from src.config import config
from src.modules.ingestion.synthetic_generator import create_synthetic_generator
from src.modules.ingestion.connectors.postgres_connector import (
    create_postgres_connector,
)

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def generate_csv_samples(num_customers=1000, num_transactions=5000):
    """
    Generar archivos de datos sintéticos

    Args:
        num_customers: Número de clientes a generar (default: 1000)
        num_transactions: Número de transacciones a generar (default: 5000)
    """
    print(
        f"\n[*] Generando archivos de datos sintéticos ({num_customers:,} clientes, {num_transactions:,} transacciones)..."
    )

    try:
        samples_dir = config.DATA_DIR / "samples"
        samples_dir.mkdir(parents=True, exist_ok=True)

        generator = create_synthetic_generator(seed=42)

        # 1. Customers - usar método completo del generador
        print(
            f"  - Generando customers (CSV, Parquet, TXT)... [{num_customers:,} registros]"
        )
        customers = generator.generate_customer_data(num_customers=num_customers)

        formats_saved = generator.save_to_formats(
            df=customers,
            base_path=samples_dir / "customers",
            formats=["csv", "parquet", "txt"],
        )

        for fmt, success in formats_saved.items():
            status = "✓" if success else "✗"
            print(f"    {status} customers.{fmt}")

        # 2. Transactions - transacciones para ejemplos de agregación
        print(f"  - Generando transactions.csv... [{num_transactions:,} registros]")
        transactions = generator.generate_transaction_data(
            num_transactions=num_transactions
        )
        transactions.to_csv(samples_dir / "transactions.csv", index=False)
        print(f"    ✓ OK: {len(transactions):,} registros")

        print(f"\n[✓] Archivos generados en: {samples_dir}")
        print(
            f"    Total: 4 archivos ({len(formats_saved)} formatos de customers + 1 transactions.csv)"
        )

        return customers, transactions

    except Exception as e:
        print(f"\n[✗] ERROR generando archivos: {e}")
        traceback.print_exc()
        return None


def populate_postgres_tables(customers_df=None, transactions_df=None):
    """
    Poblar tablas de PostgreSQL con datos de ejemplo

    Args:
        customers_df: DataFrame de customers (si no se provee, se genera automáticamente)
        transactions_df: DataFrame de transactions (si no se provee, se genera automáticamente)
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
            mode="sqlalchemy",
        )

        if not connector.connect():
            print("[✗] ERROR: No se pudo conectar a PostgreSQL")
            print("    Verifica: docker-compose up -d postgres")
            return False

        print("    ✓ Conexión exitosa")


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

        # Generar o usar transactions existentes
        if transactions_df is None:
            print("  - Generando datos de transactions...")
            if 'generator' not in locals():
                generator = create_synthetic_generator(seed=42)
            transactions_df = generator.generate_transaction_data(num_transactions=5000)

        # Guardar customers en PostgreSQL
        print("  - Insertando tabla sample_data.customers...")
        customers_df.to_sql(
            name="customers",
            schema="sample_data",
            con=connector.engine,
            if_exists="replace",
            index=False,
        )
        print(f"    ✓ Tabla creada con {len(customers_df):,} registros")

        # Guardar transactions en PostgreSQL
        print("  - Insertando tabla sample_data.transactions...")
        transactions_df.to_sql(
            name="transactions",
            schema="sample_data",
            con=connector.engine,
            if_exists="replace",
            index=False,
        )
        print(f"    ✓ Tabla creada con {len(transactions_df):,} registros")

        connector.close()
        print("\n[✓] Tablas de PostgreSQL pobladas correctamente")
        return True

    except Exception as e:
        print(f"\n[✗] ERROR poblando PostgreSQL: {e}")
        traceback.print_exc()
        return False


def main():
    """Ejecutar generación de datos de ejemplo"""
    parser = argparse.ArgumentParser(
        description="Generar datos sintéticos para testing y desarrollo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  %(prog)s                              # Generar 1,000 clientes y 5,000 transacciones (rápido)
  %(prog)s -c 100000 -t 500000          # Generar 100K clientes y 500K transacciones
  %(prog)s -c 1000000 -t 5000000        # Generar 1M clientes y 5M transacciones (lento)
  %(prog)s --skip-db                    # Solo archivos, sin poblar PostgreSQL
  %(prog)s --only-db                    # Solo PostgreSQL, sin archivos CSV
        """,
    )

    parser.add_argument(
        "-c",
        "--customers",
        type=int,
        default=1000,
        help="Número de clientes a generar (default: 1000)",
    )
    parser.add_argument(
        "-t",
        "--transactions",
        type=int,
        default=5000,
        help="Número de transacciones a generar (default: 5000)",
    )
    parser.add_argument(
        "--skip-db", action="store_true", help="No poblar tablas de PostgreSQL"
    )
    parser.add_argument(
        "--only-db",
        action="store_true",
        help="Solo poblar PostgreSQL, no generar archivos",
    )

    args = parser.parse_args()

    success_files = False
    success_db = False

    try:
        customers_df = None
        transactions_df = None

        # Generar archivos (CSV, Excel, Parquet, TXT)
        if not args.only_db:
            result = generate_csv_samples(
                num_customers=args.customers, num_transactions=args.transactions
            )
            if result is not None:
                customers_df, transactions_df = result
                success_files = True

        # Poblar PostgreSQL
        if not args.skip_db:
            if populate_postgres_tables(customers_df, transactions_df):
                success_db = True

        if success_files:
            print("\n📁 ARCHIVOS DE PRUEBA GENERADOS (data/samples/):")
            print("  ✓ customers.csv/parquet/txt - Datos ficticios para ingestar")
            print("  ✓ transactions.csv - Transacciones para ejemplos")

        if success_db:
            print("\n🗄️  TABLAS POBLADAS EN POSTGRESQL:")
            print(
                f"    ✓ sample_data.customers - {args.customers:,} clientes sintéticos"
            )
            print(
                f"    ✓ sample_data.transactions - {args.transactions:,} transacciones sintéticas"
            )

        if not (success_files and success_db):
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n[!] Generación interrumpida por el usuario")
        sys.exit(1)

    except Exception as e:
        print(f"\n[✗] ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
