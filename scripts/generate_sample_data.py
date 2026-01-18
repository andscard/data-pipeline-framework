# scripts/generate_sample_data.py
"""
Script para generar datos de ejemplo en CSV y PostgreSQL
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import config
from src.modules.ingestion.synthetic_generator import create_synthetic_generator
from src.modules.ingestion.connectors.postgres_connector import create_postgres_connector
import pandas as pd

def generate_csv_samples():
    """Generar archivos CSV de ejemplo"""
    print("\n[*] Generando archivos CSV de ejemplo...")
    
    # Crear directorio si no existe
    samples_dir = config.DATA_DIR / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)
    
    # Generar diferentes tipos de datos
    generator = create_synthetic_generator(seed=42)
    
    # 1. Datos de clientes
    print("  - Generando customers.csv...")
    customers = generator.generate_customer_data(num_customers=1000)
    customers.to_csv(samples_dir / "customers.csv", index=False)
    print(f"    OK: {len(customers)} registros")
    
    # 2. Datos de transacciones
    print("  - Generando transactions.csv...")
    transactions = generator.generate_transaction_data(num_transactions=5000)
    transactions.to_csv(samples_dir / "transactions.csv", index=False)
    print(f"    OK: {len(transactions)} registros")
    
    # 3. Datos con anomalías
    print("  - Generando data_with_anomalies.csv...")
    schema = {
        'id': 'int',
        'name': 'name',
        'email': 'email',
        'amount': 'amount',
        'date': 'date'
    }
    clean_data = generator.generate(schema, num_records=500)
    anomalous_data = generator.inject_anomalies(
        clean_data,
        anomaly_rate=0.10,
        anomaly_types=['nulls', 'outliers', 'duplicates']
    )
    anomalous_data.to_csv(samples_dir / "data_with_anomalies.csv", index=False)
    print(f"    OK: {len(anomalous_data)} registros (10% con anomalías)")
    
    print(f"\n[OK] Archivos CSV generados en: {samples_dir}")


def populate_postgres_tables():
    """Poblar tablas de PostgreSQL con datos de ejemplo"""
    print("\n[*] Poblando tablas de PostgreSQL...")
    
    try:
        # Conectar a PostgreSQL con tus puertos
        connector = create_postgres_connector(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
            mode="psycopg2"
        )
        
        if not connector.connect():
            print("[ERROR] No se pudo conectar a PostgreSQL")
            return
        
        generator = create_synthetic_generator(seed=42)
        
        # 1. Poblar pipeline.pipelines (ya tiene 1 registro de ejemplo)
        print("  - pipeline.pipelines ya tiene datos de init_db.sql")
        
        # 2. Crear una ejecución de ejemplo
        print("  - Insertando ejecución de ejemplo...")
        execution_query = """
            INSERT INTO pipeline.executions 
            (pipeline_id, status, records_processed, records_failed, metrics)
            SELECT 
                id,
                'completed',
                1000,
                50,
                '{"duration_seconds": 45.2, "throughput": 22.2}'::jsonb
            FROM pipeline.pipelines 
            WHERE name = 'example_pipeline'
            LIMIT 1;
        """
        
        # Usar SQLAlchemy para ejecutar
        from sqlalchemy import text
        with connector.engine.connect() as conn:
            conn.execute(text(execution_query))
            conn.commit()
        
        print("    OK: 1 ejecución insertada")
        
        # 3. Crear validaciones de ejemplo
        print("  - Insertando resultados de validación...")
        validation_query = """
            INSERT INTO pipeline.validation_results 
            (execution_id, rule_name, rule_type, passed, failed_count, failure_details)
            SELECT 
                e.id,
                'email_not_null',
                'not_null',
                false,
                50,
                '[{"row": 10, "column": "email", "value": null}]'::jsonb
            FROM pipeline.executions e
            ORDER BY e.start_time DESC
            LIMIT 1;
        """
        
        with connector.engine.connect() as conn:
            conn.execute(text(validation_query))
            conn.commit()
        
        print("    OK: Resultados de validación insertados")
        
        # 4. Crear escenario de ataque de ejemplo
        print("  - Insertando escenario de ataque...")
        attack_query = """
            INSERT INTO security.attack_scenarios 
            (name, description, attack_types, config, created_by)
            VALUES 
            (
                'data_poisoning_test',
                'Escenario de prueba para contaminación de datos',
                ARRAY['data_poisoning'],
                '{"poison_rate": 0.05, "target_fields": ["amount"]}'::jsonb,
                'system'
            )
            ON CONFLICT (name) DO NOTHING;
        """
        
        with connector.engine.connect() as conn:
            conn.execute(text(attack_query))
            conn.commit()
        
        print("    OK: Escenario de ataque insertado")
        
        connector.close()
        print("\n[OK] Tablas de PostgreSQL pobladas correctamente")
        
    except Exception as e:
        print(f"\n[ERROR] Error poblando PostgreSQL: {e}")


def main():
    """Ejecutar generación de datos de ejemplo"""
    print("="*60)
    print("GENERACIÓN DE DATOS DE EJEMPLO")
    print("="*60)
    
    try:
        # Generar CSVs
        generate_csv_samples()
        
        # Poblar PostgreSQL
        populate_postgres_tables()
        
        print("\n" + "="*60)
        print("[OK] Datos de ejemplo generados exitosamente")
        print("="*60)
        
        print("\nArchivos generados:")
        print(f"  - {config.DATA_DIR / 'samples' / 'customers.csv'}")
        print(f"  - {config.DATA_DIR / 'samples' / 'transactions.csv'}")
        print(f"  - {config.DATA_DIR / 'samples' / 'data_with_anomalies.csv'}")
        
        print("\nTablas pobladas en PostgreSQL:")
        print(f"  - pipeline.executions")
        print(f"  - pipeline.validation_results")
        print(f"  - security.attack_scenarios")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()