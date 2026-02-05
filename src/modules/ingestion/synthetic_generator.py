# src/modules/ingestion/synthetic_generator.py
"""
Generador de datos sintéticos para pruebas y simulaciones
"""

import pandas as pd
import numpy as np
from faker import Faker
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    """
    Generador de datos sintéticos realistas usando Faker.
    
    Soporta generación de múltiples tipos de datos:
    - Datos personales (nombres, emails, direcciones)
    - Datos de transacciones
    - Datos de clientes y productos
    """
    
    def __init__(self, locale: str = 'es_ES', seed: Optional[int] = None):
        """
        Inicializar generador.
        
        Args:
            locale: Localización para Faker (default: español de España)
            seed: Semilla para reproducibilidad
        """
        self.fake = Faker(locale)
        if seed is not None:
            Faker.seed(seed)
            np.random.seed(seed)
        
        self.locale = locale
        logger.info(f"SyntheticDataGenerator initialized with locale={locale}, seed={seed}")
    
    def generate(
        self,
        schema: Dict[str, str],
        num_records: int = 1000
    ) -> pd.DataFrame:
        """
        Generar dataset según esquema definido.
        
        Args:
            schema: Dict con nombre_columna: tipo_dato
                Tipos soportados:
                - 'int', 'float', 'string', 'bool'
                - 'name', 'email', 'phone', 'address'
                - 'date', 'datetime', 'timestamp'
                - 'uuid', 'category'
                - 'amount', 'price'
            num_records: Número de registros a generar
            
        Returns:
            pd.DataFrame con datos generados
        """
        logger.info(f"Generating {num_records} records with schema: {schema}")
        
        data = {}
        
        for column_name, column_type in schema.items():
            data[column_name] = self._generate_column(column_type, num_records)
        
        df = pd.DataFrame(data)
        logger.info(f"Generated dataset: {len(df)} records, {len(df.columns)} columns")
        
        return df
    
    def _generate_column(self, column_type: str, num_records: int) -> List[Any]:
        """Generar datos para una columna según su tipo"""
        
        # Tipos numéricos
        if column_type == 'int':
            return np.random.randint(0, 1000, num_records).tolist()
        
        elif column_type == 'float':
            return np.random.uniform(0, 1000, num_records).tolist()
        
        elif column_type == 'bool':
            return np.random.choice([True, False], num_records).tolist()
        
        # Tipos de texto
        elif column_type == 'string':
            return [self.fake.word() for _ in range(num_records)]
        
        elif column_type == 'name':
            return [self.fake.name() for _ in range(num_records)]
        
        elif column_type == 'email':
            return [self.fake.email() for _ in range(num_records)]
        
        elif column_type == 'phone':
            return [self.fake.phone_number() for _ in range(num_records)]
        
        elif column_type == 'address':
            return [self.fake.address() for _ in range(num_records)]
        
        elif column_type == 'company':
            return [self.fake.company() for _ in range(num_records)]
        
        # Tipos temporales
        elif column_type == 'date':
            return [self.fake.date_between(start_date='-1y', end_date='today') for _ in range(num_records)]
        
        elif column_type == 'datetime':
            return [self.fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_records)]
        
        elif column_type == 'timestamp':
            base = datetime.now()
            return [(base - timedelta(seconds=np.random.randint(0, 86400*365))) for _ in range(num_records)]
        
        # Identificadores
        elif column_type == 'uuid':
            return [self.fake.uuid4() for _ in range(num_records)]
        
        elif column_type == 'category':
            categories = ['A', 'B', 'C', 'D']
            return np.random.choice(categories, num_records).tolist()
        
        # Financiero
        elif column_type == 'amount' or column_type == 'price':
            return np.random.uniform(10, 10000, num_records).round(2).tolist()
        
        else:
            logger.warning(f"Unknown column type: {column_type}, using random integers")
            return np.random.randint(0, 100, num_records).tolist()
    
    def generate_customer_data(self, num_customers: int = 1000) -> pd.DataFrame:
        """
        Generar dataset de clientes completo y realista.
        
        Args:
            num_customers: Número de clientes a generar
            
        Returns:
            pd.DataFrame con datos de clientes (8 columnas)
        """
        df = pd.DataFrame({
            'customer_id': [self.fake.uuid4() for _ in range(num_customers)],
            'name': [self.fake.name() for _ in range(num_customers)],
            'email': [self.fake.email() for _ in range(num_customers)],
            'phone': [self.fake.phone_number() for _ in range(num_customers)],
            'address': [self.fake.address().replace('\n', ', ') for _ in range(num_customers)],
            'registration_date': pd.date_range('2024-01-01', periods=num_customers, freq='H'),
            'last_login': pd.date_range('2025-01-01', periods=num_customers, freq='30min'),
            'account_status': np.random.choice(
                ['active', 'inactive', 'suspended'], 
                num_customers, 
                p=[0.8, 0.15, 0.05]
            ),
            'lifetime_value': np.random.uniform(100, 10000, num_customers).round(2)
        })
        
        return df
    
    def generate_transaction_data(self, num_transactions: int = 10000) -> pd.DataFrame:
        """
        Generar dataset de transacciones completo y realista.
        
        Args:
            num_transactions: Número de transacciones a generar
            
        Returns:
            pd.DataFrame con datos de transacciones
        """
        df = pd.DataFrame({
            'transaction_id': [self.fake.uuid4() for _ in range(num_transactions)],
            'customer_id': [self.fake.uuid4() for _ in range(num_transactions)],
            'timestamp': [self.fake.date_time_between(start_date='-30d', end_date='now') 
                         for _ in range(num_transactions)],
            'amount': np.random.uniform(10, 5000, num_transactions).round(2),
            'category': np.random.choice(
                ['Electronics', 'Clothing', 'Food', 'Books', 'Other'],
                num_transactions
            ),
            'status': np.random.choice(
                ['completed', 'pending', 'cancelled'],
                num_transactions,
                p=[0.85, 0.10, 0.05]
            )
        })
        
        return df


    def save_to_formats(self, df: pd.DataFrame, base_path: Path, formats: List[str] = None) -> Dict[str, bool]:
        """
        Guardar DataFrame en múltiples formatos.
        
        Args:
            df: DataFrame a guardar
            base_path: Path base (sin extensión)
            formats: Lista de formatos ['csv', 'excel', 'parquet', 'txt']
            
        Returns:
            Dict con formato: éxito
        """
        if formats is None:
            formats = ['csv', 'excel', 'parquet', 'txt']
        
        results = {}
        base_path = Path(base_path)
        base_path.parent.mkdir(parents=True, exist_ok=True)
        
        for fmt in formats:
            try:
                if fmt == 'csv':
                    df.to_csv(f"{base_path}.csv", index=False)
                elif fmt == 'excel':
                    df.to_excel(f"{base_path}.xlsx", index=False, sheet_name='Data')
                elif fmt == 'parquet':
                    df.to_parquet(f"{base_path}.parquet", index=False, engine='pyarrow')
                elif fmt == 'txt':
                    df.to_csv(f"{base_path}.txt", index=False, sep='|')
                results[fmt] = True
            except Exception as e:
                logger.error(f"Failed to save {fmt}: {e}")
                results[fmt] = False
        
        return results


# Factory function
def create_synthetic_generator(locale: str = 'es_ES', seed: Optional[int] = None) -> SyntheticDataGenerator:
    """
    Factory function para crear un generador de datos sintéticos.
    
    Args:
        locale: Localización para Faker
        seed: Semilla para reproducibilidad
        
    Returns:
        SyntheticDataGenerator configurado
    """
    return SyntheticDataGenerator(locale=locale, seed=seed)