# src/modules/ingestion/synthetic_generator.py
"""
Generador de datos sintéticos para pruebas y simulaciones
"""

import pandas as pd
import numpy as np
from faker import Faker
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
    - Datos de sensores/IoT
    - Inyección controlada de anomalías
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
    
    def inject_anomalies(
        self,
        df: pd.DataFrame,
        anomaly_rate: float = 0.05,
        anomaly_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Inyectar anomalías controladas en un dataset.
        
        Args:
            df: DataFrame original
            anomaly_rate: Porcentaje de registros a contaminar (0.0-1.0)
            anomaly_types: Tipos de anomalías a inyectar:
                - 'nulls': Valores nulos
                - 'outliers': Outliers extremos
                - 'duplicates': Registros duplicados
                - 'type_errors': Errores de tipo
                
        Returns:
            pd.DataFrame con anomalías inyectadas
        """
        if anomaly_types is None:
            anomaly_types = ['nulls', 'outliers', 'duplicates']
        
        df_anomalous = df.copy()
        num_anomalies = int(len(df) * anomaly_rate)
        
        logger.info(f"Injecting {num_anomalies} anomalies ({anomaly_rate*100:.1f}%)")
        
        if 'nulls' in anomaly_types:
            df_anomalous = self._inject_nulls(df_anomalous, num_anomalies // len(anomaly_types))
        
        if 'outliers' in anomaly_types:
            df_anomalous = self._inject_outliers(df_anomalous, num_anomalies // len(anomaly_types))
        
        if 'duplicates' in anomaly_types:
            df_anomalous = self._inject_duplicates(df_anomalous, num_anomalies // len(anomaly_types))
        
        logger.info(f"Anomalies injected successfully")
        
        return df_anomalous
    
    def _inject_nulls(self, df: pd.DataFrame, count: int) -> pd.DataFrame:
        """Inyectar valores nulos aleatorios"""
        for _ in range(count):
            row_idx = np.random.randint(0, len(df))
            col_idx = np.random.randint(0, len(df.columns))
            df.iloc[row_idx, col_idx] = None
        return df
    
    def _inject_outliers(self, df: pd.DataFrame, count: int) -> pd.DataFrame:
        """Inyectar outliers extremos en columnas numéricas"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) == 0:
            return df
        
        for _ in range(count):
            row_idx = np.random.randint(0, len(df))
            col = np.random.choice(numeric_cols)
            
            # Generar outlier: 10x el valor máximo
            max_val = df[col].max()
            df.at[row_idx, col] = max_val * 10
        
        return df
    
    def _inject_duplicates(self, df: pd.DataFrame, count: int) -> pd.DataFrame:
        """Inyectar registros duplicados"""
        indices_to_duplicate = np.random.choice(df.index, count, replace=False)
        duplicates = df.loc[indices_to_duplicate].copy()
        df = pd.concat([df, duplicates], ignore_index=True)
        return df
    
    def generate_customer_data(self, num_customers: int = 1000) -> pd.DataFrame:
        """
        Generar dataset de clientes realista.
        
        Args:
            num_customers: Número de clientes a generar
            
        Returns:
            pd.DataFrame con datos de clientes
        """
        schema = {
            'customer_id': 'uuid',
            'name': 'name',
            'email': 'email',
            'phone': 'phone',
            'address': 'address',
            'registration_date': 'date',
            'is_active': 'bool',
            'lifetime_value': 'amount'
        }
        
        return self.generate(schema, num_customers)
    
    def generate_transaction_data(self, num_transactions: int = 10000) -> pd.DataFrame:
        """
        Generar dataset de transacciones realista.
        
        Args:
            num_transactions: Número de transacciones a generar
            
        Returns:
            pd.DataFrame con datos de transacciones
        """
        schema = {
            'transaction_id': 'uuid',
            'customer_id': 'uuid',
            'timestamp': 'datetime',
            'amount': 'amount',
            'category': 'category',
            'status': 'category'
        }
        
        df = self.generate(schema, num_transactions)
        
        # Ajustar categorías para que sean más realistas
        df['category'] = np.random.choice(
            ['Electronics', 'Clothing', 'Food', 'Books', 'Other'],
            num_transactions
        )
        df['status'] = np.random.choice(
            ['completed', 'pending', 'cancelled'],
            num_transactions,
            p=[0.85, 0.10, 0.05]  # 85% completed, 10% pending, 5% cancelled
        )
        
        return df


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