# src/modules/ingestion/synthetic_generator.py
"""
Generador de datos sintéticos para pruebas y simulaciones
"""

import pandas as pd
import numpy as np
from faker import Faker
from pathlib import Path
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    """
    Generador de datos sintéticos realistas usando Faker.
    
    Genera datasets predefinidos de:
    - Datos de clientes (generate_customer_data)
    - Datos de transacciones (generate_transaction_data)
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
    
    def generate_customer_data(self, num_customers: int = 1000) -> pd.DataFrame:
        """
        Generar dataset de clientes completo y realista con datos LIMPIOS.
        
        Los datos generados son normales y válidos. Para probar el sistema de seguridad,
        usa el módulo de infección que inyecta vulnerabilidades en los datos.
        
        Args:
            num_customers: Número de clientes a generar
            
        Returns:
            pd.DataFrame con datos de clientes (14 columnas):
            - customer_id, name, email, phone, address
            - registration_date, last_login, account_status, lifetime_value
            - user_comment, website, ip_address, credit_card_last4, age, postal_code
        """
        # Generar user_comment (campo de texto libre para comentarios de usuarios)
        user_comments = []
        for i in range(num_customers):
            if i % 5 == 0:  # 20% sin comentario
                user_comments.append(None)
            else:
                user_comments.append(self.fake.sentence(nb_words=np.random.randint(5, 15)))
        
        # Generar website (URLs válidas HTTPS)
        websites = []
        for i in range(num_customers):
            if i % 3 == 0:  # 33% sin website
                websites.append(None)
            else:
                websites.append(f"https://{self.fake.domain_name()}")
        
        # Generar ip_address (solo IPs públicas válidas)
        ip_addresses = []
        for i in range(num_customers):
            if i % 4 == 0:  # 25% sin IP
                ip_addresses.append(None)
            else:
                ip_addresses.append(self.fake.ipv4_public())
        
        # Generar credit_card_last4 (solo últimos 4 dígitos - CUMPLE PCI-DSS)
        credit_card_last4 = []
        for i in range(num_customers):
            if i % 3 == 0:  # 33% sin tarjeta
                credit_card_last4.append(None)
            else:
                credit_card_last4.append(str(np.random.randint(1000, 9999)))
        
        # Generar age (rango normal 18-80 años)
        ages = np.random.randint(18, 80, num_customers).tolist()
        
        # Generar postal_code (formato español)
        postal_codes = []
        for i in range(num_customers):
            if i % 4 == 0:  # 25% sin código postal
                postal_codes.append(None)
            else:
                postal_codes.append(f"{np.random.randint(10000, 52999)}")
        
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
            'lifetime_value': np.random.uniform(100, 10000, num_customers).round(2),
            # NUEVAS COLUMNAS PARA PROBAR SEGURIDAD
            'user_comment': user_comments,
            'website': websites,
            'ip_address': ip_addresses,
            'credit_card_last4': credit_card_last4,
            'age': ages,
            'postal_code': postal_codes
        })
        
        return df
    
    def generate_transaction_data(self, num_transactions: int = 10000) -> pd.DataFrame:
        """
        Generar dataset de transacciones completo y realista con datos LIMPIOS.
        
        Los datos generados son normales y válidos. Para probar el sistema de seguridad,
        usa el módulo de infección que inyecta vulnerabilidades en los datos.
        
        Args:
            num_transactions: Número de transacciones a generar
            
        Returns:
            pd.DataFrame con datos de transacciones (10 columnas):
            - transaction_id, customer_id, timestamp, amount, category, status
            - description, merchant_name, payment_method, ip_address
        """
        # Generar description (descripciones normales de transacciones)
        descriptions = []
        description_templates = [
            "Payment for",
            "Purchase of",
            "Order",
            "Subscription to",
            "Refund for",
            "Service fee for"
        ]
        for i in range(num_transactions):
            template = np.random.choice(description_templates)
            descriptions.append(f"{template} {self.fake.bs().title()}")
        
        # Generar merchant_name (nombres normales de comercios)
        normal_merchants = [
            "Amazon", "eBay", "Walmart", "Target", "Best Buy",
            "Home Depot", "Costco", "Apple Store", "Nike", "Zara",
            "Starbucks", "McDonald's", "Subway", "Pizza Hut", "KFC",
            "IKEA", "H&M", "Gap", "Macy's", "Nordstrom"
        ]
        merchant_names = np.random.choice(normal_merchants, num_transactions).tolist()
        
        # Generar payment_method
        payment_methods = np.random.choice(
            ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto', 'cash'],
            num_transactions,
            p=[0.40, 0.25, 0.15, 0.10, 0.05, 0.05]
        )
        
        # Generar ip_address (solo IPs públicas válidas)
        ip_addresses = [self.fake.ipv4_public() for _ in range(num_transactions)]
        
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
            ),
            # NUEVAS COLUMNAS PARA PROBAR SEGURIDAD
            'description': descriptions,
            'merchant_name': merchant_names,
            'payment_method': payment_methods,
            'ip_address': ip_addresses
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