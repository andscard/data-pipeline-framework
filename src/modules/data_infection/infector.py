# src/modules/data_infection/infector.py
"""
Motor Principal de Infección de Datos

Orquesta la aplicación de múltiples ataques basados en configuración YAML.
"""

import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import logging

from .attack_types import ATTACK_REGISTRY

logger = logging.getLogger(__name__)


@dataclass
class InfectionConfig:
    """Configuración de infección desde YAML"""
    
    input_path: str
    output_path: str
    attacks: List[Dict[str, Any]]
    global_rate: Optional[float] = None
    random_seed: Optional[int] = None
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'InfectionConfig':
        """Cargar configuración desde archivo YAML"""
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return cls(
            input_path=config['input']['path'],
            output_path=config['output']['path'],
            attacks=config.get('attacks', []),
            global_rate=config.get('global_rate'),
            random_seed=config.get('random_seed')
        )


class DataInfector:
    """
    Motor de Infección de Datos
    
    Aplica múltiples tipos de ataques a datos limpios según configuración.
    
    Ejemplo:
        >>> infector = DataInfector.from_yaml('infection_config.yml')
        >>> infected_df = infector.run()
        >>> print(infected_df.shape)
    """
    
    def __init__(self, config: InfectionConfig):
        self.config = config
        self.df_original = None
        self.df_infected = None
        self.infection_report = []
        
        if config.random_seed:
            import numpy as np
            np.random.seed(config.random_seed)
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'DataInfector':
        """
        Crear infector desde archivo YAML
        
        Args:
            yaml_path: Ruta al archivo de configuración YAML
        """
        config = InfectionConfig.from_yaml(yaml_path)
        return cls(config)
    
    def load_data(self) -> pd.DataFrame:
        """Cargar datos originales"""
        input_path = Path(self.config.input_path)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Detectar formato por extensión
        if input_path.suffix == '.csv':
            df = pd.read_csv(input_path)
        elif input_path.suffix == '.xlsx':
            df = pd.read_excel(input_path)
        elif input_path.suffix == '.parquet':
            df = pd.read_parquet(input_path)
        elif input_path.suffix == '.txt':
            df = pd.read_csv(input_path, sep='|')
        else:
            raise ValueError(f"Unsupported file format: {input_path.suffix}")
        
        logger.info(f"Loaded data: {len(df)} rows, {len(df.columns)} columns")
        self.df_original = df.copy()
        return df
    
    def apply_attacks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplicar todos los ataques configurados
        
        Args:
            df: DataFrame a infectar
        
        Returns:
            DataFrame infectado
        """
        df_infected = df.copy()
        
        for attack_config in self.config.attacks:
            attack_type = attack_config.get('type')
            
            if attack_type not in ATTACK_REGISTRY:
                logger.warning(f"Unknown attack type: {attack_type}, skipping...")
                continue
            
            # Obtener configuración del ataque
            columns = attack_config.get('columns', [])
            rate = attack_config.get('rate', self.config.global_rate or 0.05)
            
            # Soporte para "random" columns
            if columns == ['random'] or columns == 'random':
                num_cols = attack_config.get('num_random_columns', 1)
                columns = list(df_infected.columns[:num_cols])
            
            # Crear instancia del ataque
            attack_class = ATTACK_REGISTRY[attack_type]
            attack = attack_class(config=attack_config)
            
            # Aplicar ataque
            try:
                df_infected = attack.apply(df_infected, columns, rate)
                
                self.infection_report.append({
                    'attack_type': attack_type,
                    'columns': columns,
                    'rate': rate,
                    'status': 'success'
                })
            except Exception as e:
                logger.error(f"Failed to apply {attack_type}: {e}")
                self.infection_report.append({
                    'attack_type': attack_type,
                    'columns': columns,
                    'rate': rate,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return df_infected
    
    def save_infected_data(self, df: pd.DataFrame) -> Path:
        """
        Guardar datos infectados
        
        Args:
            df: DataFrame infectado
        
        Returns:
            Ruta donde se guardó
        """
        output_path = Path(self.config.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Guardar según extensión
        if output_path.suffix == '.csv':
            df.to_csv(output_path, index=False)
        elif output_path.suffix == '.xlsx':
            df.to_excel(output_path, index=False)
        elif output_path.suffix == '.parquet':
            df.to_parquet(output_path, index=False)
        elif output_path.suffix == '.txt':
            df.to_csv(output_path, index=False, sep='|')
        else:
            # Default a CSV
            df.to_csv(output_path, index=False)
        
        logger.info(f"Saved infected data: {output_path}")
        return output_path
    
    def generate_report(self) -> Dict[str, Any]:
        """
        Generar reporte de infección
        
        Returns:
            Diccionario con estadísticas de infección
        """
        return {
            'original_rows': len(self.df_original) if self.df_original is not None else 0,
            'infected_rows': len(self.df_infected) if self.df_infected is not None else 0,
            'original_columns': len(self.df_original.columns) if self.df_original is not None else 0,
            'infected_columns': len(self.df_infected.columns) if self.df_infected is not None else 0,
            'attacks_applied': len(self.infection_report),
            'attacks_successful': len([r for r in self.infection_report if r['status'] == 'success']),
            'attacks_failed': len([r for r in self.infection_report if r['status'] == 'failed']),
            'details': self.infection_report
        }
    
    def run(self) -> pd.DataFrame:
        """
        Ejecutar todo el proceso de infección
        
        Returns:
            DataFrame infectado
        """
        logger.info("Starting data infection process...")
        
        # 1. Cargar datos
        df = self.load_data()
        
        # 2. Aplicar ataques
        self.df_infected = self.apply_attacks(df)
        
        # 3. Guardar resultados
        self.save_infected_data(self.df_infected)
        
        # 4. Generar reporte
        report = self.generate_report()
        logger.info(f"Infection complete: {report['attacks_successful']}/{report['attacks_applied']} attacks applied")
        
        # Guardar reporte
        report_path = Path(self.config.output_path).parent / 'infection_report.json'
        import json
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Report saved: {report_path}")
        
        return self.df_infected


def infect_data_from_yaml(yaml_path: str) -> pd.DataFrame:
    """
    Función helper para infectar datos desde YAML
    
    Args:
        yaml_path: Ruta al archivo de configuración
    
    Returns:
        DataFrame infectado
    """
    infector = DataInfector.from_yaml(yaml_path)
    return infector.run()
