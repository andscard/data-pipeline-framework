# src/modules/data_infection/infector.py
"""
Motor Principal de Infección de Datos

Orquesta la aplicación de múltiples ataques basados en configuración YAML.
"""

import pandas as pd
import yaml
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
import logging

from .attack_types import ATTACK_REGISTRY

logger = logging.getLogger(__name__)


@dataclass
class DatasetConfig:
    """Configuración para un dataset individual"""
    name: str
    input_path: str
    output_csv: Optional[str] = None
    output_postgres_table: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    attacks: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class OutputTargetsConfig:
    """Configuración de destinos de salida"""
    csv_enabled: bool = True
    csv_path: str = "data/output/"
    postgres_enabled: bool = False
    postgres_schema: Optional[str] = None
    postgres_connection: Optional[Dict[str, str]] = None
    postgres_if_exists: str = "replace"
    postgres_create_schema: bool = True


@dataclass
class InfectionConfig:
    """Configuración de infección desde YAML"""
    
    datasets: List[DatasetConfig]
    output_targets: OutputTargetsConfig
    global_rate: Optional[float] = None
    random_seed: Optional[int] = None
    mode: str = "mixed"
    keep_clean_marker: bool = True
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'InfectionConfig':
        """Cargar configuración desde archivo YAML"""
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Parse datasets
        datasets = []
        for dataset_name, dataset_config in config.get('datasets', {}).items():
            datasets.append(DatasetConfig(
                name=dataset_name,
                input_path=dataset_config['input'],
                output_csv=dataset_config.get('output', {}).get('csv_file'),
                output_postgres_table=dataset_config.get('output', {}).get('postgres_table'),
                columns=dataset_config.get('columns', []),
                attacks=dataset_config.get('attacks', [])
            ))
        
        # Parse output targets
        output_config = config.get('output_targets', {})
        csv_config = output_config.get('csv', {})
        postgres_config = output_config.get('postgres', {})
        
        # Expand environment variables in PostgreSQL connection
        postgres_connection = None
        if postgres_config.get('connection'):
            postgres_connection = {
                k: os.path.expandvars(v) if isinstance(v, str) else v
                for k, v in postgres_config['connection'].items()
            }
        
        output_targets = OutputTargetsConfig(
            csv_enabled=csv_config.get('enabled', True),
            csv_path=csv_config.get('path', 'data/output/'),
            postgres_enabled=postgres_config.get('enabled', False),
            postgres_schema=postgres_config.get('schema'),
            postgres_connection=postgres_connection,
            postgres_if_exists=postgres_config.get('if_exists', 'replace'),
            postgres_create_schema=postgres_config.get('create_schema_if_not_exists', True)
        )
        
        # Parse global config
        global_config = config.get('global', {})
        
        return cls(
            datasets=datasets,
            output_targets=output_targets,
            global_rate=global_config.get('default_infection_rate'),
            random_seed=config.get('random_seed'),
            mode=global_config.get('mode', 'mixed'),
            keep_clean_marker=global_config.get('keep_clean_marker', True)
        )


class DataInfector:
    """
    Motor de Infección de Datos
    
    Aplica múltiples tipos de ataques a datos limpios según configuración.
    Soporta múltiples datasets y output a CSV y PostgreSQL.
    
    Ejemplo:
        >>> infector = DataInfector.from_yaml('infection_config.yml')
        >>> results = infector.run()
        >>> for dataset_name, df in results.items():
        >>>     print(f"{dataset_name}: {df.shape}")
    """
    
    def __init__(self, config: InfectionConfig):
        self.config = config
        self.results = {}  # Dict[dataset_name, infected_df]
        self.reports = {}  # Dict[dataset_name, report]
        
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
    
    def load_data(self, input_path: str) -> pd.DataFrame:
        """
        Cargar datos desde archivo
        
        Args:
            input_path: Ruta al archivo de entrada
        
        Returns:
            DataFrame con los datos cargados
        """
        path = Path(input_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        
        # Detectar formato por extensión
        if path.suffix == '.csv':
            df = pd.read_csv(path)
        elif path.suffix == '.xlsx':
            df = pd.read_excel(path)
        elif path.suffix == '.parquet':
            df = pd.read_parquet(path)
        elif path.suffix == '.txt':
            df = pd.read_csv(path, sep='|')
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")
        
        logger.info(f"Loaded {path.name}: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def apply_attacks(self, df: pd.DataFrame, attacks: List[Dict[str, Any]], 
                     dataset_name: str) -> tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Aplicar ataques a un dataset
        
        Args:
            df: DataFrame a infectar
            attacks: Lista de configuraciones de ataques
            dataset_name: Nombre del dataset (para logging)
        
        Returns:
            Tupla (DataFrame infectado, lista de reportes)
        """
        df_infected = df.copy()
        infection_report = []
        
        # Agregar columna de marcador si está configurado
        if self.config.keep_clean_marker:
            df_infected['_is_infected'] = False
        
        for attack_config in attacks:
            attack_type = attack_config.get('type')
            
            if attack_type not in ATTACK_REGISTRY:
                logger.warning(f"[{dataset_name}] Unknown attack type: {attack_type}, skipping...")
                continue
            
            # Obtener configuración del ataque
            columns = attack_config.get('target_columns', attack_config.get('columns', []))
            rate = attack_config.get('rate', self.config.global_rate or 0.05)
            
            # Soporte para "random" columns
            if columns == ['random'] or columns == 'random':
                num_cols = attack_config.get('num_random_columns', 1)
                columns = list(df_infected.columns[:num_cols])
            
            # Validar que las columnas existen (si se especificaron)
            valid_columns = [col for col in columns if col in df_infected.columns] if columns else []
            
            # Algunos ataques no requieren columnas específicas (duplicates, cross_field_violation, etc.)
            attacks_without_columns = ['duplicates', 'cross_field_violation', 'temporal_anomaly']
            if not valid_columns and attack_type not in attacks_without_columns:
                logger.warning(f"[{dataset_name}] No valid columns for {attack_type}, skipping...")
                continue
            
            # Crear instancia del ataque
            attack_class = ATTACK_REGISTRY[attack_type]
            attack = attack_class(config=attack_config)
            
            # Aplicar ataque
            try:
                df_infected = attack.apply(df_infected, valid_columns, rate)
                
                attack_name = attack_config.get('name', attack_type)
                cols_desc = valid_columns if valid_columns else 'all columns'
                logger.info(f"[{dataset_name}] Applied {attack_name} to {cols_desc} (rate={rate})")
                
                infection_report.append({
                    'attack_name': attack_name,
                    'attack_type': attack_type,
                    'columns': valid_columns if valid_columns else [],
                    'rate': rate,
                    'status': 'success'
                })
            except Exception as e:
                logger.error(f"[{dataset_name}] Failed to apply {attack_type}: {e}")
                infection_report.append({
                    'attack_name': attack_config.get('name', attack_type),
                    'attack_type': attack_type,
                    'columns': valid_columns if valid_columns else [],
                    'rate': rate,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return df_infected, infection_report
    
    def save_infected_data(self, df: pd.DataFrame, dataset_config: DatasetConfig) -> Dict[str, str]:
        """
        Guardar datos infectados a CSV y/o PostgreSQL
        
        Args:
            df: DataFrame infectado
            dataset_config: Configuración del dataset
        
        Returns:
            Diccionario con rutas/tablas donde se guardó
        """
        saved_locations = {}
        
        # Guardar a CSV si está habilitado
        if self.config.output_targets.csv_enabled and dataset_config.output_csv:
            csv_dir = Path(self.config.output_targets.csv_path)
            csv_dir.mkdir(parents=True, exist_ok=True)
            
            output_path = csv_dir / dataset_config.output_csv
            df.to_csv(output_path, index=False)
            
            logger.info(f"[{dataset_config.name}] Saved to CSV: {output_path}")
            saved_locations['csv'] = str(output_path)
        
        # Guardar a PostgreSQL si está habilitado
        if self.config.output_targets.postgres_enabled and dataset_config.output_postgres_table:
            try:
                from sqlalchemy import create_engine, text
                
                conn_info = self.config.output_targets.postgres_connection
                if not conn_info:
                    logger.warning(f"[{dataset_config.name}] PostgreSQL enabled but no connection info")
                    return saved_locations
                
                # Construir connection string
                conn_string = (
                    f"postgresql://{conn_info['user']}:{conn_info['password']}"
                    f"@{conn_info['host']}:{conn_info['port']}/{conn_info['database']}"
                )
                
                engine = create_engine(conn_string)
                
                # Crear schema si está configurado y no existe
                schema = self.config.output_targets.postgres_schema
                if schema and self.config.output_targets.postgres_create_schema:
                    with engine.begin() as conn:
                        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    logger.info(f"[{dataset_config.name}] Ensured schema exists: {schema}")
                
                # Guardar a PostgreSQL
                table_name = dataset_config.output_postgres_table
                if_exists = self.config.output_targets.postgres_if_exists
                
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists=if_exists,
                    index=False
                )
                
                full_table = f"{schema}.{table_name}" if schema else table_name
                logger.info(f"[{dataset_config.name}] Saved to PostgreSQL: {full_table}")
                saved_locations['postgres'] = full_table
                
            except ImportError:
                logger.error(f"[{dataset_config.name}] SQLAlchemy not installed, cannot save to PostgreSQL")
            except Exception as e:
                logger.error(f"[{dataset_config.name}] Failed to save to PostgreSQL: {e}")
        
        return saved_locations
    
    def generate_report(self, dataset_name: str, df_original: pd.DataFrame, 
                       df_infected: pd.DataFrame, infection_report: List[Dict[str, Any]],
                       saved_locations: Dict[str, str]) -> Dict[str, Any]:
        """
        Generar reporte de infección para un dataset
        
        Args:
            dataset_name: Nombre del dataset
            df_original: DataFrame original
            df_infected: DataFrame infectado
            infection_report: Lista de reportes de ataques
            saved_locations: Diccionario con ubicaciones donde se guardó
        
        Returns:
            Diccionario con estadísticas de infección
        """
        return {
            'dataset': dataset_name,
            'original_rows': len(df_original),
            'infected_rows': len(df_infected),
            'original_columns': len(df_original.columns),
            'infected_columns': len(df_infected.columns),
            'attacks_applied': len(infection_report),
            'attacks_successful': len([r for r in infection_report if r['status'] == 'success']),
            'attacks_failed': len([r for r in infection_report if r['status'] == 'failed']),
            'saved_to': saved_locations,
            'details': infection_report
        }
    
    def run(self) -> Dict[str, pd.DataFrame]:
        """
        Ejecutar todo el proceso de infección para todos los datasets
        
        Returns:
            Diccionario {dataset_name: DataFrame infectado}
        """
        logger.info("=" * 80)
        logger.info("Starting data infection process...")
        logger.info(f"Datasets to process: {len(self.config.datasets)}")
        logger.info(f"Mode: {self.config.mode}")
        logger.info("=" * 80)
        
        for dataset_config in self.config.datasets:
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing dataset: {dataset_config.name}")
            logger.info(f"{'='*80}")
            
            try:
                # 1. Cargar datos
                df_original = self.load_data(dataset_config.input_path)
                
                # 2. Aplicar ataques
                df_infected, infection_report = self.apply_attacks(
                    df_original, 
                    dataset_config.attacks,
                    dataset_config.name
                )
                
                # 3. Guardar resultados
                saved_locations = self.save_infected_data(df_infected, dataset_config)
                
                # 4. Generar reporte
                report = self.generate_report(
                    dataset_config.name,
                    df_original,
                    df_infected,
                    infection_report,
                    saved_locations
                )
                
                # Guardar resultados
                self.results[dataset_config.name] = df_infected
                self.reports[dataset_config.name] = report
                
                logger.info(f"\n[{dataset_config.name}] Summary:")
                logger.info(f"  - Attacks applied: {report['attacks_successful']}/{report['attacks_applied']}")
                logger.info(f"  - Rows: {report['original_rows']} -> {report['infected_rows']}")
                logger.info(f"  - Saved to: {', '.join(saved_locations.keys())}")
                
            except Exception as e:
                logger.error(f"[{dataset_config.name}] Failed to process: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Guardar reporte consolidado
        if self.config.output_targets.csv_enabled:
            report_dir = Path(self.config.output_targets.csv_path)
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / 'infection_report.json'
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'config': {
                        'mode': self.config.mode,
                        'random_seed': self.config.random_seed,
                        'global_rate': self.config.global_rate,
                        'keep_clean_marker': self.config.keep_clean_marker
                    },
                    'datasets': self.reports
                }, f, indent=2, default=str)
            
            logger.info(f"\n{'='*80}")
            logger.info(f"Infection complete! Report saved: {report_path}")
            logger.info(f"{'='*80}")
        
        return self.results


def infect_data_from_yaml(yaml_path: str) -> Dict[str, pd.DataFrame]:
    """
    Función helper para infectar datos desde YAML
    
    Args:
        yaml_path: Ruta al archivo de configuración
    
    Returns:
        Diccionario {dataset_name: DataFrame infectado}
    """
    infector = DataInfector.from_yaml(yaml_path)
    return infector.run()
