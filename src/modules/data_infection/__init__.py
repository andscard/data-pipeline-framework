# src/modules/data_infection/__init__.py
"""
Módulo de Infección de Datos - Data Poisoning Module

Este módulo se ejecuta ANTES del pipeline principal para inyectar
vulnerabilidades y problemas en los datos basados en OWASP Top 10.

Uso:
    from src.modules.data_infection import DataInfector
    
    infector = DataInfector.from_yaml('config/infection_config.yml')
    infected_data = infector.infect('data/clean_data.csv')
    infector.save(infected_data, 'data/output/infected_data.csv')
"""

from .infector import DataInfector, InfectionConfig
from .attack_types import (
    DataPoisoningAttack,
    SchemaManipulationAttack,
    InjectionAttack,
    MissingDataAttack,
    OutlierAttack,
    DuplicateAttack,
    DataLeakageAttack,
    InconsistencyAttack,
    FormatCorruptionAttack,
    TimingAttack
)

__all__ = [
    'DataInfector',
    'InfectionConfig',
    'DataPoisoningAttack',
    'SchemaManipulationAttack',
    'InjectionAttack',
    'MissingDataAttack',
    'OutlierAttack',
    'DuplicateAttack',
    'DataLeakageAttack',
    'InconsistencyAttack',
    'FormatCorruptionAttack',
    'TimingAttack'
]
