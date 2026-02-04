# src/modules/transformation/__init__.py
"""
Módulo de Transformación y Enriquecimiento de Datos
RF-07
"""

from .transformer import DataTransformer
from .operations import TransformOperation, TransformResult

__all__ = [
    'DataTransformer',
    'TransformOperation',
    'TransformResult'
]
