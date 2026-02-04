"""
Monitoring Module - Métricas operativas en tiempo real.

Este módulo captura métricas de salud del pipeline durante la ejecución.
NO persiste en base de datos, vive en memoria (RAM) durante la ejecución.

Diferencia con Auditing:
- Monitoring: Estado actual, salud operativa, tiempo real
- Auditing: Trazabilidad histórica, evidencia legal, persistencia
"""

from .collector import MonitoringCollector, StageMetrics, ExecutionMetrics

__all__ = [
    'MonitoringCollector',
    'StageMetrics',
    'ExecutionMetrics'
]
