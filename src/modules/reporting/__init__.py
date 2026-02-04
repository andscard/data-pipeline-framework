"""
Reporting Module - Generación de reportes visuales.

Este módulo genera reportes HTML profesionales combinando:
- Monitoring: Panel de salud operativa (métricas en tiempo real)
- Auditing: Trazabilidad técnica y legal (histórico completo)
"""

from .html_generator import HTMLReportGenerator

__all__ = ['HTMLReportGenerator']
