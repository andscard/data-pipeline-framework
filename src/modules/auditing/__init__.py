"""
Módulo de auditoría para registro en PostgreSQL.
Registra pipelines, ejecuciones y resultados de validación.
"""

from .audit_manager import AuditManager

__all__ = ['AuditManager']
