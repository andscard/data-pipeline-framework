"""
HTMLReportGenerator - Generador de reportes HTML profesionales.

Combina métricas de Monitoring (salud operativa) y Auditing (trazabilidad)
en un reporte visual estilo dashboard con CSS embebido.
"""

from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class HTMLReportGenerator:
    """
    Generador de reportes HTML profesionales para ejecuciones de pipeline.
    
    El reporte incluye:
    1. Executive Summary: Panel de salud general (verde/amarillo/rojo)
    2. Stage Health: Barras de progreso por etapa
    3. Quality Metrics: Métricas de validación con visualización
    4. Audit Trail: Tabla detallada de trazabilidad
    """
    
    def __init__(self):
        """Inicializar generador"""
        pass
    
    def generate_report(
        self,
        monitoring_summary: Dict[str, Any],
        audit_data: Optional[Dict[str, Any]] = None,
        output_path: Optional[Path] = None
    ) -> Path:
        """
        Generar reporte HTML completo.
        
        Args:
            monitoring_summary: Resumen de MonitoringCollector
            audit_data: Datos adicionales de AuditManager (opcional)
            output_path: Ruta donde guardar el reporte
        
        Returns:
            Path al archivo HTML generado
        """
        # Determinar ruta de salida
        if not output_path:
            execution_id = monitoring_summary.get('execution_id', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(f"reports/execution_{execution_id[:8]}_{timestamp}.html")
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Generar HTML
        html_content = self._build_html(monitoring_summary, audit_data)
        
        # Guardar archivo
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"✓ Reporte HTML generado: {output_path}")
        return output_path
    
    def _build_html(
        self,
        monitoring: Dict[str, Any],
        audit: Optional[Dict[str, Any]]
    ) -> str:
        """Construir HTML completo del reporte"""
        
        css = self._generate_css()
        header = self._generate_header(monitoring)
        executive_summary = self._generate_executive_summary(monitoring)
        security_issues = self._generate_security_issues(monitoring)  # NEW: Security section
        stage_health = self._generate_stage_health(monitoring)
        quality_metrics = self._generate_quality_metrics(monitoring, audit)  # Pass audit data
        issues_section = self._generate_issues_section(monitoring)
        audit_trail = self._generate_audit_trail(monitoring, audit)
        
        html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Execution Report - {monitoring.get('execution_id', 'N/A')[:8]}</title>
    <style>
{css}
    </style>
</head>
<body>
    <div class="container">
{header}
{executive_summary}
{security_issues}
{stage_health}
{quality_metrics}
{issues_section}
{audit_trail}
        <footer>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data Pipeline Framework v2.0</p>
        </footer>
    </div>
</body>
</html>"""
        
        return html
    
    def _generate_css(self) -> str:
        """Generar CSS embebido profesional y empresarial"""
        return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f5f7fa;
            padding: 30px;
            color: #2c3e50;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 4px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        /* Header */
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .header .execution-id {
            font-size: 1.1em;
            opacity: 0.9;
            font-family: 'Courier New', monospace;
        }
        
        .header .timestamp {
            margin-top: 5px;
            font-size: 0.9em;
            opacity: 0.8;
        }
        
        /* Section */
        .section {
            padding: 30px;
            border-bottom: 1px solid #e0e0e0;
        }
        
        .section:last-of-type {
            border-bottom: none;
        }
        
        .section-title {
            font-size: 1.8em;
            margin-bottom: 20px;
            color: #667eea;
            display: flex;
            align-items: center;
        }
        
        .section-title::before {
            content: '';
            width: 4px;
            height: 30px;
            background: #667eea;
            margin-right: 15px;
            border-radius: 2px;
        }
        
        /* Executive Summary */
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        
        .card {
            background: #f8f9fa;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        
        .card-title {
            font-size: 0.9em;
            color: #666;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .card-value {
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 5px;
        }
        
        .card-subtitle {
            font-size: 0.9em;
            color: #999;
        }
        
        /* Status badges */
        .status-badge {
            display: inline-block;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 1.1em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .status-healthy {
            background: #4caf50;
            color: white;
        }
        
        .status-warning {
            background: #ff9800;
            color: white;
        }
        
        .status-critical {
            background: #f44336;
            color: white;
        }
        
        .status-failed {
            background: #d32f2f;
            color: white;
        }
        
        /* Stage Health */
        .stage-list {
            margin-top: 20px;
        }
        
        .stage-item {
            background: #f8f9fa;
            padding: 20px;
            margin-bottom: 15px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }
        
        .stage-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .stage-name {
            font-size: 1.3em;
            font-weight: bold;
            color: #333;
        }
        
        .stage-status {
            padding: 5px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
        }
        
        .stage-status.completed {
            background: #e8f5e9;
            color: #2e7d32;
        }
        
        .stage-status.failed {
            background: #ffebee;
            color: #c62828;
        }
        
        .stage-metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 15px;
        }
        
        .metric {
            display: flex;
            flex-direction: column;
        }
        
        .metric-label {
            font-size: 0.85em;
            color: #666;
            margin-bottom: 5px;
        }
        
        .metric-value {
            font-size: 1.3em;
            font-weight: bold;
            color: #333;
        }
        
        /* Progress bar */
        .progress-bar-container {
            background: #e0e0e0;
            height: 30px;
            border-radius: 15px;
            overflow: hidden;
            position: relative;
        }
        
        .progress-bar {
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            font-size: 0.9em;
            transition: width 0.3s ease;
        }
        
        .progress-bar.high {
            background: linear-gradient(90deg, #4caf50, #66bb6a);
        }
        
        .progress-bar.medium {
            background: linear-gradient(90deg, #ff9800, #ffa726);
        }
        
        .progress-bar.low {
            background: linear-gradient(90deg, #f44336, #ef5350);
        }
        
        /* Quality Metrics */
        .quality-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        
        .quality-card {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .quality-title {
            font-size: 1.1em;
            font-weight: bold;
            margin-bottom: 15px;
            color: #667eea;
        }
        
        .quality-score {
            font-size: 3em;
            font-weight: bold;
            text-align: center;
            margin: 20px 0;
        }
        
        .score-excellent {
            color: #4caf50;
        }
        
        .score-good {
            color: #8bc34a;
        }
        
        .score-fair {
            color: #ff9800;
        }
        
        .score-poor {
            color: #f44336;
        }
        
        /* Issues */
        .issue-list {
            margin-top: 15px;
        }
        
        .issue-item {
            background: #fff3cd;
            border-left: 4px solid #ff9800;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 4px;
        }
        
        .issue-item.error {
            background: #f8d7da;
            border-left-color: #f44336;
        }
        
        .issue-icon {
            display: inline-block;
            margin-right: 10px;
            font-weight: bold;
        }
        
        /* Audit Trail */
        .audit-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background: white;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }
        
        .audit-table thead {
            background: #667eea;
            color: white;
        }
        
        .audit-table th,
        .audit-table td {
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #e0e0e0;
        }
        
        .audit-table th {
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 1px;
        }
        
        .audit-table tbody tr:hover {
            background: #f5f5f5;
        }
        
        .audit-table tbody tr:last-child td {
            border-bottom: none;
        }
        
        /* Footer */
        footer {
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            font-size: 0.9em;
        }
        
        /* Animations */
        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .section {
            animation: fadeIn 0.5s ease;
        }
        
        /* Responsive */
        @media (max-width: 768px) {
            .summary-cards {
                grid-template-columns: 1fr;
            }
            
            .stage-metrics {
                grid-template-columns: 1fr;
            }
            
            .quality-grid {
                grid-template-columns: 1fr;
            }
        }
        
        /* Quality Metrics Detailed Styles */
        .quality-summary {
            margin: 20px 0;
        }
        
        .quality-score-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px;
            border-radius: 12px;
            text-align: center;
            color: white;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .quality-title {
            font-size: 1.1em;
            font-weight: 500;
            opacity: 0.9;
            margin-bottom: 10px;
        }
        
        .quality-score-large {
            font-size: 4em;
            font-weight: bold;
            margin: 10px 0;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .quality-subtitle {
            font-size: 1em;
            opacity: 0.8;
        }
        
        .suites-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        
        .suite-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .suite-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 15px;
        }
        
        .suite-icon {
            font-size: 1.5em;
        }
        
        .suite-name {
            flex: 1;
            font-weight: 600;
            color: #333;
        }
        
        .suite-status {
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
        }
        
        .suite-status.success {
            background: #d4edda;
            color: #155724;
        }
        
        .suite-status.warning {
            background: #fff3cd;
            color: #856404;
        }
        
        .suite-status.error {
            background: #f8d7da;
            color: #721c24;
        }
        
        .suite-metrics {
            display: flex;
            gap: 20px;
            margin: 15px 0;
        }
        
        .suite-metric {
            flex: 1;
            text-align: center;
        }
        
        .suite-metric .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }
        
        .suite-metric .metric-value.success {
            color: #4caf50;
        }
        
        .suite-metric .metric-value.error {
            color: #f44336;
        }
        
        .suite-metric .metric-label {
            font-size: 0.85em;
            color: #666;
            margin-top: 5px;
        }
        
        .failures-section {
            margin-top: 40px;
            padding: 20px;
            background: #fff5f5;
            border-radius: 8px;
            border: 2px solid #ffcdd2;
        }
        
        .failure-group {
            margin-bottom: 30px;
        }
        
        .failures-table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            margin-top: 15px;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .failures-table thead {
            background: linear-gradient(135deg, #d32f2f 0%, #c62828 100%);
            color: white;
        }
        
        .failures-table th {
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 0.9em;
        }
        
        .failures-table td {
            padding: 12px;
            border-bottom: 1px solid #eee;
        }
        
        .failure-row.critical {
            background: #ffebee;
        }
        
        .failure-row.high {
            background: #fff3e0;
        }
        
        .failure-row.medium {
            background: #fffde7;
        }
        
        .failures-table code {
            background: #f5f5f5;
            padding: 4px 8px;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }
        
        .exp-type {
            color: #1976d2;
        }
        
        .column-name {
            color: #6a1b9a;
        }
        
        .pattern {
            color: #d84315;
            max-width: 300px;
            display: inline-block;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        
        .count-badge {
            background: #e3f2fd;
            color: #1976d2;
            padding: 4px 10px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.9em;
        }
        
        .percent-badge {
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: bold;
            font-size: 0.9em;
        }
        
        .percent-badge.critical {
            background: #d32f2f;
            color: white;
        }
        
        .percent-badge.high {
            background: #ff6f00;
            color: white;
        }
        
        .percent-badge.medium {
            background: #fdd835;
            color: #333;
        }
"""
    
    def _generate_header(self, monitoring: Dict[str, Any]) -> str:
        """Generar header del reporte"""
        execution_id = monitoring.get('execution_id', 'N/A')
        pipeline_name = monitoring.get('pipeline_name', 'Unknown Pipeline')
        start_time = monitoring.get('start_time', 'N/A')
        
        return f"""
        <div class="header">
            <h1>🎯 Pipeline Execution Report</h1>
            <div class="execution-id">Pipeline: {pipeline_name}</div>
            <div class="execution-id">Execution ID: {execution_id}</div>
            <div class="timestamp">Started: {start_time}</div>
        </div>
"""
    
    def _generate_executive_summary(self, monitoring: Dict[str, Any]) -> str:
        """Generar resumen ejecutivo con métricas principales"""
        health_status = monitoring.get('health_status', 'unknown')
        duration = monitoring.get('total_duration', 0)
        records = monitoring.get('total_records_processed', 0)
        quality = monitoring.get('overall_quality_score')
        
        # Determinar clase de status
        status_class = {
            'healthy': 'status-healthy',
            'warning': 'status-warning',
            'critical': 'status-critical',
            'failed': 'status-failed'
        }.get(health_status, 'status-warning')
        
        # Ícono de status
        status_icon = {
            'healthy': '✓',
            'warning': '⚠',
            'critical': '⚠',
            'failed': '✗'
        }.get(health_status, '?')
        
        quality_html = ""
        if quality is not None:
            quality_class = self._get_score_class(quality)
            quality_html = f"""
                <div class="card">
                    <div class="card-title">Overall Quality</div>
                    <div class="card-value {quality_class}">{quality:.1f}%</div>
                    <div class="card-subtitle">Validation Score</div>
                </div>
"""
        
        return f"""
        <div class="section">
            <h2 class="section-title">📊 Executive Summary</h2>
            <div class="summary-cards">
                <div class="card">
                    <div class="card-title">Status</div>
                    <div class="card-value">
                        <span class="status-badge {status_class}">{status_icon} {health_status.upper()}</span>
                    </div>
                    <div class="card-subtitle">Pipeline Health</div>
                </div>
                
                <div class="card">
                    <div class="card-title">Duration</div>
                    <div class="card-value">{duration:.1f}s</div>
                    <div class="card-subtitle">Total Execution Time</div>
                </div>
                
                <div class="card">
                    <div class="card-title">Records</div>
                    <div class="card-value">{records:,}</div>
                    <div class="card-subtitle">Processed Successfully</div>
                </div>
                {quality_html}
            </div>
        </div>
"""
    
    def _generate_stage_health(self, monitoring: Dict[str, Any]) -> str:
        """Generar sección de salud por etapa"""
        stages = monitoring.get('stages', {})
        
        if not stages:
            return ""
        
        stages_html = []
        for stage_name, stage_data in stages.items():
            status = stage_data.get('status', 'unknown')
            duration = stage_data.get('duration_seconds', 0)
            records_in = stage_data.get('records_input', 0)
            records_out = stage_data.get('records_output', 0)
            success_rate = stage_data.get('success_rate', 100)
            errors = stage_data.get('errors', [])
            warnings = stage_data.get('warnings', [])
            
            # Status badge
            status_class = 'completed' if status == 'completed' else 'failed'
            status_icon = '✓' if status == 'completed' else '✗'
            
            # Progress bar
            progress_class = self._get_progress_class(success_rate)
            
            # Quality metrics (si es VALIDATION)
            quality_html = ""
            if stage_data.get('quality_score') is not None:
                quality_score = stage_data.get('quality_score', 0)
                validations_passed = stage_data.get('validations_passed', 0)
                validations_failed = stage_data.get('validations_failed', 0)
                quality_html = f"""
                    <div class="metric">
                        <div class="metric-label">Quality Score</div>
                        <div class="metric-value">{quality_score:.1f}%</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Validations</div>
                        <div class="metric-value">{validations_passed}/{validations_passed + validations_failed}</div>
                    </div>
"""
            
            # Errors/warnings
            issues_html = ""
            if errors:
                issues_html += '<div class="issue-list">'
                for error in errors[:3]:  # Limitar a 3
                    issues_html += f'<div class="issue-item error"><span class="issue-icon">✗</span>{error}</div>'
                issues_html += '</div>'
            
            if warnings:
                issues_html += '<div class="issue-list">'
                for warning in warnings[:3]:  # Limitar a 3
                    issues_html += f'<div class="issue-item"><span class="issue-icon">⚠</span>{warning}</div>'
                issues_html += '</div>'
            
            stage_html = f"""
                <div class="stage-item">
                    <div class="stage-header">
                        <div class="stage-name">{stage_name}</div>
                        <div class="stage-status {status_class}">{status_icon} {status}</div>
                    </div>
                    <div class="stage-metrics">
                        <div class="metric">
                            <div class="metric-label">Duration</div>
                            <div class="metric-value">{duration:.2f}s</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">Records In</div>
                            <div class="metric-value">{records_in:,}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">Records Out</div>
                            <div class="metric-value">{records_out:,}</div>
                        </div>
                        {quality_html}
                    </div>
                    <div class="progress-bar-container">
                        <div class="progress-bar {progress_class}" style="width: {success_rate}%">
                            {success_rate:.1f}% Success Rate
                        </div>
                    </div>
                    {issues_html}
                </div>
"""
            stages_html.append(stage_html)
        
        return f"""
        <div class="section">
            <h2 class="section-title">🔍 Stage Health</h2>
            <div class="stage-list">
                {''.join(stages_html)}
            </div>
        </div>
"""
    
    def _generate_security_issues(self, monitoring: Dict[str, Any]) -> str:
        """Generar sección destacada de Security Issues"""
        # Buscar etapa de validación
        stages = monitoring.get('stages', {})
        validation_stage = stages.get('VALIDATION')
        
        if not validation_stage:
            return ""
        
        # Obtener métricas de calidad
        quality_score = validation_stage.get('quality_score', 100)
        validations_passed = validation_stage.get('validations_passed', 0)
        validations_failed = validation_stage.get('validations_failed', 0)
        total_validations = validations_passed + validations_failed
        
        # Si no hay validaciones fallidas o quality > 80%, no mostrar sección
        if validations_failed == 0 or quality_score > 80:
            return ""
        
        # Calcular severidad
        failure_rate = (validations_failed / total_validations * 100) if total_validations > 0 else 0
        
        if failure_rate > 50:
            severity_class = 'critical'
            severity_icon = '🔴'
            severity_label = 'CRITICAL'
            border_color = '#ff0000'
        elif failure_rate > 25:
            severity_class = 'high'
            severity_icon = '🟠'
            severity_label = 'HIGH'
            border_color = '#ff6600'
        else:
            severity_class = 'medium'
            severity_icon = '🟡'
            severity_label = 'MEDIUM'
            border_color = '#ffaa00'
        
        return f"""
        <div class="section" style="border-left: 5px solid {border_color}; background: linear-gradient(135deg, #fff5f5 0%, #ffffff 100%);">
            <div style="display: flex; align-items: center; justify-content: space-between;">
                <h2 class="section-title">{severity_icon} Security Issues Detected</h2>
                <span style="padding: 8px 16px; background: {border_color}; color: white; border-radius: 20px; font-weight: bold; font-size: 0.9em;">
                    {severity_label}
                </span>
            </div>
            
            <div style="display: flex; gap: 30px; margin: 20px 0; padding: 20px; background: rgba(255, 0, 0, 0.05); border-radius: 8px;">
                <div style="text-align: center; flex: 1;">
                    <div style="font-size: 3em; font-weight: bold; color: #ff4444;">{validations_failed}</div>
                    <div style="color: #666; margin-top: 5px;">Vulnerabilities</div>
                </div>
                <div style="text-align: center; flex: 1;">
                    <div style="font-size: 3em; font-weight: bold; color: #ff4444;">{failure_rate:.1f}%</div>
                    <div style="color: #666; margin-top: 5px;">Failure Rate</div>
                </div>
                <div style="text-align: center; flex: 1;">
                    <div style="font-size: 3em; font-weight: bold; color: #ff4444;">{total_validations}</div>
                    <div style="color: #666; margin-top: 5px;">Checks Performed</div>
                </div>
            </div>
            
            <div style="padding: 20px; background: white; border-radius: 8px; border: 1px solid #ffcccc;">
                <h3 style="color: #cc0000; margin-top: 0;">⚠️ Security Validation Failed</h3>
                <p>The data pipeline detected <strong>{validations_failed} security issues</strong> during validation. Quality score: <strong>{quality_score:.1f}%</strong></p>
                
                <p><strong>Potential Threats Detected:</strong></p>
                <ul style="columns: 2; column-gap: 30px;">
                    <li>SQL Injection patterns</li>
                    <li>NoSQL Injection operators</li>
                    <li>Cross-Site Scripting (XSS)</li>
                    <li>Command Injection attempts</li>
                    <li>LDAP Injection patterns</li>
                    <li>XML Injection entities</li>
                    <li>Path Traversal sequences</li>
                    <li>Sensitive Data Leakage</li>
                </ul>
                
                <div style="margin-top: 20px; padding: 15px; background: #fff3cd; border-left: 4px solid #ffc107; border-radius: 4px;">
                    <strong>🔒 Action Required:</strong> Review validation logs and audit trail for detailed vulnerability information. Check the data source for potential security breaches.
                </div>
            </div>
        </div>
"""
    
    def _generate_quality_metrics(self, monitoring: Dict[str, Any], audit_data: Optional[Dict[str, Any]] = None) -> str:
        """Generar sección DETALLADA de métricas de calidad"""
        stages = monitoring.get('stages', {})
        validation_stage = stages.get('VALIDATION')
        
        if not validation_stage or validation_stage.get('quality_score') is None:
            return ""
        
        quality_score = validation_stage.get('quality_score', 0)
        validations_passed = validation_stage.get('validations_passed', 0)
        validations_failed = validation_stage.get('validations_failed', 0)
        total_validations = validations_passed + validations_failed
        
        score_class = self._get_score_class(quality_score)
        
        # Obtener validation_results de audit_data
        validation_results = []
        if audit_data and 'validation_results' in audit_data:
            validation_results = audit_data['validation_results']
        
        # Agrupar por suite
        suites = {}
        for result in validation_results:
            suite_name = result['rule_name']
            if suite_name not in suites:
                suites[suite_name] = {
                    'name': suite_name,
                    'type': result['rule_type'],
                    'passed': 0,
                    'failed': 0,
                    'failed_details': []
                }
            
            if result['passed']:
                suites[suite_name]['passed'] += 1
            else:
                suites[suite_name]['failed'] += result['failed_count']
                suites[suite_name]['failed_details'].extend(result['failure_details'])
        
        # Generar tabla de suites
        suites_html = ""
        for suite_name, suite_data in suites.items():
            total = suite_data['passed'] + suite_data['failed']
            success_rate = (suite_data['passed'] / total * 100) if total > 0 else 0
            
            status_icon = '✅' if success_rate == 100 else '⚠️' if success_rate > 50 else '❌'
            status_class = 'success' if success_rate == 100 else 'warning' if success_rate > 50 else 'error'
            
            # Determinar tipo de suite
            if 'security' in suite_name.lower():
                suite_icon = '🔒'
                suite_color = '#ff6b6b'
            elif 'quality' in suite_name.lower() or 'basic' in suite_name.lower():
                suite_icon = '📊'
                suite_color = '#4dabf7'
            elif 'anomaly' in suite_name.lower():
                suite_icon = '🔍'
                suite_color = '#ffa94d'
            elif 'business' in suite_name.lower():
                suite_icon = '💼'
                suite_color = '#69db7c'
            else:
                suite_icon = '📋'
                suite_color = '#868e96'
            
            suites_html += f"""
            <div class="suite-card" style="border-left: 4px solid {suite_color};">
                <div class="suite-header">
                    <span class="suite-icon">{suite_icon}</span>
                    <span class="suite-name">{suite_name}</span>
                    <span class="suite-status {status_class}">{status_icon} {success_rate:.1f}%</span>
                </div>
                <div class="suite-metrics">
                    <div class="suite-metric">
                        <div class="metric-value success">{suite_data['passed']}</div>
                        <div class="metric-label">Passed</div>
                    </div>
                    <div class="suite-metric">
                        <div class="metric-value error">{suite_data['failed']}</div>
                        <div class="metric-label">Failed</div>
                    </div>
                    <div class="suite-metric">
                        <div class="metric-value">{total}</div>
                        <div class="metric-label">Total</div>
                    </div>
                </div>
                <div class="progress-bar-container">
                    <div class="progress-bar {self._get_progress_class(success_rate)}" style="width: {success_rate}%">
                        {success_rate:.1f}%
                    </div>
                </div>
            </div>
"""
        
        # Generar tabla de fallos detallados
        failures_html = ""
        if validation_results:
            has_failures = any(not r['passed'] for r in validation_results)
            if has_failures:
                failures_html = '<div class="failures-section">'
                failures_html += '<h3 style="margin-bottom: 20px; color: #d32f2f; font-size: 1.3em;">📋 Detailed Validation Failures</h3>'
                
                for result in validation_results:
                    if not result['passed'] and result['failure_details']:
                        suite_name = result['rule_name']
                        failures_html += f'<div class="failure-group"><h4 style="color: #c62828; margin: 20px 0 10px 0;">❌ {suite_name}</h4>'
                        failures_html += '<table class="failures-table">'
                        failures_html += '''
                        <thead>
                            <tr>
                                <th>Expectation Type</th>
                                <th>Column</th>
                                <th>Pattern/Rule</th>
                                <th>Unexpected Count</th>
                                <th>Unexpected %</th>
                            </tr>
                        </thead>
                        <tbody>
'''
                        
                        for detail in result['failure_details']:
                            exp_type = detail.get('expectation_type', 'N/A')
                            kwargs = detail.get('kwargs', {})
                            column = kwargs.get('column', 'N/A')
                            
                            # Extraer regex/pattern
                            pattern = kwargs.get('regex', kwargs.get('value_set', kwargs.get('min_value', 'N/A')))
                            if isinstance(pattern, str) and len(pattern) > 50:
                                pattern = pattern[:47] + '...'
                            
                            unexpected_count = detail.get('unexpected_count', 0) or 0
                            unexpected_percent = detail.get('unexpected_percent') or 0.0
                            element_count = detail.get('element_count', 0) or 0
                            
                            # Asegurar que son números
                            try:
                                unexpected_count = int(unexpected_count)
                                element_count = int(element_count)
                                unexpected_percent = float(unexpected_percent)
                            except (ValueError, TypeError):
                                unexpected_count = 0
                                element_count = 0
                                unexpected_percent = 0.0
                            
                            # Color de severidad
                            severity_class = 'critical' if unexpected_percent > 10 else 'high' if unexpected_percent > 5 else 'medium'
                            
                            # Simplificar nombre de expectation
                            simple_type = exp_type.replace('expect_column_values_to_', '').replace('expect_column_', '').replace('_', ' ').title()
                            
                            failures_html += f'''
                            <tr class="failure-row {severity_class}">
                                <td><code class="exp-type">{simple_type}</code></td>
                                <td><strong class="column-name">{column}</strong></td>
                                <td><code class="pattern">{pattern}</code></td>
                                <td><span class="count-badge">{unexpected_count:,} / {element_count:,}</span></td>
                                <td><span class="percent-badge {severity_class}">{unexpected_percent:.2f}%</span></td>
                            </tr>
'''
                        
                        failures_html += '</tbody></table></div>'
                
                failures_html += '</div>'
        
        return f"""
        <div class="section">
            <h2 class="section-title">📈 Quality Metrics</h2>
            
            <div class="quality-summary">
                <div class="quality-score-card">
                    <div class="quality-title">Overall Quality Score</div>
                    <div class="quality-score-large {score_class}">{quality_score:.1f}%</div>
                    <div class="quality-subtitle">{validations_passed} passed • {validations_failed} failed • {total_validations} total</div>
                    <div class="progress-bar-container" style="margin-top: 15px;">
                        <div class="progress-bar {self._get_progress_class(quality_score)}" style="width: {quality_score}%">
                            {quality_score:.1f}%
                        </div>
                    </div>
                </div>
            </div>
            
            <h3 style="margin: 30px 0 20px 0; color: #495057; font-size: 1.4em;">📦 Validation Suites</h3>
            <div class="suites-grid">
                {suites_html}
            </div>
            
            {failures_html}
        </div>
"""
    
    def _generate_issues_section(self, monitoring: Dict[str, Any]) -> str:
        """Generar sección de problemas detectados"""
        stages = monitoring.get('stages', {})
        
        all_errors = []
        all_warnings = []
        
        for stage_name, stage_data in stages.items():
            for error in stage_data.get('errors', []):
                all_errors.append((stage_name, error))
            for warning in stage_data.get('warnings', []):
                all_warnings.append((stage_name, warning))
        
        if not all_errors and not all_warnings:
            return ""
        
        issues_html = []
        
        for stage, error in all_errors:
            issues_html.append(f'<div class="issue-item error"><span class="issue-icon">✗</span>[{stage}] {error}</div>')
        
        for stage, warning in all_warnings:
            issues_html.append(f'<div class="issue-item"><span class="issue-icon">⚠</span>[{stage}] {warning}</div>')
        
        total_issues = len(all_errors) + len(all_warnings)
        
        return f"""
        <div class="section">
            <h2 class="section-title">⚠️ Issues Detected ({total_issues})</h2>
            <div class="issue-list">
                {''.join(issues_html)}
            </div>
        </div>
"""
    
    def _generate_audit_trail(
        self,
        monitoring: Dict[str, Any],
        audit: Optional[Dict[str, Any]]
    ) -> str:
        """Generar tabla de auditoría detallada"""
        stages = monitoring.get('stages', {})
        
        if not stages:
            return ""
        
        rows = []
        for stage_name, stage_data in stages.items():
            start_time = stage_data.get('start_time', 'N/A')
            end_time = stage_data.get('end_time', 'N/A')
            duration = stage_data.get('duration_seconds', 0)
            records = stage_data.get('records_output', 0)
            status = stage_data.get('status', 'unknown')
            
            # Status icon
            status_icon = '✓' if status == 'completed' else '✗'
            status_color = '#4caf50' if status == 'completed' else '#f44336'
            
            rows.append(f"""
                <tr>
                    <td><strong>{stage_name}</strong></td>
                    <td>{start_time}</td>
                    <td>{end_time}</td>
                    <td>{duration:.2f}s</td>
                    <td>{records:,}</td>
                    <td style="color: {status_color}; font-weight: bold;">{status_icon} {status}</td>
                </tr>
""")
        
        return f"""
        <div class="section">
            <h2 class="section-title">📋 Audit Trail</h2>
            <table class="audit-table">
                <thead>
                    <tr>
                        <th>Stage</th>
                        <th>Start Time</th>
                        <th>End Time</th>
                        <th>Duration</th>
                        <th>Records</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>
"""
    
    def _get_progress_class(self, percentage: float) -> str:
        """Determinar clase CSS para barra de progreso"""
        if percentage >= 90:
            return 'high'
        elif percentage >= 70:
            return 'medium'
        else:
            return 'low'
    
    def _get_score_class(self, score: float) -> str:
        """Determinar clase CSS para score de calidad"""
        if score >= 90:
            return 'score-excellent'
        elif score >= 75:
            return 'score-good'
        elif score >= 60:
            return 'score-fair'
        else:
            return 'score-poor'
