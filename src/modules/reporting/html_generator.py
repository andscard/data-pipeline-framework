"""
Generador de reportes HTML profesional - Versión 2.0
Diseño empresarial limpio y elegante
"""

import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


from .validation_formatter import ValidationFormatter

from src.config import Config

class HTMLReportGenerator:
    """Generador de reportes HTML con diseño profesional empresarial"""
    
    def generate_report(
        self,
        monitoring_summary: Dict[str, Any],
        audit_data: Optional[Dict[str, Any]] = None,
        output_path: Optional[Path] = None,
        include_stages: bool = True,
        report_type: str = "technical"
    ) -> Path:
        """Generar reporte HTML profesional
        
        Args:
            monitoring_summary: Resumen de métricas del MonitoringCollector
            audit_data: Datos de auditoría (validation_results)
            output_path: Ruta de salida (opcional)
            include_stages: Si True, incluye tabla de Pipeline Stages
            report_type: 'technical' (validación) o 'execution' (completo)
        """
        if not output_path:
            execution_id = monitoring_summary.get('execution_id', 'unknown')
            pipeline_name = monitoring_summary.get('pipeline_name', 'pipeline')
            # Sanitizar nombre del pipeline para archivos
            safe_pipeline_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pipeline_name)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            stage_suffix = "validation" if report_type == "technical" else "execution"
            
            # Formato solicitado: <NombrePipeline>_<fecha en formato YYYYmmDD_HHMMSS>_<stage>.html
            filename = f"{safe_pipeline_name}_{timestamp}_{stage_suffix}.html"
            output_path = Config.REPORTS_DIR / filename
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        html_content = self._build_html(monitoring_summary, audit_data, include_stages, report_type)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"✓ Reporte HTML generado: {output_path}")
        return output_path
    
    def _build_html(self, monitoring: Dict[str, Any], audit: Optional[Dict[str, Any]], include_stages: bool = True, report_type: str = "technical") -> str:
        execution_id = monitoring.get('execution_id', 'N/A')[:8]
        pipeline_name = monitoring.get('pipeline_name', 'Pipeline')
        start_time = monitoring.get('start_time', 'N/A')
        
        report_title = "Data Quality Validation Report" if report_type == "technical" else "Data Pipeline Execution Report"
        
        health_status = monitoring.get('health_status', 'unknown')
        duration = monitoring.get('total_duration', 0)
        records = monitoring.get('total_records_processed', 0)
        stages = monitoring.get('stages', {})
        
        validation_stage = stages.get('VALIDATION', {})
        quality_score = validation_stage.get('quality_score', 0)
        validations_passed = validation_stage.get('validations_passed', 0)
        validations_failed = validation_stage.get('validations_failed', 0)
        
        status_badge = self._get_status_badge(health_status, quality_score)
        
        validation_results = []
        if audit and 'validation_results' in audit:
            validation_results = audit['validation_results']
        
        validation_summary = audit.get('validation_summary', []) if audit else []
        quality_thresholds = audit.get('quality_thresholds', {}) if audit else {}

        suites_summary = self._group_by_suite_from_summary(validation_summary)
        
        dataset_scorecard_html = self._render_dataset_quality_summary(suites_summary, quality_thresholds)
        
        suites_html = self._render_suites(suites_summary)
        
        detailed_failures_html = self._render_detailed_failures(validation_results)
        
        stages_section = ""
        if include_stages:
            stages_html = self._render_stages(stages)
            stages_section = f"""
        <!-- Pipeline Stages -->
        <section class="stages-section">
            <h2>Pipeline Stages</h2>
            {stages_html}
        </section>
        """
        
        return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{report_title} - {execution_id}</title>
    <style>
{self._get_professional_css()}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header class="header">
            <div class="header-content">
                <h1>{report_title}</h1>
                <div class="header-meta">
                    <span><strong>Pipeline:</strong> {pipeline_name}</span>
                    <span><strong>Execution ID:</strong> {execution_id}</span>
                    <span><strong>Date:</strong> {start_time}</span>
                </div>
            </div>
        </header>
        
        <!-- Executive Summary -->
        <section class="summary">
            <h2>Executive Summary</h2>
            <div class="summary-grid">
                <div class="summary-card">
                    <div class="card-label">Status</div>
                    <div class="card-value">{status_badge}</div>
                </div>
                <div class="summary-card">
                    <div class="card-label">Duration</div>
                    <div class="card-value">{duration:.2f}s</div>
                </div>
                <div class="summary-card">
                    <div class="card-label">Records Ingested</div>
                    <div class="card-value">{records:,}</div>
                    <div style="font-size: 0.75em; color: #6c757d; margin-top: 5px;">Total unique records loaded</div>
                </div>
                <div class="summary-card">
                    <div class="card-label">Quality Score</div>
                    <div class="card-value {self._get_quality_class(quality_score)}">{quality_score:.1f}%</div>
                </div>
            </div>
            
            <!-- Data Flow Summary (solo si include_stages) -->
            {self._render_data_flow_section(stages) if include_stages else ''}
        </section>
        
        <!-- Validation Results -->
        <section class="validation-section">
            <h2>Validation Results</h2>
            <div class="validation-overview">
                <div class="validation-stat success">
                    <span class="stat-number">{validations_passed}</span>
                    <span class="stat-label">Passed</span>
                </div>
                <div class="validation-stat failed">
                    <span class="stat-number">{validations_failed}</span>
                    <span class="stat-label">Failed</span>
                </div>
                <div class="validation-stat total">
                    <span class="stat-number">{validations_passed + validations_failed}</span>
                    <span class="stat-label">Total</span>
                </div>
            </div>
            
            {dataset_scorecard_html}

            <h3 style="margin-top: 30px; margin-bottom: 20px;">Suite Detailed Breakdown</h3>
            {suites_html}
            
            <h3 style="margin-top: 40px; margin-bottom: 20px;">Failure Details</h3>
            {detailed_failures_html}
        </section>
        
        {stages_section}
        
        <!-- Footer -->
        <footer class="footer">
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data Pipeline Framework v2.0</p>
        </footer>
    </div>
</body>
</html>"""
    
    def _get_professional_css(self) -> str:
        """CSS profesional y empresarial"""
        return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
            background: #f8f9fa;
            color: #212529;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1400px;
            margin: 40px auto;
            background: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
        }
        
        /* Header */
        .header {
            background: #1a1a2e;
            color: white;
            padding: 30px 40px;
            border-bottom: 3px solid #0f3460;
        }
        
        .header h1 {
            font-size: 1.8em;
            font-weight: 600;
            margin-bottom: 15px;
        }
        
        .header-meta {
            display: flex;
            gap: 30px;
            font-size: 0.9em;
            color: #cbd5e0;
        }
        
        /* Sections */
        section {
            padding: 40px;
            border-bottom: 1px solid #e9ecef;
        }
        
        section:last-of-type {
            border-bottom: none;
        }
        
        section h2 {
            font-size: 1.5em;
            font-weight: 600;
            color: #1a1a2e;
            margin-bottom: 25px;
            padding-bottom: 10px;
            border-bottom: 2px solid #0f3460;
        }
        
        /* Summary Grid */
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }
        
        .summary-card {
            padding: 25px;
            background: #f8f9fa;
            border-left: 4px solid #0f3460;
            border-radius: 4px;
        }
        
        .card-label {
            font-size: 0.85em;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        
        .card-value {
            font-size: 2em;
            font-weight: 600;
            color: #1a1a2e;
        }
        
        /* Status Badges */
        .status-badge {
            display: inline-block;
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 0.75em;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .status-healthy {
            background: #d4edda;
            color: #155724;
        }
        
        .status-warning {
            background: #fff3cd;
            color: #856404;
        }
        
        .status-critical {
            background: #f8d7da;
            color: #721c24;
        }
        
        .quality-excellent { color: #28a745; }
        .quality-good { color: #5cb85c; }
        .quality-warning { color: #ffc107; }
        .quality-poor { color: #dc3545; }
        
        /* Validation Overview */
        .validation-overview {
            display: flex;
            gap: 40px;
            margin-bottom: 30px;
            padding: 25px;
            background: #f8f9fa;
            border-radius: 4px;
        }
        
        .validation-stat {
            text-align: center;
        }
        
        .stat-number {
            display: block;
            font-size: 2.5em;
            font-weight: 700;
        }
        
        .stat-label {
            display: block;
            font-size: 0.9em;
            color: #6c757d;
            margin-top: 5px;
        }
        
        .validation-stat.success .stat-number {
            color: #28a745;
        }
        
        .validation-stat.failed .stat-number {
            color: #dc3545;
        }
        
        .validation-stat.total .stat-number {
            color: #0f3460;
        }
        
        /* Suites Table */
        .suites-table {
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            background: white;
            border: 1px solid #dee2e6;
        }
        
        .suites-table th {
            background: #f8f9fa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #495057;
            border-bottom: 2px solid #dee2e6;
            font-size: 0.9em;
        }
        
        .suites-table td {
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }
        
        .suites-table tr:last-child td {
            border-bottom: none;
        }
        
        .suites-table tr:hover {
            background: #f8f9fa;
        }
        
        .suite-name {
            font-weight: 500;
            color: #1a1a2e;
        }
        
        .suite-type {
            display: inline-block;
            padding: 3px 10px;
            background: #e9ecef;
            border-radius: 3px;
            font-size: 0.8em;
            color: #495057;
        }
        
        /* Failures Table */
        .failures-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 30px;
            border: 1px solid #dee2e6;
        }
        
        .failures-table thead {
            background: #1a1a2e;
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
            border-bottom: 1px solid #dee2e6;
        }
        
        .failures-table tr:hover {
            background: #f8f9fa;
        }
        
        .failures-table code {
            background: #f8f9fa;
            padding: 3px 8px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            color: #495057;
        }
        
        .severity-critical {
            background: #ffe5e5 !important;
        }
        
        .severity-high {
            background: #fff5e5 !important;
        }
        
        .severity-medium {
            background: #ffffeb !important;
        }
        
        .severity-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 6px;
        }
        
        .severity-indicator.critical {
            background: #dc3545;
        }
        
        .severity-indicator.high {
            background: #fd7e14;
        }
        
        .severity-indicator.medium {
            background: #ffc107;
        }
        
        /* Stages Table */
        .stages-table {
            width: 100%;
            border-collapse: collapse;
            border: 1px solid #dee2e6;
        }
        
        .stages-table th {
            background: #f8f9fa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #495057;
            border-bottom: 2px solid #dee2e6;
            font-size: 0.9em;
        }
        
        .stages-table td {
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }
        
        .stages-table tr:hover {
            background: #f8f9fa;
        }
        
        .stage-name {
            font-weight: 600;
            color: #1a1a2e;
        }
        
        /* Footer */
        .footer {
            background: #f8f9fa;
            padding: 20px 40px;
            text-align: center;
            color: #6c757d;
            font-size: 0.9em;
            border-top: 1px solid #dee2e6;
        }
        
        /* Responsive */
        @media (max-width: 768px) {
            .container {
                margin: 0;
            }
            
            section {
                padding: 20px;
            }
            
            .header {
                padding: 20px;
            }
            
            .header-meta {
                flex-direction: column;
                gap: 10px;
            }
            
            .summary-grid {
                grid-template-columns: 1fr;
            }
        }
"""
    
    def _get_status_badge(self, status: str, quality: float) -> str:
        """Generar badge de status"""
        status_lower = str(status).lower()
        
        # Prioridad al status explícito del pipeline
        if status_lower in ['failed', 'error', 'critical', 'unhealthy']:
            return '<span class="status-badge status-critical">CRITICAL</span>'
        elif status_lower in ['warning', 'degraded']:
            return '<span class="status-badge status-warning">WARNING</span>'
            
        # Fallback basado en quality score
        if quality < 50:
            return '<span class="status-badge status-critical">CRITICAL</span>'
        elif quality < 80:
            return '<span class="status-badge status-warning">WARNING</span>'
        else:
            return '<span class="status-badge status-healthy">HEALTHY</span>'
    
    def _get_quality_class(self, score: float) -> str:
        """Clase CSS para quality score"""
        if score >= 90:
            return 'quality-excellent'
        elif score >= 70:
            return 'quality-good'
        elif score >= 50:
            return 'quality-warning'
        else:
            return 'quality-poor'
    
    def _group_by_suite_from_summary(self, validation_summary: list) -> dict:
        """Agrupar usando validation_summary (datos ya agregados en BD)"""
        suites = {}
        
        for summary in validation_summary:
            suite_name = summary.get('suite_name', 'Unknown')
            dataset_name = summary.get('dataset_name', '')
            
            # Usar dataset como diferenciador visual si existe
            if dataset_name and dataset_name != 'Unknown':
                display_name = f"{suite_name} <span style='color:#6c757d; font-size:0.85em'>({dataset_name})</span>"
            else:
                display_name = suite_name
            
            if display_name not in suites:
                suites[display_name] = {
                    'name': suite_name,
                    'dataset': dataset_name,
                    'type': 'great_expectations',
                    'passed': 0,
                    'failed': 0,
                    'total': 0
                }
            
            # Acumular conteos
            suites[display_name]['passed'] += summary.get('passed_validations', 0)
            suites[display_name]['failed'] += summary.get('failed_validations', 0)
            suites[display_name]['total'] += summary.get('total_validations', 0)
        
        return suites
    
    def _render_dataset_quality_summary(self, suites: dict, thresholds: dict) -> str:
        if not suites or not thresholds:
            return ""
            
        dataset_stats = {}
        for _, data in suites.items():
            ds = data.get('dataset')
            if not ds: continue
            
            if ds not in dataset_stats:
                dataset_stats[ds] = {'passed': 0, 'failed': 0, 'total': 0}
            
            dataset_stats[ds]['passed'] += data['passed']
            dataset_stats[ds]['failed'] += data['failed']
            dataset_stats[ds]['total'] += data['total']
            
        if not dataset_stats:
            return ""

        rows = []
        for ds, stats in dataset_stats.items():
            if ds not in thresholds: continue
            
            total = stats['total']
            score = (stats['passed'] / total * 100) if total > 0 else 0
            target = thresholds[ds] * 100
            
            is_critical = score < target
            status_label = "CRITICAL" if is_critical else "HEALTHY"
            status_class = "status-critical" if is_critical else "status-healthy"
            score_color = "#dc3545" if is_critical else "#28a745"
            
            rows.append(f"""
            <tr>
                <td style="font-weight:600;">{ds}</td>
                <td style="text-align:center;">{stats['passed']}</td>
                <td style="text-align:center;">{stats['failed']}</td>
                <td style="text-align:center;">{stats['total']}</td>
                <td style="text-align:center;">
                    <span style="color:{score_color}; font-weight:bold;">{score:.1f}%</span>
                </td>
                <td style="text-align:center; color:#6c757d;">{target:.1f}%</td>
                <td style="text-align:center;">
                    <span class="status-badge {status_class}">{status_label}</span>
                </td>
            </tr>
            """)
            
        if not rows:
            return ""
            
        return f"""
        <div style="margin-bottom: 30px;">
            <h3>Dataset Quality Scorecards</h3>
            <table class="suites-table">
                <thead>
                    <tr>
                        <th>Dataset</th>
                        <th style="text-align:center;">Passed</th>
                        <th style="text-align:center;">Failed</th>
                        <th style="text-align:center;">Total Checks</th>
                        <th style="text-align:center;">Actual Score</th>
                        <th style="text-align:center;">Target Score</th>
                        <th style="text-align:center;">Status</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>
        """

    def _render_suites(self, suites: dict) -> str:
        if not suites:
            return ""
        
        sorted_suites = sorted(suites.items(), key=lambda x: x[0])
        
        rows = []
        for suite_name, data in sorted_suites:
            success_rate = (data['passed'] / data['total'] * 100) if data['total'] > 0 else 0
            
            status_color = '#dc3545' if data['failed'] > 0 else '#28a745'
            
            rows.append(f"""
            <tr>
                <td><span class="suite-name">{suite_name}</span></td>
                <td><span class="suite-type">{data['type']}</span></td>
                <td style="text-align: center; color: #28a745; font-weight: 600;">{data['passed']}</td>
                <td style="text-align: center; color: #dc3545; font-weight: 600;">{data['failed']}</td>
                <td style="text-align: center; font-weight: 600;">{data['total']}</td>
                <td style="text-align: center;">
                   <span style="color: {status_color}; font-weight: bold;">{success_rate:.1f}%</span>
                </td>
            </tr>
            """)
        
        return f"""
        <table class="suites-table">
            <thead>
                <tr>
                    <th>Suite Name</th>
                    <th>Type</th>
                    <th style="text-align: center;">Passed</th>
                    <th style="text-align: center;">Failed</th>
                    <th style="text-align: center;">Total</th>
                    <th style="text-align: center;">Success Rate</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """
    
    def _render_failures_summary(self, suites: dict) -> str:
        """Renderizar resumen de fallos agrupado por suite (simple y directo)"""
        failed_suites = {name: data for name, data in suites.items() if data['failed'] > 0}
        
        if not failed_suites:
            return '<div style="padding: 20px; background: #d4edda; color: #155724; border-radius: 4px; margin-top: 20px;">✓ Todas las validaciones pasaron exitosamente</div>'
        
        rows = []
        sorted_suites = sorted(failed_suites.items(), key=lambda x: x[1]['failed'], reverse=True)
        
        for suite_name, data in sorted_suites:
            failed_count = data['failed']
            total_count = data['total']
            passed_count = data['passed']
            failure_rate = (failed_count / total_count * 100) if total_count > 0 else 0
            
            # Color según severidad
            if failure_rate > 50:
                severity_color = '#dc3545'
                severity_label = 'CRÍTICO'
            elif failure_rate > 20:
                severity_color = '#fd7e14'
                severity_label = 'ALTO'
            else:
                severity_color = '#ffc107'
                severity_label = 'MEDIO'
            
            rows.append(f"""
            <tr style="border-left: 4px solid {severity_color};">
                <td><strong style="color: #0f3460;">{suite_name}</strong></td>
                <td style="text-align: center;">
                    <span style="background: {severity_color}; color: white; padding: 4px 12px; border-radius: 12px; font-size: 0.85em; font-weight: 600;">
                        {severity_label}
                    </span>
                </td>
                <td style="text-align: center; color: #28a745; font-weight: 600;">{passed_count}</td>
                <td style="text-align: center; color: #dc3545; font-weight: 600;">{failed_count}</td>
                <td style="text-align: center; font-weight: 600;">{total_count}</td>
                <td style="text-align: center;">
                    <strong style="color: {severity_color}; font-size: 1.1em;">{failure_rate:.1f}%</strong>
                </td>
            </tr>
            """)
        
        total_failures = sum(data['failed'] for data in failed_suites.values())
        
        return f"""
        <div style="margin-top: 30px;">
            <h3 style="color: #dc3545; font-size: 1.3em; margin-bottom: 20px;">
                ⚠️ Resumen de Validaciones Fallidas ({total_failures} problemas detectados)
            </h3>
            <table class="failures-table" style="width: 100%; border-collapse: collapse; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <thead>
                    <tr style="background: #1a1a2e; color: white;">
                        <th style="padding: 12px; text-align: left; width:30%;">Suite</th>
                        <th style="padding: 12px; text-align: center; width: 15%;">Severidad</th>
                        <th style="padding: 12px; text-align: center; width: 13%;">Pasadas</th>
                        <th style="padding: 12px; text-align: center; width: 13%;">Fallidas</th>
                        <th style="padding: 12px; text-align: center; width: 13%;">Total</th>
                        <th style="padding: 12px; text-align: center; width: 16%;">Tasa de Fallo</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>
        """
    
    def _render_detailed_failures(self, validation_results: list) -> str:
        """Renderizar tabla detallada de todos los fallos individuales sin ocultar información con diseño en acordeón"""
        
        if not validation_results:
            return ""
        
        # Filtrar solo los fallos
        failed_results = [item for item in validation_results if not item.get('passed', True)]
        
        if not failed_results:
            return ""
        
        # Agrupar fallos por Suite/Dataset
        grouped_failures = {}
        for item in failed_results:
            # Obtener datos básicos del registro
            suite = item.get('suite_name', 'Default Suite')
            dataset_name = item.get('dataset_name', '')
            group_key = f"{suite}|{dataset_name}"
            
            if group_key not in grouped_failures:
                grouped_failures[group_key] = []
            
            grouped_failures[group_key].append(item)
            
        
        table_rows = []
        group_id_counter = 0

        for key, failures in grouped_failures.items():
            suite_name, dataset_name = key.split('|')
            group_id_counter += 1
            group_id = f"group_{group_id_counter}"
            
            # Pre-calcular total de items de detalle para el badge
            detail_row_count = 0
            temp_detail_rows = []
            
            for item in failures:
                # Datos básicos y fallback para expectation_type
                exp_type = item.get('expectation_type', 'N/A')
                failure_details = item.get('failure_details', [])
                if not failure_details: failure_details = [{}]
                if isinstance(failure_details, dict): failure_details = [failure_details]
                
                for detail in failure_details:
                    # Increment count for each DETAIL row generated
                    detail_row_count += 1
                    
                    kwargs = detail.get('kwargs', {})
                    if not kwargs and 'rule_name' in item: kwargs['expectation_type'] = item['rule_name']
                    
                    column = kwargs.get('column', kwargs.get('column_list', 'N/A'))
                    if not column or column == 'N/A': column = "Tabla Completa"
                    
                    # CORRECCION: Obtener expectation_type real del detalle, no del registro padre (que es un resumen)
                    real_exp_type = detail.get('expectation_type', exp_type)
                    
                    # Descripción legible usando el Formatter Modular
                    description = ValidationFormatter.get_description(real_exp_type, detail)
                    
                    # Métricas específicas (Priorizar datos directos del detalle)
                    result_info = detail.get('result', detail) 
                    
                    unexpected_count = result_info.get('unexpected_count', detail.get('unexpected_count'))
                    element_count = result_info.get('element_count', detail.get('element_count'))
                    observed_value = result_info.get('observed_value', detail.get('observed_value'))
                    partial_list = result_info.get('partial_unexpected_list', detail.get('partial_unexpected_list', []))

                    if unexpected_count is not None:
                        # Caso estándar: Conteo de fallos disponible
                        if element_count and element_count > 0:
                            affected_display = f"{self._format_number(unexpected_count)} / {self._format_number(element_count)}"
                        else:
                            affected_display = self._format_number(unexpected_count)
                    elif partial_list:
                        # Fallback: Si no hay conteo total pero sí ejemplos, mostramos cantidad de ejemplos +
                        affected_display = f"Min. {len(partial_list)}"
                    elif observed_value is not None:
                        # Caso especial: Validaciones a nivel tabla (row_count) o agregaciones
                        # donde observed_value es la métrica relevante
                        if 'expect_table_' in real_exp_type or 'expect_column_mean' in real_exp_type:
                             affected_display = f"Valor: {observed_value}"
                        else:
                             affected_display = "N/A"
                    else:
                        affected_display = "Ver detalle"
                    
                     # Formatear columna para display
                    if isinstance(column, list):
                        column_display = ', '.join(str(c) for c in column)
                    else:
                        column_display = str(column)

                    severity = item.get('severity', 'error')
                    severity_color = '#dc3545' if severity == 'critical' else '#fd7e14' if severity == 'error' else '#ffc107'
                    severity_label = severity.upper()

                    # Save row HTML to list
                    temp_detail_rows.append(f"""
                    <tr class="detail-row {group_id}" style="display: none; background: white; border-bottom: 1px solid #f1f2f3;">
                        <td style="width: 20px; border-left: 4px solid {severity_color};"></td>
                        <td style="padding: 10px 15px; width: 20%;">
                            <code style="color: #e83e8c;">{column_display}</code>
                        </td>
                        <td style="padding: 10px 15px;">{description}</td>
                        <td style="padding: 10px 15px; text-align: center;">
                            <span style="color: {severity_color}; font-weight: bold; font-size: 0.8em;">{severity_label}</span>
                        </td>
                        <td style="padding: 10px 15px; text-align: right; font-family: monospace;">{affected_display}</td>
                    </tr>
                    """)
            
            # Fila Principal (Encabezado del Grupo) con conteo REAL
            table_rows.append(f"""
            <tr class="group-header" onclick="toggleGroup('{group_id}')" style="background: #e9ecef; cursor: pointer; border-bottom: 2px solid #dee2e6;">
                <td colspan="5" style="padding: 12px 15px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-size: 1.1em; font-weight: bold; color: #343a40;">{suite_name}</span>
                            <span style="margin-left: 10px; font-size: 0.9em; color: #6c757d; font-family: monospace;">{dataset_name}</span>
                        </div>
                        <div>
                            <span style="background: #dc3545; color: white; padding: 2px 8px; border-radius: 10px; font-size: 0.85em; font-weight: bold;">{detail_row_count} fallos</span>
                            <span style="margin-left: 10px; font-size: 0.8em; color: #6c757d;">▼</span>
                        </div>
                    </div>
                </td>
            </tr>
            """)

            # Encabezado de la tabla interna (solo aparece una vez por grupo)
            table_rows.append(f"""
            <tr class="detail-row {group_id}" style="display: none; background: #f8f9fa;">
               <th style="padding: 8px 15px; font-size:0.85em; color: #6c757d; border-bottom: 1px solid #dee2e6;"></th>
               <th style="padding: 8px 15px; font-size:0.85em; color: #6c757d; border-bottom: 1px solid #dee2e6;">Columna</th>
               <th style="padding: 8px 15px; font-size:0.85em; color: #6c757d; border-bottom: 1px solid #dee2e6;">Detalle del Error</th>
               <th style="padding: 8px 15px; font-size:0.85em; color: #6c757d; border-bottom: 1px solid #dee2e6; text-align:center;">Severidad</th>
               <th style="padding: 8px 15px; font-size:0.85em; color: #6c757d; border-bottom: 1px solid #dee2e6; text-align:right;">Registros</th>
            </tr>
            """)
            
            # Agregar filas de detalle pre-generadas
            table_rows.extend(temp_detail_rows)

        return f"""
        <div style="margin-top: 40px;">
            <h3 style="color: #343a40; font-size: 1.2em; margin-bottom: 15px;">
                📋 Detalle de Validaciones Fallidas
            </h3>
            <table style="width: 100%; border-collapse: separate; border-spacing: 0; border: 1px solid #dee2e6; border-radius: 8px; overflow: hidden;">
                { ''.join(table_rows) }
            </table>
            
            <script>
            function toggleGroup(groupId) {{
                var rows = document.getElementsByClassName(groupId);
                for(var i = 0; i < rows.length; i++) {{
                    rows[i].style.display = rows[i].style.display === 'none' ? 'table-row' : 'none';
                }}
            }}
            </script>
        </div>
        """
    
    def _format_number(self, value) -> str:
        """Formatear número con separadores de miles"""
        if value is None or value == 'N/A':
            return 'N/A'
        try:
            return f"{int(value):,}"
        except (ValueError, TypeError):
            return str(value)
    
    def _render_data_flow_section(self, stages: dict) -> str:
        """Renderizar sección completa de Data Flow"""
        cards = self._render_data_flow_cards(stages)
        return f"""
            <div style="margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 8px;">
                <h3 style="font-size: 1.1em; margin-bottom: 15px; color: #1a1a2e;">Pipeline Data Flow</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">
                    {cards}
                </div>
            </div>
        """
    
    def _render_data_flow_cards(self, stages: dict) -> str:
        """Renderizar tarjetas de flujo de datos"""
        cards = []
        
        stage_order = ["INGESTION", "VALIDATION", "TRANSFORMATION", "OUTPUT"]
        stage_icons = {
            "INGESTION": "📥",
            "VALIDATION": "✓",
            "TRANSFORMATION": "⚙️",
            "OUTPUT": "📤"
        }
        
        for stage_name in stage_order:
            if stage_name in stages:
                data = stages[stage_name]
                records = data.get('records_output', 0)
                icon = stage_icons.get(stage_name, "•")
                
                cards.append(f"""
                <div style="text-align: center; padding: 15px; background: white; border-radius: 4px; border: 1px solid #dee2e6;">
                    <div style="font-size: 1.5em; margin-bottom: 5px;">{icon}</div>
                    <div style="font-size: 0.75em; color: #6c757d; text-transform: uppercase; margin-bottom: 5px;">{stage_name}</div>
                    <div style="font-size: 1.3em; font-weight: 600; color: #0f3460;">{records:,}</div>
                    <div style="font-size: 0.7em; color: #6c757d;">records</div>
                </div>
                """)
        
        return ''.join(cards)
    
    def _render_stages(self, stages: dict) -> str:
        """Renderizar tabla de stages"""
        if not stages:
            return ""
        
        rows = []
        for stage_name, data in stages.items():
            duration = data.get('duration_seconds', 0)
            records_in = data.get('records_input', 0)
            records_out = data.get('records_output', 0)
            errors = len(data.get('errors', []))
            
            status = '✓' if errors == 0 else '✗'
            status_color = '#28a745' if errors == 0 else '#dc3545'
            
            # Explicación contextual por etapa
            if stage_name == "INGESTION":
                context = "Loaded from sources"
            elif stage_name == "VALIDATION":
                context = f"{data.get('validations_passed', 0)}/{data.get('validations_passed', 0) + data.get('validations_failed', 0)} validations passed"
            elif stage_name == "TRANSFORMATION":
                context = "Filtered & transformed"
            elif stage_name == "OUTPUT":
                context = "Written to targets"
            else:
                context = ""
            
            rows.append(f"""
            <tr>
                <td><span class="stage-name">{stage_name}</span></td>
                <td style="text-align: right;">{duration:.2f}s</td>
                <td style="text-align: right;">{records_in:,}</td>
                <td style="text-align: right;">{records_out:,}</td>
                <td style="font-size: 0.85em; color: #6c757d;">{context}</td>
                <td style="text-align: right; color: {status_color};">{errors}</td>
                <td style="text-align: center; font-size: 1.2em; color: {status_color};">{status}</td>
            </tr>
            """)
        
        return f"""
        <table class="stages-table">
            <thead>
                <tr>
                    <th>Stage</th>
                    <th style="text-align: right;">Duration</th>
                    <th style="text-align: right;">Records In</th>
                    <th style="text-align: right;">Records Out</th>
                    <th>Context</th>
                    <th style="text-align: right;">Errors</th>
                    <th style="text-align: center;">Status</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """
    
    def _render_data_flow_section(self, stages: dict) -> str:
        """Renderizar sección completa de Data Flow"""
        cards = self._render_data_flow_cards(stages)
        return f"""
            <div style="margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 8px;">
                <h3 style="font-size: 1.1em; margin-bottom: 15px; color: #1a1a2e;">Pipeline Data Flow</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">
                    {cards}
                </div>
            </div>
        """
    
    def _render_data_flow_cards(self, stages: dict) -> str:
        """Renderizar tarjetas de flujo de datos"""
        cards = []
        
        stage_order = ["INGESTION", "VALIDATION", "TRANSFORMATION", "OUTPUT"]
        stage_icons = {
            "INGESTION": "📥",
            "VALIDATION": "✓",
            "TRANSFORMATION": "⚙️",
            "OUTPUT": "📤"
        }
        
        for stage_name in stage_order:
            if stage_name in stages:
                data = stages[stage_name]
                records = data.get('records_output', 0)
                icon = stage_icons.get(stage_name, "•")
                
                cards.append(f"""
                <div style="text-align: center; padding: 15px; background: white; border-radius: 4px; border: 1px solid #dee2e6;">
                    <div style="font-size: 1.5em; margin-bottom: 5px;">{icon}</div>
                    <div style="font-size: 0.75em; color: #6c757d; text-transform: uppercase; margin-bottom: 5px;">{stage_name}</div>
                    <div style="font-size: 1.3em; font-weight: 600; color: #0f3460;">{records:,}</div>
                    <div style="font-size: 0.7em; color: #6c757d;">records</div>
                </div>
                """)
        
        return ''.join(cards)
    
    def _render_stages(self, stages: dict) -> str:
        """Renderizar tabla de stages"""
        if not stages:
            return ""
        
        rows = []
        for stage_name, data in stages.items():
            duration = data.get('duration_seconds', 0)
            records_in = data.get('records_input', 0)
            records_out = data.get('records_output', 0)
            errors = len(data.get('errors', []))
            
            status = '✓' if errors == 0 else '✗'
            status_color = '#28a745' if errors == 0 else '#dc3545'
            
            # Explicación contextual por etapa
            if stage_name == "INGESTION":
                context = "Loaded from sources"
            elif stage_name == "VALIDATION":
                context = f"{data.get('validations_passed', 0)}/{data.get('validations_passed', 0) + data.get('validations_failed', 0)} validations passed"
            elif stage_name == "TRANSFORMATION":
                context = "Filtered & transformed"
            elif stage_name == "OUTPUT":
                context = "Written to targets"
            else:
                context = ""
            
            rows.append(f"""
            <tr>
                <td><span class="stage-name">{stage_name}</span></td>
                <td style="text-align: right;">{duration:.2f}s</td>
                <td style="text-align: right;">{records_in:,}</td>
                <td style="text-align: right;">{records_out:,}</td>
                <td style="font-size: 0.85em; color: #6c757d;">{context}</td>
                <td style="text-align: right; color: {status_color};">{errors}</td>
                <td style="text-align: center; font-size: 1.2em; color: {status_color};">{status}</td>
            </tr>
            """)
        
        return f"""
        <table class="stages-table">
            <thead>
                <tr>
                    <th>Stage</th>
                    <th style="text-align: right;">Duration</th>
                    <th style="text-align: right;">Records In</th>
                    <th style="text-align: right;">Records Out</th>
                    <th>Context</th>
                    <th style="text-align: right;">Errors</th>
                    <th style="text-align: center;">Status</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """
