"""
Generador de reportes HTML profesional - Versión 2.0
Diseño empresarial limpio y elegante
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class HTMLReportGenerator:
    """Generador de reportes HTML con diseño profesional empresarial"""
    
    def generate_report(
        self,
        monitoring_summary: Dict[str, Any],
        audit_data: Optional[Dict[str, Any]] = None,
        output_path: Optional[Path] = None
    ) -> Path:
        """Generar reporte HTML profesional"""
        if not output_path:
            execution_id = monitoring_summary.get('execution_id', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(f"reports/execution_{execution_id[:8]}_{timestamp}.html")
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        html_content = self._build_html(monitoring_summary, audit_data)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"✓ Reporte HTML generado: {output_path}")
        return output_path
    
    def _build_html(self, monitoring: Dict[str, Any], audit: Optional[Dict[str, Any]]) -> str:
        """Construir HTML completo"""
        execution_id = monitoring.get('execution_id', 'N/A')[:8]
        pipeline_name = monitoring.get('pipeline_name', 'Pipeline')
        start_time = monitoring.get('start_time', 'N/A')
        
        # Extraer métricas
        health_status = monitoring.get('health_status', 'unknown')
        duration = monitoring.get('total_duration', 0)
        records = monitoring.get('total_records_processed', 0)
        stages = monitoring.get('stages', {})
        
        validation_stage = stages.get('VALIDATION', {})
        quality_score = validation_stage.get('quality_score', 0)
        validations_passed = validation_stage.get('validations_passed', 0)
        validations_failed = validation_stage.get('validations_failed', 0)
        
        # Status badge
        status_badge = self._get_status_badge(health_status, quality_score)
        
        # Validation results
        validation_results = []
        if audit and 'validation_results' in audit:
            validation_results = audit['validation_results']
        
        # Agrupar por suite
        suites_summary = self._group_by_suite(validation_results)
        suites_html = self._render_suites(suites_summary)
        
        # Tabla de fallos
        failures_html = self._render_failures_table(validation_results)
        
        # Stage summary
        stages_html = self._render_stages(stages)
        
        return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Report - {execution_id}</title>
    <style>
{self._get_professional_css()}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header class="header">
            <div class="header-content">
                <h1>Data Pipeline Execution Report</h1>
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
                    <div class="card-label">Records</div>
                    <div class="card-value">{records:,}</div>
                </div>
                <div class="summary-card">
                    <div class="card-label">Quality Score</div>
                    <div class="card-value {self._get_quality_class(quality_score)}">{quality_score:.1f}%</div>
                </div>
            </div>
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
            
            {suites_html}
            {failures_html}
        </section>
        
        <!-- Pipeline Stages -->
        <section class="stages-section">
            <h2>Pipeline Stages</h2>
            {stages_html}
        </section>
        
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
    
    def _group_by_suite(self, validation_results: list) -> dict:
        """Agrupar validation results por suite"""
        suites = {}
        for result in validation_results:
            suite_name = result['rule_name']
            if suite_name not in suites:
                suites[suite_name] = {
                    'name': suite_name,
                    'type': result['rule_type'],
                    'passed': 0,
                    'failed': 0,
                    'total': 0,
                    'failures': []
                }
            
            if result['passed']:
                suites[suite_name]['passed'] += 1
            else:
                # Contar 1 expectativa fallida, no el número de registros afectados
                suites[suite_name]['failed'] += 1
                suites[suite_name]['failures'].extend(result.get('failure_details', []))
            
            suites[suite_name]['total'] = suites[suite_name]['passed'] + suites[suite_name]['failed']
        
        return suites
    
    def _render_suites(self, suites: dict) -> str:
        """Renderizar tabla de suites"""
        if not suites:
            return ""
        
        rows = []
        for suite_name, data in suites.items():
            success_rate = (data['passed'] / data['total'] * 100) if data['total'] > 0 else 0
            status = '✓' if success_rate == 100 else '⚠' if success_rate >= 50 else '✗'
            
            rows.append(f"""
            <tr>
                <td><span class="suite-name">{suite_name}</span></td>
                <td><span class="suite-type">{data['type']}</span></td>
                <td style="text-align: center; color: #28a745; font-weight: 600;">{data['passed']}</td>
                <td style="text-align: center; color: #dc3545; font-weight: 600;">{data['failed']}</td>
                <td style="text-align: center; font-weight: 600;">{data['total']}</td>
                <td style="text-align: center;">{success_rate:.1f}%</td>
                <td style="text-align: center; font-size: 1.2em;">{status}</td>
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
                    <th style="text-align: center;">Status</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """
    
    def _render_failures_table(self, validation_results: list) -> str:
        """Renderizar tabla detallada de fallos"""
        failures = []
        for result in validation_results:
            if not result['passed'] and result.get('failure_details'):
                for detail in result['failure_details']:
                    failures.append({
                        'suite': result['rule_name'],
                        'detail': detail
                    })
        
        if not failures:
            return '<div style="padding: 20px; background: #d4edda; color: #155724; border-radius: 4px; margin-top: 20px;">✓ All validations passed successfully</div>'
        
        rows = []
        for item in failures:
            detail = item['detail']
            exp_type = detail.get('expectation_type', 'N/A')
            kwargs = detail.get('kwargs', {})
            column = kwargs.get('column', 'N/A')
            
            # Extraer pattern
            pattern = kwargs.get('regex', kwargs.get('value_set', kwargs.get('min_value', 'N/A')))
            if isinstance(pattern, str) and len(pattern) > 60:
                pattern = pattern[:57] + '...'
            
            unexpected_count = detail.get('unexpected_count', 0) or 0
            unexpected_percent = detail.get('unexpected_percent') or 0.0
            element_count = detail.get('element_count', 0) or 0
            
            # Convertir a números
            try:
                unexpected_count = int(unexpected_count)
                element_count = int(element_count)
                unexpected_percent = float(unexpected_percent)
            except:
                unexpected_count = 0
                element_count = 0
                unexpected_percent = 0.0
            
            # Severity
            severity = 'critical' if unexpected_percent > 10 else 'high' if unexpected_percent > 5 else 'medium'
            severity_class = f'severity-{severity}'
            
            # Simplificar nombre
            simple_type = exp_type.replace('expect_column_values_to_', '').replace('expect_column_', '').replace('_', ' ').title()
            
            rows.append(f"""
            <tr class="{severity_class}">
                <td><span class="severity-indicator {severity}"></span>{item['suite']}</td>
                <td><code>{simple_type}</code></td>
                <td><strong>{column}</strong></td>
                <td><code>{pattern}</code></td>
                <td style="text-align: right;">{unexpected_count:,} / {element_count:,}</td>
                <td style="text-align: right; font-weight: 600;">{unexpected_percent:.2f}%</td>
            </tr>
            """)
        
        return f"""
        <h3 style="margin-top: 40px; color: #dc3545; font-size: 1.2em;">Validation Failures Detail</h3>
        <table class="failures-table">
            <thead>
                <tr>
                    <th>Suite</th>
                    <th>Expectation</th>
                    <th>Column</th>
                    <th>Pattern/Rule</th>
                    <th style="text-align: right;">Affected Records</th>
                    <th style="text-align: right;">%</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """
    
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
            
            rows.append(f"""
            <tr>
                <td><span class="stage-name">{stage_name}</span></td>
                <td style="text-align: right;">{duration:.2f}s</td>
                <td style="text-align: right;">{records_in:,}</td>
                <td style="text-align: right;">{records_out:,}</td>
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
                    <th style="text-align: right;">Errors</th>
                    <th style="text-align: center;">Status</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """
