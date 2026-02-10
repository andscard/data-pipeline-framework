"""
Executive Report Generator - Reporte Gerencial
Enfocado en métricas de alto nivel para stakeholders ejecutivos
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ExecutiveReportGenerator:
    """Generador de reportes ejecutivos para gerencia y stakeholders"""
    
    def generate_report(
        self,
        monitoring_summary: Dict[str, Any],
        output_path: Optional[Path] = None
    ) -> Path:
        """Generar reporte ejecutivo HTML"""
        if not output_path:
            execution_id = monitoring_summary.get('execution_id', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(f"reports/executive_{execution_id[:8]}_{timestamp}.html")
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        html_content = self._build_html(monitoring_summary)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"✓ Reporte Ejecutivo generado: {output_path}")
        return output_path
    
    def _build_html(self, monitoring: Dict[str, Any]) -> str:
        """Construir HTML del reporte ejecutivo"""
        execution_id = monitoring.get('execution_id', 'N/A')[:8]
        pipeline_name = monitoring.get('pipeline_name', 'Pipeline')
        start_time = monitoring.get('start_time', 'N/A')
        
        # Métricas principales
        health_status = monitoring.get('health_status', 'unknown')
        duration = monitoring.get('total_duration', 0)
        records = monitoring.get('total_records_processed', 0)
        stages = monitoring.get('stages', {})
        
        validation_stage = stages.get('VALIDATION', {})
        quality_score = validation_stage.get('quality_score', 0)
        
        # KPIs
        status_color = self._get_status_color(health_status)
        quality_status = self._get_quality_status(quality_score)
        
        # Cálculos de rendimiento
        throughput = records / duration if duration > 0 else 0
        
        return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Executive Report - {pipeline_name}</title>
    <style>
{self._get_executive_css()}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header Ejecutivo -->
        <header class="header">
            <div class="logo-area">
                <div class="logo">📊</div>
                <div>
                    <h1>Data Pipeline</h1>
                    <p>Executive Summary Report</p>
                </div>
            </div>
            <div class="date-info">
                <div>{start_time}</div>
                <div style="font-size: 0.85em; opacity: 0.8;">Execution ID: {execution_id}</div>
            </div>
        </header>
        
        <!-- KPIs Principales -->
        <section class="kpi-section">
            <div class="kpi-card {status_color}">
                <div class="kpi-icon">🎯</div>
                <div class="kpi-value">{health_status.upper()}</div>
                <div class="kpi-label">Pipeline Status</div>
            </div>
            
            <div class="kpi-card accent">
                <div class="kpi-icon">✓</div>
                <div class="kpi-value">{quality_score:.1f}%</div>
                <div class="kpi-label">Quality Score</div>
                <div class="kpi-sublabel">{quality_status}</div>
            </div>
            
            <div class="kpi-card">
                <div class="kpi-icon">📦</div>
                <div class="kpi-value">{records:,}</div>
                <div class="kpi-label">Records Processed</div>
            </div>
            
            <div class="kpi-card">
                <div class="kpi-icon">⏱️</div>
                <div class="kpi-value">{duration:.2f}s</div>
                <div class="kpi-label">Total Duration</div>
                <div class="kpi-sublabel">{throughput:,.0f} rec/sec</div>
            </div>
        </section>
        
        <!-- Performance Overview -->
        <section class="performance-section">
            <h2>Performance Overview</h2>
            <div class="metrics-grid">
                {self._render_stage_metrics(stages)}
            </div>
        </section>
        
        <!-- Quality Insights -->
        <section class="insights-section">
            <h2>Quality Insights</h2>
            {self._render_quality_insights(validation_stage)}
        </section>
        
        <!-- Recommendations -->
        <section class="recommendations-section">
            <h2>Recommendations</h2>
            {self._render_recommendations(monitoring)}
        </section>
        
        <!-- Footer -->
        <footer class="footer">
            <p>Data Pipeline Framework | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="font-size: 0.85em; opacity: 0.7;">Confidential - For Internal Use Only</p>
        </footer>
    </div>
</body>
</html>"""
    
    def _get_executive_css(self) -> str:
        """CSS profesional para reporte ejecutivo"""
        return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }
        
        /* Header */
        .header {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 40px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .logo-area {
            display: flex;
            align-items: center;
            gap: 20px;
        }
        
        .logo {
            font-size: 3em;
            background: rgba(255,255,255,0.2);
            width: 80px;
            height: 80px;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 12px;
        }
        
        .header h1 {
            font-size: 2em;
            font-weight: 700;
            margin-bottom: 5px;
        }
        
        .header p {
            font-size: 0.95em;
            opacity: 0.9;
        }
        
        .date-info {
            text-align: right;
            font-size: 0.95em;
        }
        
        /* KPI Section */
        .kpi-section {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8f9fa;
        }
        
        .kpi-card {
            background: white;
            padding: 30px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: transform 0.3s;
        }
        
        .kpi-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 4px 16px rgba(0,0,0,0.12);
        }
        
        .kpi-icon {
            font-size: 2.5em;
            margin-bottom: 15px;
        }
        
        .kpi-value {
            font-size: 2.5em;
            font-weight: 700;
            color: #1e3c72;
            margin-bottom: 10px;
        }
        
        .kpi-label {
            font-size: 0.9em;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 600;
        }
        
        .kpi-sublabel {
            font-size: 0.85em;
            color: #6c757d;
            margin-top: 8px;
        }
        
        .kpi-card.success { border-left: 5px solid #28a745; }
.kpi-card.success .kpi-value { color: #28a745; }
        
        .kpi-card.warning { border-left: 5px solid #ffc107; }
        .kpi-card.warning .kpi-value { color: #ffc107; }
        
        .kpi-card.danger { border-left: 5px solid #dc3545; }
        .kpi-card.danger .kpi-value { color: #dc3545; }
        
        .kpi-card.accent { border-left: 5px solid #667eea; }
        .kpi-card.accent .kpi-value { color: #667eea; }
        
        /* Sections */
        section {
            padding: 40px;
            border-bottom: 1px solid #e9ecef;
        }
        
        section:last-of-type {
            border-bottom: none;
        }
        
        section h2 {
            font-size: 1.8em;
            font-weight: 600;
            color: #1e3c72;
            margin-bottom: 25px;
            position: relative;
            padding-bottom: 15px;
        }
        
        section h2:after {
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            width: 60px;
            height: 4px;
            background: linear-gradient(90deg, #667eea, #764ba2);
            border-radius: 2px;
        }
        
        /* Metrics Grid */
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
        }
        
        .metric-card {
            background: #f8f9fa;
            padding: 25px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }
        
        .metric-title {
            font-size: 0.85em;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
        }
        
        .metric-value {
            font-size: 1.8em;
            font-weight: 700;
            color: #1e3c72;
            margin-bottom: 8px;
        }
        
        .metric-detail {
            font-size: 0.9em;
            color: #6c757d;
        }
        
        /* Insights */
        .insight-box {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 30px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        
        .insight-icon {
            font-size: 2em;
            margin-bottom: 15px;
        }
        
        .insight-title {
            font-size: 1.2em;
            font-weight: 600;
            color: #1e3c72;
            margin-bottom: 10px;
        }
        
        .insight-text {
            font-size: 1em;
            color: #495057;
            line-height: 1.6;
        }
        
        .stat-bar {
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            margin: 15px 0;
            overflow: hidden;
        }
        
        .stat-fill {
            height: 100%;
            background: linear-gradient(90deg, #28a745, #20c997);
            border-radius: 4px;
            transition: width 1s ease;
        }
        
        .stat-fill.warning {
            background: linear-gradient(90deg, #ffc107, #fd7e14);
        }
        
        .stat-fill.danger {
            background: linear-gradient(90deg, #dc3545, #c82333);
        }
        
        /* Recommendations */
        .recommendation {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 15px;
            border-left: 4px solid #667eea;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        
        .recommendation-icon {
            display: inline-block;
            font-size: 1.3em;
            margin-right: 10px;
        }
        
        .recommendation-text {
            display: inline;
            font-size: 1em;
            color: #495057;
        }
        
        /* Footer */
        .footer {
            background: #1e3c72;
            color: white;
            padding: 20px 40px;
            text-align: center;
            font-size: 0.9em;
        }
        
        .footer p {
            margin: 5px 0;
        }
        """
    
    def _get_status_color(self, status: str) -> str:
        """Obtener clase de color según status"""
        status = status.lower()
        if status == 'healthy':
            return 'success'
        elif status == 'warning':
            return 'warning'
        elif status in ['critical', 'failed']:
            return 'danger'
        return ''
    
    def _get_quality_status(self, score: float) -> str:
        """Obtener status textual de calidad"""
        if score >= 95:
            return 'Excellent'
        elif score >= 85:
            return 'Good'
        elif score >= 75:
            return 'Acceptable'
        elif score >= 60:
            return 'Needs Improvement'
        else:
            return 'Critical'
    
    def _render_stage_metrics(self, stages: dict) -> str:
        """Renderizar métricas de etapas"""
        cards = []
        
        stage_icons = {
            'INGESTION': '📥',
            'VALIDATION': '✓',
            'TRANSFORMATION': '⚙️',
            'OUTPUT': '📤'
        }
        
        for stage_name, data in stages.items():
            icon = stage_icons.get(stage_name, '•')
            duration = data.get('duration_seconds', 0)
            records = data.get('records_output', 0)
            
            cards.append(f"""
            <div class="metric-card">
                <div class="metric-title">{icon} {stage_name}</div>
                <div class="metric-value">{duration:.2f}s</div>
                <div class="metric-detail">{records:,} records processed</div>
            </div>
            """)
        
        return ''.join(cards)
    
    def _render_quality_insights(self, validation_stage: dict) -> str:
        """Renderizar insights de calidad"""
        quality_score = validation_stage.get('quality_score', 0)
        passed = validation_stage.get('validations_passed', 0)
        failed = validation_stage.get('validations_failed', 0)
        total = passed + failed
        
        bar_class = 'stat-fill'
        if quality_score < 75:
            bar_class += ' danger'
        elif quality_score < 85:
            bar_class += ' warning'
        
        insight_icon = '✅' if quality_score >= 85 else '⚠️' if quality_score >= 75 else '❌'
        
        return f"""
        <div class="insight-box">
            <div class="insight-icon">{insight_icon}</div>
            <div class="insight-title">Quality Assessment</div>
            <div class="insight-text">
                {passed} de {total} validaciones pasaron exitosamente. 
                El pipeline alcanzó un score de calidad del {quality_score:.1f}%.
            </div>
            <div class="stat-bar">
                <div class="{bar_class}" style="width: {quality_score}%"></div>
            </div>
            <div style="display: flex; justify-content: space-between; font-size: 0.9em; color: #6c757d;">
                <span>✓ {passed} Passed</span>
                <span>✗ {failed} Failed</span>
            </div>
        </div>
        """
    
    def _render_recommendations(self, monitoring: dict) -> str:
        """Renderizar recomendaciones"""
        recommendations = []
        
        health_status = monitoring.get('health_status', 'unknown')
        quality_score = monitoring.get('stages', {}).get('VALIDATION', {}).get('quality_score', 0)
        duration = monitoring.get('total_duration', 0)
        
        # Recomendaciones basadas en métricas
        if quality_score < 75:
            recommendations.append({
                'icon': '🔍',
                'text': 'El score de calidad está por debajo del umbral. Se recomienda revisar las validaciones fallidas y corregir los datos en origen.'
            })
        
        if quality_score >= 95:
            recommendations.append({
                'icon': '🎯',
                'text': 'Excelente calidad de datos. El pipeline está operando dentro de los parámetros óptimos.'
            })
        
        if duration > 30:
            recommendations.append({
                'icon': '⚡',
                'text': f'El tiempo de ejecución ({duration:.1f}s) podría optimizarse. Considere implementar procesamiento paralelo o ajustar índices de base de datos.'
            })
        
        if health_status == 'failed':
            recommendations.append({
                'icon': '🚨',
                'text': 'El pipeline falló. Se recomienda investigar los errores reportados y tomar acción correctiva inmediata.'
            })
        
        if not recommendations:
            recommendations.append({
                'icon': '👍',
                'text': 'El pipeline está funcionando correctamente sin problemas detectados.'
            })
        
        html = []
        for rec in recommendations:
            html.append(f"""
            <div class="recommendation">
                <span class="recommendation-icon">{rec['icon']}</span>
                <span class="recommendation-text">{rec['text']}</span>
            </div>
            """)
        
        return ''.join(html)
