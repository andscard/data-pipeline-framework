"""
Executive Report Generator - Reporte Gerencial Corporativo
Diseño profesional y sobrio para stakeholders ejecutivos
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ExecutiveReportGenerator:
    """Generador de reportes ejecutivos corporativos"""
    
    def generate_report(
        self,
        monitoring_summary: Dict[str, Any],
        output_path: Optional[Path] = None
    ) -> Path:
        """Generar reporte ejecutivo HTML"""
        if not output_path:
            execution_id = monitoring_summary.get('execution_id', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pipeline_name = monitoring_summary.get('pipeline_name', 'Pipeline').replace(' ', '_')
            
            filename = f"{pipeline_name}_{timestamp}_executive.html"
            output_path = Path("reports") / filename
        
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
        
        health_status = monitoring.get('health_status', 'unknown')
        duration = monitoring.get('total_duration', 0)
        records = monitoring.get('total_records_processed', 0)
        stages = monitoring.get('stages', {})
        
        validation_stage = stages.get('VALIDATION', {})
        quality_score = validation_stage.get('quality_score', 0)
        validations_passed = validation_stage.get('validations_passed', 0)
        validations_failed = validation_stage.get('validations_failed', 0)
        
        status_class = self._get_status_class(health_status)
        quality_status = self._get_quality_status(quality_score)
        throughput = records / duration if duration > 0 else 0
        
        return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte Ejecutivo - {pipeline_name}</title>
    <style>
{self._get_css()}
    </style>
</head>
<body>
    <div class="container">
        <header class="header">
            <div class="header-left">
                <div class="logo">📊</div>
                <div>
                    <h1>Reporte de Ejecución del Pipeline</h1>
                    <p>{pipeline_name} • {start_time[:19]}</p>
                </div>
            </div>
            <div class="header-right">
                <span class="badge {status_class}">{health_status.upper()}</span>
            </div>
        </header>
        
        <section class="kpis">
            <div class="kpi">
                <div class="kpi-label">Puntaje de Calidad</div>
                <div class="kpi-value">{quality_score:.1f}%</div>
                <div class="kpi-sub {self._get_quality_class(quality_score)}">{quality_status}</div>
            </div>
            <div class="kpi">
                <div class="kpi-label">Registros</div>
                <div class="kpi-value">{records:,}</div>
                <div class="kpi-sub">{duration:.1f}s total</div>
            </div>
            <div class="kpi">
                <div class="kpi-label">Rendimiento</div>
                <div class="kpi-value">{throughput:,.0f}</div>
                <div class="kpi-sub">reg/seg</div>
            </div>
            <div class="kpi">
                <div class="kpi-label">Validaciones</div>
                <div class="kpi-value">{validations_passed}/{validations_passed + validations_failed}</div>
                <div class="kpi-sub {'error' if validations_failed > 0 else 'success'}">{validations_failed} fallidas</div>
            </div>
        </section>
        
        <section class="flow">
            <h2>Flujo del Pipeline</h2>
            {self._render_flow(stages)}
        </section>
        
        <section class="metrics">
            <h2>Métricas de Rendimiento</h2>
            {self._render_metrics_table(stages, monitoring)}
        </section>
        
        <section class="summary">
            <h2>Resumen Ejecutivo</h2>
            {self._render_summary(validation_stage, monitoring, stages)}
        </section>
        
        <footer class="footer">
            <p>Data Pipeline Framework • ID de Ejecución: {execution_id} • Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </footer>
    </div>
</body>
</html>"""
    
    def _get_css(self) -> str:
        return """
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: #f5f7fa;
            padding: 20px;
            color: #2c3e50;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        }
        
        /* Header */
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 30px 40px;
            background: #2c3e50;
            color: white;
            border-bottom: 3px solid #34495e;
        }
        
        .header-left { display: flex; align-items: center; gap: 15px; }
        .logo {
            font-size: 2em;
            width: 50px;
            height: 50px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: rgba(255,255,255,0.1);
            border-radius: 8px;
        }
        .header h1 { font-size: 1.5em; font-weight: 600; margin-bottom: 5px; }
        .header p { font-size: 0.9em; opacity: 0.85; }
        
        .badge {
            padding: 6px 16px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .badge.healthy { background: #27ae60; color: white; }
        .badge.warning { background: #f39c12; color: white; }
        .badge.critical { background: #e74c3c; color: white; }
        
        /* KPIs */
        .kpis {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1px;
            background: #e0e6ed;
            border-bottom: 1px solid #e0e6ed;
        }
        
        .kpi {
            background: white;
            padding: 25px 20px;
            text-align: center;
        }
        
        .kpi-label {
            font-size: 0.8em;
            color: #7f8c8d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
            font-weight: 600;
        }
        
        .kpi-value {
            font-size: 2em;
            font-weight: 700;
            color: #2c3e50;
            margin-bottom: 5px;
        }
        
        .kpi-sub {
            font-size: 0.85em;
            color: #95a5a6;
        }
        .kpi-sub.success { color: #27ae60; }
        .kpi-sub.error { color: #e74c3c; }
        .kpi-sub.warning { color: #f39c12; }
        
        /* Sections */
        section {
            padding: 30px 40px;
            border-bottom: 1px solid #ecf0f1;
        }
        
        h2 {
            font-size: 1.2em;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #3498db;
        }
        
        /* Flow */
        .flow-viz {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            margin: 20px 0;
        }
        
        .stage {
            flex: 1;
            background: #ecf0f1;
            padding: 20px 15px;
            border-radius: 6px;
            text-align: center;
            border-left: 4px solid #3498db;
        }
        
        .stage-icon { font-size: 1.8em; margin-bottom: 8px; }
        .stage-name {
            font-weight: 600;
            font-size: 0.85em;
            color: #2c3e50;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .stage-stats {
            display: flex;
            justify-content: space-around;
            font-size: 0.8em;
            color: #7f8c8d;
        }
        .stage-stat { text-align: center; }
        .stage-stat-value { display: block; font-weight: 700; font-size: 1.2em; color: #2c3e50; }
        
        .arrow {
            font-size: 1.5em;
            color: #95a5a6;
            flex-shrink: 0;
        }
        
        /* Metrics Table */
        .metrics-table {
            width: 100%;
            border-collapse: collapse;
        }
        
        .metrics-table th {
            background: #ecf0f1;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 0.85em;
            color: #2c3e50;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-bottom: 2px solid #bdc3c7;
        }
        
        .metrics-table td {
            padding: 12px;
            border-bottom: 1px solid #ecf0f1;
            font-size: 0.9em;
        }
        
        .metrics-table tr:hover { background: #f8f9fa; }
        
        /* Summary Cards */
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
        }
        
        .summary-card {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 6px;
            border-left: 4px solid #3498db;
        }
        
        .summary-card h3 {
            font-size: 0.95em;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 8px;
        }
        
        .summary-card p {
            font-size: 0.9em;
            line-height: 1.6;
            color: #5a6c7d;
        }
        
        .summary-card.success { border-left-color: #27ae60; }
        .summary-card.warning { border-left-color: #f39c12; }
        .summary-card.error { border-left-color: #e74c3c; }
        
        /* Footer */
        .footer {
            background: #ecf0f1;
            padding: 15px 40px;
            text-align: center;
            font-size: 0.85em;
            color: #7f8c8d;
        }
        
        @media (max-width: 768px) {
            .kpis { grid-template-columns: repeat(2, 1fr); }
            .flow-viz { flex-direction: column; }
            .arrow { display: none; }
            .summary-cards { grid-template-columns: 1fr; }
        }
        """
    
    def _render_flow(self, stages: dict) -> str:
        stage_order = ['INGESTION', 'VALIDATION', 'TRANSFORMATION', 'OUTPUT']
        icons = {'INGESTION': '📥', 'VALIDATION': '✓', 'TRANSFORMATION': '⚙️', 'OUTPUT': '📤'}
        
        html = ['<div class="flow-viz">']
        for i, name in enumerate(stage_order):
            if name not in stages:
                continue
            data = stages[name]
            duration = data.get('duration_seconds', 0)
            records = data.get('records_output', 0)
            
            html.append(f"""
            <div class="stage">
                <div class="stage-icon">{icons.get(name, '•')}</div>
                <div class="stage-name">{name}</div>
                <div class="stage-stats">
                    <div class="stage-stat">
                        <span class="stage-stat-value">{records:,}</span>
                        <span>registros</span>
                    </div>
                    <div class="stage-stat">
                        <span class="stage-stat-value">{duration:.1f}s</span>
                        <span>duración</span>
                    </div>
                </div>
            </div>
            """)
            
            if i < len([s for s in stage_order if s in stages]) - 1:
                html.append('<div class="arrow">→</div>')
        
        html.append('</div>')
        return ''.join(html)
    
    def _render_metrics_table(self, stages: dict, monitoring: dict) -> str:
        validation_stage = stages.get('VALIDATION', {})
        quality_score = validation_stage.get('quality_score', 0)
        passed = validation_stage.get('validations_passed', 0)
        failed = validation_stage.get('validations_failed', 0)
        
        duration = monitoring.get('total_duration', 0)
        records = monitoring.get('total_records_processed', 0)
        throughput = records / duration if duration > 0 else 0
        
        slowest = max(stages.items(), key=lambda x: x[1].get('duration_seconds', 0)) if stages else ('N/A', {})
        
        return f"""
        <table class="metrics-table">
            <tr>
                <th>Métrica</th>
                <th>Valor</th>
                <th>Detalles</th>
            </tr>
            <tr>
                <td><strong>Calidad de Datos</strong></td>
                <td>{quality_score:.1f}%</td>
                <td>{passed} de {passed + failed} validaciones exitosas ({failed} fallidas)</td>
            </tr>
            <tr>
                <td><strong>Velocidad de Procesamiento</strong></td>
                <td>{throughput:,.0f} reg/s</td>
                <td>Procesados {records:,} registros en {duration:.2f} segundos</td>
            </tr>
            <tr>
                <td><strong>Eficiencia del Pipeline</strong></td>
                <td>{len(stages)} etapas</td>
                <td>Todas las etapas completadas exitosamente</td>
            </tr>
            <tr>
                <td><strong>Cuello de Botella</strong></td>
                <td>{slowest[0]}</td>
                <td>La etapa más lenta tomó {slowest[1].get('duration_seconds', 0):.2f}s</td>
            </tr>
        </table>
        """
    
    def _render_summary(self, validation_stage: dict, monitoring: dict, stages: dict) -> str:
        quality_score = validation_stage.get('quality_score', 0)
        passed = validation_stage.get('validations_passed', 0)
        failed = validation_stage.get('validations_failed', 0)
        health = monitoring.get('health_status', 'unknown')
        records = monitoring.get('total_records_processed', 0)
        duration = monitoring.get('total_duration', 0)
        throughput = records / duration if duration > 0 else 0
        
        cards = []
        
        # Quality card
        if quality_score >= 90:
            cards.append({
                'class': 'success',
                'title': '✓ Calidad de Datos Excelente',
                'text': f'Se alcanzó un puntaje de calidad {quality_score:.1f}%. Todas las validaciones críticas pasaron exitosamente. Los datos cumplen los estándares de negocio.'
            })
        elif quality_score >= 75:
            cards.append({
                'class': 'warning',
                'title': '⚠ Calidad Aceptable',
                'text': f'El puntaje de calidad es {quality_score:.1f}%. {failed} validación(es) fallaron. Se recomienda revisión aunque la operación puede continuar.'
            })
        else:
            cards.append({
                'class': 'error',
                'title': '✗ Problemas de Calidad',
                'text': f'Puntaje de calidad bajo umbral ({quality_score:.1f}%). {failed} fallos detectados. Se requiere revisión inmediata.'
            })
        
        # Performance card
        if throughput > 5000:
            cards.append({
                'class': 'success',
                'title': '⚡ Alto Rendimiento',
                'text': f'Procesando a {throughput:,.0f} reg/s. El pipeline opera con eficiencia óptima.'
            })
        elif throughput > 1000:
            cards.append({
                'class': 'success',
                'title': '📊 Rendimiento Estándar',
                'text': f'Procesando a {throughput:,.0f} reg/s. Rendimiento dentro de parámetros esperados.'
            })
        else:
            cards.append({
                'class': 'warning',
                'title': '🔧 Oportunidad de Optimización',
                'text': f'Procesando a {throughput:,.0f} reg/s. Considere ajustes de rendimiento para ejecución más rápida.'
            })
        
        html = ['<div class="summary-cards">']
        for card in cards:
            html.append(f"""
            <div class="summary-card {card['class']}">
                <h3>{card['title']}</h3>
                <p>{card['text']}</p>
            </div>
            """)
        html.append('</div>')
        
        return ''.join(html)
    
    def _get_status_class(self, status: str) -> str:
        status = status.lower()
        if status == 'healthy': return 'healthy'
        elif status == 'warning': return 'warning'
        return 'critical'
    
    def _get_quality_status(self, score: float) -> str:
        if score >= 95: return 'Excelente'
        elif score >= 85: return 'Bueno'
        elif score >= 75: return 'Aceptable'
        elif score >= 60: return 'Revisión'
        return 'Crítico'
    
    def _get_quality_class(self, score: float) -> str:
        if score >= 85: return 'success'
        elif score >= 75: return 'warning'
        return 'error'
