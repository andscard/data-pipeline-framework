"""
CLI principal del framework.
"""

import click
import sys
import yaml
from pathlib import Path
from typing import Optional
import logging
from src.modules.auditing.log_exporter import LogExporter
from src.modules.ingestion.config import POSTGRES_CONFIG
from src.config import Config
from datetime import datetime
from src.pipeline_executor import PipelineExecutor

logger = logging.getLogger(__name__)

@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Data Pipeline Framework."""
    pass


@cli.group()
def run():
    """Ejecutar pipelines."""
    pass


@cli.command()
@click.option('-c', '--config', 'config_path', required=True, 
              type=click.Path(exists=True), 
              help='Ruta al archivo de configuración YAML de infección')
@click.option('--dry-run', is_flag=True, 
              help='Simular infección sin guardar resultados')
def infect(config_path: str, dry_run: bool):
    """
    Infectar datos ANTES del pipeline.
    
    Este comando aplica ataques configurados a datos limpios para simular
    escenarios de seguridad basados en OWASP Top 10.
    """
    try:
        from src.modules.data_infection import DataInfector
        
        click.secho(f"Iniciando infección con config: {Path(config_path).name}", fg='cyan')
        
        # Crear infector
        infector = DataInfector.from_yaml(config_path)
        
        click.echo(f"Datasets: {len(infector.config.datasets)} | Modo: {infector.config.mode}")
        
        if dry_run:
            click.secho("\nMODO DRY RUN ACTIVADO - No se guardarán archivos", fg='yellow')
            # Mostrar plan resumido
            for dataset in infector.config.datasets:
                click.echo(f"- {dataset.name}: {len(dataset.attacks)} ataques configurados")
            return
        
        # Ejecutar infección
        infector.run()
        
        # Mostrar reporte consolidado
        click.secho("\nREPORTE DE INFECCIÓN", fg='green', bold=True)
        
        for dataset_name, report in infector.reports.items():
            click.echo(f"Dataset: {dataset_name}")
            click.echo(f"  Rows: {report['original_rows']:,} -> {report['infected_rows']:,}")
            click.echo(f"  Ataques: {report['attacks_successful']}/{report['attacks_applied']} exitosos")
            
            if report['attacks_failed'] > 0:
                click.secho(f"    Fallidos: {report['attacks_failed']}", fg='red')
                # Mostrar detalles solo de fallidos
                failed_attacks = [d for d in report['details'] if d['status'] == 'failed']
                for detail in failed_attacks[:3]:
                    click.echo(f"    ✗ {detail.get('attack_name', detail['attack_type'])}: {detail.get('error', 'Unknown')}")

        report_path = Path(infector.config.output_targets.csv_path) / 'infection_report.json'
        click.echo(f"\nReporte completo: {report_path}")
        click.echo(f"Datos infectados en: {infector.config.output_targets.csv_path}")
        
    except Exception as e:
        logger.exception(f"Infection failed: {e}")
        click.secho(f"\nError crítico: {e}", fg='red', bold=True)
        sys.exit(1)


@run.command(name='pipeline')
@click.option('-c', '--config', 'config_path', required=True,
              type=click.Path(exists=True),
              help='Ruta al archivo YAML de configuración')
@click.option('-n', '--name', help='Nombre del pipeline (opcional)')
@click.option('--stage', type=click.Choice(['ingestion', 'validation', 'transformation', 'output']),
              help='Ejecutar solo una etapa específica (requiere estado previo)')
@click.option('--dry-run', is_flag=True, help='Simular ejecución')
def run_pipeline(config_path: str, name: Optional[str], stage: Optional[str], dry_run: bool):
    """Ejecutar pipeline desde archivo YAML."""
    try:
        # Cargar configuración
        yaml_path = Path(config_path)
        with open(yaml_path, 'r', encoding='utf-8') as f:
            pipeline_config = yaml.safe_load(f)
        
        # Obtener nombre del pipeline
        pipeline_name = name or pipeline_config.get('pipeline', {}).get('name', yaml_path.stem)
        
        click.secho(f"Pipeline: {pipeline_name} | Config: {yaml_path.name}", fg='cyan')
        
        if stage:
            click.secho(f"Stage único: {stage.upper()}", fg='yellow')
        
        if dry_run:
            click.secho("Modo Dry Run", fg='yellow')
        
        # Crear y ejecutar
        executor = PipelineExecutor(pipeline_name, pipeline_config)
        result = executor.execute(dry_run=dry_run, stage=stage)
        
        status_color = 'green' if result.status == "completed" else 'red'
        click.secho(f"Estado: {result.status.upper()}", fg=status_color)
        
        click.echo(f"Duración: {result.duration_seconds:.2f}s | Registros: {result.records_processed:,}")
        
        if result.report_path:
            click.echo(f"Reporte: {result.report_path}")
            
        sys.exit(0 if result.status == "completed" else 1)
        
    except Exception as e:
        logger.exception(f"Error: {e}")
        click.secho(f"\nError crítico: {e}", fg='red', bold=True)
        sys.exit(1)


@cli.command()
@click.option('-n', '--name', 'pipeline_name', required=True,
              help='Nombre del pipeline a exportar')
@click.option('-o', '--output', 'output_dir', 
              type=click.Path(),
              help='Directorio de salida (default: artifacts/<pipeline_name>/manual_exports/<timestamp>)')
def export_logs(pipeline_name: str, output_dir: str):
    """
    Exportar logs y métricas del pipeline a archivos CSV.
    
    Genera múltiples archivos CSV con información útil cruzando
    todas las estructuras de auditoría:
    
      - executions_summary: Resumen de todas las ejecuciones
      - stages_performance: Performance detallada por stage
      - validation_quality: Métricas de calidad por suite
      - validation_failures: Detalle de validaciones fallidas
      - timeline: Timeline de eventos del pipeline
      - errors_analysis: Análisis de errores y warnings
    
    Ejemplo:
      $ data-framework export-logs -n CustomerTransactionPipeline
      $ data-framework export-logs -n MyPipeline -o exports/
    """
    try:     
        # Determinar directorio de salida por defecto si no se especifica
        if not output_dir:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = str(Config.ARTIFACTS_DIR / pipeline_name / "exports" / timestamp)

        click.secho(f"Exportando logs de '{pipeline_name}' a {output_dir}", fg='cyan')
        
        # Crear exporter
        exporter = LogExporter(POSTGRES_CONFIG)
        exporter.connect()
        
        # Verificar que el pipeline existe
        available_pipelines = exporter.get_available_pipelines()
        
        if not available_pipelines:
            click.secho("No se encontraron pipelines en la base de datos.", fg='yellow')
            sys.exit(1)
        
        if pipeline_name not in available_pipelines:
            click.secho(f"Pipeline '{pipeline_name}' no encontrado. Disponibles: {', '.join(available_pipelines)}", fg='red')
            sys.exit(1)
        
        # Exportar todos los logs
        output_path = Path(output_dir)
        exported_files = exporter.export_all(pipeline_name, output_path)
        
        exporter.close()
        
        click.secho("Exportación completada.", fg='green')
        
        for file_type, file_path in exported_files.items():
            file_name = Path(file_path).name
            click.echo(f" - {file_name}")
        
    except Exception as e:
        logger.exception(f"Export failed: {e}")
        click.secho(f"Error crítico al exportar: {e}", fg='red')
        sys.exit(1)

if __name__ == '__main__':
    cli()