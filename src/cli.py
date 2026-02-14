"""
CLI principal del framework.
"""

import click
import sys
import yaml
from pathlib import Path
from typing import Optional
import logging

from src.pipeline_executor import PipelineExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version='2.0.0')
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
    
    Flujo:
      1. Lee datos limpios (input en YAML)
      2. Aplica ataques configurados
      3. Guarda datos infectados (output en YAML)
      4. Genera reporte de infección
    
    Ejemplo:
      $ data-pipeline-framework infect -c examples/infection_config.yml
    """
    try:
        from src.modules.data_infection import DataInfector
        
        click.echo(f"Loading infection config: {config_path}")
        click.echo()
        
        # Crear infector
        infector = DataInfector.from_yaml(config_path)
        
        # Mostrar configuración
        click.echo(f"Datasets: {len(infector.config.datasets)}")
        click.echo(f"Mode: {infector.config.mode}")
        click.echo()
        
        for dataset in infector.config.datasets:
            click.echo(f"  - {dataset.name}:")
            click.echo(f"      Input:   {dataset.input_path}")
            click.echo(f"      Attacks: {len(dataset.attacks)}")
        click.echo()
        
        if dry_run:
            click.echo("[DRY RUN MODE - No files will be saved]")
            click.echo()
            
            # Mostrar plan para cada dataset
            for dataset in infector.config.datasets:
                click.echo(f"Dataset: {dataset.name}")
                click.echo(f"  Input: {dataset.input_path}")
                click.echo(f"  Attacks to apply:")
                
                for i, attack in enumerate(dataset.attacks, 1):
                    attack_type = attack.get('type', 'unknown')
                    attack_name = attack.get('name', attack_type)
                    columns = attack.get('target_columns', attack.get('columns', []))
                    rate = attack.get('rate', infector.config.global_rate or 0.05)
                    click.echo(f"    {i}. {attack_name}")
                    click.echo(f"       Type: {attack_type}")
                    click.echo(f"       Columns: {columns[:3]}{'...' if len(columns) > 3 else ''}")
                    click.echo(f"       Rate: {rate*100:.1f}%")
                click.echo()
            
            click.echo("Run without --dry-run to execute infection")
            return
        
        # Ejecutar infección
        click.echo("Starting infection process...")
        results = infector.run()
        
        # Mostrar reporte consolidado
        click.echo()
        click.echo("=" * 80)
        click.echo("INFECTION REPORT")
        click.echo("=" * 80)
        
        for dataset_name, report in infector.reports.items():
            click.echo()
            click.echo(f"Dataset: {dataset_name}")
            click.echo(f"  Original rows:    {report['original_rows']:,}")
            click.echo(f"  Infected rows:    {report['infected_rows']:,}")
            click.echo(f"  Attacks applied:  {report['attacks_successful']}/{report['attacks_applied']}")
            
            if report['attacks_failed'] > 0:
                click.echo(f"  Attacks failed:   {report['attacks_failed']}")
            
            click.echo(f"  Saved to:")
            for output_type, location in report.get('saved_to', {}).items():
                click.echo(f"    - {output_type}: {location}")
            
            # Mostrar primeros 3 ataques exitosos
            successful_attacks = [d for d in report['details'] if d['status'] == 'success']
            if successful_attacks:
                click.echo(f"  Top attacks:")
                for detail in successful_attacks[:3]:
                    click.echo(f"    [OK] {detail.get('attack_name', detail['attack_type'])}")
                if len(successful_attacks) > 3:
                    click.echo(f"    ... and {len(successful_attacks) - 3} more")
            
            # Mostrar ataques fallidos
            failed_attacks = [d for d in report['details'] if d['status'] == 'failed']
            if failed_attacks:
                click.echo(f"  Failed attacks:")
                for detail in failed_attacks[:3]:
                    click.echo(f"    [FAIL] {detail.get('attack_name', detail['attack_type'])}: {detail.get('error', 'Unknown')}")
        
        click.echo()
        click.echo("=" * 80)
        
        report_path = Path(infector.config.output_targets.csv_path) / 'infection_report.json'
        click.echo(f"Report saved: {report_path}")
        click.echo()
        
        click.echo("Next steps:")
        click.echo(f"  1. Verify infected data in: {infector.config.output_targets.csv_path}")
        click.echo(f"  2. Run pipeline with infected data to test validation")
        click.echo(f"  3. Check validation stage detects all attacks")
        
        if infector.config.output_targets.postgres_enabled:
            click.echo()
            click.echo("PostgreSQL Output:")
            click.echo(f"  Schema: {infector.config.output_targets.postgres_schema}")
            if infector.config.output_targets.postgres_create_schema:
                click.echo("  Schema created automatically if not exists")
            for dataset_name in infector.reports.keys():
                dataset_config = next(d for d in infector.config.datasets if d.name == dataset_name)
                if dataset_config.output_postgres_table:
                    click.echo(f"  Table: {infector.config.output_targets.postgres_schema}.{dataset_config.output_postgres_table}")
        
    except Exception as e:
        logger.exception(f"Infection failed: {e}")
        click.echo(f"Error: {e}")
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
    """Ejecutar pipeline desde archivo YAML.
    
    Puede ejecutar el pipeline completo o etapas individuales:
    - ingestion: Cargar datos desde fuentes
    - validation: Validar esquema y calidad
    - transformation: Aplicar transformaciones
    - output: Escribir resultados
    """
    try:
        # Cargar configuración
        yaml_path = Path(config_path)
        with open(yaml_path, 'r', encoding='utf-8') as f:
            pipeline_config = yaml.safe_load(f)
        
        # Obtener nombre del pipeline
        pipeline_name = name or pipeline_config.get('pipeline', {}).get('name', yaml_path.stem)
        
        click.echo(f"Executing pipeline: {pipeline_name}")
        click.echo(f"Config: {yaml_path}")
        
        if stage:
            click.echo(f"Stage: {stage} (individual execution)")
        
        if dry_run:
            click.echo("(Dry run mode)")
        
        # Crear y ejecutar
        executor = PipelineExecutor(pipeline_name, pipeline_config)
        result = executor.execute(dry_run=dry_run, stage=stage)
        
        click.echo(f"\nExecution completed")
        click.echo(f"  Status: {result.status}")
        click.echo(f"  Duration: {result.duration_seconds:.2f}s")
        click.echo(f"  Records: {result.records_processed}")
        
        if result.report_path:
            click.echo(f"  Report: {result.report_path}")
        
        sys.exit(0 if result.status == "completed" else 1)
        
    except Exception as e:
        logger.exception(f"Error: {e}")
        click.echo(f"Error: {e}")
        sys.exit(1)


@cli.command()
@click.option('-n', '--name', 'pipeline_name', required=True,
              help='Nombre del pipeline a exportar')
@click.option('-o', '--output', 'output_dir', 
              type=click.Path(),
              default='logs',
              help='Directorio de salida (default: logs/)')
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
        from src.modules.auditing.log_exporter import LogExporter
        from src.modules.ingestion.config import POSTGRES_CONFIG
        
        click.echo(f"Exporting logs for pipeline: {pipeline_name}")
        click.echo(f"Output directory: {output_dir}")
        click.echo()
        
        # Crear exporter
        exporter = LogExporter(POSTGRES_CONFIG)
        exporter.connect()
        
        # Verificar que el pipeline existe
        available_pipelines = exporter.get_available_pipelines()
        
        if not available_pipelines:
            click.echo("⚠️  No pipelines found in database")
            click.echo("Run a pipeline first to generate audit data")
            sys.exit(1)
        
        if pipeline_name not in available_pipelines:
            click.echo(f"❌ Pipeline '{pipeline_name}' not found")
            click.echo()
            click.echo("Available pipelines:")
            for p in available_pipelines:
                click.echo(f"  - {p}")
            sys.exit(1)
        
        # Exportar todos los logs
        output_path = Path(output_dir)
        click.echo("Exporting data...")
        exported_files = exporter.export_all(pipeline_name, output_path)
        
        exporter.close()
        
        # Mostrar reporte
        click.echo()
        click.echo("=" * 80)
        click.echo("EXPORT COMPLETED")
        click.echo("=" * 80)
        click.echo()
        
        for file_type, file_path in exported_files.items():
            file_name = Path(file_path).name
            file_size = Path(file_path).stat().st_size
            
            # Contar líneas (sin header)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f) - 1  # -1 para header
            except:
                line_count = 0
            
            click.echo(f"✓ {file_type}:")
            click.echo(f"    File: {file_name}")
            click.echo(f"    Rows: {line_count:,}")
            click.echo(f"    Size: {file_size:,} bytes")
            click.echo()
        
        click.echo(f"All files saved in: {output_path.absolute()}")
        click.echo()
        
        # Tips
        click.echo("Next steps:")
        click.echo("  1. Open CSV files with Excel, Google Sheets, or pandas")
        click.echo("  2. Analyze validation failures to identify quality issues")
        click.echo("  3. Review stages performance to optimize slow stages")
        click.echo("  4. Check timeline to understand execution flow")
        
    except Exception as e:
        logger.exception(f"Export failed: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    cli()

