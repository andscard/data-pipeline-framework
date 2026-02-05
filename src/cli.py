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
        click.echo(f"Input:  {infector.config.input_path}")
        click.echo(f"Output: {infector.config.output_path}")
        click.echo(f"Attacks: {len(infector.config.attacks)}")
        click.echo()
        
        if dry_run:
            click.echo("[DRY RUN MODE - No files will be saved]")
            click.echo()
            
            # Solo cargar y mostrar plan
            df = infector.load_data()
            click.echo(f"Data loaded: {len(df)} rows, {len(df.columns)} columns")
            click.echo()
            click.echo("Attacks to apply:")
            for i, attack in enumerate(infector.config.attacks, 1):
                attack_type = attack.get('type', 'unknown')
                columns = attack.get('columns', [])
                rate = attack.get('rate', infector.config.global_rate or 0.05)
                click.echo(f"  {i}. {attack_type}")
                click.echo(f"     Columns: {columns}")
                click.echo(f"     Rate: {rate*100:.1f}%")
            
            click.echo()
            click.echo("Run without --dry-run to execute infection")
            return
        
        # Ejecutar infección
        click.echo("Starting infection process...")
        infected_df = infector.run()
        
        # Mostrar reporte
        report = infector.generate_report()
        click.echo()
        click.echo("INFECTION REPORT")
        click.echo(f"Original rows:    {report['original_rows']:,}")
        click.echo(f"Infected rows:    {report['infected_rows']:,}")
        click.echo(f"Attacks applied:  {report['attacks_successful']}/{report['attacks_applied']}")
        
        if report['attacks_failed'] > 0:
            click.echo(f"Attacks failed:   {report['attacks_failed']}")
        
        click.echo()
        click.echo("Attack details:")
        for detail in report['details']:
            status_icon = "✓" if detail['status'] == 'success' else "✗"
            click.echo(f"  {status_icon} {detail['attack_type']}")
            click.echo(f"    Columns: {detail['columns']}")
            click.echo(f"    Rate: {detail['rate']*100:.1f}%")
            if detail['status'] == 'failed':
                click.echo(f"    Error: {detail.get('error', 'Unknown')}")
        
        click.echo()
        click.echo(f"Infected data saved: {infector.config.output_path}")
        
        report_path = Path(infector.config.output_path).parent / 'infection_report.json'
        click.echo(f"Report saved: {report_path}")
        click.echo()
        
        click.echo("Next steps:")
        click.echo(f"  1. Verify infected data: {infector.config.output_path}")
        click.echo(f"  2. Run pipeline with infected data")
        click.echo(f"  3. Check validation stage detects all attacks")
        
    except Exception as e:
        logger.exception(f"Infection failed: {e}")
        click.echo(f"Error: {e}")
        sys.exit(1)


@run.command(name='pipeline')
@click.option('-c', '--config', 'config_path', required=True,
              type=click.Path(exists=True),
              help='Ruta al archivo YAML de configuración')
@click.option('-n', '--name', help='Nombre del pipeline (opcional)')
@click.option('--dry-run', is_flag=True, help='Simular ejecución')
def run_pipeline(config_path: str, name: Optional[str], dry_run: bool):
    """Ejecutar pipeline desde archivo YAML."""
    try:
        # Cargar configuración
        yaml_path = Path(config_path)
        with open(yaml_path, 'r', encoding='utf-8') as f:
            pipeline_config = yaml.safe_load(f)
        
        # Obtener nombre del pipeline
        pipeline_name = name or pipeline_config.get('pipeline', {}).get('name', yaml_path.stem)
        
        click.echo(f"Executing pipeline: {pipeline_name}")
        click.echo(f"Config: {yaml_path}")
        
        if dry_run:
            click.echo("(Dry run mode)")
        
        # Crear y ejecutar
        executor = PipelineExecutor(pipeline_name, pipeline_config)
        result = executor.execute(dry_run=dry_run)
        
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


if __name__ == '__main__':
    cli()
