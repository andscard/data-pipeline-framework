"""
CLI principal del framework.
"""

import click
import sys
import yaml
from pathlib import Path
from typing import Optional
import logging
import os

# Asegurar que el directorio raíz está en el path si se ejecuta directamente
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

try:
    from src.pipeline_executor import PipelineExecutor
except ImportError:
    # Fallback en caso de problemas con el path relativo
    from pipeline_executor import PipelineExecutor

# Configuración básica de logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

class WindowsSymlinkFilter(logging.Filter):
    def filter(self, record):
        return "attempting to symlink" not in record.getMessage()

logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger().addFilter(WindowsSymlinkFilter())
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
        
        click.secho("\n🛡️  INICIANDO PROCESO DE INFECCIÓN DE DATOS", fg='cyan', bold=True)
        click.echo("=" * 60)
        click.echo(f"📁 Configuración: {config_path}")
        
        # Crear infector
        infector = DataInfector.from_yaml(config_path)
        
        # Mostrar configuración
        click.echo(f"📊 Datasets: {len(infector.config.datasets)}")
        click.echo(f"⚙️  Modo: {infector.config.mode}")
        click.echo("-" * 60)
        
        for dataset in infector.config.datasets:
            click.secho(f"  📂 {dataset.name}:", fg='blue', bold=True)
            click.echo(f"      Entrada: {dataset.input_path}")
            click.echo(f"      Ataques configurados: {len(dataset.attacks)}")
        
        if dry_run:
            click.secho("\n⚠️  MODO DRY RUN ACTIVADO - No se guardarán archivos", fg='yellow', bold=True)
            
            # Mostrar plan para cada dataset
            for dataset in infector.config.datasets:
                click.secho(f"\n📋 Plan para dataset: {dataset.name}", bold=True)
                click.echo(f"  Entrada: {dataset.input_path}")
                click.echo(f"  Ataques a aplicar:")
                
                for i, attack in enumerate(dataset.attacks, 1):
                    attack_type = attack.get('type', 'unknown')
                    attack_name = attack.get('name', attack_type)
                    columns = attack.get('target_columns', attack.get('columns', []))
                    rate = attack.get('rate', infector.config.global_rate or 0.05)
                    click.echo(f"    {i}. {attack_name}")
                    click.echo(f"       Tipo: {attack_type}")
                    click.echo(f"       Columnas: {columns[:3]}{'...' if len(columns) > 3 else ''}")
                    click.echo(f"       Tasa: {rate*100:.1f}%")
            
            click.secho("\n💡 Ejecuta sin --dry-run para aplicar los cambios.", fg='green')
            return
        
        # Ejecutar infección
        click.secho("\n🚀 Ejecutando infección...", fg='cyan')
        results = infector.run()
        
        # Mostrar reporte consolidado
        click.echo("\n" + "=" * 60)
        click.secho("✅ REPORTE DE INFECCIÓN", fg='green', bold=True)
        click.echo("=" * 60)
        
        for dataset_name, report in infector.reports.items():
            click.secho(f"\n📦 Dataset: {dataset_name}", bold=True)
            click.echo(f"  Filas originales:    {report['original_rows']:,}")
            click.echo(f"  Filas infectadas:    {report['infected_rows']:,}")
            click.echo(f"  Ataques aplicados:   {report['attacks_successful']}/{report['attacks_applied']}")
            
            if report['attacks_failed'] > 0:
                click.secho(f"  ❌ Ataques fallidos:   {report['attacks_failed']}", fg='red')
            
            click.echo(f"  💾 Guardado en:")
            for output_type, location in report.get('saved_to', {}).items():
                click.echo(f"    - {output_type}: {location}")
            
            # Mostrar primeros 3 ataques exitosos
            successful_attacks = [d for d in report['details'] if d['status'] == 'success']
            if successful_attacks:
                click.echo(f"  🎯 Top ataques exitosos:")
                for detail in successful_attacks[:3]:
                    click.echo(f"    ✓ {detail.get('attack_name', detail['attack_type'])}")
                if len(successful_attacks) > 3:
                    click.echo(f"    ... y {len(successful_attacks) - 3} más")
            
            # Mostrar ataques fallidos
            failed_attacks = [d for d in report['details'] if d['status'] == 'failed']
            if failed_attacks:
                click.echo(f"  ⚠️ Ataques fallidos:")
                for detail in failed_attacks[:3]:
                    click.echo(f"    ✗ {detail.get('attack_name', detail['attack_type'])}: {detail.get('error', 'Unknown')}")
        
        click.echo("\n" + "=" * 60)
        
        report_path = Path(infector.config.output_targets.csv_path) / 'infection_report.json'
        click.echo(f"📄 Reporte completo guardado en: {report_path}")
        
        click.secho("\n👣 Próximos pasos:", bold=True)
        click.echo(f"  1. Verifica los datos infectados en: {infector.config.output_targets.csv_path}")
        click.echo(f"  2. Ejecuta el pipeline con estos datos para probar la validación")
        click.echo(f"  3. Comprueba que la etapa de validación detecte los ataques")
        
        if infector.config.output_targets.postgres_enabled:
            click.echo("\n🗄️  PostgreSQL Output:")
            click.echo(f"  Schema: {infector.config.output_targets.postgres_schema}")
            if infector.config.output_targets.postgres_create_schema:
                click.echo("  (Schema creado automáticamente)")
            for dataset_name in infector.reports.keys():
                dataset_config = next(d for d in infector.config.datasets if d.name == dataset_name)
                if dataset_config.output_postgres_table:
                    click.echo(f"  Tabla: {infector.config.output_targets.postgres_schema}.{dataset_config.output_postgres_table}")
        
    except Exception as e:
        logger.exception(f"Infection failed: {e}")
        click.secho(f"\n❌ Error crítico: {e}", fg='red', bold=True)
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
        
        click.secho(f"\n🚀 INICIANDO PIPELINE: {pipeline_name}", fg='cyan', bold=True)
        click.echo(f"📁 Configuración: {yaml_path}")
        
        if stage:
            click.secho(f"📍 MODO ETAPA ÚNICA: {stage.upper()}", fg='yellow')
        
        if dry_run:
            click.secho("⚠️  MODO DRY RUN (Simulación)", fg='yellow')
        
        click.echo("-" * 40)
        
        # Crear y ejecutar
        executor = PipelineExecutor(pipeline_name, pipeline_config)
        result = executor.execute(dry_run=dry_run, stage=stage)
        
        status_color = 'green' if result.status == "completed" else 'red'
        click.echo("\n" + "=" * 60)
        click.secho(f"🏁 EJECUCIÓN {result.status.upper()}", fg=status_color, bold=True)
        click.echo("=" * 60)
        
        click.echo(f"⏱️  Duración:  {result.duration_seconds:.2f} segundos")
        click.echo(f"📊 Registros: {result.records_processed:,}")
        
        if result.report_path:
            click.echo(f"📄 Reporte:   {result.report_path}")
            
        click.echo("\n")
        
        sys.exit(0 if result.status == "completed" else 1)
        
    except Exception as e:
        logger.exception(f"Error: {e}")
        click.secho(f"\n❌ ERROR FATAL: {e}", fg='red', bold=True)
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
        
        click.secho(f"\n📤 EXPORTANDO LOGS: {pipeline_name}", fg='cyan', bold=True)
        click.echo("=" * 60)
        click.echo(f"📁 Directorio de salida: {output_dir}")
        click.echo("-" * 60)
        
        # Crear exporter
        exporter = LogExporter(POSTGRES_CONFIG)
        exporter.connect()
        
        # Verificar que el pipeline existe
        available_pipelines = exporter.get_available_pipelines()
        
        if not available_pipelines:
            click.secho("\n⚠️  ADVERTENCIA: No se encontraron pipelines en la base de datos.", fg='yellow')
            click.echo("   Ejecuta un pipeline primero para generar datos de auditoría.\n")
            sys.exit(1)
        
        if pipeline_name not in available_pipelines:
            click.secho(f"\n❌ Pipeline '{pipeline_name}' no encontrado.", fg='red')
            click.echo("\nPipelines disponibles:")
            for p in available_pipelines:
                click.echo(f"  • {p}")
            click.echo()
            sys.exit(1)
        
        # Exportar todos los logs
        output_path = Path(output_dir)
        click.echo("⏳ Generando archivos CSV...")
        exported_files = exporter.export_all(pipeline_name, output_path)
        
        exporter.close()
        
        # Mostrar reporte
        click.echo("\n" + "=" * 60)
        click.secho("✅ EXPORTACIÓN COMPLETADA", fg='green', bold=True)
        click.echo("=" * 60)
        
        for file_type, file_path in exported_files.items():
            file_name = Path(file_path).name
            file_size = Path(file_path).stat().st_size
            
            # Contar líneas (sin header)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    rows = sum(1 for _ in f)
                    line_count = max(0, rows - 1)
            except:
                line_count = 0
            
            click.secho(f"\n📄 {file_type.replace('_', ' ').title()}", bold=True)
            click.echo(f"    Archivo:   {file_name}")
            click.echo(f"    Registros: {line_count:,}")
            click.echo(f"    Tamaño:    {file_size/1024:.1f} KB")
        
        click.secho(f"\n💾 Archivos guardados en: {output_path.absolute()}", fg='cyan')
        
        # Tips
        click.secho("\n💡 Tips de Análisis:", bold=True)
        click.echo("  1. Abre los CSV en Excel o PowerBI para visualización rápida")
        click.echo("  2. Revisa 'validation_failures.csv' para identificar patrones de errores")
        click.echo("  3. Usa 'stages_performance.csv' para encontrar cuellos de botella")
        click.echo("\n")
        
    except Exception as e:
        logger.exception(f"Export failed: {e}")
        click.secho(f"❌ Error crítico al exportar: {e}", fg='red')
        sys.exit(1)


if __name__ == '__main__':
    # Habilitar import directo como script
    if str(Path(__file__).resolve().parent) not in sys.path:
        sys.path.append(str(Path(__file__).resolve().parent.parent))
    cli()

