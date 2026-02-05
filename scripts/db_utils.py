#!/usr/bin/env python3
"""
DB Utils - Herramienta de gestión de base de datos del Data Pipeline Framework

Este script proporciona comandos organizados para administrar y monitorear
la base de datos del framework de manera eficiente.

Categorías:
  - INFO: Consultar información y estadísticas
  - QUERY: Consultas específicas sobre ejecuciones y validaciones
  - MAINTENANCE: Limpieza y mantenimiento de datos
  - ADMIN: Operaciones administrativas
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import config
from src.modules.ingestion.connectors.postgres_connector import create_postgres_connector
from sqlalchemy import text
import argparse
from datetime import datetime, timedelta
from typing import Optional


# ============================================
# UTILIDADES DE CONEXIÓN
# ============================================

def get_connection():
    """Obtener conexión a la base de datos"""
    connector = create_postgres_connector(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        database='data_framework',
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        mode="sqlalchemy"
    )
    
    if not connector.connect():
        print("\n[ERROR] No se pudo conectar a la base de datos")
        print(f"Host: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
        print(f"Database: data_framework")
        sys.exit(1)
    
    return connector


# ============================================
# CATEGORÍA: INFO - Información y estadísticas
# ============================================

def show_status():
    """Ver estado general de todas las tablas"""
    print("\n" + "="*80)
    print("  ESTADO DE LA BASE DE DATOS - data_framework")
    print("="*80)
    
    connector = get_connection()
    
    with connector.engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                schemaname,
                relname as tablename,
                n_live_tup as row_count,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as size
            FROM pg_stat_user_tables
            WHERE schemaname IN ('pipeline', 'sample_data')
            ORDER BY schemaname, relname;
        """))
        
        current_schema = None
        total_rows = 0
        
        for row in result:
            if current_schema != row.schemaname:
                if current_schema is not None:
                    print()
                current_schema = row.schemaname
                print(f"\n  Schema: {row.schemaname.upper()}")
                print("  " + "-" * 76)
            
            status = "[OK]" if row.row_count > 0 else "[EMPTY]"
            print(f"  {status:8} {row.tablename:35} {row.row_count:>10,} rows  |  {row.size:>8}")
            total_rows += row.row_count
        
        print("\n" + "="*80)
        print(f"  TOTAL: {total_rows:,} registros")
        print("="*80 + "\n")
    
    connector.close()


def show_pipelines():
    """Listar todos los pipelines registrados"""
    print("\n" + "="*80)
    print("  PIPELINES REGISTRADOS")
    print("="*80)
    
    connector = get_connection()
    
    with connector.engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                id,
                name,
                version,
                owner,
                is_active,
                run_count,
                last_run_at,
                created_at
            FROM pipeline.pipelines
            ORDER BY last_run_at DESC NULLS LAST, created_at DESC;
        """))
        
        rows = list(result)
        if not rows:
            print("\n  [INFO] No hay pipelines registrados\n")
            connector.close()
            return
        
        for row in rows:
            status = "Activo" if row.is_active else "Inactivo"
            print(f"\n  Pipeline: {row.name}")
            print(f"  " + "-" * 76)
            print(f"    ID:           {row.id}")
            print(f"    Version:      {row.version or 'N/A'}")
            print(f"    Owner:        {row.owner or 'N/A'}")
            print(f"    Estado:       {status}")
            print(f"    Ejecuciones:  {row.run_count or 0}")
            print(f"    Última ejec.: {row.last_run_at or 'Nunca'}")
            print(f"    Creado:       {row.created_at}")
        
        print("\n" + "="*80 + "\n")
    
    connector.close()


def show_executions(limit: int = 10):
    """Ver últimas ejecuciones de pipelines"""
    print("\n" + "="*80)
    print(f"  ÚLTIMAS {limit} EJECUCIONES")
    print("="*80)
    
    connector = get_connection()
    
    with connector.engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                e.id,
                p.name as pipeline_name,
                e.status,
                e.execution_type,
                e.environment,
                e.triggered_by,
                e.duration_seconds,
                e.quality_score,
                e.records_processed,
                e.start_time,
                e.end_time
            FROM pipeline.executions e
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            ORDER BY e.start_time DESC
            LIMIT {limit};
        """))
        
        rows = list(result)
        if not rows:
            print("\n  [INFO] No hay ejecuciones registradas\n")
            connector.close()
            return
        
        for row in rows:
            status_icon = {
                'completed': '[OK]',
                'failed': '[FAIL]',
                'running': '[RUN]',
                'pending': '[PEND]'
            }.get(row.status, '[?]')
            
            duration = f"{row.duration_seconds}s" if row.duration_seconds else "N/A"
            quality = f"{row.quality_score}/100" if row.quality_score else "N/A"
            
            print(f"\n  {status_icon} {row.pipeline_name}")
            print(f"  " + "-" * 76)
            print(f"    ID:           {row.id}")
            print(f"    Tipo:         {row.execution_type or 'N/A'}")
            print(f"    Ambiente:     {row.environment or 'N/A'}")
            print(f"    Ejecutado por: {row.triggered_by or 'N/A'}")
            print(f"    Duración:     {duration}")
            print(f"    Calidad:      {quality}")
            print(f"    Registros:    {row.records_processed or 0:,}")
            print(f"    Inicio:       {row.start_time}")
            print(f"    Fin:          {row.end_time or 'En progreso'}")
        
        print("\n" + "="*80 + "\n")
    
    connector.close()


def show_validations(status: Optional[str] = None, limit: int = 20):
    """Ver resultados de validaciones"""
    # Convertir status a filtro booleano para la columna 'passed'
    if status == 'passed':
        status_filter = "AND v.passed = true"
    elif status == 'failed':
        status_filter = "AND v.passed = false"
    else:
        status_filter = ""
    
    status_label = f" ({status.upper()})" if status else ""
    
    print("\n" + "="*80)
    print(f"  VALIDACIONES{status_label} - Últimas {limit}")
    print("="*80)
    
    connector = get_connection()
    
    with connector.engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                v.id,
                p.name as pipeline_name,
                v.rule_name as validation_name,
                CASE WHEN v.passed THEN 'passed' ELSE 'failed' END as status,
                v.severity,
                v.dataset_name,
                v.expectation_type,
                v.total_records,
                v.failed_count as failed_records,
                v.timestamp as created_at
            FROM pipeline.validation_results v
            JOIN pipeline.executions e ON v.execution_id = e.id
            JOIN pipeline.pipelines p ON e.pipeline_id = p.id
            WHERE 1=1 {status_filter}
            ORDER BY v.timestamp DESC
            LIMIT {limit};
        """))
        
        rows = list(result)
        if not rows:
            print(f"\n  [INFO] No hay validaciones{status_label.lower()} registradas\n")
            connector.close()
            return
        
        # Agrupar por pipeline
        from collections import defaultdict
        by_pipeline = defaultdict(list)
        for row in rows:
            by_pipeline[row.pipeline_name].append(row)
        
        for pipeline_name, validations in by_pipeline.items():
            print(f"\n  Pipeline: {pipeline_name}")
            print(f"  " + "-" * 76)
            
            for v in validations:
                status_icon = "[OK]" if v.status == "passed" else "[FAIL]"
                
                severity_label = f"[{v.severity.upper()}]" if v.severity else ""
                failed_pct = (v.failed_records / v.total_records * 100) if v.total_records and v.failed_records else 0
                
                print(f"    {status_icon} {severity_label:10} {v.validation_name}")
                print(f"        Dataset: {v.dataset_name or 'N/A'} | Tipo: {v.expectation_type or 'N/A'}")
                print(f"        Registros: {v.total_records or 0:,} | Fallidos: {v.failed_records or 0} ({failed_pct:.1f}%)")
                print(f"        Fecha: {v.created_at}")
                print()
        
        print("="*80 + "\n")
    
    connector.close()


def show_stats():
    """Mostrar estadísticas generales del framework"""
    print("\n" + "="*80)
    print("  ESTADÍSTICAS DEL FRAMEWORK")
    print("="*80)
    
    connector = get_connection()
    
    with connector.engine.connect() as conn:
        # Estadísticas de pipelines
        stats = conn.execute(text("""
            SELECT 
                COUNT(DISTINCT p.id) as total_pipelines,
                COUNT(e.id) as total_executions,
                COUNT(CASE WHEN e.status = 'completed' THEN 1 END) as successful_executions,
                COUNT(CASE WHEN e.status = 'failed' THEN 1 END) as failed_executions,
                COUNT(v.id) as total_validations,
                COUNT(CASE WHEN v.passed = false THEN 1 END) as failed_validations,
                AVG(e.duration_seconds) as avg_duration,
                AVG(e.quality_score) as avg_quality
            FROM pipeline.pipelines p
            LEFT JOIN pipeline.executions e ON p.id = e.pipeline_id
            LEFT JOIN pipeline.validation_results v ON e.id = v.execution_id;
        """)).fetchone()
        
        # Ejecuciones por día (últimos 7 días)
        daily_stats = conn.execute(text("""
            SELECT 
                DATE(start_time) as date,
                COUNT(*) as executions,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful
            FROM pipeline.executions
            WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(start_time)
            ORDER BY date DESC;
        """)).fetchall()
        
        print("\n  GENERAL")
        print("  " + "-" * 76)
        print(f"    Pipelines registrados:     {stats.total_pipelines}")
        print(f"    Total ejecuciones:         {stats.total_executions}")
        print(f"    - Exitosas:                {stats.successful_executions}")
        print(f"    - Fallidas:                {stats.failed_executions}")
        success_rate = (stats.successful_executions / stats.total_executions * 100) if stats.total_executions else 0
        print(f"    - Tasa de éxito:           {success_rate:.1f}%")
        
        print(f"\n    Total validaciones:        {stats.total_validations}")
        print(f"    - Fallidas:                {stats.failed_validations}")
        
        if stats.avg_duration:
            print(f"\n    Duración promedio:         {stats.avg_duration:.1f}s")
        if stats.avg_quality:
            print(f"    Calidad promedio:          {stats.avg_quality:.1f}/100")
        
        if daily_stats:
            print(f"\n  ACTIVIDAD (Últimos 7 días)")
            print("  " + "-" * 76)
            for day in daily_stats:
                print(f"    {day.date}:  {day.executions} ejecuciones ({day.successful} exitosas)")
        
        print("\n" + "="*80 + "\n")
    
    connector.close()


# ============================================
# CATEGORÍA: MAINTENANCE - Limpieza y mantenimiento
# ============================================

def clean_sample_data():
    """Limpiar solo datos de ejemplo (sample_data schema)"""
    print("\n" + "="*80)
    print("  LIMPIEZA DE DATOS DE EJEMPLO")
    print("="*80)
    print("\n  [INFO] Esto eliminará solo sample_data.* (datos sintéticos)")
    print("  [INFO] Las tablas pipeline.* NO se tocarán\n")
    
    response = input("  ¿Continuar? (y/n): ").strip().lower()
    if response not in ['y', 'yes', 's', 'si']:
        print("\n  [CANCEL] Operación cancelada\n")
        return
    
    connector = get_connection()
    
    print("\n  Limpiando datos...")
    with connector.engine.begin() as conn:
        # Obtener todas las tablas en sample_data
        tables = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='sample_data'
        """)).fetchall()
        
        for table in tables:
            table_name = table[0]
            conn.execute(text(f"TRUNCATE TABLE sample_data.{table_name} CASCADE;"))
            print(f"  [OK] sample_data.{table_name} limpiada")
    
    print("\n  [SUCCESS] Limpieza completada")
    print("  [TIP] Regenera datos: python scripts/generate_sample_data.py -c 10000 -t 50000\n")
    
    connector.close()


def clean_old_executions(days: int = 30):
    """Limpiar ejecuciones antiguas (mantener auditoría reciente)"""
    print("\n" + "="*80)
    print(f"  LIMPIEZA DE EJECUCIONES ANTIGUAS (>{days} días)")
    print("="*80)
    
    connector = get_connection()
    
    with connector.engine.connect() as conn:
        # Contar ejecuciones a eliminar
        count = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM pipeline.executions 
            WHERE start_time < CURRENT_DATE - INTERVAL '{days} days'
        """)).scalar()
        
        if count == 0:
            print(f"\n  [INFO] No hay ejecuciones mayores a {days} días\n")
            connector.close()
            return
        
        print(f"\n  [INFO] Se eliminarán {count} ejecuciones antiguas")
        print(f"  [INFO] Los registros de validaciones asociados también se eliminarán\n")
        
        response = input("  ¿Continuar? (y/n): ").strip().lower()
        if response not in ['y', 'yes', 's', 'si']:
            print("\n  [CANCEL] Operación cancelada\n")
            connector.close()
            return
    
    with connector.engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM pipeline.executions 
            WHERE start_time < CURRENT_DATE - INTERVAL '{days} days'
        """))
    
    print(f"\n  [SUCCESS] {count} ejecuciones eliminadas")
    print(f"  [INFO] Las ejecuciones recientes (<{days} días) se mantienen intactas\n")
    
    connector.close()


def vacuum_database():
    """Ejecutar VACUUM ANALYZE para optimizar la base de datos"""
    print("\n" + "="*80)
    print("  OPTIMIZACIÓN DE BASE DE DATOS")
    print("="*80)
    print("\n  [INFO] Ejecutando VACUUM ANALYZE en todas las tablas...")
    print("  [INFO] Esto puede tardar unos momentos...\n")
    
    connector = get_connection()
    
    # VACUUM requiere autocommit
    with connector.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        schemas = ['pipeline', 'sample_data']
        
        for schema in schemas:
            tables = conn.execute(text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema='{schema}'
            """)).fetchall()
            
            print(f"  Schema: {schema}")
            for table in tables:
                table_name = table[0]
                conn.execute(text(f"VACUUM ANALYZE {schema}.{table_name}"))
                print(f"    [OK] {schema}.{table_name}")
            print()
    
    print("  [SUCCESS] Optimización completada\n")
    connector.close()


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    parser = argparse.ArgumentParser(
        description='DB Utils - Herramienta de gestión de base de datos',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
CATEGORÍAS DE COMANDOS:

INFO - Consultar información:
  status              Ver estado de todas las tablas
  pipelines           Listar pipelines registrados
  executions          Ver últimas ejecuciones (default: 10)
  validations         Ver resultados de validaciones
  stats               Estadísticas generales del framework

MAINTENANCE - Limpieza y mantenimiento:
  clean-samples       Limpiar datos de ejemplo (sample_data schema)
  clean-old           Limpiar ejecuciones antiguas (default: >30 días)
  vacuum              Optimizar base de datos (VACUUM ANALYZE)

EJEMPLOS:
  %(prog)s status                    # Ver estado general
  %(prog)s executions -l 20          # Ver últimas 20 ejecuciones
  %(prog)s validations -s failed     # Ver solo validaciones fallidas
  %(prog)s clean-old -d 60           # Limpiar ejecuciones >60 días
  %(prog)s stats                     # Ver estadísticas completas
        ''')
    
    parser.add_argument('command', 
                        choices=['status', 'pipelines', 'executions', 'validations', 
                                'stats', 'clean-samples', 'clean-old', 'vacuum'],
                        help='Comando a ejecutar')
    
    parser.add_argument('-l', '--limit', type=int, default=10,
                        help='Límite de registros a mostrar (default: 10)')
    
    parser.add_argument('-s', '--status', 
                        choices=['passed', 'failed'],
                        help='Filtrar validaciones por estado')
    
    parser.add_argument('-d', '--days', type=int, default=30,
                        help='Días de retención para clean-old (default: 30)')
    
    args = parser.parse_args()
    
    try:
        # INFO commands
        if args.command == 'status':
            show_status()
        elif args.command == 'pipelines':
            show_pipelines()
        elif args.command == 'executions':
            show_executions(limit=args.limit)
        elif args.command == 'validations':
            show_validations(status=args.status, limit=args.limit)
        elif args.command == 'stats':
            show_stats()
        
        # MAINTENANCE commands
        elif args.command == 'clean-samples':
            clean_sample_data()
        elif args.command == 'clean-old':
            clean_old_executions(days=args.days)
        elif args.command == 'vacuum':
            vacuum_database()
            
    except KeyboardInterrupt:
        print("\n\n  [CANCEL] Operación cancelada por el usuario\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n  [ERROR] {str(e)}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
