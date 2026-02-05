-- scripts/reset_database.sql
-- ============================================
-- Script de limpieza y reset completo de la base de datos
-- ============================================
--
-- Este script limpia TODAS las tablas y datos del framework
-- ¡CUIDADO! Esta operación es DESTRUCTIVA y NO se puede deshacer
--
-- USO:
--   docker exec -i framework_postgres psql -U admin -d pipeline_db < scripts/reset_database.sql
--
-- ============================================

\echo '============================================'
\echo 'LIMPIEZA COMPLETA DE BASE DE DATOS'
\echo '============================================'
\echo ''
\echo '⚠️  ATENCIÓN: Esta operación eliminará TODOS los datos'
\echo '⚠️  Presiona Ctrl+C en los próximos 3 segundos para cancelar'
\echo ''

-- Esperar 3 segundos para dar chance de cancelar
SELECT pg_sleep(3);

\echo '🗑️  Iniciando limpieza...'
\echo ''

-- ============================================
-- TRUNCATE: Vaciar tablas pero mantener estructura
-- ============================================

\echo '[*] Vaciando tablas (TRUNCATE)...'

-- Tablas de datos de ejemplo
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'sample_data' AND table_name = 'customers') THEN
        TRUNCATE TABLE sample_data.customers CASCADE;
        RAISE NOTICE '  ✓ sample_data.customers';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'sample_data' AND table_name = 'customers_processed') THEN
        TRUNCATE TABLE sample_data.customers_processed CASCADE;
        RAISE NOTICE '  ✓ sample_data.customers_processed';
    END IF;
    
    -- Tablas de auditoría (schema pipeline)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pipeline' AND table_name = 'validation_results') THEN
        TRUNCATE TABLE pipeline.validation_results CASCADE;
        RAISE NOTICE '  ✓ pipeline.validation_results';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pipeline' AND table_name = 'executions') THEN
        TRUNCATE TABLE pipeline.executions CASCADE;
        RAISE NOTICE '  ✓ pipeline.executions';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pipeline' AND table_name = 'audit_logs') THEN
        TRUNCATE TABLE pipeline.audit_logs CASCADE;
        RAISE NOTICE '  ✓ pipeline.audit_logs';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pipeline' AND table_name = 'pipelines') THEN
        TRUNCATE TABLE pipeline.pipelines CASCADE;
        RAISE NOTICE '  ✓ pipeline.pipelines';
    END IF;
END $$;

\echo ''
\echo '[✓] LIMPIEZA COMPLETADA'
\echo ''
\echo '📊 Estado actual:'
\echo '  • Todas las tablas están VACÍAS'
\echo '  • Las estructuras (schemas, tablas, índices) están INTACTAS'
\echo '  • Listo para generar datos frescos'
\echo ''
\echo '📝 Siguiente paso:'
\echo '  python scripts/generate_sample_data.py -c 10000 -t 50000'
\echo ''
