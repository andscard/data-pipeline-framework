-- scripts/init_db.sql
-- ============================================
-- Script de inicialización de base de datos
-- ============================================
-- 
-- Este script es IDEMPOTENTE y puede ejecutarse múltiples veces sin errores.
-- Utiliza CREATE IF NOT EXISTS para evitar fallos en re-ejecuciones.
--
-- USO:
--
-- 1. Con Docker (recomendado para entorno local):
--    docker exec -i framework_postgres psql -U admin -d pipeline_db < scripts/init_db.sql
--    
--    ¿Qué hace Docker aquí?
--    - 'docker exec': Ejecuta un comando dentro de un contenedor corriendo
--    - '-i': Modo interactivo (permite pasar input al comando)
--    - 'framework_postgres': Nombre del contenedor de PostgreSQL
--    - 'psql': Cliente de PostgreSQL
--    - '-U admin': Usuario de PostgreSQL
--    - '-d pipeline_db': Base de datos destino
--    - '< scripts/init_db.sql': Redirige el contenido del script como input
--    
--    Ventajas de usar Docker:
--    ✓ Aislamiento: No interfiere con PostgreSQL local
--    ✓ Reproducible: Mismo entorno en todos los desarrolladores
--    ✓ Fácil limpieza: docker-compose down -v elimina todo
--    ✓ Múltiples versiones: Puedes tener diferentes versiones de PostgreSQL
--
-- 2. Sin Docker (PostgreSQL local):
--    psql -U admin -d pipeline_db -f scripts/init_db.sql
--    
--    Nota: Requiere que PostgreSQL esté instalado localmente
--
-- 3. Desde Python (usado en setup_environment.py):
--    Se ejecuta usando SQLAlchemy, statement por statement
--
-- ============================================

-- Configurar comportamiento en caso de errores
\set ON_ERROR_STOP off

-- Mostrar mensajes informativos
\echo '============================================'
\echo 'Inicializando base de datos...'
\echo '============================================'

-- ============================================
-- Crear extensiones necesarias
-- ============================================

\echo ''
\echo '[*] Creando extensiones...'

-- UUID para generar identificadores únicos
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
\echo '  ✓ uuid-ossp'

-- Funciones de texto completo (full-text search)
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
\echo '  ✓ pg_trgm'

-- Funciones criptográficas
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
\echo '  ✓ pgcrypto'

-- ============================================
-- Crear esquemas
-- ============================================

\echo ''
\echo '[*] Creando esquemas...'

CREATE SCHEMA IF NOT EXISTS pipeline;
\echo '  ✓ pipeline'

CREATE SCHEMA IF NOT EXISTS security;
\echo '  ✓ security'

CREATE SCHEMA IF NOT EXISTS monitoring;
\echo '  ✓ monitoring'

-- ============================================
-- Tabla: pipeline.pipelines
-- Configuraciones de pipelines
-- ============================================

\echo ''
\echo '[*] Creando tabla pipeline.pipelines...'

CREATE TABLE IF NOT EXISTS pipeline.pipelines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    config JSONB NOT NULL DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pipelines_name_not_empty CHECK (length(trim(name)) > 0)
);

-- Índices para búsqueda rápida
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pipelines_name') THEN
        CREATE INDEX idx_pipelines_name ON pipeline.pipelines(name);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pipelines_is_active') THEN
        CREATE INDEX idx_pipelines_is_active ON pipeline.pipelines(is_active);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pipelines_created_at') THEN
        CREATE INDEX idx_pipelines_created_at ON pipeline.pipelines(created_at DESC);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Tabla: pipeline.executions
-- Registro de ejecuciones de pipelines
-- ============================================

\echo ''
\echo '[*] Creando tabla pipeline.executions...'

CREATE TABLE IF NOT EXISTS pipeline.executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES pipeline.pipelines(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metrics JSONB DEFAULT '{}',
    
    CONSTRAINT executions_status_valid CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
    ),
    CONSTRAINT executions_end_after_start CHECK (
        end_time IS NULL OR end_time >= start_time
    )
);

-- Índices
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_pipeline_id') THEN
        CREATE INDEX idx_executions_pipeline_id ON pipeline.executions(pipeline_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_status') THEN
        CREATE INDEX idx_executions_status ON pipeline.executions(status);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_start_time') THEN
        CREATE INDEX idx_executions_start_time ON pipeline.executions(start_time DESC);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Tabla: pipeline.validation_results
-- Resultados de validaciones de calidad
-- ============================================

\echo ''
\echo '[*] Creando tabla pipeline.validation_results...'

CREATE TABLE IF NOT EXISTS pipeline.validation_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline.executions(id) ON DELETE CASCADE,
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(100) NOT NULL,
    passed BOOLEAN NOT NULL,
    failed_count INTEGER DEFAULT 0,
    failure_details JSONB DEFAULT '[]',
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT validation_results_failed_count_positive CHECK (failed_count >= 0)
);

-- Índices
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_execution_id') THEN
        CREATE INDEX idx_validation_results_execution_id ON pipeline.validation_results(execution_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_rule_name') THEN
        CREATE INDEX idx_validation_results_rule_name ON pipeline.validation_results(rule_name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_passed') THEN
        CREATE INDEX idx_validation_results_passed ON pipeline.validation_results(passed);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_timestamp') THEN
        CREATE INDEX idx_validation_results_timestamp ON pipeline.validation_results(timestamp DESC);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Tabla: pipeline.audit_logs
-- Logs de auditoría del sistema
-- ============================================

\echo ''
\echo '[*] Creando tabla pipeline.audit_logs...'

CREATE TABLE IF NOT EXISTS pipeline.audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID REFERENCES pipeline.executions(id) ON DELETE SET NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(20) NOT NULL DEFAULT 'INFO',
    module VARCHAR(100) NOT NULL,
    event VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(100),
    context JSONB DEFAULT '{}',
    
    CONSTRAINT audit_logs_level_valid CHECK (
        level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    )
);

-- Índices para búsquedas eficientes
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_audit_logs_execution_id') THEN
        CREATE INDEX idx_audit_logs_execution_id ON pipeline.audit_logs(execution_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_audit_logs_timestamp') THEN
        CREATE INDEX idx_audit_logs_timestamp ON pipeline.audit_logs(timestamp DESC);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_audit_logs_level') THEN
        CREATE INDEX idx_audit_logs_level ON pipeline.audit_logs(level);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_audit_logs_module') THEN
        CREATE INDEX idx_audit_logs_module ON pipeline.audit_logs(module);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_audit_logs_correlation_id') THEN
        CREATE INDEX idx_audit_logs_correlation_id ON pipeline.audit_logs(correlation_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_audit_logs_context') THEN
        CREATE INDEX idx_audit_logs_context ON pipeline.audit_logs USING GIN (context);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Tabla: security.attack_scenarios
-- Configuraciones de escenarios de ataque
-- ============================================

\echo ''
\echo '[*] Creando tabla security.attack_scenarios...'

CREATE TABLE IF NOT EXISTS security.attack_scenarios (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    attack_types TEXT[] NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT attack_scenarios_name_not_empty CHECK (length(trim(name)) > 0),
    CONSTRAINT attack_scenarios_types_not_empty CHECK (array_length(attack_types, 1) > 0)
);

-- Índices
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_attack_scenarios_name') THEN
        CREATE INDEX idx_attack_scenarios_name ON security.attack_scenarios(name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_attack_scenarios_created_at') THEN
        CREATE INDEX idx_attack_scenarios_created_at ON security.attack_scenarios(created_at DESC);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Tabla: security.simulation_results
-- Resultados de simulaciones de ataques
-- ============================================

\echo ''
\echo '[*] Creando tabla security.simulation_results...'

CREATE TABLE IF NOT EXISTS security.simulation_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID REFERENCES pipeline.executions(id) ON DELETE CASCADE,
    attack_type VARCHAR(100) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    attempts_total INTEGER DEFAULT 0,
    attempts_detected INTEGER DEFAULT 0,
    attempts_blocked INTEGER DEFAULT 0,
    attempts_successful INTEGER DEFAULT 0,
    mttd_avg_ms NUMERIC(10, 2),
    vulnerabilities JSONB DEFAULT '[]',
    security_score NUMERIC(5, 2),
    
    CONSTRAINT simulation_results_end_after_start CHECK (
        end_time IS NULL OR end_time >= start_time
    ),
    CONSTRAINT simulation_results_counts_valid CHECK (
        attempts_total >= 0 AND
        attempts_detected >= 0 AND
        attempts_blocked >= 0 AND
        attempts_successful >= 0
    ),
    CONSTRAINT simulation_results_security_score_range CHECK (
        security_score IS NULL OR (security_score >= 0 AND security_score <= 100)
    )
);

-- Índices
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_simulation_results_execution_id') THEN
        CREATE INDEX idx_simulation_results_execution_id ON security.simulation_results(execution_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_simulation_results_attack_type') THEN
        CREATE INDEX idx_simulation_results_attack_type ON security.simulation_results(attack_type);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_simulation_results_start_time') THEN
        CREATE INDEX idx_simulation_results_start_time ON security.simulation_results(start_time DESC);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_simulation_results_vulnerabilities') THEN
        CREATE INDEX idx_simulation_results_vulnerabilities ON security.simulation_results USING GIN (vulnerabilities);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Tabla: monitoring.metrics
-- Métricas históricas del sistema
-- ============================================

\echo ''
\echo '[*] Creando tabla monitoring.metrics...'

CREATE TABLE IF NOT EXISTS monitoring.metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_name VARCHAR(255) NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value NUMERIC NOT NULL,
    labels JSONB DEFAULT '{}',
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT metrics_type_valid CHECK (
        metric_type IN ('counter', 'gauge', 'histogram', 'summary')
    )
);

-- Índices para consultas de métricas
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metrics_name') THEN
        CREATE INDEX idx_metrics_name ON monitoring.metrics(metric_name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metrics_timestamp') THEN
        CREATE INDEX idx_metrics_timestamp ON monitoring.metrics(timestamp DESC);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metrics_name_timestamp') THEN
        CREATE INDEX idx_metrics_name_timestamp ON monitoring.metrics(metric_name, timestamp DESC);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_metrics_labels') THEN
        CREATE INDEX idx_metrics_labels ON monitoring.metrics USING GIN (labels);
    END IF;
END $$;

\echo '  ✓ Tabla y índices creados'

-- ============================================
-- Funciones útiles
-- ============================================

\echo ''
\echo '[*] Creando funciones...'

-- Función para actualizar updated_at automáticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

\echo '  ✓ update_updated_at_column()'

-- Aplicar trigger a tablas con updated_at (solo si no existe)
DO $$
BEGIN
    -- Trigger para pipeline.pipelines
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_pipelines_updated_at') THEN
        CREATE TRIGGER update_pipelines_updated_at
            BEFORE UPDATE ON pipeline.pipelines
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
    
    -- Trigger para security.attack_scenarios
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_attack_scenarios_updated_at') THEN
        CREATE TRIGGER update_attack_scenarios_updated_at
            BEFORE UPDATE ON security.attack_scenarios
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

\echo '  ✓ Triggers aplicados'

-- ============================================
-- Función para calcular duración de ejecución
-- ============================================

-- Funciones y vistas eliminadas - no se usan en el código actual

-- ============================================
-- Datos de prueba (opcional - solo para desarrollo)
-- ============================================

\echo ''
\echo '[*] Insertando datos de ejemplo...'

-- Pipeline de ejemplo (con ON CONFLICT para re-ejecuciones)
INSERT INTO pipeline.pipelines (name, description, config)
VALUES (
    'example_pipeline',
    'Pipeline de ejemplo para validación inicial',
    '{"source": "postgresql", "validations": ["not_null", "unique"]}'::jsonb
) ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    config = EXCLUDED.config,
    updated_at = CURRENT_TIMESTAMP;

\echo '  ✓ Pipeline de ejemplo'

-- Escenario de ataque de ejemplo (con ON CONFLICT)
INSERT INTO security.attack_scenarios (name, description, attack_types, config)
VALUES (
    'sql_injection_basic',
    'Escenario básico de prueba de inyección SQL',
    ARRAY['sql_injection'],
    '{"payloads": ["boolean_based", "union_based"], "frequency": "5/min"}'::jsonb
) ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    attack_types = EXCLUDED.attack_types,
    config = EXCLUDED.config,
    updated_at = CURRENT_TIMESTAMP;

\echo '  ✓ Escenario de ataque de ejemplo'

-- ============================================
-- Permisos (opcional - ajustar según necesidad)
-- ============================================

-- Otorgar permisos al usuario de la aplicación
GRANT USAGE ON SCHEMA pipeline TO PUBLIC;
GRANT USAGE ON SCHEMA security TO PUBLIC;
GRANT USAGE ON SCHEMA monitoring TO PUBLIC;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pipeline TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA security TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA monitoring TO PUBLIC;

-- ============================================
-- Mensaje de confirmación
-- ============================================

\echo ''
\echo '============================================'
\echo 'Base de datos inicializada correctamente'
\echo '============================================'
\echo ''
\echo 'Esquemas creados: pipeline, security, monitoring'
\echo 'Tablas creadas: 7 tablas principales'
\echo 'Funciones creadas: 1 función auxiliar (update_updated_at)'
\echo 'Datos de ejemplo: 2 registros insertados'
\echo ''
\echo '✓ El script es idempotente y puede ejecutarse múltiples veces'
\echo ''
\echo 'Siguiente paso:'
\echo '  python scripts/generate_sample_data.py'
\echo '  o'
\echo '  python scripts/setup_environment.py'
\echo ''

-- Resumen en formato de consulta
DO $$
DECLARE
    tabla_count INTEGER;
    funcion_count INTEGER;
BEGIN
    -- Contar tablas
    SELECT COUNT(*) INTO tabla_count
    FROM information_schema.tables
    WHERE table_schema IN ('pipeline', 'security', 'monitoring')
    AND table_type = 'BASE TABLE';
    
    -- Contar funciones
    SELECT COUNT(*) INTO funcion_count
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'public'
    AND p.proname = 'update_updated_at_column';
    
    RAISE NOTICE 'Estadísticas finales:';
    RAISE NOTICE '  - Tablas: %', tabla_count;
    RAISE NOTICE '  - Funciones: %', funcion_count;
END $$;