-- scripts/init_db.sql
-- Script de inicialización de base de datos
-- Se ejecuta automáticamente al crear el contenedor de PostgreSQL

-- ============================================
-- Crear extensiones necesarias
-- ============================================

-- UUID para generar identificadores únicos
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Funciones de texto completo (full-text search)
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Funciones criptográficas
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- Crear esquemas
-- ============================================

CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE SCHEMA IF NOT EXISTS security;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ============================================
-- Tabla: pipeline.pipelines
-- Configuraciones de pipelines
-- ============================================

CREATE TABLE IF NOT EXISTS pipeline.pipelines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    config JSONB NOT NULL DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    
    CONSTRAINT pipelines_name_not_empty CHECK (length(trim(name)) > 0)
);

-- Índices para búsqueda rápida
CREATE INDEX idx_pipelines_name ON pipeline.pipelines(name);
CREATE INDEX idx_pipelines_is_active ON pipeline.pipelines(is_active);
CREATE INDEX idx_pipelines_created_at ON pipeline.pipelines(created_at DESC);

-- ============================================
-- Tabla: pipeline.executions
-- Registro de ejecuciones de pipelines
-- ============================================

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
CREATE INDEX idx_executions_pipeline_id ON pipeline.executions(pipeline_id);
CREATE INDEX idx_executions_status ON pipeline.executions(status);
CREATE INDEX idx_executions_start_time ON pipeline.executions(start_time DESC);

-- ============================================
-- Tabla: pipeline.validation_results
-- Resultados de validaciones de calidad
-- ============================================

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
CREATE INDEX idx_validation_results_execution_id ON pipeline.validation_results(execution_id);
CREATE INDEX idx_validation_results_rule_name ON pipeline.validation_results(rule_name);
CREATE INDEX idx_validation_results_passed ON pipeline.validation_results(passed);
CREATE INDEX idx_validation_results_timestamp ON pipeline.validation_results(timestamp DESC);

-- ============================================
-- Tabla: pipeline.audit_logs
-- Logs de auditoría del sistema
-- ============================================

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
CREATE INDEX idx_audit_logs_execution_id ON pipeline.audit_logs(execution_id);
CREATE INDEX idx_audit_logs_timestamp ON pipeline.audit_logs(timestamp DESC);
CREATE INDEX idx_audit_logs_level ON pipeline.audit_logs(level);
CREATE INDEX idx_audit_logs_module ON pipeline.audit_logs(module);
CREATE INDEX idx_audit_logs_correlation_id ON pipeline.audit_logs(correlation_id);

-- Índice GIN para búsqueda en JSONB
CREATE INDEX idx_audit_logs_context ON pipeline.audit_logs USING GIN (context);

-- ============================================
-- Tabla: security.attack_scenarios
-- Configuraciones de escenarios de ataque
-- ============================================

CREATE TABLE IF NOT EXISTS security.attack_scenarios (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    attack_types TEXT[] NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    created_by VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT attack_scenarios_name_not_empty CHECK (length(trim(name)) > 0),
    CONSTRAINT attack_scenarios_types_not_empty CHECK (array_length(attack_types, 1) > 0)
);

-- Índices
CREATE INDEX idx_attack_scenarios_name ON security.attack_scenarios(name);
CREATE INDEX idx_attack_scenarios_created_at ON security.attack_scenarios(created_at DESC);

-- ============================================
-- Tabla: security.simulation_results
-- Resultados de simulaciones de ataques
-- ============================================

CREATE TABLE IF NOT EXISTS security.simulation_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario_id UUID NOT NULL REFERENCES security.attack_scenarios(id) ON DELETE CASCADE,
    execution_id UUID REFERENCES pipeline.executions(id) ON DELETE SET NULL,
    attack_type VARCHAR(100) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    attempts_total INTEGER DEFAULT 0,
    attempts_detected INTEGER DEFAULT 0,
    attempts_blocked INTEGER DEFAULT 0,
    attempts_successful INTEGER DEFAULT 0,
    mttd_avg_ms NUMERIC(10, 2),
    mttd_p50_ms NUMERIC(10, 2),
    mttd_p95_ms NUMERIC(10, 2),
    mttd_p99_ms NUMERIC(10, 2),
    false_positives INTEGER DEFAULT 0,
    vulnerabilities JSONB DEFAULT '[]',
    security_score NUMERIC(5, 2),
    report_path TEXT,
    
    CONSTRAINT simulation_results_end_after_start CHECK (
        end_time IS NULL OR end_time >= start_time
    ),
    CONSTRAINT simulation_results_counts_valid CHECK (
        attempts_total >= 0 AND
        attempts_detected >= 0 AND
        attempts_blocked >= 0 AND
        attempts_successful >= 0 AND
        attempts_detected + attempts_blocked + attempts_successful <= attempts_total
    ),
    CONSTRAINT simulation_results_security_score_range CHECK (
        security_score IS NULL OR (security_score >= 0 AND security_score <= 100)
    )
);

-- Índices
CREATE INDEX idx_simulation_results_scenario_id ON security.simulation_results(scenario_id);
CREATE INDEX idx_simulation_results_execution_id ON security.simulation_results(execution_id);
CREATE INDEX idx_simulation_results_attack_type ON security.simulation_results(attack_type);
CREATE INDEX idx_simulation_results_start_time ON security.simulation_results(start_time DESC);
CREATE INDEX idx_simulation_results_security_score ON security.simulation_results(security_score DESC);

-- Índice GIN para búsqueda en vulnerabilidades
CREATE INDEX idx_simulation_results_vulnerabilities ON security.simulation_results USING GIN (vulnerabilities);

-- ============================================
-- Tabla: monitoring.metrics
-- Métricas históricas del sistema
-- ============================================

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
CREATE INDEX idx_metrics_name ON monitoring.metrics(metric_name);
CREATE INDEX idx_metrics_timestamp ON monitoring.metrics(timestamp DESC);
CREATE INDEX idx_metrics_name_timestamp ON monitoring.metrics(metric_name, timestamp DESC);

-- Índice GIN para búsqueda en labels
CREATE INDEX idx_metrics_labels ON monitoring.metrics USING GIN (labels);

-- ============================================
-- Funciones útiles
-- ============================================

-- Función para actualizar updated_at automáticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Aplicar trigger a tablas con updated_at
CREATE TRIGGER update_pipelines_updated_at
    BEFORE UPDATE ON pipeline.pipelines
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_attack_scenarios_updated_at
    BEFORE UPDATE ON security.attack_scenarios
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Función para calcular duración de ejecución
-- ============================================

CREATE OR REPLACE FUNCTION pipeline.calculate_execution_duration(execution_uuid UUID)
RETURNS INTERVAL AS $$
DECLARE
    duration INTERVAL;
BEGIN
    SELECT (end_time - start_time) INTO duration
    FROM pipeline.executions
    WHERE id = execution_uuid;
    
    RETURN duration;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- Vista: pipeline.execution_summary
-- Resumen de ejecuciones con métricas calculadas
-- ============================================

CREATE OR REPLACE VIEW pipeline.execution_summary AS
SELECT 
    e.id,
    e.pipeline_id,
    p.name AS pipeline_name,
    e.status,
    e.start_time,
    e.end_time,
    EXTRACT(EPOCH FROM (e.end_time - e.start_time)) AS duration_seconds,
    e.records_processed,
    e.records_failed,
    CASE 
        WHEN e.records_processed > 0 
        THEN ROUND((e.records_failed::NUMERIC / e.records_processed * 100), 2)
        ELSE 0
    END AS failure_rate_percent,
    e.metrics,
    e.error_message
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id;

-- ============================================
-- Vista: security.security_posture
-- Resumen de postura de seguridad
-- ============================================

CREATE OR REPLACE VIEW security.security_posture AS
SELECT 
    attack_type,
    COUNT(*) AS total_simulations,
    ROUND(AVG(security_score), 2) AS avg_security_score,
    ROUND(AVG(attempts_detected::NUMERIC / NULLIF(attempts_total, 0) * 100), 2) AS avg_detection_rate,
    ROUND(AVG(mttd_avg_ms), 2) AS avg_mttd_ms,
    SUM(attempts_successful) AS total_successful_attacks,
    MAX(start_time) AS last_simulation_date
FROM security.simulation_results
WHERE attempts_total > 0
GROUP BY attack_type;

-- ============================================
-- Datos de prueba (opcional - solo para desarrollo)
-- ============================================

-- Pipeline de ejemplo
INSERT INTO pipeline.pipelines (name, description, config, created_by)
VALUES (
    'example_pipeline',
    'Pipeline de ejemplo para validación inicial',
    '{"source": "postgresql", "validations": ["not_null", "unique"]}'::jsonb,
    'system'
) ON CONFLICT (name) DO NOTHING;

-- Escenario de ataque de ejemplo
INSERT INTO security.attack_scenarios (name, description, attack_types, config, created_by)
VALUES (
    'sql_injection_basic',
    'Escenario básico de prueba de inyección SQL',
    ARRAY['sql_injection'],
    '{"payloads": ["boolean_based", "union_based"], "frequency": "5/min"}'::jsonb,
    'system'
) ON CONFLICT (name) DO NOTHING;

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

DO $$
BEGIN
    RAISE NOTICE 'Base de datos inicializada correctamente';
    RAISE NOTICE 'Esquemas creados: pipeline, security, monitoring';
    RAISE NOTICE 'Tablas creadas: 9 tablas principales';
    RAISE NOTICE 'Vistas creadas: 2 vistas de resumen';
    RAISE NOTICE 'Funciones creadas: 2 funciones auxiliares';
END $$;