-- scripts/init_db.sql

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.pipelines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    config JSONB NOT NULL DEFAULT '{}',
    version VARCHAR(50) DEFAULT '1.0.0',
    owner VARCHAR(255),
    tags TEXT[] DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    run_count INTEGER DEFAULT 0,
    last_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pipelines_name_not_empty CHECK (length(trim(name)) > 0)
);

COMMENT ON TABLE pipeline.pipelines IS 'Configuraciones de pipelines de datos';
COMMENT ON COLUMN pipeline.pipelines.version IS 'Versión del pipeline (semantic versioning)';
COMMENT ON COLUMN pipeline.pipelines.owner IS 'Usuario o equipo responsable del pipeline';
COMMENT ON COLUMN pipeline.pipelines.tags IS 'Etiquetas para clasificación (ej: [producción, finanzas])';
COMMENT ON COLUMN pipeline.pipelines.run_count IS 'Contador de ejecuciones completadas';
COMMENT ON COLUMN pipeline.pipelines.last_run_at IS 'Timestamp de la última ejecución exitosa';

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
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pipelines_owner') THEN
        CREATE INDEX idx_pipelines_owner ON pipeline.pipelines(owner);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pipelines_tags') THEN
        CREATE INDEX idx_pipelines_tags ON pipeline.pipelines USING GIN(tags);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_pipelines_last_run_at') THEN
        CREATE INDEX idx_pipelines_last_run_at ON pipeline.pipelines(last_run_at DESC);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS pipeline.executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES pipeline.pipelines(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    execution_type VARCHAR(50) DEFAULT 'manual',
    triggered_by VARCHAR(255),
    environment VARCHAR(50) DEFAULT 'development',
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 3),
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metrics JSONB DEFAULT '{}',
    stages_summary JSONB DEFAULT '[]',
    quality_score NUMERIC(5, 2),
    health_status VARCHAR(50),
    total_errors INTEGER DEFAULT 0,
    total_warnings INTEGER DEFAULT 0,
    report_path TEXT,
    executive_report_path TEXT,
    
    CONSTRAINT executions_status_valid CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
    ),
    CONSTRAINT executions_end_after_start CHECK (
        end_time IS NULL OR end_time >= start_time
    ),
    CONSTRAINT executions_execution_type_valid CHECK (
        execution_type IN ('manual', 'scheduled', 'triggered', 'api')
    ),
    CONSTRAINT executions_environment_valid CHECK (
        environment IN ('development', 'staging', 'production')
    ),
    CONSTRAINT executions_health_status_valid CHECK (
        health_status IS NULL OR health_status IN ('healthy', 'warning', 'critical', 'failed')
    )
);

COMMENT ON TABLE pipeline.executions IS 'Historial de ejecuciones de pipelines';
COMMENT ON COLUMN pipeline.executions.execution_type IS 'Tipo: manual, scheduled, triggered, api';
COMMENT ON COLUMN pipeline.executions.triggered_by IS 'Usuario o sistema que inició la ejecución';
COMMENT ON COLUMN pipeline.executions.environment IS 'Entorno: development, staging, production';
COMMENT ON COLUMN pipeline.executions.duration_seconds IS 'Duración total en segundos';
COMMENT ON COLUMN pipeline.executions.stages_summary IS 'Resumen de etapas ejecutadas (INGESTION, VALIDATION, TRANSFORMATION)';
COMMENT ON COLUMN pipeline.executions.quality_score IS 'Puntaje de calidad 0-100 basado en validaciones';
COMMENT ON COLUMN pipeline.executions.health_status IS 'Estado de salud: healthy, warning, critical, failed';
COMMENT ON COLUMN pipeline.executions.total_errors IS 'Número total de errores en la ejecución';
COMMENT ON COLUMN pipeline.executions.total_warnings IS 'Número total de warnings en la ejecución';
COMMENT ON COLUMN pipeline.executions.report_path IS 'Ruta del reporte técnico HTML generado';
COMMENT ON COLUMN pipeline.executions.executive_report_path IS 'Ruta del reporte ejecutivo HTML generado';

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
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_execution_type') THEN
        CREATE INDEX idx_executions_execution_type ON pipeline.executions(execution_type);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_triggered_by') THEN
        CREATE INDEX idx_executions_triggered_by ON pipeline.executions(triggered_by);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_environment') THEN
        CREATE INDEX idx_executions_environment ON pipeline.executions(environment);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_duration') THEN
        CREATE INDEX idx_executions_duration ON pipeline.executions(duration_seconds DESC);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_quality_score') THEN
        CREATE INDEX idx_executions_quality_score ON pipeline.executions(quality_score DESC);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_health_status') THEN
        CREATE INDEX idx_executions_health_status ON pipeline.executions(health_status);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_executions_total_errors') THEN
        CREATE INDEX idx_executions_total_errors ON pipeline.executions(total_errors) WHERE total_errors > 0;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS pipeline.stage_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline.executions(id) ON DELETE CASCADE,
    stage_name VARCHAR(50) NOT NULL,
    stage_order INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 3),
    records_in INTEGER DEFAULT 0,
    records_out INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    memory_usage_mb NUMERIC(10, 2),
    cpu_usage_percent NUMERIC(5, 2),
    error_count INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    error_details JSONB DEFAULT '[]',
    metrics JSONB DEFAULT '{}',
    
    CONSTRAINT stage_executions_status_valid CHECK (
        status IN ('running', 'completed', 'failed', 'skipped')
    ),
    CONSTRAINT stage_executions_stage_name_valid CHECK (
        stage_name IN ('INGESTION', 'VALIDATION', 'TRANSFORMATION', 'OUTPUT')
    ),
    CONSTRAINT stage_executions_end_after_start CHECK (
        end_time IS NULL OR end_time >= start_time
    ),
    CONSTRAINT stage_executions_unique_stage UNIQUE (execution_id, stage_name)
);

COMMENT ON TABLE pipeline.stage_executions IS 'Tracking granular de performance por stage del pipeline';
COMMENT ON COLUMN pipeline.stage_executions.stage_order IS 'Orden de ejecución: 1=INGESTION, 2=VALIDATION, 3=TRANSFORMATION, 4=OUTPUT';
COMMENT ON COLUMN pipeline.stage_executions.memory_usage_mb IS 'Uso de memoria durante el stage en MB';
COMMENT ON COLUMN pipeline.stage_executions.cpu_usage_percent IS 'Uso promedio de CPU durante el stage';
COMMENT ON COLUMN pipeline.stage_executions.metrics IS 'Métricas adicionales específicas del stage (quality_score, validations_passed, etc)';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_stage_executions_execution_id') THEN
        CREATE INDEX idx_stage_executions_execution_id ON pipeline.stage_executions(execution_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_stage_executions_stage_name') THEN
        CREATE INDEX idx_stage_executions_stage_name ON pipeline.stage_executions(stage_name);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_stage_executions_status') THEN
        CREATE INDEX idx_stage_executions_status ON pipeline.stage_executions(status);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_stage_executions_duration') THEN
        CREATE INDEX idx_stage_executions_duration ON pipeline.stage_executions(duration_seconds DESC);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_stage_executions_start_time') THEN
        CREATE INDEX idx_stage_executions_start_time ON pipeline.stage_executions(start_time DESC);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS pipeline.validation_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline.executions(id) ON DELETE CASCADE,
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(100) NOT NULL,
    dataset_name VARCHAR(255),
    suite_name VARCHAR(255),
    expectation_type VARCHAR(255),
    passed BOOLEAN NOT NULL,
    failed_count INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    severity VARCHAR(50) DEFAULT 'error',
    failure_details JSONB DEFAULT '[]',
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT validation_results_failed_count_positive CHECK (failed_count >= 0),
    CONSTRAINT validation_results_severity_valid CHECK (
        severity IN ('critical', 'error', 'warning', 'info')
    )
);

COMMENT ON TABLE pipeline.validation_results IS 'Resultados de validaciones de calidad y seguridad';
COMMENT ON COLUMN pipeline.validation_results.dataset_name IS 'Nombre del dataset validado (ej: raw_customers)';
COMMENT ON COLUMN pipeline.validation_results.suite_name IS 'Nombre del suite de validación (ej: Suite 01: Estructura)';
COMMENT ON COLUMN pipeline.validation_results.expectation_type IS 'Tipo de expectativa GE (ej: expect_column_values_to_not_be_null)';
COMMENT ON COLUMN pipeline.validation_results.total_records IS 'Total de registros validados';
COMMENT ON COLUMN pipeline.validation_results.severity IS 'Severidad: critical, error, warning, info';

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
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_dataset_name') THEN
        CREATE INDEX idx_validation_results_dataset_name ON pipeline.validation_results(dataset_name);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_suite_name') THEN
        CREATE INDEX idx_validation_results_suite_name ON pipeline.validation_results(suite_name);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_severity') THEN
        CREATE INDEX idx_validation_results_severity ON pipeline.validation_results(severity);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_results_expectation_type') THEN
        CREATE INDEX idx_validation_results_expectation_type ON pipeline.validation_results(expectation_type);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS pipeline.validation_summary (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline.executions(id) ON DELETE CASCADE,
    suite_name VARCHAR(255) NOT NULL,
    dataset_name VARCHAR(255) NOT NULL,
    total_validations INTEGER NOT NULL,
    passed_validations INTEGER NOT NULL,
    failed_validations INTEGER NOT NULL,
    quality_score NUMERIC(5, 2) NOT NULL,
    total_records INTEGER DEFAULT 0,
    execution_time_ms INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT validation_summary_totals_match CHECK (
        total_validations = passed_validations + failed_validations
    ),
    CONSTRAINT validation_summary_unique UNIQUE (execution_id, suite_name, dataset_name)
);

COMMENT ON TABLE pipeline.validation_summary IS 'Resumen agregado de validaciones por suite y dataset - Reduce registros innecesarios';
COMMENT ON COLUMN pipeline.validation_summary.quality_score IS 'Score de calidad calculado: (passed/total)*100';
COMMENT ON COLUMN pipeline.validation_summary.execution_time_ms IS 'Tiempo de ejecución del suite en milisegundos';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_summary_execution_id') THEN
        CREATE INDEX idx_validation_summary_execution_id ON pipeline.validation_summary(execution_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_summary_suite_name') THEN
        CREATE INDEX idx_validation_summary_suite_name ON pipeline.validation_summary(suite_name);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_summary_quality_score') THEN
        CREATE INDEX idx_validation_summary_quality_score ON pipeline.validation_summary(quality_score);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_validation_summary_dataset_name') THEN
        CREATE INDEX idx_validation_summary_dataset_name ON pipeline.validation_summary(dataset_name);
    END IF;
END $$;

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

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_pipelines_updated_at') THEN
        CREATE TRIGGER update_pipelines_updated_at
            BEFORE UPDATE ON pipeline.pipelines
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

GRANT USAGE ON SCHEMA pipeline TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pipeline TO PUBLIC;
