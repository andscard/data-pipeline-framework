# Queries SQL Útiles - Data Pipeline Framework

Colección de consultas SQL listas para usar para análisis, monitoreo y debugging de la base de datos `data_framework`.

## 📋 Contenidos

1. [Consultas Básicas](#consultas-básicas)
2. [Análisis de Calidad](#análisis-de-calidad)
3. [Análisis de Seguridad](#análisis-de-seguridad)
4. [Análisis de Performance](#análisis-de-performance)
5. [Reportes Ejecutivos](#reportes-ejecutivos)

---

## Consultas Básicas

### 1. Últimas 10 Ejecuciones

```sql
SELECT 
    e.id,
    p.name as pipeline_name,
    e.status,
    e.execution_type,
    e.environment,
    e.duration_seconds,
    e.quality_score,
    e.records_processed,
    e.start_time,
    e.end_time
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
ORDER BY e.start_time DESC
LIMIT 10;
```

### 2. Count de Ejecuciones por Pipeline

```sql
SELECT 
    p.name,
    p.owner,
    COUNT(e.id) as total_executions,
    COUNT(CASE WHEN e.status = 'completed' THEN 1 END) as successful,
    COUNT(CASE WHEN e.status = 'failed' THEN 1 END) as failed,
    ROUND(AVG(e.duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(e.quality_score), 2) as avg_quality_score
FROM pipeline.pipelines p
LEFT JOIN pipeline.executions e ON p.id = e.pipeline_id
GROUP BY p.name, p.owner
ORDER BY total_executions DESC;
```

### 3. Validaciones Fallidas por Pipeline

```sql
SELECT 
    p.name as pipeline_name,
    v.rule_name,
    v.severity,
    COUNT(*) as times_failed,
    SUM(v.failed_count) as total_records_failed,
    MAX(v.timestamp) as last_occurrence
FROM pipeline.validation_results v
JOIN pipeline.executions e ON v.execution_id = e.id
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE v.passed = false
GROUP BY p.name, v.rule_name, v.severity
ORDER BY times_failed DESC, severity;
```

---

## Análisis de Calidad

### 4. Trend de Quality Score

```sql
SELECT 
    DATE(e.start_time) as date,
    p.name as pipeline_name,
    COUNT(e.id) as executions,
    ROUND(AVG(e.quality_score), 2) as avg_quality_score,
    MIN(e.quality_score) as min_quality_score,
    MAX(e.quality_score) as max_quality_score
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.start_time >= CURRENT_DATE - INTERVAL '30 days'
  AND e.status = 'completed'
GROUP BY DATE(e.start_time), p.name
ORDER BY date DESC, pipeline_name;
```

### 5. Validaciones con Mayor Tasa de Fallo

```sql
SELECT 
    v.rule_name,
    v.expectation_type,
    v.severity,
    COUNT(*) as total_checks,
    COUNT(CASE WHEN v.passed = false THEN 1 END) as failed_checks,
    ROUND(
        COUNT(CASE WHEN v.passed = false THEN 1 END)::numeric / COUNT(*)::numeric * 100, 
        2
    ) as failure_rate_pct,
    SUM(v.failed_count) as total_records_affected
FROM pipeline.validation_results v
GROUP BY v.rule_name, v.expectation_type, v.severity
HAVING COUNT(*) >= 10  -- Al menos 10 checks
ORDER BY failure_rate_pct DESC
LIMIT 20;
```

---

## Análisis de Seguridad

### 6. Vulnerabilidades de Seguridad Detectadas

```sql
SELECT 
    e.start_time,
    p.name as pipeline_name,
    v.rule_name,
    v.dataset_name,
    v.failed_count,
    v.total_records,
    ROUND(v.failed_count::numeric / v.total_records::numeric * 100, 2) as infection_rate_pct,
    v.failure_details::jsonb->0->'kwargs'->>'column' as affected_column,
    v.failure_details::jsonb->0->'kwargs'->>'regex' as attack_pattern
FROM pipeline.validation_results v
JOIN pipeline.executions e ON v.execution_id = e.id
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE v.suite_name = 'security_detection_suite'
  AND v.passed = false
  AND v.severity IN ('critical', 'error')
ORDER BY e.start_time DESC, v.failed_count DESC;
```

### 7. Columnas Más Atacadas

```sql
SELECT 
    v.failure_details::jsonb->0->'kwargs'->>'column' as column_name,
    COUNT(*) as attack_detections,
    COUNT(DISTINCT e.id) as affected_executions,
    SUM(v.failed_count) as total_malicious_records,
    ARRAY_AGG(DISTINCT v.rule_name) as attack_types
FROM pipeline.validation_results v
JOIN pipeline.executions e ON v.execution_id = e.id
WHERE v.suite_name = 'security_detection_suite'
  AND v.passed = false
  AND v.failure_details::jsonb->0->'kwargs'->>'column' IS NOT NULL
GROUP BY v.failure_details::jsonb->0->'kwargs'->>'column'
ORDER BY attack_detections DESC
LIMIT 10;
```

---

## Análisis de Performance

### 8. Pipelines Más Lentos

```sql
SELECT 
    p.name,
    p.owner,
    COUNT(e.id) as executions,
    ROUND(AVG(e.duration_seconds), 2) as avg_seconds,
    ROUND(MIN(e.duration_seconds), 2) as min_seconds,
    ROUND(MAX(e.duration_seconds), 2) as max_seconds,
    ROUND(STDDEV(e.duration_seconds), 2) as stddev_seconds,
    ROUND(AVG(e.records_processed), 0) as avg_records,
    ROUND(AVG(e.records_processed / NULLIF(e.duration_seconds, 0)), 0) as avg_records_per_second
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.status = 'completed'
  AND e.duration_seconds IS NOT NULL
GROUP BY p.name, p.owner
HAVING COUNT(e.id) >= 3
ORDER BY avg_seconds DESC
LIMIT 10;
```

### 9. Stages Summary - Breakdown de Duración

```sql
SELECT 
    p.name as pipeline_name,
    e.id as execution_id,
    e.start_time,
    jsonb_array_elements(e.stages_summary) ->> 'stage_name' as stage_name,
    (jsonb_array_elements(e.stages_summary) ->> 'duration')::numeric as stage_duration_seconds,
    jsonb_array_elements(e.stages_summary) ->> 'status' as stage_status,
    (jsonb_array_elements(e.stages_summary) ->> 'records')::integer as stage_records
FROM pipeline.executions e
JOIN pipeline.pipelines p ON e.pipeline_id = p.id
WHERE e.stages_summary IS NOT NULL
  AND jsonb_array_length(e.stages_summary) > 0
ORDER BY e.start_time DESC, stage_duration_seconds DESC
LIMIT 50;
```

---

## Reportes Ejecutivos

### 10. Dashboard Diario - Métricas del día

```sql
SELECT 
    COUNT(DISTINCT p.id) as active_pipelines,
    COUNT(e.id) as total_executions,
    COUNT(CASE WHEN e.status = 'completed' THEN 1 END) as successful_executions,
    COUNT(CASE WHEN e.status = 'failed' THEN 1 END) as failed_executions,
    ROUND(
        COUNT(CASE WHEN e.status = 'completed' THEN 1 END)::numeric / 
        NULLIF(COUNT(e.id), 0)::numeric * 100, 
        2
    ) as success_rate_pct,
    ROUND(AVG(CASE WHEN e.status = 'completed' THEN e.duration_seconds END), 2) as avg_duration_seconds,
    ROUND(AVG(CASE WHEN e.status = 'completed' THEN e.quality_score END), 2) as avg_quality_score,
    SUM(CASE WHEN e.status = 'completed' THEN e.records_processed ELSE 0 END) as total_records_processed,
    COUNT(DISTINCT CASE WHEN v.passed = false AND v.severity = 'critical' THEN v.id END) as critical_issues
FROM pipeline.pipelines p
LEFT JOIN pipeline.executions e ON p.id = e.pipeline_id 
    AND DATE(e.start_time) = CURRENT_DATE
LEFT JOIN pipeline.validation_results v ON e.id = v.execution_id;
```

### 11. Top 5 Pipelines por Volumen de Datos

```sql
SELECT 
    p.name,
    p.owner,
    COUNT(e.id) as total_runs,
    SUM(e.records_processed) as total_records_processed,
    ROUND(AVG(e.records_processed), 0) as avg_records_per_run,
    ROUND(AVG(e.duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(e.quality_score), 2) as avg_quality_score
FROM pipeline.pipelines p
JOIN pipeline.executions e ON p.id = e.pipeline_id
WHERE e.status = 'completed'
  AND e.start_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.name, p.owner
ORDER BY total_records_processed DESC
LIMIT 5;
```
