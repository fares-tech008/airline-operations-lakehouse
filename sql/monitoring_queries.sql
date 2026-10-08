-- ============================================================
-- Airline Operations Lakehouse
-- Monitoring & Operational Queries
-- ============================================================

-- 1. Latest pipeline executions

SELECT *
FROM workspace.gold.pipeline_audit
ORDER BY start_time DESC;


-- 2. Failed pipeline executions

SELECT *
FROM workspace.gold.pipeline_audit
WHERE status = 'FAIL'
ORDER BY start_time DESC;


-- 3. Average pipeline runtime

SELECT
    pipeline_name,
    ROUND(AVG(duration_seconds), 2) AS avg_runtime_seconds
FROM workspace.gold.pipeline_audit
GROUP BY pipeline_name
ORDER BY avg_runtime_seconds DESC;


-- 4. Total rows processed by pipeline

SELECT
    pipeline_name,
    SUM(rows_processed) AS total_rows_processed
FROM workspace.gold.pipeline_audit
GROUP BY pipeline_name
ORDER BY total_rows_processed DESC;


-- 5. Latest data quality results

SELECT *
FROM workspace.gold.data_quality_results
ORDER BY execution_time DESC;


-- 6. Failed quality checks

SELECT *
FROM workspace.gold.data_quality_results
WHERE status = 'FAIL'
ORDER BY execution_time DESC;


-- 7. Data quality failures by table

SELECT
    table_name,
    COUNT(*) AS failed_checks
FROM workspace.gold.data_quality_results
WHERE status = 'FAIL'
GROUP BY table_name
ORDER BY failed_checks DESC;


-- 8. Pipeline quality score history

SELECT
    pipeline_name,
    quality_score,
    failed_checks,
    execution_time
FROM workspace.gold.pipeline_metrics
ORDER BY execution_time DESC;


-- 9. Average quality score by pipeline

SELECT
    pipeline_name,
    ROUND(AVG(quality_score), 2) AS avg_quality_score
FROM workspace.gold.pipeline_metrics
GROUP BY pipeline_name
ORDER BY avg_quality_score DESC;


-- 10. Processed file history

SELECT *
FROM workspace.gold.file_tracker
ORDER BY processed_time DESC;


-- 11. Total processed files

SELECT
    COUNT(*) AS total_processed_files
FROM workspace.gold.file_tracker;


-- 12. Most recently processed file

SELECT *
FROM workspace.gold.file_tracker
ORDER BY processed_time DESC
LIMIT 1;