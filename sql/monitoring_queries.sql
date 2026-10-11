-- ============================================================
-- Airline Operations Lakehouse
-- Monitoring & Operational Queries 
-- ============================================================

-- 1. Latest pipeline runs

SELECT *
FROM workspace.ops.run_summary
ORDER BY run_started DESC;


-- 2. Failed pipeline runs

SELECT *
FROM workspace.ops.run_summary
WHERE run_status = 'FAILED'
ORDER BY run_started DESC;


-- 3. Average runtime per step

SELECT
    step_name,
    ROUND(AVG(duration_seconds), 2) AS avg_runtime_seconds
FROM workspace.ops.audit_log
GROUP BY step_name
ORDER BY avg_runtime_seconds DESC;


-- 4. Rows processed by the latest run of each step

SELECT step_name, rows_processed, status, ended_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY step_name ORDER BY ended_at DESC) AS rn
    FROM workspace.ops.audit_log
)
WHERE rn = 1
ORDER BY step_name;


-- 5. Latest data quality results

SELECT *
FROM workspace.ops.dq_results
ORDER BY checked_at DESC;


-- 6. Failed quality checks

SELECT *
FROM workspace.ops.dq_results
WHERE status = 'FAIL'
ORDER BY checked_at DESC;


-- 7. Failures by layer and check

SELECT
    layer,
    check_name,
    severity,
    COUNT(*) AS times_failed
FROM workspace.ops.dq_results
WHERE status = 'FAIL'
GROUP BY layer, check_name, severity
ORDER BY times_failed DESC;


-- 8. Quality score history

SELECT run_id, quality_score_pct, error_checks_failed, warning_checks_failed, run_started
FROM workspace.ops.run_summary
WHERE checks_run IS NOT NULL
ORDER BY run_started DESC;


-- 9. Average quality score

SELECT ROUND(AVG(quality_score_pct), 2) AS avg_quality_score_pct
FROM workspace.ops.run_summary
WHERE checks_run IS NOT NULL;


-- 10. Processed file history

SELECT *
FROM workspace.ops.file_tracker
ORDER BY processed_time DESC;


-- 11. Total processed files

SELECT COUNT(*) AS total_processed_files
FROM workspace.ops.file_tracker;


-- 12. Most recently processed file

SELECT *
FROM workspace.ops.file_tracker
ORDER BY processed_time DESC
LIMIT 1;
