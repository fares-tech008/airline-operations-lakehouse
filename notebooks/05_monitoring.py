# create the view

spark.sql("""
CREATE OR REPLACE VIEW workspace.ops.run_summary AS
WITH steps AS (
    SELECT run_id,
           MIN(started_at) AS run_started,
           MAX(ended_at) AS run_ended,
           ROUND(SUM(duration_seconds), 2) AS total_seconds,
           COUNT(*) AS steps_logged,
           SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_steps,
           MAX(CASE WHEN step_name = '03_gold' THEN rows_processed END) AS gold_fact_rows
    FROM workspace.ops.audit_log
    GROUP BY run_id
),
dq AS (
    SELECT run_id,
           COUNT(*) AS checks_run,
           SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS checks_passed,
           SUM(CASE WHEN severity = 'ERROR' AND status = 'FAIL' THEN 1 ELSE 0 END) AS error_checks_failed,
           SUM(CASE WHEN severity = 'WARN' AND status = 'FAIL' THEN 1 ELSE 0 END) AS warning_checks_failed
    FROM workspace.ops.dq_results
    GROUP BY run_id
)
SELECT s.run_id, s.run_started, s.run_ended, s.total_seconds, s.steps_logged,
       s.failed_steps, s.gold_fact_rows,
       d.checks_run, d.checks_passed, d.error_checks_failed, d.warning_checks_failed,
       ROUND(100.0 * try_divide(d.checks_passed, d.checks_run), 2) AS quality_score_pct,
       CASE WHEN s.failed_steps > 0 OR COALESCE(d.error_checks_failed, 0) > 0 THEN 'FAILED'
            WHEN d.run_id IS NULL THEN 'NO_QUALITY_DATA'
            WHEN d.warning_checks_failed > 0 THEN 'WARNING'
            ELSE 'HEALTHY' END AS run_status
FROM steps s
LEFT JOIN dq d ON s.run_id = d.run_id
""")
print("view created")


display(spark.sql("SELECT * FROM workspace.ops.run_summary ORDER BY run_started DESC"))
