import uuid
from datetime import datetime

dbutils.widgets.text("run_id", "")
RUN_ID = dbutils.widgets.get("run_id") or f"manual-{uuid.uuid4()}"
run_id = RUN_ID   # same value, for notebooks that use the lowercase name
AUDIT_TABLE = "workspace.ops.audit_log"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    run_id STRING, step_name STRING, status STRING,
    started_at TIMESTAMP, ended_at TIMESTAMP, duration_seconds DOUBLE,
    rows_processed BIGINT, message STRING
) USING DELTA
""")


def log_step(step, status, rows=None, started=None, message=None):
    """Write ONE row to the audit log. status: SUCCESS or FAILED."""
    ended = datetime.now()
    started = started or ended
    row = [(RUN_ID, step, status, started, ended,
            (ended - started).total_seconds(),
            None if rows is None else int(rows),
            None if message is None else str(message)[:1000])]
    schema = ("run_id string, step_name string, status string, started_at timestamp, "
              "ended_at timestamp, duration_seconds double, rows_processed long, message string")
    spark.createDataFrame(row, schema).write.format(
        "delta").mode("append").saveAsTable(AUDIT_TABLE)
    print(f"[audit] {RUN_ID} | {step} | {status} | rows={rows}")
