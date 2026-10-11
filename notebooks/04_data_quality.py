%run ./00_audit_helpers





from datetime import datetime
started = datetime.now()





import uuid
from datetime import datetime

SILVER = "workspace.silver.silver_flights"
GOLD = "workspace.gold"
RESULTS_TABLE = "workspace.ops.dq_results"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
    run_id STRING, checked_at TIMESTAMP, layer STRING,
    check_name STRING, severity STRING, failing_rows BIGINT, status STRING
) USING DELTA
""")

results = []   # every check adds one row


def record(layer, name, severity, failing):
    failing = int(failing or 0)
    results.append((run_id, datetime.now(), layer, name, severity, failing,
                    "PASS" if failing == 0 else "FAIL"))


def one_pass(layer, table, checks):
    """Run many checks on one table with ONE scan. Each check = (name, severity, 'condition that means BAD')."""
    parts = ", ".join(f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END) AS c{i}"
                      for i, (_, _, cond) in enumerate(checks))
    row = spark.sql(f"SELECT COUNT(*) AS total, {parts} FROM {table}").first()
    record(layer, "table_not_empty", "ERROR", 1 if row["total"] == 0 else 0)
    for i, (name, severity, _) in enumerate(checks):
        record(layer, name, severity, row[f"c{i}"])


def scalar(layer, name, severity, sql):
    """A check whose SQL returns ONE number = how many rows are bad."""
    record(layer, name, severity, spark.sql(sql).first()[0])


# Silver checks
one_pass("silver", SILVER, [
    ("origin_is_null",              "ERROR", "ORIGIN IS NULL"),
    ("dest_is_null",                "ERROR", "DEST IS NULL"),
    ("flight_date_is_null",         "ERROR", "FL_DATE IS NULL"),
    ("distance_not_positive",       "ERROR",
     "DISTANCE IS NOT NULL AND DISTANCE <= 0"),
    ("cancelled_and_diverted",      "ERROR", "CANCELLED = 1 AND DIVERTED = 1"),
    ("origin_equals_dest",          "WARN",  "ORIGIN = DEST"),
    ("completed_missing_arr_delay", "WARN",
     "FLIGHT_STATUS = 'COMPLETED' AND ARR_DELAY IS NULL"),
])


# Gold checks

scalar("gold", "fact_rows_differ_from_silver", "ERROR", f"""
    SELECT ABS((SELECT COUNT(*) FROM {GOLD}.fact_flights) - (SELECT COUNT(*) FROM {SILVER}))""")

scalar("gold", "fact_orphan_date", "ERROR", f"""
    SELECT COUNT(*) FROM {GOLD}.fact_flights f
    LEFT ANTI JOIN {GOLD}.dim_date d ON f.DATE_KEY = d.DATE_KEY""")

scalar("gold", "fact_orphan_origin", "ERROR", f"""
    SELECT COUNT(*) FROM {GOLD}.fact_flights f
    LEFT ANTI JOIN {GOLD}.dim_airport a ON f.ORIGIN = a.AIRPORT_CODE""")

scalar("gold", "fact_orphan_dest", "ERROR", f"""
    SELECT COUNT(*) FROM {GOLD}.fact_flights f
    LEFT ANTI JOIN {GOLD}.dim_airport a ON f.DEST = a.AIRPORT_CODE""")

scalar("gold", "dim_date_duplicate_keys", "ERROR", f"""
    SELECT COUNT(*) - COUNT(DISTINCT DATE_KEY) FROM {GOLD}.dim_date""")

scalar("gold", "dim_airport_duplicate_codes", "ERROR", f"""
    SELECT COUNT(*) - COUNT(DISTINCT AIRPORT_CODE) FROM {GOLD}.dim_airport""")

scalar("gold", "monthly_total_differs_from_fact", "ERROR", f"""
    SELECT ABS((SELECT SUM(TOTAL_FLIGHTS) FROM {GOLD}.monthly_trends)
             - (SELECT COUNT(*) FROM {GOLD}.fact_flights))""")

scalar("gold", "delay_rate_outside_5_to_50", "WARN", f"""
    SELECT CASE WHEN try_divide(SUM(DELAYED_FLIGHTS), SUM(COMPLETED_FLIGHTS)) BETWEEN 0.05 AND 0.50
                THEN 0 ELSE 1 END
    FROM {GOLD}.monthly_trends""")


# save the results

schema = ("run_id string, checked_at timestamp, layer string, check_name string, "
          "severity string, failing_rows long, status string")
results_df = spark.createDataFrame(results, schema)
results_df.write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)

display(results_df.orderBy("layer", "check_name"))


# the gate

errors = [r for r in results if r[4] == "ERROR" and r[5] > 0]
warns  = [r for r in results if r[4] == "WARN"  and r[5] > 0]

print(f"Checks run: {len(results)} | ERRORS: {len(errors)} | WARNINGS: {len(warns)}")
for r in warns:
    print(f"  WARN  {r[2]}.{r[3]}: {r[5]:,} rows")

if errors:
    names = ", ".join(f"{r[2]}.{r[3]}" for r in errors)
    log_step("04_data_quality", "FAILED", rows=len(results), started=started,
             message=f"Errors: {names}")
    raise Exception(f"Data quality gate FAILED: {names}")




log_step("04_data_quality", "SUCCESS", rows=len(results), started=started,
         message=f"{len(warns)} warning(s)")
print("Gate passed. The pipeline can continue.")
