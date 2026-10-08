# Databricks notebook source
# MAGIC %md
# MAGIC # 1 — Start Pipeline Run

# COMMAND ----------

from datetime import datetime

pipeline_start_time = datetime.now()

print(
    f"Pipeline Started: {pipeline_start_time}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 — Create Run ID

# COMMAND ----------

import uuid

run_id = str(uuid.uuid4())

print(run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 — Incremental Check

# COMMAND ----------

processed_files = spark.table(
    "gold.file_tracker"
).count()

print(
    f"Tracked Files: {processed_files}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 — Quality Summary

# COMMAND ----------

quality_df = spark.sql("""
SELECT 
    COUNT(*) AS total_checks,
    SUM(
        CASE
            WHEN status='FAIL'
            THEN 1
            ELSE 0
        END
    ) AS failed_checks  
FROM gold.data_quality_results                 
""")

# COMMAND ----------

display(quality_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 — Audit Summary

# COMMAND ----------

audit_df = spark.sql("""
SELECT *
FROM gold.pipeline_audit
ORDER BY start_time DESC
LIMIT 5                     
""")

# COMMAND ----------

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6 — Monitoring Summary

# COMMAND ----------

metrics_df = spark.sql("""
SELECT *
FROM gold.pipeline_metrics
ORDER BY execution_time DESC
LIMIT 5             
""")

# COMMAND ----------

 display(metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7 — Pipeline Health Status

# COMMAND ----------

failed_checks = spark.sql("""
SELECT COUNT(*) 
FROM gold.data_quality_results
WHERE status='FAIL'                         
""").collect()[0][0]

# COMMAND ----------

if failed_checks == 0:
    pipeline_status = "HEALTHY"
else:
    pipeline_status = "WARNING"

# COMMAND ----------

print(
    f"Pipeline Status: {pipeline_status}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #  8 — Execution Summary

# COMMAND ----------

pipeline_end_time = datetime.now()

duration = (
    pipeline_end_time -
    pipeline_start_time
).total_seconds()

print(f"Elapsed Time Since Notebook Start: {duration:.2f} seconds")