# Databricks notebook source
# MAGIC %md
# MAGIC # 1 — Create Monitoring Table

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

metrics_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("runtime_seconds", DoubleType(), True),
    StructField("failed_checks", IntegerType(), True),
    StructField("quality_score", DoubleType(), True),
    StructField("execution_time", TimestampType(), True)
])

# COMMAND ----------

empty_metrics_df = spark.createDataFrame(
    [],
    metrics_schema
)

empty_metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.pipeline_metrics")

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM gold.pipeline_metrics
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 — Read Audit Data

# COMMAND ----------

audit_df = spark.sql("""
SELECT *
FROM gold.pipeline_audit
ORDER BY start_time DESC
LIMIT 1
""")

# COMMAND ----------

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 — Count Failed Quality Checks

# COMMAND ----------

failed_checks = spark.sql("""
SELECT COUNT(*) AS failed_count
FROM gold.data_quality_results
WHERE status = 'FAIL'
""").collect()[0]["failed_count"]

print(failed_checks)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 — Calculate Quality Score

# COMMAND ----------

total_checks = spark.table(
    "gold.data_quality_results"
).count()

# COMMAND ----------

quality_score = (
    (total_checks - failed_checks)
    / total_checks
) * 100

# COMMAND ----------

print(quality_score)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 — Extract Audit Values

# COMMAND ----------

latest_audit = audit_df.collect()[0]

# COMMAND ----------

pipeline_name = latest_audit["pipeline_name"]
rows_processed = latest_audit["rows_processed"]
runtime_seconds = latest_audit["duration_seconds"]

# COMMAND ----------

# MAGIC %md
# MAGIC # 6 — Create Metrics Record

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

metrics_record = [
    (
        pipeline_name,
        rows_processed,
        runtime_seconds,
        failed_checks,
        quality_score,
        datetime.now()
    )
]

# COMMAND ----------

metrics_df = spark.createDataFrame(
    metrics_record,
    schema=metrics_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7 — Save Metrics

# COMMAND ----------

metrics_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("gold.pipeline_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8 — View Monitoring Table

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM gold.pipeline_metrics
    ORDER BY execution_time DESC
    """)
)