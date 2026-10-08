# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("rows_processed", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# COMMAND ----------

empty_audit_df = spark.createDataFrame([], audit_schema)

empty_audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.pipeline_audit")

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM gold.pipeline_audit
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 2 -- Imports

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 3 -- Start Audit Function

# COMMAND ----------

def start_audit():

    return datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 4 -- End Audit Function

# COMMAND ----------

def end_audit(
    pipeline_name,
    start_time,
    rows_processed,
    status,
    error_message=None
):
    
    end_time = datetime.now()

    duration = (
        end_time - start_time
    ).total_seconds()

    audit_record = [
        (
            pipeline_name,
            start_time,
            end_time,
            duration,
            rows_processed,
            status,
            error_message
        )
    ]

    audit_df = spark.createDataFrame(
        audit_record,
        schema=audit_schema
    )

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("gold.pipeline_audit")

    print(
        f"{pipeline_name} logged successfully"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 5 -- Test the Framework

# COMMAND ----------

start_time = start_audit()

# COMMAND ----------

rows_processed = spark.table(
    "silver.silver_flights"
).count()

print(rows_processed)

# COMMAND ----------

end_audit(
    pipeline_name="silver_row_count_test",
    start_time=start_time,
    rows_processed=rows_processed,
    status="SUCCESS"
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM gold.pipeline_audit
        ORDER BY start_time DESC
    """)
)