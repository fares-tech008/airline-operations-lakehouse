# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Results Table

# COMMAND ----------

dq_schema = StructType([
    StructField("check_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("failed_records", LongType(), True),
    StructField("execution_time", TimestampType(), True)
])

empty_df = spark.createDataFrame([], dq_schema)

empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.data_quality_results")

print("DQ Results Table Created")

# COMMAND ----------

spark.sql("""
SELECT *
FROM gold.data_quality_results
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Silver Table

# COMMAND ----------

silver_df = spark.table("silver.silver_flights")

print("Rows:", silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Reusable Function

# COMMAND ----------

def log_check(check_name, table_name, failed_count):
    
    status = "PASS" if failed_count == 0 else "FAIL"

    result = [
        (
            check_name,
            table_name,
            status,
            failed_count,
            datetime.now()
        )
    ]

    result_df = spark.createDataFrame(
        result,
        [
            "check_name",
            "table_name",
            "status",
            "failed_records",
            "execution_time"
        ]
    )

    result_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("gold.data_quality_results")

    print(
        f"{check_name}: {status} ({failed_count} failures)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Check 1: Null Flight Date

# COMMAND ----------

null_dates = silver_df.filter(
    F.col("FL_DATE").isNull()
).count()

log_check(
    "Null FL_DATE Check",
    "silver_flights",
    null_dates
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check 2: Negative Distance

# COMMAND ----------

negative_distance = silver_df.filter(
    F.col("DISTANCE") < 0
).count()

log_check(
    "Negative Distance Check",
    "silver_flights",
    negative_distance
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check 3: Invalid Cancellation Flag

# COMMAND ----------

invalid_cancel = silver_df.filter(
    ~F.col("CANCELLED").isin([0,1])
).count()

log_check(
    "Cancellation Flag Check",
    "silver_flights",
    invalid_cancel
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check 4: Future Flight Dates

# COMMAND ----------

future_dates = silver_df.filter(
    F.col("FL_DATE") > F.current_timestamp()
).count()

log_check(
    "Future Date Check",
    "silver_flights",
    future_dates
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check 5: Duplicate Rows

# COMMAND ----------

total_rows = silver_df.count()

distinct_rows = silver_df.distinct().count()

duplicates = total_rows - distinct_rows

log_check(
    "Duplicate Check",
    "silver_flights",
    duplicates
)

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM gold.data_quality_results
    ORDER BY execution_time DESC
    """)
)