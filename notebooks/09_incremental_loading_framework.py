# Databricks notebook source
# MAGIC %md
# MAGIC # 1 — Create File Tracking Table

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

tracker_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("processed_time", TimestampType(), True)
])

# COMMAND ----------

empty_tracker_df = spark.createDataFrame(
    [],
    tracker_schema
)

empty_tracker_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.file_tracker")

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM gold.file_tracker
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 — Simulate Available Files

# COMMAND ----------

available_files = [
    "2025_01.csv",
    "2025_02.csv",
    "2025_03.csv",
    "2025_04.csv",
    "2025_05.csv",
    "2025_06.csv",
    "2025_07.csv",
    "2025_08.csv",
    "2025_09.csv",
    "2025_10.csv",
    "2025_11.csv",
    "2025_12.csv"
]

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 — Get Previously Processed Files

# COMMAND ----------

processed_files = [
    row.file_name
    for row in spark.table(
        "gold.file_tracker"
    ).collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 — Detect New Files

# COMMAND ----------

new_files = [
    file
    for file in available_files
    if file not in processed_files
]

# COMMAND ----------

print(new_files)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 — Process New Files

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

for file in new_files:

    print(f"Processing {file}")

    tracking_record = [
        (
            file,
            datetime.now()
        )
    ]

    tracking_df = spark.createDataFrame(
        tracking_record,
        tracker_schema
    )

    tracking_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("gold.file_tracker")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6 — Verify

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM gold.file_tracker
    ORDER BY processed_time DESC
    """)
)