# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    to_date,
    when,
    upper
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze

# COMMAND ----------

bronze_df = spark.read.table("workspace.bronze.bronze_flights")

print("Bronze Rows:", bronze_df.count())

# COMMAND ----------

silver_df = bronze_df

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning

# COMMAND ----------

silver_df.select("FL_DATE").show(10, False)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

silver_df = silver_df.withColumn(
    "FL_DATE",
    to_timestamp(
        col("FL_DATE"),
        "M/d/yyyy h:mm:ss a"
    )
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# Standardize Airport Codes

silver_df = silver_df.withColumn(
    "ORIGIN",
    upper(col("ORIGIN"))
)

silver_df = silver_df.withColumn(
    "DEST",
    upper(col("DEST"))
)

# COMMAND ----------

# Delay Standard

silver_df = silver_df.withColumn(
    "IS_DELAYED",
    when(col("ARR_DELAY") >= 15, 1)
    .otherwise(0)
)

# COMMAND ----------

# Distance Category

silver_df = silver_df.withColumn(
    "DISTANCE_CATEGORY",
    when(col("DISTANCE") < 500, "SHORT_HAUL")
     .when(
         (col("DISTANCE") >= 500) &
         (col("DISTANCE") <= 1500),
         "MEDIUM_HAUL"
     )
     .otherwise("LONG_HAUL")

)

# COMMAND ----------

print("Silver Rows:", silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ## IS_DELAYED

# COMMAND ----------


silver_df.groupBy("IS_DELAYED").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DISTANCE_CATEGORY

# COMMAND ----------

silver_df.groupBy("DISTANCE_CATEGORY").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Silver Table

# COMMAND ----------

# Write Silver Table

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.silver_flights")

# COMMAND ----------

spark.table("silver.silver_flights").count()