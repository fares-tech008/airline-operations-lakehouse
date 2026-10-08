# Databricks notebook source
# MAGIC %md
# MAGIC # 1- Load Data

# COMMAND ----------

fact_df = spark.table("gold.fact_flights")

display(
    fact_df.select("FL_DATE").limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2- Extract Unique Dates

# COMMAND ----------

date_df = (
    fact_df
    .select("FL_DATE")
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3- Create Date Attributes

# COMMAND ----------

from pyspark.sql.functions import(
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    date_format
)

dim_date_df = (
    date_df
    .withColumn(
        "YEAR",
        year("FL_DATE")
    )
    .withColumn(
        "MONTH",
        month("FL_DATE")
    )
    .withColumn(
        "MONTH_NAME",
        date_format("FL_DATE", "MMMM")
    )
    .withColumn(
        "QUARTER",
        quarter("FL_DATE")
    )
    .withColumn(
        "DAY",
        dayofmonth("FL_DATE")
    )
    .withColumn(
        "DAY_OF_WEEK",
        date_format("FL_DATE", "EEEE")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4- Verify

# COMMAND ----------

print(
    "Date Count:",
    dim_date_df.count()
)

display(
    dim_date_df.orderBy("FL_DATE")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5- Save Table

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS gold.dim_date       
""")

dim_date_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_date")