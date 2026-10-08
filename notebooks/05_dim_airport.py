# Databricks notebook source
# MAGIC %md
# MAGIC # 1- Load Silver Data

# COMMAND ----------

silver_df = spark.table("silver.silver_flights")

display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2- Build Origin Airport Dimension

# COMMAND ----------

origin_airports_df = (
    silver_df
    .select(
        "ORIGIN",
        "ORIGIN_CITY_NAME"
    )
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3- Rename Columns

# COMMAND ----------

from pyspark.sql.functions import col

dim_airport_df = (
    origin_airports_df
    .withColumnRenamed(
        "ORIGIN",
        "AIRPORT_CODE"
    )
    .withColumnRenamed(
        "ORIGIN_CITY_NAME",
        "CITY_NAME"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4- Validate

# COMMAND ----------

print(
    "Airport Count:",
    dim_airport_df.count()
)

display(
    dim_airport_df.orderBy("AIRPORT_CODE")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5- Save Table

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS gold.dim_airport
""")

dim_airport_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_airport")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6- Verify

# COMMAND ----------

spark.sql("""
SELECT *
FROM gold.dim_airport
LIMIT 20
""").show()