# Databricks notebook source
# MAGIC %md
# MAGIC # 1- Load Silver

# COMMAND ----------

silver_df = spark.table("silver.silver_flights")

display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2- Select Warehouse Columns

# COMMAND ----------

fact_flights_df = (
    silver_df
    .select(
        "FL_DATE",
        "ORIGIN",
        "DEST",
        "DISTANCE",
        "DEP_DELAY",
        "ARR_DELAY",
        "CANCELLED",
        "DIVERTED",
        "IS_DELAYED"        
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3- Validate

# COMMAND ----------

print("Rows:", fact_flights_df.count())

display(fact_flights_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # 4- Save Table

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS gold.fact_flights       
""")

fact_flights_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.fact_flights")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5- Verify

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM gold.fact_flights
""").show()