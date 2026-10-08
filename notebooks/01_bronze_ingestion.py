# Databricks notebook source
# MAGIC %md
# MAGIC # Read Raw Files

# COMMAND ----------

# Read all flight files

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/workspace/bronze/airline_raw_files/*.csv")
)

print("Rows:", df.count())
print("Columns:", len(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Validation

# COMMAND ----------

df.printSchema()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM workspace.bronze.bronze_flights
""").show()

# COMMAND ----------

bronze_df = spark.table("bronze.bronze_flights")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Assessment

# COMMAND ----------

# Remove duplicates
bronze_df = bronze_df.dropDuplicates()

print("Rows after deduplication:", bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Bronze Table

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.bronze_flights")

# COMMAND ----------

# MAGIC %md
# MAGIC # Table Metadata

# COMMAND ----------

spark.sql("""
DESCRIBE workspace.bronze.bronze_flights    
""").show(50, False)