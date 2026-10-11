# Databricks notebook source
# MAGIC %run ./00_audit_helpers

# COMMAND ----------

from datetime import datetime
started = datetime.now()

# COMMAND ----------

# MAGIC %run ./00_rules

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Silver transformation
# MAGIC Turns the raw text in Bronze into clean, typed data. The rules live in `00_rules` (and are tested in `tests/`).

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.bronze.bronze_flights"
SILVER_TABLE = "workspace.silver.silver_flights"

bronze_df = spark.table(BRONZE_TABLE)
bronze_rows = bronze_df.count()
print("Bronze rows:", f"{bronze_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check that the columns we need exist

# COMMAND ----------

expected = ["FL_DATE", "ORIGIN", "DEST", "DISTANCE", "DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED"]
missing = [c for c in expected if c not in bronze_df.columns]
assert not missing, f"Bronze is missing columns: {missing}"
print("All expected columns found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform and write

# COMMAND ----------

silver_df = build_silver(bronze_df)

(silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the result

# COMMAND ----------

silver_check = spark.table(SILVER_TABLE)
silver_rows = silver_check.count()
print("Bronze rows:       ", f"{bronze_rows:,}")
print("Silver rows:       ", f"{silver_rows:,}")
print("Duplicates removed:", f"{bronze_rows - silver_rows:,}")
print("Rows with unreadable FL_DATE:", silver_check.filter(F.col("FL_DATE").isNull()).count())
display(silver_check.groupBy("FLIGHT_STATUS", "IS_DELAYED").count().orderBy("FLIGHT_STATUS", "IS_DELAYED"))

# COMMAND ----------

log_step("02_silver", "SUCCESS", rows=silver_rows, started=started)
