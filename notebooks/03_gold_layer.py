# Databricks notebook source
# MAGIC %run ./00_audit_helpers

# COMMAND ----------

from datetime import datetime
started = datetime.now()

# COMMAND ----------

# MAGIC %run ./00_rules

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Gold layer
# MAGIC Star schema (dim_date, dim_airport, fact_flights) and the 4 marts built from it. The rules live in `00_rules` (tested in `tests/`).

# COMMAND ----------

from pyspark.sql import functions as F

SILVER = "workspace.silver.silver_flights"
GOLD = "workspace.gold"

silver = spark.table(SILVER)
print("Silver rows:", f"{silver.count():,}")


def save(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{name}"))
    print(f"{name}: {spark.table(f'{GOLD}.{name}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

bounds = silver.agg(F.min("FL_DATE").alias("d0"), F.max("FL_DATE").alias("d1")).first()
save(build_dim_date(spark, f"{bounds.d0.year}-01-01", f"{bounds.d1.year}-12-31"), "dim_date")
save(build_dim_airport(silver), "dim_airport")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact table (one row per flight)

# COMMAND ----------

save(build_fact_flights(silver), "fact_flights")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marts (built from the star schema)

# COMMAND ----------

fact = spark.table(f"{GOLD}.fact_flights")
dim_date = spark.table(f"{GOLD}.dim_date")
dim_airport = spark.table(f"{GOLD}.dim_airport")

save(build_airport_performance(fact, dim_airport), "airport_performance")
save(build_monthly_trends(fact, dim_date), "monthly_trends")
save(build_route_performance(fact), "route_performance")
save(build_delay_causes(fact), "delay_causes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick proof

# COMMAND ----------

print("Overall delay rate (completed flights only):")
display(spark.sql(f"""
    SELECT ROUND(100.0 * SUM(DELAYED_FLIGHTS) / SUM(COMPLETED_FLIGHTS), 2) AS DELAY_RATE_PCT,
           SUM(TOTAL_FLIGHTS) AS FLIGHTS_IN_MONTHLY
    FROM {GOLD}.monthly_trends"""))

# COMMAND ----------

log_step("03_gold", "SUCCESS", rows=spark.table(f"{GOLD}.fact_flights").count(), started=started)
