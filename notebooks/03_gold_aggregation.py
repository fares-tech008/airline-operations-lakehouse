# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Table 1 - Airport Performance

# COMMAND ----------

silver_df = spark.table("silver.silver_flights")

# COMMAND ----------

# Create Airport KPI Table

from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round,
    col,
)

gold_airport_df = (
    silver_df
    .groupBy(
        "ORIGIN",
        "ORIGIN_CITY_NAME"
    )
    .agg(
        count("*").alias("TOTAL_FLIGHTS"),

        sum("IS_DELAYED").alias("DELAYED_FLIGHTS"),

        round(
            (sum("IS_DELAYED") / count("*")) * 100,
            2
        ).alias("DELAY_RATE_PCT"),

        round(
            avg("ARR_DELAY"),
            2
        ).alias("AVG_ARR_DELAY"),

        round(
            (sum("CANCELLED") / count("*")) * 100,
            2
        ).alias("CANCELLATION_RATE_PCT")
    )
)

# COMMAND ----------

# View Results

display(
    gold_airport_df.orderBy(
        col("DELAY_RATE_PCT").desc()
    )
)

# COMMAND ----------

gold_airport_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.airport_performance")

# COMMAND ----------

spark.table("gold.airport_performance").count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 2 - Monthly Trends

# COMMAND ----------

# Create Monthly Trends Table

gold_monthly_df = (
    silver_df
    .groupBy(
        "YEAR",
        "MONTH"
    )
    .agg(
        count("*").alias("TOTAL_FLIGHTS"),

        sum("IS_DELAYED").alias("DELAYED_FLIGHTS"),

        round(
            (sum("IS_DELAYED") / count("*")) * 100,
            2
        ).alias("DELAY_RATE_PCT"),

        round(
            avg("ARR_DELAY"),
            2
        ).alias("AVG_ARR_DELAY"),

        round(
            (sum("CANCELLED") / count("*")) * 100,
            2
        ).alias("CANCELLATION_RATE_PCT")
    )
    .orderBy("YEAR", "MONTH")
)

# COMMAND ----------

display(gold_monthly_df)

# COMMAND ----------

gold_monthly_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.monthly_trends")

# COMMAND ----------

spark.table("gold.monthly_trends").show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 3 - Delay Causes

# COMMAND ----------

# The Data

delay_data = [
    ("Carrier", silver_df.agg({"CARRIER_DELAY": "sum"}).collect()[0][0] / 60),
    ("Weather", silver_df.agg({"WEATHER_DELAY": "sum"}).collect()[0][0] / 60),
    ("NAS", silver_df.agg({"NAS_DELAY": "sum"}).collect()[0][0] / 60),
    ("Security", silver_df.agg({"SECURITY_DELAY": "sum"}).collect()[0][0] / 60),
    ("Late Aircraft", silver_df.agg({"LATE_AIRCRAFT_DELAY": "sum"}).collect()[0][0] / 60)
]

# COMMAND ----------

# Gold DataFrame

gold_delay_causes_df = spark.createDataFrame(
    delay_data,
    ["DELAY_CAUSE", "TOTAL_DELAY_HOURS"]
)

# COMMAND ----------

# Round Values

from pyspark.sql.functions import round, col

gold_delay_causes_df = gold_delay_causes_df.withColumn(
    "TOTAL_DELAY_HOURS",
    round(col("TOTAL_DELAY_HOURS"), 2)
)

# COMMAND ----------

# View Results

display(
    gold_delay_causes_df.orderBy(
        col("TOTAL_DELAY_HOURS").desc()
    )
)

# COMMAND ----------

# Save The Table

gold_delay_causes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.delay_causes")

# COMMAND ----------

spark.table("gold.delay_causes").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 4 - Route Performance

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, round

gold_route_df = (
    silver_df
    .groupBy(
        "ORIGIN",
        "DEST"
    )
    .agg(
        count("*").alias("TOTAL_FLIGHTS"),
        sum("IS_DELAYED").alias("DELAYED_FLIGHTS"),
        round(
            (sum("IS_DELAYED") / count("*")) * 100,
            2
        ).alias("DELAY_RATE_PCT"),
        round(
            avg("ARR_DELAY"),
            2
        ).alias("AVG_ARR_DELAY")
    )
)

# COMMAND ----------

gold_route_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.route_performance")

# COMMAND ----------

spark.table("workspace.gold.route_performance").show()