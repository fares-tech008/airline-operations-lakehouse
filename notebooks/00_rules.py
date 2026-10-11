# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Rules (shared transformation code)
# MAGIC All Silver and Gold business rules live here as functions. Notebooks 02 and 03 call them with `%run ./00_rules`,
# MAGIC and the tests in `tests/` import this same file, so the tests check the exact code the pipeline runs.
# MAGIC This notebook only DEFINES functions. It does not read or write any table.

# COMMAND ----------

from pyspark.sql import functions as F

DECIMAL_COLUMNS = ["DISTANCE", "DEP_DELAY", "ARR_DELAY",
                   "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]
WHOLE_COLUMNS = ["YEAR", "MONTH", "CANCELLED", "DIVERTED"]
DELAY_CAUSES = {
    "Carrier": "CARRIER_DELAY", "Weather": "WEATHER_DELAY", "NAS": "NAS_DELAY",
    "Security": "SECURITY_DELAY", "Late Aircraft": "LATE_AIRCRAFT_DELAY",
}
DELAY_THRESHOLD_MINUTES = 15
CARRIER_CANDIDATES = ["OP_UNIQUE_CARRIER", "OP_CARRIER", "CARRIER", "MKT_UNIQUE_CARRIER"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver rules

# COMMAND ----------

def parse_flight_date(column="FL_DATE"):
    """Text -> date. Two known formats; anything else becomes NULL (never an error)."""
    return F.to_date(F.coalesce(
        F.expr(f"try_to_timestamp({column}, 'M/d/yyyy h:mm:ss a')"),
        F.expr(f"try_to_timestamp({column}, 'yyyy-MM-dd')"),
    ))


def cast_numbers(df):
    """Text -> numbers with try_cast: a bad value becomes NULL instead of crashing."""
    for c in DECIMAL_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS DOUBLE)"))
    for c in WHOLE_COLUMNS:
        if c in df.columns:
            df = df.withColumn(c, F.expr(f"try_cast(try_cast(`{c}` AS DOUBLE) AS INT)"))
    return df


def clean_airport_codes(df):
    for c in ["ORIGIN", "DEST"]:
        df = df.withColumn(c, F.upper(F.trim(F.col(c))))
    return df


def add_flight_status(df):
    return df.withColumn(
        "FLIGHT_STATUS",
        F.when(F.col("CANCELLED") == 1, "CANCELLED")
         .when(F.col("DIVERTED") == 1, "DIVERTED")
         .when((F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0), "COMPLETED")
         .otherwise("UNKNOWN"),
    )


def add_is_delayed(df):
    """1 = arrived 15+ min late, 0 = on time, NULL = not a completed flight with an arrival delay."""
    return df.withColumn(
        "IS_DELAYED",
        F.when(
            (F.col("FLIGHT_STATUS") == "COMPLETED") & F.col("ARR_DELAY").isNotNull(),
            F.when(F.col("ARR_DELAY") >= DELAY_THRESHOLD_MINUTES, 1).otherwise(0),
        ).cast("int"),
    )


def add_distance_category(df):
    return df.withColumn(
        "DISTANCE_CATEGORY",
        F.when(F.col("DISTANCE") < 500, "SHORT_HAUL")
         .when(F.col("DISTANCE") <= 1500, "MEDIUM_HAUL")
         .when(F.col("DISTANCE").isNotNull(), "LONG_HAUL"),
    )


def remove_exact_duplicates(df):
    df = df.drop("_ingested_at")
    business_columns = [c for c in df.columns if not c.startswith("_")]
    return df.dropDuplicates(business_columns)


def build_silver(bronze_df):
    """Bronze (all text) -> Silver (typed, clean, deduplicated)."""
    df = bronze_df.withColumn("FL_DATE", parse_flight_date("FL_DATE"))
    df = cast_numbers(df)
    df = clean_airport_codes(df)
    df = add_flight_status(df)
    df = add_is_delayed(df)
    df = add_distance_category(df)
    return remove_exact_duplicates(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold rules

# COMMAND ----------

def find_column(columns, candidates):
    return next((c for c in candidates if c in columns), None)


def build_dim_date(spark, start, end):
    """Full calendar (one row per day) from start to end, inclusive. start/end are 'YYYY-MM-DD' text."""
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS FULL_DATE")
    iso_dow = F.expr("weekday(FULL_DATE) + 1")   # 1 = Monday ... 7 = Sunday
    return days.select(
        F.date_format("FULL_DATE", "yyyyMMdd").cast("int").alias("DATE_KEY"),
        "FULL_DATE",
        F.year("FULL_DATE").alias("YEAR"),
        F.quarter("FULL_DATE").alias("QUARTER"),
        F.month("FULL_DATE").alias("MONTH"),
        F.date_format("FULL_DATE", "MMMM").alias("MONTH_NAME"),
        F.dayofmonth("FULL_DATE").alias("DAY"),
        iso_dow.alias("DAY_OF_WEEK"),
        F.date_format("FULL_DATE", "EEEE").alias("DAY_NAME"),
        (iso_dow >= 6).alias("IS_WEEKEND"),
    )


def build_dim_airport(silver_df):
    """One row per airport code, taken from BOTH the origin and the destination columns."""
    cols = silver_df.columns
    origin_state = find_column(cols, ["ORIGIN_STATE_NM", "ORIGIN_STATE_ABR"])
    dest_state = find_column(cols, ["DEST_STATE_NM", "DEST_STATE_ABR"])
    dest_city = "DEST_CITY_NAME" if "DEST_CITY_NAME" in cols else None
    origin_city = "ORIGIN_CITY_NAME" if "ORIGIN_CITY_NAME" in cols else None

    def side(code, city, state):
        return silver_df.select(
            F.col(code).alias("AIRPORT_CODE"),
            (F.col(city) if city else F.lit(None)).cast("string").alias("CITY_NAME"),
            (F.col(state) if state else F.lit(None)).cast("string").alias("STATE_NAME"),
        )

    both = side("ORIGIN", origin_city, origin_state).unionByName(side("DEST", dest_city, dest_state))
    return (both.where(F.col("AIRPORT_CODE").isNotNull())
            .groupBy("AIRPORT_CODE")
            .agg(F.max("CITY_NAME").alias("CITY_NAME"), F.max("STATE_NAME").alias("STATE_NAME")))


def build_fact_flights(silver_df):
    """Grain: one row per flight. DATE_KEY -> dim_date, ORIGIN and DEST -> dim_airport."""
    cols = silver_df.columns
    selected = [F.date_format("FL_DATE", "yyyyMMdd").cast("int").alias("DATE_KEY"), F.col("FL_DATE")]
    carrier = find_column(cols, CARRIER_CANDIDATES)
    if carrier:
        selected.append(F.col(carrier).alias("AIRLINE_CODE"))
    selected += [F.col(c) for c in ["ORIGIN", "DEST", "DISTANCE", "DISTANCE_CATEGORY", "FLIGHT_STATUS",
                                    "CANCELLED", "DIVERTED", "DEP_DELAY", "ARR_DELAY", "IS_DELAYED"]]
    selected += [F.col(c) for c in DELAY_CAUSES.values() if c in cols]
    return silver_df.select(*selected)


def pct(numerator, denominator):
    """Percentage that gives NULL (not an error) when the denominator is 0."""
    return F.expr(f"round(100.0 * try_divide({numerator}, {denominator}), 2)")


def delay_metrics():
    """Delay rate uses COMPLETED flights only: COUNT(IS_DELAYED) skips NULLs."""
    return [
        F.count("*").alias("TOTAL_FLIGHTS"),
        F.count("IS_DELAYED").alias("COMPLETED_FLIGHTS"),
        F.sum("IS_DELAYED").alias("DELAYED_FLIGHTS"),
        pct("sum(IS_DELAYED)", "count(IS_DELAYED)").alias("DELAY_RATE_PCT"),
        F.round(F.avg("ARR_DELAY"), 2).alias("AVG_ARR_DELAY"),
    ]


def build_airport_performance(fact, dim_airport):
    agg = fact.groupBy("ORIGIN").agg(*delay_metrics(),
                                     pct("sum(CANCELLED)", "count(*)").alias("CANCELLATION_RATE_PCT"))
    cities = dim_airport.select(F.col("AIRPORT_CODE").alias("_c"), "CITY_NAME")
    return agg.join(cities, agg["ORIGIN"] == cities["_c"], "left").drop("_c")


def build_monthly_trends(fact, dim_date):
    months = dim_date.select("DATE_KEY", "YEAR", "MONTH", "MONTH_NAME")
    return (fact.join(months, "DATE_KEY")
            .groupBy("YEAR", "MONTH", "MONTH_NAME")
            .agg(*delay_metrics(), pct("sum(CANCELLED)", "count(*)").alias("CANCELLATION_RATE_PCT"))
            .orderBy("YEAR", "MONTH"))


def build_route_performance(fact):
    return fact.groupBy("ORIGIN", "DEST").agg(*delay_metrics())


def build_delay_causes(fact):
    """Total delay minutes and hours per cause, computed in ONE pass over the fact table."""
    present = {label: col for label, col in DELAY_CAUSES.items() if col in fact.columns}
    totals = fact.agg(*[F.coalesce(F.sum(c), F.lit(0.0)).alias(c) for c in present.values()])
    pairs = ", ".join(f"'{label}', {c}" for label, c in present.items())
    return (totals.selectExpr(f"stack({len(present)}, {pairs}) AS (DELAY_CAUSE, TOTAL_DELAY_MINUTES)")
            .withColumn("TOTAL_DELAY_HOURS", F.round(F.col("TOTAL_DELAY_MINUTES") / 60, 2)))
