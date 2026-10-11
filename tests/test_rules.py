"""Tests for notebooks/00_rules.py: the exact rules the Silver and Gold notebooks run."""
import datetime as dt

import pytest

BRONZE_SCHEMA = (
    "FL_DATE string, YEAR string, MONTH string, ORIGIN string, DEST string, "
    "ORIGIN_CITY_NAME string, DEST_CITY_NAME string, DISTANCE string, DEP_DELAY string, "
    "ARR_DELAY string, CANCELLED string, DIVERTED string, CARRIER_DELAY string, "
    "WEATHER_DELAY string, NAS_DELAY string, SECURITY_DELAY string, LATE_AIRCRAFT_DELAY string, "
    "_source_file string, _ingested_at string"
)


def bronze_row(**overrides):
    base = dict(FL_DATE="1/5/2025 12:00:00 AM", YEAR="2025", MONTH="1", ORIGIN="JFK", DEST="LAX",
                ORIGIN_CITY_NAME="New York, NY", DEST_CITY_NAME="Los Angeles, CA", DISTANCE="2475",
                DEP_DELAY="5", ARR_DELAY="20", CANCELLED="0.00", DIVERTED="0.00", CARRIER_DELAY="10",
                WEATHER_DELAY="0", NAS_DELAY="5", SECURITY_DELAY="0", LATE_AIRCRAFT_DELAY="5",
                _source_file="a.csv", _ingested_at="2025-01-01")
    base.update(overrides)
    return tuple(base[c.split()[0]] for c in BRONZE_SCHEMA.split(", "))


def make_bronze(spark, *rows):
    return spark.createDataFrame(list(rows), BRONZE_SCHEMA)


def silver_rows(spark, rules, *rows):
    return {r["ORIGIN"] + "-" + r["DEST"]: r for r in rules.build_silver(make_bronze(spark, *rows)).collect()}


# ------------------------------------------------------------------ Silver
def test_both_date_formats_are_read_and_bad_dates_become_null(spark, rules):
    df = make_bronze(spark,
                     bronze_row(ORIGIN="AAA", FL_DATE="1/5/2025 12:00:00 AM"),
                     bronze_row(ORIGIN="BBB", FL_DATE="2025-01-06"),
                     bronze_row(ORIGIN="CCC", FL_DATE="not a date"))
    out = {r["ORIGIN"]: r["FL_DATE"] for r in rules.build_silver(df).collect()}
    assert out["AAA"] == dt.date(2025, 1, 5)
    assert out["BBB"] == dt.date(2025, 1, 6)
    assert out["CCC"] is None


def test_bad_numbers_become_null_and_do_not_crash(spark, rules):
    row = silver_rows(spark, rules, bronze_row(
        DISTANCE="abc", ARR_DELAY="", DEP_DELAY="x"))["JFK-LAX"]
    assert row["DISTANCE"] is None and row["ARR_DELAY"] is None and row["DEP_DELAY"] is None


def test_airport_codes_are_trimmed_and_upper_cased(spark, rules):
    rows = silver_rows(spark, rules, bronze_row(ORIGIN=" jfk ", DEST="lax"))
    assert "JFK-LAX" in rows


@pytest.mark.parametrize("cancelled,diverted,expected", [
    ("0.00", "0.00", "COMPLETED"), ("1.00", "0.00", "CANCELLED"),
    ("0.00", "1.00", "DIVERTED"), ("", "0.00", "UNKNOWN"),
])
def test_flight_status(spark, rules, cancelled, diverted, expected):
    row = silver_rows(spark, rules, bronze_row(
        CANCELLED=cancelled, DIVERTED=diverted))["JFK-LAX"]
    assert row["FLIGHT_STATUS"] == expected


@pytest.mark.parametrize("arr_delay,expected", [("14", 0), ("15", 1), ("-10", 0), ("90", 1)])
def test_is_delayed_threshold_is_15_minutes(spark, rules, arr_delay, expected):
    row = silver_rows(spark, rules, bronze_row(ARR_DELAY=arr_delay))["JFK-LAX"]
    assert row["IS_DELAYED"] == expected


def test_cancelled_and_diverted_flights_are_not_counted_as_on_time(spark, rules):
    """The original bug: a cancelled flight has no ARR_DELAY and was counted as on time (0)."""
    rows = silver_rows(spark, rules,
                       bronze_row(ORIGIN="CAN", CANCELLED="1.00",
                                  ARR_DELAY=""),
                       bronze_row(ORIGIN="DIV", DIVERTED="1.00", ARR_DELAY=""))
    assert rows["CAN-LAX"]["IS_DELAYED"] is None
    assert rows["DIV-LAX"]["IS_DELAYED"] is None


def test_completed_flight_without_arrival_delay_is_not_counted(spark, rules):
    row = silver_rows(spark, rules, bronze_row(ARR_DELAY=""))["JFK-LAX"]
    assert row["FLIGHT_STATUS"] == "COMPLETED" and row["IS_DELAYED"] is None


@pytest.mark.parametrize("distance,expected", [
    ("499", "SHORT_HAUL"), ("500", "MEDIUM_HAUL"), ("1500", "MEDIUM_HAUL"),
    ("1501", "LONG_HAUL"), ("", None),
])
def test_distance_category(spark, rules, distance, expected):
    row = silver_rows(spark, rules, bronze_row(DISTANCE=distance))["JFK-LAX"]
    assert row["DISTANCE_CATEGORY"] == expected


def test_exact_duplicates_are_removed_but_different_flights_are_kept(spark, rules):
    df = make_bronze(spark,
                     bronze_row(), bronze_row(_ingested_at="2025-02-01"),   # same flight, loaded twice
                     bronze_row(ORIGIN="SFO"))
    assert rules.build_silver(df).count() == 2


# ------------------------------------------------------------------ Gold
def silver_df(spark, rules):
    return rules.build_silver(make_bronze(
        spark,
        # delayed
        bronze_row(ORIGIN="JFK", DEST="LAX", ARR_DELAY="20"),
        bronze_row(ORIGIN="JFK", DEST="LAX", ARR_DELAY="0",
                   FL_DATE="2025-01-06"),     # on time
        bronze_row(ORIGIN="JFK", DEST="LAX", CANCELLED="1.00",
                   ARR_DELAY="", FL_DATE="2025-01-07"),
        bronze_row(ORIGIN="LAX", DEST="ORD",
                   ORIGIN_CITY_NAME="Los Angeles, CA", DEST_CITY_NAME="Chicago, IL"),
    ))


def test_dim_date_is_a_full_calendar(spark, rules):
    dim = rules.build_dim_date(spark, "2025-01-01", "2025-12-31")
    assert dim.count() == 365
    assert dim.select("DATE_KEY").distinct().count() == 365
    # 2025-01-04 is a Saturday
    row = dim.where("DATE_KEY = 20250104").first()
    assert row["DAY_NAME"] == "Saturday" and row["IS_WEEKEND"] is True and row["DAY_OF_WEEK"] == 6
    assert dim.where("DATE_KEY = 20250106").first()[
        "IS_WEEKEND"] is False   # Monday


def test_dim_airport_contains_destination_only_airports(spark, rules):
    codes = {r["AIRPORT_CODE"]
             for r in rules.build_dim_airport(silver_df(spark, rules)).collect()}
    # ORD only ever appears as a destination
    assert codes == {"JFK", "LAX", "ORD"}


def test_dim_airport_has_one_row_per_code(spark, rules):
    dim = rules.build_dim_airport(silver_df(spark, rules))
    assert dim.count() == dim.select("AIRPORT_CODE").distinct().count()


def test_fact_keeps_one_row_per_flight_and_has_valid_keys(spark, rules):
    silver = silver_df(spark, rules)
    fact = rules.build_fact_flights(silver)
    dim_date = rules.build_dim_date(spark, "2025-01-01", "2025-12-31")
    dim_airport = rules.build_dim_airport(silver)
    assert fact.count() == silver.count()
    assert fact.join(dim_date, "DATE_KEY", "left_anti").count() == 0
    assert fact.join(dim_airport, fact.ORIGIN ==
                     dim_airport.AIRPORT_CODE, "left_anti").count() == 0
    assert fact.join(dim_airport, fact.DEST ==
                     dim_airport.AIRPORT_CODE, "left_anti").count() == 0


def test_delay_rate_uses_completed_flights_only(spark, rules):
    fact = rules.build_fact_flights(silver_df(spark, rules))
    route = {(r["ORIGIN"], r["DEST"])             : r for r in rules.build_route_performance(fact).collect()}
    jfk_lax = route[("JFK", "LAX")]
    # includes the cancelled flight
    assert jfk_lax["TOTAL_FLIGHTS"] == 3
    assert jfk_lax["COMPLETED_FLIGHTS"] == 2      # cancelled flight is skipped
    assert jfk_lax["DELAY_RATE_PCT"] == 50.0      # 1 of 2, NOT 1 of 3


def test_rate_is_null_instead_of_an_error_when_there_are_no_completed_flights(spark, rules):
    silver = rules.build_silver(make_bronze(
        spark, bronze_row(CANCELLED="1.00", ARR_DELAY="")))
    route = rules.build_route_performance(
        rules.build_fact_flights(silver)).first()
    assert route["COMPLETED_FLIGHTS"] == 0 and route["DELAY_RATE_PCT"] is None


def test_airport_performance_gets_city_from_dim_airport(spark, rules):
    silver = silver_df(spark, rules)
    perf = rules.build_airport_performance(
        rules.build_fact_flights(silver), rules.build_dim_airport(silver))
    jfk = perf.where("ORIGIN = 'JFK'").first()
    assert jfk["CITY_NAME"] == "New York, NY"
    assert jfk["CANCELLATION_RATE_PCT"] == round(100 / 3, 2)


def test_monthly_trends_adds_up_to_the_fact_table(spark, rules):
    silver = silver_df(spark, rules)
    fact = rules.build_fact_flights(silver)
    monthly = rules.build_monthly_trends(
        fact, rules.build_dim_date(spark, "2025-01-01", "2025-12-31"))
    assert monthly.agg({"TOTAL_FLIGHTS": "sum"}).first()[0] == fact.count()
    assert monthly.first()["MONTH_NAME"] == "January"


def test_delay_causes_are_summed_in_minutes_and_hours(spark, rules):
    silver = rules.build_silver(make_bronze(spark,
                                            bronze_row(
                                                ORIGIN="A", CARRIER_DELAY="60", WEATHER_DELAY="30"),
                                            bronze_row(ORIGIN="B", CARRIER_DELAY="60", WEATHER_DELAY="")))
    causes = {r["DELAY_CAUSE"]: r for r in rules.build_delay_causes(
        rules.build_fact_flights(silver)).collect()}
    assert causes["Carrier"]["TOTAL_DELAY_MINUTES"] == 120.0 and causes["Carrier"]["TOTAL_DELAY_HOURS"] == 2.0
    # the empty value is ignored, not an error
    assert causes["Weather"]["TOTAL_DELAY_MINUTES"] == 30.0
