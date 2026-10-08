-- ============================================================
-- Airline Operations Lakehouse
-- Data Validation Queries
-- ============================================================

-- 1. Check for missing flight dates

SELECT
    COUNT(*) AS null_flight_dates
FROM workspace.silver.silver_flights
WHERE FL_DATE IS NULL;


-- 2. Check for missing origin airports

SELECT
    COUNT(*) AS null_origin_airports
FROM workspace.silver.silver_flights
WHERE ORIGIN IS NULL;


-- 3. Check for missing destination airports

SELECT
    COUNT(*) AS null_destination_airports
FROM workspace.silver.silver_flights
WHERE DEST IS NULL;


-- 4. Check for invalid flight distances

SELECT
    COUNT(*) AS invalid_distance_rows
FROM workspace.silver.silver_flights
WHERE DISTANCE <= 0;


-- 5. Check for extreme departure delays

SELECT
    COUNT(*) AS extreme_departure_delays
FROM workspace.silver.silver_flights
WHERE DEP_DELAY > 1440;


-- 6. Check for extreme arrival delays

SELECT
    COUNT(*) AS extreme_arrival_delays
FROM workspace.silver.silver_flights
WHERE ARR_DELAY > 1440;


-- 7. Check for duplicate flights

SELECT
    FL_DATE,
    ORIGIN,
    DEST,
    COUNT(*) AS duplicate_count
FROM workspace.silver.silver_flights
GROUP BY
    FL_DATE,
    ORIGIN,
    DEST
HAVING COUNT(*) > 1;


-- 8. Validate cancellation flag

SELECT
    CANCELLED,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY CANCELLED;


-- 9. Validate diversion flag

SELECT
    DIVERTED,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY DIVERTED;


-- 10. Delayed flight distribution

SELECT
    IS_DELAYED,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY IS_DELAYED;