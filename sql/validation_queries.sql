-- ============================================================
-- Airline Operations Lakehouse
-- Data Validation Queries 
-- ============================================================

-- 1. Missing flight dates 

SELECT COUNT(*) AS null_flight_dates
FROM workspace.silver.silver_flights
WHERE FL_DATE IS NULL;


-- 2. Missing origin airports

SELECT COUNT(*) AS null_origin_airports
FROM workspace.silver.silver_flights
WHERE ORIGIN IS NULL;


-- 3. Missing destination airports

SELECT COUNT(*) AS null_destination_airports
FROM workspace.silver.silver_flights
WHERE DEST IS NULL;


-- 4. Invalid flight distances 

SELECT COUNT(*) AS invalid_distance_rows
FROM workspace.silver.silver_flights
WHERE DISTANCE IS NOT NULL AND DISTANCE <= 0;


-- 5. Extreme departure delays (more than 24 hours)

SELECT COUNT(*) AS extreme_departure_delays
FROM workspace.silver.silver_flights
WHERE DEP_DELAY > 1440;


-- 6. Extreme arrival delays (more than 24 hours)

SELECT COUNT(*) AS extreme_arrival_delays
FROM workspace.silver.silver_flights
WHERE ARR_DELAY > 1440;


-- 7. Flight status breakdown (the groups must add up to the total)

SELECT
    FLIGHT_STATUS,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY FLIGHT_STATUS
ORDER BY flight_count DESC;


-- 8. Cancellation flag

SELECT
    CANCELLED,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY CANCELLED;


-- 9. Diversion flag

SELECT
    DIVERTED,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY DIVERTED;


-- 10. Delay flag (NULL = not a completed flight with an arrival delay)

SELECT
    IS_DELAYED,
    COUNT(*) AS flight_count
FROM workspace.silver.silver_flights
GROUP BY IS_DELAYED;


-- 11. Completed flights with no arrival delay (known WARN: expected about 1)

SELECT *
FROM workspace.silver.silver_flights
WHERE FLIGHT_STATUS = 'COMPLETED' AND ARR_DELAY IS NULL;


-- 12. Gold fact table row count must equal Silver

SELECT
    (SELECT COUNT(*) FROM workspace.silver.silver_flights) AS silver_rows,
    (SELECT COUNT(*) FROM workspace.gold.fact_flights) AS fact_rows;
