-- ============================================================
-- Airline Operations Lakehouse
-- Business Analytics Queries
-- ============================================================

-- 1. Top 10 airports by average departure delay

SELECT 
    ORIGIN,
    AVG(DEP_DELAY) AS avg_departure_delay
FROM workspace.gold.fact_flights
WHERE DEP_DELAY IS NOT NULL
GROUP BY ORIGIN
ORDER BY avg_departure_delay DESC
LIMIT 10;


-- 2. Top 10 routes by average arrival delay

SELECT 
    ORIGIN,
    DEST,
    ROUND(AVG(ARR_DELAY), 2) AS avg_arrival_delay
FROM workspace.gold.fact_flights
WHERE ARR_DELAY IS NOT NULL
GROUP BY ORIGIN, DEST
ORDER BY avg_arrival_delay DESC
LIMIT 10;


-- 3. Monthly flight volume

SELECT 
    d.YEAR,
    d.MONTH,
    d.MONTH_NAME,
    COUNT(*) AS total_flights
FROM workspace.gold.fact_flights f
JOIN workspace.gold.dim_date d
    ON f.FL_DATE = d.FL_DATE
GROUP BY
    d.YEAR,
    d.MONTH,
    d.MONTH_NAME
ORDER BY 
    d.YEAR,
    d.MONTH;

-- 4. Delayed flight percentage

SELECT
    ROUND(
        100.0 * SUM(IS_DELAYED) / COUNT(*),
        2
    ) AS delayed_flight_percentage
FROM workspace.gold.fact_flights;


-- 5. Cancellation rate

SELECT
    ROUND(
        100.0 * SUM(CANCELLED) / COUNT(*),
        2
    ) AS cancellation_rate
FROM workspace.gold.fact_flights;


-- 6. Diversion rate

SELECT
    ROUND(
        100.0 * SUM(DIVERTED) / COUNT(*),
        2
    ) AS diversion_rate
FROM workspace.gold.fact_flights;


-- 7. Average delay by origin airport

SELECT
    a.AIRPORT_CODE,
    a.CITY_NAME,
    ROUND(AVG(f.DEP_DELAY), 2) AS avg_departure_delay
FROM workspace.gold.fact_flights f
JOIN workspace.gold.dim_airport a
    ON f.ORIGIN = a.AIRPORT_CODE
WHERE f.DEP_DELAY IS NOT NULL
GROUP BY
    a.AIRPORT_CODE,
    a.CITY_NAME
ORDER BY avg_departure_delay DESC;


-- 8. Average flight distance by route

SELECT
    ORIGIN,
    DEST,
    ROUND(AVG(DISTANCE), 2) AS avg_distance
FROM workspace.gold.fact_flights
GROUP BY
    ORIGIN,
    DEST
ORDER BY avg_distance DESC;