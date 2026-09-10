-- NYC Taxi Analytics
-- Source: Silver NYC Yellow Taxi dataset
-- Query Engine: Amazon Athena


-- 1. Dataset overview
SELECT
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_value,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1';


-- 2. Daily trip and revenue trends
SELECT
    pickup_day,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_value
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1'
GROUP BY pickup_day
ORDER BY pickup_day;


-- 3. Payment method performance
SELECT
    payment_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_value,
    ROUND(AVG(tip_amount), 2) AS avg_tip
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1'
GROUP BY payment_type
ORDER BY total_trips DESC;


-- 4. Most active pickup locations
SELECT
    pickup_location_id,
    COUNT(*) AS total_pickups,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_value
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1'
GROUP BY pickup_location_id
ORDER BY total_pickups DESC
LIMIT 10;


-- 5. Most active dropoff locations
SELECT
    dropoff_location_id,
    COUNT(*) AS total_dropoffs,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1'
GROUP BY dropoff_location_id
ORDER BY total_dropoffs DESC
LIMIT 10;


-- 6. Trip distance analysis
SELECT
    CASE
        WHEN trip_distance < 1 THEN '< 1 mile'
        WHEN trip_distance < 3 THEN '1-3 miles'
        WHEN trip_distance < 5 THEN '3-5 miles'
        WHEN trip_distance < 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END AS distance_group,
    COUNT(*) AS total_trips,
    ROUND(AVG(total_amount), 2) AS avg_trip_value,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1'
GROUP BY 1
ORDER BY total_trips DESC;


-- 7. Tip analysis
SELECT
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(SUM(tip_amount), 2) AS total_tips,
    ROUND(
        100.0 * SUM(tip_amount) / NULLIF(SUM(total_amount), 0),
        2
    ) AS tips_pct_of_total
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1';


-- 8. Airport trip analysis
SELECT
    COUNT(*) AS airport_trips,
    ROUND(SUM(airport_fee), 2) AS total_airport_fees,
    ROUND(AVG(total_amount), 2) AS avg_trip_value
FROM yellow_taxi
WHERE pickup_year = '2025'
  AND pickup_month = '1'
  AND airport_fee > 0;