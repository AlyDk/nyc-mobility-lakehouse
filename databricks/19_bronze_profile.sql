-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze data profile
-- MAGIC Counts the data quality issues the Silver rules must handle. Read only.

-- COMMAND ----------

SELECT
  'yellow' AS service,
  COUNT(*) AS rows,
  SUM(IF(fare_amount < 0 OR total_amount < 0, 1, 0)) AS negative_amounts,
  SUM(IF(trip_distance <= 0, 1, 0)) AS zero_distance,
  SUM(IF(trip_distance > 200, 1, 0)) AS distance_over_200mi,
  SUM(IF(tpep_dropoff_datetime <= tpep_pickup_datetime, 1, 0)) AS dropoff_not_after_pickup,
  SUM(IF(timestampdiff(HOUR, tpep_pickup_datetime, tpep_dropoff_datetime) > 24, 1, 0)) AS duration_over_24h,
  SUM(IF(date_format(tpep_pickup_datetime, 'yyyy-MM') <> _period, 1, 0)) AS pickup_outside_file_month,
  SUM(IF(passenger_count IS NULL, 1, 0)) AS passenger_null,
  SUM(IF(passenger_count = 0, 1, 0)) AS passenger_zero,
  SUM(IF(pulocationid IN (264, 265) OR dolocationid IN (264, 265), 1, 0)) AS unknown_zone
FROM nyc_mobility.bronze.yellow_trips
UNION ALL
SELECT
  'green',
  COUNT(*),
  SUM(IF(fare_amount < 0 OR total_amount < 0, 1, 0)),
  SUM(IF(trip_distance <= 0, 1, 0)),
  SUM(IF(trip_distance > 200, 1, 0)),
  SUM(IF(lpep_dropoff_datetime <= lpep_pickup_datetime, 1, 0)),
  SUM(IF(timestampdiff(HOUR, lpep_pickup_datetime, lpep_dropoff_datetime) > 24, 1, 0)),
  SUM(IF(date_format(lpep_pickup_datetime, 'yyyy-MM') <> _period, 1, 0)),
  SUM(IF(passenger_count IS NULL, 1, 0)),
  SUM(IF(passenger_count = 0, 1, 0)),
  SUM(IF(pulocationid IN (264, 265) OR dolocationid IN (264, 265), 1, 0))
FROM nyc_mobility.bronze.green_trips

-- COMMAND ----------

SELECT 'yellow' AS service, SUM(n - 1) AS duplicate_rows
FROM (
  SELECT COUNT(*) AS n FROM nyc_mobility.bronze.yellow_trips
  GROUP BY vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, trip_distance, total_amount
  HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'green', SUM(n - 1)
FROM (
  SELECT COUNT(*) AS n FROM nyc_mobility.bronze.green_trips
  GROUP BY vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, pulocationid, dolocationid, trip_distance, total_amount
  HAVING COUNT(*) > 1
)

-- COMMAND ----------

SELECT 'yellow' AS service, payment_type, COUNT(*) AS rows FROM nyc_mobility.bronze.yellow_trips GROUP BY payment_type
UNION ALL
SELECT 'green', payment_type, COUNT(*) FROM nyc_mobility.bronze.green_trips GROUP BY payment_type
ORDER BY service, payment_type