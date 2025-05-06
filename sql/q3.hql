-- Query 3: General info (like ds.decribe())
USE team11_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE TABLE q3_results AS
SELECT 'count' AS stat,
       COUNT(vendorid) AS vendorid,
       COUNT(passenger_count) AS passenger_count,
       COUNT(trip_distance) AS trip_distance,
       COUNT(pickup_longitude) AS pickup_longitude,
       COUNT(pickup_latitude) AS pickup_latitude,
       COUNT(ratecodeid) AS ratecodeid,
       COUNT(dropoff_longitude) AS dropoff_longitude,
       COUNT(dropoff_latitude) AS dropoff_latitude,
       COUNT(fare_amount) AS fare_amount,
       COUNT(extra) AS extra,
       COUNT(mta_tax) AS mta_tax,
       COUNT(tip_amount) AS tip_amount,
       COUNT(tolls_amount) AS tolls_amount,
       COUNT(improvement_surcharge) AS improvement_surcharge,
       COUNT(total_amount) AS total_amount,
       COUNT(payment_type) AS payment_type
FROM team11_projectdb.taxi_trips
WHERE RAND() <= 0.01

UNION ALL

SELECT 'mean',
       AVG(vendorid),
       AVG(passenger_count),
       AVG(trip_distance),
       AVG(pickup_longitude),
       AVG(pickup_latitude),
       AVG(ratecodeid),
       AVG(dropoff_longitude),
       AVG(dropoff_latitude),
       AVG(fare_amount),
       AVG(extra),
       AVG(mta_tax),
       AVG(tip_amount),
       AVG(tolls_amount),
       AVG(improvement_surcharge),
       AVG(total_amount),
       AVG(payment_type)
FROM team11_projectdb.taxi_trips
WHERE RAND() <= 0.01

UNION ALL

SELECT 'std',
       STDDEV(vendorid),
       STDDEV(passenger_count),
       STDDEV(trip_distance),
       STDDEV(pickup_longitude),
       STDDEV(pickup_latitude),
       STDDEV(ratecodeid),
       STDDEV(dropoff_longitude),
       STDDEV(dropoff_latitude),
       STDDEV(fare_amount),
       STDDEV(extra),
       STDDEV(mta_tax),
       STDDEV(tip_amount),
       STDDEV(tolls_amount),
       STDDEV(improvement_surcharge),
       STDDEV(total_amount),
       STDDEV(payment_type)
FROM team11_projectdb.taxi_trips
WHERE RAND() <= 0.01

UNION ALL

SELECT 'min',
       MIN(vendorid),
       MIN(passenger_count),
       MIN(trip_distance),
       MIN(pickup_longitude),
       MIN(pickup_latitude),
       MIN(ratecodeid),
       MIN(dropoff_longitude),
       MIN(dropoff_latitude),
       MIN(fare_amount),
       MIN(extra),
       MIN(mta_tax),
       MIN(tip_amount),
       MIN(tolls_amount),
       MIN(improvement_surcharge),
       MIN(total_amount),
       MIN(payment_type)
FROM team11_projectdb.taxi_trips
WHERE RAND() <= 0.01

UNION ALL

SELECT 'median',
       PERCENTILE_APPROX(CAST(vendorid AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(passenger_count AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(trip_distance AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(pickup_longitude AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(pickup_latitude AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(ratecodeid AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(dropoff_longitude AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(dropoff_latitude AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(fare_amount AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(extra AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(mta_tax AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(tip_amount AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(tolls_amount AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(improvement_surcharge AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(total_amount AS DOUBLE), 0.5),
       PERCENTILE_APPROX(CAST(payment_type AS DOUBLE), 0.5)
FROM team11_projectdb.taxi_trips
WHERE RAND() <= 0.01

UNION ALL

SELECT 'max',
       MAX(vendorid),
       MAX(passenger_count),
       MAX(trip_distance),
       MAX(pickup_longitude),
       MAX(pickup_latitude),
       MAX(ratecodeid),
       MAX(dropoff_longitude),
       MAX(dropoff_latitude),
       MAX(fare_amount),
       MAX(extra),
       MAX(mta_tax),
       MAX(tip_amount),
       MAX(tolls_amount),
       MAX(improvement_surcharge),
       MAX(total_amount),
       MAX(payment_type)
FROM team11_projectdb.taxi_trips
WHERE RAND() <= 0.01;
