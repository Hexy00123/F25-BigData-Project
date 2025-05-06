-- Query 4: Trip Distance Analysis
USE team11_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE TABLE q4_results AS
SELECT 
    CASE 
        WHEN trip_distance < 1 THEN '0-1'
        WHEN trip_distance < 3 THEN '1-3'
        WHEN trip_distance < 5 THEN '3-5'
        WHEN trip_distance < 10 THEN '5-10'
        ELSE '10+'
    END AS distance_range,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(total_amount), 2) AS avg_total,
    ROUND(AVG(passenger_count), 2) AS avg_passengers
FROM 
    taxi_trips_part
GROUP BY 
    CASE 
        WHEN trip_distance < 1 THEN '0-1'
        WHEN trip_distance < 3 THEN '1-3'
        WHEN trip_distance < 5 THEN '3-5'
        WHEN trip_distance < 10 THEN '5-10'
        ELSE '10+'
    END
ORDER BY 
    distance_range;
    
SELECT * FROM q1_results;