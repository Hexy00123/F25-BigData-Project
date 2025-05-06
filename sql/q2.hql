-- Query 2: Average Fare Amount by Passenger Count
USE team11_projectdb;

DROP TABLE IF EXISTS q2_results;
CREATE TABLE q2_results AS
SELECT 
    passenger_count,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(total_amount), 2) AS avg_total
FROM 
    taxi_trips_part
WHERE 
    passenger_count BETWEEN 1 AND 6
GROUP BY 
    passenger_count
ORDER BY 
    passenger_count;
    
SELECT * FROM q1_results;