-- Query 1: Payment Type Distribution Analysis
USE team11_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE TABLE q1_results AS
SELECT 
    payment_type,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM 
    taxi_trips_part
GROUP BY 
    payment_type
ORDER BY 
    trip_count DESC;
    
SELECT * FROM q1_results;