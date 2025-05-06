-- Query 5: Vendor Comparison
USE team11_projectdb;

DROP TABLE IF EXISTS q5_results;
CREATE TABLE q5_results AS
SELECT 
    VendorID,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(passenger_count), 2) AS avg_passengers
FROM 
    taxi_trips_part
GROUP BY 
    VendorID
ORDER BY 
    trip_count DESC;
    
SELECT * FROM q1_results;