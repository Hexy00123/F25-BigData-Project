-- Query 7: Tip Analysis by Payment Type
USE team11_projectdb;

DROP TABLE IF EXISTS q7_results;
CREATE TABLE q7_results AS
SELECT 
    payment_type,
    COUNT(*) AS trip_count,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(MAX(tip_amount), 2) AS max_tip,
    ROUND(MIN(tip_amount), 2) AS min_tip,
    ROUND(PERCENTILE(CAST(tip_amount AS BIGINT), 0.5), 2) AS median_tip
FROM 
    taxi_trips_part
GROUP BY 
    payment_type
ORDER BY 
    avg_tip DESC;
    
SELECT * FROM q1_results;