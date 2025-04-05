START TRANSACTION; 

DROP TABLE IF EXISTS taxi_trips CASCADE;

-- Create main table
CREATE TABLE IF NOT EXISTS taxi_trips (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    RatecodeID INTEGER,
    store_and_fwd_flag CHAR(1),
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
);

-- Add constraints

-- Pickup time is before dropoff time
-- ALTER TABLE taxi_trips ADD CONSTRAINT chk_pickup_before_dropoff CHECK (tpep_pickup_datetime <= tpep_dropoff_datetime);

--  RatecodeID is within valid range
-- ALTER TABLE taxi_trips ADD CONSTRAINT chk_ratecodeid CHECK (RatecodeID BETWEEN 1 AND 6);

-- payment_type is within valid range
ALTER TABLE taxi_trips ADD CONSTRAINT chk_payment_type CHECK (payment_type BETWEEN 1 AND 6);

ALTER DATABASE team11_projectdb SET datestyle TO iso, ymd;

COMMIT;