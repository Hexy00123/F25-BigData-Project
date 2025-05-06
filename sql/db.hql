SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;

-- Drop and recreate DB
DROP DATABASE IF EXISTS team11_projectdb CASCADE;
CREATE DATABASE team11_projectdb LOCATION 'project/hive/warehouse';
USE team11_projectdb;

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Create partitioned table in Parquet format
CREATE EXTERNAL TABLE taxi_trips_part (
    VendorID INT,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag STRING,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE
)
PARTITIONED BY (payment_type INT)
STORED AS PARQUET
LOCATION 'project/hive/warehouse/taxi_trips_part';

-- Create base table (unpartitioned) for the imported Sqoop data
CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips (
    VendorID INT,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag STRING,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type INT
)
STORED AS PARQUET
LOCATION 'project/warehouse/taxi_trips_fixed';

-- Insert into partitioned table
INSERT OVERWRITE TABLE taxi_trips_part
PARTITION (payment_type)
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
    RatecodeID,
    store_and_fwd_flag,
    dropoff_longitude,
    dropoff_latitude,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type
FROM taxi_trips;

-- Check result
SHOW PARTITIONS taxi_trips_part;
SELECT * FROM taxi_trips_part WHERE payment_type = 1 LIMIT 10;
