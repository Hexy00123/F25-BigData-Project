"""Fix of timestamp formats in parquet"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

OLD_PATH = "hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/warehouse/taxi_trips"
FIXED_PATH = "hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/warehouse/taxi_trips_fixed"

spark = SparkSession.builder \
    .appName("Fix Timestamps for Hive") \
    .getOrCreate()

df = spark.read.parquet(OLD_PATH)

df_fixed = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("string")) \
             .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("string"))

df_fixed.write.mode("overwrite").parquet(FIXED_PATH)
