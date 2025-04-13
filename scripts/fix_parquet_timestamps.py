from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Fix Timestamps for Hive") \
    .getOrCreate()

df = spark.read.parquet("hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/warehouse/taxi_trips")

df_fixed = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("string")) \
             .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("string"))

df_fixed.write.mode("overwrite").parquet("hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/warehouse/taxi_trips_fixed")

