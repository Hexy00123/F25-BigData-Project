#!/usr/bin/env python3
"""
Stage 3: Spark ML modeling pipeline
Reads Hive table, preprocesses, trains baseline and tuned models, saves outputs
"""

import time
from pprint import pprint
import math
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col, hour, month, to_timestamp, from_unixtime
from pyspark.sql.functions import sin, cos, lit


class UnixMillisToTimestamp(Transformer):
    def __init__(self, inputCol=None, outputCol=None):
        super(UnixMillisToTimestamp, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, dataset):
        return dataset.withColumn(
            self.outputCol,
            to_timestamp(from_unixtime(col(self.inputCol)/1000))
        )


# Custom Transformer to extract hour and month
class ExtractHourMonth(Transformer):
    def __init__(self, inputCol=None, prefix="pickup"):
        super(ExtractHourMonth, self).__init__()
        self.inputCol = inputCol
        self.prefix = prefix

    def _transform(self, dataset):
        dataset = dataset.withColumn(f"{self.prefix}_hour",
                                     hour(col(self.inputCol)))
        return dataset.withColumn(f"{self.prefix}_month",
                                  month(col(self.inputCol)))


# Custom Transformer for cyclical encoding
class CyclicalEncoder(Transformer):
    def __init__(self):
        super(CyclicalEncoder, self).__init__()

    def _transform(self, dataset):
        dataset = dataset.withColumn(
            "pickup_hour_sin",
            sin(2 * math.pi * col("pickup_hour") / lit(24)))
        dataset = dataset.withColumn(
            "pickup_hour_cos",
            cos(2 * math.pi * col("pickup_hour") / lit(24)))
        dataset = dataset.withColumn(
            "pickup_month_sin",
            sin(2 * math.pi * col("pickup_month") / lit(12)))
        dataset = dataset.withColumn(
            "pickup_month_cos",
            cos(2 * math.pi * col("pickup_month") / lit(12)))
        dataset = dataset.withColumn(
            "dropoff_hour_sin",
            sin(2 * math.pi * col("dropoff_hour") / lit(24)))
        dataset = dataset.withColumn(
            "dropoff_hour_cos",
            cos(2 * math.pi * col("dropoff_hour") / lit(24)))
        dataset = dataset.withColumn(
            "dropoff_month_sin",
            sin(2 * math.pi * col("dropoff_month") / lit(12)))
        return dataset.withColumn(
            "dropoff_month_cos",
            cos(2 * math.pi * col("dropoff_month") / lit(12)))


# Custom Transformer to select features and rename label
class SelectAndRename(Transformer):
    def _transform(self, dataset):
        cols = [
            'total_amount', 'vendorid',
            'passenger_count', 'trip_distance',
            'pickup_longitude', 'pickup_latitude',
            'dropoff_longitude', 'dropoff_latitude',
            'pickup_hour_sin', 'pickup_hour_cos',
            'pickup_month_sin', 'pickup_month_cos',
            'dropoff_hour_sin', 'dropoff_hour_cos',
            'dropoff_month_sin', 'dropoff_month_cos'
        ]
        dataset = dataset.select(*cols)
        return dataset.withColumnRenamed("total_amount", "label")


def main():
    # 1. Init Spark
    spark = SparkSession.builder \
        .appName("team11 - stage3") \
        .master("yarn") \
        .config("hive.metastore.uris",
                "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir",
                "/user/team11/project/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    # 2. Read & clean
    df = spark.read.format("avro").table('team11_projectdb.taxi_trips')
    df = df.filter(col("total_amount").between(0, 1e5))

    # 3. Split
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # 4. Preprocessing + feature pipeline
    pipeline = Pipeline(stages=[
        UnixMillisToTimestamp("tpep_pickup_datetime", "pickup_ts"),
        UnixMillisToTimestamp("tpep_dropoff_datetime", "dropoff_ts"),
        ExtractHourMonth("pickup_ts", "pickup"),
        ExtractHourMonth("dropoff_ts", "dropoff"),
        CyclicalEncoder(),
        SelectAndRename(),
        VectorAssembler(
            inputCols=[
                'vendorid',
                'passenger_count', 'trip_distance',
                'pickup_longitude', 'pickup_latitude',
                'dropoff_longitude', 'dropoff_latitude',
                'pickup_hour_sin', 'pickup_hour_cos',
                'pickup_month_sin', 'pickup_month_cos',
                'dropoff_hour_sin', 'dropoff_hour_cos',
                'dropoff_month_sin', 'dropoff_month_cos'
            ], outputCol='features_raw'
        ),
        StandardScaler(inputCol='features_raw',
                       outputCol='features',
                       withMean=True)
    ])
    prep = pipeline.fit(train_df)
    train = prep.transform(train_df)
    test = prep.transform(test_df)

    # Save splits
    train.select("features", "label").coalesce(1)\
        .write.mode("overwrite").json("project/data/train")
    test.select("features", "label").coalesce(1)\
        .write.mode("overwrite").json("project/data/test")

    # 5. Define evaluators & models
    evaluator = RegressionEvaluator(labelCol="label",
                                    predictionCol="prediction")

    models_config = [
        {
            "name": "RandomForest",
            "estimator": RandomForestRegressor(featuresCol="features",
                                               labelCol="label"),
            "param_grid": ParamGridBuilder()
                    .addGrid(RandomForestRegressor().numTrees, [10, 40])
                    .addGrid(RandomForestRegressor().maxDepth, [5, 10])
                    .build(),
            "output_model": "model1",
            "output_pred": "model1_predictions_"
        },
        {
            "name": "GBT",
            "estimator": GBTRegressor(featuresCol="features",
                                      labelCol="label"),
            "param_grid": ParamGridBuilder()
                    .addGrid(GBTRegressor().maxDepth, [3, 8])
                    .addGrid(GBTRegressor().maxBins, [24, 32])
                    .build(),
            "output_model": "model2",
            "output_pred": "model2_predictions_"
        }
    ]
    results = []

    # 6. Loop through configs
    for config in models_config:
        print(f"\n=== Running baseline model for: {config['name']} ===")

        # Step 1: Train baseline model (no tuning)
        base_estimator = config["estimator"]
        baseline_start = time.time()
        baseline_model = base_estimator.fit(train)
        baseline_train_time = time.time() - baseline_start
        print(f"Baseline training time: {baseline_train_time:.2f} seconds")

        baseline_start_test = time.time()
        baseline_predictions = baseline_model.transform(test)
        baseline_rmse = evaluator.setMetricName("rmse")\
            .evaluate(baseline_predictions)
        baseline_r2 = evaluator.setMetricName("r2")\
            .evaluate(baseline_predictions)
        baseline_test_time = time.time() - baseline_start_test
        print(f"Baseline RMSE: {baseline_rmse:.4f}, R²: {baseline_r2:.4f}")
        print(f"Baseline test time: {baseline_test_time:.2f} seconds")

        results.append((
            f"{config['name']}_baseline",
            "{}",
            baseline_rmse,
            baseline_r2,
            baseline_train_time,
            baseline_test_time
        ))

        # Save baseline model and predictions
        baseline_model.write().overwrite()\
            .save(f"project/models/{config['output_model']}_baseline")
        baseline_predictions.select("label", "prediction") \
            .coalesce(1) \
            .write.mode("overwrite")\
            .csv(f"project/output/{config['output_pred'].replace('_', '_baseline')}", header=True)

        # Step 2: Hyperparameter tuning with CrossValidator
        print(f"\n--- Running hyperparameter tuning for: {config['name']} ---")
        pprint(config["param_grid"])
        tuning_start = time.time()

        cv = CrossValidator(
            estimator=config["estimator"],
            estimatorParamMaps=config["param_grid"],
            evaluator=evaluator.setMetricName("rmse"),
            numFolds=3,
            parallelism=4
        )
        tuned_model = cv.fit(train).bestModel
        tuning_train_time = time.time() - tuning_start
        print(f"Tuning completed in {tuning_train_time:.2f} seconds")

        param_map = tuned_model.extractParamMap()
        params = {p.name: tuned_model.getOrDefault(p) for p in param_map.keys()}

        # Save tuned model and predictions
        tuned_model.write().overwrite()\
            .save(f"project/models/{config['output_model']}")
        tuning_start_test = time.time()
        tuned_predictions = tuned_model.transform(test)
        tuned_predictions.select('label', 'prediction') \
            .coalesce(1) \
            .write.mode('overwrite')\
            .csv(f"project/output/{config['output_pred']}", header=True)

        tuned_rmse = evaluator.setMetricName("rmse").evaluate(tuned_predictions)
        tuned_r2 = evaluator.setMetricName("r2").evaluate(tuned_predictions)
        print(f"Tuned RMSE: {tuned_rmse:.4f}, R²: {tuned_r2:.4f}")
        tuning_test_time = time.time() - tuning_start_test
        print(f"Baseline test time: {tuning_test_time:.2f} seconds")

        results.append((
            f"{config['name']}_tuned",
            str(params),
            tuned_rmse,
            tuned_r2,
            tuning_train_time,
            tuning_test_time))

        # 7. Write evaluation summary
        spark.createDataFrame(results, ["model", "params", "rmse", "r2"]) \
            .coalesce(1).write.mode("overwrite")\
            .csv("project/output/evaluation", header=True)


if __name__ == '__main__':
    main()