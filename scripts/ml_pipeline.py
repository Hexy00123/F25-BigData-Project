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
    """Custom Transformer to convert Unix milliseconds to timestamp."""

    def __init__(self, input_col=None, output_col=None):
        super().__init__()
        self.input_col = input_col
        self.output_col = output_col

    def _transform(self, dataset):
        return dataset.withColumn(
            self.output_col,
            to_timestamp(from_unixtime(col(self.input_col) / 1000))
        )


# Custom Transformer to extract hour and month
class ExtractHourMonth(Transformer):
    """Custom Transformer to extract hour and month from a timestamp."""

    def __init__(self, input_col=None, prefix="pickup"):
        super().__init__()
        self.input_col = input_col
        self.prefix = prefix

    def _transform(self, dataset):
        dataset = dataset.withColumn(f"{self.prefix}_hour",
                                     hour(col(self.input_col)))
        return dataset.withColumn(f"{self.prefix}_month",
                                  month(col(self.input_col)))


# Custom Transformer for cyclical encoding
class CyclicalTimeEncoder(Transformer):
    """Custom Transformer to apply cyclical encoding to time features."""

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


class CyclicalGeoEncoder(Transformer):
    """Encodes latitude and longitude cyclically."""

    def _transform(self, dataset):
        lon_norm = (col("pickup_longitude") + lit(180.0)) / lit(360.0)
        lat_norm = (col("pickup_latitude") + lit(90.0)) / lit(180.0)
        dataset = dataset.withColumn(
            "pickup_lon_sin", sin(2 * math.pi * lon_norm)
        ).withColumn(
            "pickup_lon_cos", cos(2 * math.pi * lon_norm)
        ).withColumn(
            "pickup_lat_sin", sin(2 * math.pi * lat_norm)
        ).withColumn(
            "pickup_lat_cos", cos(2 * math.pi * lat_norm)
        )
        lon_norm = (col("dropoff_longitude") + lit(180.0)) / lit(360.0)
        lat_norm = (col("dropoff_latitude") + lit(90.0)) / lit(180.0)
        return dataset.withColumn(
            "dropoff_lon_sin", sin(2 * math.pi * lon_norm)
        ).withColumn(
            "dropoff_lon_cos", cos(2 * math.pi * lon_norm)
        ).withColumn(
            "dropoff_lat_sin", sin(2 * math.pi * lat_norm)
        ).withColumn(
            "dropoff_lat_cos", cos(2 * math.pi * lat_norm)
        )


# Custom Transformer to select features and rename label
class SelectAndRename(Transformer):
    """Selects specific columns and renames target column to 'label'."""

    def _transform(self, dataset):
        cols = [
            'total_amount', 'vendorid',
            'passenger_count', 'trip_distance',
            'pickup_lon_sin', 'pickup_lon_cos',
            'pickup_lat_sin', 'pickup_lat_cos',
            'dropoff_lon_sin', 'dropoff_lon_cos',
            'dropoff_lat_sin', 'dropoff_lat_cos',
            'pickup_hour_sin', 'pickup_hour_cos',
            'pickup_month_sin', 'pickup_month_cos',
            'dropoff_hour_sin', 'dropoff_hour_cos',
            'dropoff_month_sin', 'dropoff_month_cos'
        ]
        dataset = dataset.select(*cols)
        return dataset.withColumnRenamed("total_amount", "label")


def main():
    """Main function to execute the ML pipeline."""
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
    raw_df = spark.read.format("avro").table('team11_projectdb.taxi_trips')
    raw_df = raw_df.filter(col("total_amount").between(0, 1e5))

    # 3. Split
    train_df, test_df = raw_df.randomSplit([0.8, 0.2], seed=42)

    # 4. Preprocessing + feature pipeline
    pipeline = Pipeline(stages=[
        UnixMillisToTimestamp("tpep_pickup_datetime", "pickup_ts"),
        UnixMillisToTimestamp("tpep_dropoff_datetime", "dropoff_ts"),
        ExtractHourMonth("pickup_ts", "pickup"),
        ExtractHourMonth("dropoff_ts", "dropoff"),
        CyclicalTimeEncoder(),
        CyclicalGeoEncoder(),
        SelectAndRename(),
        VectorAssembler(
            inputCols=[
                'vendorid',
                'passenger_count', 'trip_distance',
                'pickup_lon_sin', 'pickup_lon_cos',
                'pickup_lat_sin', 'pickup_lat_cos',
                'dropoff_lon_sin', 'dropoff_lon_cos',
                'dropoff_lat_sin', 'dropoff_lat_cos',
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
        },
        {
            "name": "GBT",
            "estimator": GBTRegressor(featuresCol="features",
                                      labelCol="label"),
        },
    ]

    # Build parameter grids referring to the same estimator instances:
    models_config[0]["param_grid"] = ParamGridBuilder()  \
        .addGrid(models_config[0]["estimator"].numTrees, [10, 40])  \
        .addGrid(models_config[0]["estimator"].maxDepth, [5, 10])  \
        .build()

    models_config[1]["param_grid"] = ParamGridBuilder()  \
        .addGrid(models_config[1]["estimator"].maxDepth, [3, 8])  \
        .addGrid(models_config[1]["estimator"].maxBins, [24, 32])  \
        .build()

    # Add output paths
    models_config[0].update({"output_model": "model1",
                             "output_pred": "model1_predictions"})
    models_config[1].update({"output_model": "model2",
                             "output_pred": "model2_predictions"})

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
            .csv(f"project/output/{config['output_pred']}_baseline",
                 header=True)

        # Step 2: Hyperparameter tuning with CrossValidator
        print(f"\n--- Running hyperparameter tuning for: {config['name']} ---")
        pprint(config["param_grid"])
        tuning_start = time.time()

        cross_validator = CrossValidator(
            estimator=config["estimator"],
            estimatorParamMaps=config["param_grid"],
            evaluator=evaluator.setMetricName("rmse"),
            numFolds=3,
            parallelism=4
        )
        tuned_model = cross_validator.fit(train).bestModel
        tuning_train_time = time.time() - tuning_start
        print(f"Tuning completed in {tuning_train_time:.2f} seconds")

        param_map = tuned_model.extractParamMap()
        params = {p.name: tuned_model.getOrDefault(p) for p in param_map.keys()}
        grid_params = config['param_grid'][0].keys()
        relevant_param_names = [gp.name for gp in grid_params]
        filtered_params = {k: v for k, v in params.items() if k in relevant_param_names}

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
            str(filtered_params),
            tuned_rmse,
            tuned_r2,
            tuning_train_time,
            tuning_test_time))

        # 7. Write evaluation summary
        spark.createDataFrame(results, ["model", "params", "rmse", "r2",
                                        "train_time_sec", "eval_time_sec"]) \
            .coalesce(1).write.mode("overwrite")\
            .csv("project/output/evaluation", header=True)


if __name__ == '__main__':
    main()
