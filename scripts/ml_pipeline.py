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


def init_spark():
    """Spark session init"""
    return SparkSession.builder \
        .appName("team11 - stage3") \
        .master("yarn") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", "/user/team11/project/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()


def load_and_clean_data(spark):
    """Load, filter and split data"""
    df_raw = spark.read.format("avro").table('team11_projectdb.taxi_trips')
    df_raw = df_raw.filter(col("total_amount").between(0, 1e5))
    train_df, test_df = df_raw.randomSplit([0.8, 0.2], seed=42)
    return train_df, test_df


def prepare_data(train_df, test_df):
    """Preprocess train and test with pipeline"""
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

    fitted_pipeline = pipeline.fit(train_df)
    train_transformed = fitted_pipeline.transform(train_df)
    test_transformed = fitted_pipeline.transform(test_df)
    return train_transformed, test_transformed


def build_model_config():
    """Model config for hyperparameter tuning"""
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

    return models_config


def extract_relevant_params(model, config):
    """Get relevant best parameters for given model and its config"""
    param_map = model.extractParamMap()
    params = {p.name: model.getOrDefault(p) for p in param_map.keys()}
    relevant_param_names = [gp.name for gp in config['param_grid'][0].keys()]
    filtered_params = {k: v for k, v in params.items() if k in relevant_param_names}
    return filtered_params


def training_pipeline(models_config, evaluator, train, test):
    """Whole pipeline with baseline models and hyperparameter tuning"""
    results = []

    # Loop through configs
    for config in models_config:
        print(f"\n=== Running baseline model for: {config['name']} ===")

        # Step 1: Train baseline model (no tuning)
        start = time.time()
        model = config["estimator"].fit(train)
        end = time.time() - start
        print(f"Baseline training time: {end:.2f} seconds")

        start_test = time.time()
        predictions = model.transform(test)
        rmse = evaluator.setMetricName("rmse")\
            .evaluate(predictions)
        r_2 = evaluator.setMetricName("r2")\
            .evaluate(predictions)
        end_test = time.time() - start_test
        print(f"Baseline RMSE: {rmse:.4f}, R²: {r_2:.4f}")
        print(f"Baseline test time: {end_test:.2f} seconds")

        results.append((
            f"{config['name']}_baseline",
            "{}",
            rmse,
            r_2,
            end,
            end_test
        ))

        # Save baseline model and predictions
        model.write().overwrite()\
            .save(f"project/models/{config['output_model']}_baseline")
        predictions.select("label", "prediction") \
            .coalesce(1) \
            .write.mode("overwrite")\
            .csv(f"project/output/{config['output_pred']}_baseline",
                 header=True)

        # Step 2: Hyperparameter tuning with CrossValidator
        print(f"\n--- Running hyperparameter tuning for: {config['name']} ---")
        pprint(config["param_grid"])
        start = time.time()

        model = CrossValidator(
            estimator=config["estimator"],
            estimatorParamMaps=config["param_grid"],
            evaluator=evaluator.setMetricName("rmse"),
            numFolds=3,
            parallelism=4
        ).fit(train).bestModel

        end = time.time() - start
        print(f"Tuning completed in {end:.2f} seconds")

        filtered_params = extract_relevant_params(model, config)

        # Save tuned model and predictions
        model.write().overwrite()\
            .save(f"project/models/{config['output_model']}")
        start_test = time.time()
        predictions = model.transform(test)
        predictions.select('label', 'prediction') \
            .coalesce(1) \
            .write.mode('overwrite')\
            .csv(f"project/output/{config['output_pred']}", header=True)

        rmse = evaluator.setMetricName("rmse").evaluate(predictions)
        r_2 = evaluator.setMetricName("r2").evaluate(predictions)
        print(f"Tuned RMSE: {rmse:.4f}, R²: {r_2:.4f}")
        end_test = time.time() - start_test
        print(f"Baseline test time: {end_test:.2f} seconds")

        results.append((
            f"{config['name']}_tuned",
            str(filtered_params),
            rmse,
            r_2,
            end,
            end_test))
    return results


def main():
    """Main function to execute the ML pipeline."""

    spark = init_spark()
    train_df, test_df = load_and_clean_data(spark)
    train, test = prepare_data(train_df, test_df)

    train.select("features", "label").coalesce(1)\
        .write.mode("overwrite").json("project/data/train")
    test.select("features", "label").coalesce(1)\
        .write.mode("overwrite").json("project/data/test")

    evaluator = RegressionEvaluator(labelCol="label",
                                    predictionCol="prediction")

    models_config = build_model_config()

    results = training_pipeline(models_config, evaluator, train, test)

    spark.createDataFrame(results, ["model", "params", "rmse", "r2",
                                    "train_time_sec", "eval_time_sec"]) \
        .coalesce(1).write.mode("overwrite")\
        .csv("project/output/evaluation", header=True)


if __name__ == '__main__':
    main()
