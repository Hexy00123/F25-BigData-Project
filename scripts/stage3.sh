#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# 1. Run the Spark Python pipeline
spark-submit --master yarn scripts/ml_pipeline.py

# 2. Retrieve train/test JSON splits
hdfs dfs -getmerge project/data/train data/train.json
hdfs dfs -getmerge project/data/test data/test.json

# 3. Retrieve model predictions
hdfs dfs -getmerge project/output/model1_predictions_baseline data/model1_predictions_baseline.csv
hdfs dfs -getmerge project/output/model2_predictions_baseline data/model2_predictions_baseline.csv

hdfs dfs -getmerge project/output/model1_predictions data/model1_predictions.csv
hdfs dfs -getmerge project/output/model2_predictions data/model2_predictions.csv

# 4. Pull models
rm -rf models/model1_baseline
rm -rf models/model2_baseline
rm -rf models/model1
rm -rf models/model2

hdfs dfs -get project/models/model1_baseline models/
hdfs dfs -get project/models/model2_baseline models/

hdfs dfs -get project/models/model1 models/
hdfs dfs -get project/models/model2 models/

# 5. Retrieve evaluation summary
hdfs dfs -getmerge project/output/evaluation data/evaluation.csv