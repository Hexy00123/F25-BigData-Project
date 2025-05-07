# 📁 scripts/

This folder contains all the scripts used to automate each stage of the end-to-end data pipeline — from collection and ingestion to modeling and evaluation.

## Contents

### Pipeline Stages

- **`stage1.sh`**  
  Orchestrates data download, database creation, and conversion to Parquet.  
  Runs:
  - `data_collection.sh`
  - `data_storage.sh`
  - `fix_parquet_timestamps.py`
  - Sqoop to import data into HDFS

- **`stage2.sh`**  
  Executes HiveQL scripts to create tables and partitions.  
  Also runs analytical queries (`q1.hql`–`q7.hql`) and saves outputs to CSV.

- **`stage3.sh`**  
  Runs the full Spark ML pipeline for training, tuning, and evaluation.  
  Retrieves trained models, predictions, and metrics from HDFS.

- **`stage4.sh`**  
  Creates additional Hive tables (`mq1`, `mq_model_1`, `mq_model_2`) to support dashboarding and visualization.

---

### Supporting Scripts

- **`data_collection.sh`**  
  Downloads the raw NYC taxi dataset from Kaggle and extracts the target CSV file.

- **`data_storage.sh`**  
  Sets up a virtual environment and runs the Python script to load the CSV into PostgreSQL.

- **`build_projectdb.py`**  
  Creates the `taxi_trips` table and loads the cleaned CSV into a PostgreSQL database.

- **`fix_parquet_timestamps.py`**  
  Spark script to correct timestamp formatting issues in the imported Parquet files.

- **`ml_pipeline.py`**  
  Core machine learning pipeline implemented in PySpark.  
  Includes preprocessing, feature engineering, model training, hyperparameter tuning, and evaluation.

- **`preprocess.sh` / `postprocess.sh`**  
  Optional utility scripts -- not used in our pipeline, since they would introduce redundancy.

## Notes

- All scripts are orchestrated to run sequentially through `stage*.sh` wrappers.
- Environment variables and secrets (e.g., database passwords) are read securely from local files.

> Tip: Run each `stage*.sh` file from the root of the project to ensure all paths resolve correctly.

