# 📁 sql/

This folder contains all SQL and HiveQL scripts used throughout the project — from table creation and data loading to analysis queries and dashboard support.

## Contents

### Database Setup

- **`create_tables.sql`**  
  Defines the structure of the `taxi_trips` table in PostgreSQL. Includes constraints (e.g., valid `payment_type` values).

- **`import_data.sql`**  
  Loads the raw CSV data into the PostgreSQL table using `COPY FROM STDIN`.

- **`test_database.sql`**  
  Contains test queries to verify the contents of the PostgreSQL database after import.

---

### Hive Setup & Processing

- **`db.hql`**  
  Main HiveQL script used in Stage 2.  
  - Creates the Hive database (`team11_projectdb`)
  - Defines two tables: one unpartitioned (`taxi_trips`) and one partitioned (`taxi_trips_part`)
  - Enables dynamic partitioning and loads data from the Sqoop-imported Parquet files

- **`mq1.hql`**, **`mq_model_1.hql`**, **`mq_model_2.hql`**  
  Scripts for Stage 4 that create additional Hive tables to support the Superset dashboard:
  - `mq1` – aggregated or enriched dataset for general analysis
  - `mq_model_1`, `mq_model_2` – tables storing model outputs or evaluation results

---

### Analytical Queries (EDA)

- **`q1.hql`** to **`q7.hql`**  
  A set of predefined Hive queries that extract insights from the taxi trip data, such as:
  - Trip duration and distance distributions
  - Fare and tip patterns by payment type or time
  - Trip counts by location, day, or passenger count

These results are saved as CSVs in the `output/` folder and used in dashboards or reporting.

## Notes

- All `.hql` scripts are executed via Beeline in the corresponding pipeline stages.
- Make sure the Hive metastore and HDFS paths are configured before running these scripts.

> Tip: You can preview and test individual `.hql` scripts using Beeline with the `-f` option.

