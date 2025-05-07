# 📁 output/

This folder contains various output artifacts produced throughout the pipeline — including Hive query results, Avro schema files, and visualizations used in analysis and dashboard reporting.

## Contents

### `hive_results.txt`  
Output log from Stage 2’s Hive script execution. Includes table creation, partitioning confirmations, and sanity-check query results.

### `q1.csv` to `q7.csv`  
CSV results of Hive queries written in `q1.hql` through `q7.hql`. These were used for analysis and visualization in later stages of the project.

### `*.avsc`  
Avro schema definitions automatically generated during data export steps. These define the structure of the Parquet/Avro-serialized data.

### `*.java`  
Java class files generated from the Avro schemas. Used by Spark or Hadoop when working with Avro-formatted data.

---

### Data Visualization Files (`*.jpg`)

These image files were exported from Superset as part of the **EDA and reporting process**. Each corresponds to a specific analysis or insight and was used in the final dashboard and report:

- **Trip Timing & Trends**
  - `q-0-1-avg-total-amount-by-days.jpg`: Shows day-wise fare averages
  - `q-0-2-trip-duration-by-vendor.jpg`: Vendor-based duration comparison
  - `q-0-3-trip-duration.jpg`: Histogram of ride durations

- **Passenger-Based Insights**
  - `q-0-4-fares-by-passenger-count.jpg`: Fare distribution by group size
  - `q-2-1-fares-by-passenger-count.jpg`: Same focus, with additional filtering
  - `q-2-2-trip-count-by-passenger-count.jpg`: Frequency by passenger count

- **Payment & Geography**
  - `q-1-payment-type-distribution.jpg`: Proportion of rides by payment method
  - `q-3-data-description.jpg`: Snapshot of key dataset stats and metrics

- **Distance-Based Analysis**
  - `q-4-1-prices-by-distance-range.jpg`: Fare trends by trip distance
  - `q-4-2-passenger-count-by-distance-range.jpg`: Passenger profiles by distance
  - `q-4-3-trip-count-by-distance-range.jpg`: Frequency of distances taken

- **Vendor Comparison**
  - `q-5-1-trip-count-by-vendors.jpg`
  - `q-5-2-distance-by-vendors.jpg`
  - `q-5-3-prices-by-vendors.jpg`
  - `q-5-4-passengers-by-vendors.jpg`

- **Correlations**
  - `q-6-correlation-with-total-amount.jpg`: Heatmap showing feature correlation with target

---

## Notes

- Visualizations are timestamped upon export for traceability.
- File names follow the pattern: `q-[section]-[description]-[timestamp].jpg`
- Most of this folder is auto-generated and used in reporting or dashboards, not as direct pipeline inputs.

> Tip: You can preview all `.jpg` files in Superset or markdown renderers to quickly review EDA results.

