# 🚕 NYC Yellow Taxi Fare Prediction – Big Data Pipeline Project

This repository is a template for the **final project** of a Big Data course. The goal is to build a complete end-to-end pipeline for predicting taxi fares in New York City using large-scale real-world data.

The pipeline handles everything from data collection and preprocessing to machine learning model training and result visualization — all built using distributed computing tools like Spark, Hive, and Hadoop.

The project report is too large for GitHub to render. Thus, you can either download the PDF report locally or navigate to the [Typst (LaTeX alternative) project](https://typst.app/project/rSmrrga9HCvA8eirT2UTFu) that renders the report.

---

## 🧱 Project Structure

The project is organized into the following directories:

### `data/`
Contains output artifacts generated during the modeling pipeline, including:
- Model evaluation metrics (`evaluation.csv`)
- Predictions from each model (tuned and baseline)

### `models/`
Holds all trained Spark ML models, saved in Spark’s native format. These models can be loaded and reused in Spark for prediction or inspection.

### `notebooks/`
Contains Jupyter notebooks used for prototyping and exploration. These are **not** part of the production pipeline and are only for learning purposes.

### `output/`
Stores intermediate files and query outputs, including:
- HiveQL query results
- Logs
- Generated schema files (`.avsc`, `.java`)

### `scripts/`
The core of the project. Includes all Bash and Python scripts that make up the four main pipeline stages:
1. Data ingestion and cleaning
2. Hive table setup and queries
3. Machine learning training and evaluation
4. Dashboard-ready table generation

### `sql/`
Contains all SQL and HiveQL scripts:
- PostgreSQL setup
- Hive table creation
- EDA and dashboard query logic

---

## ⚙️ Pipeline Automation

### `main.sh`
The main execution script that runs all pipeline stages in order. This is the **only script** that will be executed during assessment. It must produce all final outputs in the `output/` folder.

---

## 📦 Environment Setup

### `requirements.txt`
Lists all Python dependencies required to run the scripts in the pipeline. You can add packages here as needed.

---

## 💡 Notes and Constraints

- The pipeline assumes access to a working Hadoop + Hive + Spark cluster.
- Make sure database passwords are stored securely in the `secrets/` directory.
- Only content from `scripts/`, `sql/`, `data/`, `models/`, and `output/` is used in evaluation.
- Notebooks are excluded from the automated pipeline and should only be used for your own learning.

---

## 🏁 Goal

Build a scalable, modular, and interpretable pipeline for predicting NYC taxi ride fares — while demonstrating strong practical knowledge of big data technologies.

