# 📁 models/

This folder contains the serialized versions of all trained machine learning models used in the project.

## Contents

Each model is saved as a separate directory containing Spark ML model metadata and binary files required for loading the model back into a Spark session.

### `model1_baseline/`  
Baseline **Random Forest** model trained with default hyperparameters.

### `model1/`  
Tuned **Random Forest** model trained using grid search and cross-validation.

### `model2_baseline/`  
Baseline **Gradient Boosted Trees (GBT)** model with default settings.

### `model2/`  
Tuned **Gradient Boosted Trees** model with optimized `maxDepth` and `maxBins`.

### `model3_baseline/`  
Baseline **Decision Tree** model — used mainly as a reference.

### `model3/`  
Tuned **Decision Tree** model (also optimized via grid search).

## Notes

- Models were trained and saved using Spark MLlib.
- Each folder contains Spark’s standard model structure (`metadata`, `stages`, `data`, etc.), which allows them to be reloaded directly using `PipelineModel.load(path)` in Spark.
- These models were used for generating predictions and evaluating performance, with results stored in the `data/` folder.

> Tip: Always load these models using the same Spark version used for training to ensure compatibility.

