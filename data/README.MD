# 📁 data/

This folder contains all the output artifacts generated during the final stage of the project — mainly model predictions and evaluation metrics.

## Contents

### 📄 `evaluation.csv`
A summary table of model performance. For each model (baseline and tuned), it includes:
- **RMSE** and **R²** scores on the test set
- **Training time** and **evaluation time** in seconds
- **Hyperparameters used** (if any)

This file is useful for comparing models and selecting the best-performing one.

### 📄 `model1_predictions.csv`  
### 📄 `model1_predictions_baseline.csv`  
Predicted vs actual fare amounts using the **Random Forest** model.  
- `model1_predictions.csv` contains the tuned model results.  
- `model1_predictions_baseline.csv` contains the results from the baseline (untuned) model.

### 📄 `model2_predictions.csv`  
### 📄 `model2_predictions_baseline.csv`  
Predicted vs actual fare amounts using the **Gradient Boosted Trees** model.  
Similar structure as above, with separate files for tuned and baseline predictions.

> Note: Predictions were generated on a test split and are stored as CSVs for downstream visualization and error analysis (e.g., scatter plots, residuals, etc.).

## Notes

- All prediction outputs were coalesced into a single CSV file per model using Spark.
- File naming follows this pattern:  
  `model[model_number]_predictions[_baseline].csv`

