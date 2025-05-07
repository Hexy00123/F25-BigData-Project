# 📁 notebooks/

This folder contains the Jupyter notebook used during **Phase 3** of the project for developing and testing the machine learning models.

## Contents

### 📓 `modeling.ipynb`

This notebook documents the experimentation and prototyping steps for building a fare prediction model. It mirrors much of the logic used in the production pipeline (`ml_pipeline.py`) but was used earlier in development for:

- Exploring the modeling problem in an interactive environment
- Manually testing feature extraction and transformation steps
- Training and evaluating regression models (Random Forest, GBT, etc.)
- Comparing baseline vs tuned performance before full automation

The notebook served as a sandbox before converting the logic into Spark jobs for scalable training and deployment.

## Notes

- This notebook assumes a Spark session is running in the notebook environment.
- Final models and evaluations were moved to the automated pipeline in `scripts/stage3.sh`.

> Tip: Useful for debugging or revisiting modeling decisions in an interactive way before re-running the full pipeline.

