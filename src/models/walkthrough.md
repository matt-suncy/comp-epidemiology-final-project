# Analysis Pipeline Migration Walkthrough

I have fully migrated the exploratory logic from your notebook `eda_2026-04-18.py` into a dynamic, production-ready script in `src/models/analysis.py`.

## What Was Accomplished

1. **CLI Extensibility**: You can now run your machine learning pipeline directly from the terminal and hook it up to batch jobs.
    - Example usage: 
    ```bash
    python src/models/analysis.py --target_cohort "data/my_cohort.csv" --predictors "data/my_predictors.csv" --target_col "recurrent_uti_90d_flag" --methods "log_reg" "xgboost"
    ```

2. **Unified Preprocessing**: I extracted all the data cleaning steps into a `preprocess_data` function. It automatically:
    - Merges and formats the DataFrames loaded by `CohortDataLoader`.
    - Handles `0` imputation for missing features.
    - Automatically builds dummy variables (`pd.get_dummies`) if `gender_concept_id` is passed as a predictor.
    - Performs the 80/20 `train_test_split` with `stratify`.
    - Scales features seamlessly using `StandardScaler`.

3. **Isolated Model Execution**:
    - **Logistic Regression**: Lives in `run_logistic_regression(...)`. It builds the model, trains it on the scaled data, and cleanly prints out the ROC-AUC, classification metrics, and top feature coefficients.
    - **XGBoost**: Lives in `run_xgboost(...)`. It builds the classifier strictly matching your hyperparameter configuration from the notebook (`n_estimators=300`, `max_depth=5`, etc.) and returns the identical metrics interface so you can easily compare model strengths side-by-side.

> [!TIP]
> Because you used `argparse` with `nargs="+"`, you can pass one or multiple methods to `--methods`. Running `--methods log_reg xgboost` will run the data loader once, preprocess once, and then sequentially train and evaluate both models on the exact same data split!
