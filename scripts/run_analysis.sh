#!/bin/bash

# Make sure we're in in the repo directory
cd "$(git rev-parse --show-toplevel)"

# # Acivtate virtual env (change to ur env)
# pyenv activate test-ml-env

# Run script on default
python3 src/models/analysis.py \
--data src/features/uti_cohort_default.csv \
--target_col recurrent_uti_90d_flag \
--methods log_reg xgboost svm \
--random_state 42 \
--cv_folds 5

sleep 1s

# Run script on more inclusive cohort
python3 src/models/analysis.py \
--data src/features/uti_all.csv \
--target_col recurrent_uti_90d_flag \
--methods log_reg xgboost svm \
--random_state 42 \
--cv_folds 5