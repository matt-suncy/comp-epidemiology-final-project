import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix
)
import xgboost as xgb

class CohortDataLoader:
    def __init__(self, data_path: str, target_col: str, drop_cols: list = None, separator: str = ','):
        self.data_path = data_path
        self.target_col = target_col
        self.drop_cols = drop_cols if drop_cols else []
        self.separator = separator
        self.patients_info = None
        self.predictors = None
        self.targets = None

    def load_data(self):
        '''
        Loads the unified cohort dataset.
        Segments it into patient_info, targets, and predictors.
        Sets self.patients_info, self.predictors, and self.targets.
        Returns:
            patients_info: DataFrame containing non-predictor logistical info
            predictors: DataFrame containing predictor variables
            targets: DataFrame containing the target variable
        '''
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"Data file not found: {self.data_path}")

        print(f"Loading unified data from {self.data_path}...")
        df = pd.read_csv(self.data_path, sep=self.separator)
        
        print(f"Total dataset shape: {df.shape}")

        if self.target_col not in df.columns:
            raise ValueError(f"Target column '{self.target_col}' not found in dataset!")

        # 1. Extract Target
        self.targets = df[[self.target_col]]

        # 2. Extract Patient Info / Drop Columns
        actual_drop_cols = [c for c in self.drop_cols if c in df.columns]
        self.patients_info = df[actual_drop_cols]

        # 3. Extract Predictors (Everything Else)
        # Drop both the target and the patient info from the features
        self.predictors = df.drop(columns=actual_drop_cols + [self.target_col])

        print(f"Targets shape: {self.targets.shape}")
        print(f"Patient Info shape: {self.patients_info.shape}")
        print(f"Predictors shape: {self.predictors.shape}")

        return self.patients_info, self.predictors, self.targets

def preprocess_data(patients_info: pd.DataFrame, predictors: pd.DataFrame, targets: pd.DataFrame, target_col: str):
    """
    Combines predictors and targets, imputes missing values.
    Returns un-scaled features to allow K-Fold cross validation to scale safely inside folds.
    """
    print("\n--- Preprocessing Data ---")
    
    if target_col not in targets.columns:
        raise ValueError(f"Target column '{target_col}' not found in target dataframe.")

    y = targets[target_col]
    X = predictors.copy()

    # Fill missing values
    X = X.fillna(0)

    # Automatically dummy-encode any remaining object/string columns
    object_cols = X.select_dtypes(include=['object', 'string']).columns.tolist()
    if object_cols:
        X = pd.get_dummies(X, columns=object_cols, drop_first=True)

    # Convert categoricals if present (gender_concept_id is natively int, so it needs explicit handling)
    if "gender_concept_id" in X.columns:
        X = pd.get_dummies(X, columns=["gender_concept_id"], drop_first=True)

    print(f"Features shape after dummy-encoding: {X.shape}")
    print(f"Target distribution for '{target_col}':")
    print(y.value_counts(normalize=True))

    return X, y, X.columns

def save_results(method_name: str, metrics_str: str, results_df: pd.DataFrame):
    """Saves metrics and patient-level predictions to the results folder."""
    os.makedirs("results", exist_ok=True)
    
    # Generate EST timestamp
    est = pytz.timezone('America/New_York')
    timestamp = datetime.now(est).strftime("%Y%m%d_%H%M%S_EST")
    
    # Save Metrics
    metrics_path = f"results/{method_name}_metrics_{timestamp}.txt"
    with open(metrics_path, "w") as f:
        f.write(metrics_str)
    
    # Save Predictions
    preds_path = f"results/{method_name}_predictions_{timestamp}.csv"
    results_df.to_csv(preds_path, index=False)
    
    print(f"\n[+] Results saved successfully to:\n   - {metrics_path}\n   - {preds_path}")

def run_logistic_regression(X, y, patients_info, feature_names, cv_folds: int = 5, random_state: int = 42):
    print("\n" + "="*40)
    print(f"Executing LOGISTIC REGRESSION ({cv_folds}-Fold CV)")
    print("="*40)
    
    cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=random_state)
    
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('model', LogisticRegression(max_iter=1000, class_weight="balanced", random_state=random_state))
    ])

    print(f"Running cross validation...")
    y_pred = cross_val_predict(pipeline, X, y, cv=cv, method="predict")
    y_proba = cross_val_predict(pipeline, X, y, cv=cv, method="predict_proba")[:, 1]

    # Generate Metrics
    roc_val = roc_auc_score(y, y_proba)
    cr_val = classification_report(y, y_pred)
    cm_val = confusion_matrix(y, y_pred)
    
    # Fit globally once just to gracefully extract final feature coefficients
    pipeline.fit(X, y)
    model = pipeline.named_steps['model']
    coefficients = pd.DataFrame({
        "feature": feature_names,
        "coefficient": model.coef_[0]
    })
    coefficients["abs_coef"] = coefficients["coefficient"].abs()
    coefficients = coefficients.sort_values(by="abs_coef", ascending=False)
    top_10 = coefficients.head(10).to_string()

    # Compile the printed output block
    metrics_block = (
        f"Cross-Validated ROC-AUC: {roc_val}\n\n"
        f"Classification Report:\n{cr_val}\n\n"
        f"Confusion Matrix:\n{cm_val}\n\n"
        f"Top 10 Feature Coefficients (Global Fit):\n{top_10}\n"
    )
    print(metrics_block)

    # Append tracking columns
    test_results = patients_info.copy()
    test_results['true_label'] = y
    test_results['log_reg_pred'] = y_pred
    test_results['log_reg_prob'] = y_proba

    save_results("log_reg", metrics_block, test_results)
    return test_results

def run_xgboost(X, y, patients_info, cv_folds: int = 5, random_state: int = 42):
    print("\n" + "="*40)
    print(f"Executing XGBOOST ({cv_folds}-Fold CV)")
    print("="*40)

    cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=random_state)
    
    # XGBoost does not strictly need scaling, but the pipeline maintains analytical uniformity
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('model', xgb.XGBClassifier(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=random_state,
            eval_metric="logloss"
        ))
    ])

    print(f"Running cross validation...")
    y_pred_xgb = cross_val_predict(pipeline, X, y, cv=cv, method="predict")
    y_proba_xgb = cross_val_predict(pipeline, X, y, cv=cv, method="predict_proba")[:, 1]

    roc_val = roc_auc_score(y, y_proba_xgb)
    cr_val = classification_report(y, y_pred_xgb)
    cm_val = confusion_matrix(y, y_pred_xgb)

    metrics_block = (
        f"Cross-Validated ROC-AUC: {roc_val}\n\n"
        f"Classification Report:\n{cr_val}\n\n"
        f"Confusion Matrix:\n{cm_val}\n"
    )
    print(metrics_block)

    test_results = patients_info.copy()
    test_results['true_label'] = y
    test_results['xgb_pred'] = y_pred_xgb
    test_results['xgb_prob'] = y_proba_xgb

    save_results("xgboost", metrics_block, test_results)
    return test_results

def main():
    parser = argparse.ArgumentParser(description="Run Cohort Analysis Pipeline")
    parser.add_argument("--data", type=str, required=True, help="Path to the unified cohort CSV")
    parser.add_argument("--target_col", type=str, default="recurrent_uti_90d_flag", help="Outcome column to predict")
    parser.add_argument("--methods", nargs="+", choices=["log_reg", "xgboost"], required=True, help="Methods to run")
    parser.add_argument("--random_state", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--cv_folds", type=int, default=5, help="Number of folds for Cross Validation (default: 5)")

    args = parser.parse_args()

    drop_cols = [
        "person_id",
        "index_uti_date",
        "first_recurrent_uti_date",
        "first_abx_0_10d_date"
    ]

    # 1. Load Data
    loader = CohortDataLoader(
        data_path=args.data,
        target_col=args.target_col,
        drop_cols=drop_cols
    )
    patients_info, predictors, targets = loader.load_data()

    # 2. Preprocess Data (No scaling/splitting here, delayed for CV pipeline)
    X, y, feature_names = preprocess_data(
        patients_info=patients_info,
        predictors=predictors, 
        targets=targets, 
        target_col=args.target_col
    )

    # 3. Model Execution
    if "log_reg" in args.methods:
        run_logistic_regression(X, y, patients_info, feature_names, cv_folds=args.cv_folds, random_state=args.random_state)

    if "xgboost" in args.methods:
        run_xgboost(X, y, patients_info, cv_folds=args.cv_folds, random_state=args.random_state)

if __name__ == "__main__":
    main()