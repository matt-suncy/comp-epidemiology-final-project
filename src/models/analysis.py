import os
import argparse
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix
)
import xgboost as xgb

class CohortDataLoader:
    def __init__(self, target_cohort_path: str, predictors_path: str, separator: str = ','):
        self.target_cohort_path = target_cohort_path
        self.predictors_path = predictors_path
        self.separator = separator
        self.patients = None
        self.predictors = None
        self.targets = None

    def load_data(self):
        '''
        Loads and validates target cohort and predictors.
        Sets self.patients, self.predictors, and self.targets.
        Returns:
            patients: DataFrame containing person_id and index_date
            predictors: DataFrame containing predictor variables
            targets: DataFrame containing target variables
        '''
        # Check if the files exist
        if not os.path.exists(self.target_cohort_path):
            raise FileNotFoundError(f"Target cohort file not found: {self.target_cohort_path}")
        if not os.path.exists(self.predictors_path):
            raise FileNotFoundError(f"Predictors file not found: {self.predictors_path}")

        # Check if first two columns are person_id and index_date by peeking at headers
        target_head = pd.read_csv(self.target_cohort_path, nrows=0, sep=self.separator)
        pred_head = pd.read_csv(self.predictors_path, nrows=0, sep=self.separator)
        
        if list(target_head.columns[:2]) != ['person_id', 'index_date']:
            raise ValueError("First two columns of target cohort file must be person_id and index_date")
        if list(pred_head.columns[:2]) != ['person_id', 'index_date']:
            raise ValueError("First two columns of predictors file must be person_id and index_date")

        # Load the data efficiently once
        target_cohort = pd.read_csv(self.target_cohort_path, sep=self.separator)
        predictors_df = pd.read_csv(self.predictors_path, sep=self.separator)

        # Print basic info about the data
        print(f"Target cohort shape: {target_cohort.shape}")
        print(f"Predictors shape: {predictors_df.shape}")
        
        # Partition data into attributes
        self.targets = target_cohort.iloc[:, 2:]
        self.predictors = predictors_df.iloc[:, 2:]
        self.patients = target_cohort.iloc[:, :2]

        return self.patients, self.predictors, self.targets

def preprocess_data(predictors: pd.DataFrame, targets: pd.DataFrame, target_col: str, random_state: int = 42):
    """
    Combines predictors and targets, imputes missing values, and prepares
    the data for modeling (train/test split and scaling).
    """
    print("\n--- Preprocessing Data ---")
    
    # Merge targets onto predictors (assuming index alignments match)
    # Since they were sliced from the same dataframe in the dataloader:
    if target_col not in targets.columns:
        raise ValueError(f"Target column '{target_col}' not found in target dataframe.")

    y = targets[target_col]
    X = predictors.copy()

    # Fill missing values
    X = X.fillna(0)

    # Convert categoricals if present
    if "gender_concept_id" in X.columns:
        X = pd.get_dummies(X, columns=["gender_concept_id"], drop_first=True)

    print(f"Features shape after dummy-encoding: {X.shape}")
    print(f"Target distribution for '{target_col}':")
    print(y.value_counts(normalize=True))

    # Train / Test Splitting
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=random_state,
        stratify=y
    )

    # Scaling Features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    return X_train_scaled, X_test_scaled, y_train, y_test, X.columns

def run_logistic_regression(X_train, X_test, y_train, y_test, feature_names, random_state: int = 42):
    print("\n" + "="*40)
    print("Executing LOGISTIC REGRESSION")
    print("="*40)
    
    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=random_state
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("ROC-AUC:", roc_auc_score(y_test, y_proba))
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    # Feature Importance (Coefficients)
    coefficients = pd.DataFrame({
        "feature": feature_names,
        "coefficient": model.coef_[0]
    })
    coefficients["abs_coef"] = coefficients["coefficient"].abs()
    coefficients = coefficients.sort_values(by="abs_coef", ascending=False)
    
    print("\nTop 10 Feature Coefficients:")
    print(coefficients.head(10))

def run_xgboost(X_train, X_test, y_train, y_test, random_state: int = 42):
    print("\n" + "="*40)
    print("Executing XGBOOST")
    print("="*40)

    xgb_model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=random_state,
        eval_metric="logloss"
    )

    # Note: XGBoost handles non-scaled data fine, but we pass scaled sets for uniformity
    xgb_model.fit(X_train, y_train)

    y_pred_xgb = xgb_model.predict(X_test)
    y_proba_xgb = xgb_model.predict_proba(X_test)[:, 1]

    print("XGBoost ROC-AUC:", roc_auc_score(y_test, y_proba_xgb))
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred_xgb))
    
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred_xgb))


def main():
    parser = argparse.ArgumentParser(description="Run Cohort Analysis Pipeline")
    parser.add_argument("--target_cohort", type=str, required=True, help="Path to the target cohort CSV")
    parser.add_argument("--predictors", type=str, required=True, help="Path to the predictors CSV")
    parser.add_argument("--target_col", type=str, default="recurrent_uti_90d_flag", help="Outcome column to predict")
    parser.add_argument("--methods", nargs="+", choices=["log_reg", "xgboost"], required=True, help="Methods to run")
    parser.add_argument("--random_state", type=int, default=42, help="Random seed for reproducibility")

    args = parser.parse_args()

    # 1. Load Data
    print("Loading data via CohortDataLoader...")
    loader = CohortDataLoader(
        target_cohort_path=args.target_cohort,
        predictors_path=args.predictors
    )
    patients, predictors, targets = loader.load_data()

    # 2. Preprocess Data
    X_train_scaled, X_test_scaled, y_train, y_test, feature_names = preprocess_data(
        predictors=predictors, 
        targets=targets, 
        target_col=args.target_col,
        random_state=args.random_state
    )

    # 3. Model Execution
    if "log_reg" in args.methods:
        run_logistic_regression(X_train_scaled, X_test_scaled, y_train, y_test, feature_names, random_state=args.random_state)

    if "xgboost" in args.methods:
        run_xgboost(X_train_scaled, X_test_scaled, y_train, y_test, random_state=args.random_state)

if __name__ == "__main__":
    main()