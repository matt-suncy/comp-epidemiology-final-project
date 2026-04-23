# %%
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix,
    RocCurveDisplay
)

import matplotlib.pyplot as plt

# %%
file_path = "/Users/jackpapciak/Columbia/Computational_Epidemiology/project/comp-epidemiology-final-project/data/external/uti_cohort_2026-04-18.csv"

df = pd.read_csv(file_path)

print(df.shape)
df.head()

# %%
# Drop obvious non-feature columns
drop_cols = [
    "person_id",
    "index_uti_date",
    "first_recurrent_uti_date",
    "first_abx_0_10d_date"
]

df_model = df.drop(columns=[c for c in drop_cols if c in df.columns])

# Fill missing values
df_model = df_model.fillna(0)

df_model.head()

# %%
target = "recurrent_uti_90d_flag"

X = df_model.drop(columns=[target])
y = df_model[target]

print("Features:", X.shape)
print("Target distribution:")
print(y.value_counts(normalize=True))

# %%
# Convert gender_concept_id to categorical
if "gender_concept_id" in X.columns:
    X = pd.get_dummies(X, columns=["gender_concept_id"], drop_first=True)

# %%
X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# %%
scaler = StandardScaler()

X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# %%
model = LogisticRegression(
    max_iter=1000,
    class_weight="balanced"  # helpful if classes are imbalanced
)

model.fit(X_train_scaled, y_train)

# %%
y_pred = model.predict(X_test_scaled)
y_proba = model.predict_proba(X_test_scaled)[:, 1]

print("ROC-AUC:", roc_auc_score(y_test, y_proba))

print("\nClassification Report:")
print(classification_report(y_test, y_pred))

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

# %%
RocCurveDisplay.from_predictions(y_test, y_proba)
plt.show()

# %%
feature_names = X.columns

coefficients = pd.DataFrame({
    "feature": feature_names,
    "coefficient": model.coef_[0]
})

coefficients["abs_coef"] = coefficients["coefficient"].abs()
coefficients = coefficients.sort_values(by="abs_coef", ascending=False)

coefficients.head(20)

# %%
import xgboost as xgb
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix

# %%
xgb_model = xgb.XGBClassifier(
    n_estimators=300,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    eval_metric="logloss"
)

xgb_model.fit(X_train, y_train)

# %%
y_pred_xgb = xgb_model.predict(X_test)
y_proba_xgb = xgb_model.predict_proba(X_test)[:, 1]

print("XGBoost ROC-AUC:", roc_auc_score(y_test, y_proba_xgb))

print("\nClassification Report:")
print(classification_report(y_test, y_pred_xgb))

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred_xgb))


