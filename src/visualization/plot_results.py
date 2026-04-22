import os
import sys
import glob
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import precision_recall_curve, average_precision_score, confusion_matrix, recall_score
from sklearn.calibration import calibration_curve

# Hook into existing analysis module explicitly to reuse data loading architecture
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.models.analysis import CohortDataLoader, preprocess_data
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
import xgboost as xgb

def load_results(run_dir):
    """Loads all prediction CSVs from the specified run directory."""
    files = glob.glob(os.path.join(run_dir, "*_predictions.csv"))
    if not files:
        raise FileNotFoundError(f"No prediction CSVs found in {run_dir}")
    
    results = {}
    for f in files:
        # Extract method name from filename (e.g., 'log_reg_predictions.csv' -> 'log_reg')
        bn = os.path.basename(f)
        method = bn.replace("_predictions.csv", "")
        df = pd.read_csv(f)
        
        # Normalize XGBoost column shorthand from analysis.py
        if method == "xgboost":
            df = df.rename(columns={"xgb_pred": "xgboost_pred", "xgb_prob": "xgboost_prob"})
            
        results[method] = df
    return results

def plot_pr_curves(results_dict, out_dir):
    plt.figure(figsize=(8, 6))
    for method, df in results_dict.items():
        y_true = df['true_label']
        y_prob = df[f'{method}_prob']
        precision, recall, _ = precision_recall_curve(y_true, y_prob)
        ap = average_precision_score(y_true, y_prob)
        plt.plot(recall, precision, label=f"{method.upper()} (AP: {ap:.2f})")
    
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('Comparative Precision-Recall Curves')
    plt.legend(loc='lower left')
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(out_dir, "pr_curves.png"), dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

def plot_confusion_matrices(results_dict, out_dir):
    n_methods = len(results_dict)
    fig, axes = plt.subplots(1, n_methods, figsize=(5 * n_methods, 5))
    if n_methods == 1:
        axes = [axes]
    
    for ax, (method, df) in zip(axes, results_dict.items()):
        y_true = df['true_label']
        y_pred = df[f'{method}_pred']
        cm = confusion_matrix(y_true, y_pred)
        
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax, cbar=False)
        ax.set_title(f"Confusion Matrix: {method.upper()}")
        ax.set_ylabel('True Label')
        ax.set_xlabel('Predicted Label')
        
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "confusion_matrices.png"), dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

def plot_calibration_curves(results_dict, out_dir):
    plt.figure(figsize=(8, 8))
    
    # Plot perfectly calibrated diagonal
    plt.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")
    
    for method, df in results_dict.items():
        y_true = df['true_label']
        y_prob = df[f'{method}_prob']
        
        prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=10)
        plt.plot(prob_pred, prob_true, "s-", label=method.upper())
        
    plt.ylabel("Fraction of Positives")
    plt.xlabel("Mean Predicted Probability")
    plt.title('Reliability Diagrams (Calibration Curves)')
    plt.legend(loc="upper left")
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(out_dir, "calibration_curves.png"), dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

def plot_age_stratified_recall(results_dict, data_df, out_dir):
    # Prepare combined dataframe
    if 'age_at_index' not in data_df.columns:
        print("Warning: 'age_at_index' not found in master data. Skipping age stratification plot.")
        return
        
    # Bin age cleanly
    data_df = data_df.copy()
    try:
        data_df['age_group'] = pd.qcut(data_df['age_at_index'], q=4)
    except ValueError:
        data_df['age_group'] = pd.cut(data_df['age_at_index'], bins=4)
        
    age_map = data_df[['person_id', 'age_group']].drop_duplicates()
    
    # Process recall per age bracket
    plot_data = []
    
    for method, res_df in results_dict.items():
        merged = pd.merge(res_df, age_map, on='person_id', how='inner')
        
        # Sort categorical age brackets appropriately
        for name, group in merged.groupby('age_group', observed=True):
            y_t = group['true_label']
            y_p = group[f'{method}_pred']
            # Safeguard if group has no positive cases
            if y_t.sum() > 0:
                rec = recall_score(y_t, y_p, zero_division=0)
            else:
                rec = 0.0
                
            # Stringify interval nicely (e.g. 52-64)
            if isinstance(name, pd.Interval):
                bracket = f"{int(name.left)}-{int(name.right)}"
            else:
                bracket = str(name)
                
            plot_data.append({
                'Method': method.upper(),
                'Age Bracket': bracket,
                'Sensitivity (Recall)': rec
            })
            
    p_df = pd.DataFrame(plot_data)
    
    if p_df.empty:
        return
        
    plt.figure(figsize=(10, 6))
    sns.barplot(data=p_df, x='Age Bracket', y='Sensitivity (Recall)', hue='Method')
    plt.title('Model Sensitivity (Recall) Stratified by Patient Age')
    plt.ylim(0, 1.05)
    plt.grid(axis='y', alpha=0.3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.savefig(os.path.join(out_dir, "age_stratified_recall.png"), dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

def plot_feature_importances(data_path, out_dir):
    """Refits the baseline LogReg and XGBoost globally purely to extract standard Importances."""
    print("Re-fitting models globally for deterministic Feature Importances...")
    
    # Hook into analysis module loader
    drop_cols = ["person_id", "index_uti_date", "first_recurrent_uti_date", "first_abx_0_10d_date"]
    target_col = "recurrent_uti_90d_flag"
    
    # Route STDOUT dynamically to prevent script spam
    import sys, io
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    
    loader = CohortDataLoader(data_path=data_path, target_col=target_col, drop_cols=drop_cols)
    patients_info, predictors, targets = loader.load_data()
    X, y, feature_names = preprocess_data(patients_info, predictors, targets, target_col)
    
    # Restore stdout
    sys.stdout = old_stdout
    
    # --- Logistic Regression Coefficients ---
    lr_pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('model', LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42))
    ])
    lr_pipe.fit(X, y)
    coefs = lr_pipe.named_steps['model'].coef_[0]
    
    lr_df = pd.DataFrame({'feature': feature_names, 'importance': coefs})
    lr_df['abs_imp'] = lr_df['importance'].abs()
    lr_df = lr_df.sort_values(by='abs_imp', ascending=False).head(15)
    
    # --- XGBoost Gain ---
    xgb_pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('model', xgb.XGBClassifier(
            n_estimators=300, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, random_state=42, eval_metric="logloss"
        ))
    ])
    xgb_pipe.fit(X, y)
    xgb_imp = xgb_pipe.named_steps['model'].feature_importances_
    
    xgb_df = pd.DataFrame({'feature': feature_names, 'importance': xgb_imp})
    xgb_df = xgb_df.sort_values(by='importance', ascending=False).head(15)
    
    # Plotting side by side
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    sns.barplot(data=lr_df, x='importance', y='feature', ax=ax1, palette="coolwarm")
    ax1.set_title("Top 15 Logistic Regression Coefficients (Log-Odds)")
    ax1.set_xlabel("Coefficient Weight")
    ax1.set_ylabel("")
    
    sns.barplot(data=xgb_df, x='importance', y='feature', ax=ax2, palette="viridis")
    ax2.set_title("Top 15 XGBoost Feature Importances (Gain)")
    ax2.set_xlabel("Importance Level")
    ax2.set_ylabel("")
    
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "feature_importances.png"), dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

def main():
    parser = argparse.ArgumentParser(description="Generate Visualizations from Analysis Results")
    parser.add_argument("--run_dir", type=str, required=True, help="Path to the targeted results directory")
    parser.add_argument("--data", type=str, required=True, help="Path to original data CSV for metadata joining")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.run_dir):
        print(f"Error: Directory {args.run_dir} does not exist.")
        return
        
    plots_dir = os.path.join(args.run_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)
    
    print(f"Loading outputs from {args.run_dir}...")
    results_dict = load_results(args.run_dir)
    
    print(f"Generating PR Curves...")
    plot_pr_curves(results_dict, plots_dir)
    
    print(f"Generating Confusion Matrices...")
    plot_confusion_matrices(results_dict, plots_dir)
    
    print(f"Generating Calibration Curves...")
    plot_calibration_curves(results_dict, plots_dir)
    
    print(f"Generating Age-Stratified Sensitivity Bar Chart...")
    raw_data = pd.read_csv(args.data)
    plot_age_stratified_recall(results_dict, raw_data, plots_dir)
    
    print(f"Generating Feature Importance Plots...")
    plot_feature_importances(args.data, plots_dir)
    
    print(f"\n[+] All plots fully generated and safely placed in: {plots_dir}")

if __name__ == "__main__":
    main()
