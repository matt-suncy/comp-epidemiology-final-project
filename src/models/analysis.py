import os
import pandas as pd

def load_data(target_cohort_path: str, predictors_path: str, separator: str = ','):
    '''
    Unified function for loading target cohort and predictors.
    Arguments:
        target_cohort_path: path to the target cohort CSV file
        predictors_path: path to the predictors CSV file
    Returns:
        patients: DataFrame containing person_id and index_date
        predictors: DataFrame containing predictor variables (each col is a feature)
        targets: DataFrame containing target variables (each col is a target)
    '''
    # Check if the files exist
    if not os.path.exists(target_cohort_path):
        raise FileNotFoundError(f"Target cohort file not found: {target_cohort_path}")
    if not os.path.exists(predictors_path):
        raise FileNotFoundError(f"Predictors file not found: {predictors_path}")

    # Check if first two rows are patient_id and index_date
    target_cohort = pd.read_csv(target_cohort_path, nrows=2)
    predictors = pd.read_csv(predictors_path, nrows=2)
    if target_cohort.iloc[0, 0] != 'person_id' or target_cohort.iloc[0, 1] != 'index_date':
        raise ValueError("First two columns of target cohort file must be person_id and index_date")
    if predictors.iloc[0, 0] != 'person_id' or predictors.iloc[0, 1] != 'index_date':
        raise ValueError("First two columns of predictors file must be person_id and index_date")

    # Print basic info about the data
    print(f"Target cohort shape: {pd.read_csv(target_cohort_path).shape}")
    print(f"Predictors shape: {pd.read_csv(predictors_path).shape}")
    print(f"Target cohort columns: {pd.read_csv(target_cohort_path).columns}")
    print(f"Predictors columns: {pd.read_csv(predictors_path).columns}")
    
    # Load the data
    target_cohort = pd.read_csv(target_cohort_path)
    predictors = pd.read_csv(predictors_path)

    # drop first two columns (person_id, index_date) from both dataframes
    targets = target_cohort.iloc[:, 2:]
    predictors = predictors.iloc[:, 2:]
    patients = target_cohort.iloc[:, :2]

    return patients, predictors, targets

    

    
    
    
    