import os
import pandas as pd

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
        print(f"Target cohort columns: {list(target_cohort.columns)}")
        print(f"Predictors columns: {list(predictors_df.columns)}")
        
        # Partition data into attributes
        self.targets = target_cohort.iloc[:, 2:]
        self.predictors = predictors_df.iloc[:, 2:]
        self.patients = target_cohort.iloc[:, :2]

        return self.patients, self.predictors, self.targets

def main():
    target_path = ''
    predictor_path = ''
    loader = CohortDataLoader(
        target_cohort_path="data/target_cohort.csv",
        predictors_path="data/predictors.csv"
    )
    patients, predictors, targets = loader.load_data()

if __name__ == "__main__":
    main()
    
    
    
    