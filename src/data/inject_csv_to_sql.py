import csv
import os

# ==============================================================================
# Configuration
# ==============================================================================
CSV_PATH = '/Users/matt_suncy/Documents/git-repos/comp-epidemiology-final-project/src/data/recurrent_uti_with_dx.csv'
SQL_PATH = '/Users/matt_suncy/Documents/git-repos/comp-epidemiology-final-project/src/data/predictors.sql'
OUTPUT_SQL_PATH = '/Users/matt_suncy/Documents/git-repos/comp-epidemiology-final-project/src/data/predictors_runnable.sql'

def main():
    print(f"Loading data from {CSV_PATH}...")
    
    if not os.path.exists(CSV_PATH):
        print(f"Error: Could not find {CSV_PATH}. Make sure you are running the script from the 'src/data' directory.")
        return
        
    values_list = []
    
    with open(CSV_PATH, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        
        for row in reader:
            if not row or len(row) < 2: 
                continue # Skip empty or malformed rows
                
            pid = row[0].strip()
            # The second column is the uti_date
            dt = str(row[1]).strip().split(" ")[0]
            values_list.append(f"({pid}, '{dt}')")
            
    print(f"Detected {len(values_list)} rows. Generating VALUES block...")
    
    # Join them with a comma and a newline for clean SQL formatting
    values_string = ",\n            ".join(values_list)
    
    # Build the new CTE
    new_cte = f"""WITH patient_cohort AS (
    SELECT person_id, CAST(index_date AS DATE) AS index_date
    FROM (
        VALUES 
            {values_string}
    ) AS t(person_id, index_date)
),

-- ---------------------------------------------------------------------------
-- 1. Concept Sets for Clinical Predictors
-- Using concept_ancestor allows us to capture all specific subtypes of a disease.
-- ---------------------------------------------------------------------------
"""

    print(f"Reading SQL template from {SQL_PATH}...")
    with open(SQL_PATH, 'r') as f:
        sql_content = f.read()
        
    # Safely replace the old CTE manually by targeting the markers
    start_marker = "WITH patient_cohort AS ("
    end_marker = "kidney_disease_concepts AS ("
    
    start_idx = sql_content.find(start_marker)
    end_idx = sql_content.find(end_marker)
    
    if start_idx == -1 or end_idx == -1:
        print("Error: Could not find the baseline CTEs in predictors.sql! Have they been renamed?")
        return
        
    new_sql_content = sql_content[:start_idx] + new_cte + sql_content[end_idx:]
    
    # Write out the ready-to-run query
    with open(OUTPUT_SQL_PATH, 'w') as f:
        f.write(new_sql_content)
        
    print(f"Success! Runnable SQL query with {len(values_list)} hardcoded patients saved to '{OUTPUT_SQL_PATH}'.\n"
          f"You can now copy and paste the contents of {OUTPUT_SQL_PATH} directly into your SQL runner!")

if __name__ == "__main__":
    main()
