# A "notebook" of SQL queries for the OHDSI database. 
# These are not meant to be run as a script, 
# but rather to be copied and pasted into a SQL client for execution.

-- Define schema parameter
-- USE YOUR SPECIFIC SCHEMA: e.g., @cdm_schema = 'synpuf5.dbo'

WITH uti_concepts AS (
    -- SNOMED CT concepts for UTI and descendants
    SELECT descendant_concept_id AS concept_id
    FROM @cdm_schema.concept_ancestor
    WHERE ancestor_concept_id = 224 -- Example OMOP concept ID for UTI
),

abx_ingredients AS (
    -- RxNorm Ingredient concepts for targeted empirical antibiotics
    SELECT descendant_concept_id AS concept_id
    FROM @cdm_schema.concept_ancestor
    WHERE ancestor_concept_id IN (
        -- Replace with exact RxNorm Ingredient Concept IDs for your study
        -- E.g., Nitrofurantoin, Cephalexin, Ciprofloxacin, Trimethoprim
        @nitrofurantoin_id, @cephalexin_id, @ciprofloxacin_id, @trimethoprim_id 
    )
),

sepsis_concepts AS (
    -- SNOMED CT concepts for Sepsis and Urosepsis
    SELECT descendant_concept_id AS concept_id
    FROM @cdm_schema.concept_ancestor
    WHERE ancestor_concept_id = 132281007 -- Example OMOP concept ID for Sepsis
),

-- Step 1: Identify Index Events (Outpatient UTI Diagnosis)
outpatient_uti AS (
    SELECT 
        p.person_id, 
        v.visit_start_date AS index_date,
        p.year_of_birth
    FROM @cdm_schema.person p
    JOIN @cdm_schema.visit_occurrence v ON p.person_id = v.person_id
    JOIN @cdm_schema.condition_occurrence co ON v.visit_occurrence_id = co.visit_occurrence_id
    WHERE v.visit_concept_id = 9202 -- Standard OMOP Outpatient Visit Concept
      AND co.condition_concept_id IN (SELECT concept_id FROM uti_concepts)
),

-- Step 2: Form Target Cohort (T) - All treated outpatients aged 65+
target_cohort_unfiltered AS (
    SELECT 
        ou.person_id,
        ou.index_date AS uti_date,
        de.drug_era_start_date AS index_antibiotic_date,
        de.drug_concept_id AS index_antibiotic_concept_id
    FROM outpatient_uti ou
    -- Join to drug_era to group overlapping prescriptions of the same active ingredient
    JOIN @cdm_schema.drug_era de 
        ON ou.person_id = de.person_id
        AND de.drug_concept_id IN (SELECT concept_id FROM abx_ingredients)
        AND de.drug_era_start_date BETWEEN ou.index_date AND DATEADD(day, 3, ou.index_date)
    -- Ensure 365 days of continuous prior observation and 30 days post-index
    JOIN @cdm_schema.observation_period op 
        ON ou.person_id = op.person_id
        AND op.observation_period_start_date <= DATEADD(day, -365, de.drug_era_start_date)
        AND op.observation_period_end_date >= DATEADD(day, 30, de.drug_era_start_date)
    WHERE (YEAR(de.drug_era_start_date) - ou.year_of_birth) >= 65
),

-- Restrict to the first UTI event per patient to maintain independence
target_cohort AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY index_antibiotic_date ASC) as row_num
        FROM target_cohort_unfiltered
    ) ranked
    WHERE row_num = 1
),

-- Step 3: Disaggregate Outcomes (O) (3 to 30 days post-index)
outcome_switch AS (
    SELECT DISTINCT c.person_id, 1 AS switch_flag
    FROM target_cohort c
    JOIN @cdm_schema.drug_era de2 
        ON c.person_id = de2.person_id
        AND de2.drug_concept_id IN (SELECT concept_id FROM abx_ingredients)
        AND de2.drug_concept_id!= c.index_antibiotic_concept_id -- Different pharmacological ingredient
        AND de2.drug_era_start_date BETWEEN DATEADD(day, 3, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
),

outcome_er AS (
    SELECT DISTINCT c.person_id, 1 AS er_flag
    FROM target_cohort c
    JOIN @cdm_schema.visit_occurrence v 
        ON c.person_id = v.person_id
        AND v.visit_concept_id = 9203 -- Standard OMOP Emergency Room Visit Concept
        AND v.visit_start_date BETWEEN DATEADD(day, 3, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
),

outcome_inpatient AS (
    SELECT DISTINCT c.person_id, 1 AS inpatient_flag
    FROM target_cohort c
    JOIN @cdm_schema.visit_occurrence v 
        ON c.person_id = v.person_id
        AND v.visit_concept_id = 9201 -- Standard OMOP Inpatient Visit Concept
        AND v.visit_start_date BETWEEN DATEADD(day, 3, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
),

outcome_sepsis AS (
    SELECT DISTINCT c.person_id, 1 AS sepsis_flag
    FROM target_cohort c
    JOIN @cdm_schema.condition_occurrence co 
        ON c.person_id = co.person_id
        AND co.condition_concept_id IN (SELECT concept_id FROM sepsis_concepts)
        AND co.condition_start_date BETWEEN DATEADD(day, 3, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
),

-- Step 4: Extract Temporal Features (e.g., Prior Antibiotic Use)
feature_prior_abx AS (
    SELECT DISTINCT c.person_id, 1 AS prior_abx_90d_flag
    FROM target_cohort c
    JOIN @cdm_schema.drug_era de 
        ON c.person_id = de.person_id
        AND de.drug_concept_id IN (SELECT concept_id FROM abx_ingredients)
        AND de.drug_era_end_date BETWEEN DATEADD(day, -90, c.index_antibiotic_date) AND DATEADD(day, -1, c.index_antibiotic_date)
)

-- Step 5: Final Dataset Assembly
SELECT 
    c.person_id,
    c.index_antibiotic_date,
    c.index_antibiotic_concept_id,
    
    -- Independent Target Outcome Columns (Disaggregated)
    COALESCE(os.switch_flag, 0) AS outcome_abx_switch_30d,
    COALESCE(oe.er_flag, 0) AS outcome_er_visit_30d,
    COALESCE(oi.inpatient_flag, 0) AS outcome_inpatient_30d,
    COALESCE(ose.sepsis_flag, 0) AS outcome_sepsis_30d,
    
    -- Baseline Features
    COALESCE(fpa.prior_abx_90d_flag, 0) AS feature_prior_abx_90d

FROM target_cohort c
LEFT JOIN outcome_switch os ON c.person_id = os.person_id
LEFT JOIN outcome_er oe ON c.person_id = oe.person_id
LEFT JOIN outcome_inpatient oi ON c.person_id = oi.person_id
LEFT JOIN outcome_sepsis ose ON c.person_id = ose.person_id
LEFT JOIN feature_prior_abx fpa ON c.person_id = fpa.person_id;