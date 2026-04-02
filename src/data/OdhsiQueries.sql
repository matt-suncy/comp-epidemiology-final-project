-- A "notebook" of SQL queries for the OHDSI database. 
-- These are not meant to be run as a script, 
-- but rather to be copied and pasted into a SQL client for execution.

-- Define schema parameter
-- USE YOUR SPECIFIC SCHEMA: e.g., @cdm_schema = 'synpuf5.dbo'

WITH uti_concepts AS (
    -- SNOMED CT concepts for UTI and descendants
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 81902 -- Example OMOP concept ID for UTI
),
abx_ingredients AS (
    -- RxNorm Ingredient concepts for targeted empirical antibiotics
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id IN (
        -- TODO: Replace with exact RxNorm Ingredient Concept IDs for your study
        -- E.g., Nitrofurantoin, Cephalexin, Ciprofloxacin, Trimethoprim
        920293, 1786621, 1797513, 1705674 
    )
),

sepsis_concepts AS (
    -- SNOMED CT concepts for Sepsis and Urosepsis
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 132797
),

-- Step 1: Identify Index Events (Outpatient UTI Diagnosis)
outpatient_uti AS (
    SELECT 
        p.person_id, 
        v.visit_start_date AS index_date
    FROM person p
    JOIN visit_occurrence v ON p.person_id = v.person_id
    JOIN condition_occurrence co ON v.visit_occurrence_id = co.visit_occurrence_id
    WHERE (v.visit_concept_id = 9202 OR v.visit_concept_id = 9201)
      AND co.condition_concept_id IN (SELECT concept_id FROM uti_concepts)
),

-- Step 2: Form Target Cohort (T)
-- Patients with a UTI diagnosis who receive an antibiotic within 3 days
target_cohort_unfiltered AS (
    SELECT 
        ou.person_id,
        ou.index_date AS uti_date,
        de.drug_era_start_date AS index_antibiotic_date,
        de.drug_concept_id AS index_antibiotic_concept_id
    FROM outpatient_uti ou
    JOIN drug_era de 
        ON ou.person_id = de.person_id
        AND de.drug_concept_id IN (SELECT concept_id FROM abx_ingredients)
        AND de.drug_era_start_date BETWEEN ou.index_date AND DATEADD(day, 3, ou.index_date)
    -- Ensure 365 days of continuous prior observation and 30 days post-index
    JOIN observation_period op 
        ON ou.person_id = op.person_id
        AND op.observation_period_start_date <= DATEADD(day, -365, de.drug_era_start_date)
        AND op.observation_period_end_date >= DATEADD(day, 30, de.drug_era_start_date)
),

-- Restrict to the first UTI event per patient to maintain statistical independence
target_cohort AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY index_antibiotic_date ASC) as row_num
        FROM target_cohort_unfiltered
    ) ranked
    WHERE row_num = 1
),

-- Step 3: Identify First Occurrence of Each Failure Tier (within 30 days)
-- Tier 1: Antibiotic Switch
outcome_switch AS (
    SELECT 
        c.person_id,
        MIN(de2.drug_era_start_date) AS first_switch_date
    FROM target_cohort c
    JOIN drug_era de2 
        ON c.person_id = de2.person_id
        AND de2.drug_concept_id IN (SELECT concept_id FROM abx_ingredients)
        AND de2.drug_concept_id!= c.index_antibiotic_concept_id 
        AND de2.drug_era_start_date BETWEEN DATEADD(day, 1, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
    GROUP BY c.person_id
),

-- Tier 2: ER Visit
outcome_er AS (
    SELECT 
        c.person_id,
        MIN(v.visit_start_date) AS first_er_date
    FROM target_cohort c
    JOIN visit_occurrence v 
        ON c.person_id = v.person_id
        AND v.visit_concept_id = 9203 -- Standard OMOP Emergency Room Visit Concept
        AND v.visit_start_date BETWEEN DATEADD(day, 1, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
    GROUP BY c.person_id
),

-- Tier 3: Sepsis
outcome_sepsis AS (
    SELECT 
        c.person_id,
        MIN(co.condition_start_date) AS first_sepsis_date
    FROM target_cohort c
    JOIN condition_occurrence co 
        ON c.person_id = co.person_id
        AND co.condition_concept_id IN (SELECT concept_id FROM sepsis_concepts)
        AND co.condition_start_date BETWEEN DATEADD(day, 1, c.index_antibiotic_date) AND DATEADD(day, 30, c.index_antibiotic_date)
    GROUP BY c.person_id
)

-- Step 4: Final Dataset Assembly with Early/Late Timeframes
SELECT 
    c.person_id,
    c.index_antibiotic_date,
    c.index_antibiotic_concept_id,
    
    -- Overall Binary Failure Flag (Any failure within 30 days)
    CASE WHEN os.person_id IS NOT NULL OR oe.person_id IS NOT NULL OR ose.person_id IS NOT NULL THEN 1 ELSE 0 END AS failure_any_30d_flag,
    
    -- Tier 1: Antibiotic Switch (Early vs Late)
    CASE WHEN os.first_switch_date <= DATEADD(day, 7, c.index_antibiotic_date) THEN 1 ELSE 0 END AS tier1_switch_early_7d_flag,
    CASE WHEN os.first_switch_date > DATEADD(day, 7, c.index_antibiotic_date) THEN 1 ELSE 0 END AS tier1_switch_late_8_30d_flag,

    -- Tier 2: ER Visit (Early vs Late)
    CASE WHEN oe.first_er_date <= DATEADD(day, 7, c.index_antibiotic_date) THEN 1 ELSE 0 END AS tier2_er_early_7d_flag,
    CASE WHEN oe.first_er_date > DATEADD(day, 7, c.index_antibiotic_date) THEN 1 ELSE 0 END AS tier2_er_late_8_30d_flag,
    
    -- Tier 3: Sepsis (Early vs Late)
    CASE WHEN ose.first_sepsis_date <= DATEADD(day, 7, c.index_antibiotic_date) THEN 1 ELSE 0 END AS tier3_sepsis_early_7d_flag,
    CASE WHEN ose.first_sepsis_date > DATEADD(day, 7, c.index_antibiotic_date) THEN 1 ELSE 0 END AS tier3_sepsis_late_8_30d_flag

FROM target_cohort c
LEFT JOIN outcome_switch os ON c.person_id = os.person_id
LEFT JOIN outcome_er oe ON c.person_id = oe.person_id
LEFT JOIN outcome_sepsis ose ON c.person_id = ose.person_id;