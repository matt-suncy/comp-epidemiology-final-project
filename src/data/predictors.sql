/*
==============================================================================
Predictors Extraction for Patient Cohort
==============================================================================
Purpose:
This script extracts demographic and clinical predictors for a list of patients.
It is designed to run against an OMOP Common Data Model (CDM) database.

Required inputs:
A pre-existing table or CTE named `patient_cohort` containing:
  - person_id: the patient identifier
  - index_date: the reference date (e.g., UTI diagnosis or antibiotic start)
                to look back from.

Predictors extracted:
1. Demographics: Age (at index), Sex
2. Clinical History:
   - Any Kidney Disease (prior to index)
   - Chronic Kidney Disease (CKD) (prior to index)
3. Healthcare Utilization:
   - Prior Hospitalization (within 1 year prior to index)
==============================================================================
*/

-- ---------------------------------------------------------------------------
-- 0. Define the input cohort
-- NOTE: Replace this CTE with a SELECT from your actual cohort table.
-- ---------------------------------------------------------------------------
WITH patient_cohort AS (
    -- Example placeholder:
    -- SELECT person_id, index_date FROM my_schema.my_target_cohort
    SELECT
        person_id,
        uti_date AS index_date
    FROM target_cohort 
),

-- ---------------------------------------------------------------------------
-- 1. Concept Sets for Clinical Predictors
-- Using concept_ancestor allows us to capture all specific subtypes of a disease.
-- ---------------------------------------------------------------------------
kidney_disease_concepts AS (
    -- Broad definition: "Disorder of kidney" (SNOMED: 443611)
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 443611
),

ckd_concepts AS (
    -- Narrow definition: "Chronic kidney disease" (SNOMED: 46271022)
    -- This is a child of 443611, but worth separating out as a specific predictor.
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 46271022
),

-- ---------------------------------------------------------------------------
-- 2. Demographics Baseline
-- Extracts Age and Sex from the person table.
-- ---------------------------------------------------------------------------
demographics AS (
    SELECT
        c.person_id,
        c.index_date,
        -- Calculate age precisely with 2 decimal places based on accurate days between birth and index
        CAST(DATEDIFF(day, DATEFROMPARTS(p.year_of_birth, COALESCE(p.month_of_birth, 1), COALESCE(p.day_of_birth, 1)), c.index_date) / 365.25 AS DECIMAL(10,2)) AS age_at_index,
        p.gender_concept_id,
        CASE
            WHEN p.gender_concept_id = 8507 THEN 'Male'
            WHEN p.gender_concept_id = 8532 THEN 'Female'
            ELSE 'Unknown/Other'
        END AS sex
    FROM patient_cohort c
    JOIN person p
      ON c.person_id = p.person_id
),

-- ---------------------------------------------------------------------------
-- 3. Clinical Conditions (Lookback: Anytime and 365 Days Prior)
-- Uses MAX() to roll up multiple occurrences into single yes/no flags.
-- ---------------------------------------------------------------------------
condition_history AS (
    SELECT
        c.person_id,
        c.index_date,
        -- "Ever" flags (Anytime strictly before index date)
        MAX(CASE WHEN kdc.concept_id IS NOT NULL THEN 1 ELSE 0 END) AS history_any_kidney_disease_ever,
        MAX(CASE WHEN ckd.concept_id IS NOT NULL THEN 1 ELSE 0 END) AS history_ckd_ever,
        -- "Within 365 Days" flags
        MAX(CASE WHEN kdc.concept_id IS NOT NULL AND CAST(co.condition_start_date AS DATE) >= DATEADD(day, -365, CAST(c.index_date AS DATE)) THEN 1 ELSE 0 END) AS history_any_kidney_disease_365d,
        MAX(CASE WHEN ckd.concept_id IS NOT NULL AND CAST(co.condition_start_date AS DATE) >= DATEADD(day, -365, CAST(c.index_date AS DATE)) THEN 1 ELSE 0 END) AS history_ckd_365d
    FROM patient_cohort c
    LEFT JOIN condition_occurrence co
           ON c.person_id = co.person_id
          -- Ensure the condition occurred strictly BEFORE the index date
          AND CAST(co.condition_start_date AS DATE) < CAST(c.index_date AS DATE)
    LEFT JOIN kidney_disease_concepts kdc
           ON co.condition_concept_id = kdc.concept_id
    LEFT JOIN ckd_concepts ckd
           ON co.condition_concept_id = ckd.concept_id
    GROUP BY c.person_id, c.index_date
),

-- ---------------------------------------------------------------------------
-- 4. Healthcare Utilization (Lookback: 1 Year Prior)
-- Counts the number of inpatient admissions within the 365 days leading up to index.
-- ---------------------------------------------------------------------------
prior_hospitalization AS (
    SELECT
        c.person_id,
        c.index_date,
        -- Count the unique admissions they've had
        COUNT(DISTINCT v.visit_occurrence_id) AS prior_hosp_1yr_count
    FROM patient_cohort c
    LEFT JOIN visit_occurrence v
           ON c.person_id = v.person_id
          -- Standard OMOP concepts for Inpatient admissions
          AND v.visit_concept_id IN (9201, 262, 8717)
          -- Restrict to the 1 year (365 days) prior to the index date to capture recent severe illness
          AND CAST(v.visit_start_date AS DATE) BETWEEN DATEADD(day, -365, CAST(c.index_date AS DATE)) 
                                                   AND DATEADD(day, -1, CAST(c.index_date AS DATE))
    GROUP BY c.person_id, c.index_date
)

-- ---------------------------------------------------------------------------
-- 5. Final Output Assembly
-- Bring all predictor modules together into one wide, patient-level analytical table.
-- ---------------------------------------------------------------------------
SELECT
    d.person_id,
    d.index_date,
    d.age_at_index,
    d.sex,
    
    -- COALESCE ensures that if a patient had no records (resulting in NULL from the LEFT JOIN), 
    -- they are safely assigned a 0 (No/None).
    COALESCE(ch.history_any_kidney_disease_ever, 0) AS hx_kidney_disease_ever_flag,
    COALESCE(ch.history_ckd_ever, 0) AS hx_ckd_ever_flag,
    COALESCE(ch.history_any_kidney_disease_365d, 0) AS hx_kidney_disease_365d_flag,
    COALESCE(ch.history_ckd_365d, 0) AS hx_ckd_365d_flag,
    COALESCE(ph.prior_hosp_1yr_count, 0) AS prior_hospitalization_1yr_count

FROM demographics d
LEFT JOIN condition_history ch
       ON d.person_id = ch.person_id AND d.index_date = ch.index_date
LEFT JOIN prior_hospitalization ph
       ON d.person_id = ph.person_id AND d.index_date = ph.index_date;
