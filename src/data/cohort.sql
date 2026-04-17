WITH uti_concepts AS (
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 81902
),

     abx_concepts AS (
         -- Use concept_ancestor to capture all formulations/strengths of each ingredient,
         -- rather than enumerating specific drug concept IDs.
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id IN (
             -- Cephalosporins
             1778162,  -- Cefazolin (Ingredient)
             1774470,  -- Ceftriaxone (Ingredient)
             -- Fluoroquinolones
             1741122,  -- Levofloxacin (Ingredient)
             1797513,  -- Ciprofloxacin (Ingredient)
             -- Penicillins
             723013,   -- Amoxicillin (Ingredient)
             1717327,  -- Ampicillin (Ingredient)
             -- Nitrofurans
             1717206,  -- Nitrofurantoin (Ingredient)
             -- TMP-SMX components
             1836430,  -- Sulfamethoxazole (Ingredient)
             1719799,  -- Trimethoprim (Ingredient)
             -- Other commonly used antibiotics
             1789276,  -- Fosfomycin (Ingredient)
             919345,   -- Gentamicin (Ingredient)
             1777806,  -- Vancomycin (Ingredient)
             1707346,  -- Metronidazole (Ingredient)
             1734104,  -- Tetracycline (Ingredient)
             997881,   -- Clindamycin (Ingredient)
             1738521   -- Erythromycin (Ingredient)
         )
     ),

-- Create an inclusive list of all systemic antibiotics for switch/escalation tracking
     all_antibiotics AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id = 21602796 -- ATC Class J01: ANTIBACTERIALS FOR SYSTEMIC USE
     ),

     sepsis_concepts AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id in (
            132797, -- Sepsis
            75576,  -- Pyelonephritis (Kidney infection)
            197320  -- Acute kidney injury
         )
     ),

-- Step 1: Identify UTI diagnosis events
     outpatient_uti AS (
         SELECT
             v.person_id,
             v.visit_start_date AS index_date,
             v.visit_occurrence_id
         FROM visit_occurrence v
                  JOIN condition_occurrence co
                       ON v.visit_occurrence_id = co.visit_occurrence_id
         WHERE v.visit_concept_id IN (
              9202,    -- Outpatient Visit
              9201,    -- Inpatient Visit
              581477,  -- Outpatient Visit (non-hospital)
              5083,    -- Urgent Care
              32693    -- Telehealth
          )
           AND co.condition_concept_id IN (SELECT concept_id FROM uti_concepts)
     ),

-- Step 2: Define index antibiotic treatment
     target_cohort_unfiltered AS (
         SELECT
             ou.person_id,
             ou.index_date AS uti_date,
             de.drug_exposure_start_date AS index_antibiotic_date,
             de.drug_concept_id AS index_antibiotic_concept_id
         FROM outpatient_uti ou
                  JOIN drug_exposure de
                       ON ou.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN DATEADD(day, -3, ou.index_date)
                              AND DATEADD(day, 14, ou.index_date)
                  JOIN observation_period op
                       ON ou.person_id = op.person_id
                           AND op.observation_period_start_date <= DATEADD(day, -90, de.drug_exposure_start_date)
                           AND op.observation_period_end_date   >= DATEADD(day,  30, de.drug_exposure_start_date)
     ),

-- Keep first eligible antibiotic-treated UTI per person
     target_cohort AS (
         SELECT *
         FROM (
                  SELECT
                      *,
                      ROW_NUMBER() OVER (
                          PARTITION BY person_id, uti_date
                          ORDER BY index_antibiotic_date ASC
                          ) AS row_num
                  FROM target_cohort_unfiltered
              ) ranked
         WHERE row_num = 1
     ),

-- Tier 1: Antibiotic switch within 30 days
     outcome_switch AS (
         SELECT
             c.person_id,
             c.uti_date,
             MIN(de2.drug_exposure_start_date) AS first_switch_date
         FROM target_cohort c
                  JOIN drug_exposure de2
                       ON c.person_id = de2.person_id
                           AND de2.drug_concept_id IN (SELECT concept_id FROM all_antibiotics)
                           AND de2.drug_concept_id <> c.index_antibiotic_concept_id
                           AND CAST(de2.drug_exposure_start_date AS DATE) BETWEEN DATEADD(day, 1, CAST(c.index_antibiotic_date AS DATE))
                              AND DATEADD(day, 30, CAST(c.index_antibiotic_date AS DATE))
         GROUP BY c.person_id, c.uti_date
     ),

-- Tier 2: ER visit within 30 days
     outcome_er AS (
         SELECT
             c.person_id,
             c.uti_date,
             MIN(v.visit_start_date) AS first_er_date
         FROM target_cohort c
                  JOIN visit_occurrence v
                       ON c.person_id = v.person_id
                           AND v.visit_concept_id IN (9201, 9203, 262, 8717, 32036) --NOTE: does 9202 (outpaatient) belong here?
                           AND CAST(v.visit_start_date AS DATE) BETWEEN DATEADD(day, 1, CAST(c.index_antibiotic_date AS DATE))
                              AND DATEADD(day, 30, CAST(c.index_antibiotic_date AS DATE))
         GROUP BY c.person_id, c.uti_date
     ),

-- Tier 3: Sepsis within 30 days
     outcome_sepsis AS (
         SELECT
             c.person_id,
             c.uti_date,
             MIN(co.condition_start_date) AS first_sepsis_date
         FROM target_cohort c
                  JOIN condition_occurrence co
                       ON c.person_id = co.person_id
                           AND co.condition_concept_id IN (SELECT concept_id FROM sepsis_concepts)
                           AND CAST(co.condition_start_date AS DATE) BETWEEN DATEADD(day, 1, CAST(c.index_antibiotic_date AS DATE))
                              AND DATEADD(day, 30, CAST(c.index_antibiotic_date AS DATE))
         GROUP BY c.person_id, c.uti_date
     )

-- Final analytic dataset
SELECT
    c.person_id,
    c.uti_date,
    c.index_antibiotic_date,
    c.index_antibiotic_concept_id,

    CASE
        WHEN os.person_id IS NOT NULL
            OR oe.person_id IS NOT NULL
            OR ose.person_id IS NOT NULL
            THEN 1 ELSE 0
        END AS failure_any_30d_flag,

    CASE
        WHEN os.first_switch_date IS NOT NULL
            AND os.first_switch_date <= DATEADD(day, 7, c.index_antibiotic_date)
            THEN 1 ELSE 0
        END AS tier1_switch_early_7d_flag,

    CASE
        WHEN os.first_switch_date IS NOT NULL
            AND os.first_switch_date > DATEADD(day, 7, c.index_antibiotic_date)
            THEN 1 ELSE 0
        END AS tier1_switch_late_8_30d_flag,

    CASE
        WHEN oe.first_er_date IS NOT NULL
            AND oe.first_er_date <= DATEADD(day, 7, c.index_antibiotic_date)
            THEN 1 ELSE 0
        END AS tier2_er_early_7d_flag,

    CASE
        WHEN oe.first_er_date IS NOT NULL
            AND oe.first_er_date > DATEADD(day, 7, c.index_antibiotic_date)
            THEN 1 ELSE 0
        END AS tier2_er_late_8_30d_flag,

    CASE
        WHEN ose.first_sepsis_date IS NOT NULL
            AND ose.first_sepsis_date <= DATEADD(day, 7, c.index_antibiotic_date)
            THEN 1 ELSE 0
        END AS tier3_sepsis_early_7d_flag,

    CASE
        WHEN ose.first_sepsis_date IS NOT NULL
            AND ose.first_sepsis_date > DATEADD(day, 7, c.index_antibiotic_date)
            THEN 1 ELSE 0
        END AS tier3_sepsis_late_8_30d_flag

FROM target_cohort c
         LEFT JOIN outcome_switch os
                   ON c.person_id = os.person_id AND c.uti_date = os.uti_date
         LEFT JOIN outcome_er oe
                   ON c.person_id = oe.person_id AND c.uti_date = oe.uti_date
         LEFT JOIN outcome_sepsis ose
                   ON c.person_id = ose.person_id AND c.uti_date = ose.uti_date;