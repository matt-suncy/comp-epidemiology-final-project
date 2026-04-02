WITH uti_concepts AS (
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 81902
),

     abx_concepts AS (
         -- Broad antibiotic set based
         SELECT concept_id
         FROM (
                  VALUES
                      -- Cephalosporins
                      (46287340), -- Cefazolin 500 MG Injection
                      (19074977), -- Ceftriaxone 250 MG Injection

                      -- Fluoroquinolones
                      (46287433), -- Levofloxacin Injection
                      (1797513),  -- Ciprofloxacin (Ingredient)
                      (19075380), -- Ciprofloxacin 500 MG Oral Tablet
                      (1797515),  -- Ciprofloxacin 250 MG Oral Tablet
                      (1797516),  -- Ciprofloxacin 750 MG Oral Tablet

                      -- Penicillins
                      (19073187), -- Amoxicillin 500 MG Oral Capsule
                      (19073183), -- Amoxicillin 250 MG Oral Capsule
                      (19073189), -- Amoxicillin 875 MG Oral Tablet
                      (1717327),  -- Ampicillin (Ingredient)
                      (19073219), -- Ampicillin 500 MG Oral Capsule

                      -- Nitrofurans
                      (920334),   -- Nitrofurantoin, Macrocrystals 50 MG Oral Capsule

                      -- TMP-SMX
                      (1836434),  -- Sulfamethoxazole 800 MG / Trimethoprim 160 MG Oral Tablet
                      (1836433),  -- Sulfamethoxazole 400 MG / Trimethoprim 80 MG Oral Tablet

                      -- Other antibiotics observed in your data
                      (919345),   -- Gentamicin Sulfate (USP)
                      (40087247), -- Vancomycin Injectable Solution
                      (1707346),  -- Metronidazole 500 MG Oral Tablet
                      (19080187), -- Metronidazole 250 MG Oral Tablet
                      (19019852), -- Tetracycline 250 MG Oral Capsule
                      (997899),   -- Clindamycin 300 MG Oral Capsule
                      (1836521)   -- Erythromycin Ethylsuccinate / Sulfisoxazole Oral Suspension
              ) AS t(concept_id)
     ),

     sepsis_concepts AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id = 132797
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
         WHERE (v.visit_concept_id = 9202 OR v.visit_concept_id = 9201)
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
                           AND de.drug_exposure_start_date BETWEEN DATEADD(day, -1, ou.index_date)
                              AND DATEADD(day,  7, ou.index_date)
                  JOIN observation_period op
                       ON ou.person_id = op.person_id
                           AND op.observation_period_start_date <= DATEADD(day, -180, de.drug_exposure_start_date)
                           AND op.observation_period_end_date   >= DATEADD(day,   30, de.drug_exposure_start_date)
     ),

-- Keep first eligible antibiotic-treated UTI per person
     target_cohort AS (
         SELECT *
         FROM (
                  SELECT
                      *,
                      ROW_NUMBER() OVER (
                          PARTITION BY person_id
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
             MIN(de2.drug_exposure_start_date) AS first_switch_date
         FROM target_cohort c
                  JOIN drug_exposure de2
                       ON c.person_id = de2.person_id
                           AND de2.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de2.drug_concept_id <> c.index_antibiotic_concept_id
                           AND de2.drug_exposure_start_date BETWEEN DATEADD(day, 1, c.index_antibiotic_date)
                              AND DATEADD(day, 30, c.index_antibiotic_date)
         GROUP BY c.person_id
     ),

-- Tier 2: ER visit within 30 days
     outcome_er AS (
         SELECT
             c.person_id,
             MIN(v.visit_start_date) AS first_er_date
         FROM target_cohort c
                  JOIN visit_occurrence v
                       ON c.person_id = v.person_id
                           AND v.visit_concept_id = 9203
                           AND v.visit_start_date BETWEEN DATEADD(day, 1, c.index_antibiotic_date)
                              AND DATEADD(day, 30, c.index_antibiotic_date)
         GROUP BY c.person_id
     ),

-- Tier 3: Sepsis within 30 days
     outcome_sepsis AS (
         SELECT
             c.person_id,
             MIN(co.condition_start_date) AS first_sepsis_date
         FROM target_cohort c
                  JOIN condition_occurrence co
                       ON c.person_id = co.person_id
                           AND co.condition_concept_id IN (SELECT concept_id FROM sepsis_concepts)
                           AND co.condition_start_date BETWEEN DATEADD(day, 1, c.index_antibiotic_date)
                              AND DATEADD(day, 30, c.index_antibiotic_date)
         GROUP BY c.person_id
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
                   ON c.person_id = os.person_id
         LEFT JOIN outcome_er oe
                   ON c.person_id = oe.person_id
         LEFT JOIN outcome_sepsis ose
                   ON c.person_id = ose.person_id;