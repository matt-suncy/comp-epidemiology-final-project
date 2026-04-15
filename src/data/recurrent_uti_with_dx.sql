WITH uti_concepts AS (
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 81902
),

     abx_concepts AS (
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

     uti_diagnoses AS (
         SELECT
             co.person_id,
             co.condition_start_date AS uti_date,
             co.condition_occurrence_id
         FROM condition_occurrence co
         WHERE co.condition_concept_id IN (SELECT concept_id FROM uti_concepts)
     ),

     cohort_unfiltered AS (
         SELECT
             u.person_id,
             u.uti_date,
             de.drug_exposure_start_date AS index_antibiotic_date,
             de.drug_concept_id AS index_antibiotic_concept_id
         FROM uti_diagnoses u
                  JOIN drug_exposure de
                       ON u.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN u.uti_date
                              AND DATEADD(day, 10, u.uti_date)
                  JOIN observation_period op
                       ON u.person_id = op.person_id
                           AND op.observation_period_start_date <= DATEADD(day, -90, u.uti_date)
                           AND op.observation_period_end_date   >= DATEADD(day,  90, u.uti_date)
     ),

     cohort AS (
         SELECT *
         FROM (
                  SELECT
                      *,
                      ROW_NUMBER() OVER (
                          PARTITION BY person_id
                          ORDER BY uti_date ASC, index_antibiotic_date ASC
                          ) AS row_num
                  FROM cohort_unfiltered
              ) ranked
         WHERE row_num = 1
     ),

     recurrent_uti AS (
         SELECT
             c.person_id,
             MIN(u2.uti_date) AS first_recurrent_uti_date
         FROM cohort c
                  JOIN uti_diagnoses u2
                       ON c.person_id = u2.person_id
                           AND u2.uti_date >= DATEADD(day, 7, c.uti_date)
                           AND u2.uti_date <= DATEADD(day, 90, c.uti_date)
         GROUP BY c.person_id
     ),

     abx_count_in_index_window AS (
         SELECT
             c.person_id,
             COUNT(DISTINCT de.drug_concept_id) AS num_distinct_abx_in_window
         FROM cohort c
                  JOIN drug_exposure de
                       ON c.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN c.uti_date
                              AND DATEADD(day, 10, c.uti_date)
         GROUP BY c.person_id
     )

SELECT
    c.person_id,
    c.uti_date,
    c.index_antibiotic_date,
    c.index_antibiotic_concept_id,

    CASE
        WHEN r.person_id IS NOT NULL THEN 1
        ELSE 0
        END AS recurrent_uti_90d_flag,

    COALESCE(a.num_distinct_abx_in_window, 0) AS num_distinct_abx_in_window

FROM cohort c
         LEFT JOIN recurrent_uti r
                   ON c.person_id = r.person_id
         LEFT JOIN abx_count_in_index_window a
                   ON c.person_id = a.person_id;