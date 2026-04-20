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

     ckd_concepts AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id IN (46271022)
     ),

     htn_concepts AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id IN (316866)
     ),

     kidney_failure_concepts AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id IN (14669001)
     ),

     diabetes_concepts AS (
         SELECT descendant_concept_id AS concept_id
         FROM concept_ancestor
         WHERE ancestor_concept_id IN (201820)
     ),

     uti_diagnoses AS (
         SELECT
             co.person_id,
             co.condition_start_date AS uti_date,
             co.condition_occurrence_id
         FROM condition_occurrence co
         WHERE co.condition_concept_id IN (SELECT concept_id FROM uti_concepts)
     ),

     eligible_uti AS (
         SELECT
             u.person_id,
             u.uti_date,
             op.observation_period_start_date,
             op.observation_period_end_date
         FROM uti_diagnoses u
                  JOIN observation_period op
                       ON u.person_id = op.person_id
                           AND op.observation_period_start_date <= DATEADD(day, -90, u.uti_date)
                           AND op.observation_period_end_date   >= DATEADD(day,  90, u.uti_date)
     ),

     treated_uti_candidates AS (
         SELECT
             e.person_id,
             e.uti_date,
             de.drug_exposure_start_date AS first_abx_0_10d_date,
             de.drug_concept_id AS first_abx_0_10d_concept_id,
             ROW_NUMBER() OVER (
                 PARTITION BY e.person_id, e.uti_date
                 ORDER BY de.drug_exposure_start_date ASC, de.drug_concept_id ASC
                 ) AS abx_row_num
         FROM eligible_uti e
                  JOIN drug_exposure de
                       ON e.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN e.uti_date
                              AND DATEADD(day, 10, e.uti_date)
     ),

     treated_uti_events AS (
         SELECT
             person_id,
             uti_date,
             first_abx_0_10d_date,
             first_abx_0_10d_concept_id
         FROM treated_uti_candidates
         WHERE abx_row_num = 1
     ),

     index_uti AS (
         SELECT
             person_id,
             uti_date,
             first_abx_0_10d_date,
             first_abx_0_10d_concept_id
         FROM (
                  SELECT
                      person_id,
                      uti_date,
                      first_abx_0_10d_date,
                      first_abx_0_10d_concept_id,
                      ROW_NUMBER() OVER (
                          PARTITION BY person_id
                          ORDER BY uti_date ASC, first_abx_0_10d_date ASC
                          ) AS row_num
                  FROM treated_uti_events
              ) x
         WHERE row_num = 1
     ),

     recurrent_uti AS (
         SELECT
             i.person_id,
             MIN(u2.uti_date) AS first_recurrent_uti_date
         FROM index_uti i
                  JOIN uti_diagnoses u2
                       ON i.person_id = u2.person_id
                           AND u2.uti_date >= DATEADD(day, 7, i.uti_date)
                           AND u2.uti_date <= DATEADD(day, 90, i.uti_date)
         GROUP BY i.person_id
     ),

     abx_0_10d_all AS (
         SELECT
             i.person_id,
             de.drug_exposure_start_date,
             de.drug_concept_id
         FROM index_uti i
                  JOIN drug_exposure de
                       ON i.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN i.uti_date
                              AND DATEADD(day, 10, i.uti_date)
     ),

     abx_0_10d_summary AS (
         SELECT
             person_id,
             COUNT(DISTINCT drug_concept_id) AS num_distinct_abx_0_10d
         FROM abx_0_10d_all
         GROUP BY person_id
     ),

     repeat_abx_11_30d AS (
         SELECT
             i.person_id,
             MIN(de.drug_exposure_start_date) AS first_repeat_abx_date
         FROM index_uti i
                  JOIN drug_exposure de
                       ON i.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN DATEADD(day, 11, i.uti_date)
                              AND DATEADD(day, 30, i.uti_date)
         GROUP BY i.person_id
     ),

     er_30d AS (
         SELECT
             i.person_id,
             MIN(v.visit_start_date) AS first_er_date
         FROM index_uti i
                  JOIN visit_occurrence v
                       ON i.person_id = v.person_id
                           AND v.visit_concept_id = 9203
                           AND v.visit_start_date BETWEEN DATEADD(day, 1, i.uti_date)
                              AND DATEADD(day, 30, i.uti_date)
         GROUP BY i.person_id
     ),

     demographics AS (
         SELECT
             i.person_id,
             i.uti_date,
             p.gender_concept_id,
             CASE
                 WHEN p.year_of_birth IS NOT NULL THEN YEAR(i.uti_date) - p.year_of_birth
                 ELSE NULL
                 END AS age_at_index
         FROM index_uti i
                  JOIN person p
                       ON i.person_id = p.person_id
     ),

     prior_utilization AS (
         SELECT
             i.person_id,
             SUM(CASE
                     WHEN v.visit_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                         AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS total_visits_1yr,

             SUM(CASE
                     WHEN v.visit_concept_id = 9202
                         AND v.visit_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                              AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS outpatient_visits_1yr,

             SUM(CASE
                     WHEN v.visit_concept_id = 9203
                         AND v.visit_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                              AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS er_visits_1yr,

             SUM(CASE
                     WHEN v.visit_concept_id = 9201
                         AND v.visit_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                              AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS inpatient_visits_1yr
         FROM index_uti i
                  LEFT JOIN visit_occurrence v
                            ON i.person_id = v.person_id
                                AND v.visit_start_date < i.uti_date
         GROUP BY i.person_id
     ),

     prior_uti_history AS (
         SELECT
             i.person_id,
             COUNT(*) AS prior_uti_count_1yr
         FROM index_uti i
                  JOIN uti_diagnoses u
                       ON i.person_id = u.person_id
                           AND u.uti_date BETWEEN DATEADD(day, -365, i.uti_date)
                              AND DATEADD(day, -1, i.uti_date)
         GROUP BY i.person_id
     ),

     prior_abx_history AS (
         SELECT
             i.person_id,
             COUNT(*) AS prior_abx_exposure_count_1yr,
             COUNT(DISTINCT de.drug_concept_id) AS prior_distinct_abx_count_1yr
         FROM index_uti i
                  JOIN drug_exposure de
                       ON i.person_id = de.person_id
                           AND de.drug_concept_id IN (SELECT concept_id FROM abx_concepts)
                           AND de.drug_exposure_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                              AND DATEADD(day, -1, i.uti_date)
         GROUP BY i.person_id
     ),

     ckd_dx AS (
         SELECT
             i.person_id,
             i.uti_date,
             co.condition_start_date
         FROM index_uti i
                  JOIN condition_occurrence co
                       ON i.person_id = co.person_id
                           AND co.condition_start_date < i.uti_date
                  JOIN ckd_concepts c
                       ON co.condition_concept_id = c.concept_id
     ),

     htn_dx AS (
         SELECT
             i.person_id,
             i.uti_date,
             co.condition_start_date
         FROM index_uti i
                  JOIN condition_occurrence co
                       ON i.person_id = co.person_id
                           AND co.condition_start_date < i.uti_date
                  JOIN htn_concepts c
                       ON co.condition_concept_id = c.concept_id
     ),

     kidney_failure_dx AS (
         SELECT
             i.person_id,
             i.uti_date,
             co.condition_start_date
         FROM index_uti i
                  JOIN condition_occurrence co
                       ON i.person_id = co.person_id
                           AND co.condition_start_date < i.uti_date
                  JOIN kidney_failure_concepts c
                       ON co.condition_concept_id = c.concept_id
     ),

     diabetes_dx AS (
         SELECT
             i.person_id,
             i.uti_date,
             co.condition_start_date
         FROM index_uti i
                  JOIN condition_occurrence co
                       ON i.person_id = co.person_id
                           AND co.condition_start_date < i.uti_date
                  JOIN diabetes_concepts c
                       ON co.condition_concept_id = c.concept_id
     ),

     ckd_features AS (
         SELECT
             i.person_id,
             CASE WHEN COUNT(c.condition_start_date) > 0 THEN 1 ELSE 0 END AS ckd_ever_flag,
             CASE WHEN SUM(CASE
                               WHEN c.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                                   AND DATEADD(day, -1, i.uti_date)
                                   THEN 1 ELSE 0 END) > 0
                      THEN 1 ELSE 0 END AS ckd_1yr_flag,
             SUM(CASE
                     WHEN c.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                         AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS ckd_1yr_count
         FROM index_uti i
                  LEFT JOIN ckd_dx c
                            ON i.person_id = c.person_id
                                AND i.uti_date = c.uti_date
         GROUP BY i.person_id, i.uti_date
     ),

     htn_features AS (
         SELECT
             i.person_id,
             CASE WHEN COUNT(h.condition_start_date) > 0 THEN 1 ELSE 0 END AS htn_ever_flag,
             CASE WHEN SUM(CASE
                               WHEN h.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                                   AND DATEADD(day, -1, i.uti_date)
                                   THEN 1 ELSE 0 END) > 0
                      THEN 1 ELSE 0 END AS htn_1yr_flag,
             SUM(CASE
                     WHEN h.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                         AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS htn_1yr_count
         FROM index_uti i
                  LEFT JOIN htn_dx h
                            ON i.person_id = h.person_id
                                AND i.uti_date = h.uti_date
         GROUP BY i.person_id, i.uti_date
     ),

     kidney_failure_features AS (
         SELECT
             i.person_id,
             CASE WHEN COUNT(k.condition_start_date) > 0 THEN 1 ELSE 0 END AS kidney_failure_ever_flag,
             CASE WHEN SUM(CASE
                               WHEN k.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                                   AND DATEADD(day, -1, i.uti_date)
                                   THEN 1 ELSE 0 END) > 0
                      THEN 1 ELSE 0 END AS kidney_failure_1yr_flag,
             SUM(CASE
                     WHEN k.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                         AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS kidney_failure_1yr_count
         FROM index_uti i
                  LEFT JOIN kidney_failure_dx k
                            ON i.person_id = k.person_id
                                AND i.uti_date = k.uti_date
         GROUP BY i.person_id, i.uti_date
     ),

     diabetes_features AS (
         SELECT
             i.person_id,
             CASE WHEN COUNT(d.condition_start_date) > 0 THEN 1 ELSE 0 END AS diabetes_ever_flag,
             CASE WHEN SUM(CASE
                               WHEN d.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                                   AND DATEADD(day, -1, i.uti_date)
                                   THEN 1 ELSE 0 END) > 0
                      THEN 1 ELSE 0 END AS diabetes_1yr_flag,
             SUM(CASE
                     WHEN d.condition_start_date BETWEEN DATEADD(day, -365, i.uti_date)
                         AND DATEADD(day, -1, i.uti_date)
                         THEN 1 ELSE 0 END) AS diabetes_1yr_count
         FROM index_uti i
                  LEFT JOIN diabetes_dx d
                            ON i.person_id = d.person_id
                                AND i.uti_date = d.uti_date
         GROUP BY i.person_id, i.uti_date
     )

SELECT
    i.person_id,
    i.uti_date AS index_uti_date,

    d.age_at_index,
    d.gender_concept_id,

    CASE
        WHEN r.person_id IS NOT NULL THEN 1 ELSE 0
        END AS recurrent_uti_90d_flag,

    r.first_recurrent_uti_date,

    i.first_abx_0_10d_date,
    i.first_abx_0_10d_concept_id,
    COALESCE(asum.num_distinct_abx_0_10d, 0) AS num_distinct_abx_0_10d,

    CASE
        WHEN ra.person_id IS NOT NULL THEN 1 ELSE 0
        END AS repeat_abx_11_30d_flag,

    CASE
        WHEN e.person_id IS NOT NULL THEN 1 ELSE 0
        END AS er_30d_flag,

    COALESCE(pu.total_visits_1yr, 0) AS total_visits_1yr,
    COALESCE(pu.outpatient_visits_1yr, 0) AS outpatient_visits_1yr,
    COALESCE(pu.er_visits_1yr, 0) AS er_visits_1yr,
    COALESCE(pu.inpatient_visits_1yr, 0) AS inpatient_visits_1yr,

    COALESCE(ph.prior_uti_count_1yr, 0) AS prior_uti_count_1yr,
    COALESCE(pa.prior_abx_exposure_count_1yr, 0) AS prior_abx_exposure_count_1yr,
    COALESCE(pa.prior_distinct_abx_count_1yr, 0) AS prior_distinct_abx_count_1yr,

    COALESCE(cf.ckd_ever_flag, 0) AS ckd_ever_flag,
    COALESCE(cf.ckd_1yr_flag, 0) AS ckd_1yr_flag,
    COALESCE(cf.ckd_1yr_count, 0) AS ckd_1yr_count,

    COALESCE(hf.htn_ever_flag, 0) AS htn_ever_flag,
    COALESCE(hf.htn_1yr_flag, 0) AS htn_1yr_flag,
    COALESCE(hf.htn_1yr_count, 0) AS htn_1yr_count,

    COALESCE(kff.kidney_failure_ever_flag, 0) AS kidney_failure_ever_flag,
    COALESCE(kff.kidney_failure_1yr_flag, 0) AS kidney_failure_1yr_flag,
    COALESCE(kff.kidney_failure_1yr_count, 0) AS kidney_failure_1yr_count,

    COALESCE(df.diabetes_ever_flag, 0) AS diabetes_ever_flag,
    COALESCE(df.diabetes_1yr_flag, 0) AS diabetes_1yr_flag,
    COALESCE(df.diabetes_1yr_count, 0) AS diabetes_1yr_count

FROM index_uti i
         LEFT JOIN demographics d
                   ON i.person_id = d.person_id
         LEFT JOIN recurrent_uti r
                   ON i.person_id = r.person_id
         LEFT JOIN abx_0_10d_summary asum
                   ON i.person_id = asum.person_id
         LEFT JOIN repeat_abx_11_30d ra
                   ON i.person_id = ra.person_id
         LEFT JOIN er_30d e
                   ON i.person_id = e.person_id
         LEFT JOIN prior_utilization pu
                   ON i.person_id = pu.person_id
         LEFT JOIN prior_uti_history ph
                   ON i.person_id = ph.person_id
         LEFT JOIN prior_abx_history pa
                   ON i.person_id = pa.person_id
         LEFT JOIN ckd_features cf
                   ON i.person_id = cf.person_id
         LEFT JOIN htn_features hf
                   ON i.person_id = hf.person_id
         LEFT JOIN kidney_failure_features kff
                   ON i.person_id = kff.person_id
         LEFT JOIN diabetes_features df
                   ON i.person_id = df.person_id;