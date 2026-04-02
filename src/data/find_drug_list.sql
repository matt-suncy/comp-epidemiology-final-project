WITH uti_concepts AS (
    SELECT descendant_concept_id AS concept_id
    FROM concept_ancestor
    WHERE ancestor_concept_id = 81902
),
     outpatient_uti AS (
         SELECT
             v.person_id,
             v.visit_start_date AS index_date
         FROM visit_occurrence v
                  JOIN condition_occurrence co
                       ON v.visit_occurrence_id = co.visit_occurrence_id
         WHERE (v.visit_concept_id = 9202 OR v.visit_concept_id = 9201)
           AND co.condition_concept_id IN (SELECT concept_id FROM uti_concepts)
     )
SELECT TOP 500
    de.drug_concept_id,
    c.concept_name,
    c.concept_class_id,
    c.vocabulary_id,
    COUNT(*) AS n_exposures,
    COUNT(DISTINCT de.person_id) AS n_people
FROM outpatient_uti ou
         JOIN drug_exposure de
              ON ou.person_id = de.person_id
                  AND de.drug_exposure_start_date BETWEEN DATEADD(day, -1, ou.index_date)
                     AND DATEADD(day,  7, ou.index_date)
         LEFT JOIN concept c
                   ON de.drug_concept_id = c.concept_id
GROUP BY
    de.drug_concept_id,
    c.concept_name,
    c.concept_class_id,
    c.vocabulary_id
ORDER BY n_people DESC;