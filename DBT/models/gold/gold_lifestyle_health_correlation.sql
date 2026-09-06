{{ config(materialized='table', schema='gold') }}

SELECT
    age_group,
    gender,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(AVG(smoking_index), 4) AS avg_smoking_index,
    ROUND(AVG(alcohol_index), 4) AS avg_alcohol_index,
    ROUND(AVG(exercise_hours), 4) AS avg_exercise_hours,
    ROUND(AVG(sleep_hours), 4) AS avg_sleep_hours,
    ROUND(AVG(diet_score), 4) AS avg_diet_score,
    ROUND(AVG(lifestyle_risk), 4) AS avg_lifestyle_risk,
    ROUND(AVG(health_score), 4) AS avg_health_score,
    ROUND(AVG(risk_probability), 4) AS avg_risk_probability,
    ROUND(AVG(patient_risk_score), 4) AS avg_patient_risk_score
FROM {{ ref('fact_health_metrics') }}
GROUP BY
    age_group,
    gender
