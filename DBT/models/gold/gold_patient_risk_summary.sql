{{ config(materialized='table', schema='gold') }}

SELECT
    age_group,
    gender,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(AVG(risk_probability), 4) AS avg_risk_probability,
    ROUND(AVG(severity_score), 4) AS avg_severity_score,
    ROUND(AVG(patient_risk_score), 4) AS avg_patient_risk_score,
    ROUND(MAX(patient_risk_score), 4) AS max_patient_risk_score
FROM {{ ref('fact_health_metrics') }}
GROUP BY
    age_group,
    gender
