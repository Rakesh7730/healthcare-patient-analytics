{{ config(materialized='table', schema='gold') }}

SELECT
    year,
    quarter,
    month,
    month_name,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(AVG(risk_probability), 4) AS avg_risk_probability,
    ROUND(AVG(severity_score), 4) AS avg_severity_score,
    ROUND(AVG(patient_risk_score), 4) AS avg_patient_risk_score,
    ROUND(SUM(treatment_cost), 2) AS total_treatment_cost,
    ROUND(SUM(total_test_cost), 2) AS total_test_cost,
    ROUND(SUM(insurance_claim), 2) AS total_insurance_claim
FROM {{ ref('fact_health_metrics') }}
GROUP BY
    year,
    quarter,
    month,
    month_name
