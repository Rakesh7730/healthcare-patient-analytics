{{ config(materialized='table', schema='gold') }}

SELECT
    hospital_id,
    hospital_name,
    hospital_city,
    hospital_state,
    diagnosis_code,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(SUM(treatment_cost), 2) AS total_treatment_cost,
    ROUND(SUM(total_test_cost), 2) AS total_test_cost,
    ROUND(SUM(insurance_claim), 2) AS total_insurance_claim,
    ROUND(AVG(treatment_cost), 2) AS avg_treatment_cost,
    ROUND(AVG(total_test_cost), 2) AS avg_test_cost,
    ROUND(AVG(insurance_claim), 2) AS avg_insurance_claim,
    ROUND(AVG(patient_risk_score), 4) AS avg_patient_risk_score
FROM {{ ref('fact_health_metrics') }}
GROUP BY
    hospital_id,
    hospital_name,
    hospital_city,
    hospital_state,
    diagnosis_code
