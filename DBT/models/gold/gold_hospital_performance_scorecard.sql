{{ config(materialized='table', schema='gold') }}

SELECT
    hospital_id,
    hospital_name,
    hospital_city,
    hospital_state,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(AVG(infection_rate), 4) AS avg_infection_rate,
    ROUND(AVG(utilization_rate), 4) AS avg_utilization_rate,
    ROUND(AVG(avg_wait_time), 4) AS avg_wait_time,
    ROUND(AVG(equipment_score), 4) AS avg_equipment_score,
    ROUND(AVG(patient_load), 4) AS avg_patient_load,
    SUM(surgery_count) AS total_surgery_count,
    SUM(emergency_cases) AS total_emergency_cases,
    ROUND(AVG(patient_risk_score), 4) AS avg_patient_risk_score
FROM {{ ref('fact_health_metrics') }}
GROUP BY
    hospital_id,
    hospital_name,
    hospital_city,
    hospital_state
