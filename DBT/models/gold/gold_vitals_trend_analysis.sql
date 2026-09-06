{{ config(materialized='table', schema='gold') }}

SELECT
    age_group,
    year,
    quarter,
    month,
    month_name,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(AVG(avg_heart_rate), 4) AS avg_heart_rate,
    ROUND(AVG(avg_cholesterol), 4) AS avg_cholesterol,
    ROUND(AVG(avg_bmi), 4) AS avg_bmi,
    ROUND(AVG(avg_glucose_level), 4) AS avg_glucose_level,
    ROUND(AVG(avg_blood_pressure_sys), 4) AS avg_blood_pressure_sys,
    ROUND(AVG(avg_blood_pressure_dia), 4) AS avg_blood_pressure_dia,
    ROUND(AVG(avg_oxygen_level), 4) AS avg_oxygen_level,
    ROUND(AVG(patient_risk_score), 4) AS avg_patient_risk_score
FROM {{ ref('fact_health_metrics') }}
GROUP BY
    age_group,
    year,
    quarter,
    month,
    month_name
