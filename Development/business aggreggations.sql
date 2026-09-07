-- ============================================================
-- FILE 1: gold_cost_analysis.sql
-- ============================================================

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
    diagnosis_code;


-- ============================================================
-- FILE 2: gold_hospital_performance_scorecard.sql
-- ============================================================

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
    hospital_state;


-- ============================================================
-- FILE 3: gold_lab_abnormality_rate.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH lab_flags AS (

    SELECT
        l.lab_test_name,
        l.patient_id,
        l.hospital_id,
        l.record_date,

        CASE
            WHEN l.data_quality_flag IS NOT NULL
            THEN 1
            ELSE 0
        END AS is_abnormal_lab

    FROM {{ ref('silver_lab_results') }} l
)

SELECT
    lab_test_name,

    COUNT(*) AS lab_observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,

    SUM(is_abnormal_lab) AS abnormal_lab_count,

    ROUND(
        (SUM(is_abnormal_lab) / COUNT(*)) * 100,
        2
    ) AS abnormal_lab_percentage

FROM lab_flags

GROUP BY
    lab_test_name;


-- ============================================================
-- FILE 4: gold_lifestyle_health_correlation.sql
-- ============================================================

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
    gender;


-- ============================================================
-- FILE 5: gold_monthly_quarterly_trend.sql
-- ============================================================

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
    month_name;


-- ============================================================
-- FILE 6: gold_patient_risk_summary.sql
-- ============================================================

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
    gender;


-- ============================================================
-- FILE 7: gold_readmission_risk_distribution.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH grouped AS (

    SELECT
        hospital_id,
        hospital_name,
        readmission_risk,

        COUNT(*) AS observation_count,
        COUNT(DISTINCT patient_id) AS patient_count

    FROM {{ ref('fact_health_metrics') }}

    GROUP BY
        hospital_id,
        hospital_name,
        readmission_risk
),

with_totals AS (

    SELECT
        *,

        SUM(patient_count) OVER (
            PARTITION BY hospital_id, hospital_name
        ) AS total_patients_by_hospital

    FROM grouped
)

SELECT
    hospital_id,
    hospital_name,
    readmission_risk,

    observation_count,
    patient_count,

    total_patients_by_hospital,

    ROUND(
        (patient_count / total_patients_by_hospital) * 100,
        2
    ) AS patient_percentage

FROM with_totals;


-- ============================================================
-- FILE 8: gold_vitals_trend_analysis.sql
-- ============================================================

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

    ROUND(
        AVG(avg_blood_pressure_sys),
        4
    ) AS avg_blood_pressure_sys,

    ROUND(
        AVG(avg_blood_pressure_dia),
        4
    ) AS avg_blood_pressure_dia,

    ROUND(
        AVG(avg_oxygen_level),
        4
    ) AS avg_oxygen_level,

    ROUND(
        AVG(patient_risk_score),
        4
    ) AS avg_patient_risk_score

FROM {{ ref('fact_health_metrics') }}

GROUP BY
    age_group,
    year,
    quarter,
    month,
    month_name;
