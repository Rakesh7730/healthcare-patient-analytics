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
        100.0 * patient_count / NULLIF(total_patients_by_hospital, 0),
        2
    ) AS patient_percentage
FROM with_totals
