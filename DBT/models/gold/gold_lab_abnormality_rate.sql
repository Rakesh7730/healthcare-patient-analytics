{{ config(materialized='table', schema='gold') }}

WITH lab_flags AS (
    SELECT
        l.lab_test_name,
        l.patient_id,
        l.hospital_id,
        l.record_date,
        CASE
            WHEN l.data_quality_flag IS NOT NULL THEN 1
            ELSE 0
        END AS is_abnormal_lab
    FROM {{ ref('silver_lab_results') }} AS l
)

SELECT
    lab_test_name,
    COUNT(*) AS lab_observation_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    SUM(is_abnormal_lab) AS abnormal_lab_count,
    ROUND(
        100.0 * SUM(is_abnormal_lab) / NULLIF(COUNT(*), 0),
        2
    ) AS abnormal_lab_percentage
FROM lab_flags
GROUP BY lab_test_name
