{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(patient_id), '') AS patient_id,
        INITCAP(TRIM(patient_name)) AS patient_name,
        INITCAP(TRIM(hospital_name)) AS hospital_name,
        TRY_CAST(record_date AS DATE) AS record_date,

        INITCAP(TRIM(device_type)) AS device_type,

        TRY_CAST(heart_rate AS DOUBLE) AS heart_rate,
        TRY_CAST(blood_pressure_sys AS DOUBLE) AS blood_pressure_sys,
        TRY_CAST(blood_pressure_dia AS DOUBLE) AS blood_pressure_dia,
        TRY_CAST(oxygen_level AS DOUBLE) AS oxygen_level,
        TRY_CAST(body_temp AS DOUBLE) AS body_temp,
        TRY_CAST(respiration_rate AS DOUBLE) AS respiration_rate,
        TRY_CAST(glucose_level AS DOUBLE) AS glucose_level,
        TRY_CAST(cholesterol AS DOUBLE) AS cholesterol,
        TRY_CAST(bmi AS DOUBLE) AS bmi,
        TRY_CAST(stress_index AS DOUBLE) AS stress_index,

        CURRENT_TIMESTAMP() AS _ingested_at,
        'patient_vitals.parquet' AS _source_file

    FROM {{ source('bronze', 'patient_vitals') }}

),

validated AS (

    SELECT
        patient_id,
        patient_name,
        hospital_name,
        record_date,
        device_type,

        CASE
            WHEN heart_rate BETWEEN 30 AND 220
            THEN heart_rate
            ELSE NULL
        END AS heart_rate,

        CASE
            WHEN blood_pressure_sys BETWEEN 70 AND 200
            THEN blood_pressure_sys
            ELSE NULL
        END AS blood_pressure_sys,

        CASE
            WHEN blood_pressure_dia BETWEEN 40 AND 130
            THEN blood_pressure_dia
            ELSE NULL
        END AS blood_pressure_dia,

        CASE
            WHEN oxygen_level BETWEEN 0 AND 100
            THEN oxygen_level
            ELSE NULL
        END AS oxygen_level,

        CASE
            WHEN body_temp BETWEEN 90 AND 110
            THEN body_temp
            ELSE NULL
        END AS body_temp,

        CASE
            WHEN respiration_rate BETWEEN 5 AND 60
            THEN respiration_rate
            ELSE NULL
        END AS respiration_rate,

        CASE
            WHEN glucose_level BETWEEN 40 AND 500
            THEN glucose_level
            ELSE NULL
        END AS glucose_level,

        CASE
            WHEN cholesterol BETWEEN 50 AND 500
            THEN cholesterol
            ELSE NULL
        END AS cholesterol,

        CASE
            WHEN bmi BETWEEN 10 AND 80
            THEN bmi
            ELSE NULL
        END AS bmi,

        CASE
            WHEN stress_index BETWEEN 0 AND 100
            THEN stress_index
            ELSE NULL
        END AS stress_index,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',

            CASE
                WHEN heart_rate IS NOT NULL
                     AND NOT heart_rate BETWEEN 30 AND 220
                THEN 'invalid_heart_rate_nulled'
            END,

            CASE
                WHEN blood_pressure_sys IS NOT NULL
                     AND NOT blood_pressure_sys BETWEEN 70 AND 200
                THEN 'invalid_blood_pressure_sys_nulled'
            END,

            CASE
                WHEN blood_pressure_dia IS NOT NULL
                     AND NOT blood_pressure_dia BETWEEN 40 AND 130
                THEN 'invalid_blood_pressure_dia_nulled'
            END,

            CASE
                WHEN oxygen_level IS NOT NULL
                     AND NOT oxygen_level BETWEEN 0 AND 100
                THEN 'invalid_oxygen_level_nulled'
            END,

            CASE
                WHEN body_temp IS NOT NULL
                     AND NOT body_temp BETWEEN 90 AND 110
                THEN 'invalid_body_temp_nulled'
            END,

            CASE
                WHEN respiration_rate IS NOT NULL
                     AND NOT respiration_rate BETWEEN 5 AND 60
                THEN 'invalid_respiration_rate_nulled'
            END,

            CASE
                WHEN glucose_level IS NOT NULL
                     AND NOT glucose_level BETWEEN 40 AND 500
                THEN 'invalid_glucose_level_nulled'
            END,

            CASE
                WHEN cholesterol IS NOT NULL
                     AND NOT cholesterol BETWEEN 50 AND 500
                THEN 'invalid_cholesterol_nulled'
            END,

            CASE
                WHEN bmi IS NOT NULL
                     AND NOT bmi BETWEEN 10 AND 80
                THEN 'invalid_bmi_nulled'
            END,

            CASE
                WHEN stress_index IS NOT NULL
                     AND NOT stress_index BETWEEN 0 AND 100
                THEN 'invalid_stress_index_nulled'
            END

        ) AS data_quality_flag,

        _ingested_at,
        _source_file

    FROM source_data

    WHERE patient_id IS NOT NULL
      AND hospital_name IS NOT NULL
      AND record_date IS NOT NULL

),

deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                patient_id,
                hospital_name,
                record_date
            ORDER BY _ingested_at DESC
        ) AS rn

    FROM validated

),

patient_lookup AS (

    SELECT DISTINCT
        patient_id

    FROM {{ ref('silver_patient_demographics') }}

),

hospital_lookup AS (

    SELECT
        hospital_name,
        MAX(hospital_id) AS hospital_id

    FROM {{ ref('silver_hospital_info') }}

    GROUP BY hospital_name

),

with_dimensions AS (

    SELECT
        v.*,
        p.patient_id AS resolved_patient_id,
        h.hospital_id AS resolved_hospital_id

    FROM deduped v

    LEFT JOIN patient_lookup p
        ON v.patient_id = p.patient_id

    LEFT JOIN hospital_lookup h
        ON v.hospital_name = h.hospital_name

)

SELECT
    patient_id,
    resolved_hospital_id AS hospital_id,

    patient_name,
    hospital_name,
    record_date,

    day,
    month,
    month_name,
    quarter,
    year,

    device_type,
    heart_rate,
    blood_pressure_sys,
    blood_pressure_dia,
    oxygen_level,
    body_temp,
    respiration_rate,
    glucose_level,
    cholesterol,
    bmi,
    stress_index,

    NULLIF(data_quality_flag, '') AS data_quality_flag,

    _ingested_at,
    _source_file

FROM with_dimensions

WHERE rn = 1
  AND resolved_patient_id IS NOT NULL
  AND resolved_hospital_id IS NOT NULL
