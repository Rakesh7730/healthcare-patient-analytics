{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(hospital_id), '') AS hospital_id,
        INITCAP(TRIM(hospital_name)) AS hospital_name,
        INITCAP(TRIM(city)) AS city,
        INITCAP(TRIM(state)) AS state,
        TRY_CAST(record_date AS DATE) AS record_date,

        TRY_CAST(bed_capacity AS INT) AS bed_capacity,
        TRY_CAST(icu_beds AS INT) AS icu_beds,
        TRY_CAST(staff_count AS INT) AS staff_count,
        TRY_CAST(infection_rate AS DOUBLE) AS infection_rate,
        TRY_CAST(utilization_rate AS DOUBLE) AS utilization_rate,
        TRY_CAST(avg_wait_time AS DOUBLE) AS avg_wait_time,
        TRY_CAST(equipment_score AS DOUBLE) AS equipment_score,
        TRY_CAST(patient_load AS DOUBLE) AS patient_load,
        TRY_CAST(surgery_count AS INT) AS surgery_count,
        TRY_CAST(emergency_cases AS INT) AS emergency_cases,

        CURRENT_TIMESTAMP() AS _ingested_at,
        'hospital_info.parquet' AS _source_file

    FROM {{ source('bronze', 'hospital_info') }}

),

validated AS (

    SELECT
        hospital_id,
        hospital_name,
        city,
        state,
        record_date,

        CASE
            WHEN bed_capacity >= 0
            THEN bed_capacity
            ELSE NULL
        END AS bed_capacity,

        CASE
            WHEN icu_beds >= 0
                 AND icu_beds <= bed_capacity
            THEN icu_beds
            ELSE NULL
        END AS icu_beds,

        CASE
            WHEN staff_count >= 0
            THEN staff_count
            ELSE NULL
        END AS staff_count,

        CASE
            WHEN infection_rate BETWEEN 0 AND 100
            THEN infection_rate
            ELSE NULL
        END AS infection_rate,

        CASE
            WHEN utilization_rate BETWEEN 0 AND 100
            THEN utilization_rate
            ELSE NULL
        END AS utilization_rate,

        CASE
            WHEN avg_wait_time >= 0
            THEN avg_wait_time
            ELSE NULL
        END AS avg_wait_time,

        CASE
            WHEN equipment_score BETWEEN 0 AND 100
            THEN equipment_score
            ELSE NULL
        END AS equipment_score,

        CASE
            WHEN patient_load >= 0
            THEN patient_load
            ELSE NULL
        END AS patient_load,

        CASE
            WHEN surgery_count >= 0
            THEN surgery_count
            ELSE NULL
        END AS surgery_count,

        CASE
            WHEN emergency_cases >= 0
            THEN emergency_cases
            ELSE NULL
        END AS emergency_cases,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',
            CASE
                WHEN bed_capacity IS NOT NULL
                     AND bed_capacity < 0
                THEN 'invalid_bed_capacity_nulled'
            END,

            CASE
                WHEN icu_beds IS NOT NULL
                     AND (
                         icu_beds < 0
                         OR icu_beds > bed_capacity
                     )
                THEN 'invalid_icu_beds_nulled'
            END,

            CASE
                WHEN staff_count IS NOT NULL
                     AND staff_count < 0
                THEN 'invalid_staff_count_nulled'
            END,

            CASE
                WHEN infection_rate IS NOT NULL
                     AND NOT infection_rate BETWEEN 0 AND 100
                THEN 'invalid_infection_rate_nulled'
            END,

            CASE
                WHEN utilization_rate IS NOT NULL
                     AND NOT utilization_rate BETWEEN 0 AND 100
                THEN 'invalid_utilization_rate_nulled'
            END,

            CASE
                WHEN avg_wait_time IS NOT NULL
                     AND avg_wait_time < 0
                THEN 'invalid_avg_wait_time_nulled'
            END,

            CASE
                WHEN equipment_score IS NOT NULL
                     AND NOT equipment_score BETWEEN 0 AND 100
                THEN 'invalid_equipment_score_nulled'
            END,

            CASE
                WHEN patient_load IS NOT NULL
                     AND patient_load < 0
                THEN 'invalid_patient_load_nulled'
            END,

            CASE
                WHEN surgery_count IS NOT NULL
                     AND surgery_count < 0
                THEN 'invalid_surgery_count_nulled'
            END,

            CASE
                WHEN emergency_cases IS NOT NULL
                     AND emergency_cases < 0
                THEN 'invalid_emergency_cases_nulled'
            END
        ) AS data_quality_flag,

        _ingested_at,
        _source_file

    FROM source_data

    WHERE hospital_id IS NOT NULL
      AND hospital_name IS NOT NULL
      AND record_date IS NOT NULL

),

deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY hospital_id, record_date
            ORDER BY _ingested_at DESC
        ) AS rn

    FROM validated

)

SELECT
    hospital_id,
    hospital_name,
    city,
    state,
    record_date,

    day,
    month,
    month_name,
    quarter,
    year,

    bed_capacity,
    icu_beds,
    staff_count,
    infection_rate,
    utilization_rate,
    avg_wait_time,
    equipment_score,
    patient_load,
    surgery_count,
    emergency_cases,

    NULLIF(data_quality_flag, '') AS data_quality_flag,

    _ingested_at,
    _source_file

FROM deduped

WHERE rn = 1
