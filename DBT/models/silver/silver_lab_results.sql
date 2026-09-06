{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(patient_id), '') AS patient_id,
        INITCAP(TRIM(lab_test_name)) AS lab_test_name,
        INITCAP(TRIM(hospital_name)) AS hospital_name,
        INITCAP(TRIM(technician_name)) AS technician_name,
        TRY_CAST(record_date AS DATE) AS record_date,

        TRY_CAST(hemoglobin AS DOUBLE) AS hemoglobin,
        TRY_CAST(platelets AS DOUBLE) AS platelets,
        TRY_CAST(wbc_count AS DOUBLE) AS wbc_count,
        TRY_CAST(rbc_count AS DOUBLE) AS rbc_count,
        TRY_CAST(creatinine AS DOUBLE) AS creatinine,
        TRY_CAST(sodium AS DOUBLE) AS sodium,
        TRY_CAST(potassium AS DOUBLE) AS potassium,
        TRY_CAST(calcium AS DOUBLE) AS calcium,
        TRY_CAST(bilirubin AS DOUBLE) AS bilirubin,
        TRY_CAST(test_cost AS DOUBLE) AS test_cost,

        CURRENT_TIMESTAMP() AS _ingested_at,
        'lab_results.parquet' AS _source_file

    FROM {{ source('bronze', 'lab_results') }}

),

validated AS (

    SELECT
        patient_id,
        lab_test_name,
        hospital_name,
        technician_name,
        record_date,

        CASE
            WHEN hemoglobin BETWEEN 3 AND 25
            THEN hemoglobin
            ELSE NULL
        END AS hemoglobin,

        CASE
            WHEN platelets BETWEEN 10000 AND 1000000
            THEN platelets
            ELSE NULL
        END AS platelets,

        CASE
            WHEN wbc_count BETWEEN 0.5 AND 100
            THEN wbc_count
            ELSE NULL
        END AS wbc_count,

        CASE
            WHEN rbc_count BETWEEN 1 AND 8
            THEN rbc_count
            ELSE NULL
        END AS rbc_count,

        CASE
            WHEN creatinine BETWEEN 0.1 AND 20
            THEN creatinine
            ELSE NULL
        END AS creatinine,

        CASE
            WHEN sodium BETWEEN 100 AND 180
            THEN sodium
            ELSE NULL
        END AS sodium,

        CASE
            WHEN potassium BETWEEN 1.5 AND 8
            THEN potassium
            ELSE NULL
        END AS potassium,

        CASE
            WHEN calcium BETWEEN 5 AND 15
            THEN calcium
            ELSE NULL
        END AS calcium,

        CASE
            WHEN bilirubin BETWEEN 0 AND 30
            THEN bilirubin
            ELSE NULL
        END AS bilirubin,

        CASE
            WHEN test_cost >= 0
            THEN test_cost
            ELSE NULL
        END AS test_cost,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',
            CASE
                WHEN hemoglobin IS NOT NULL
                     AND NOT hemoglobin BETWEEN 3 AND 25
                THEN 'invalid_hemoglobin_nulled'
            END,

            CASE
                WHEN platelets IS NOT NULL
                     AND NOT platelets BETWEEN 10000 AND 1000000
                THEN 'invalid_platelets_nulled'
            END,

            CASE
                WHEN wbc_count IS NOT NULL
                     AND NOT wbc_count BETWEEN 0.5 AND 100
                THEN 'invalid_wbc_count_nulled'
            END,

            CASE
                WHEN rbc_count IS NOT NULL
                     AND NOT rbc_count BETWEEN 1 AND 8
                THEN 'invalid_rbc_count_nulled'
            END,

            CASE
                WHEN creatinine IS NOT NULL
                     AND NOT creatinine BETWEEN 0.1 AND 20
                THEN 'invalid_creatinine_nulled'
            END,

            CASE
                WHEN sodium IS NOT NULL
                     AND NOT sodium BETWEEN 100 AND 180
                THEN 'invalid_sodium_nulled'
            END,

            CASE
                WHEN potassium IS NOT NULL
                     AND NOT potassium BETWEEN 1.5 AND 8
                THEN 'invalid_potassium_nulled'
            END,

            CASE
                WHEN calcium IS NOT NULL
                     AND NOT calcium BETWEEN 5 AND 15
                THEN 'invalid_calcium_nulled'
            END,

            CASE
                WHEN bilirubin IS NOT NULL
                     AND NOT bilirubin BETWEEN 0 AND 30
                THEN 'invalid_bilirubin_nulled'
            END,

            CASE
                WHEN test_cost IS NOT NULL
                     AND test_cost < 0
                THEN 'invalid_test_cost_nulled'
            END
        ) AS data_quality_flag,

        _ingested_at,
        _source_file

    FROM source_data

    WHERE patient_id IS NOT NULL
      AND hospital_name IS NOT NULL
      AND lab_test_name IS NOT NULL
      AND record_date IS NOT NULL

),

deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                patient_id,
                hospital_name,
                record_date,
                lab_test_name
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
        l.*,
        p.patient_id AS resolved_patient_id,
        h.hospital_id AS resolved_hospital_id

    FROM deduped l

    LEFT JOIN patient_lookup p
        ON l.patient_id = p.patient_id

    LEFT JOIN hospital_lookup h
        ON l.hospital_name = h.hospital_name

)

SELECT
    patient_id,
    resolved_hospital_id AS hospital_id,

    lab_test_name,
    hospital_name,
    technician_name,
    record_date,

    day,
    month,
    month_name,
    quarter,
    year,

    hemoglobin,
    platelets,
    wbc_count,
    rbc_count,
    creatinine,
    sodium,
    potassium,
    calcium,
    bilirubin,
    test_cost,

    NULLIF(data_quality_flag, '') AS data_quality_flag,

    _ingested_at,
    _source_file

FROM with_dimensions

WHERE rn = 1
  AND resolved_patient_id IS NOT NULL
  AND resolved_hospital_id IS NOT NULL
