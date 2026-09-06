{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(patient_id), '') AS patient_id,
        INITCAP(TRIM(patient_name)) AS patient_name,

        CASE
            WHEN UPPER(TRIM(gender)) IN ('M', 'MALE')
            THEN 'M'

            WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE')
            THEN 'F'

            ELSE 'Unknown'
        END AS gender,

        INITCAP(TRIM(city)) AS city,
        TRY_CAST(record_date AS DATE) AS record_date,

        CAST(
            ROUND(TRY_CAST(age AS DOUBLE))
            AS INT
        ) AS age,

        TRY_CAST(income_index AS DOUBLE) AS income_index,
        TRY_CAST(health_score AS DOUBLE) AS health_score,
        TRY_CAST(lifestyle_risk AS DOUBLE) AS lifestyle_risk,
        TRY_CAST(exercise_hours AS DOUBLE) AS exercise_hours,
        TRY_CAST(sleep_hours AS DOUBLE) AS sleep_hours,
        TRY_CAST(alcohol_index AS DOUBLE) AS alcohol_index,
        TRY_CAST(smoking_index AS DOUBLE) AS smoking_index,
        TRY_CAST(diet_score AS DOUBLE) AS diet_score,
        TRY_CAST(insurance_score AS DOUBLE) AS insurance_score,

        CURRENT_TIMESTAMP() AS _ingested_at,
        'patient_demographics.parquet' AS _source_file

    FROM {{ source('bronze', 'patient_demographics') }}

),

validated AS (

    SELECT
        patient_id,
        patient_name,
        gender,
        city,
        record_date,

        CASE
            WHEN age BETWEEN 0 AND 120
            THEN age
            ELSE NULL
        END AS age,

        CASE
            WHEN income_index BETWEEN 0 AND 1
            THEN income_index
            ELSE NULL
        END AS income_index,

        CASE
            WHEN health_score BETWEEN 0 AND 100
            THEN health_score
            ELSE NULL
        END AS health_score,

        CASE
            WHEN lifestyle_risk BETWEEN 0 AND 100
            THEN lifestyle_risk
            ELSE NULL
        END AS lifestyle_risk,

        CASE
            WHEN exercise_hours BETWEEN 0 AND 24
            THEN exercise_hours
            ELSE NULL
        END AS exercise_hours,

        CASE
            WHEN sleep_hours BETWEEN 0 AND 24
            THEN sleep_hours
            ELSE NULL
        END AS sleep_hours,

        CASE
            WHEN alcohol_index BETWEEN 0 AND 1
            THEN alcohol_index
            ELSE NULL
        END AS alcohol_index,

        CASE
            WHEN smoking_index BETWEEN 0 AND 1
            THEN smoking_index
            ELSE NULL
        END AS smoking_index,

        CASE
            WHEN diet_score BETWEEN 0 AND 100
            THEN diet_score
            ELSE NULL
        END AS diet_score,

        CASE
            WHEN insurance_score BETWEEN 0 AND 100
            THEN insurance_score
            ELSE NULL
        END AS insurance_score,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',

            CASE
                WHEN age IS NOT NULL
                     AND NOT age BETWEEN 0 AND 120
                THEN 'invalid_age_nulled'
            END,

            CASE
                WHEN income_index IS NOT NULL
                     AND NOT income_index BETWEEN 0 AND 1
                THEN 'invalid_income_index_nulled'
            END,

            CASE
                WHEN health_score IS NOT NULL
                     AND NOT health_score BETWEEN 0 AND 100
                THEN 'invalid_health_score_nulled'
            END,

            CASE
                WHEN lifestyle_risk IS NOT NULL
                     AND NOT lifestyle_risk BETWEEN 0 AND 100
                THEN 'invalid_lifestyle_risk_nulled'
            END,

            CASE
                WHEN exercise_hours IS NOT NULL
                     AND NOT exercise_hours BETWEEN 0 AND 24
                THEN 'invalid_exercise_hours_nulled'
            END,

            CASE
                WHEN sleep_hours IS NOT NULL
                     AND NOT sleep_hours BETWEEN 0 AND 24
                THEN 'invalid_sleep_hours_nulled'
            END,

            CASE
                WHEN alcohol_index IS NOT NULL
                     AND NOT alcohol_index BETWEEN 0 AND 1
                THEN 'invalid_alcohol_index_nulled'
            END,

            CASE
                WHEN smoking_index IS NOT NULL
                     AND NOT smoking_index BETWEEN 0 AND 1
                THEN 'invalid_smoking_index_nulled'
            END,

            CASE
                WHEN diet_score IS NOT NULL
                     AND NOT diet_score BETWEEN 0 AND 100
                THEN 'invalid_diet_score_nulled'
            END,

            CASE
                WHEN insurance_score IS NOT NULL
                     AND NOT insurance_score BETWEEN 0 AND 100
                THEN 'invalid_insurance_score_nulled'
            END

        ) AS data_quality_flag,

        _ingested_at,
        _source_file

    FROM source_data

    WHERE patient_id IS NOT NULL
      AND record_date IS NOT NULL

),

deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, record_date
            ORDER BY _ingested_at DESC
        ) AS rn

    FROM validated

)

SELECT
    patient_id,
    patient_name,
    gender,
    city,
    record_date,

    day,
    month,
    month_name,
    quarter,
    year,

    age,
    income_index,
    health_score,
    lifestyle_risk,
    exercise_hours,
    sleep_hours,
    alcohol_index,
    smoking_index,
    diet_score,
    insurance_score,

    NULLIF(data_quality_flag, '') AS data_quality_flag,

    _ingested_at,
    _source_file

FROM deduped

WHERE rn = 1
