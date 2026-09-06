{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(patient_id), '') AS patient_id,
        UPPER(TRIM(diagnosis_code)) AS diagnosis_code,
        INITCAP(TRIM(doctor_name)) AS doctor_name,
        INITCAP(TRIM(hospital_name)) AS hospital_name,
        TRY_CAST(record_date AS DATE) AS record_date,

        TRY_CAST(severity_score AS DOUBLE) AS raw_severity_score,
        TRY_CAST(risk_probability AS DOUBLE) AS raw_risk_probability,

        TRY_CAST(treatment_cost AS DOUBLE) AS treatment_cost,
        TRY_CAST(insurance_claim AS DOUBLE) AS insurance_claim,
        TRY_CAST(medication_count AS INT) AS medication_count,
        TRY_CAST(visit_duration AS DOUBLE) AS visit_duration,
        TRY_CAST(procedure_count AS INT) AS procedure_count,
        TRY_CAST(recovery_days AS INT) AS recovery_days,
        TRY_CAST(comorbidity_score AS DOUBLE) AS comorbidity_score,

        CASE
            WHEN UPPER(TRIM(readmission_risk)) IN ('LOW', 'L')
            THEN 'Low'

            WHEN UPPER(TRIM(readmission_risk))
                 IN ('MEDIUM', 'MED', 'MODERATE', 'M')
            THEN 'Medium'

            WHEN UPPER(TRIM(readmission_risk))
                 IN ('HIGH', 'CRITICAL', 'H')
            THEN 'High'

            WHEN TRY_CAST(readmission_risk AS DOUBLE) < 60
            THEN 'Low'

            WHEN TRY_CAST(readmission_risk AS DOUBLE)
                 BETWEEN 60 AND 120
            THEN 'Medium'

            WHEN TRY_CAST(readmission_risk AS DOUBLE) > 120
            THEN 'High'

            ELSE 'Unknown'
        END AS readmission_risk,

        CURRENT_TIMESTAMP() AS _ingested_at,
        'patient_diagnosis.parquet' AS _source_file

    FROM {{ source('bronze', 'patient_diagnosis') }}

),

validated AS (

    SELECT
        patient_id,
        diagnosis_code,
        doctor_name,
        hospital_name,
        record_date,

        CASE
            WHEN raw_severity_score BETWEEN 0 AND 1
            THEN raw_severity_score

            WHEN raw_severity_score > 1
                 AND raw_severity_score <= 100
            THEN raw_severity_score / 100

            ELSE NULL
        END AS severity_score,

        CASE
            WHEN raw_risk_probability BETWEEN 0 AND 1
            THEN raw_risk_probability

            WHEN raw_risk_probability > 1
                 AND raw_risk_probability <= 100
            THEN raw_risk_probability / 100

            ELSE NULL
        END AS risk_probability,

        CASE
            WHEN treatment_cost >= 0
            THEN treatment_cost
            ELSE NULL
        END AS treatment_cost,

        CASE
            WHEN insurance_claim >= 0
            THEN insurance_claim
            ELSE NULL
        END AS insurance_claim,

        CASE
            WHEN medication_count >= 0
            THEN medication_count
            ELSE NULL
        END AS medication_count,

        CASE
            WHEN visit_duration >= 0
            THEN visit_duration
            ELSE NULL
        END AS visit_duration,

        CASE
            WHEN procedure_count >= 0
            THEN procedure_count
            ELSE NULL
        END AS procedure_count,

        CASE
            WHEN recovery_days >= 0
            THEN recovery_days
            ELSE NULL
        END AS recovery_days,

        CASE
            WHEN comorbidity_score BETWEEN 0 AND 100
            THEN comorbidity_score
            ELSE NULL
        END AS comorbidity_score,

        readmission_risk,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',

            CASE
                WHEN raw_severity_score IS NOT NULL
                     AND NOT raw_severity_score BETWEEN 0 AND 100
                THEN 'invalid_severity_score_nulled'
            END,

            CASE
                WHEN raw_risk_probability IS NOT NULL
                     AND NOT raw_risk_probability BETWEEN 0 AND 100
                THEN 'invalid_risk_probability_nulled'
            END,

            CASE
                WHEN treatment_cost IS NOT NULL
                     AND treatment_cost < 0
                THEN 'invalid_treatment_cost_nulled'
            END,

            CASE
                WHEN insurance_claim IS NOT NULL
                     AND insurance_claim < 0
                THEN 'invalid_insurance_claim_nulled'
            END,

            CASE
                WHEN medication_count IS NOT NULL
                     AND medication_count < 0
                THEN 'invalid_medication_count_nulled'
            END,

            CASE
                WHEN visit_duration IS NOT NULL
                     AND visit_duration < 0
                THEN 'invalid_visit_duration_nulled'
            END,

            CASE
                WHEN procedure_count IS NOT NULL
                     AND procedure_count < 0
                THEN 'invalid_procedure_count_nulled'
            END,

            CASE
                WHEN recovery_days IS NOT NULL
                     AND recovery_days < 0
                THEN 'invalid_recovery_days_nulled'
            END,

            CASE
                WHEN comorbidity_score IS NOT NULL
                     AND NOT comorbidity_score BETWEEN 0 AND 100
                THEN 'invalid_comorbidity_score_nulled'
            END

        ) AS data_quality_flag,

        _ingested_at,
        _source_file

    FROM source_data

    WHERE patient_id IS NOT NULL
      AND hospital_name IS NOT NULL
      AND diagnosis_code IS NOT NULL
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
                diagnosis_code
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
        d.*,
        p.patient_id AS resolved_patient_id,
        h.hospital_id AS resolved_hospital_id

    FROM deduped d

    LEFT JOIN patient_lookup p
        ON d.patient_id = p.patient_id

    LEFT JOIN hospital_lookup h
        ON d.hospital_name = h.hospital_name

)

SELECT
    patient_id,
    resolved_hospital_id AS hospital_id,

    diagnosis_code,
    doctor_name,
    hospital_name,
    record_date,

    day,
    month,
    month_name,
    quarter,
    year,

    severity_score,
    risk_probability,
    readmission_risk,
    treatment_cost,
    insurance_claim,
    medication_count,
    visit_duration,
    procedure_count,
    recovery_days,
    comorbidity_score,

    NULLIF(data_quality_flag, '') AS data_quality_flag,

    _ingested_at,
    _source_file

FROM with_dimensions

WHERE rn = 1
  AND resolved_patient_id IS NOT NULL
  AND resolved_hospital_id IS NOT NULL
