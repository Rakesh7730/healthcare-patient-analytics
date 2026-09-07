-- ============================================================
-- HEALTHCARE PATIENT ANALYTICS PIPELINE
-- SILVER LAYER — DBT MODELS
-- Azure Databricks + Unity Catalog
-- ============================================================


-- ============================================================
-- 1. silver_hospital_info.sql
-- ============================================================

{{ config(materialized='table', schema='silver') }}

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
            WHEN bed_capacity >= 0 THEN bed_capacity
            ELSE NULL
        END AS bed_capacity,

        CASE
            WHEN icu_beds >= 0
             AND icu_beds <= bed_capacity
            THEN icu_beds
            ELSE NULL
        END AS icu_beds,

        CASE
            WHEN staff_count >= 0 THEN staff_count
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
            WHEN avg_wait_time >= 0 THEN avg_wait_time
            ELSE NULL
        END AS avg_wait_time,

        CASE
            WHEN equipment_score BETWEEN 0 AND 100
            THEN equipment_score
            ELSE NULL
        END AS equipment_score,

        CASE
            WHEN patient_load >= 0 THEN patient_load
            ELSE NULL
        END AS patient_load,

        CASE
            WHEN surgery_count >= 0 THEN surgery_count
            ELSE NULL
        END AS surgery_count,

        CASE
            WHEN emergency_cases >= 0 THEN emergency_cases
            ELSE NULL
        END AS emergency_cases,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',
            CASE
                WHEN bed_capacity IS NOT NULL AND bed_capacity < 0
                THEN 'invalid_bed_capacity_nulled'
            END,

            CASE
                WHEN icu_beds IS NOT NULL
                 AND (icu_beds < 0 OR icu_beds > bed_capacity)
                THEN 'invalid_icu_beds_nulled'
            END,

            CASE
                WHEN staff_count IS NOT NULL AND staff_count < 0
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
                WHEN avg_wait_time IS NOT NULL AND avg_wait_time < 0
                THEN 'invalid_avg_wait_time_nulled'
            END,

            CASE
                WHEN equipment_score IS NOT NULL
                 AND NOT equipment_score BETWEEN 0 AND 100
                THEN 'invalid_equipment_score_nulled'
            END,

            CASE
                WHEN patient_load IS NOT NULL AND patient_load < 0
                THEN 'invalid_patient_load_nulled'
            END,

            CASE
                WHEN surgery_count IS NOT NULL AND surgery_count < 0
                THEN 'invalid_surgery_count_nulled'
            END,

            CASE
                WHEN emergency_cases IS NOT NULL AND emergency_cases < 0
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

WHERE rn = 1;


-- ============================================================
-- 2. silver_patient_demographics.sql
-- ============================================================

{{ config(materialized='table', schema='silver') }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(patient_id), '') AS patient_id,
        INITCAP(TRIM(patient_name)) AS patient_name,

        CASE
            WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'M'
            WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'F'
            ELSE 'Unknown'
        END AS gender,

        INITCAP(TRIM(city)) AS city,
        TRY_CAST(record_date AS DATE) AS record_date,

        CAST(ROUND(TRY_CAST(age AS DOUBLE)) AS INT) AS age,
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

        CASE WHEN age BETWEEN 0 AND 120
            THEN age ELSE NULL END AS age,

        CASE WHEN income_index BETWEEN 0 AND 1
            THEN income_index ELSE NULL END AS income_index,

        CASE WHEN health_score BETWEEN 0 AND 100
            THEN health_score ELSE NULL END AS health_score,

        CASE WHEN lifestyle_risk BETWEEN 0 AND 100
            THEN lifestyle_risk ELSE NULL END AS lifestyle_risk,

        CASE WHEN exercise_hours BETWEEN 0 AND 24
            THEN exercise_hours ELSE NULL END AS exercise_hours,

        CASE WHEN sleep_hours BETWEEN 0 AND 24
            THEN sleep_hours ELSE NULL END AS sleep_hours,

        CASE WHEN alcohol_index BETWEEN 0 AND 1
            THEN alcohol_index ELSE NULL END AS alcohol_index,

        CASE WHEN smoking_index BETWEEN 0 AND 1
            THEN smoking_index ELSE NULL END AS smoking_index,

        CASE WHEN diet_score BETWEEN 0 AND 100
            THEN diet_score ELSE NULL END AS diet_score,

        CASE WHEN insurance_score BETWEEN 0 AND 100
            THEN insurance_score ELSE NULL END AS insurance_score,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',
            CASE WHEN age IS NOT NULL AND NOT age BETWEEN 0 AND 120
                THEN 'invalid_age_nulled' END,

            CASE WHEN income_index IS NOT NULL
                      AND NOT income_index BETWEEN 0 AND 1
                THEN 'invalid_income_index_nulled' END,

            CASE WHEN health_score IS NOT NULL
                      AND NOT health_score BETWEEN 0 AND 100
                THEN 'invalid_health_score_nulled' END,

            CASE WHEN lifestyle_risk IS NOT NULL
                      AND NOT lifestyle_risk BETWEEN 0 AND 100
                THEN 'invalid_lifestyle_risk_nulled' END,

            CASE WHEN exercise_hours IS NOT NULL
                      AND NOT exercise_hours BETWEEN 0 AND 24
                THEN 'invalid_exercise_hours_nulled' END,

            CASE WHEN sleep_hours IS NOT NULL
                      AND NOT sleep_hours BETWEEN 0 AND 24
                THEN 'invalid_sleep_hours_nulled' END,

            CASE WHEN alcohol_index IS NOT NULL
                      AND NOT alcohol_index BETWEEN 0 AND 1
                THEN 'invalid_alcohol_index_nulled' END,

            CASE WHEN smoking_index IS NOT NULL
                      AND NOT smoking_index BETWEEN 0 AND 1
                THEN 'invalid_smoking_index_nulled' END,

            CASE WHEN diet_score IS NOT NULL
                      AND NOT diet_score BETWEEN 0 AND 100
                THEN 'invalid_diet_score_nulled' END,

            CASE WHEN insurance_score IS NOT NULL
                      AND NOT insurance_score BETWEEN 0 AND 100
                THEN 'invalid_insurance_score_nulled' END
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

WHERE rn = 1;


-- ============================================================
-- 3. silver_lab_results.sql
-- ============================================================

{{ config(materialized='table', schema='silver') }}

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

        CASE WHEN hemoglobin BETWEEN 3 AND 25
            THEN hemoglobin ELSE NULL END AS hemoglobin,

        CASE WHEN platelets BETWEEN 10000 AND 1000000
            THEN platelets ELSE NULL END AS platelets,

        CASE WHEN wbc_count BETWEEN 0.5 AND 100
            THEN wbc_count ELSE NULL END AS wbc_count,

        CASE WHEN rbc_count BETWEEN 1 AND 8
            THEN rbc_count ELSE NULL END AS rbc_count,

        CASE WHEN creatinine BETWEEN 0.1 AND 20
            THEN creatinine ELSE NULL END AS creatinine,

        CASE WHEN sodium BETWEEN 100 AND 180
            THEN sodium ELSE NULL END AS sodium,

        CASE WHEN potassium BETWEEN 1.5 AND 8
            THEN potassium ELSE NULL END AS potassium,

        CASE WHEN calcium BETWEEN 5 AND 15
            THEN calcium ELSE NULL END AS calcium,

        CASE WHEN bilirubin BETWEEN 0 AND 30
            THEN bilirubin ELSE NULL END AS bilirubin,

        CASE WHEN test_cost >= 0
            THEN test_cost ELSE NULL END AS test_cost,

        DAY(record_date) AS day,
        MONTH(record_date) AS month,
        DATE_FORMAT(record_date, 'MMMM') AS month_name,
        QUARTER(record_date) AS quarter,
        YEAR(record_date) AS year,

        CONCAT_WS(', ',
            CASE WHEN hemoglobin IS NOT NULL
                      AND NOT hemoglobin BETWEEN 3 AND 25
                THEN 'invalid_hemoglobin_nulled' END,

            CASE WHEN platelets IS NOT NULL
                      AND NOT platelets BETWEEN 10000 AND 1000000
                THEN 'invalid_platelets_nulled' END,

            CASE WHEN wbc_count IS NOT NULL
                      AND NOT wbc_count BETWEEN 0.5 AND 100
                THEN 'invalid_wbc_count_nulled' END,

            CASE WHEN rbc_count IS NOT NULL
                      AND NOT rbc_count BETWEEN 1 AND 8
                THEN 'invalid_rbc_count_nulled' END,

            CASE WHEN creatinine IS NOT NULL
                      AND NOT creatinine BETWEEN 0.1 AND 20
                THEN 'invalid_creatinine_nulled' END,

            CASE WHEN sodium IS NOT NULL
                      AND NOT sodium BETWEEN 100 AND 180
                THEN 'invalid_sodium_nulled' END,

            CASE WHEN potassium IS NOT NULL
                      AND NOT potassium BETWEEN 1.5 AND 8
                THEN 'invalid_potassium_nulled' END,

            CASE WHEN calcium IS NOT NULL
                      AND NOT calcium BETWEEN 5 AND 15
                THEN 'invalid_calcium_nulled' END,

            CASE WHEN bilirubin IS NOT NULL
                      AND NOT bilirubin BETWEEN 0 AND 30
                THEN 'invalid_bilirubin_nulled' END,

            CASE WHEN test_cost IS NOT NULL AND test_cost < 0
                THEN 'invalid_test_cost_nulled' END
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
            PARTITION BY patient_id,
                         hospital_name,
                         record_date,
                         lab_test_name
            ORDER BY _ingested_at DESC
        ) AS rn

    FROM validated

),

patient_lookup AS (

    SELECT patient_id
    FROM {{ ref('silver_patient_demographics') }}
    GROUP BY patient_id

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

    NULLIF(
        CONCAT_WS(', ',
            data_quality_flag,

            CASE
                WHEN resolved_patient_id IS NULL
                THEN 'orphan_patient_reference'
            END,

            CASE
                WHEN resolved_hospital_id IS NULL
                THEN 'orphan_hospital_reference'
            END
        ),
        ''
    ) AS data_quality_flag,

    _ingested_at,
    _source_file

FROM with_dimensions

WHERE rn = 1
  AND resolved_patient_id IS NOT NULL
  AND resolved_hospital_id IS NOT NULL;


-- ============================================================
-- 4. silver_patient_diagnosis.sql
-- ============================================================

{{ config(materialized='table', schema='silver') }}

WITH source_data AS (

    SELECT
        NULLIF(TRIM(patient_id), '') AS patient_id,
        UPPER(TRIM(diagnosis_code)) AS diagnosis_code,
        INITCAP(TRIM(doctor_name)) AS doctor_name,
        INITCAP(TRIM(hospital_name)) AS hospital_name,
        TRY_CAST(record_date AS DATE) AS record_date,

        TRY_CAST(severity_score AS DOUBLE) AS severity_score,
        TRY_CAST(risk_probability AS DOUBLE) AS risk_probability,
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
            WHEN severity_score BETWEEN 0 AND 1
                THEN severity_score
            WHEN severity_score > 1
             AND severity_score <= 100
                THEN severity_score / 100
            ELSE NULL
        END AS severity_score,

        CASE
            WHEN risk_probability BETWEEN 0 AND 1
                THEN risk_probability
            WHEN risk_probability > 1
             AND risk_probability <= 100
                THEN risk_probability / 100
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
                WHEN severity_score IS NOT NULL
                 AND NOT severity_score BETWEEN 0 AND 100
                THEN 'invalid_severity_score_nulled'
            END,

            CASE
                WHEN risk_probability IS NOT NULL
                 AND NOT risk_probability BETWEEN 0 AND 100
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
            PARTITION BY patient_id,
                         hospital_name,
                         record_date,
                         diagnosis_code
            ORDER BY _ingested_at DESC
        ) AS rn

    FROM validated

),

patient_lookup AS (

    SELECT patient_id
    FROM {{ ref('silver_patient_demographics') }}
    GROUP BY patient_id

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

    NULLIF(
        CONCAT_WS(', ',
            data_quality_flag,

            CASE
                WHEN resolved_patient_id IS NULL
                THEN 'orphan_patient_reference'
            END,

            CASE
                WHEN resolved_hospital_id IS NULL
                THEN 'orphan_hospital_reference'
            END
        ),
        ''
    ) AS data_quality_flag,

    _ingested_at,
    _source_file

FROM with_dimensions

WHERE rn = 1
  AND resolved_patient_id IS NOT NULL
  AND resolved_hospital_id IS NOT NULL;


-- ============================================================
-- 5. silver_patient_vitals.sql
-- ============================================================

{{ config(materialized='table', schema='silver') }}

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
            PARTITION BY patient_id,
                         hospital_name,
                         record_date
            ORDER BY _ingested_at DESC
        ) AS rn

    FROM validated

),

patient_lookup AS (

    SELECT patient_id
    FROM {{ ref('silver_patient_demographics') }}
    GROUP BY patient_id

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

    NULLIF(
        CONCAT_WS(', ',
            data_quality_flag,

            CASE
                WHEN resolved_patient_id IS NULL
                THEN 'orphan_patient_reference'
            END,

            CASE
                WHEN resolved_hospital_id IS NULL
                THEN 'orphan_hospital_reference'
            END
        ),
        ''
    ) AS data_quality_flag,

    _ingested_at,
    _source_file

FROM with_dimensions

WHERE rn = 1
  AND resolved_patient_id IS NOT NULL
  AND resolved_hospital_id IS NOT NULL;
