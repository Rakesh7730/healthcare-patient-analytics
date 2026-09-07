-- ============================================================
-- FILE 1: dim_date.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH all_dates AS (
    SELECT record_date FROM {{ ref('silver_patient_demographics') }}
    UNION
    SELECT record_date FROM {{ ref('silver_hospital_info') }}
    UNION
    SELECT record_date FROM {{ ref('silver_patient_diagnosis') }}
    UNION
    SELECT record_date FROM {{ ref('silver_lab_results') }}
    UNION
    SELECT record_date FROM {{ ref('silver_patient_vitals') }}
),

deduped AS (
    SELECT DISTINCT record_date
    FROM all_dates
    WHERE record_date IS NOT NULL
)

SELECT
    CAST(DATE_FORMAT(record_date, 'yyyyMMdd') AS INT) AS date_key,
    record_date,
    DAY(record_date) AS day,
    MONTH(record_date) AS month,
    DATE_FORMAT(record_date, 'MMMM') AS month_name,
    QUARTER(record_date) AS quarter,
    YEAR(record_date) AS year,
    DATE_FORMAT(record_date, 'E') AS day_name,

    CASE
        WHEN DATE_FORMAT(record_date, 'E') IN ('Sat', 'Sun')
        THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM deduped;


-- ============================================================
-- FILE 2: dim_hospital.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH ranked_hospitals AS (

    SELECT
        hospital_id,
        hospital_name,
        city,
        state,
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
        record_date,
        _ingested_at,
        _source_file,

        ROW_NUMBER() OVER (
            PARTITION BY hospital_id
            ORDER BY record_date DESC, _ingested_at DESC
        ) AS rn

    FROM {{ ref('silver_hospital_info') }}
)

SELECT
    SHA2(CAST(hospital_id AS STRING), 256) AS hospital_key,

    hospital_id,
    hospital_name,
    city,
    state,

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

    record_date AS latest_record_date,

    _ingested_at,
    _source_file

FROM ranked_hospitals

WHERE rn = 1;


-- ============================================================
-- FILE 3: dim_observations.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH diagnosis_observations AS (

    SELECT DISTINCT

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        diagnosis_code,
        doctor_name,

        CAST(NULL AS STRING) AS lab_test_name,
        CAST(NULL AS STRING) AS technician_name,
        CAST(NULL AS STRING) AS device_type,

        'DIAGNOSIS' AS observation_type

    FROM {{ ref('silver_patient_diagnosis') }}
),

lab_observations AS (

    SELECT DISTINCT

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        CAST(NULL AS STRING) AS diagnosis_code,
        CAST(NULL AS STRING) AS doctor_name,

        lab_test_name,
        technician_name,

        CAST(NULL AS STRING) AS device_type,

        'LAB' AS observation_type

    FROM {{ ref('silver_lab_results') }}
),

vital_observations AS (

    SELECT DISTINCT

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        CAST(NULL AS STRING) AS diagnosis_code,
        CAST(NULL AS STRING) AS doctor_name,

        CAST(NULL AS STRING) AS lab_test_name,
        CAST(NULL AS STRING) AS technician_name,

        device_type,

        'VITAL' AS observation_type

    FROM {{ ref('silver_patient_vitals') }}
),

combined AS (

    SELECT * FROM diagnosis_observations

    UNION

    SELECT * FROM lab_observations

    UNION

    SELECT * FROM vital_observations
)

SELECT

    SHA2(
        CONCAT_WS(
            '|',
            CAST(patient_id AS STRING),
            CAST(hospital_id AS STRING),
            CAST(record_date AS STRING),
            COALESCE(diagnosis_code, ''),
            COALESCE(lab_test_name, ''),
            COALESCE(device_type, ''),
            observation_type
        ),
        256
    ) AS observation_key,

    patient_id,
    hospital_id,
    hospital_name,
    record_date,

    diagnosis_code,
    doctor_name,

    lab_test_name,
    technician_name,

    device_type,

    observation_type

FROM combined;


-- ============================================================
-- FILE 4: dim_patients.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH ranked_patients AS (

    SELECT

        patient_id,
        patient_name,
        gender,
        city,
        age,

        CASE
            WHEN age < 18 THEN '0-17'
            WHEN age BETWEEN 18 AND 35 THEN '18-35'
            WHEN age BETWEEN 36 AND 50 THEN '36-50'
            WHEN age BETWEEN 51 AND 65 THEN '51-65'
            WHEN age > 65 THEN '65+'
            ELSE 'Unknown'
        END AS age_group,

        income_index,
        health_score,
        lifestyle_risk,

        exercise_hours,
        sleep_hours,

        alcohol_index,
        smoking_index,

        diet_score,
        insurance_score,

        record_date,
        _ingested_at,
        _source_file,

        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY record_date DESC, _ingested_at DESC
        ) AS rn

    FROM {{ ref('silver_patient_demographics') }}
)

SELECT

    SHA2(
        CAST(patient_id AS STRING),
        256
    ) AS patient_key,

    patient_id,
    patient_name,
    gender,
    city,

    age,
    age_group,

    income_index,
    health_score,
    lifestyle_risk,

    exercise_hours,
    sleep_hours,

    alcohol_index,
    smoking_index,

    diet_score,
    insurance_score,

    record_date AS latest_record_date,

    _ingested_at,
    _source_file

FROM ranked_patients

WHERE rn = 1;


-- ============================================================
-- FILE 5: fact_health_metrics.sql
-- ============================================================

{{ config(materialized='table', schema='gold') }}

WITH diagnosis AS (

    SELECT

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        diagnosis_code,
        doctor_name,

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

        data_quality_flag AS diagnosis_quality_flag

    FROM {{ ref('silver_patient_diagnosis') }}
),

patient AS (

    SELECT

        patient_key,
        patient_id,

        age,
        age_group,
        gender,
        city,

        lifestyle_risk,
        smoking_index,
        alcohol_index,

        health_score,

        exercise_hours,
        sleep_hours,

        diet_score,
        insurance_score

    FROM {{ ref('dim_patients') }}
),

hospital AS (

    SELECT

        hospital_key,
        hospital_id,
        hospital_name,

        city AS hospital_city,
        state AS hospital_state,

        infection_rate,
        utilization_rate,
        avg_wait_time,

        equipment_score,
        patient_load,

        surgery_count,
        emergency_cases

    FROM {{ ref('dim_hospital') }}
),

date_dim AS (

    SELECT

        date_key,
        record_date,

        day,
        month,
        month_name,

        quarter,
        year

    FROM {{ ref('dim_date') }}
),

vitals AS (

    SELECT

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        AVG(heart_rate) AS avg_heart_rate,

        AVG(blood_pressure_sys)
            AS avg_blood_pressure_sys,

        AVG(blood_pressure_dia)
            AS avg_blood_pressure_dia,

        AVG(oxygen_level)
            AS avg_oxygen_level,

        AVG(body_temp)
            AS avg_body_temp,

        AVG(respiration_rate)
            AS avg_respiration_rate,

        AVG(glucose_level)
            AS avg_glucose_level,

        AVG(cholesterol)
            AS avg_cholesterol,

        AVG(bmi)
            AS avg_bmi,

        AVG(stress_index)
            AS avg_stress_index,

        MAX(
            CASE
                WHEN data_quality_flag IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS has_vital_quality_flag

    FROM {{ ref('silver_patient_vitals') }}

    GROUP BY

        patient_id,
        hospital_id,
        hospital_name,
        record_date
),

labs AS (

    SELECT

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        AVG(hemoglobin)
            AS avg_hemoglobin,

        AVG(platelets)
            AS avg_platelets,

        AVG(wbc_count)
            AS avg_wbc_count,

        AVG(rbc_count)
            AS avg_rbc_count,

        AVG(creatinine)
            AS avg_creatinine,

        AVG(sodium)
            AS avg_sodium,

        AVG(potassium)
            AS avg_potassium,

        AVG(calcium)
            AS avg_calcium,

        AVG(bilirubin)
            AS avg_bilirubin,

        SUM(test_cost)
            AS total_test_cost,

        MAX(
            CASE
                WHEN data_quality_flag IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS has_lab_quality_flag

    FROM {{ ref('silver_lab_results') }}

    GROUP BY

        patient_id,
        hospital_id,
        hospital_name,
        record_date
),

observation AS (

    SELECT

        observation_key,

        patient_id,
        hospital_id,
        hospital_name,
        record_date,

        diagnosis_code,
        doctor_name

    FROM {{ ref('dim_observations') }}

    WHERE observation_type = 'DIAGNOSIS'
),

fact_base AS (

    SELECT

        p.patient_key,
        h.hospital_key,

        dt.date_key,

        o.observation_key,

        d.patient_id,
        d.hospital_id,
        d.hospital_name,

        d.record_date,

        d.diagnosis_code,
        d.doctor_name,

        p.age,
        p.age_group,
        p.gender,

        p.city AS patient_city,

        p.lifestyle_risk,
        p.smoking_index,
        p.alcohol_index,

        p.health_score,

        p.exercise_hours,
        p.sleep_hours,

        p.diet_score,
        p.insurance_score,

        h.hospital_city,
        h.hospital_state,

        h.infection_rate,
        h.utilization_rate,
        h.avg_wait_time,

        h.equipment_score,
        h.patient_load,

        h.surgery_count,
        h.emergency_cases,

        dt.day,
        dt.month,
        dt.month_name,

        dt.quarter,
        dt.year,

        d.severity_score,
        d.risk_probability,
        d.readmission_risk,

        d.treatment_cost,
        d.insurance_claim,

        d.medication_count,
        d.visit_duration,

        d.procedure_count,
        d.recovery_days,

        d.comorbidity_score,

        v.avg_heart_rate,
        v.avg_blood_pressure_sys,
        v.avg_blood_pressure_dia,

        v.avg_oxygen_level,
        v.avg_body_temp,

        v.avg_respiration_rate,
        v.avg_glucose_level,

        v.avg_cholesterol,
        v.avg_bmi,

        v.avg_stress_index,

        COALESCE(
            v.has_vital_quality_flag,
            0
        ) AS has_vital_quality_flag,

        l.avg_hemoglobin,
        l.avg_platelets,

        l.avg_wbc_count,
        l.avg_rbc_count,

        l.avg_creatinine,

        l.avg_sodium,
        l.avg_potassium,
        l.avg_calcium,

        l.avg_bilirubin,

        COALESCE(
            l.total_test_cost,
            0
        ) AS total_test_cost,

        COALESCE(
            l.has_lab_quality_flag,
            0
        ) AS has_lab_quality_flag,

        d.diagnosis_quality_flag

    FROM diagnosis d

    INNER JOIN patient p
        ON d.patient_id = p.patient_id

    INNER JOIN hospital h
        ON d.hospital_id = h.hospital_id

    INNER JOIN date_dim dt
        ON d.record_date = dt.record_date

    LEFT JOIN observation o
        ON d.patient_id = o.patient_id
        AND d.hospital_id = o.hospital_id
        AND d.record_date = o.record_date
        AND d.diagnosis_code = o.diagnosis_code

    LEFT JOIN vitals v
        ON d.patient_id = v.patient_id
        AND d.hospital_id = v.hospital_id
        AND d.record_date = v.record_date

    LEFT JOIN labs l
        ON d.patient_id = l.patient_id
        AND d.hospital_id = l.hospital_id
        AND d.record_date = l.record_date
),

final AS (

    SELECT

        SHA2(
            CONCAT_WS(
                '|',

                CAST(patient_id AS STRING),
                CAST(hospital_id AS STRING),
                CAST(record_date AS STRING),

                COALESCE(
                    diagnosis_code,
                    ''
                ),

                COALESCE(
                    observation_key,
                    ''
                )
            ),
            256
        ) AS health_metric_key,

        patient_key,
        hospital_key,

        date_key,
        observation_key,

        patient_id,
        hospital_id,
        hospital_name,

        record_date,

        diagnosis_code,
        doctor_name,

        age,
        age_group,
        gender,

        patient_city,

        hospital_city,
        hospital_state,

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

        avg_heart_rate,

        avg_blood_pressure_sys,
        avg_blood_pressure_dia,

        avg_oxygen_level,
        avg_body_temp,

        avg_respiration_rate,
        avg_glucose_level,

        avg_cholesterol,
        avg_bmi,

        avg_stress_index,

        avg_hemoglobin,
        avg_platelets,

        avg_wbc_count,
        avg_rbc_count,

        avg_creatinine,

        avg_sodium,
        avg_potassium,

        avg_calcium,
        avg_bilirubin,

        total_test_cost,

        infection_rate,
        utilization_rate,
        avg_wait_time,

        equipment_score,
        patient_load,

        surgery_count,
        emergency_cases,

        lifestyle_risk,

        smoking_index,
        alcohol_index,

        health_score,

        exercise_hours,
        sleep_hours,

        diet_score,
        insurance_score,

        has_vital_quality_flag,
        has_lab_quality_flag,

        ROUND(
            (
                  0.38
                    * COALESCE(risk_probability, 0)

                + 0.24
                    * COALESCE(severity_score, 0)

                + 0.14
                    * COALESCE(
                        comorbidity_score / 100,
                        0
                    )

                + 0.10
                    * COALESCE(
                        lifestyle_risk / 100,
                        0
                    )

                + 0.09
                    * COALESCE(
                        (
                            smoking_index
                            + alcohol_index
                        ) / 2,
                        0
                    )

                + 0.05
                    * CASE
                        WHEN
                            has_vital_quality_flag = 1
                            OR has_lab_quality_flag = 1
                        THEN 1
                        ELSE 0
                      END
            ),
            4
        ) AS patient_risk_score,

        diagnosis_quality_flag

    FROM fact_base
),

deduped AS (

    SELECT

        *,

        ROW_NUMBER() OVER (
            PARTITION BY health_metric_key
            ORDER BY record_date DESC
        ) AS rn

    FROM final
)

SELECT

    health_metric_key,

    patient_key,
    hospital_key,
    date_key,
    observation_key,

    patient_id,
    hospital_id,
    hospital_name,

    record_date,

    diagnosis_code,
    doctor_name,

    age,
    age_group,
    gender,

    patient_city,

    hospital_city,
    hospital_state,

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

    avg_heart_rate,

    avg_blood_pressure_sys,
    avg_blood_pressure_dia,

    avg_oxygen_level,
    avg_body_temp,

    avg_respiration_rate,
    avg_glucose_level,

    avg_cholesterol,
    avg_bmi,

    avg_stress_index,

    avg_hemoglobin,
    avg_platelets,

    avg_wbc_count,
    avg_rbc_count,

    avg_creatinine,

    avg_sodium,
    avg_potassium,

    avg_calcium,
    avg_bilirubin,

    total_test_cost,

    infection_rate,
    utilization_rate,
    avg_wait_time,

    equipment_score,
    patient_load,

    surgery_count,
    emergency_cases,

    lifestyle_risk,

    smoking_index,
    alcohol_index,

    health_score,

    exercise_hours,
    sleep_hours,

    diet_score,
    insurance_score,

    has_vital_quality_flag,
    has_lab_quality_flag,

    patient_risk_score,

    diagnosis_quality_flag

FROM deduped

WHERE rn = 1;
