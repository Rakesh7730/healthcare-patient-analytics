{{ config(materialized='table', schema='gold') }}

WITH diagnosis AS (

    SELECT

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        diagnosis\_code,

        doctor\_name,

        severity\_score,

        risk\_probability,

        readmission\_risk,

        treatment\_cost,

        insurance\_claim,

        medication\_count,

        visit\_duration,

        procedure\_count,

        recovery\_days,

        comorbidity\_score,

        data\_quality\_flag AS diagnosis\_quality\_flag

    FROM {{ ref('silver\_patient\_diagnosis') }}

),

patient AS (

    SELECT

        patient\_key,

        patient\_id,

        age,

        age\_group,

        gender,

        city,

        lifestyle\_risk,

        smoking\_index,

        alcohol\_index,

        health\_score,

        exercise\_hours,

        sleep\_hours,

        diet\_score,

        insurance\_score

    FROM {{ ref('dim\_patient') }}

),

hospital AS (

    SELECT

        hospital\_key,

        hospital\_id,

        hospital\_name,

        city AS hospital\_city,

        state AS hospital\_state,

        infection\_rate,

        utilization\_rate,

        avg\_wait\_time,

        equipment\_score,

        patient\_load,

        surgery\_count,

        emergency\_cases

    FROM {{ ref('dim\_hospital') }}

),

date\_dim AS (

    SELECT

        date\_key,

        record\_date,

        day,

        month,

        month\_name,

        quarter,

        year

    FROM {{ ref('dim\_date') }}

),

vitals AS (

    SELECT

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        AVG(heart\_rate) AS avg\_heart\_rate,

        AVG(blood\_pressure\_sys) AS avg\_blood\_pressure\_sys,

        AVG(blood\_pressure\_dia) AS avg\_blood\_pressure\_dia,

        AVG(oxygen\_level) AS avg\_oxygen\_level,

        AVG(body\_temp) AS avg\_body\_temp,

        AVG(respiration\_rate) AS avg\_respiration\_rate,

        AVG(glucose\_level) AS avg\_glucose\_level,

        AVG(cholesterol) AS avg\_cholesterol,

        AVG(bmi) AS avg\_bmi,

        AVG(stress\_index) AS avg\_stress\_index,

        MAX(CASE WHEN data\_quality\_flag IS NOT NULL THEN 1 ELSE 0 END) AS has\_vital\_quality\_flag

    FROM {{ ref('silver\_patient\_vitals') }}

    GROUP BY patient\_id, hospital\_id, hospital\_name, record\_date

),

labs AS (

    SELECT

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        AVG(hemoglobin) AS avg\_hemoglobin,

        AVG(platelets) AS avg\_platelets,

        AVG(wbc\_count) AS avg\_wbc\_count,

        AVG(rbc\_count) AS avg\_rbc\_count,

        AVG(creatinine) AS avg\_creatinine,

        AVG(sodium) AS avg\_sodium,

        AVG(potassium) AS avg\_potassium,

        AVG(calcium) AS avg\_calcium,

        AVG(bilirubin) AS avg\_bilirubin,

        SUM(test\_cost) AS total\_test\_cost,

        MAX(CASE WHEN data\_quality\_flag IS NOT NULL THEN 1 ELSE 0 END) AS has\_lab\_quality\_flag

    FROM {{ ref('silver\_lab\_results') }}

    GROUP BY patient\_id, hospital\_id, hospital\_name, record\_date

),

observation AS (

    SELECT

        observation\_key,

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        diagnosis\_code,

        doctor\_name

    FROM {{ ref('dim\_observation') }}

    WHERE observation\_type = 'DIAGNOSIS'

),

fact\_base AS (

    SELECT

        p.patient\_key,

        h.hospital\_key,

        dt.date\_key,

        o.observation\_key,

        d.patient\_id,

        d.hospital\_id,

        d.hospital\_name,

        d.record\_date,

        d.diagnosis\_code,

        d.doctor\_name,

        p.age,

        p.age\_group,

        p.gender,

        p.city AS patient\_city,

        p.lifestyle\_risk,

        p.smoking\_index,

        p.alcohol\_index,

        p.health\_score,

        p.exercise\_hours,

        p.sleep\_hours,

        p.diet\_score,

        p.insurance\_score,

        h.hospital\_city,

        h.hospital\_state,

        h.infection\_rate,

        h.utilization\_rate,

        h.avg\_wait\_time,

        h.equipment\_score,

        h.patient\_load,

        h.surgery\_count,

        h.emergency\_cases,

        dt.day,

        dt.month,

        dt.month\_name,

        dt.quarter,

        dt.year,

        d.severity\_score,

        d.risk\_probability,

        d.readmission\_risk,

        d.treatment\_cost,

        d.insurance\_claim,

        d.medication\_count,

        d.visit\_duration,

        d.procedure\_count,

        d.recovery\_days,

        d.comorbidity\_score,

        v.avg\_heart\_rate,

        v.avg\_blood\_pressure\_sys,

        v.avg\_blood\_pressure\_dia,

        v.avg\_oxygen\_level,

        v.avg\_body\_temp,

        v.avg\_respiration\_rate,

        v.avg\_glucose\_level,

        v.avg\_cholesterol,

        v.avg\_bmi,

        v.avg\_stress\_index,

        COALESCE(v.has\_vital\_quality\_flag, 0) AS has\_vital\_quality\_flag,

        l.avg\_hemoglobin,

        l.avg\_platelets,

        l.avg\_wbc\_count,

        l.avg\_rbc\_count,

        l.avg\_creatinine,

        l.avg\_sodium,

        l.avg\_potassium,

        l.avg\_calcium,

        l.avg\_bilirubin,

        COALESCE(l.total\_test\_cost, 0) AS total\_test\_cost,

        COALESCE(l.has\_lab\_quality\_flag, 0) AS has\_lab\_quality\_flag,

        d.diagnosis\_quality\_flag

    FROM diagnosis d

    INNER JOIN patient p

        ON d.patient\_id = p.patient\_id

    INNER JOIN hospital h

        ON d.hospital\_id = h.hospital\_id

    INNER JOIN date\_dim dt

        ON d.record\_date = dt.record\_date

    LEFT JOIN observation o

        ON d.patient\_id = o.patient\_id

       AND d.hospital\_id = o.hospital\_id

       AND d.record\_date = o.record\_date

       AND d.diagnosis\_code = o.diagnosis\_code

    LEFT JOIN vitals v

        ON d.patient\_id = v.patient\_id

       AND d.hospital\_id = v.hospital\_id

       AND d.record\_date = v.record\_date

    LEFT JOIN labs l

        ON d.patient\_id = l.patient\_id

       AND d.hospital\_id = l.hospital\_id

       AND d.record\_date = l.record\_date

),

final AS (

    SELECT

        SHA2(

            CONCAT\_WS('|',

                patient\_id,

                hospital\_id,

                CAST(record\_date AS STRING),

                diagnosis\_code,

                COALESCE(observation\_key, '')

            ),

            256

        ) AS health\_metric\_key,

        patient\_key,

        hospital\_key,

        date\_key,

        observation\_key,

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        diagnosis\_code,

        doctor\_name,

        age,

        age\_group,

        gender,

        patient\_city,

        hospital\_city,

        hospital\_state,

        day,

        month,

        month\_name,

        quarter,

        year,

        severity\_score,

        risk\_probability,

        readmission\_risk,

        treatment\_cost,

        insurance\_claim,

        medication\_count,

        visit\_duration,

        procedure\_count,

        recovery\_days,

        comorbidity\_score,

        avg\_heart\_rate,

        avg\_blood\_pressure\_sys,

        avg\_blood\_pressure\_dia,

        avg\_oxygen\_level,

        avg\_body\_temp,

        avg\_respiration\_rate,

        avg\_glucose\_level,

        avg\_cholesterol,

        avg\_bmi,

        avg\_stress\_index,

        avg\_hemoglobin,

        avg\_platelets,

        avg\_wbc\_count,

        avg\_rbc\_count,

        avg\_creatinine,

        avg\_sodium,

        avg\_potassium,

        avg\_calcium,

        avg\_bilirubin,

        total\_test\_cost,

        infection\_rate,

        utilization\_rate,

        avg\_wait\_time,

        equipment\_score,

        patient\_load,

        surgery\_count,

        emergency\_cases,

        lifestyle\_risk,

        smoking\_index,

        alcohol\_index,

        health\_score,

        exercise\_hours,

        sleep\_hours,

        diet\_score,

        insurance\_score,

        has\_vital\_quality\_flag,

        has\_lab\_quality\_flag,

        ROUND(

            (

                0.40 \* COALESCE(risk\_probability, 0)

              + 0.25 \* COALESCE(severity\_score, 0)

              + 0.15 \* COALESCE(comorbidity\_score / 100, 0)

              + 0.10 \* COALESCE(lifestyle\_risk / 100, 0)

              + 0.10 \* COALESCE((smoking\_index + alcohol\_index) / 2, 0)

              + 0.05 \* CASE

                    WHEN has\_vital\_quality\_flag = 1 OR has\_lab\_quality\_flag = 1 THEN 1

                    ELSE 0

                END

            ),

            4

        ) AS patient\_risk\_score,

        diagnosis\_quality\_flag

    FROM fact\_base

),

deduped AS (

    SELECT

        \*,

        ROW\_NUMBER() OVER (

            PARTITION BY health\_metric\_key

            ORDER BY record\_date DESC

        ) AS rn

    FROM final

)

SELECT

    health\_metric\_key,

    patient\_key,

    hospital\_key,

    date\_key,

    observation\_key,

    patient\_id,

    hospital\_id,

    hospital\_name,

    record\_date,

    diagnosis\_code,

    doctor\_name,

    age,

    age\_group,

    gender,

    patient\_city,

    hospital\_city,

    hospital\_state,

    day,

    month,

    month\_name,

    quarter,

    year,

    severity\_score,

    risk\_probability,

    readmission\_risk,

    treatment\_cost,

    insurance\_claim,

    medication\_count,

    visit\_duration,

    procedure\_count,

    recovery\_days,

    comorbidity\_score,

    avg\_heart\_rate,

    avg\_blood\_pressure\_sys,

    avg\_blood\_pressure\_dia,

    avg\_oxygen\_level,

    avg\_body\_temp,

    avg\_respiration\_rate,

    avg\_glucose\_level,

    avg\_cholesterol,

    avg\_bmi,

    avg\_stress\_index,

    avg\_hemoglobin,

    avg\_platelets,

    avg\_wbc\_count,

    avg\_rbc\_count,

    avg\_creatinine,

    avg\_sodium,

    avg\_potassium,

    avg\_calcium,

    avg\_bilirubin,

    total\_test\_cost,

    infection\_rate,

    utilization\_rate,

    avg\_wait\_time,

    equipment\_score,

    patient\_load,

    surgery\_count,

    emergency\_cases,

    lifestyle\_risk,

    smoking\_index,

    alcohol\_index,

    health\_score,

    exercise\_hours,

    sleep\_hours,

    diet\_score,

    insurance\_score,

    has\_vital\_quality\_flag,

    has\_lab\_quality\_flag,

    patient\_risk\_score,

    diagnosis\_quality\_flag

FROM deduped

WHERE rn = 1
