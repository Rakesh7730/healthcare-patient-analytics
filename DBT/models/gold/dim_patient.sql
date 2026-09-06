{{ config(materialized='table', schema='gold') }}

WITH ranked\_patients AS (

    SELECT

        patient\_id,

        patient\_name,

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

        END AS age\_group,

        income\_index,

        health\_score,

        lifestyle\_risk,

        exercise\_hours,

        sleep\_hours,

        alcohol\_index,

        smoking\_index,

        diet\_score,

        insurance\_score,

        record\_date,

        \_ingested\_at,

        \_source\_file,

        ROW\_NUMBER() OVER (

            PARTITION BY patient\_id

            ORDER BY record\_date DESC, \_ingested\_at DESC

        ) AS rn

    FROM {{ ref('silver\_patient\_demographics') }}

)

SELECT

    SHA2(CAST(patient\_id AS STRING), 256) AS patient\_key,

    patient\_id,

    patient\_name,

    gender,

    city,

    age,

    age\_group,

    income\_index,

    health\_score,

    lifestyle\_risk,

    exercise\_hours,

    sleep\_hours,

    alcohol\_index,

    smoking\_index,

    diet\_score,

    insurance\_score,

    record\_date AS latest\_record\_date,

    \_ingested\_at,

    \_source\_file

FROM ranked\_patients

WHERE rn = 1
