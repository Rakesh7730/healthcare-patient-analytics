{{ config(materialized='table', schema='gold') }}

WITH ranked\_hospitals AS (

    SELECT

        hospital\_id,

        hospital\_name,

        city,

        state,

        bed\_capacity,

        icu\_beds,

        staff\_count,

        infection\_rate,

        utilization\_rate,

        avg\_wait\_time,

        equipment\_score,

        patient\_load,

        surgery\_count,

        emergency\_cases,

        record\_date,

        \_ingested\_at,

        \_source\_file,

        ROW\_NUMBER() OVER (

            PARTITION BY hospital\_id

            ORDER BY record\_date DESC, \_ingested\_at DESC

        ) AS rn

    FROM {{ ref('silver\_hospital\_info') }}

)

SELECT

    SHA2(CAST(hospital\_id AS STRING), 256) AS hospital\_key,

    hospital\_id,

    hospital\_name,

    city,

    state,

    bed\_capacity,

    icu\_beds,

    staff\_count,

    infection\_rate,

    utilization\_rate,

    avg\_wait\_time,

    equipment\_score,

    patient\_load,

    surgery\_count,

    emergency\_cases,

    record\_date AS latest\_record\_date,

    \_ingested\_at,

    \_source\_file

FROM ranked\_hospitals

WHERE rn = 1
