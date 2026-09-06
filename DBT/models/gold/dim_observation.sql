{{ config(materialized='table', schema='gold') }}

WITH diagnosis\_observations AS (

    SELECT DISTINCT

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        diagnosis\_code,

        doctor\_name,

        CAST(NULL AS STRING) AS lab\_test\_name,

        CAST(NULL AS STRING) AS technician\_name,

        CAST(NULL AS STRING) AS device\_type,

        'DIAGNOSIS' AS observation\_type

    FROM {{ ref('silver\_patient\_diagnosis') }}

),

lab\_observations AS (

    SELECT DISTINCT

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        CAST(NULL AS STRING) AS diagnosis\_code,

        CAST(NULL AS STRING) AS doctor\_name,

        lab\_test\_name,

        technician\_name,

        CAST(NULL AS STRING) AS device\_type,

        'LAB' AS observation\_type

    FROM {{ ref('silver\_lab\_results') }}

),

vital\_observations AS (

    SELECT DISTINCT

        patient\_id,

        hospital\_id,

        hospital\_name,

        record\_date,

        CAST(NULL AS STRING) AS diagnosis\_code,

        CAST(NULL AS STRING) AS doctor\_name,

        CAST(NULL AS STRING) AS lab\_test\_name,

        CAST(NULL AS STRING) AS technician\_name,

        device\_type,

        'VITAL' AS observation\_type

    FROM {{ ref('silver\_patient\_vitals') }}

),

combined AS (

    SELECT \* FROM diagnosis\_observations

    UNION

    SELECT \* FROM lab\_observations

    UNION

    SELECT \* FROM vital\_observations

)

SELECT

    SHA2(

        CONCAT\_WS('|',

            patient\_id,

            hospital\_id,

            CAST(record\_date AS STRING),

            COALESCE(diagnosis\_code, ''),

            COALESCE(lab\_test\_name, ''),

            COALESCE(device\_type, ''),

            observation\_type

        ),

        256

    ) AS observation\_key,

    patient\_id,

    hospital\_id,

    hospital\_name,

    record\_date,

    diagnosis\_code,

    doctor\_name,

    lab\_test\_name,

    technician\_name,

    device\_type,

    observation\_type

FROM combined
