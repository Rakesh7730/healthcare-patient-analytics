{{ config(materialized='table', schema='gold') }}

WITH all\_dates AS (

    SELECT record\_date FROM {{ ref('silver\_patient\_demographics') }}

    UNION

    SELECT record\_date FROM {{ ref('silver\_hospital\_info') }}

    UNION

    SELECT record\_date FROM {{ ref('silver\_patient\_diagnosis') }}

    UNION

    SELECT record\_date FROM {{ ref('silver\_lab\_results') }}

    UNION

    SELECT record\_date FROM {{ ref('silver\_patient\_vitals') }}

),

deduped AS (

    SELECT DISTINCT record\_date

    FROM all\_dates

    WHERE record\_date IS NOT NULL

)

SELECT

    CAST(DATE\_FORMAT(record\_date, 'yyyyMMdd') AS INT) AS date\_key,

    record\_date,

    DAY(record\_date) AS day,

    MONTH(record\_date) AS month,

    DATE\_FORMAT(record\_date, 'MMMM') AS month\_name,

    QUARTER(record\_date) AS quarter,

    YEAR(record\_date) AS year,

    DATE\_FORMAT(record\_date, 'E') AS day\_name,

    CASE

        WHEN DATE\_FORMAT(record\_date, 'E') IN ('Sat', 'Sun') THEN true

        ELSE false

    END AS is\_weekend

FROM deduped
