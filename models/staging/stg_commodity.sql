{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('commodity_source', 'RAW_COMMODITY') }}

),

renamed AS (

    SELECT
        CAST(COMMODITY_ID AS STRING)          AS COMMODITY_ID,
        CAST(COMMODITY_NAME AS STRING)        AS COMMODITY_NAME,
        CAST(CATEGORY AS STRING)              AS CATEGORY,
        CAST(PRICE AS NUMBER(10, 2))          AS PRICE,
        CAST(QUANTITY AS NUMBER(10, 0))       AS QUANTITY,
        CAST(LOCATION AS STRING)              AS LOCATION,
        CAST(RECORDED_AT AS TIMESTAMP_TZ)     AS RECORDED_AT
    FROM source

)

SELECT *
FROM renamed
