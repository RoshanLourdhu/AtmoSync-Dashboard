{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('sensor_source', 'RAW_SENSOR') }}

),

renamed AS (

    SELECT
        CAST(SENSOR_ID AS STRING)             AS SENSOR_ID,
        CAST(SENSOR_NAME AS STRING)           AS SENSOR_NAME,
        CAST(LOCATION AS STRING)              AS LOCATION,
        CAST(TEMPERATURE AS FLOAT)            AS TEMPERATURE,
        CAST(HUMIDITY AS FLOAT)               AS HUMIDITY,
        CAST(RECORDED_AT AS TIMESTAMP_TZ)     AS RECORDED_AT
    FROM source

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SENSOR_ID
        ORDER BY RECORDED_AT DESC
    ) = 1

)

SELECT *
FROM renamed
