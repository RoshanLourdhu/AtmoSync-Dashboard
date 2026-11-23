WITH source AS (

    SELECT *
    FROM {{ source('sensor_source', 'RAW_SENSOR') }}

),

cleaned AS (

    SELECT
        TRY_TO_NUMBER(SENSOR_ID) AS SENSOR_ID,

        NULLIF(TRIM(SENSOR_NAME), '') AS SENSOR_NAME,

        NULLIF(TRIM(LOCATION), '') AS LOCATION,

        TRY_TO_DOUBLE(TEMPERATURE) AS TEMPERATURE,

        TRY_TO_DOUBLE(HUMIDITY) AS HUMIDITY,

        RECORDED_AT AS RECORDED_AT

    FROM source

),

deduplicated AS (

    SELECT *
    FROM cleaned

    QUALIFY SENSOR_ID IS NULL
        OR ROW_NUMBER() OVER (
            PARTITION BY SENSOR_ID
            ORDER BY RECORDED_AT DESC NULLS LAST
        ) = 1

)

SELECT *
FROM deduplicated