WITH source AS (

    SELECT *
    FROM {{ source('sensor_source', 'RAW_SENSOR') }}

),

renamed AS (

    SELECT
        SENSOR_ID,
        SENSOR_NAME,
        LOCATION,
        TEMPERATURE,
        HUMIDITY,
        RECORDED_AT
    FROM source

    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SENSOR_ID
    ORDER BY RECORDED_AT DESC
    ) = 1


)

SELECT *
FROM renamed