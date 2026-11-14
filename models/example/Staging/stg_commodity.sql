WITH source AS (

    SELECT *
    FROM {{ source('sensor_source', 'RAW_COMMODITY') }}

),

renamed AS (

    SELECT
        COMMODITY_ID,
        COMMODITY_NAME,
        CATEGORY,
        PRICE,
        QUANTITY,
        LOCATION,
        RECORDED_AT
    FROM source

)

SELECT *
FROM renamed