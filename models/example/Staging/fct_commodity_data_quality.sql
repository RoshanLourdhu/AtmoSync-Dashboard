WITH commodity AS (

    SELECT *
    FROM {{ ref('stg_commodity') }}

),

quality_checks AS (

    SELECT
        COMMODITY_ID,
        COMMODITY_NAME,
        CATEGORY,
        PRICE,
        QUANTITY,
        LOCATION,
        RECORDED_AT,
        RECEIVED_DATE,

        CASE
            WHEN COMMODITY_NAME IS NULL THEN 'Missing Commodity Name'
            WHEN CATEGORY IS NULL THEN 'Missing Category'
            WHEN PRICE IS NULL THEN 'Missing Price'
            WHEN PRICE <= 0 THEN 'Invalid Price'
            WHEN QUANTITY IS NULL THEN 'Missing Quantity'
            WHEN QUANTITY <= 0 THEN 'Invalid Quantity'
            WHEN LOCATION IS NULL THEN 'Missing Location'
            ELSE 'Valid'
        END AS DATA_QUALITY_STATUS

    FROM commodity

)

SELECT
    *,
    CASE
        WHEN DATA_QUALITY_STATUS = 'Valid' THEN 1
        ELSE 0
    END AS IS_VALID_RECORD

FROM quality_checks