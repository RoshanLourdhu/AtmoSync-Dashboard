WITH commodity AS (

    SELECT *
    FROM {{ ref('stg_commodity') }}

),

shelf_life AS (

    SELECT
        COMMODITY_ID,
        COMMODITY_NAME,
        CATEGORY,
        PRICE,
        QUANTITY,
        LOCATION,
        RECORDED_AT,

        CASE
            WHEN CATEGORY = 'Fruits' THEN 5
            WHEN CATEGORY = 'Vegetables' THEN 7
            WHEN CATEGORY = 'Grains' THEN 30
            ELSE NULL
        END AS SHELF_LIFE_DAYS

    FROM commodity

),

spoilage AS (

    SELECT
        *,
        DATEDIFF(
            'day',
            TO_DATE(RECORDED_AT),
            CURRENT_DATE()
        ) AS DAYS_STORED

    FROM shelf_life

),

calculation AS (

    SELECT
        *,
        CASE
            WHEN SHELF_LIFE_DAYS IS NULL THEN NULL
            WHEN DAYS_STORED >= SHELF_LIFE_DAYS THEN 100
            ELSE 0
        END AS SPOILAGE_PERCENTAGE

    FROM spoilage

)

SELECT
    *,
    ROUND(
        QUANTITY * SPOILAGE_PERCENTAGE / 100,
        2
    ) AS SPOILED_QUANTITY,

    ROUND(
        QUANTITY - (QUANTITY * SPOILAGE_PERCENTAGE / 100),
        2
    ) AS GOOD_QUANTITY

FROM calculation