WITH expected AS (

    SELECT
        *,
        CASE
            WHEN CATEGORY = 'Fruits' THEN 5
            WHEN CATEGORY = 'Vegetables' THEN 7
            WHEN CATEGORY = 'Grains' THEN 30
            ELSE NULL
        END AS EXPECTED_SHELF_LIFE

    FROM {{ ref('fct_commodity_spoilage') }}

),

validated AS (

    SELECT
        *,
        CASE
            WHEN EXPECTED_SHELF_LIFE IS NULL THEN NULL
            WHEN DAYS_STORED >= EXPECTED_SHELF_LIFE THEN 100
            ELSE 0
        END AS EXPECTED_SPOILAGE_PERCENTAGE

    FROM expected

)

SELECT *
FROM validated
WHERE
       SHELF_LIFE_DAYS IS DISTINCT FROM EXPECTED_SHELF_LIFE

    OR SPOILAGE_PERCENTAGE IS DISTINCT FROM EXPECTED_SPOILAGE_PERCENTAGE

    OR (
        SPOILAGE_PERCENTAGE IS NOT NULL
        AND (
            SPOILED_QUANTITY + GOOD_QUANTITY
            IS DISTINCT FROM QUANTITY
        )
    )