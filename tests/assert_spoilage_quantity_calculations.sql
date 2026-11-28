SELECT *
FROM {{ ref('fct_commodity_spoilage') }}
WHERE
    (
        SPOILAGE_PERCENTAGE = 100
        AND (
            SPOILED_QUANTITY IS DISTINCT FROM ROUND(QUANTITY, 2)
            OR GOOD_QUANTITY IS DISTINCT FROM 0
        )
    )
    OR
    (
        SPOILAGE_PERCENTAGE = 0
        AND (
            SPOILED_QUANTITY IS DISTINCT FROM 0
            OR GOOD_QUANTITY IS DISTINCT FROM ROUND(QUANTITY, 2)
        )
    )
    OR
    (
        SPOILAGE_PERCENTAGE IS NULL
        AND (
            SPOILED_QUANTITY IS NOT NULL
            OR GOOD_QUANTITY IS NOT NULL
        )
    )
