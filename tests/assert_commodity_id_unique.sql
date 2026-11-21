SELECT
    COMMODITY_ID,
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_commodity') }}
GROUP BY COMMODITY_ID
HAVING COUNT(*) > 1