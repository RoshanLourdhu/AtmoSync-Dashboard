SELECT
    COMMODITY_ID,
    COMMODITY_NAME,
    PRICE
FROM {{ ref('stg_commodity') }}
WHERE PRICE <= 0
   OR PRICE IS NULL