SELECT
    COMMODITY_NAME,
    RECORD_DATE
FROM {{ ref('fct_commodity_arbitrage') }}
GROUP BY
    COMMODITY_NAME,
    RECORD_DATE
HAVING COUNT(*) > 1
