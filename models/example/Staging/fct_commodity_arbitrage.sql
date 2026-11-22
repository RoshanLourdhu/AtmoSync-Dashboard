WITH commodity_prices AS (

    SELECT
        COMMODITY_NAME,
        CATEGORY,
        PRICE,
        QUANTITY,
        LOCATION
    FROM {{ ref('stg_commodity') }}

),

ranked_prices AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY COMMODITY_NAME
            ORDER BY PRICE ASC, LOCATION
        ) AS BUY_RANK,

        ROW_NUMBER() OVER (
            PARTITION BY COMMODITY_NAME
            ORDER BY PRICE DESC, LOCATION
        ) AS SELL_RANK,

        COUNT(*) OVER (
            PARTITION BY COMMODITY_NAME
        ) AS LOCATION_COUNT

    FROM commodity_prices

),

buy_prices AS (

    SELECT *
    FROM ranked_prices
    WHERE BUY_RANK = 1

),

sell_prices AS (

    SELECT *
    FROM ranked_prices
    WHERE SELL_RANK = 1

)

SELECT
    b.COMMODITY_NAME,
    b.CATEGORY,

    b.LOCATION AS BUY_LOCATION,
    s.LOCATION AS SELL_LOCATION,

    b.PRICE AS BUY_PRICE,
    s.PRICE AS SELL_PRICE,

    b.QUANTITY AS BUY_QUANTITY,
    s.QUANTITY AS SELL_QUANTITY,

    LEAST(b.QUANTITY, s.QUANTITY) AS TRADEABLE_QUANTITY,

    s.PRICE - b.PRICE AS PRICE_DIFFERENCE,

    ROUND(
        ((s.PRICE - b.PRICE) / NULLIF(b.PRICE, 0)) * 100,
        2
    ) AS ARBITRAGE_PERCENTAGE,

    ROUND(
        (s.PRICE - b.PRICE) *
        LEAST(b.QUANTITY, s.QUANTITY),
        2
    ) AS ESTIMATED_GROSS_PROFIT,

    CASE
        WHEN s.PRICE > b.PRICE THEN 'Opportunity'
        ELSE 'No Opportunity'
    END AS ARBITRAGE_STATUS

FROM buy_prices b

JOIN sell_prices s
    ON b.COMMODITY_NAME = s.COMMODITY_NAME

WHERE b.LOCATION_COUNT >= 2