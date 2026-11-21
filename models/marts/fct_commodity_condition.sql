{{ config(materialized='table') }}

WITH commodity AS (
    SELECT *
    FROM {{ ref('stg_commodity') }}
),

commodity_condition AS (
    SELECT
        COMMODITY_ID,
        COMMODITY_NAME,
        CATEGORY,
        PRICE,
        QUANTITY,
        LOCATION,
        RECORDED_AT,

        CASE
            WHEN PRICE >= 100 THEN 'High Price'
            WHEN PRICE >= 50 THEN 'Medium Price'
            ELSE 'Low Price'
        END AS PRICE_STATUS,

        CASE
            WHEN QUANTITY <= 10 THEN 'Low Stock'
            WHEN QUANTITY <= 50 THEN 'Medium Stock'
            ELSE 'Available Stock'
        END AS STOCK_STATUS

    FROM commodity
)

SELECT *
FROM commodity_condition
