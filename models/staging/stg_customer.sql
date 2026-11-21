{{ config(materialized='view') }}

WITH source AS (

    SELECT *
    FROM {{ source('tpch_source', 'CUSTOMER') }}

),

renamed AS (

    SELECT
        CAST(C_CUSTKEY AS NUMBER)             AS CUSTOMER_ID,
        CAST(C_NAME AS STRING)                AS CUSTOMER_NAME,
        CAST(C_ADDRESS AS STRING)             AS CUSTOMER_ADDRESS,
        CAST(C_NATIONKEY AS NUMBER)           AS NATION_ID,
        CAST(C_PHONE AS STRING)               AS PHONE_NUMBER,
        CAST(C_ACCTBAL AS NUMBER(12, 2))      AS ACCOUNT_BALANCE,
        CAST(C_MKTSEGMENT AS STRING)          AS MARKET_SEGMENT,
        CAST(C_COMMENT AS STRING)             AS CUSTOMER_COMMENT
    FROM source

)

SELECT *
FROM renamed
