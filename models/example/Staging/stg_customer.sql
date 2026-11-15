WITH source AS (

    SELECT *
    FROM {{ source ('tpch_source','CUSTOMER') }}

),

renamed AS (

    SELECT
        C_CUSTKEY AS CUSTOMER_ID,
        C_NAME AS CUSTOMER_NAME,
        C_ADDRESS AS CUSTOMER_ADDRESS,
        C_NATIONKEY AS NATION_ID,
        C_PHONE AS PHONE_NUMBER,
        C_ACCTBAL AS ACCOUNT_BALANCE,
        C_MKTSEGMENT AS MARKET_SEGMENT,
        C_COMMENT AS CUSTOMER_COMMENT

    FROM source

)

SELECT *
FROM renamed