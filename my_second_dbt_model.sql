SELECT
    id,
    message,
    UPPER(message) AS uppercase_message
FROM {{ ref('my_first_dbt_model') }}
