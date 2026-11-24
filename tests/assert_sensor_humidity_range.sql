SELECT *
FROM {{ ref('stg_sensor') }}
WHERE HUMIDITY < 0
   OR HUMIDITY > 100