-- Fails if any staging record violates the 0.0 to 100.0 relative humidity range or is null
SELECT
    container_id,
    shipment_id,
    recorded_at,
    humidity
FROM {{ ref('stg_sensor_readings') }}
WHERE humidity < 0.0
   OR humidity > 100.0
   OR humidity IS NULL
