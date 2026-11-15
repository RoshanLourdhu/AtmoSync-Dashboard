-- Fails if any staging record has physically impossible temperature reading or is null
SELECT
    container_id,
    shipment_id,
    recorded_at,
    temperature
FROM {{ ref('stg_sensor_readings') }}
WHERE temperature < -50.0
   OR temperature > 60.0
   OR temperature IS NULL
