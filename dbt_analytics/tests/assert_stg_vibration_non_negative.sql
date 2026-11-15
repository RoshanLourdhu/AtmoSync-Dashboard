-- Fails if any staging record has negative vibration magnitude or is null
SELECT
    container_id,
    shipment_id,
    recorded_at,
    vibration
FROM {{ ref('stg_sensor_readings') }}
WHERE vibration < 0.0
   OR vibration IS NULL
