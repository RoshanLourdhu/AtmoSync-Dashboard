-- Fails if duplicate sensor events exist for the same container at the same timestamp
SELECT
    container_id,
    recorded_at,
    COUNT(*) AS event_count
FROM {{ ref('stg_sensor_readings') }}
GROUP BY container_id, recorded_at
HAVING COUNT(*) > 1
