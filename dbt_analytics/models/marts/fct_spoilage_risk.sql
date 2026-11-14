{{ config(materialized='table') }}

/*
    AtmoSync Container Spoilage Risk Analytical Mart

    Computes container risk flags and composite spoilage scores based on:
    - Commodity-specific thermal thresholds (Avocados: 8.0C, Mangoes: 10.5C, Bananas: 15.0C)
    - Relative humidity limit (>85.0%)
    - Mechanical vibration transit limit (>0.28g)
    - Composite Spoilage Score: 0.6 * Temp + 0.3 * Humidity + 0.1 * Vibration
*/

WITH sensor_readings AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

risk_evaluated AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        temperature,
        humidity,
        vibration,
        recorded_at,
        ingested_at,

        -- Commodity-specific maximum thermal threshold
        CASE
            WHEN commodity = 'Avocados' THEN 8.0
            WHEN commodity = 'Mangoes' THEN 10.5
            WHEN commodity = 'Bananas' THEN 15.0
            ELSE 10.0
        END AS temp_threshold_c,

        -- Risk Indicator Flags
        CASE
            WHEN temperature > (CASE
                WHEN commodity = 'Avocados' THEN 8.0
                WHEN commodity = 'Mangoes' THEN 10.5
                WHEN commodity = 'Bananas' THEN 15.0
                ELSE 10.0
            END) THEN 1
            ELSE 0
        END AS temp_risk_flag,

        CASE
            WHEN humidity > 85.0 THEN 1
            ELSE 0
        END AS hum_risk_flag,

        CASE
            WHEN vibration > 0.28 THEN 1
            ELSE 0
        END AS vib_risk_flag

    FROM sensor_readings
),

scored AS (
    SELECT
        *,
        -- Composite Spoilage Score: 0.6 * Temp + 0.3 * Humidity + 0.1 * Vibration
        ROUND(
            (0.6 * temp_risk_flag) + (0.3 * hum_risk_flag) + (0.1 * vib_risk_flag),
            2
        ) AS spoilage_score
    FROM risk_evaluated
),

classified AS (
    SELECT
        *,
        CASE
            WHEN spoilage_score >= 0.70 THEN 'CRITICAL'
            WHEN spoilage_score >= 0.30 THEN 'WARNING'
            ELSE 'NORMAL'
        END AS risk_level
    FROM scored
)

SELECT * FROM classified
