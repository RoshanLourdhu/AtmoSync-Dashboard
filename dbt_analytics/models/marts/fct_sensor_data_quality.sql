{{ config(materialized='table') }}

/*
    AtmoSync Sensor Data Quality & Audit Mart (fct_sensor_data_quality)

    Purpose:
    Performs comprehensive data-quality profiling on raw IoT sensor telemetry
    ingested into ATMOSYNC_DB.RAW.SENSOR_READINGS before and alongside downstream consumption.

    Quality Dimensions Evaluated:
    1. Completeness: Presence of all 7 mandatory contract fields.
    2. Validity (Temperature): Bounds checking for physical impossibility (< -50C or > 60C)
       and refrigeration operational departures (< 0C or > 25C).
    3. Validity (Humidity): Physical bounds (0.0 <= humidity <= 100.0) and near-saturation alert (> 95.0%).
    4. Validity (Vibration): Non-negativity (vibration >= 0.0) and severe shock alerts (> 0.50g).
    5. Timeliness & Clock Drift: Missing or future timestamps (> 5 mins in future).
    6. Uniqueness & Deduplication: Duplicate record detection via (container_id, recorded_at).
    7. Commodity Validity: Checks against known perishable commodity registry.

    Classification Statuses:
    - PASS: All mandatory fields present, valid physical ranges, no duplicates, normal operational bounds.
    - WARNING: Structurally valid, but exhibits duplicates, near-saturation humidity, shock vibration, or operational departures.
    - FAIL: Missing mandatory fields, physically impossible values (< -50C or > 60C, humidity < 0 or > 100, vibration < 0), null timestamp.
*/

WITH raw_source AS (
    SELECT
        EVENT_PAYLOAD,
        INGESTED_AT,
        KAFKA_TOPIC,
        KAFKA_PARTITION,
        KAFKA_OFFSET
    FROM {{ source('raw', 'SENSOR_READINGS') }}
),

extracted AS (
    SELECT
        -- Raw payload extraction
        EVENT_PAYLOAD:container_id::STRING          AS container_id,
        EVENT_PAYLOAD:shipment_id::STRING           AS shipment_id,
        EVENT_PAYLOAD:commodity::STRING             AS commodity,
        EVENT_PAYLOAD:temperature::FLOAT            AS temperature,
        EVENT_PAYLOAD:humidity::FLOAT               AS humidity,
        EVENT_PAYLOAD:vibration::FLOAT              AS vibration,
        EVENT_PAYLOAD:timestamp::TIMESTAMP_TZ       AS recorded_at,

        -- Ingestion audit coordinates
        KAFKA_TOPIC                                 AS kafka_topic,
        KAFKA_PARTITION                             AS kafka_partition,
        KAFKA_OFFSET                                AS kafka_offset,
        INGESTED_AT                                 AS ingested_at
    FROM raw_source
),

ranked_records AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY container_id, recorded_at
            ORDER BY ingested_at DESC, kafka_offset DESC
        ) AS duplicate_occurrence_rank
    FROM extracted
),

anomaly_evaluated AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        temperature,
        humidity,
        vibration,
        recorded_at,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        ingested_at,
        duplicate_occurrence_rank,

        -- 1. Missing Required Fields (Fatal)
        CASE
            WHEN container_id IS NULL OR TRIM(container_id) = ''
              OR shipment_id IS NULL OR TRIM(shipment_id) = ''
              OR commodity IS NULL OR TRIM(commodity) = ''
              OR temperature IS NULL
              OR humidity IS NULL
              OR vibration IS NULL
              OR recorded_at IS NULL
            THEN 1 ELSE 0
        END AS is_missing_required_fields,

        -- 2. Temperature Checks
        CASE
            WHEN temperature IS NOT NULL AND (temperature < -50.0 OR temperature > 60.0)
            THEN 1 ELSE 0
        END AS is_temperature_physically_impossible,

        CASE
            WHEN temperature IS NOT NULL AND (temperature < 0.0 OR temperature > 25.0)
            THEN 1 ELSE 0
        END AS is_temperature_operational_anomaly,

        -- 3. Humidity Checks
        CASE
            WHEN humidity IS NOT NULL AND (humidity < 0.0 OR humidity > 100.0)
            THEN 1 ELSE 0
        END AS is_humidity_out_of_bounds,

        CASE
            WHEN humidity IS NOT NULL AND humidity > 95.0
            THEN 1 ELSE 0
        END AS is_humidity_near_saturation,

        -- 4. Vibration Checks
        CASE
            WHEN vibration IS NOT NULL AND vibration < 0.0
            THEN 1 ELSE 0
        END AS is_vibration_negative,

        CASE
            WHEN vibration IS NOT NULL AND vibration > 0.50
            THEN 1 ELSE 0
        END AS is_vibration_shock_alert,

        -- 5. Timestamp Checks
        CASE
            WHEN recorded_at IS NULL THEN 1 ELSE 0
        END AS is_timestamp_missing,

        CASE
            WHEN recorded_at IS NOT NULL AND recorded_at > DATEADD('minute', 5, CURRENT_TIMESTAMP())
            THEN 1 ELSE 0
        END AS is_timestamp_future,

        -- 6. Duplicate Event Check
        CASE
            WHEN duplicate_occurrence_rank > 1 THEN 1 ELSE 0
        END AS is_duplicate_event,

        -- 7. Commodity Validity Check
        CASE
            WHEN commodity IS NOT NULL AND commodity NOT IN ('Avocados', 'Mangoes', 'Bananas')
            THEN 1 ELSE 0
        END AS is_unknown_commodity

    FROM ranked_records
),

classified AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        temperature,
        humidity,
        vibration,
        recorded_at,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        ingested_at,
        duplicate_occurrence_rank,

        -- Detailed boolean validation flags
        is_missing_required_fields,
        is_temperature_physically_impossible,
        is_temperature_operational_anomaly,
        is_humidity_out_of_bounds,
        is_humidity_near_saturation,
        is_vibration_negative,
        is_vibration_shock_alert,
        is_timestamp_missing,
        is_timestamp_future,
        is_duplicate_event,
        is_unknown_commodity,

        -- Data Quality Status Classification
        CASE
            WHEN is_missing_required_fields = 1
              OR is_temperature_physically_impossible = 1
              OR is_humidity_out_of_bounds = 1
              OR is_vibration_negative = 1
              OR is_timestamp_missing = 1
            THEN 'FAIL'

            WHEN is_duplicate_event = 1
              OR is_timestamp_future = 1
              OR is_temperature_operational_anomaly = 1
              OR is_humidity_near_saturation = 1
              OR is_vibration_shock_alert = 1
              OR is_unknown_commodity = 1
            THEN 'WARNING'

            ELSE 'PASS'
        END AS quality_status,

        -- Descriptive issue reason concatenation for operational investigation
        TRIM(
            CONCAT(
                CASE WHEN is_missing_required_fields = 1 THEN 'MISSING_REQUIRED_FIELDS; ' ELSE '' END,
                CASE WHEN is_temperature_physically_impossible = 1 THEN 'TEMPERATURE_PHYSICALLY_IMPOSSIBLE; ' ELSE '' END,
                CASE WHEN is_humidity_out_of_bounds = 1 THEN 'HUMIDITY_OUT_OF_BOUNDS; ' ELSE '' END,
                CASE WHEN is_vibration_negative = 1 THEN 'VIBRATION_NEGATIVE; ' ELSE '' END,
                CASE WHEN is_timestamp_missing = 1 THEN 'TIMESTAMP_MISSING; ' ELSE '' END,
                CASE WHEN is_timestamp_future = 1 THEN 'TIMESTAMP_IN_FUTURE; ' ELSE '' END,
                CASE WHEN is_duplicate_event = 1 THEN 'DUPLICATE_EVENT; ' ELSE '' END,
                CASE WHEN is_temperature_operational_anomaly = 1 AND is_temperature_physically_impossible = 0 THEN 'TEMPERATURE_OPERATIONAL_ANOMALY; ' ELSE '' END,
                CASE WHEN is_humidity_near_saturation = 1 AND is_humidity_out_of_bounds = 0 THEN 'HUMIDITY_NEAR_SATURATION; ' ELSE '' END,
                CASE WHEN is_vibration_shock_alert = 1 AND is_vibration_negative = 0 THEN 'VIBRATION_SHOCK_ALERT; ' ELSE '' END,
                CASE WHEN is_unknown_commodity = 1 THEN 'UNKNOWN_COMMODITY; ' ELSE '' END
            ),
            '; '
        ) AS quality_issue_reasons

    FROM anomaly_evaluated
)

SELECT * FROM classified
