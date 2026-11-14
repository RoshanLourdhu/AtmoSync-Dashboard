{{ config(materialized='view') }}

WITH source AS (
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
        -- Unpack 7 mandatory fields from JSON contract
        EVENT_PAYLOAD:container_id::STRING          AS container_id,
        EVENT_PAYLOAD:shipment_id::STRING           AS shipment_id,
        EVENT_PAYLOAD:commodity::STRING             AS commodity,
        EVENT_PAYLOAD:temperature::FLOAT            AS temperature,
        EVENT_PAYLOAD:humidity::FLOAT               AS humidity,
        EVENT_PAYLOAD:vibration::FLOAT              AS vibration,
        EVENT_PAYLOAD:timestamp::TIMESTAMP_TZ       AS recorded_at,

        -- Streaming lineage metadata
        KAFKA_TOPIC                                 AS kafka_topic,
        KAFKA_PARTITION                             AS kafka_partition,
        KAFKA_OFFSET                                AS kafka_offset,
        INGESTED_AT                                 AS ingested_at
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY container_id, recorded_at
            ORDER BY ingested_at DESC, kafka_offset DESC
        ) AS dedup_rank
    FROM extracted
)

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
    ingested_at
FROM deduplicated
WHERE dedup_rank = 1
