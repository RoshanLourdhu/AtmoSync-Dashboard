{{ config(materialized='table') }}

/*
    AtmoSync Commodity Arbitrage & Rerouting Readiness Mart

    Business Rule:
    Identifies shipments currently at elevated risk (WARNING or CRITICAL) that require
    rerouting consideration or expedited dispatch.

    Data Availability Note:
    External destination wholesale price feeds (e.g., Chennai vs Bengaluru vs Hyderabad spot prices)
    are pending integration in Phase 2 of data engineering. To prevent analytical distortions,
    monetary price spreads are NOT fabricated. Once market price feeds are ingested, price differential
    and net margin columns will be joined here.
*/

WITH spoilage_risk AS (
    SELECT * FROM {{ ref('fct_spoilage_risk') }}
),

latest_container_status AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        temperature,
        humidity,
        vibration,
        recorded_at,
        risk_level,
        spoilage_score,
        temp_risk_flag,
        hum_risk_flag,
        vib_risk_flag,
        ROW_NUMBER() OVER (
            PARTITION BY container_id
            ORDER BY recorded_at DESC
        ) AS recency_rank
    FROM spoilage_risk
),

actionable_containers AS (
    SELECT
        container_id,
        shipment_id,
        commodity,
        temperature,
        humidity,
        vibration,
        recorded_at,
        risk_level,
        spoilage_score,
        CASE
            WHEN risk_level = 'CRITICAL' THEN 'IMMEDIATE_REROUTE_OR_LIQUIDATION'
            WHEN risk_level = 'WARNING' THEN 'INSPECT_AND_EXPEDITE'
            ELSE 'MAINTAIN_SCHEDULE'
        END AS logistics_action_recommendation,
        CASE
            WHEN risk_level IN ('CRITICAL', 'WARNING') THEN TRUE
            ELSE FALSE
        END AS is_reroute_candidate
    FROM latest_container_status
    WHERE recency_rank = 1
)

SELECT * FROM actionable_containers
