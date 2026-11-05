# Sensor data contract

Version: 0.1 (foundation)

Schema file: `shared/schemas/sensor_reading.schema.json`

JSON Schema dialect: Draft 2020-12

This contract is the only agreed shape for a raw sensor event. Producers and consumers must follow it. Spoilage thresholds, price fields, and dashboard metrics are downstream and are not part of this event.

## Example event

```json
{
  "container_id": "CONT_001",
  "shipment_id": "SHIP_1001",
  "commodity": "Apples",
  "temperature": 8.5,
  "humidity": 75.2,
  "vibration": 0.35,
  "timestamp": "2026-11-05T10:30:00Z"
}
```

The example is illustrative. It does not imply a safe operating range for apples or for any other commodity.

## Fields

| Field | JSON type | Required | Validation | Unit |
| --- | --- | --- | --- | --- |
| `container_id` | string | yes | Non-empty. Must contain a non-whitespace character. | Identifier, no unit |
| `shipment_id` | string | yes | Non-empty. Must contain a non-whitespace character. | Identifier, no unit |
| `commodity` | string | yes | Non-empty. Must contain a non-whitespace character. | Name, no unit |
| `temperature` | number | yes | Any JSON number, including integers. Strings are invalid. | **Not agreed.** Store the number only. |
| `humidity` | number | yes | Inclusive range 0 to 100. | Percentage scale unless the team revises the contract. |
| `vibration` | number | yes | Must be greater than or equal to 0. | **Not agreed.** Team agreement is required before comparison or risk use. |
| `timestamp` | string | yes | ISO 8601 date-time using the RFC 3339 profile: `YYYY-MM-DDTHH:MM:SS` with optional fractional seconds and a `Z` or numeric offset. | Instant |

All seven fields are required. Additional properties are rejected (`additionalProperties: false`).

## Validation requirements

A consumer or simulator must reject an event when any of the following is true:

- A required field is missing.
- A field is present with the wrong JSON type.
- `container_id`, `shipment_id`, or `commodity` is empty or whitespace only.
- `humidity` is below 0 or above 100.
- `vibration` is negative.
- `timestamp` is not an RFC 3339 date-time string. Date-only values, timestamps without a timezone, and timezone offsets without a colon are invalid.
- The object contains any field other than the seven listed above.

`temperature` has no minimum or maximum in this contract. Do not add commodity-specific limits here.

The schema sets both `format: date-time` and an explicit pattern so a validator that does not assert `format` still rejects malformed timestamps.

## Units that still need a decision

1. **Temperature.** The sample value `8.5` is only an example. The team must record the unit (for example a named scale) in a later contract revision before dbt or Superset treats readings as comparable.
2. **Vibration.** The sample value `0.35` has no agreed physical unit. Sam, Roshan, and Nisha need to agree on the unit before abnormal-scenario thresholds or spoilage features use this field.
3. **Humidity.** Bounds of 0 and 100 match a percentage. This document treats humidity as that percentage scale. If the simulator uses another scale, the schema and this document must change together.

## Change process

1. Propose the change against `shared/schemas/sensor_reading.schema.json` and this document in the same update.
2. Roshan coordinates the review because the consumer and Snowflake raw table depend on the contract.
3. Sam confirms the simulator can emit the new shape.
4. Nisha confirms staging models can accept it.
5. Do not publish a new shape to `container-sensor-data` until the schema and the raw table design match.

## Non-goals

- No spoilage threshold per commodity.
- No price, route, or recommendation fields on the sensor event.
- No Kafka headers, keys, or offset metadata in the JSON body. Those may be added later as ingestion columns, not as required event fields.
