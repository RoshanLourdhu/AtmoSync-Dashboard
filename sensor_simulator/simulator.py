"""
AtmoSync Sensor Simulator Module.

Generates continuous IoT sensor readings for refrigerated transport containers.
Preserves Sam Roshan's continuous environmental physics, mean-reverting drift,
sensor noise, and transport anomaly injection, while standardizing emitted payloads
to strictly conform to `shared/schemas/sensor_reading.schema.json`.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from shared.validation import validate_sensor_event

# Baseline container registry with target parameters and shipment mappings
DEFAULT_CONTAINERS: list[dict[str, Any]] = [
    {
        "container_id": "C001",
        "shipment_id": "SHIP-1001",
        "commodity": "Avocados",
        "temp_target": 5.5,
        "temp_max": 8.0,
    },
    {
        "container_id": "C002",
        "shipment_id": "SHIP-1002",
        "commodity": "Mangoes",
        "temp_target": 8.0,
        "temp_max": 10.5,
    },
    {
        "container_id": "C003",
        "shipment_id": "SHIP-1003",
        "commodity": "Bananas",
        "temp_target": 13.0,
        "temp_max": 15.0,
    },
]


def clamp(value: float, low: float, high: float) -> float:
    """Clamps a floating-point value within [low, high]."""
    return max(low, min(high, value))


@dataclass
class ContainerState:
    temperature: float
    humidity: float
    vibration: float


class SensorSimulator:
    """
    Simulates real-time IoT environmental readings across monitored containers.
    Preserves Sam's physical simulation equations while strictly conforming
    to the official AtmoSync JSON schema.
    """

    def __init__(
        self,
        containers: list[dict[str, Any]] | None = None,
        initial_states: dict[str, ContainerState] | None = None,
    ) -> None:
        self.containers = containers or [dict(c) for c in DEFAULT_CONTAINERS]
        if initial_states is not None:
            self.state = initial_states
        else:
            self.state = {
                "C001": ContainerState(temperature=5.5, humidity=85.0, vibration=0.15),
                "C002": ContainerState(temperature=8.0, humidity=78.0, vibration=0.12),
                "C003": ContainerState(temperature=13.0, humidity=82.0, vibration=0.18),
            }
            # Initialize any unlisted container with baseline targets
            for c in self.containers:
                cid = c["container_id"]
                if cid not in self.state:
                    self.state[cid] = ContainerState(
                        temperature=float(c.get("temp_target", 8.0)),
                        humidity=80.0,
                        vibration=0.15,
                    )

    def generate_event(
        self,
        container: dict[str, Any],
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        """
        Advances the physical state for a single container and returns a schema-valid event.
        """
        cid = container["container_id"]
        st = self.state[cid]
        temp_target = float(container.get("temp_target", 8.0))

        # Mean-reverting environmental drift + sensor noise (Sam's dynamic equation)
        st.temperature += (temp_target - st.temperature) * 0.15 + random.uniform(-0.25, 0.3)
        st.humidity += random.uniform(-0.6, 0.6)
        st.vibration += (0.15 - st.vibration) * 0.25 + random.uniform(-0.02, 0.03)

        # Inject realistic transport anomalies
        anomaly = random.choices(
            ["NONE", "TEMP_SPIKE", "HIGH_VIBRATION", "SENSOR_FAULT"],
            weights=[82, 9, 6, 3],
        )[0]

        if anomaly == "TEMP_SPIKE":
            st.temperature += random.uniform(2.5, 4.5)
        elif anomaly == "HIGH_VIBRATION":
            st.vibration = random.uniform(0.32, 0.55)

        # Apply realistic physical boundary clamping
        st.temperature = round(clamp(st.temperature, 1.0, 25.0), 2)
        st.humidity = round(clamp(st.humidity, 45.0, 98.0), 2)
        st.vibration = round(clamp(st.vibration, 0.02, 1.0), 3)

        # Use RFC 3339 formatted UTC timestamp conforming to schema pattern
        if timestamp is None:
            event_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            event_timestamp = timestamp

        event = {
            "container_id": cid,
            "shipment_id": str(container["shipment_id"]),
            "commodity": str(container["commodity"]),
            "temperature": float(st.temperature),
            "humidity": float(st.humidity),
            "vibration": float(st.vibration),
            "timestamp": event_timestamp,
        }

        # Verify strict compliance
        validate_sensor_event(event)
        return event

    def generate_batch(
        self,
        timestamp: str | None = None,
    ) -> list[dict[str, Any]]:
        """Generates one event for each registered container."""
        return [
            self.generate_event(container, timestamp=timestamp)
            for container in self.containers
        ]
