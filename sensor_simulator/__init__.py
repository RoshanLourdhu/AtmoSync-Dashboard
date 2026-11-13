"""
AtmoSync Sensor Simulator Package.

Provides realistic IoT container telemetry simulation with physical drift and anomaly models.
"""

from .simulator import (
    DEFAULT_CONTAINERS,
    ContainerState,
    SensorSimulator,
)

__all__ = [
    "DEFAULT_CONTAINERS",
    "ContainerState",
    "SensorSimulator",
]
