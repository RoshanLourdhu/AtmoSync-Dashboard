import pandas as pd
from validator import validate_sensor_data

# Sample sensor dataset
data = {
    "container_id": ["C101", "C102", "C101", "C103"],
    "timestamp": [
        "2026-11-15 10:00:00",
        "2026-11-15 10:05:00",
        "2026-11-15 10:00:00",
        "invalid-date"
    ],
    "temperature": [25.5, 28.0, 25.5, 100.0],
    "humidity": [65, 70, 65, 120],
    "vibration": [2.1, 3.5, 2.1, 4.0]
}

df = pd.DataFrame(data)

result = validate_sensor_data(df)

print("\n===== ATMOSYNC DATA QUALITY REPORT =====")

for key, value in result.items():
    print(f"{key}: {value}")