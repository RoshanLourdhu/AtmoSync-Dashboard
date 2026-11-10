from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event = {
    "event_id": "TEST001",
    "container_id": "C001",
    "commodity": "Avocados",
    "temperature_c": 5.54,
    "humidity_pct": 86.10,
    "vibration_g": 0.125,
    "sensor_status": "NORMAL"
}

producer.send("atmosync-sensor-data", event)
producer.flush()
producer.close()

print("Test event sent to Kafka successfully!")
