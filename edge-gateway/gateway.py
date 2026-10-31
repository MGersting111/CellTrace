from pathlib import Path
import uuid
from datetime import datetime, timezone
import json
import random
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField


TOPIC = "telemetry.station.v1"
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "telemetry_recorded.avsc"
schema_str = SCHEMA_PATH.read_text()

registry = SchemaRegistryClient({"url": "http://localhost:8081"})

serializer = AvroSerializer(
    registry,
    schema_str,
    conf={
        "auto.register.schemas": False,   # nur das Skript/CI registriert
        "use.latest.version": True,       # nimm die registrierte Version
    },
)


producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True})

#expected dict: {"device_id": "arduino-01", "station_id": "S6", "temp": 22.5, "hum": 48.0, "dist": 120.0, "motion": false}
def build_event(seq_no: int, reading: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "TelemetryRecorded",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc),
        "correlation_id": str(uuid.uuid4()),
        "causation_id": None,
        "producer": "edge-gateway",
        "actor": "service:edge-gateway",
        "payload": {
            "device_id": reading.get("device_id", "arduino-01"),
            "station_id": reading.get("station_id", "S6"),
            "seq_no": seq_no,
            "temperature_c": reading.get("temp"),
            "humidity_pct": reading.get("hum"),
            "distance_cm": reading.get("dist"),
            "motion_detected": reading.get("motion"),
        },
    }

def fake_reading()->dict:
    return {
        "device_id": "arduino-01",
        "station_id": "S6",
        "temp": random.uniform(20.0, 30.0),
        "hum": random.uniform(30.0, 70.0),
        "dist": random.uniform(100.0, 150.0),
        "motion": random.choice([True, False]),
    }


def on_delivery(err, msg):
    if err is not None:
        print(f"FEHLER: {err}")
    else:
        print(f"ok: {msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def produce():
    for seq_no in range(1, 11):
        reading = fake_reading()
        event = build_event(seq_no, reading)
        print(f"Producing event: {event}")
        producer.produce(
            topic=TOPIC,
            key=event["payload"]["device_id"],
            value=serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=on_delivery,
        )
        producer.flush()

#MAIN
produce()
