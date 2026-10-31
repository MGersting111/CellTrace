from pathlib import Path
import uuid
from datetime import datetime, timezone
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField


#1. Schema aus schemas/telemetry_recorded.avsc laden (Datei lesen)

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "telemetry_recorded.avsc"
schema_str = SCHEMA_PATH.read_text()

#2. SchemaRegistryClient(url=...)  →  AvroSerializer(registry, schema_str, to_dict-Funktion)
registry = SchemaRegistryClient({"url": "http://localhost:8081"})

serializer = AvroSerializer(
    registry,
    schema_str,
    conf={
        "auto.register.schemas": False,   # nur das Skript/CI registriert
        "use.latest.version": True,       # nimm die registrierte Version
    },
)

#3. Producer({"bootstrap.servers": "localhost:9092"})

producer = Producer({"bootstrap.servers": "localhost:9092"})

#4. Event-Dict bauen: Envelope-Felder + payload-Dict

event = {
    "event_id": str(uuid.uuid4()),
    "event_type": "TelemetryRecorded",
    "schema_version": 1,
    "occurred_at": datetime.now(timezone.utc),
    "correlation_id": str(uuid.uuid4()),
    "causation_id": None,
    "producer": "edge-gateway",
    "actor": "service:edge-gateway",
    "payload": {
        "device_id": "arduino-01",
        "station_id": "S6",
        "seq_no": 1,
        "temperature_c": 22.5,
        "humidity_pct": 48.0,
        "distance_cm": 120.0,
        "motion_detected": False,
    },
}

#5. producer.produce(topic, key=device_id, value=serializer(event, SerializationContext(topic, MessageField.VALUE)))

TOPIC = "telemetry.station.v1"

def on_delivery(err, msg):
    if err is not None:
        print(f"FEHLER: {err}")
    else:
        print(f"ok: {msg.topic()} partition={msg.partition()} offset={msg.offset()}")

producer.produce(
    topic=TOPIC,
    key=event["payload"]["device_id"],
    value=serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
    on_delivery=on_delivery,
)

#6. producer.flush()
producer.flush()
