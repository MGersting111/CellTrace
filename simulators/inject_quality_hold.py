"""Schreibt ein QualityHoldPlaced-Event Avro-serialisiert in die Outbox.

Stand-in für den Traceability Service (ADR-14): Das Event wird vor dem Commit gegen das
registrierte Schema serialisiert, Debezium reicht die Bytes nach mes.quality weiter.

Aufruf (vom Repo-Root):
    set -a; source infra/compose/.env; set +a
    python simulators/inject_quality_hold.py P-0088 TEMP_OUT_OF_RANGE
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "mes.quality"
SCHEMA = (Path(__file__).parent.parent / "schemas" / "mes_quality.avsc").read_text()
PAYLOAD_TYPE = "io.celltrace.events.QualityHoldPlacedPayload"


def build_event(event_id: uuid.UUID, serial: str, reason_code: str) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "QualityHoldPlaced",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc),
        "correlation_id": str(uuid.uuid4()),
        "causation_id": None,
        "producer": "simulator",
        "actor": "service:simulator",
        # "-type" wählt den Union-Zweig (fastavro-Konvention)
        "payload": {
            "-type": PAYLOAD_TYPE,
            "hold_id": str(uuid.uuid4()),
            "serial": serial,
            "reason_code": reason_code,
        },
    }


def main(serial: str, reason_code: str) -> None:
    registry = SchemaRegistryClient({"url": os.environ.get("REGISTRY", "http://localhost:8081")})
    serializer = AvroSerializer(
        registry,
        SCHEMA,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )

    event_id = uuid.uuid4()
    value = serializer(
        build_event(event_id, serial, reason_code),
        SerializationContext(TOPIC, MessageField.VALUE),
    )

    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        port=int(os.environ.get("POSTGRES_PORT", "5433")),
    ) as conn:
        conn.execute(
            "INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload) "
            "VALUES (%s, %s, %s, %s, %s)",
            (event_id, "quality", serial, "QualityHoldPlaced", value),
        )
    print(f"outbox: QualityHoldPlaced {serial} ({event_id})")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: inject_quality_hold.py <serial> <reason_code>")
    main(sys.argv[1], sys.argv[2])
