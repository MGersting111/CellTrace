# ADR-14 Outbox-Payload als Avro-Bytes

**Status:** akzeptiert

## Kontext
`data-model.md` sah `outbox.payload` als `jsonb` vor. Debezium würde das als JSON-String nach Kafka reichen. Die `mes.*`-Topics wären damit nicht durch die Schema Registry abgesichert, obwohl Avro mit `BACKWARD`-Kompatibilität gilt.

## Entscheidung
`outbox.payload` ist `bytea` und enthält das vom Traceability Service mit dem `AvroSerializer` erzeugte Event (Confluent Wire Format). Debezium reicht die Bytes unverändert nach Kafka (`ByteArrayConverter`).

## Begründung
Der Vertrag wird dort erzwungen, wo das Event entsteht, vor dem Commit. Ein schemawidriges Event kann nicht in die Outbox gelangen. Alle Consumer lesen `mes.*` und `telemetry` einheitlich per Avro.

## Trade-off
- Outbox in Postgres nicht mehr als Klartext lesbar (Debugging über Tools).
- Der Service hängt beim Schreiben an der Registry (Schema-Cache entschärft das).
- `data-model.md` und Connector-Konfiguration müssen angepasst werden.
- Zu prüfen: Zusammenspiel von `EventRouter` und `ByteArrayConverter`.
