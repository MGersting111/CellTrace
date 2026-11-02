# ADR-15 Ein Schema pro Topic, Payload als Union

**Status:** vorgeschlagen

## Kontext
`mes.passage` enthält `StationEntered` und `StationLeft`. Die Standard-Subject-Strategie der Registry (`TopicNameStrategy`, Subject `<topic>-value`) erlaubt ein Schema pro Topic. Beide Events müssen im selben Topic liegen, damit die Reihenfolge pro `serial` (Key) erhalten bleibt.

## Entscheidung
`TopicNameStrategy` bleibt. Pro Topic gibt es **ein** Envelope-Schema, dessen `payload` eine Avro-**Union** der Payload-Records des Topics ist (z. B. `[StationEnteredPayload, StationLeftPayload]`). `event_type` benennt den Zweig.

## Begründung
- Ordnung pro Teil bleibt (ein Topic).
- Subject-Schema bleibt überall gleich (`<topic>-value`), wie bei `telemetry`.
- Flink und ClickHouse-Sink erwarten standardmäßig ein Schema pro Topic.

## Trade-off
- Union erhöht die Komplexität beim Serialisieren (Zweig muss benannt werden).
- Ein neuer Event-Typ im Topic ist eine Schemaänderung (Zweig hinzufügen), die `BACKWARD` erlauben muss.
- Alternative `RecordNameStrategy` verworfen: Tooling-Support schwächer, Topic-Schema-Zuordnung geht verloren.
