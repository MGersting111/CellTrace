# CellTrace – Datenmodelle

Drei Modelle, drei Zwecke:

| Modell | Zweck | Quelle der Wahrheit |
|---|---|---|
| 1. OLTP (Postgres) | Schreiben: konsistent, schnell, rückverfolgbar | Traceability Service |
| 2. Event-Katalog | Öffentlicher Vertrag zwischen allen Komponenten | Avro-Schemas in `schemas/` |
| 3. OLAP (ClickHouse) | Lesen: Analyse, OEE, Rückruf | dbt-Modelle in `warehouse/dbt/` |

> 💬 *Interview:* "Das OLTP-Modell ist privat und darf sich ändern. Der Event-Katalog ist der Vertrag. Das Warehouse kennt nur Events, nie Tabellen."

---

## 1. OLTP-Modell (Postgres)

```mermaid
erDiagram
    supplier ||--o{ cell_batch : liefert
    cell_batch ||--o{ part : "enthält Zellen"
    line ||--o{ station : hat
    part ||--o{ component_installation : "ist Parent"
    part ||--o{ component_installation : "ist Child"
    station ||--o{ component_installation : "verbaut an"
    part ||--o{ passage : durchläuft
    station ||--o{ passage : "an Station"
    part ||--o{ test_result : "wird geprüft"
    station ||--o{ test_result : "geprüft an"
    part ||--o{ quality_hold : "wird gesperrt"
    part ||--o{ bms_flash_job : "wird geflasht"
    shipment ||--|{ shipment_item : enthält
    part ||--o| shipment_item : "wird versendet"

    supplier {
        text supplier_id PK
        text name
    }
    cell_batch {
        text batch_id PK
        text supplier_id FK
        text cell_type "z.B. 21700"
        timestamptz received_at
    }
    line {
        text line_id PK
        text plant_id
        text name
    }
    station {
        text station_id PK
        text line_id FK
        text station_type "INBOUND | MATCHING | MODULE | PACK | FLASH | STORAGE | EOL | SHIPPING"
        jsonb config "Toleranzen, Sollwerte"
        int version "Optimistic Locking"
    }
    part {
        text serial PK "C-4711, M-0123, P-0088"
        text part_type "CELL | MODULE | PACK"
        text batch_id FK "nur CELL"
        text status "OK | HOLD | SCRAPPED | SHIPPED"
        timestamptz created_at
        int version "Optimistic Locking"
    }
    component_installation {
        bigint id PK
        text parent_serial FK
        text child_serial FK
        text station_id FK
        timestamptz installed_at
        timestamptz removed_at "NULL = verbaut"
        text removal_reason "nur bei Rework"
    }
    passage {
        bigint id PK
        text serial FK
        text station_id FK
        timestamptz entered_at
        timestamptz left_at "NULL = noch an Station"
        text result "PASS | FAIL | REWORK"
    }
    test_result {
        bigint id PK "partitioniert nach measured_at"
        text serial FK
        text station_id FK
        text test_type "OCV | IR | INSULATION | SELF_DISCHARGE"
        numeric value
        text unit
        numeric lower_limit
        numeric upper_limit
        bool passed
        timestamptz measured_at
    }
    quality_hold {
        uuid hold_id PK
        text serial FK
        text reason_code
        timestamptz placed_at
        text placed_by "user:... | service:flink"
        uuid causation_event_id "auslösendes Event"
        timestamptz released_at "NULL = aktiv"
        text released_by "nur Rolle qualitaet"
        text release_comment
    }
    bms_flash_job {
        uuid job_id PK
        text serial FK
        text firmware_version
        text status "REQUESTED | DONE | FAILED"
        int attempts
        timestamptz updated_at
    }
    shipment {
        uuid shipment_id PK
        text customer
        timestamptz shipped_at
    }
    shipment_item {
        uuid shipment_id PK, FK
        text serial PK, FK "UNIQUE: ein Pack nur einmal"
    }
    outbox {
        uuid id PK "= event_id"
        text aggregatetype "routet ins Topic mes.<x>"
        text aggregateid "= Kafka-Key (serial)"
        text type "Event-Typ"
        bytea payload "Envelope + Payload"
        timestamptz created_at
    }
    idempotency_key {
        text key PK "vom Client gesendet"
        text request_hash
        jsonb response "gecachte Antwort"
        timestamptz created_at "TTL per Cleanup-Job"
    }
```

`outbox` und `idempotency_key` sind technische Tabellen ohne Beziehungen zu den anderen.

**Constraints, die die DB erzwingt (nicht der Code):**

```sql
-- Ein Child steckt nie in zwei Parents gleichzeitig
CREATE UNIQUE INDEX ux_one_active_parent
  ON component_installation (child_serial) WHERE removed_at IS NULL;

-- Höchstens eine aktive Sperre pro Teil
CREATE UNIQUE INDEX ux_one_active_hold
  ON quality_hold (serial) WHERE released_at IS NULL;

-- Ausbau nicht vor Einbau
ALTER TABLE component_installation
  ADD CHECK (removed_at IS NULL OR removed_at >= installed_at);

-- Ein Teil ist höchstens einmal gleichzeitig an einer Station
CREATE UNIQUE INDEX ux_one_open_passage
  ON passage (serial) WHERE left_at IS NULL;
```

**Designentscheidungen:**
- **`part` als eine Tabelle für alle Ebenen** statt `cell`, `module`, `pack`: Die Genealogie wird damit eine einzige selbstreferenzierende Struktur. Ebenenregeln (Zelle nur in Modul, Modul nur in Pack) prüft der Service.
- **`part.status` ist denormalisiert** (eigentlich aus `quality_hold` und `shipment_item` ableitbar). Er dient dem Hot Path: "Darf dieser Pack weiter?" ist eine Primärschlüssel-Abfrage statt eines Joins. Der Status wird in derselben Transaktion gepflegt wie die Quelle.
- **Kein gesperrter Pack wird versendet:** Der Versand liest `part` mit `version` und setzt `SHIPPED` nur, wenn sich `version` nicht geändert hat (Optimistic Locking). Setzt Flink parallel eine Sperre, schlägt einer von beiden fehl und wiederholt den Vorgang.
- **`test_result` wird nach Zeit partitioniert**, weil sie die mit Abstand größte Tabelle ist. Alte Partitionen können günstig archiviert werden.
- **`outbox` folgt den Spaltennamen des Debezium Outbox Event Routers** (`aggregatetype`, `aggregateid`, `type`, `payload`), sodass keine Mapping-Konfiguration nötig ist.

---

## 2. Event-Katalog (Data Contracts)

**Fluss: Producer → Topic → Consumer**

```mermaid
flowchart LR
    subgraph Producer
        GW[Edge Gateway]
        DBZ[Debezium<br/>Outbox Router]
        FL1[Flink]
        FW1[Flash Worker]
    end

    subgraph Topics
        T1[[telemetry.station.v1<br/>key: device_id]]
        T2[[mes.part · mes.installation<br/>mes.passage · mes.test<br/>key: serial]]
        T3[[mes.quality · mes.shipment<br/>key: serial]]
        T4[[mes.masterdata<br/>key: entity_id]]
        T5[[cmd.quality-hold.v1<br/>key: serial]]
        T6[[cmd.bms-flash.v1<br/>+ .retry / .dlq]]
        T7[[evt.bms-flash-result.v1]]
    end

    subgraph Consumer
        FL2[Flink]
        TS[Traceability Service]
        TB[Terminal Backend]
        FW2[Flash Worker]
        CH[ClickHouse Sink]
    end

    GW --> T1
    DBZ --> T2 & T3 & T4 & T6
    FL1 --> T5
    FW1 --> T7

    T1 --> FL2 & TB & CH
    T2 --> FL2 & TB & CH
    T3 --> TB & CH
    T4 --> CH
    T5 --> TS
    T6 --> FW2
    T7 --> TS
```

**Schemas: Envelope + Payloads**

```mermaid
classDiagram
    class Envelope {
        uuid event_id
        string event_type
        int schema_version
        timestamp occurred_at
        uuid correlation_id
        uuid causation_id
        string producer
        string actor
        Payload payload
    }
    class TelemetryRecorded {
        string device_id
        string station_id
        long seq_no
        float temperature_c
        float humidity_pct
        float distance_cm
        boolean motion_detected
    }
    class ComponentInstalled {
        string parent_serial
        string child_serial
        string station_id
    }
    class ComponentRemoved {
        string parent_serial
        string child_serial
        string removal_reason
    }
    class StationEntered {
        string serial
        string station_id
    }
    class StationLeft {
        string serial
        string station_id
        string result
    }
    class TestResultRecorded {
        string serial
        string station_id
        string test_type
        decimal value
        decimal lower_limit
        decimal upper_limit
        boolean passed
    }
    class QualityHoldRequested {
        string serial
        string reason_code
        timestamp window_start
        timestamp window_end
    }
    class QualityHoldPlaced {
        uuid hold_id
        string serial
        string reason_code
    }
    class QualityHoldReleased {
        uuid hold_id
        string serial
        string release_comment
    }
    class PackShipped {
        uuid shipment_id
        string serial
        string customer
    }
    class StationConfigChanged {
        string station_id
        string line_id
        string station_type
        string config_json
    }
    Envelope *-- TelemetryRecorded
    Envelope *-- ComponentInstalled
    Envelope *-- ComponentRemoved
    Envelope *-- StationEntered
    Envelope *-- StationLeft
    Envelope *-- TestResultRecorded
    Envelope *-- QualityHoldRequested
    Envelope *-- QualityHoldPlaced
    Envelope *-- QualityHoldReleased
    Envelope *-- PackShipped
    Envelope *-- StationConfigChanged
```

Nicht dargestellt, gleiches Muster: `PartRegistered`, `BmsFlashRequested`, `BmsFlashCompleted`, `BatchReceived`.

**Phase 0 braucht nur:** `TelemetryRecorded`, `StationEntered`/`StationLeft`, `QualityHoldRequested`, `QualityHoldPlaced`.

**Designentscheidungen:**
- **Kafka-Key = `serial`** für alle `mes.*`-Topics: Alle Events eines Teils landen in derselben Partition und kommen **in Reihenfolge** an. `StationLeft` kann so nie vor `StationEntered` verarbeitet werden.
- **Telemetrie-Key = `device_id`**: Die Reihenfolge pro Sensor bleibt erhalten, was die Deduplizierung über `seq_no` vereinfacht.
- **Commands im Imperativ-Präfix `cmd.`, Events in der Vergangenheitsform**: `QualityHoldRequested` ist eine Bitte, `QualityHoldPlaced` eine Tatsache. Am Namen ist erkennbar, wer entscheidet.
- **Version im Topic-Namen nur bei Breaking Changes** (`.v1` → `.v2`). Kompatible Änderungen laufen über die Schema Registry (`BACKWARD`).
- **`mes.masterdata`** für Stammdaten (Station, Charge, Lieferant) ist die Grundlage für SCD2 im Warehouse.

---

## 3. OLAP-Modell (ClickHouse, Snowflake-Core)

```mermaid
erDiagram
    dim_plant ||--o{ dim_line : enthält
    dim_line ||--o{ dim_station : enthält
    dim_supplier ||--o{ dim_cell_batch : liefert
    dim_part_type ||--o{ dim_part : typisiert
    dim_cell_batch ||--o{ dim_part : "Charge der Zelle"

    dim_part ||--o{ fct_station_passage : ""
    dim_station ||--o{ fct_station_passage : ""
    dim_shift ||--o{ fct_station_passage : ""
    dim_date ||--o{ fct_station_passage : ""

    dim_part ||--o{ fct_test_result : ""
    dim_station ||--o{ fct_test_result : ""
    dim_date ||--o{ fct_test_result : ""

    dim_part ||--o{ fct_quality_hold : ""
    dim_date ||--o{ fct_quality_hold : ""

    dim_station ||--o{ fct_telemetry_1min : ""
    dim_date ||--o{ fct_telemetry_1min : ""

    dim_part ||--o{ bridge_genealogy : "als Pack"
    dim_part ||--o{ bridge_genealogy : "als Zelle"

    dim_plant {
        string plant_sk PK
        string plant_id
        string name
    }
    dim_line {
        string line_sk PK
        string plant_sk FK
        string line_id
    }
    dim_station {
        string station_sk PK "pro Version"
        string line_sk FK
        string station_id
        string station_type
        string config
        datetime valid_from "SCD2"
        datetime valid_to
        bool is_current
    }
    dim_supplier {
        string supplier_sk PK
        string name
    }
    dim_cell_batch {
        string batch_sk PK
        string supplier_sk FK
        string batch_id
        string cell_type
        datetime received_at
    }
    dim_part_type {
        string part_type_sk PK
        string part_type "CELL, MODULE, PACK"
    }
    dim_part {
        string serial PK
        string part_type_sk FK
        string batch_sk FK "nur Zellen"
    }
    dim_shift {
        string shift_sk PK
        string shift_name "Früh, Spät, Nacht"
    }
    dim_date {
        date date_sk PK
        int iso_week
        bool is_workday
    }
    fct_station_passage {
        string serial FK
        string station_sk FK
        date date_sk FK
        string shift_sk FK
        datetime entered_at
        datetime left_at
        int cycle_time_s
        string result
    }
    fct_test_result {
        string serial FK
        string station_sk FK
        date date_sk FK
        string test_type
        decimal value
        bool passed
    }
    fct_quality_hold {
        string hold_id
        string serial FK
        date date_sk FK
        string reason_code
        datetime placed_at
        datetime released_at
        int time_to_release_s
        string placed_by
    }
    fct_telemetry_1min {
        string station_sk FK
        date date_sk FK
        datetime minute
        float temp_avg
        float temp_max
        float humidity_avg
        int motion_events
        int objects_detected
    }
    bridge_genealogy {
        string pack_serial FK
        string module_serial
        string cell_serial FK
        datetime valid_from "eingebaut"
        datetime valid_to "ausgebaut, sonst offen"
    }
```

**Rückruf-Query im OLAP (ohne Rekursion):**

```sql
SELECT DISTINCT g.pack_serial
FROM bridge_genealogy g
JOIN dim_part c        ON c.serial = g.cell_serial
JOIN dim_cell_batch b  ON b.batch_sk = c.batch_sk
JOIN fct_station_passage s ON s.serial = g.pack_serial   -- Versandstation
WHERE b.batch_id = 'B-2026-117'
  AND s.station_sk IN (SELECT station_sk FROM dim_station WHERE station_type = 'SHIPPING')
  AND s.entered_at >= g.valid_from
  AND (g.valid_to IS NULL OR s.entered_at < g.valid_to);
```

**Designentscheidungen:**
- **Snowflake im Core, flach in den Marts:** Die Hierarchie Station → Linie → Werk bleibt normalisiert und pflegbar. Für Dashboards baut dbt flache Marts, z. B. `mart_oee_shift` (Station, Linie, Schicht, Verfügbarkeit, Leistung, Qualität, OEE in einer Zeile), weil ClickHouse breite Tabellen deutlich schneller liest als Joins.
- **Surrogate Keys (`_sk`) nur dort, wo SCD2 nötig ist oder IDs instabil sein könnten.** `dim_part` nutzt die Seriennummer direkt, weil sie fachlich eindeutig und unveränderlich ist.
- **SCD2 in `dim_station` aus `StationConfigChanged`-Events:** Eine Toleranzänderung an Station 6 ist historisch korrekt zugeordnet. "Warum wurde am 12.03. gesperrt?" wird mit der damals gültigen Toleranz beantwortet.
- **`bridge_genealogy` löst die OLTP-Rekursion einmal in dbt auf**, sodass der Rückruf ein einfacher Join wird.
- **`fct_telemetry_1min` statt Rohdaten im Core:** Die Rohtelemetrie bleibt in `raw_*` (mit TTL), aggregiert pro Minute speist sie die Analyse. Echtzeit-Aggregate für Grafana kommen aus einer ClickHouse-Materialized-View.
