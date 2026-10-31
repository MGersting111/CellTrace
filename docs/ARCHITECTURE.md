# CellTrace – Datenplattform für die eBike-Akkupack-Montage

Lernprojekt für Data- und Software-Architektur: ein OLTP-System zur Rückverfolgbarkeit, Echtzeit-Streaming von IoT-Daten und ein OLAP-Warehouse in **einer** fachlich zusammenhängenden Domäne. Alles läuft lokal.

> **Die Story in einem Satz:** Ein Sensor im Klimalager meldet eine Abweichung. Das System sperrt automatisch alle betroffenen Akkupacks, der Werker sieht den Alarm sofort auf dem iPad, und das Qualitätsmanagement kann später jeden Akku bis zur einzelnen Zelle zurückverfolgen.

Diagramme: `docs/diagrams/architektur.excalidraw` (Gesamtsystem), `docs/diagrams/kern-loop.excalidraw` (Datenfluss der Qualitätssperre)
Datenmodelle (OLTP, Event-Katalog, OLAP): `docs/data-model.md`

---

## 1. Domäne: eBike-Akkupack-Montage

Der Hersteller kauft Zellen (Typ 21700) chargenweise zu und montiert daraus Packs.
**Hierarchie:** 10 Zellen → 1 Modul, 4 Module + BMS → 1 Pack (40 Zellen, didaktisch vereinfacht)

| # | Station | Was passiert | Quelle |
|---|---|---|---|
| 1 | Wareneingang | Zellen scannen, Spannung und Innenwiderstand messen, Charge zuordnen | Simulator |
| 2 | Zellen-Matching | Zellen mit ähnlichen Werten gruppieren | Simulator |
| 3 | Modulbau | 10 Zellen verschweißen, ergibt ein Modul | Simulator |
| 4 | Pack-Montage | 4 Module + BMS + Gehäuse, ergibt einen Pack | Simulator |
| 5 | BMS-Flash | Firmware aufspielen (Job über Kafka) | Flash Worker |
| 6 | **Lagerung / Ruhetest** | 1–3 Tage im Klimaraum, danach Selbstentladungsprüfung | **Arduino (echt)** |
| 7 | End-of-Line-Test | Laden/Entladen, Isolation, Pass/Fail | Simulator |
| 8 | Versand | Auslieferung an den Fahrradhersteller | Simulator |

**Arduino an Station 6** deckt alle drei OEE-Faktoren ab:

| Sensor | Bedeutung | OEE-Faktor |
|---|---|---|
| HC-SR04 Ultraschall | Pack kommt an oder geht → Takt, Stückzahl | Leistung |
| HC-SR501 PIR | Person in der Sicherheitszone → Stillstand | Verfügbarkeit |
| DHT11 Temperatur/Feuchte | Klima außerhalb der Toleranz → Sperre | Qualität |

> 💬 *Interview:* "Telemetrie und Transaktionen haben völlig unterschiedliche Eigenschaften. Telemetrie geht in Kafka und in Zeitreihen, Buchungen gehen in eine transaktionale Datenbank. Die Domäne verbindet beide über die Qualitätssperre."

---

## 2. Komponenten

| Komponente | Technik | Verantwortung | Besitzt |
|---|---|---|---|
| Arduino | C++ | Sensoren auslesen, über Serial senden | – |
| Edge Gateway | Python (später Rust) | Serial lesen, Event-Time und Seq-Nr. stempeln, puffern, nach Kafka senden | SQLite-Puffer |
| Stations-Simulatoren | Python | Stationen 1–8 simulieren, Last erzeugen, Fehler injizieren | – |
| Traceability Service | Python / FastAPI | MES-Kern: Teile, Einbauten, Prüfungen, Sperren. **Einziger Schreiber ins OLTP** | Postgres |
| Flash Worker | Python | BMS-Flash-Jobs ausführen, mit Retry/DLQ | zustandslos |
| Flink Jobs | Flink SQL (bei Bedarf Java) | Deduplizieren, Anomalien erkennen, Sperren anfordern | Flink State |
| Terminal Backend | **Go** | WebSocket/SSE zur PWA, Werker-Aktionen weiterleiten | Redis |
| Werker-Terminal | PWA auf dem iPad | Live-Status, Alarme, Aktionen | – |
| Kafka + Schema Registry | Kafka, Avro | Event-Backbone, Data Contracts | Logs |
| Kafka Connect | Debezium, ClickHouse Sink | Outbox → Kafka, Kafka → ClickHouse | – |
| ClickHouse | ClickHouse | OLAP-Warehouse | Warehouse |
| dbt | dbt-clickhouse | Transformationen und Tests | – |
| Airflow | Airflow + astronomer-cosmos | Batch-Orchestrierung | – |
| Superset | Apache Superset | Analyse für QM und Werksleitung | – |
| Grafana + Prometheus | | Live-Sicht für Ops und Schichtleitung | – |
| Keycloak | | Login, Rollen | – |

**Zielgruppen und Latenzen:**

| Tool | Zielgruppe | Latenz |
|---|---|---|
| PWA | Werker am Band | Sekunden, Push |
| Grafana | Schichtleitung, Instandhaltung, Ops | Sekunden bis Minuten |
| Superset | Qualitätsmanagement, Werksleitung | Stunden bis Tage |

---

## 3. Kern-Loop: Qualitätssperre

1. DHT11 misst ein Klima außerhalb der Toleranz.
2. Das Edge Gateway stempelt `occurred_at` und `seq_no` und sendet an `telemetry.station.v1` (idempotenter Producer, Puffer bei Ausfall).
3. Flink dedupliziert per `(device_id, seq_no)` und arbeitet mit **Event-Time und Watermarks**. Die Regel: mindestens 10 Minuten außerhalb der Toleranz.
4. Flink joint die Anomalie mit den Lagerbelegungen aus `mes.passage` und sendet pro betroffenem Pack einen Command `QualityHoldRequested` an `cmd.quality-hold.v1`.
5. Der Traceability Service prüft die Geschäftsregeln (Pack schon versendet? Schon gesperrt?), setzt die Sperre und schreibt das Event in die **Outbox, in derselben Transaktion**.
6. Debezium (Outbox Event Router) publiziert `QualityHoldPlaced` an `mes.quality`.
7. Das Terminal Backend aktualisiert Redis und pusht per WebSocket, das iPad zeigt den Alarm.
8. Parallel landet das Event über den ClickHouse Sink im Warehouse und erscheint in Superset (Sperrquote, Reaktionszeit).
9. QM hebt die Sperre über das iPad auf (Rolle `qualitaet`, Begründung Pflicht). Das läuft wieder über API, Outbox und Kafka.

**Showcase-Kennzahl:** *Detection-to-Terminal-Latenz*, also Sensorabweichung bis iPad-Alarm. Ziel: **p95 < 5 s**.

---

## 4. OLTP-Design (Postgres)

**Genealogie als append-only Einbau-Historie:**

```sql
part(
  serial      text PRIMARY KEY,         -- 'C-4711', 'M-0123', 'P-0088'
  part_type   text NOT NULL,            -- CELL | MODULE | PACK
  batch_id    text NULL                 -- nur Zellen: Lieferanten-Charge
);

component_installation(
  id             bigserial PRIMARY KEY,
  parent_serial  text NOT NULL REFERENCES part,
  child_serial   text NOT NULL REFERENCES part,
  installed_at   timestamptz NOT NULL,
  removed_at     timestamptz NULL,      -- NULL = steckt noch drin
  station_id     text NOT NULL
);

-- Eine Zelle steckt nie gleichzeitig in zwei Modulen. Das erzwingt die DB, nicht der Code.
CREATE UNIQUE INDEX one_active_parent
  ON component_installation(child_serial) WHERE removed_at IS NULL;
```

- Einbau ist ein `INSERT`, Ausbau ein `UPDATE removed_at`. Es wird nie gelöscht.
- Die Rückruf-Query ist eine rekursive CTE mit Zeitbezug: *"Was war zum Auslieferungszeitpunkt verbaut?"*

**Nicht-funktionale Anforderungen (per Lasttest mit k6 oder Locust belegt):**

| Anforderung | Ziel |
|---|---|
| Durchsatz | ~1.000 Buchungen/s dauerhaft, Spitze bis 3.000/s |
| Latenz Scan-Buchung | p99 < 50 ms |
| Korrektheit | 0 verlorene, 0 doppelte Einbauten |
| Konsistenz | keine Zelle in zwei Modulen, kein gesperrter Pack wird versendet |

**Techniken:** Idempotency Keys pro API-Call · kurze Transaktionen mit Constraints · PgBouncer · Batch-Inserts für Messwerte · Optimistic Locking · Partitionierung von `test_result` nach Zeit

---

## 5. Events und Data Contracts

**Einheitlicher Envelope für jedes Event, ab Tag 1:**

```json
{
  "event_id": "uuid",            // Deduplizierung
  "event_type": "QualityHoldPlaced",
  "schema_version": 1,
  "occurred_at": "2026-...",     // Event-Time, nicht Processing-Time
  "correlation_id": "uuid",      // zieht sich durch die ganze Kette, später Trace-ID
  "causation_id": "uuid",        // welches Event hat dieses ausgelöst?
  "producer": "traceability-service",
  "actor": "user:qm-anna | service:flink",   // Audit
  "payload": { }
}
```

**Topics** (verwaltet ab Phase 3 per Terraform):

| Topic | Art | Producer → Consumer |
|---|---|---|
| `telemetry.station.v1` | Event | Gateway → Flink, ClickHouse, Terminal Backend |
| `mes.part`, `mes.installation`, `mes.passage`, `mes.test`, `mes.quality`, `mes.shipment` | Event (Outbox), Key `serial` | Debezium → Flink, Terminal Backend, ClickHouse |
| `mes.masterdata` | Event (Outbox), Stammdaten für SCD2 | Debezium → ClickHouse |
| `cmd.quality-hold.v1` | Command | Flink → Traceability Service |
| `cmd.bms-flash.v1` (+ `.retry`, `.dlq`) | Command | Traceability (über Outbox) → Flash Worker |
| `evt.bms-flash-result.v1` | Event | Flash Worker → Traceability Service |

**Schema-Regeln:** Avro, Kompatibilität `BACKWARD`. Neue Felder nur mit Default.

---

## 6. Streaming (Flink)

- **Flink trifft Entscheidungen** (Deduplizierung, Anomalie, Sperr-Command), **ClickHouse-Materialized-Views aggregieren** (Ø pro Minute für Grafana).
- Event-Time, Watermarks und Allowed Lateness, weil Store-and-Forward im Gateway späte Events erzeugt.
- Flink schreibt **nie** direkt in Postgres, sondern sendet nur Commands.

---

## 7. OLAP (ClickHouse + dbt)

```
Kafka ──ClickHouse Sink──▶ raw_*      ReplacingMergeTree, dedupliziert per event_id
                             │ dbt
                           staging_*  typisiert, ein Modell pro Event-Typ
                             │ dbt
                           core       Snowflake-Schema
                             │ dbt
                           marts      flache, breite Tabellen für Superset und Grafana
```

| Fakt | Grain (eine Zeile = ...) | Beantwortet |
|---|---|---|
| `fct_station_passage` | ein Teil durchläuft eine Station | OEE, Taktzeit, Durchlaufzeit |
| `fct_test_result` | ein Messwert einer Prüfung | Ausschuss, Prüfstatistik |
| `fct_quality_hold` | eine Sperre | Sperrquote, Reaktionszeit |
| `fct_telemetry_1min` | Station × Minute | Klima vs. Qualität |
| `bridge_genealogy` | Zelle → Pack mit Gültigkeitszeitraum | Rückruf ohne Rekursion |

**Dimensionen (Snowflake):** `dim_station → dim_line → dim_plant` · `dim_cell_batch → dim_supplier` · `dim_part → dim_part_type` · `dim_shift`, `dim_date`, `dim_time`
**SCD2 aus Events** (z. B. `StationConfigChanged` → `valid_from`/`valid_to`), weil es keine Tabellen für `dbt snapshot` gibt.

---

## 8. Orchestrierung (Airflow)

| DAG | Takt | Zweck |
|---|---|---|
| `dbt_transform` | stündlich | `dbt build` über cosmos. Testfehler stoppen abhängige Marts |
| `pipeline_reconciliation` | stündlich | Events in Outbox vs. ClickHouse raw zählen → beweist "0 verloren" |
| `contract_check` | täglich + CI | Avro-Schemas vs. dbt-Sources |
| `backfill_replay` | manuell | Zeitraum aus Kafka neu laden, Partitionen neu bauen |

---

## 9. Security (Basis-Hygiene)

- **Phase 1:** Keycloak mit den Rollen `werker`, `schichtleiter`, `qualitaet`. Sperre setzen dürfen alle, **aufheben nur `qualitaet`** mit Begründung. Audit über das Feld `actor` in den unveränderlichen Events.
- **Später:** Kafka mTLS und ACLs (Gateway darf nur auf `telemetry.*` schreiben), NetworkPolicies für eine OT-Zone, Sealed Secrets.

---

## 10. Infrastruktur

| Phase | Umgebung |
|---|---|
| 0–2 | **Docker Compose**: gesamter Stack, ein `docker compose up` |
| 3 | **k3d** mit Operatoren (Strimzi, Flink Operator, Altinity ClickHouse, CloudNativePG, Airflow Helm) |
| 3 | **Terraform** für die Plattform (Namespaces, Operatoren, Kafka-Topics, Schema-Regeln) |
| 3 | **ArgoCD** für Anwendungen (GitOps), **GitHub Actions** für Tests, Image-Builds, `dbt build`, Contract-Checks |

Rechne mit 12–16 GB RAM für den vollen Stack.

---

## 11. Architekturentscheidungen (ADRs)

Format: **Entscheidung** · *Warum (Interview-Argument)* · Trade-off

**ADR-01 Domäne eBike-Akkupack**
Fertigung mit Zellen-Genealogie. · *Das ist eine echte Rückruf-Problematik (Batteriepass, Produkthaftung), und die Hierarchie ist tief genug, dass OLTP- und OLAP-Modellierung nicht trivial werden.* · Die Domäne muss man sich erst erarbeiten.

**ADR-02 Kafka als einziger Broker, kein RabbitMQ**
Events *und* Commands über Kafka. · *Debezium braucht Kafka Connect, Flink und das Warehouse brauchen ein persistentes, wiederabspielbares Log. Ein zweiter Broker bedeutet doppelten Betrieb ohne fachlichen Mehrwert.* · Kafka hat keine Retries pro Nachricht. Lösung: Retry- und DLQ-Topics.

**ADR-03 Edge Gateway statt direkter Geräteanbindung**
Der Arduino spricht Serial, das Gateway puffert und sendet. · *So läuft es auch in echten Fabriken: Maschinen hängen hinter einem Gateway (in echt: OPC UA oder MQTT). Store-and-Forward verhindert Datenverlust bei Netzausfall.* · Erzeugt Duplikate und späte Events. Daher Sequenznummern und Event-Time.

**ADR-04 Append-only Einbau-Historie**
Statt Fremdschlüssel `cell.module_id`. · *Ein einfacher FK überschreibt bei Nacharbeit die Historie, und Rückverfolgbarkeit wäre dann falsch. Der Partial Unique Index lässt die DB eine physikalische Regel erzwingen.* · Rekursive Queries sind komplexer. Im OLAP gelöst durch eine vorberechnete Bridge.

**ADR-05 Transactional Outbox + Debezium, Event-only Warehouse**
Keine Table-CDC. · *Interne Tabellen bleiben privat, das Event ist der Vertrag. Kein Dual Write. Eine einzige Wahrheit für Services und Warehouse.* · Alles, was die Analyse braucht, muss als Event designt werden. Das ist Mehraufwand, aber genau der Kern von Data Contracts.

**ADR-06 Flink für Entscheidungen, ClickHouse-MVs für Aggregation**
· *Die Sperre braucht zustandsbehaftete Verarbeitung mit Event-Time-Fenstern und einen Stream-Join. Dafür ist Flink der Standard. Reine Aggregation kann ClickHouse günstiger.* · Flink ist ressourcenhungrig und hat eine steile Lernkurve.

**ADR-07 Single Writer: Flink schreibt nie ins OLTP**
Flink sendet Commands, der Traceability Service entscheidet. · *Geschäftsregeln liegen an einer Stelle, und Änderungen fließen automatisch per Outbox überall hin. Kein verteilter Monolith.* · Ein zusätzlicher Hop erhöht die Latenz.

**ADR-08 ClickHouse, Snowflake-Core mit flachen Marts**
· *Läuft lokal, kann Echtzeit-Ingest, hat native Anbindung an Grafana und Superset. Snowflake-Schema für saubere Modellierung, flache Marts, weil ClickHouse breite Tabellen mag.* · Mehr dbt-Schichten.

**ADR-09 Superset statt PowerBI**
· *Konsequent lokal und Open Source, SQL-first, einbettbar. PowerBI Desktop läuft nur unter Windows, und der Cloud-Service bräuchte ein Gateway zum lokalen ClickHouse.* · Weniger verbreitet in Fachabteilungen als PowerBI.

**ADR-10 Python als Basis, Go fürs Terminal Backend, Rust später**
· *Die OLTP-Performance hängt an der DB, nicht an der Sprache. Go genau dort einsetzen, wo es glänzt: viele parallele WebSocket-Verbindungen. Rust-Rewrite des Gateways mit Benchmark als gezielte Entscheidung statt Hype.* · Polyglotter Stack.

**ADR-11 Airflow für Batch-Korrektheit, nicht für Streaming**
· *Echtzeit macht Flink. Airflow übernimmt, was Orchestrierung braucht: Abhängigkeiten, Abgleich, Backfills.* · Ein weiterer schwergewichtiger Dienst.

**ADR-12 Compose zuerst, Kubernetes ab Phase 3**
· *Fachlogik vor Plattform. Kubernetes dann mit Operatoren, um zustandsbehaftete Systeme zu betreiben, nicht nur Web-Apps.* · Zwei Deployment-Varianten pflegen.

**ADR-13 Walking Skeleton zuerst**
· *Ein dünner Durchstich beweist früh, dass die Architektur trägt, und ab dann ist jederzeit etwas vorzeigbar.* · Phase 0 ist bewusst unvollständig.

---

## 12. Roadmap

| Phase | Ziel | Fertig wenn |
|---|---|---|
| **0 Walking Skeleton** | Kern-Loop einmal komplett | Klimaabweichung am Arduino führt zum Alarm auf dem iPad |
| **1 OLTP-Kern** | Rückverfolgbarkeit und Performance | 8 Stationen simuliert, Genealogie mit Rework, Lasttest-Report, Keycloak |
| **2 OLAP** | Analyse | dbt (Core, Marts, Tests), Airflow-DAGs, Superset, Rückruf-Bridge |
| **3 Betrieb** | Plattform | k3d + Operatoren, Terraform, ArgoCD, CI/CD, Ops-Dashboards |
| **4 Erweiterungen** | Tiefe | OpenTelemetry-Tracing, Rust-Gateway + Benchmark, Kafka-Security |

**Regel:** Jede Phase endet mit einer laufenden Demo, einem README-Abschnitt und den neuen ADRs.

### Phase-0-Backlog

- [ ] Repo-Struktur und `docker-compose.yml`: Kafka (KRaft), Schema Registry, Kafka Connect, Postgres, Flink, Redis
- [ ] Arduino-Sketch: DHT11 + HC-SR04 + PIR als JSON-Zeilen über Serial
- [ ] Event-Envelope als Avro-Schema, `telemetry` registrieren
- [ ] Edge Gateway: Serial lesen → Envelope → Kafka (idempotenter Producer, Seq-Nr.)
- [ ] Postgres-Schema minimal: `part`, `passage`, `quality_hold`, `outbox`
- [ ] Traceability Service: `POST /passages` (Pack ein-/auslagern) und Consumer für `cmd.quality-hold.v1`, setzt Sperre + Outbox in 1 TX
- [ ] Debezium-Connector mit Outbox Event Router
- [ ] Mini-Simulator: lagert ein paar Packs ein
- [ ] Flink SQL: Telemetrie-Fenster + Join mit `mes.passage` → `cmd.quality-hold.v1`
- [ ] Terminal Backend (Go): Consumer für `mes.quality` → WebSocket
- [ ] Minimal-PWA: Liste der Sperren, live aktualisiert
- [ ] Demo: DHT11 anhauchen bzw. erwärmen, Alarm erscheint auf dem iPad. Latenz messen.

---

## 13. Offene Punkte

- Simulator-Design: Taktzeiten, Fehlerraten, Fehlerinjektion für Demos
- PWA-Framework und Offline-Verhalten bei WLAN-Abbruch
- Retention und TTL in Kafka und ClickHouse
- Flink-Regeln: Schwellwerte, Fensterlängen, Sensor-Ausfall vs. echte Anomalie
