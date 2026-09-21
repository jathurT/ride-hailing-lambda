# 04 — Requirement 1: Ingestion & Kafka Design

> **PDF preferred stack:** *"Ingestion: Apache Kafka (**Producers, Topics, Partitions**)."* — the three concepts named in brackets are the ones the design must visibly exercise.
>
> **Rubric weight: 15 marks** (shared with `03`).

---

## 1. Topic design

| Topic | Partitions | RF | Key | Cleanup policy | Retention | Purpose |
|---|---|---|---|---|---|---|
| `fleet.telemetry.v1` | **6** | 1 | `vehicle_id` | `delete` | **7 days** | The raw event stream. Three independent consumer groups read it. |
| `fleet.vehicle.registry` | 3 | 1 | `vehicle_id` | **`compact`** | infinite | Slowly-changing vehicle reference data. Stream-static enrichment source. |
| `fleet.alerts.v1` | 3 | 1 | `vehicle_id` | `delete` | 7 days | **Business** alerts emitted by the speed layer. |
| `fleet.telemetry.dlq` | 1 | 1 | `vehicle_id` | `delete` | 14 days | Dead-letter queue for events that fail validation. |

Created declaratively by `scripts/create_topics.py` (idempotent, run by an init container), never by auto-creation — `auto.create.topics.enable=false`, so a typo in a topic name fails loudly instead of silently creating a topic with default settings.

### 1.1 Why 6 partitions on the telemetry topic

Three reasons, and the report should give all three because "6 seemed fine" is not an answer:

1. **Consumer parallelism ceiling.** The module: *"Topic `user_clicks` with 6 partitions → 3 consumers = 2 partitions each."* Partitions cap how many consumers in a group can work in parallel. Our Spark job runs 2 workers × 2 cores = 4 concurrent tasks; 6 partitions gives headroom to scale to 6 without re-partitioning (which is disruptive on a live topic).
2. **Key distribution.** 150 vehicle keys across 6 partitions ≈ 25 keys per partition. Kafka's default murmur2 hash spreads them evenly, so no partition is hot. Verified by a Kafka UI screenshot in the report showing near-equal per-partition message counts — the module explicitly warns about *"careful partition key selection needed to avoid hotspotting"*, so we show the evidence.
3. **Not more, because** each partition is a set of files per broker and a unit of overhead; 6 is comfortably enough at 240 events/s (each partition sees ~40 events/s, trivial).

### 1.2 Why `vehicle_id` is the partition key — the load-bearing decision

> The module: *"**Ordering Guarantee: Messages are ordered within a partition, not across partitions.** Partition Key: Ensures related messages (e.g., same `user_id`) always go to the same partition."*

Our idle-detection logic is a **per-vehicle state machine**. It must observe a vehicle's status transitions in the order they occurred:

```
V042: on_trip → on_trip → idle → idle → idle → idle   ⇒ idle for 4 pings, alert if ≥ 45 sim-min
```

If `V042`'s pings were spread across partitions, they could be consumed out of order and the state machine would compute nonsense — a vehicle would appear to alternate between idle and busy, and the idle timer would keep resetting. **Keying by `vehicle_id` is not a performance tuning choice; it is a correctness requirement of the business logic.** That framing is what earns the marks.

Alternatives we considered and rejected (put this in the report — it shows the choice was made, not defaulted to):

| Key | Why rejected |
|---|---|
| `zone_id` | Only 12 distinct values across 6 partitions, and demand is heavily skewed toward CBD zones → **hotspotting**, exactly what the module warns about. Also breaks per-vehicle ordering. |
| `trip_id` | High cardinality so distribution would be excellent, but a vehicle's `idle` pings have `trip_id = null`, so idle events could not be co-located with the trip events that precede them — which is precisely the sequence the idle detector needs. |
| `null` (round-robin) | Best possible balance, zero ordering guarantees. Would make the idle detector incorrect. |
| `driver_id` | Works today (1:1 with vehicle) but breaks the moment a driver swaps vehicles, which is a normal ride-hailing event. Keying on the entity the state is *about* (the vehicle) is the durable choice. |

### 1.3 Where log compaction is genuinely right — `fleet.vehicle.registry`

> The module: *"**Log Compaction: Keeps only the latest record per key.** Example: User profile changes → only latest name/email is retained. Benefit: **Kafka can act as a durable state store.**"*

Vehicle reference data (model, fuel type, capacity, home zone, current driver) changes rarely — a vehicle is re-assigned, a driver swaps, a vehicle is retired. We need the **current** value of each, forever, and we need it available to the streaming job for enrichment.

Compaction gives exactly that: the topic is a **table**, materialised from a log. Reading it from `earliest` yields one record per vehicle — the latest. This is the textbook use of the feature and it is worth a paragraph in the report because it demonstrates understanding of *why* the two cleanup policies exist.

Deliberately **not** compacted:
- `fleet.telemetry.v1` — we need every ping, not the latest per vehicle. Compacting it would destroy the entire dataset.
- `fleet.alerts.v1` — an alert history is a log, not a table; keeping only the latest alert per vehicle would lose the incident record.

### 1.4 Retention: why 7 days and not more

`retention.ms = 604800000` (7 days) — matching the module's own example (*"Time-based (7 days) or size-based (1GB)"*).

**This number is an architecture statement, and the report should say so.** Under Lambda, Kafka is a *buffer and a short-term replay window*, not the historical store — MinIO is. So retention only has to cover: (a) the longest plausible consumer outage, (b) enough history to replay a day if the master-dataset writer fails.

A Kappa system would need retention ≥ the entire historical requirement (30 days here), on broker-attached storage. **This is the concrete form of the cost argument in `01 §2.5`** — Lambda lets the expensive tier be small.

Storage check: 72,000 events/simulated-day × 288 simulated-days-per-real-day ≈ 20.7M events per real day at ~250 bytes Avro ≈ **5.2 GB/real-day**. Over 7 days that is ~36 GB — too much for a laptop. **Therefore, for the demo we set `retention.ms` to 1 hour and `retention.bytes` to 2 GB**, and state in the report that the 7-day figure is the *design* value while the demo uses a reduced one for laptop constraints. Being caught with a config that cannot possibly work is worse than declaring the simplification.

---

## 2. Serialization — Avro + Schema Registry

Justified in `02 §10`. Implementation notes:

- Schemas live in `src/fleet/common/schemas/*.avsc`, registered at start-up by `scripts/register_schemas.py`.
- Subject naming: `TopicNameStrategy` → `fleet.telemetry.v1-value`.
- **Compatibility mode: `BACKWARD`** — new consumers can read old data. This is the right choice for us because the streaming job is upgraded more often than the producer, and it is what permits adding a nullable field without breaking anything.
- The message **key** is a plain UTF-8 string (`vehicle_id`), not Avro. Keeping keys as strings makes them readable in Kafka UI and in `kafka-console-consumer`, and there is no evolution story needed for a key that is one field.
- `tests/contract/test_schema_evolution.py` registers v1, registers v2 (adds nullable `passenger_count`), and asserts the registry accepts it under `BACKWARD` and that a v1-serialised record deserialises under the v2 reader schema.

---

## 3. Producer configuration — and why each setting

```python
{
    "bootstrap.servers": settings.kafka_bootstrap,
    "enable.idempotence": True,      # ← module: "Idempotent Producers: Kafka assigns
                                     #   sequence numbers → prevents duplicates"
    "acks": "all",                   # ← wait for all in-sync replicas; with RF=1 this is
                                     #   the leader only, but the setting is correct for
                                     #   production and costs nothing here
    "retries": 10,
    "retry.backoff.ms": 200,
    "max.in.flight.requests.per.connection": 5,  # ← safe with idempotence enabled;
                                     #   Kafka preserves ordering despite pipelining
    "compression.type": "snappy",    # ← ~2× on Avro telemetry, cheap CPU
    "linger.ms": 20,                 # ← batch for 20 ms; at 240 ev/s that's ~5 events
                                     #   per batch — throughput without meaningful latency
    "batch.size": 65536,
    "queue.buffering.max.messages": 100000,
    "client.id": f"telemetry-producer-{producer_id}",
}
```

Every one of these is defensible in a viva. The two most likely questions:
- *"Why is `max.in.flight > 1` safe?"* — because `enable.idempotence=true` makes the broker deduplicate and reorder by sequence number, so pipelining does not break ordering. Without idempotence this would have to be 1.
- *"Why `acks=all` with RF=1?"* — it is a no-op today, but it is the correct production setting and leaving it as `acks=1` would be a latent bug when replication is added. We configure for the design, not the demo.

---

## 4. Consumer groups — three, deliberately

The speed layer runs **three independent Structured Streaming queries** off the same topic, each with its own consumer group and its own checkpoint:

| Query | Consumer group | Does | Why separate |
|---|---|---|---|
| `Q1_master_dataset` | `fleet-lake-writer` | Append validated raw events → MinIO Parquet | This is the **system of record**. It must not stop because Redis is down. Its failure is the most serious in the system. |
| `Q2_zone_aggregates` | `fleet-speed-agg` | Window + aggregate → Redis | Purely operational; can be restarted freely, losing nothing permanent. |
| `Q3_idle_detector` | `fleet-idle-state` | Stateful per-vehicle idle tracking → alerts topic | Holds state; recovery has a different profile from the stateless queries. |

**One query with three sinks would couple all three failure domains** — a Redis timeout would stall the master-dataset write. Three queries means three offsets, three checkpoints, three failure domains, and three separate consumer-lag series in Grafana so we can see *which* stage is falling behind. This is both better engineering and better observability, and it is a good report paragraph.

Cost: the topic is read three times. At 240 events/s that is irrelevant, and we say so rather than pretending there is no cost.

---

## 5. Delivery semantics — stated honestly

The module taught *"exactly once or at least once"* and Kafka's EOS/transactions. What we actually achieve:

| Path | Guarantee | Why |
|---|---|---|
| Producer → Kafka | **Exactly once** (per producer session) | `enable.idempotence=true` deduplicates broker-side by producer ID + sequence number |
| Kafka → MinIO Parquet (Q1) | **Effectively exactly once** | Structured Streaming's file sink uses a `_spark_metadata` commit log; a re-processed micro-batch does not double-commit files |
| Kafka → Redis (Q2) | **At least once, made idempotent** | `foreachBatch` may re-run a micro-batch after failure. Writes are `HSET` of a computed aggregate keyed by `(zone, window_end)` — **overwriting, not incrementing** — so replaying a batch produces the identical state. **This is why we never use `HINCRBY` for aggregates.** |
| Kafka → alerts topic (Q3) | **At least once** | Duplicate alerts possible after a failure. Deduplicated downstream by `alert_id = hash(vehicle_id, idle_start_sim_time)` — the same idle episode always produces the same ID. |
| Kafka → Kafka (DLQ) | **At least once** | Duplicates in the DLQ are harmless; it is a diagnostic store. |

**End-to-end exactly-once is not achieved**, and the report says so plainly in the limitations section. Achieving it would require Kafka transactions spanning the read and the sink write, which Structured Streaming supports only for Kafka sinks — not for Redis or arbitrary `foreachBatch` targets. **Idempotent sinks are the standard production answer, and that is what we implemented.** Claiming exactly-once would be false and is exactly the sort of thing a viva probes.

---

## 6. The dead-letter path

The PDF asks for *"cleaning"* as a meaningful transformation. Cleaning that silently drops data is not observable and is not good engineering. Every rejected event goes to `fleet.telemetry.dlq` with a diagnostic envelope:

```json
{
  "original_payload": "<base64 of the raw Avro bytes>",
  "rejection_reason": "IMPLAUSIBLE_SPEED",
  "rejection_detail": "speed_kmh=247.3 exceeds max 200",
  "validator": "speed_range_check",
  "rejected_at_sim": "2026-03-02T14:31:00Z",
  "rejected_at_real": "2026-08-04T18:22:07.412Z",
  "source_topic": "fleet.telemetry.v1",
  "source_partition": 3,
  "source_offset": 184722,
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

Carrying **partition and offset** means any DLQ record can be traced back to the exact position in the source log — which is the kind of detail that makes a pipeline debuggable and is worth calling out. Metrics: `events_dlq_total{reason}`. Alert: `HighDLQRate` when the DLQ ratio exceeds 5% over 5 minutes.

---

## 7. Making ingestion visible

| Where | What a reviewer sees |
|---|---|
| **Kafka UI** (`:8080`) | Topics list; per-partition message counts (proving even key distribution); live throughput; consumer groups with lag per partition; message browser with **Avro deserialised via Schema Registry** so payloads are readable |
| **Grafana → Pipeline Health** | `events_produced_total` rate, producer error rate, DLQ rate by reason, consumer lag per group |
| **Prometheus** (`:9090`) | Raw series, alert rule state |
| **Producer logs** | `docker compose logs -f telemetry-producer` — JSON lines with `stage="ingest"` |

---

## 8. Implementation checklist

- [ ] `scripts/create_topics.py` — idempotent, declarative, config-driven; `auto.create.topics.enable=false` on the broker
- [ ] `scripts/register_schemas.py` — registers all `.avsc`, sets `BACKWARD` compatibility
- [ ] `common/kafka_client.py` — producer/consumer factories with the config above; single place where Kafka settings are constructed
- [ ] `common/serialization.py` — Avro serialiser/deserialiser wrappers over `confluent-kafka`'s `AvroSerializer`
- [ ] `common/dlq.py` — `reject(event, reason, detail, source_meta)` → DLQ envelope + metric + log
- [ ] Producer config exactly as §3, with an inline comment on each non-obvious setting
- [ ] OTel `traceparent` injected into Kafka message headers on every send
- [ ] `make peek TOPIC=fleet.telemetry.v1` — CLI helper that deserialises Avro to JSON
- [ ] `make lag` — prints consumer lag for all three groups
- [ ] Kafka UI wired to Schema Registry (`KAFKA_CLUSTERS_0_SCHEMAREGISTRY`)
- [ ] Tests: schema round-trip, schema evolution under `BACKWARD`, key-distribution test (10k events → no partition holds >25% of messages)
