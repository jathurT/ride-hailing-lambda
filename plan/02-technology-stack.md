# 02 — Technology Stack Selection & Justification

> **Rubric weight: 10 marks.** Assessed on *"appropriateness of chosen tools for each layer; justification tied to use-case constraints rather than **generic popularity**."*
>
> That last phrase is the whole brief. "Kafka is the industry standard for streaming" scores nothing. "We key by `vehicle_id` because idle-duration detection is a per-vehicle stateful computation and Kafka only guarantees ordering *within* a partition" scores.

---

## 1. The rule we write to

Every justification below answers three questions:
1. **What constraint in *this* scenario demands it?**
2. **What did we consider instead, and what would we have lost?**
3. **What trade-off are we accepting by choosing it?**

If a justification would read identically for the hospital use case or the smart-grid use case, it is not a justification — it is a description. Rewrite it.

---

## 2. Ingestion — Apache Kafka (KRaft mode)

**Image:** `confluentinc/cp-kafka:7.6.1` · single broker, KRaft (no ZooKeeper)

### Why Kafka for this scenario

| Constraint in this use case | How Kafka satisfies it |
|---|---|
| Telemetry is **unbounded and continuous** — 150 vehicles pinging forever | The module defines a stream as *"an append-only, ordered log of event records that are persisted over a longer duration"*, distinct from a message queue where messages are *"removed from a queue once delivered and consumed"*. We need persistence: the same events feed the speed layer, the master-dataset writer, and the idle detector — three independent consumers reading the same records. A queue cannot do this. |
| **Idle-duration detection is stateful per vehicle** — we track how long a vehicle has been continuously `idle`, which requires seeing its status transitions **in order** | Kafka guarantees ordering *within a partition*, not across partitions. Keying by `vehicle_id` puts all of one vehicle's events on one partition, so the state machine sees `on_trip → idle → idle → idle` in the correct sequence. **This is the single most important design decision in the ingestion layer and it is dictated by the business logic.** |
| Three consumers with **different failure characteristics** (a crashed Redis sink must not stop Parquet writes) | Independent consumer groups with independent offsets. Each query fails and recovers alone. |
| We need the **decoupled pipeline paradigm** the module taught — *"producers generate data without knowing who will consume it"* | The simulator writes to a topic and knows nothing about Spark, Redis, MinIO or the API. We can add a fourth consumer without touching the producer. |
| Bursty load (rush-hour demand spikes in the simulator) | The log absorbs the burst; consumers drain at their own rate. The module's ingestion checklist calls this *"elastic scaling with buffers to absorb bursts/backlogs"*. |

### Alternatives considered

| Option | Why rejected |
|---|---|
| **RabbitMQ** | The module's own comparison answers this: messages are removed on consumption, so we could not have three independent consumers replaying the same events, and *"reprocessing old messages is hard → no easy replay"*. Replay is load-bearing for us (see `01 §2.7`). |
| **Direct writes to MinIO/Postgres from the simulator** | Tight coupling. The module names the failure mode: *"system failures cascade through the entire pipeline"*, *"adding new consumers requires modifying existing producers"*. Also removes any buffer, so a slow sink backpressures the simulator. |
| **Redis Streams** | Lighter, and we already run Redis — genuinely tempting. Rejected because retention/replay semantics are weaker, there is no schema registry integration, no consumer-lag tooling ecosystem, and the module taught Kafka specifically. We would have lost the compaction feature we use for the vehicle registry. |

### Trade-off accepted
A single broker with `replication.factor=1` has **no fault tolerance** — a broker loss is total data loss for unconsumed records. This is a demo-scale simplification, stated in the limitations section, with the production answer (RF=3, `min.insync.replicas=2`, `acks=all` across three AZs) written out.

### KRaft vs ZooKeeper — the version note
The module states Kafka used ZooKeeper *"before version 2.8"*. We run 3.x in **KRaft** mode, so there is no ZooKeeper ensemble in our compose file even though the lecture diagram shows one. This is called out explicitly in the report and README so it reads as an informed choice. Benefit here: one fewer container and ~400 MB of RAM back, which matters on a 14 GB laptop.

---

## 3. Stream processing — PySpark Structured Streaming (not Storm)

**Image:** `bitnami/spark:3.5.1` (bundles Java 17 internally — do **not** run Spark against the host's Java 21)

The assignment permits *"Apache Spark (Structured Streaming) OR Apache Storm"*. This choice needs a real defence because the module taught Storm as the streaming engine and Spark as the batch engine.

### Why Spark Structured Streaming

1. **We need one engine for both layers.** This is the decisive reason. Our answer to the module's "multiple codebases" criticism of Lambda is a shared `transforms/` package of DataFrame functions called by both the streaming job and the batch job. That only works if **both layers run on the same engine with the same API**. Choosing Storm for the speed layer would force two implementations of every transformation — in Java/Clojure for Storm and PySpark for batch — which would *manufacture* the exact weakness we are trying to mitigate. **Storm is architecturally incompatible with our headline mitigation.**

2. **The module taught the theory, Spark implements it.** The stream concepts we need were taught in the Storm deck — event time vs processing time, watermarks (*"I am reasonably confident that no more events with a timestamp earlier than X will arrive"*), and tumbling/sliding/session/global windows. Structured Streaming's `withWatermark()` and `window()` are a direct 1:1 implementation of those concepts. The report makes this bridge explicitly: we are applying the taught theory, using the engine whose API expresses it most directly.

3. **Throughput over per-event latency is the right trade for this workload.** The module's own framing: *"In Spark, grouping 1,000 records together before processing is efficient for the CPU (high throughput), but the first record has to wait... Processing every record the microsecond it arrives (like in Apache Storm) gives you instant results but creates more overhead."* Our latency budget for Question A is **seconds**, not milliseconds. A dispatcher looking at a wall dashboard cannot perceive the difference between a 2-second and a 200-millisecond update. We take the throughput.

4. **The module gave a PySpark path and no Storm-in-Python path.** The only external API link in the entire deck set is the PySpark documentation. Storm was taught at the level of *"Topologies / Spouts / Bolts"* with no code, no Python integration, no grouping types and no Trident. Our whole stack is Python; adopting Storm would mean inventing a multi-language integration the module never showed, on a two-week deadline. That is a risk with no compensating benefit.

5. **Structured Streaming gives us the sinks we need for free** — `foreachBatch` for Redis upserts, native Parquet append for the master dataset, native Kafka sink for the DLQ and alert topics, and checkpointing for recovery.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **Apache Storm** | See above. Kills the shared-transforms mitigation; no Python path in the module; no benefit at a seconds-level latency budget. We do concede in the report that if the requirement were *"alert within 50 ms of a GPS ping"*, Storm's per-event model would win outright. |
| **Kafka Streams / ksqlDB** | Both taught briefly. Kafka Streams is a **Java library** (the module says so), which breaks a Python stack. ksqlDB could express the windowed aggregations elegantly, but cannot run our stateful idle-detection logic, cannot write Parquet to MinIO, and cannot be reused for the batch layer. |
| **Apache Flink** | Genuinely the strongest technical alternative — true event-at-a-time with excellent watermark semantics and a good Python API. Rejected because it was not taught in this module (a defensibility cost in the viva) and because it does not give us the single-engine-for-both-layers property that Spark does. |
| **Plain Python consumer** | Would work at 240 events/second and is easy to defend line-by-line. Rejected because it demonstrates none of the module's processing learning outcomes — no windowing, no watermarking, no distributed execution — and it would not scale to the 14.4M events/day the scenario implies. |

### Trade-off accepted
Micro-batch latency floor of ~1–2 seconds, and Structured Streaming's stateful API (`flatMapGroupsWithState`) is the most complex part of the build. Fallback documented in `05-req-processing-speed-layer.md §6`.

---

## 4. Speed-layer store — Redis

**Image:** `redis:7.2-alpine`

### Why Redis for this scenario

The module maps Lambda's speed layer onto **NoSQL databases**, and defines the key–value family as *"simple key→value access for **ultra-fast lookups**"*. Our speed-layer access pattern is exactly that:

| Access pattern in this use case | Redis structure |
|---|---|
| "Give me the current stats for zone 7" — a point lookup by known key, called on every dashboard refresh | `HGETALL fleet:zone:7` — O(1) |
| "Give me the global fleet snapshot" | `HGETALL fleet:snapshot` — O(1) |
| "What's the current state of vehicle V042?" | `HGETALL fleet:vehicle:V042:state` — O(1) |
| "Most recent idle alerts, newest first" | `ZREVRANGE fleet:alerts:idle 0 49` — sorted set, O(log N + M) |

There are **no ad-hoc queries, no joins, and no aggregations** in the speed layer — all aggregation already happened in Spark. A relational database's query planner would be dead weight.

**The TTL argument is the strongest one and is specific to Lambda:** the speed layer's whole job is to cover the window *since* the batch layer last ran, and to be discarded afterwards. Redis makes that automatic — every key carries a 2-simulated-hour TTL, so the transient view expires itself. We never write cleanup code, and Redis can never grow without bound. In a Kappa system this property would be a liability; in a Lambda speed layer it is exactly right.

Secondary: Redis being disposable is what makes the fault-tolerance argument in `01 §2.6` true. We can `FLUSHDB` and lose nothing permanent.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **Cassandra** | The module's canonical wide-column store, and a perfectly good speed-layer choice. Rejected here because our speed-layer working set is *tiny* (12 zones + 150 vehicles + a short alert list) and entirely transient — Cassandra's strengths (petabyte scale, durable write-heavy workloads, tunable consistency) buy us nothing, while costing ~2 GB RAM and a 90-second start-up on a laptop already running Spark and Airflow. **Note: the sibling hospital project chose Cassandra, correctly, because its serving store is durable and write-heavy. Same taxonomy, different answer, driven by the workload — worth mentioning in the viva as evidence the choice was reasoned.** |
| **PostgreSQL (reuse the batch DB)** | Fewer containers. Rejected because it collapses the speed and batch layers into one store, which destroys the clean architectural separation the report is arguing for, couples the two layers' failure domains, and puts high-frequency small writes on a database tuned for analytical reads. |
| **In Spark memory / `memory` sink** | Not queryable by an external API, lost on restart, and does not survive as a serving store. Fine for debugging only. |

### Trade-off accepted
Redis is not durable by default (we run with AOF off — deliberately, since the data is disposable). If Redis dies, the live dashboard is blank until the next micro-batch repopulates it (~seconds). Stated in limitations.

---

## 5. Master dataset — MinIO (S3-compatible) + Apache Parquet

**Image:** `minio/minio` · buckets `fleet-lake`, prefixes `raw/telemetry/sim_date=…/`, `landing/expenses/`, `reports/`

### Why object storage + Parquet for this scenario

| Constraint | How this satisfies it |
|---|---|
| The batch layer must recompute **any past day exactly**, on demand, after a restatement | An immutable, append-only, addressable partition per simulated date. Re-running the job over `sim_date=2026-03-14/` is deterministic and idempotent. This *is* the reprocessing capability that won the architecture argument. |
| The daily job reads **one day out of a growing history** | Hive-style partitioning `sim_date=YYYY-MM-DD` gives Spark **partition pruning** — the 7-day rolling trend scans 7 partitions, not the whole lake. |
| The job needs a few columns out of ~12 | Parquet is **columnar** — column pruning means the profitability job reads `vehicle_id, fare, status, lat, lon, event_time` and skips the rest. |
| Long retention is required but must be cheap (see the cost argument in `01 §2.5`) | Object storage is the cheapest durable tier, and Parquet's compression typically gives 5–10× over raw JSON. |
| The expense file must land somewhere Airflow can sense | Same store, different prefix. One dependency instead of two. |
| Reports must be retrievable by a human | `reports/` prefix, browsable in the MinIO console during the demo. |

The assignment explicitly sanctions this: *"Storage/Sink: ... File System (HDFS/S3 bucket with Parquet)"*. MinIO gives us the S3 API locally with no cloud account and no cost.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **HDFS** | Named in the assignment and the closest thing to the "classic" big-data answer. Rejected on operational cost: a NameNode + DataNode pair is ~1.5 GB of RAM and significant configuration for a laptop stack that already runs Kafka, Spark, Airflow and Postgres, and it gives us nothing MinIO doesn't. MinIO also matches the S3 API our production answer would actually use. |
| **Real AWS S3** | Needs credentials, costs money, and breaks the *"reproducible from a fresh clone with `docker compose up`"* requirement. |
| **Storing raw events in PostgreSQL** | Row-oriented, no partition pruning, no column pruning, and 30 days of telemetry (430M rows at production scale) in an OLTP database is exactly what the module's slide means by *"volumes that are too large for a traditional database"*. |
| **Delta Lake / Apache Iceberg** | Would give ACID transactions and time travel on the lake, which is genuinely better. Rejected for scope: an extra dependency, extra Spark packages, and a concept the module never covered. **We name it in the "what we'd do differently at production scale" section** — that is the right place for it. |

### Trade-off accepted
No ACID on the lake. A crashed streaming job can leave a partial Parquet file in a partition. Mitigated by writing through a checkpointed Structured Streaming query (Spark's `_spark_metadata` commit log makes the sink effectively exactly-once for the file sink) and by the batch job being idempotent on re-run.

---

## 6. Batch-layer store — PostgreSQL (star-schema data mart)

**Image:** `postgres:16-alpine`

### Why PostgreSQL for this scenario

The module maps Lambda's batch layer onto a **data warehouse**, and defines a **data mart** as *"a refined subset of a data warehouse ... an additional transformation stage beyond initial ETL/ELT pipelines, improving performance for complex queries by **pre-joining and aggregating** data"*. That is a precise description of `mart.fact_vehicle_daily_pnl`: telemetry pre-joined with expenses and pre-aggregated to one row per vehicle per day.

| Constraint | How Postgres satisfies it |
|---|---|
| These are **financial figures** feeding driver settlements | The module: RDBMS are *"ACID compliant"*. A restatement upsert must be atomic — either the whole day's recomputation lands or none of it does. A partially-updated P&L table is worse than a stale one. |
| Restatements must **overwrite cleanly and idempotently** | `INSERT … ON CONFLICT (vehicle_id, sim_date) DO UPDATE` on a primary key. Re-running the DAG twice gives the identical result. |
| The API needs **flexible analytical queries** — "top 10 unprofitable vehicles over 7 days by zone" | SQL with joins, window functions (`AVG() OVER (PARTITION BY vehicle_id ORDER BY sim_date ROWS 6 PRECEDING)` computes the rolling trend in one query). |
| Grafana must chart historical trends directly | First-class Postgres datasource, no plugin needed. |
| The serving layer needs a **transactional high-water-mark** | `mart.batch_high_water_mark` updated in the same transaction semantics as the fact upsert — so the boundary can never advance past data that isn't committed. **This is why the merge logic is safe, and it requires a transactional store.** |

The star schema (`dim_vehicle` SCD2, `dim_driver`, `dim_zone`, `dim_date`, `fact_vehicle_daily_pnl`, `fact_zone_hourly`) directly exercises the module's OLAP material — *"star schema, snowflake schema"*, *"drill down, roll up, slice and dice"*, *"pre-aggregation"*.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **Cassandra** | No joins, no aggregation, no window functions, and `ALLOW FILTERING` is a trap. The batch layer's entire job is joining and aggregating — this is the workload Cassandra is explicitly bad at. The module says so: *"fast by row key but limited complex queries—often export to analytics engines"*. |
| **DuckDB / SQLite on the lake** | Lighter and genuinely good for analytics. Rejected because we need concurrent access from Airflow (writing), FastAPI (reading) and Grafana (reading) — a server, not an embedded file. |
| **ClickHouse** | Excellent for this shape of analytics. Rejected on scope: not taught, another heavy container, and Postgres is entirely sufficient at our row counts (150 vehicles × 30 days = 4,500 fact rows). Choosing a columnar OLAP store for 4,500 rows would be exactly the "generic popularity" reasoning the rubric penalises. |
| **Parquet only, queried by Spark** | Would remove a container. Rejected because the serving API would then need a Spark session per request (unusable latency), and there is no transactional high-water-mark. |

### Trade-off accepted
Postgres will not scale to production-volume raw telemetry — but it does not have to, because it only ever holds **aggregated** rows. Raw data stays in the lake. This separation is a design point worth stating: the mart is small by construction.

---

## 7. Orchestration — Apache Airflow

**Image:** `apache/airflow:2.9.3-python3.11`

### Why Airflow for this scenario

The module hands us the justification almost verbatim:

> *"Airflow is designed for finite, batch-oriented workflows... Airflow often complements streaming systems like Apache Kafka. **Kafka handles real-time ingestion, writing data to storage. Airflow can then periodically pick up that data and process it in batch.**"*

That sentence is a description of our system. It is quoted in the report.

| Requirement | Airflow feature (all explicitly taught) |
|---|---|
| Wait for the expense file to land — it may be late | **Sensor** (the module names *"file arrival"* as a sensor use case) |
| Handle a malformed or missing expense file without failing the whole day | **Branching** + **Trigger Rules** |
| Recover from a transient Spark or Postgres failure | **Retries** |
| Run once per simulated day | **Scheduling** (`*/5 * * * *` real = 1 simulated day) |
| Pass the resolved `sim_date` between tasks | **XCom** |
| Make pipeline health *visible* | **Web UI** — DAG graph, task states, run history, per-task logs. This is a required observability deliverable, not a convenience. |
| Re-run a past day after a restatement | Manual DAG-run trigger with a `sim_date` param |

**Critically: Airflow does no processing.** The module is emphatic — *"Airflow only orchestrates. NOT a data processing engine, NOT a database, NOT a distributed computing framework."* Our DAG files contain sensing, validation calls, `spark-submit` invocation, watermark advancement, report rendering and failure callbacks. Every transformation lives in `src/fleet/transforms/` and executes in Spark. This is checkable by grep and it is an explicit viva answer.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **cron** | The module's own critique: *"Difficult dependency tracking / No visualization / No retries / Poor monitoring / Hard to scale."* We need all four of the things cron lacks, and the visualisation is a graded deliverable. |
| **Prefect / Dagster** | Better developer ergonomics and native data-asset lineage. Rejected purely on defensibility: the module taught Airflow, and the viva is on this module's material. |
| **A `while True` Python scheduler** | No retries, no UI, no history, no dependency graph. Would forfeit observability marks. |
| **Spark's own `Trigger.Once` for the batch job** | Would remove Airflow entirely — but then nothing senses the file, nothing retries, nothing branches on validation failure, and there is no orchestration UI. Also, the assignment lists Airflow in the preferred stack. |

### Trade-off accepted
Airflow is the heaviest single component (~1.5 GB with its own metadata Postgres). We run `LocalExecutor` rather than Celery to keep it to two processes.

---

## 8. Serving — FastAPI

**Build:** `python:3.11-slim` + `fastapi` + `uvicorn`

### Why FastAPI for this scenario

The suggested output is *"API endpoint to return real-time fleet utilization metrics"*, so an API is mandated. FastAPI because:

1. **The serving layer is where Lambda's hardest problem lives** — reconciling two views. That needs real application code (`merge.py`), not a thin database proxy. See `07-req-storage-serving.md §5`.
2. **Async I/O matters here specifically**: a single merged request hits Redis *and* Postgres. `asyncio.gather` runs both concurrently, so the merged endpoint costs roughly `max(redis, postgres)` rather than their sum.
3. **Pydantic response models make the consistency contract explicit and self-documenting.** The `source` and `consistency` fields are part of the schema, so the OpenAPI docs at `/docs` *show* a reviewer that we track provenance. That is a report screenshot.
4. `prometheus-fastapi-instrumentator` and the OpenTelemetry FastAPI instrumentation both drop in with a few lines — the serving layer is instrumented for free.

### Alternatives considered

| Option | Why rejected |
|---|---|
| **Flask** | Synchronous by default (so the two-store fan-out serialises), no built-in validation, no automatic OpenAPI. |
| **Grafana querying stores directly, no API** | Would satisfy "dashboard" but not the explicit *"API endpoint"* requirement, and — more importantly — there would be **nowhere for the reconciliation logic to live**. The merge would have to be duplicated into every Grafana panel query. |
| **PostgREST** | Auto-generates an API over Postgres, but has no access to Redis and cannot merge two sources. |

---

## 9. Observability stack — Prometheus + Grafana + Alertmanager + OpenTelemetry/Jaeger

Full design in `09-req-observability.md`. Stack-level justification:

| Component | Why |
|---|---|
| **Prometheus** | Pull-based scraping suits a compose stack where every service exposes `/metrics`; PromQL expresses the alert conditions the PDF asks for directly (`rate(events_produced_total[2m]) == 0`). |
| **Pushgateway** | The pragmatic bridge out of a PySpark driver, which is a batch-ish process that cannot reliably be scraped. Alternative (Spark's `PrometheusServlet`) is evaluated and rejected in `09 §3.3`. |
| **Grafana** | Provisioned as code (dashboards + datasources in the repo), so `docker compose up` yields working dashboards on a fresh clone — a reproducibility mark. |
| **Alertmanager** | The PDF requires *"at least one basic alert or health-check rule"*. Real routing, grouping and inhibition beats a `print()` statement. |
| **OpenTelemetry + Jaeger** | The rubric says *"tracing"*. W3C trace context propagates through Kafka headers. **We are honest about the limit**: true per-event tracing through a micro-batch engine is not feasible, so we trace at micro-batch granularity and link sampled events. See `09 §5`. |
| **`structlog`** | The PDF requires *"structured logging across ingestion, processing, and storage stages"*. JSON to stdout with a fixed field schema including `stage`, `trace_id` and `sim_date`. |

**Honesty requirement:** none of these five were taught in this module. The report introduces them in a clearly-labelled paragraph as deliberate extensions beyond lecture scope. Claiming them as course content would be dishonest and easy to catch.

---

## 10. Serialization — Avro + Confluent Schema Registry

**Image:** `confluentinc/cp-schema-registry:7.6.1`

### Why Avro rather than JSON

The module taught Schema Registry explicitly: *"stores the structure of messages (schemas) to ensure data consistency. Prevents producers and consumers from misinterpreting messages. Supports **schema evolution** (e.g., adding optional fields without breaking consumers)."*

Scenario-specific reasons:
1. **The schema will evolve during this project.** `zone_id` is added by enrichment; a later version adds `passenger_count`. We demonstrate a v1-producer → v2-consumer compatibility test in `tests/contract/`.
2. **Binary Avro is ~40–60% smaller than JSON** for this record shape, which matters at 240 events/second sustained and directly reduces both broker storage and the Parquet footprint.
3. **Schema-on-write catches malformed events at the producer**, not three hops downstream — the module's ingestion checklist warns that serialization *"mismatches make data inert/unusable"*.
4. Parquet is itself schema-carrying and columnar; Avro → Parquet is a natural, well-supported path.

### Trade-off accepted, stated plainly
Avro adds a container, a registry dependency, and makes debugging harder — you cannot `cat` a message. Mitigation: Kafka UI is configured with the Schema Registry so messages are human-readable in the browser, and a `make peek TOPIC=…` helper deserialises to JSON on the CLI.

**Fallback if time is short:** switch to JSON with a Pydantic model as the contract, and document the change and its cost honestly. This is item 5 on the cut list in `15-risks-and-cuts.md`.

---

## 11. Summary table for the report (§5)

Reproduce this in the report as the technology-stack table. Keep the "Trade-off accepted" column — it is what turns a list into a justification.

| Layer | Chosen | Alternatives considered | Decisive reason *for this scenario* | Trade-off accepted |
|---|---|---|---|---|
| Ingestion | Kafka (KRaft) | RabbitMQ, Redis Streams, direct writes | Per-vehicle ordering via `vehicle_id` partition key; 3 independent consumers replaying one log | RF=1 — no fault tolerance at demo scale |
| Serialization | Avro + Schema Registry | JSON+Pydantic, Protobuf | Schema will evolve; 40–60% smaller at 240 ev/s | Harder to debug; extra container |
| Speed processing | PySpark Structured Streaming | Storm, Flink, Kafka Streams, ksqlDB | **Same engine as the batch layer** → one shared `transforms/` package → answers the "multiple codebases" critique | ~1–2 s micro-batch latency floor |
| Speed store | Redis | Cassandra, Postgres, in-memory | Point lookups only; **native TTL makes the transient view self-expiring** | Not durable (deliberately) |
| Master dataset | MinIO + Parquet | HDFS, S3, Postgres, Delta/Iceberg | Immutable partitions → deterministic, idempotent restatement re-runs; partition + column pruning | No ACID on the lake |
| Batch processing | PySpark (batch) | dbt, pandas, SQL-only | Shared code with speed layer; handles the wide-transformation join at scale | Spark start-up overhead per DAG run |
| Batch store | PostgreSQL star mart | Cassandra, DuckDB, ClickHouse | ACID for financial figures; window functions for the rolling trend; **transactional high-water-mark** | Won't scale to raw telemetry — by design, it only holds aggregates |
| Orchestration | Airflow | cron, Prefect, Dagster, `Trigger.Once` | File sensing + retries + branching + **a UI that makes health visible** | Heaviest container (~1.5 GB) |
| Serving | FastAPI | Flask, PostgREST, Grafana-direct | Somewhere for the **merge logic** to live; async fan-out to two stores | Custom code to maintain and defend |
| Dashboard | Grafana | Streamlit, custom JS | Provisioned as code → reproducible on fresh clone | Less bespoke than a hand-built UI |
| Metrics | Prometheus (+Pushgateway) | StatsD, OTel metrics | PromQL expresses the required alert rules directly | Pushgateway is a known anti-pattern for long-lived jobs — justified in `09 §3.3` |
| Alerting | Alertmanager | Log-and-hope, email script | Real routing/grouping; required deliverable | Another container |
| Tracing | OTel Collector + Jaeger | None, Zipkin | Rubric explicitly asks for tracing | Micro-batch granularity only — stated honestly |
| Logging | `structlog` → JSON | stdlib `logging`, Loki | Required structured logging with a fixed field schema | Loki (log aggregation) is a stretch goal |
