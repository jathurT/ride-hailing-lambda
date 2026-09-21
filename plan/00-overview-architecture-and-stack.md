# 00 — Overview: Architecture & Technology Stack

> This is the single document that describes **what we are building and what we are building it with**. Every other document expands one box on this page.

---

## 1. The use case in one table

| | |
|---|---|
| **Scenario** | A ride-hailing operator wants live visibility into fleet activity and utilization, while reconciling it daily against per-vehicle running costs submitted by fuel providers. |
| **Streaming source** | GPS/telemetry per trip: `trip_id, driver_id, vehicle_id, lat, lon, speed, status[idle\|enroute\|on_trip], fare, timestamp` — every few seconds. |
| **Daily-batch source** | One CSV per simulated day of vehicle expense records from garages and fuel partners: `vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag`. |
| **Business question** | *What is fleet utilization and earnings by area/time-of-day **right now**, and which vehicles are **becoming unprofitable** once **yesterday's** fuel/maintenance costs are factored in?* |
| **Required outputs** | (1) API for real-time fleet utilization metrics; (2) threshold alerts for prolonged vehicle idling; (3) a daily per-vehicle profitability reconciliation report. |

---

## 2. Architecture: **Lambda**

### The decision in one sentence

The business question is **two questions bolted together** — one that needs an answer in seconds and tolerates approximation ("right now"), and one that needs an answer once a day and must be exactly right because it is money ("unprofitable... yesterday's costs") — and serving both from a single processing path would mean either making the financial number approximate or making the operational number a day late. Lambda exists precisely for this shape of problem.

Full reasoning, the rejected Kappa alternative, and the viva defence: **`01-architecture-decision.md`**.

### The three layers, in the module's own vocabulary

The lecturer defines Lambda as *"three independent systems: a **batch layer** that processes historical data in systems like **data warehouses**, a **speed layer** that processes real-time data with low latency using **NoSQL databases**, and a **serving layer** that combines results from both layers."*

Our implementation maps onto that definition literally — this is deliberate, so the report can point at the slide and then at the code:

| Lecturer's layer | Lecturer says it uses | We use | Serves |
|---|---|---|---|
| **Batch layer** | a data warehouse | **PostgreSQL star-schema data mart**, fed by a Spark batch job over the **MinIO/Parquet master dataset** | Exact per-vehicle daily profitability; historical trends |
| **Speed layer** | a NoSQL database | **Redis** (key-value), fed by **Spark Structured Streaming** | Approximate live utilization, earnings by zone, idle alerts |
| **Serving layer** | combines results from both | **FastAPI** with an explicit high-water-mark merge | One API that answers both halves of the business question |

---

## 3. System dataflow

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              SIMULATED SOURCES (Python)                          │
│                                                                                  │
│   telemetry_producer.py                          expense_dropper.py              │
│   150 vehicles · state machine                   1 CSV per simulated day         │
│   ping every 3 sim-minutes                       (fuel, maintenance, distance)   │
└───────────────┬──────────────────────────────────────────────┬───────────────────┘
                │                                              │
                ▼ Avro + Schema Registry                       ▼ writes object
      ┌───────────────────────┐                       ┌──────────────────────┐
      │   APACHE KAFKA        │                       │  MinIO  landing/     │
      │   (KRaft mode)        │                       │  expenses_<date>.csv │
      │                       │                       └──────────┬───────────┘
      │ fleet.telemetry.v1    │                                  │
      │   6 partitions        │                                  │ S3KeySensor
      │   key = vehicle_id    │                                  ▼
      │ fleet.vehicle.registry│                       ┌──────────────────────┐
      │   (log compacted)     │                       │   APACHE AIRFLOW     │
      │ fleet.alerts.v1       │◀────────┐             │   fleet_daily_       │
      │ fleet.telemetry.dlq   │◀──┐     │             │   reconciliation DAG │
      └───────────┬───────────┘   │     │             │   (every 5 real min  │
                  │               │     │             │    = 1 simulated day)│
                  │ readStream    │     │             └──────────┬───────────┘
                  ▼               │     │                        │ spark-submit
  ╔═══════════════════════════════╪═════╪══════════════╗         │
  ║  SPEED LAYER                  │     │              ║         ▼
  ║  PySpark Structured Streaming │     │              ║  ╔══════════════════════════╗
  ║                               │     │              ║  ║  BATCH LAYER             ║
  ║  Q1 raw ──────────────────────┼─────┼──────┐       ║  ║  PySpark batch job       ║
  ║  Q2 validate → enrich → window│     │      │       ║  ║                          ║
  ║       → aggregate ────────────┼─────┼──┐   │       ║  ║  read Parquet partition  ║
  ║  Q3 stateful idle detection ──┘     │  │   │       ║  ║   ⨝ expenses CSV         ║
  ║       (per vehicle_id)              │  │   │       ║  ║   → per-vehicle P&L      ║
  ╚═════════════════════════════════════╪══╪═══╪═══════╝  ║   → 7-day rolling trend  ║
                                        │  │   │          ╚═══════════┬══════════════╝
              invalid events ───────────┘  │   │                      │
                                           ▼   ▼                      ▼
                              ┌──────────────┐ ┌──────────────────────────────────┐
                              │  REDIS       │ │  MinIO  raw/telemetry/           │
                              │  (speed view)│ │  sim_date=YYYY-MM-DD/*.parquet   │
                              │  TTL'd       │ │  ── THE MASTER DATASET ──        │
                              │  approximate │ │  immutable · append-only         │
                              └──────┬───────┘ └──────────────────────────────────┘
                                     │                      │
                                     │                      ▼
                                     │         ┌────────────────────────────────┐
                                     │         │  POSTGRESQL  (data mart)       │
                                     │         │  star schema · exact · ACID    │
                                     │         │  fact_vehicle_daily_pnl        │
                                     │         │  batch_high_water_mark ◀───────┼── the boundary
                                     │         └────────────┬───────────────────┘
                                     │                      │
                                     └──────────┬───────────┘
                                                ▼
                         ╔══════════════════════════════════════════════╗
                         ║  SERVING LAYER — FastAPI                     ║
                         ║  merge.py: [from … hwm] ← batch              ║
                         ║            (hwm … now] ← speed               ║
                         ║  every row stamped {"source": batch|speed}   ║
                         ╚══════════════════┬═══════════════════════════╝
                                            │
                        ┌───────────────────┼──────────────────────┐
                        ▼                   ▼                      ▼
                  ┌───────────┐      ┌─────────────┐      ┌────────────────┐
                  │  GRAFANA  │      │ Daily report│      │ Alertmanager   │
                  │ dashboards│      │  PDF / HTML │      │ (health rules) │
                  └───────────┘      └─────────────┘      └────────────────┘

  ── OBSERVABILITY SPINE (cross-cutting, touches every box above) ────────────────
     structured JSON logs · Prometheus metrics · OpenTelemetry traces → Jaeger
     Airflow UI · Kafka UI · Spark UI · Grafana · Alertmanager
```

---

## 4. Technology stack — the summary table

Full justification per layer, with rejected alternatives: **`02-technology-stack.md`**.

| Layer | Technology | Version / image | One-line reason it fits *this* scenario |
|---|---|---|---|
| **Ingestion** | Apache Kafka (KRaft) | `confluentinc/cp-kafka:7.6.1` | Telemetry is an unbounded ordered log; partitioning by `vehicle_id` preserves the per-vehicle ordering that idle-duration detection depends on |
| Schema management | Confluent Schema Registry + Avro | `cp-schema-registry:7.6.1` | Telemetry schema will evolve; the module taught schema evolution explicitly |
| Broker UI | Kafka UI (Provectus) | `provectuslabs/kafka-ui` | Makes topics, partitions, offsets and consumer lag *visible* — an observability deliverable |
| **Speed layer processing** | PySpark Structured Streaming | `bitnami/spark:3.5.1` | Micro-batch gives throughput at 240 ev/s; `withWatermark`/`window` map 1:1 onto the taught stream theory |
| **Speed layer store** | Redis | `redis:7.2-alpine` | Lecturer's own mapping: speed layer ⇒ NoSQL key-value, "ultra-fast lookups"; native TTL means the transient view self-expires |
| **Master dataset** | MinIO (S3-compatible) + Parquet | `minio/minio` | Immutable, append-only, columnar, partitioned by `sim_date` so the batch job scans one partition; the cheapest storage tier |
| **Batch layer processing** | PySpark (DataFrame API, batch) | `bitnami/spark:3.5.1` | Exactly what the module taught — Catalyst, lazy evaluation, the join is the canonical wide transformation |
| **Batch layer store** | PostgreSQL, star-schema data mart | `postgres:16-alpine` | Lecturer's mapping: batch layer ⇒ data warehouse; "data mart = pre-joining and aggregating"; ACID matters because these are financial figures |
| **Orchestration** | Apache Airflow | `apache/airflow:2.9.3` | The lecturer's literal prescription: *"Kafka handles real-time ingestion, writing data to storage. Airflow can then periodically pick up that data and process it in batch."* |
| **Serving** | FastAPI + Uvicorn | build | The Lambda serving layer in code — merges Redis and Postgres views behind one contract |
| **Dashboard** | Grafana | `grafana/grafana:11.1.0` | Live dashboards over Postgres + Redis; provisioned as code so it is reproducible |
| **Metrics** | Prometheus + Pushgateway + exporters | `prom/prometheus:v2.53.0` | Pull-based scraping across every stage; Pushgateway is the pragmatic path out of a PySpark driver |
| **Alerting** | Alertmanager | `prom/alertmanager:v0.27.0` | Real alert routing for the required health-check rules |
| **Tracing** | OpenTelemetry Collector + Jaeger | `otel/opentelemetry-collector-contrib`, `jaegertracing/all-in-one` | The rubric says "tracing"; W3C trace context propagates through Kafka headers |
| **Logging** | `structlog` → JSON → stdout (+ optional Loki) | — | Structured logs across ingestion, processing and storage, as the PDF requires |

> **Honesty note carried into the report:** Prometheus, Grafana, OpenTelemetry, MinIO/S3 and Parquet were **not** taught in this module. They are deliberate extensions and the report presents them as such rather than implying they were course content. Kafka, Spark, Airflow, PostgreSQL and the NoSQL taxonomy (which is where Redis comes from) *were* taught, and the report uses the module's vocabulary for those.

---

## 5. Simulated clock — stated up front

The assignment permits time compression and requires us to state it clearly.

| Parameter | Value |
|---|---|
| **1 simulated day** | **5 real minutes** |
| **Speed-up factor** | **288×** |
| Fleet size | 150 vehicles |
| Telemetry ping interval | every 3 simulated minutes (≈ every 0.625 real seconds per vehicle) |
| Aggregate event rate | ≈ **240 events/second** |
| Events per simulated day | 150 × 480 = **72,000** |
| Batch source cadence | 1 expense CSV per simulated day = 1 file per 5 real minutes |
| Airflow DAG schedule | `*/5 * * * *` (real time) = once per simulated day |

Every process — producers, Spark, Airflow, the API — derives simulated time from **one shared module** (`common/simclock.py`) anchored to a single epoch written at stack start-up, so nothing can drift. The subtle failure mode this prevents, and how watermarks must be expressed in *simulated* time, is covered in `03-req-data-sources.md §3`.

---

## 6. How the pipeline stays observable

The PDF requires structured logging plus at least one alert rule; the rubric asks for "logging, metrics and **tracing** across pipeline stages". We do all three plus live UIs. Full design in `09-req-observability.md`.

**What you can look at while it runs:**

| URL | What it shows |
|---|---|
| `localhost:3000` | **Grafana** — 4 provisioned dashboards: Pipeline Health, Fleet Operations, Batch Reconciliation, Traces & Logs |
| `localhost:8080` | **Kafka UI** — topics, partitions, message throughput, consumer group lag |
| `localhost:8082` | **Airflow UI** — DAG graph, task states, run history, task logs |
| `localhost:8090` | **Spark UI** — the Structured Streaming tab: micro-batch durations, input/processing rates, watermark |
| `localhost:9090` | **Prometheus** — raw metric exploration, alert rule state |
| `localhost:9093` | **Alertmanager** — currently firing alerts |
| `localhost:16686` | **Jaeger** — end-to-end trace waterfalls |
| `localhost:8000/docs` | **FastAPI** — interactive OpenAPI docs for the serving layer |
| `localhost:9001` | **MinIO Console** — the master dataset and generated reports |

**Alerting is split in two, deliberately** — a distinction the report makes explicitly:
- **Pipeline-health alerts** (Alertmanager): no telemetry ingested for 3 min, DLQ rate > 5%, consumer lag > 10k, streaming query stalled, Airflow DAG failed, batch high-water-mark stale.
- **Business alerts** (emitted by the pipeline into `fleet.alerts.v1`, surfaced in the API and dashboard): vehicle idle > 45 simulated minutes, vehicle unprofitable over 7 simulated days, zone underserved.

---

## 7. What "done" looks like

A single command from a fresh clone brings up the whole stack, and within ~15 real minutes (3 simulated days) a reviewer can see:

1. Telemetry flowing in Kafka UI, partitioned across 6 partitions by `vehicle_id`.
2. The Fleet Operations dashboard updating live with utilization and earnings by zone.
3. A seeded vehicle going idle and triggering a **business alert** visible in the API and dashboard.
4. Parquet files accumulating in MinIO under `raw/telemetry/sim_date=…/`.
5. An expense CSV landing, an Airflow DAG sensing it and turning green.
6. A **daily profitability PDF** appearing in MinIO `reports/`.
7. The serving API returning a merged response with `"source": "batch"` rows and `"source": "speed"` rows and an `as_of` boundary.
8. A **restated expense file** on simulated day 3 causing the batch layer to recompute day 2 and the numbers to correct themselves — the argument for Lambda, demonstrated live.
9. A killed producer causing a **pipeline-health alert** to fire in Alertmanager within 3 minutes.
10. A Jaeger trace showing a sampled event's journey from producer through the micro-batch to the sink.
