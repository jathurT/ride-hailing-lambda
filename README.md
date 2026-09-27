# Ride-Hailing Fleet Operations — a Lambda Architecture Data Pipeline

EC8203 Applied Big Data Engineering · mini project · Use Case 1

A data platform that ingests continuous vehicle telemetry and a once-a-day partner expense
file, processes both, and answers one business question from two directions at once.

---

## The business question

> *"What is fleet utilization and earnings by area/time-of-day **right now**, and which vehicles
> are **becoming unprofitable** once **yesterday's** fuel/maintenance costs are factored in?"*

It contains two questions with opposite tolerances, and that is the whole reason the
architecture has two paths:

|  | "right now" | "becoming unprofitable" |
|---|---|---|
| Asked by | Dispatch operations | Finance |
| Latency budget | Seconds | Hours — the expense file does not exist until the day ends |
| Accuracy | Approximate is fine | Exact; it feeds driver settlements |
| Needs history | No | **Yes** — "becoming" is a multi-day trend |

One forgives error and demands speed. The other forgives delay and demands correctness.

---

## Architecture

| Layer | Technology | Answers |
|---|---|---|
| **Speed layer** | Spark Structured Streaming → **Redis** | *"right now"* — approximate, seconds old |
| **Batch layer** | Spark batch over **MinIO/Parquet** → **PostgreSQL** star-schema mart | *"unprofitable"* — exact, recomputable |
| **Serving layer** | **FastAPI**, merging both at an explicit high-water-mark | one API, every row labelled with its source |

Telemetry enters through Kafka (KRaft, Avro + Schema Registry), keyed by `vehicle_id` so each
vehicle's events stay ordered — which the stateful idle detector depends on. The daily expense
file is **not** streamed: it is written straight to object storage, because a once-a-day
reference feed is not an event stream.

**The merge rule.** The batch layer records the last simulated date it has fully processed.
The serving layer serves `[from, hwm]` from PostgreSQL and `(hwm, now]` from Redis — the
watermark date belongs to exactly one side, so no date is double-counted and none is dropped.
Every response states which view produced each row and how far the exact data extends.

`src/fleet/transforms/` holds pure DataFrame functions imported by **both** layers. That is the
answer to Lambda's stated weakness of "managing multiple codebases": one tested library, two
entry points, enforced by an architecture test.

---

## Quick start

**Prerequisite:** Docker Desktop with WSL integration enabled.

```bash
make setup                 # check docker, create .env
make clean && make up      # fresh simulation + full pipeline
make up-obs                # add Prometheus, Grafana, Alertmanager
```

Then open **<http://localhost:3000>** (Grafana) and **<http://localhost:8000/docs>** (the API).

> **Start every run with `make clean`.** The simulation always begins at simulated day 1, so
> restarting over retained data writes a second day 1 on top of the first — nothing errors, the
> dates and trends just go quietly wrong. The init container detects this and refuses to start.
> To deliberately continue an existing run instead, use `RESUME=1 make up`.

The batch layer needs a few complete simulated days before trends mean anything:

```bash
make backfill DAYS=7       # run the batch layer over the last complete sim days
make batch-check           # the numbers that prove it did what it claims
```

---

## What to look at

| URL | What it shows |
|---|---|
| <http://localhost:8000/docs> | **The API** — every endpoint, and the merged response model |
| <http://localhost:3000> | **Grafana** — Fleet Operations, Batch Reconciliation, Pipeline Health |
| <http://localhost:8082> | **Airflow** — the daily reconciliation DAG (`admin`/`admin`) |
| <http://localhost:9090/alerts> | **Prometheus** — alert rules and their state |
| <http://localhost:9093> | **Alertmanager** — firing alerts, grouping and inhibition |
| <http://localhost:8080> | **Kafka UI** — topics, partitions, message counts |
| <http://localhost:4040> | **Spark UI** — Structured Streaming progress |
| <http://localhost:9001> | **MinIO** — the master dataset, partitioned by simulated date |
| <http://localhost:8081/subjects> | **Schema Registry** — registered Avro subjects |

Two dashboards answer the business question directly; the third shows whether the pipeline
answering it is healthy. The Grafana panels are driven through this project's own API rather
than by reading Redis and PostgreSQL directly, so a panel that breaks is telling the truth
about the serving layer.

---

## Simulated clock

One simulated day is compressed into **300 real seconds** — a **288×** speed-up. The simulation
starts at simulated date `2026-03-01`. Every container derives simulated time from one shared
anchor file written at start-up, so no two services can disagree about the date.

Durations are marked *real* or *simulated* throughout. A 30-simulated-minute watermark is
6.25 real seconds; a 2-simulated-hour TTL is 25 real seconds.

```bash
make simclock              # where the simulated clock is now
```

---

## Trying things

```bash
# Does the reconciliation actually happen?
curl 'localhost:8000/api/v1/fleet/utilization?from=2026-03-02&to=2026-03-08'

# Which vehicles are becoming unprofitable?
curl 'localhost:8000/api/v1/vehicles/unprofitable?limit=10'

# Graceful degradation: stop a store, the API keeps answering
docker stop fleet-redis
curl -i 'localhost:8000/api/v1/fleet/utilization?from=2026-03-02&to=2026-03-08'   # 200 + X-Data-Degraded
docker compose up -d --force-recreate redis   # NOT `docker start` — see below

# Does the alerting work? (fires in ~71 s, resolves in ~51 s)
make chaos-kill-producer
make alerts
make chaos-heal

# Speed view vs batch view for one simulated date
make reconcile DATE=2026-03-04
```

> **Bring a stopped container back with `docker compose up -d --force-recreate`, not
> `docker start`.** A container restarted with `docker start` can come up without its
> published host ports, and nothing reports it: everything inside the Docker network keeps
> working, so the pipeline looks healthy while connections from the host silently fail.
> `make chaos-heal` does this correctly.

`make ports` lists everything that is listening. `make help` lists every target.

---

## Repository layout

```
src/fleet/
├── common/          config, the shared simulated clock, logging, metrics
├── transforms/      ★ pure DataFrame functions — shared by BOTH layers
├── ingestion/       the two simulated sources
├── speed_layer/     Structured Streaming queries, sinks, progress listener
├── batch_layer/     Spark batch: zone rollup, profitability, validation
├── store/           data access for the lake, the mart and the speed view
└── serving/         FastAPI — including merge.py, the reconciliation
airflow/dags/        the daily reconciliation DAG
observability/       Prometheus scrape config and alert rules, provisioned Grafana
sql/ddl/             the mart schema
scripts/             stack bootstrap, backfill, figure capture
docs/                the report, its diagrams, and the measurement evidence
plan/                the design documents, one per requirement
tests/unit/          418 tests
```

---

## Development

```bash
make test       # full suite; store-backed tests skip if the stack is down
make lint       # ruff + format check + mypy
make fmt        # auto-format
```

Configuration is typed and centralised in `src/fleet/common/config.py`, one settings class per
layer, loaded from the environment. There are no magic numbers elsewhere in the codebase — a
threshold hard-coded in a `.py` file is treated as a bug. Several settings carry validators that
turn a misconfiguration into a start-up failure rather than a wrong number later.

---

## Documentation

| | |
|---|---|
| [`docs/report/`](docs/report/) | the submitted report — `make report` builds the PDF |
| [`docs/evidence/`](docs/evidence/) | every measurement quoted in the report, with how it was taken |
| [`docs/diagrams/`](docs/diagrams/) | the report's diagrams as draw.io files, generated by `build_drawio.py`; `make diagrams` exports them to PDF |
| [`plan/`](plan/README.md) | the design documents written before the build |

`make figures` re-captures the report's screenshots from the running stack. Both the diagrams
and the screenshots are regenerated from source, so the report cannot silently drift from the
system it describes.

---

## Known limitations

Stated here rather than left to be discovered; the report discusses each in full.

- **Distributed tracing was not implemented.** Observability rests on structured logging,
  metrics and alerting. A request cannot currently be followed across process boundaries.
- **A rendered PDF daily report was not implemented.** The provisioned dashboards discharge the
  requirement for a consolidated report *or* dashboard.
- **Exactly-once is not achieved.** The system is at-least-once with idempotent sinks, which is
  why every sink overwrites rather than increments.
- **Consumer lag cannot measure the streaming queries.** They checkpoint their own offsets and
  never commit to a consumer group, so lag reads zero whether a query is healthy or dead. The
  liveness signal is the age of the last progress timestamp instead.
- **Three of six streaming queries exceed their 10-second trigger at p95** on this hardware —
  the windowed and stateful ones. The system keeps up on average, not at the tail.
- **The expense file is generated from our own simulated distance**, so the two sources are only
  pseudo-independent and the reconciliation is weaker than it appears.
- **Single broker, `replication.factor=1`, no auth or TLS anywhere.** Demo scale only.

---

## Individual contributions

| Member | Contribution |
|---|---|
| Jathur | Architecture decision and justification; simulated clock and typed configuration; stateful idle detection; serving layer and the merge boundary |
| Rifath | Simulated sources and ingestion; Avro and Schema Registry; speed-layer queries and sinks; observability — metrics, alert rules and dashboards |
| Shamil | Storage design — lake layout, star schema, speed-view keys; batch layer — zone rollup and per-vehicle profitability; restatement and idempotency |

All three contributed to testing, the demonstration, and the report.
