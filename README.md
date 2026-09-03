# Ride-Hailing Fleet Operations — a Lambda Architecture Data Pipeline

**EC8203 Applied Big Data Engineering — Mini Project** · Use Case 1 · **Lambda architecture**

> ⚠️ **Status: under construction.** Day 1 of 14 complete (foundations + ingestion backbone).
> See [`plan/14-schedule.md`](plan/14-schedule.md) for what lands when.

---

## The business question

> *"What is fleet utilization and earnings by area/time-of-day **right now**, and which vehicles
> are **becoming unprofitable** once **yesterday's** fuel/maintenance costs are factored in?"*

That is **two questions with two different latency and accuracy requirements**. One wants an
answer in seconds and tolerates approximation; the other wants an answer once a day and must be
exactly right, because it is money. Serving both from a single processing path would mean either
making the financial number approximate or making the operational number a day late.

That is why this is a **Lambda** architecture. The full argument — walked against the module's
own eight decision criteria, with the rejected Kappa alternative argued fairly first — is in
[`plan/01-architecture-decision.md`](plan/01-architecture-decision.md).

---

## Architecture at a glance

| Layer | Technology | Answers |
|---|---|---|
| **Speed layer** | Spark Structured Streaming → **Redis** | *"right now"* — approximate, seconds old |
| **Batch layer** | Spark batch over **MinIO/Parquet** → **PostgreSQL** star-schema mart | *"unprofitable"* — exact, T+1 |
| **Serving layer** | **FastAPI**, merging both at an explicit high-water-mark | one API, every row labelled with its source |

The module defines Lambda as *"three independent systems: a **batch layer** … in systems like
**data warehouses**, a **speed layer** … using **NoSQL databases**, and a **serving layer** that
combines results from both."* The mapping above is deliberate — the report points at the slide,
then at the code.

---

## Quick start

**Prerequisite:** Docker Desktop with WSL integration enabled
(Settings → Resources → WSL Integration). See [`plan/10`](plan/10-infrastructure-docker.md) §0.

```bash
make setup      # check docker, create .env
make up         # start the stack (~8.2 G, profile: run)
make topics     # confirm the topics exist
```

Then open **<http://localhost:8080>** (Kafka UI).

To stop: `make down` keeps your data, **`make clean` wipes it**.

> **Start every run with `make clean && make up`.** The simulation always begins at simulated
> day 1, so restarting over retained data writes a second day 1 on top of the first — nothing
> errors, the dates and trends just go quietly wrong. The init container detects this and
> refuses to start. See [`plan/10`](plan/10-infrastructure-docker.md) §4.3.

---

## What to look at

| URL | What it shows | Available from |
|---|---|---|
| <http://localhost:8080> | **Kafka UI** — topics, partitions, cleanup policies, consumer lag | ✅ day 1 |
| <http://localhost:8081/subjects> | **Schema Registry** — registered Avro subjects | ✅ day 1 |
| <http://localhost:8082> | Airflow — DAG runs, task logs | day 6 |
| <http://localhost:4040> | Spark UI — Structured Streaming progress | day 4 |
| <http://localhost:8000/docs> | FastAPI — the serving layer | day 7 |
| <http://localhost:3000> | Grafana — dashboards | day 9 |
| <http://localhost:9001> | MinIO console — the master dataset and reports | day 3 |

---

## Simulated clock

The assignment permits time compression and requires it to be stated clearly.

| | |
|---|---|
| **1 simulated day** | **5 real minutes** |
| **Speed-up** | **288×** — one real second is 4.8 simulated minutes |
| Simulation starts at | simulated `2026-03-01T00:00:00Z` (fixed, so screenshots are stable) |
| Fleet | 150 vehicles across 12 zones |
| Telemetry | one ping per vehicle per 3 simulated minutes ≈ **240 events/second** |

Every process derives simulated time from **one shared anchor** written once by the `init`
container (`state/sim_epoch.json`). Nobody computes it locally — four processes disagreeing
about the date is a failure mode that produces no error, just an empty join.

Events carry **two** timestamps: `event_time` (simulated — used for windowing, watermarking and
partitioning) and `ingest_time` (real — used for latency metrics). Mixing them up is the classic
bug in a compressed-time simulation.

**Watermarks are expressed in simulated time.** A 30-simulated-minute watermark is only
**6.25 real seconds** of lateness tolerance at 288×. `config.py` refuses to start if that drops
below 5 seconds, and a unit test fails CI if the speed-up is changed without revisiting it.
See [`plan/03`](plan/03-req-data-sources.md) §3.3.

`make simclock` prints the current anchor.

---

## Repository layout

```
src/fleet/
├── common/          config, the shared simulated clock, logging, Avro schemas
├── transforms/      ★ pure DataFrame functions — shared by BOTH layers
├── ingestion/       the two simulated sources
├── speed_layer/     Structured Streaming: 3 queries
├── batch_layer/     Spark batch: the two-source join and profitability
└── serving/         FastAPI — including merge.py, the reconciliation
scripts/             topic creation, schema registration, stack bootstrap
plan/                the full 17-document design (read plan/README.md first)
tests/unit/          pure-Python tests: no Spark, no Docker
```

`transforms/` is the answer to the module's stated Lambda weakness — *"managing multiple
codebases"*. The same tested function is imported by the streaming job and the batch job, so
there is one codebase with two entry points, not two implementations.

---

## Development

```bash
make venv       # create .venv and install dev deps (uv)
make test       # unit tests — no docker needed
make lint       # ruff + format check + mypy
make fmt        # auto-format
```

---

## Documentation

The design lives in [`plan/`](plan/README.md) — 17 documents, one per requirement, each mapped
to the marking rubric. Start with [`plan/00-overview-architecture-and-stack.md`](plan/00-overview-architecture-and-stack.md).

Decision records are in `docs/adr/`.

---

## Individual contributions

_To be completed before submission (required for group submissions)._
