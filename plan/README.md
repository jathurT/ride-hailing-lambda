# Project A — Ride-Hailing Fleet Operations · Plan Index

**Module:** EC8203 Applied Big Data Engineering — Mini Project (25% of module grade)
**Use Case:** 1 — Ride-Hailing Fleet Operations
**Architecture:** **Lambda** (batch layer + speed layer + serving layer)
**Status:** PLANNING — no code written yet. Review these documents, then approve to start building.

---

## The business question we must answer

> *"What is fleet utilization and earnings by area/time-of-day **right now**, and which vehicles are **becoming unprofitable** once **yesterday's** fuel/maintenance costs are factored in?"*

Read that sentence carefully — it contains **two questions with two completely different latency and accuracy requirements**, and that single observation is what drives the entire architecture decision. See `01-architecture-decision.md`.

---

## How to read this plan

Read in this order. Documents 00–02 are the "why"; 03–10 are the "how"; 11–15 are the "deliver".

| # | Document | What it covers | Rubric marks it targets |
|---|---|---|---|
| **00** | [Overview: Architecture & Technology](00-overview-architecture-and-stack.md) | **START HERE.** The whole system on one page: architecture, every technology, dataflow, and how the pieces connect | Context for all |
| **01** | [Architecture Decision — Lambda vs Kappa](01-architecture-decision.md) | The 8-criteria argument, the rejected alternative, conceded weaknesses, viva defence | **20** |
| **02** | [Technology Stack Justification](02-technology-stack.md) | Every layer: what we chose, what we rejected, why — tied to *this* scenario | **10** |
| **03** | [Requirement 1 — Simulated Data Sources](03-req-data-sources.md) | Both simulators, the simulated clock, realistic generation, seeded anomalies | **15** |
| **04** | [Requirement 1 — Ingestion & Kafka Design](04-req-ingestion-kafka.md) | Topics, partitions, keys, retention, compaction, Avro, DLQ, delivery semantics | **15** |
| **05** | [Requirement 2 — Speed Layer Processing](05-req-processing-speed-layer.md) | Structured Streaming: validation, enrichment, windows, watermarks, stateful idle detection | **15** |
| **06** | [Requirement 2 — Batch Layer Processing](06-req-processing-batch-layer.md) | Master dataset, Spark batch profitability job, the two-source join, restatement handling | **15** |
| **07** | [Requirement 3 — Storage & Serving Layer](07-req-storage-serving.md) | MinIO/Parquet, Postgres star schema, Redis, FastAPI — **and the reconciliation logic** | **10** |
| **08** | [Orchestration with Airflow](08-orchestration-airflow.md) | DAGs, sensors, retries, branching, the daily report task | **15** (processing) |
| **09** | [Requirement 4 — Observability](09-req-observability.md) | Metrics catalogue, log schema, OTel tracing, alert rules, Grafana dashboards | **10** |
| **10** | [Infrastructure & Docker Compose](10-infrastructure-docker.md) | Service topology, images, memory budget, WSL2 gotchas, reproducibility | **5** |
| **11** | [Code Quality, Testing & Repo Structure](11-code-quality-testing.md) | Directory tree, what gets tested and how, config management, CI | **5** |
| **12** | [Report Writing Plan](12-report-plan.md) | Section-by-section outline, page budget, diagrams, screenshots | **15** |
| **13** | [Demo Video & Submission](13-demo-and-submission.md) | Demo script with timestamps, submission checklist | Required |
| **14** | [Two-Week Schedule](14-schedule.md) | Day-by-day build order, critical path | — |
| **15** | [Risks & What to Cut](15-risks-and-cuts.md) | Ranked risk register and the cut-order if time runs short | — |

---

## The one-paragraph summary

We ingest continuous GPS/telemetry from a simulated fleet into **Kafka**, keyed by `vehicle_id` so each vehicle's event ordering is preserved. A **PySpark Structured Streaming** job forms the **speed layer**: it validates and enriches events, computes windowed utilization/earnings per zone, detects prolonged idling with stateful processing, and writes the approximate "right now" view to **Redis** — while simultaneously appending every validated raw event to **MinIO as Parquet**, partitioned by simulated date. That Parquet lake is the **immutable master dataset**. Once per simulated day, a fuel/garage partner drops an expense CSV; **Airflow** senses it, validates it, and triggers a **Spark batch job** that reads the day's Parquet partition, joins it with the expenses, and computes exact per-vehicle profitability into a **PostgreSQL star-schema data mart** — the **batch layer**. A **FastAPI serving layer** answers queries by merging the two views at an explicit `batch_high_water_mark` boundary, labelling every row with its source, so there is provably no gap and no double-counting. The entire pipeline is instrumented with structured JSON logs, Prometheus metrics, OpenTelemetry traces, Alertmanager rules and Grafana dashboards.

---

## Non-negotiables (do not let these slip)

These are the things that, if missing, cost the most marks:

1. **The architecture argument** must walk the lecturer's own eight criteria and honestly concede where they cut against us (`01`).
2. **The shared `transforms/` package** must be real — the same tested function called by both the streaming job and the batch job. This is our answer to the lecturer's stated Lambda weakness ("managing multiple codebases"), and it must exist *in code*, not just in prose.
3. **The serving-layer merge** (`serving/merge.py`) with the explicit high-water-mark boundary. This is our answer to the other stated Lambda weakness ("reconciling data between systems").
4. **At least one alert that visibly fires** during the demo.
5. **The daily consolidated report** must actually be produced as a file by Airflow.
6. **`make clean && make up && make demo` must work from a fresh clone.**

---

## Deliberate differences from the sibling project

This repository is submitted by a different group than `../hospital-vitals-kappa`. The two projects are genuinely different in architecture (Lambda vs Kappa), storage (Redis + MinIO/Parquet + PostgreSQL vs Cassandra), domain logic, dashboards and code layout. Nothing is shared between the repos — they are independent submissions that happen to sit in one workspace. See `11-code-quality-testing.md` for the structural differences.
