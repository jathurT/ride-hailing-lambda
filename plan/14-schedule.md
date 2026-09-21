# 14 — Two-Week Schedule

> Assumes **both projects are built in parallel** (worst case: the same person builds both). Where a day says "A" and "B", the work is genuinely different; where it says "shared", the same foundation module is written once and adapted.
>
> Duration per the PDF: **2 weeks**.

---

## Day 0 — Prerequisites (½ day, do this first)

- [ ] **Enable Docker Desktop WSL integration** (`10 §0`) and verify `docker run hello-world`
- [ ] Create `.wslconfig` with `memory=11GB`, `swap=8GB`, `wsl --shutdown`, restart (host is 15.6 GB — see `10 §0.2`)
- [ ] `docker pull` every image in both compose files (~8 GB; do it once, not during the build)
- [ ] `git init` both repos, `.gitignore`, initial commit
- [ ] Scaffold both directory trees empty
- [ ] Confirm `uv`, Python 3.11/3.12, `pdflatex` available

**Milestone:** `docker compose version` works; images cached locally.

---

## Day 1 — Shared foundations

Write once, adapt into both repos (they are separate submissions, so the code is copied and adjusted, not shared via a package).

- `simclock.py`, `config.py` (pydantic-settings with the watermark validator), `logging.py` (structlog JSON), `metrics.py`, `tracing.py` (OTel bootstrap), `kafka_client.py`, `serialization.py`
- Avro schemas for both domains
- Compose skeleton: Kafka (KRaft) + Schema Registry + Kafka UI healthy in **both** stacks, on their distinct port ranges
- `init` container: create topics, register schemas, write the clock epoch

**Milestone:** `make up` in each repo brings up Kafka and Kafka UI shows the topics.
**Risk:** the KRaft `CLUSTER_ID` dance. Budget an hour.

---

## Day 2 — Producers

- **A:** `telemetry_producer.py` — vehicle state machine, 12 zones, diurnal demand, fares, seeded anomalies (`V007`, `V113`), defect injection; `expense_dropper.py` with atomic drop, late/missing/restatement scenarios
- **B:** `bedside_monitor.py` — per-patient baselines, AR(1) vitals walk, circadian drift, sensor artefacts, the scripted `P014` deterioration; `lab_uploader.py`; `admissions_producer.py`
- Both: `/metrics`, `/health`, structured logs, graceful shutdown, retry/backoff, idempotent producer config

**Milestone:** events visible in Kafka UI in both stacks; producer metrics scraped by Prometheus.

---

## Day 3 — Storage layers + DDL

- **A:** MinIO buckets + prefixes; Postgres star schema DDL; Redis key design; DAO modules
- **B:** Cassandra keyspace + all query-first tables + TTLs; DAO module; verify no `ALLOW FILTERING` is needed by any query
- Both: `make db-init` idempotent from scratch

**Milestone:** `make clean && make up` recreates all schemas without manual steps.
**Risk (A):** Spark ↔ MinIO `s3a://` configuration (`hadoop-aws` jars, path-style access, endpoint). **Budget 1–2 hours; this reliably bites.**
**Risk (B):** Cassandra's 60–90 s start-up; get the healthcheck and `start_period` right now rather than during the demo.

---

## Days 4–5 — ★ CRITICAL PATH: Spark Structured Streaming ★

Everything downstream depends on this. If it slips, the schedule slips.

- **A:** the three queries — Q1 raw → Parquet (partitioned by `sim_date`), Q2 validate → enrich → window → aggregate → Redis via `foreachBatch`, Q3 stateful idle detection → alerts topic. Checkpointing for all three.
- **B:** the single pipeline — clean → validate → enrich (admissions broadcast) → window (trend) → NEWS2 scoring → lab join → Cassandra sinks + alerts topic.
- Both: transforms written as **pure DataFrame functions** from the start. Do not write them inline in the streaming job and promise to extract them later — that never happens, and the whole shared-logic argument depends on it.

**Milestone:** an event produced at one end appears in Redis/Cassandra at the other.
**Expect to lose half a day** to watermark-vs-simulated-time bugs. When windows come back empty, check the watermark arithmetic first (`03 §3.3`).

---

## Day 6 — Batch orchestration

- **A:** the Spark batch profitability job (trip reconstruction, haversine, `full_outer` join, rolling trend, upsert); `fleet_daily_reconciliation` DAG; `fleet_restatement_watcher`; the report renderer
- **B:** `orchestration/` DAGs — lab-file sensor → producer, daily report generation, retention, DQ checks, replay trigger; the replay runner

**Milestone:** a daily report file is produced automatically in both projects.

---

## Day 7 — Serving layer

- **A:** FastAPI with **`merge.py` — do not skimp on this.** It is the 20-mark architecture argument expressed in code. All endpoints, health checks, OpenAPI docs.
- **B:** ward endpoints, patient detail, risk history, alert feed, replay-comparison endpoint.

**Milestone:** `/docs` works; every endpoint returns real data from a live run.

---

## Day 8 — Observability, part 1: metrics and alerts

- Prometheus scrape configs; all exporters (kafka, postgres/redis or cassandra-jmx, statsd)
- `StreamingQueryListener` → Pushgateway
- Alert rules; Alertmanager routing; the alert-sink webhook
- **Verify alerts actually fire** — run every `make chaos-*` scenario and record the detection times

**Milestone:** an alert visibly fires and resolves in Alertmanager.

---

## Day 9 — Observability, part 2: dashboards and traces

- 4 Grafana dashboards per project, **provisioned as code**
- OTel Collector + Jaeger; producer/API/Airflow instrumentation; the micro-batch span with sampled links
- Loki + Promtail **only if ahead of schedule**

**Milestone:** dashboards appear automatically on a fresh `make up`; one end-to-end trace visible in Jaeger.

---

## Day 10 — Testing, hardening, READMEs

- Unit tests to the coverage gate on `transforms/` + `merge.py` + `simclock.py` (A) and `clinical/` + `simclock.py` (B)
- Integration smoke tests
- **`make clean && make up && make demo` from a fresh clone, twice** — fix every reproducibility bug found
- Write both READMEs
- CI workflow

**Milestone:** a fresh clone works end to end with no manual steps.

---

## Day 11 — Demo dry run and screenshot capture

Run in **three passes**, dropping a profile level between each so memory never binds.

**Pass 1 — `make up-obs` (the main session, ~2 h)**
- Run each stack for a full multi-simulated-day session
- Verify every scripted narrative fires on cue: A — `V007` idle, `V113` unprofitable, the day-3 restatement; B — `P014` deterioration, lab corroboration, the v1-vs-v2 replay comparison
- Capture R1–R8, R10, R12–R15 (`12 §3`)
- Fill in the measured-numbers table with real values

**Pass 2 — `make chaos-*` (still on `up-obs`)**
- Run every chaos scenario; capture the firing alerts (R9) and the Pipeline Health cliff
- Record the actual detection times for the report's verification table

**Pass 3 — ★ `make up-full`, one stack only, nothing else running (~30 min) ★**

This pass exists because Jaeger/OTel and the Spark master/worker pair only run in the `full`
profile (`10 §1.3`). **It is not optional** — skipping it forfeits observability marks and the
one screenshot that shows distributed execution.

- [ ] `make down` the *other* project first, and close anything else consuming RAM
- [ ] **R11 — the Jaeger trace waterfall** (producer → micro-batch → sink → API). This is the
      only evidence for the rubric's "tracing" requirement; without it that part of the
      10-mark observability criterion is unevidenced.
- [ ] **Spark UI with the real cluster** — the Executors tab showing tasks distributed across
      two workers, and the DAG/stage view showing the shuffle boundary. Pair it with the
      local-mode screenshot so the report can show both and explain the choice.
- [ ] Note the observed `up-full` memory figure for `10 §1.3` — the profile numbers are
      estimates from image defaults and configured limits, not measurements, and this is
      where they get validated.
- [ ] `make down` immediately afterwards; drop back to `up-obs` for anything further

Then: rehearse both demo scripts with a timer.

**Milestone:** every screenshot captured **including R11 and the cluster view**; both demos rehearsed and timed.

---

## Days 12–13 — Reports

- **Day 12: diagrams first.** They take longer than expected. D1–D4 per project in draw.io/Mermaid, exported as SVG/PDF.
- **Day 12–13: prose.** Section 3 (architecture, 20 marks) first, while fresh. Then 5, 7, 9, then the rest.
- **Day 13: produce the PDFs** via the LaTeX pipeline; proofread both; check page counts (8–15).

**Milestone:** two report PDFs, complete, proofread.

---

## Days 13–14 — Video, polish, submit

- Record both demo videos (9 minutes each); re-record segments as needed
- Final repo polish: tag `v1.0-submission`, verify `.env` is not committed
- Individual contribution statements
- Assemble and submit both bundles

---

## Critical path and slack

```
Day 0  prereqs
Day 1  foundations ──┐
Day 2  producers ────┤
Day 3  storage ──────┤
Day 4  ★ STREAMING ★ │  ← everything below depends on this
Day 5  ★ STREAMING ★ ┘
Day 6  batch/orchestration
Day 7  serving
Day 8  observability I
Day 9  observability II
Day 10 testing        ← first real slack; can compress to ½ day
Day 11 dry run        ← cannot compress; the screenshots are report inputs
Day 12 diagrams+report
Day 13 report+video   ← cannot compress
Day 14 submit
```

**If days 4–5 overrun, cut from day 9 (observability II), not from day 11 or 13.** Screenshots and the report are graded artefacts; a fourth Grafana dashboard is not.

The cut order when time runs short is in `15-risks-and-cuts.md`.
