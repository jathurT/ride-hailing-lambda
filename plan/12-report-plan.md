# 12 — Report Writing Plan

> **PDF:** *"Report (recommended 8–15 pages)"* covering: use case and business requirements; architecture decision Lambda vs Kappa **with explicit justification and rejected alternative**; architecture diagrams covering ingestion, processing, storage and serving; technology stack with justification per component; observability design — *"what is measured, how, and why"*; results with screenshots; limitations, trade-offs, and what you would do differently at production scale.
>
> **Rubric weight: 15 marks** — *"clarity of architecture diagrams, explanation of design decisions, tech stack rationale, and presentation of results; **honesty about limitations**."*
>
> Note that the report carries 15 marks but *describes* the sections worth another 40 (architecture 20 + stack 10 + observability 10). **A weak report loses marks twice.** Write it as the primary deliverable, not as documentation of the code.

---

## 1. Target: 13 pages of body

| § | Section | Pages | Sources from this plan |
|---|---|---|---|
| 1 | Introduction & Use Case | 0.75 | `00 §1` |
| 2 | Business Requirements Interpretation | 0.75 | `01 §1` |
| 3 | **Architecture Decision: Lambda vs Kappa** | **3.0** | `01` (whole document) |
| 4 | Architecture Design & Diagrams | 2.0 | `00 §3`, `07 §5.1` |
| 5 | Technology Stack Justification | 1.5 | `02` |
| 6 | Implementation | 2.5 | `03`–`08` |
| 7 | Observability Design | 1.5 | `09` |
| 8 | Results | 1.5 | `13` |
| 9 | Limitations, Trade-offs & Production Scale | 1.0 | §5 below |
| 10 | Conclusion | 0.4 | |
| — | References + Appendices | (excluded from the 13) | |

Section 3 gets the most space because it carries the most marks. Resist the temptation to spend pages on implementation detail — the code is a separate deliverable.

---

## 2. Section-by-section notes

### §1 Introduction & Use Case (0.75 p)
Scenario in two paragraphs. Restate the business question verbatim and **immediately decompose it into its two halves** (`01 §1`) — this sets up section 3 and signals from page 1 that the architecture was driven by the requirement. Name the stakeholders (dispatch operations; finance). State group members and, per the PDF, include the **individual contributions statement** (appendix is fine, but it must exist).

### §2 Business Requirements Interpretation (0.75 p)
A requirements table with explicit non-functional SLOs. Markers reward requirements that are *measurable*:

| ID | Requirement | Type | Target | Satisfied by |
|---|---|---|---|---|
| FR-1 | Live fleet utilization by zone | Functional | — | Speed layer → `/api/v1/fleet/live` |
| FR-2 | Per-vehicle daily profitability vs expenses | Functional | — | Batch layer → `fact_vehicle_daily_pnl` |
| FR-3 | Identify vehicles *becoming* unprofitable | Functional | 7-day trend | `classification` column |
| FR-4 | Threshold alert on prolonged idling | Functional | ≥45 sim-min | Stateful streaming |
| FR-5 | Consolidated daily report | Functional | 1/simulated day | Airflow → PDF |
| NFR-1 | Live view freshness | Latency | < 30 s real | Micro-batch trigger |
| NFR-2 | Profitability accuracy | Accuracy | Exact | Batch over master dataset |
| NFR-3 | Restatement handling | Correctness | Idempotent re-run | Upsert on `(vehicle_id, sim_date)` |
| NFR-4 | Pipeline failure detection | Observability | < 3 min | Alert rules |
| NFR-5 | Reproducibility | Ops | 1 command | Docker Compose |

### §3 Architecture Decision (3.0 p) — the big one
Structure exactly as `01 §6`. Non-negotiables:
- Sub-headings named after **the module's five criteria**.
- The 8-criterion summary table.
- **Both concessions** (complexity, cost) stated as concessions, with the code-level mitigations.
- A full, fair statement of the Kappa case *before* rejecting it — including the compacted-expenses-topic design we genuinely considered.
- The falsification conditions (`01 §4.3`) — "we would switch to Kappa if…". Very few reports do this and it reads as confidence.

### §4 Architecture Design & Diagrams (2.0 p)

Four diagrams. **Drawn, not screenshotted** — hand-drawn-looking or ASCII diagrams cost marks under "clarity of architecture diagrams".

| Diagram | Tool | Shows |
|---|---|---|
| **D1 — Context** | draw.io → SVG → PDF | External actors (fleet, fuel partners, dispatch, finance) and the system boundary |
| **D2 — Layered architecture** ★ | draw.io | The full dataflow from `00 §3`, with the three layers **labelled in the module's vocabulary** (batch layer / speed layer / serving layer) and every technology named. This is the report's centrepiece — give it a full half page. |
| **D3 — Event sequence** | Mermaid → SVG | One telemetry event: producer → Kafka → Spark micro-batch → Redis → API → dashboard, with the trace-id carried through |
| **D4 — The merge boundary** ★ | draw.io | The timeline from `07 §5.1`. Small, but it is the visual proof that the reconciliation problem was solved. |

Optional D5: the simulated-clock timeline (real minutes vs simulated days), which makes §6.1 much easier to follow.

Use one consistent colour convention across all diagrams (e.g. blue = speed path, green = batch path, grey = infrastructure) and state it in a legend.

### §5 Technology Stack (1.5 p)
The summary table from `02 §11`, with the **"Trade-off accepted"** column kept — it is what turns a list into a justification. Then 3–4 short paragraphs expanding only the choices that most need defending: **Spark over Storm**, **Redis for the speed layer**, **Parquet on object storage over a database for the master dataset**, and **Avro over JSON**.

Add the honesty paragraph: *"Kafka, Spark, Airflow, PostgreSQL and the NoSQL taxonomy from which Redis is drawn were covered in the module. MinIO/S3, Parquet, Prometheus, Grafana, Alertmanager and OpenTelemetry were not; they are deliberate extensions and are introduced as such."*

### §6 Implementation (2.5 p)
- **6.1 Simulated sources and the simulated clock** — the declaration block from `03 §7` verbatim. The PDF demands the clock be stated clearly; make it a call-out box so it cannot be missed.
- **6.2 Ingestion** — the topic table from `04 §1`, and the `vehicle_id` partition-key argument (`04 §1.2`), which is the strongest ingestion-design point.
- **6.3 Processing** — the three-query diagram; windowing and watermark choices in simulated time; the `full_outer` join with its three cases; the trend classification.
- **6.4 Storage** — star schema diagram; Redis key design; the MinIO layout.
- **6.5 Serving** — endpoint table with the `source` column; the merge algorithm; the self-describing response.

### §7 Observability (1.5 p)
The rubric wants *"what is measured, how, and why"* — so use a three-column table, and make sure the **why** column is not empty. Include: the log schema; the metric catalogue by stage; the **pipeline-health vs business alerts** distinction (this is the most original point in the section); the tracing approach **with its stated limitation**; the dashboard list.

### §8 Results (1.5 p)
See §3 below.

### §9 Limitations & Production Scale (1.0 p)
See §5 below. **Do not shorten this section.** The rubric names "honesty about limitations" explicitly, and it is the cheapest section to write well.

### §10 Conclusion (0.4 p)
What was built, whether it answers the business question, and the single most important thing learned. One honest sentence about what you would do differently beats three paragraphs of summary.

---

## 3. Results — the screenshot plan

Capture these during the day-11 dry run, at 1920×1080, cropped, with sensitive-looking values consistent across shots (same simulated day where possible).

| # | Screenshot | Proves |
|---|---|---|
| R1 | Grafana **Fleet Operations** dashboard, mid-run | The live half of the business question is answered |
| R2 | Grafana **Batch Reconciliation** — top 10 unprofitable, with `V113` at the top | The daily half is answered |
| R3 | Page 1 + the profitability table of the **generated daily report PDF** | The consolidated report deliverable exists |
| R4 | **Airflow grid view** — several green simulated-day columns | The batch layer runs on schedule |
| R5 | **Airflow graph view** with the branch taken on a failed validation | Branching and error handling |
| R6 | **Kafka UI** — topics with per-partition counts | Even key distribution, no hotspotting |
| R7 | **Kafka UI** — consumer groups with three groups and their lag | Three independent consumers |
| R8 | **Spark UI** Structured Streaming tab — input/processing rate, batch duration | The streaming job's health |
| R9 | **Alertmanager** with `NoTelemetryIngested` firing after `make chaos-kill-producer` | The required alert rule works |
| R10 | Grafana **Pipeline Health** during the same incident — the throughput cliff | Observability locates failures |
| R11 | **Jaeger** trace waterfall: producer → micro-batch → sink → API | Tracing across stages |
| R12 | **`/docs`** showing the merged response model with `source` and `consistency` | The reconciliation contract is explicit |
| R13 | **Before/after the restatement** — same vehicle's profitability changing | The reprocessing argument, executed |
| R14 | **MinIO console** — `sim_date=` partitions and `reports/` | The master dataset is real |
| R15 | `make test` output with coverage | Testing |

Also include a **measured numbers table** — reports that quantify score better than reports that describe:

| Metric | Observed |
|---|---|
| Sustained ingestion rate | ~240 events/s |
| End-to-end latency (producer → API visible), p50 / p95 | _measure_ |
| Micro-batch duration, mean / p95 | _measure_ |
| Batch job duration (72k rows) | _measure_ |
| Restatement re-run duration | _measure_ |
| Speed-vs-batch divergence, `active_vehicles` / `earnings` | _measure_ |
| Alert detection time (producer kill → alert firing) | _measure_ |
| Cold start to first dashboard data | _measure_ |

---

## 4. Diagram production

- **draw.io / diagrams.net** (desktop or web) for D1, D2, D4. Save `.drawio` sources in `docs/diagrams/` — version-controlled diagrams are a code-quality signal.
- **Mermaid** for D3 (sequence) and any state diagrams; render to SVG with `mmdc`.
- Export everything to **PDF or SVG**, never PNG — raster diagrams look poor in a printed report.
- Consistent palette and a legend on D2.

---

## 5. Limitations — write these honestly

The rubric rewards honesty here, and every one of these is a thing a viva could otherwise catch you on.

**Demo-scale simplifications**
- Single Kafka broker, `replication.factor=1` — no fault tolerance. A broker loss loses unconsumed data.
- Kafka retention reduced from the 7-day design value to ~1 hour / 2 GB for laptop constraints (`04 §1.4`).
- **Spark runs in `local[4]` mode for the default demo profile**, not on a standalone cluster, because the host has 15.6 GB of RAM (`10 §1.3`). Structured Streaming semantics — windowing, watermarking, checkpointing, state handling — are identical; what is not demonstrated is task distribution across separate executor nodes. The cluster topology is brought up separately under `make up-full` for one screenshot. **State plainly that at 240 events/second the demo was never a distributed-scale demonstration in any configuration** — it demonstrates architectural correctness and pipeline behaviour, and the performance table in §8 gives the scaling analysis instead.
- 150 vehicles instead of a realistic several thousand.
- No authentication, no TLS, no ACLs anywhere. Default credentials in `.env.example`.

**Correctness and semantics**
- **End-to-end exactly-once is not achieved.** At-least-once with idempotent sinks (`04 §5`). Say this plainly.
- Speed-layer distinct counts are HyperLogLog (~2% error) by design.
- Earnings in the live view lag by up to one trip duration (`05 §3.4`).
- Events later than the 30-simulated-minute watermark are dropped from windowed aggregates (they still reach the master dataset).
- No ACID on the lake — a crashed writer can leave a partial Parquet file; mitigated by the streaming commit log, not eliminated.
- Changing the streaming aggregation invalidates checkpoints and requires a reset.

**Observability**
- **Per-event distributed tracing is not achieved** — micro-batch granularity with sampled links (`09 §5.2`).
- Pushgateway retains last values, so a dead job would look alive; mitigated by alerting on progress-timestamp *age*.
- Loki/log aggregation is optional and may not be in the final build.

**Data realism**
- Straight-line trip paths, not road-network routing; fares from a simple model; expenses generated from our own simulated distance, so the "independent source" is only pseudo-independent. **This is the most important simplification to admit**, because the whole reconciliation story would be more convincing with a genuinely independent cost source.

**What we would do differently at production scale**
- Kafka: RF=3, `min.insync.replicas=2`, multi-AZ, tiered storage; mTLS + SASL + ACLs; Schema Registry with `FULL` compatibility and CI-enforced schema checks.
- Processing: Spark on Kubernetes with dynamic allocation; RocksDB state backend for the stateful detector; separate clusters for speed and batch so a batch job cannot starve the streaming job.
- Storage: **Delta Lake or Apache Iceberg** on the master dataset for ACID, schema evolution and time travel — which would make restatements a `MERGE INTO` and give free auditability. A managed warehouse (Snowflake/BigQuery) for the mart.
- Transformations: **dbt** for the mart layer with tests and lineage; **Great Expectations** for data quality with quarantine and alerting.
- Serving: horizontally-scaled API behind a load balancer; a read replica for Postgres; Redis Cluster.
- Observability: OTel across every component with a proper backend (Tempo/Datadog); SLOs with error budgets rather than static thresholds; on-call routing.
- Governance (tying to the module's own pillars): a **data catalogue with lineage**, hot/warm/cold **tiering and retention policies**, access control and audit trails, PII handling for driver data under GDPR.
- Process: blue/green deploys for the streaming job; schema-change CI gates; a staging environment with replayed production traffic.

---

## 6. Production tooling

Write the source in **Markdown** in `docs/report/`, then produce the PDF with the **`professional-latex-pdf-engine`** skill available in this workspace (`pdflatex` and `lualatex` are installed on the host). Keep diagrams as external SVG/PDF assets so they can be regenerated without touching prose.

**Timeline:** diagrams on day 12 (they take longer than expected), prose on days 12–13, final PDF on day 13. Do **not** leave the report to the last day — it carries 15 marks directly and describes 40 more.

---

## 7. Final checklist

- [ ] 8–15 pages of body (target 13)
- [ ] Every PDF-mandated section present
- [ ] Simulated clock stated clearly, in a call-out
- [ ] Rejected alternative (Kappa) argued fairly before being rejected
- [ ] All four diagrams vector, legible, consistently styled, with a legend
- [ ] Technology table has a per-component justification tied to *this* scenario
- [ ] Observability section answers **what / how / why**
- [ ] ≥ 12 screenshots, all legible at print size
- [ ] Measured-numbers table filled in with real values
- [ ] Limitations section is genuinely honest, not performative
- [ ] Individual contributions statement included
- [ ] References: module lecture decks cited explicitly, plus Kafka/Spark/Airflow docs, NEWS-style external sources where used
- [ ] Proofread; no placeholder text; no "TODO"
