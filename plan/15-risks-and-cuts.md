# 15 — Risks & What to Cut

---

## 1. Risk register

Ordered by expected cost (probability × impact).

| # | Risk | Prob | Impact | Mitigation | Trigger to act |
|---|---|---|---|---|---|
| R1 | **Watermark / simulated-time bugs** — windows come back empty and the cause is non-obvious | High | High | Watermark expressed in simulated time with the arithmetic in a comment; a startup validator rejects an unsafe combination; a unit test fails CI if the speed-up changes without revisiting it | Windows empty for >1 h of debugging → drop the speed-up to 96× temporarily to widen the real-time tolerance and confirm the logic before restoring it |
| R2 | **`flatMapGroupsWithState` (idle detector)** — fiddly PySpark API, hard to debug | High | Medium | Written as a plain Python function testable without Spark | >½ day spent → switch to the Redis-side fallback in `05 §6` and document the trade-off honestly |
| R3 | **Spark ↔ MinIO `s3a://` configuration** — missing `hadoop-aws` jars, endpoint/path-style settings | High | Medium | Pin the jar versions in the Spark image build on day 3, not day 5 | If unresolved in 2 h → fall back to a local bind-mounted volume for Parquet, and note the S3 API as the production target |
| R4 | **Memory exhaustion** — host is only **15.6 GB**, so Docker gets ~11 GB (`10 §0.2`) | **High** | High | Compose **profiles** (`core`/`run`/`obs`/`full`); **Spark in local mode by default** (saves 3.5 G); explicit `mem_limit` everywhere; `KAFKA_HEAP_OPTS`; **only one stack up at a time**; 8 GB swap as a safety net | Swap thrashing → drop a profile level (`up-full` → `up-obs` → `up`); `make down` the other project |
| R5 | **Two projects, one person, two weeks** | Medium | High | Shared foundations written once on day 1; the two projects diverge only where they must | Behind by >1 day at day 7 → freeze project B's observability at the "lean" level and finish A fully first |
| R6 | **OpenTelemetry through Spark** — trace propagation into a micro-batch engine | Medium | Medium | Scope is already limited to batch-granularity spans with sampled links, and the limitation is a *stated report finding* rather than a failure | Not working by end of day 9 → ship tracing on producers + API + Airflow only, and expand the limitation paragraph |
| R7 | **Airflow start-up problems** — DB init, connection URIs, DAG import errors | Medium | Medium | `airflow db migrate` in an init container; `test_dag_integrity.py` catches import errors before the UI does | >2 h lost → use `airflow standalone` in a single container for the demo, note it as a simplification |
| R8 | **Cassandra start-up/heap** (project B) | Medium | Medium | Pin `MAX_HEAP_SIZE=1G`, `HEAP_NEWSIZE=256M`; healthcheck with `start_period: 90s` | Instability persists → fall back to TimescaleDB and rewrite the storage justification honestly (it is still defensible) |
| R9 | **Avro / Schema Registry friction** | Low | Medium | Registered by the init container; Kafka UI wired to the registry for readable messages | >3 h lost → switch to JSON + Pydantic contracts, document the change and what was given up |
| R10 | **Demo narrative doesn't fire on cue** | Low | High | Everything seeded; rehearsed on day 11 with a timer | Rehearsal reveals timing drift → adjust the scripted trigger times in config, not the code |
| R11 | **Report left too late** | Low | **Very High** | Diagrams on day 12, prose 12–13; §3 written first | Behind at day 12 → write §3, §5, §7, §9 (the graded-content sections) and compress §6 |
| R12 | Laptop sleep breaks the simulated clock mid-run | Medium | Low | `SimClockDrift` alert; `make restart-sim` re-anchors | — |

---

## 2. Cut order

If time runs short, cut **in this order**. Everything above the line can go without materially affecting the grade; everything below it is load-bearing.

| Order | Cut | Marks at risk | Why it's safe |
|---|---|---|---|
| 1 | **Loki / log aggregation** | ~0 | Structured JSON logging is the requirement; `docker compose logs` satisfies it. Loki is presentation. |
| 2 | **Grafana dashboards 3 & 4** (Batch Reconciliation, Traces & Logs) | ~1 | Pipeline Health + the business dashboard already evidence observability. |
| 3 | **OTel tracing reduced** to producer + API + Airflow only (drop the Spark micro-batch span) | ~1–2 | The limitation is already a stated report finding; reduce scope and expand the honesty. |
| 4 | **`flatMapGroupsWithState`** → Redis-side idle tracking | ~0–1 | Arguably better engineering at this scale anyway. Document the trade-off. |
| 5 | **Avro + Schema Registry** → JSON + Pydantic | ~1 | Loses a taught concept; document the reasoning. Removes two containers and a class of bugs. |
| 6 | **SCD Type 2** on `dim_vehicle` → SCD Type 1 | ~0.5 | Nice warehousing detail, not required by the brief. |
| 7 | **The reconciliation-delta chart** (`07 §5.4`) | ~1 | A bonus observability point; the merge itself must stay. |
| 8 | **Fleet size 150 → 50**, event rate reduced | ~0 | Purely a resource lever. Update the stated numbers everywhere. |
| ══ | **════ DO NOT CUT BELOW THIS LINE ════** | | |
| — | The Lambda-vs-Kappa argument with the rejected alternative | **20** | The largest single criterion |
| — | The shared `transforms/` package and its test | ~5 | The answer to the module's stated Lambda weakness |
| — | `serving/merge.py` and the high-water-mark | ~5 | The answer to the other stated weakness |
| — | Both simulated sources with the stated clock | **15** | Explicit requirement |
| — | The streaming job with real windowing and watermarks | **15** | Explicit requirement |
| — | The batch job with the two-source join | **15** | Explicit requirement — this is the "join between the two sources" the PDF names |
| — | The daily consolidated report file | ~5 | Explicit deliverable |
| — | ≥1 alert rule that visibly fires | **10** | Explicit requirement |
| — | Structured logging across all stages | **10** | Explicit requirement |
| — | Docker Compose that works from a fresh clone | **5** | Explicit rubric line |
| — | The report | **15** | And it describes 40 more marks |
| — | The demo video | Required | Non-submission risk |

---

## 3. Fallback positions, written in advance

Decide these now so they are not made under pressure at 2 a.m.:

| If this fails | Fall back to | Report says |
|---|---|---|
| Spark ↔ MinIO | Local volume for Parquet | "Object storage via the S3 API is the production target; the demo uses a local filesystem mount with an identical partition layout." |
| `flatMapGroupsWithState` | Redis-side idle tracking in `foreachBatch` | "State is held in Redis rather than a checkpointed Spark state store; simpler and adequate at this scale, at the cost of exactly-once state recovery." |
| Avro | JSON + Pydantic | "Schema Registry was scoped out; contracts are enforced by Pydantic models with a versioned schema field." |
| SparkSubmitOperator | `BashOperator` → `docker exec spark-master spark-submit` | Nothing — functionally equivalent. |
| Airflow full deployment | `airflow standalone` | "Single-process Airflow for the demo; LocalExecutor with a separate metadata database is the intended configuration." |
| Cassandra (project B) | TimescaleDB | Rewrite the storage justification around hypertables, continuous aggregates and retention policies — still a strong, honest argument. |
| Grafana Redis datasource | Infinity/JSON datasource → our own FastAPI | Actually **better**: the dashboard exercises the serving layer rather than bypassing it. |

---

## 4. Weekly checkpoints

**End of day 5 — go/no-go on scope.** If the streaming job is not producing data into the serving store by the end of day 5, immediately execute cuts 1–3 and reassess at day 7.

**End of day 9 — feature freeze.** No new features after day 9. Days 10–14 are testing, screenshots, report and video only. Adding a feature on day 11 is how projects arrive with great code and no report.

**End of day 11 — screenshot freeze.** All results captured. If a screenshot is missing after day 11, the report describes what was built rather than waiting for a re-run.
