# Live verification — serving layer, observability, orchestration

> Historical record of an earlier run. The report's numbers now come from the final
> clean run in [`rerun-measurements.md`](rerun-measurements.md); the R-numbered figures and
> TikZ diagrams named below were replaced.

Captured from a running stack on a fresh `make clean && make up`, simulated clock
anchored 2026-03-01, `SIM_DAY_SECONDS=300` (one simulated day per 5 real minutes,
a 288x speed-up). Every number below was read from the live system, not computed
by hand. Figures for the report should be cited from here.

---

## 1. Batch layer — the cross-check that matters

`make backfill DAYS=7` over the first four complete simulated days, then
`make batch-check`:

| check | result |
|---|---|
| P&L rows | **600** = 4 days x 150 vehicles |
| distinct sim_dates | 4 |
| zone-hourly rows | 1,140 (276 + 288 x 3; day 1 has 23 hours, the run started mid-hour) |
| restated rows after re-run | **0** — idempotent |
| watermark | advanced once, on the final day only |

### Two independent paths to one number

`fact_vehicle_daily_pnl.revenue` is summed per VEHICLE from a trip-deduplicated
join against the partner expense file. `fact_zone_hourly.earnings` is summed per
ZONE per HOUR from a different deduplication rule (last on-trip ping, chosen for
determinism across zone boundaries). Different code paths, different groupings,
different dedup strategies:

| sim_date | P&L revenue | zone earnings | delta |
|---|---|---|---|
| 2026-03-01 | 46,878.04 | 46,878.04 | **0.00** |
| 2026-03-02 | 50,920.07 | 50,920.07 | **0.00** |
| 2026-03-03 | 49,736.90 | 49,736.90 | **0.00** |
| 2026-03-04 | 49,808.52 | 49,808.52 | **0.00** |

Agreement to the penny on all four days. This is the strongest single piece of
evidence that the batch layer is correct, and it is the check to re-run after any
change to either aggregation.

### Trend classification

`MIN_DAYS_FOR_TREND = 4`, so the first three days are correctly unclassifiable:

| sim_date | classification | count |
|---|---|---|
| 2026-03-01 .. 03 | INSUFFICIENT_DATA | 150 each |
| 2026-03-04 | HEALTHY | 96 |
| 2026-03-04 | WATCH | 54 |

---

## 2. Serving layer — the merge

`GET /api/v1/fleet/utilization?from=2026-03-02&to=2026-03-06`, watermark at
2026-03-04, simulated clock inside 2026-03-05:

```
rows        : 48   (36 batch + 12 speed)
degraded    : False
consistency : batch-complete-through 2026-03-04; 1 date(s) in neither view ...
```

| sim_date | served by |
|---|---|
| 2026-03-02 | batch |
| 2026-03-03 | batch |
| 2026-03-04 | batch  <- the boundary date, owned by exactly one side |
| 2026-03-05 | speed |

**No date appears twice and none is missing.** That is the half-open interval rule
holding on live data, and it is the property `test_merge_boundary.py` asserts
exhaustively without a stack running.

### Degradation, measured

| scenario | HTTP | body |
|---|---|---|
| both stores up | 200 | 48 rows, `degraded: false` |
| `docker stop fleet-redis` | **200** | 36 batch rows, `X-Data-Degraded: true`, `missing_stores: ["redis"]` |
| `docker stop fleet-postgres-mart` | **200** | 12 speed rows, `batch_complete_thru: null` + *"watermark unknown while postgres is unreachable"* |
| both stopped | **503** | `no serving store reachable (postgres, redis)` |
| restart both | 200 | `degraded: false`, pool reconnected with no intervention |

The Postgres-down case is the subtle one: losing the database also loses the
watermark, so `hwm = null` there means *unknown*, not *cold start*. The two are
distinguished in the response so a client cannot mistake an outage for a pipeline
that has never run.

### Connection pooling

Measured on `/health/deep`, which times each store:

| | connection per request | pooled |
|---|---|---|
| Postgres | 30–65 ms | **8–15 ms** |
| Redis | 1.5–10 ms | **0.4–1.3 ms** |

---

## 3. Observability

`make obs-check` against the live stack:

- **7/7 Prometheus targets up**: api, kafka-exporter, both producers, prometheus,
  pushgateway, streaming-job.
- **28 `spark_streaming_*` series** in the Pushgateway across **5 distinct queries**
  — the `StreamingQueryListener` is pushing, which is the only way these queries are
  observable at all.
- Grafana dashboard `fleet-pipeline-health` provisioned automatically, 20 panels,
  both datasources registered.

Spot-checks through the Prometheus query API:

| query | value | note |
|---|---|---|
| `sum(rate(events_produced_total{status="ok"}[1m]))` | **240.2/s** | against a configured `FLEET_TARGET_EPS=240` — independent confirmation the producer hits its target |
| `max(time() - spark_streaming_last_progress_timestamp)` | 10.2 s | healthy against a 10 s trigger |
| `sum(rate(sink_writes_total[1m]))` | 2.19/s | |
| `serving_merge_boundary_crossings_total` | increments per boundary-spanning request | 0 would mean the merge is never exercised |

### One panel has no data, and it is supposed to

`kafka_consumergroup_lag` returns nothing, and
`kafka-consumer-groups --list` on the broker returns **no groups at all**. This is
direct confirmation of the claim in `plan/09 section 3`: the Structured Streaming
queries checkpoint their own offsets and never commit to a consumer group, so
consumer lag cannot measure them. Forcing `kafka.group.id` to make the metric work
is what killed the DLQ query after ~4,500 micro-batches. The panel is retained and
labelled, because the empty panel is itself the evidence.

---

## 3b. Alerting — and the alert that could not fire

Six rules in `observability/prometheus/alerts.yml`, routed through Alertmanager.
The assignment's minimum requirement is *"at least one basic alert or health-check
rule (e.g. no data received in N minutes)"*; `NoTelemetryIngested` is literally that
example.

### The finding: writing the rule was not enough

The first version read:

```promql
sum(rate(events_produced_total{status="ok"}[2m])) == 0
```

It loaded cleanly, showed up in the Prometheus UI, and looked correct. The producer
was then stopped and the alert **stayed inactive for six and a half minutes**.

The cause is a Prometheus trap worth stating plainly in the report: **absent is not
zero.** When the producer process dies its scrape target goes down, the series goes
stale and disappears entirely. `sum(rate(...))` over no series returns an *empty
vector*, and `empty == 0` is also empty — so the rule for "no data received" was
defeated by there being no data. Confirmed directly against the query API: the
expression returned `EMPTY VECTOR`, while `(… or vector(0)) == 0` returned `0`.

Two changes followed:

1. `or vector(0)` substitutes a literal zero when the series is absent, which is the
   condition actually meant.
2. A companion rule, `ProducerTargetDown`, on `up{...} == 0`. `up` is synthesised by
   Prometheus for every configured target, so unlike an application metric it is
   never absent — it goes to 0 instead of vanishing. "The process is gone" and "the
   process is running but emitting nothing" are different incidents with different
   runbooks, and only the second is visible to a metric-based rule.

**An alert nobody has watched fire is not evidence of anything.** That is the whole
argument for the chaos target existing.

### Measured lifecycle

| event | timing |
|---|---|
| `make chaos-kill-producer` → `NoTelemetryIngested` **firing** | **71 s** (60 s `for:` + evaluation delay) |
| `make chaos-heal` → all rules back to inactive | **51 s** |

### Inhibition — a second thing that had to be watched

Stopping the producer raises eight alerts: one `NoTelemetryIngested`, one
`ProducerTargetDown`, and five `StreamingQueryStalled` (one per streaming query).
Only the first is a cause; the rest are consequences of it.

The first inhibition rule matched `severity="warning"` with `equal: [stage]` and
suppressed **nothing** — the consequential alerts are `critical`, not warnings, and
they carry `stage=process` while the cause carries `stage=ingest`. An inhibition that
matches nothing is indistinguishable from no inhibition at all, and only watching it
run revealed that.

Corrected to a causal chain — `ProducerTargetDown` suppresses `NoTelemetryIngested`,
which suppresses `StreamingQueryStalled` — and re-measured:

```
5 alerts | 1 ACTIONABLE | 4 suppressed as consequences
  -> ProducerTargetDown  telemetry-producer:8001
  (suppressed) StreamingQueryStalled q_master / q_dlq / q_vehicles
  (suppressed) NoTelemetryIngested
```

On-call sees one alert naming the actual cause rather than five symptoms.

### Rule set

| alert | fires on | severity |
|---|---|---|
| `NoTelemetryIngested` | no events for 1 min (with the absent-vs-zero fix) | critical |
| `ProducerTargetDown` | `up == 0` on producers / streaming-job / api | critical |
| `StreamingQueryStalled` | progress timestamp older than 120 s | critical |
| `HighDLQRate` | dlq output rows / master output rows > 5% | warning |
| `BatchWatermarkStale` | watermark more than 2 simulated days behind | warning |
| `ServingLayerDegraded` | a store unreachable and responses degrading | warning |

`HighDLQRate` is derived from the two queries' output-row counts rather than a counter
inside the DLQ sink: that sink writes straight to Kafka with no `foreachBatch`, so
instrumenting it directly would mean restructuring the query and resetting its
checkpoint. The listener already receives `numOutputRows` per query, so the ratio is
free.

---

## 3c. Dashboards — answering the question, not just monitoring the pipeline

Three dashboards, all provisioned from `observability/grafana/dashboards/` and all
appearing on a clean `make up-obs` with no manual import.

| dashboard | answers |
|---|---|
| **Pipeline Health** | is the system working — INGEST / PROCESS / STORE / SERVE |
| **Fleet Operations** | *"fleet utilization and earnings by area right now"* |
| **Batch Reconciliation** | *"which vehicles are becoming unprofitable"* |

The two business dashboards are driven through the **Infinity datasource pointed at
our own FastAPI**, not by reading Redis and Postgres directly. That is deliberate
(`plan/07 §6`): a dashboard that bypasses the serving layer proves nothing about the
serving layer, whereas one that breaks when the API breaks is telling the truth about
the system. Verified through Grafana's own query API:

| panel query | result |
|---|---|
| `/api/v1/fleet/zones` | 200, 12 rows of live zone data |
| `/api/v1/vehicles/unprofitable?limit=10` | 200, 10 rows |
| `/api/v1/pipeline/status` | 200, 1 row |
| `/api/v1/alerts/idle?limit=50` | 200, populated (see below) |

### A defect the dashboard exposed: the alert feed deleted itself

Building the idle-alerts panel surfaced a bug nothing else would have. `ZCARD
fleet:alerts:idle` was oscillating between **200 and 0** while alerts streamed
steadily into `fleet.alerts.v1` the whole time — the topic grew by 123 messages in 45
seconds while the Redis feed was intermittently empty.

Cause: the feed carried the standard speed-view TTL, and **a Redis TTL applies to the
whole key, not to individual members**. Two simulated hours is **25 real seconds** at
a 288× clock, so the entire feed was deleted 25 seconds after the last push — all 200
alerts at once — and reappeared on the next one.

The TTL existed to bound memory. The `ZREMRANGEBYRANK` trim immediately above it
already does exactly that, permanently, at 200 members. The TTL bought nothing and
cost the feed, so it was removed; every other speed-view key keeps its TTL, because
those are continuously overwritten and a stale one really is garbage. This key is an
append-only feed and is the one exception.

Confirmed after the fix: `TTL = -1` and the feed holds steady at its 200-member cap
instead of collapsing to zero.

This is worth stating in the report because of *how* it was found. The pipeline was
healthy, Kafka had every alert, no error appeared anywhere, and every test passed. It
took building a panel that displayed the data to notice that the data was not there
half the time.

---

## 3d. An operational trap found by the chaos test itself

After the degradation demo — `docker stop fleet-redis`, `docker stop
fleet-postgres-mart`, then `docker start` on both — six integration tests that had
been passing began failing with `psycopg.errors.ConnectionTimeout`.

Postgres was healthy. Seven of its hundred connections were in use. `make psql`
worked. The API answered `/api/v1/vehicles/unprofitable` with HTTP 200 and
`/health/deep` reported both stores up.

`docker port fleet-postgres-mart` returned **nothing at all**:

| container | published ports |
|---|---|
| `fleet-postgres-mart` | **none** |
| `fleet-redis` | **none** |
| `fleet-telemetry-producer` | 8001 → 8001 |
| `fleet-api` | 8000 → 8000 |
| `fleet-kafka` | 9092 → 9092 |
| `fleet-grafana` | 3000 → 3000 |

Only the two containers brought back with `docker start` had lost their host port
bindings, while compose still *declared* `5442:5432` and `6389:6379`. Everything
inside the Docker network was unaffected, because the API reaches
`postgres-mart:5432` over the network and never touches the published port — so the
pipeline looked completely healthy and **only host-originating connections broke**.

Two consequences, both worth stating:

1. **`make chaos-heal` now uses `docker compose up -d`, not `docker start`.** Only
   `up -d` reconciles a container against the compose file. `docker start` returns it
   to running without necessarily restoring its published ports, and nothing reports
   the difference.
2. **This is the failure mode this project has hit before.** Earlier in its history,
   ports 5432/6379 were occupied by the host's native Postgres and Redis, Docker
   silently failed to publish them, and the test suite connected to the *wrong
   database* and reported a clean skip. Same shape: a networking detail that is
   invisible from inside the system, and whose only symptom is that something on the
   outside quietly stops working.

Restored with `docker compose up -d --force-recreate postgres-mart redis` — the
Postgres volume survives, Redis repopulates its speed view within seconds — after
which all 31 storage tests passed again and the API's connection pool reconnected
without intervention.

---

## 3e. Measured performance

All figures from the running stack at 150 vehicles and a configured 240 events/second,
Spark in `local[4]`, everything on one 15.6 GB WSL2 host. **This was never a
distributed-scale demonstration** — it measures architectural behaviour, and the
numbers should be read that way.

### End-to-end freshness (producer → visible through the API)

29 samples, 2 s apart, of `as_of_sim` minus `updated_at_sim` on `/api/v1/fleet/live`,
converted from simulated to real time at the 288× clock:

| | real | simulated |
|---|---|---|
| p50 | **7.85 s** | 37.7 min |
| p95 | **23.64 s** | 113.5 min |
| max | 24.08 s | 115.6 min |

`NFR-1` targets live-view freshness under 30 s real. **p95 = 23.6 s — met**, but with
little margin.

One of the 30 samples returned all nulls. `fleet:snapshot` carries the speed-view TTL
of 2 simulated hours — **25 real seconds** — and is refreshed roughly every 6 s by the
activity query, so a read landing in the gap after an expiry sees nothing. The API
reports `null` rather than `0`, which keeps "the cache momentarily expired"
distinguishable from "no vehicles are active"; a dashboard renders it as *No data*.

### Micro-batch duration, by query (30-minute window)

| query | p50 | p95 | processed rows/s (p50) |
|---|---|---|---|
| `q_master` | 1.49 s | 3.62 s | 1,577 |
| `q_dlq` | 0.98 s | 3.51 s | 212 |
| `q_vehicles` | 1.99 s | 5.95 s | 1,202 |
| `q_earnings` | 3.90 s | **17.72 s** | 601 |
| `q_activity` | 3.96 s | **17.58 s** | 604 |
| `q_idle` | 6.04 s | **21.75 s** | 377 |

**Three of the six queries exceed their own 10-second trigger interval at p95.** The
trigger is `PROC_TRIGGER_SECONDS=10`, so when a batch takes 17–22 s the next one starts
late and the lag compounds — which is the direct cause of the 23.6 s p95 freshness
above. The three affected are exactly the windowed and stateful ones (`q_activity`,
`q_earnings`, `q_idle`); the stateless pass-throughs (`q_master`, `q_dlq`, `q_vehicles`)
stay comfortably inside the trigger even while processing more rows per second.

This is a real capacity limit of the demo configuration and belongs in the report as
one. It is also the honest answer to "does it keep up?": **on average yes, at the tail
no.**

### Serving layer

| | value |
|---|---|
| API request latency p50 / p95 / p99 | **50 ms / 95 ms / 99 ms** |
| Postgres query latency (pooled) | 8–15 ms |
| Redis query latency (pooled) | 0.4–1.3 ms |

### Batch layer

| | value |
|---|---|
| One simulated day, cold JVM | **73 s** |
| Same day, warm | **50 s** |
| Airflow DAG run, end to end | 3–4 min against a 5-min schedule |

### Idempotency and restatement (NFR-3), demonstrated

Re-running the batch layer for `2026-04-16`:

| run | `job_run_id` | `sum(restatement_count)` |
|---|---|---|
| before | — | 300 |
| A | `fixed-idem-test` (new) | 450 (+150 — every row republished under a new run, correctly audited) |
| B | `fixed-idem-test` (same) | **450 (delta 0 — idempotent)** |

Two behaviours proven at once. A re-run under the **same** `job_run_id` changes
nothing, which is what makes the batch layer safe to retry. A run under a **new** id
increments `restatement_count` and stamps `restated_at`, which is the audit trail an
examiner asks for when a published figure changes.

Throughout, `batch_complete_thru` stayed at **2026-04-19** while `2026-04-16` was
recomputed — the watermark does not move backwards, and an explicit `--from/--to`
range never advances it, because such a range is by definition a restatement rather
than fresh progress.

---

## 3f. Figures — captured, and verified

`scripts/capture_figures.py` drives headless Chromium over the running stack and
writes ten 1920x1080 figures (2x device scale) into `docs/report/figures/`.
Screenshots are evidence, and evidence that cannot be regenerated is weak evidence:
if a figure is challenged in a viva, "run this script" beats "we took it on Tuesday".

| figure | proves |
|---|---|
| `R1-fleet-operations` | live utilization and earnings by area |
| `R2-batch-reconciliation` | which vehicles are becoming unprofitable |
| `R10-pipeline-health` | observability across ingest / process / store / serve |
| `R9-alertmanager`, `R9b-prometheus-alerts` | the alert rules, firing |
| `R12-api-docs` | the serving-layer contract and merged response model |
| `R6-kafka-topics` | topics, partitions, message counts |
| `R4-airflow-grid`, `R5-airflow-graph` | the batch layer on schedule, and the branch |
| `R14-minio` | the master dataset partitioned on object storage |

### The script lied, twice

Its first run reported **10/10 ok**. Two of those were worthless:

- **Airflow** captured the *login page*. The auth step waited for
  `networkidle`, which never arrives on a UI that polls, so the wait timed out, and
  because the timeout was caught the navigation back to the target URL was skipped
  entirely.
- **Kafka UI** captured a *blank page*. The URL used cluster name `local` — the
  kafka-ui default — while compose names the cluster `fleet`. The SPA renders an
  unknown cluster as an empty body rather than an error.

Both were reported as successes because the script's only check was "did
`page.screenshot()` raise?". It now asserts, on every capture, that required text is
present and that login-form text is absent, and refuses to write the file otherwise.
Verified by pointing a shot at Airflow with deliberately wrong credentials:

```
wrong credentials -> REJECTED: page shows 'Enter your login and password' - not authenticated
```

This is the same lesson as the alert that never fired and the DAG test that passed
vacuously: **a check that cannot fail is not a check.**

### Kafka topics, from `R6`

| topic | partitions | messages | size |
|---|---|---|---|
| `fleet.telemetry.v1` | 6 | **3,830,077** | 284 MB |
| `fleet.alerts.v1` | 3 | 55,009 | 8 MB |
| `fleet.telemetry.dlq` | 1 | 41,915 | 9 MB |
| `fleet.vehicle.registry` | 3 | **150** | 4 KB |
| `_schemas` | 1 | 6 | 7 KB |

Two of these are control checks rather than statistics:

- `fleet.vehicle.registry` holds **exactly 150** messages against `FLEET_SIZE=150` —
  one compacted record per vehicle, which is what a registry topic should be.
- **Dead-letter rate = 41,915 / 3,830,077 = 1.09%**, against a configured
  `FLEET_DEFECT_RATE` of **1%**. The simulator injects a known fraction of corrupt
  events and the validator rejects very nearly that fraction: the ~9% excess is
  naturally-invalid events on top of the injected ones. This is the strongest
  available evidence that the validation path catches what it claims to, because the
  expected answer was known before the measurement was taken.

### Airflow, from `R4`

Airflow's own statistics over 25 displayed runs: **22 success, 3 failed**, mean run
duration **00:01:51**, max 00:04:32, min 00:00:10. The three failures are the
pre-fix runs where the branch trigger rule skipped the batch path. Task composition:
11 tasks — 6 `@task`, 1 `@task.branch`, 2 `BashOperator`, 2 `EmptyOperator`.

(An earlier estimate in this document of "3-4 minutes per run" was eyeballed from a
handful of samples; Airflow's own mean of 1:51 supersedes it.)

### The observability tooling is just more software

The Airflow webserver had been **dead for three hours** when the capture first ran,
exiting 1 after its gunicorn workers were OOM-killed — four workers at roughly
250-350 MB each do not fit in a 900 MB limit. Nothing noticed, because the
*scheduler* is a separate container and kept executing DAGs perfectly. The pipeline
was entirely healthy; only the window onto it was gone.

Fixed with `AIRFLOW__WEBSERVER__WORKERS=2`, a raised limit and
`restart: unless-stopped`. Worth stating in the report when claiming the system is
observable: the observability stack fails like any other software, and a dashboard
that is down looks exactly like a system with nothing to report.

---

## 3g. Diagrams

Four vector diagrams in `docs/diagrams/`, authored in **TikZ** rather than draw.io.
The report is LaTeX, so these compile to vector art with no external asset pipeline,
their fonts match the body text exactly, and the source sits in version control beside
the code. `make diagrams` rebuilds all four from scratch — a diagram that can be
regenerated cannot silently drift from the system it describes.

| | shows |
|---|---|
| **D1** Context | the two sources, the two consumers, and the tension between them: dispatch trades accuracy for latency, finance trades latency for accuracy. That tension is the whole reason the architecture has two paths. Deliberately the only diagram with no technology on it. |
| **D2** Layered architecture ★ | the full dataflow, every technology named, the three layers labelled in the module's own vocabulary (batch / speed / serving), with a colour legend |
| **D3** Event sequence | one telemetry event from simulator to dashboard, annotated with the **measured** p50/p95, and showing the fork where the same event takes two paths |
| **D4** Merge boundary ★ | the half-open interval rule, and the gap case when the batch layer falls behind |

### D2 was wrong before it was right

The first version drew the daily expense file flowing **through Kafka**. It does not:
`expense_dropper.py` writes it straight to object storage with `put_object`, and its
own docstring records that forcing a once-a-day reference feed into the log was
considered and rejected. The diagram was corrected to route it directly to MinIO.

Worth noting because of where the error would have landed. D2 is the figure the
20-mark architecture criterion is read from, and a reader comparing it against the
code would have found the architecture misstated on exactly the page it is marked
from.

---

## 4. Orchestration

`fleet_daily_reconciliation` loads under real Airflow with **zero import errors**,
11 tasks, and all 11 DAG integrity tests pass *inside the scheduler image*.

A full successful run:

```
resolve_sim_date              success
snapshot_speed_view           success
validate_expense_file         success
run_zone_hourly               success
run_spark_profitability       success
verify_watermark_advanced     success
compute_reconciliation_delta  success
notify_success                success
handle_missing_expense_file   skipped   (file was present)
quarantine_file               skipped
notify_failure                skipped
```

Run durations are 3–4 minutes against a 5-minute schedule, so the DAG sustains one
simulated day per cycle — but only just. With `catchup=False`, simulated days missed
during an outage are never picked up automatically; `scripts/backfill.py` is the
remedy, and it is why that script exists separately from the DAG.

---

## 5. Speed-vs-batch reconciliation — a measured limitation

This is the Lambda weakness the module notes describe, turned into a number.
`plan/07 section 5.4` predicted 1–3% on counts and up to 8% on earnings. **The
measured result does not match that prediction, and the reason is worth more than
the prediction was.**

Fleet-wide, simulated day 2026-03-11:

| metric | speed | batch | delta |
|---|---|---|---|
| `fleet_idle_ratio` | 0.522 | 0.576 | **−9.3%** |
| `fleet_earnings_per_min` | 19.355 | 31.165 | −37.9% |
| `fleet_trips_per_min` | 1.000 | 7.183 | −86.1% |

Three defects were found and fixed while producing this table, and each one is
instructive:

1. **Unit mismatch.** The first version compared a 15-simulated-minute sliding
   window against a whole batch day and reported ~99% divergence on everything.
   That was arithmetic, not accuracy. Flow metrics are now normalised to a
   per-minute rate on both sides before comparison.
2. **Wrong join key.** Snapshots were keyed on *when the orchestrator ran* rather
   than on the window the data describes. Measured live, the speed view lags the
   clock by ~93 simulated minutes — only ~19 real seconds at 288x, but more than an
   hour in simulated time, so it routinely crosses an hour boundary. One hour's
   speed reading was being compared against a different hour's batch figure. Fixing
   this moved `idle_ratio` from +23% to −9.3%.
3. **Point-in-time sampling of an open window.** The residual flow-metric gap is
   not a fault. Per zone the snapshot recorded 0–3 trips and 1–7 vehicles where the
   batch hour recorded 25–65 trips and 25–54 vehicles. At
   `FLEET_PING_INTERVAL_SIM_MINUTES=3`, a *complete* 15-minute window should hold
   about 12 distinct vehicles per zone, and a live read of `fleet:zone:Z01` showed
   15 — so the snapshot is catching a **partially accumulated** sliding window, not
   a closed one. The speed view is eventually consistent *within* a window, and
   sampling it at an instant systematically under-reads.

`idle_ratio` is the only metric here that is independent of window width — it is a
ratio, not a count — and it is therefore the figure to quote: **−9.3%**, the speed
layer reading slightly more idle than the exact recomputation.

The honest conclusion for the report is that reconciling a point-in-time approximate
view against a windowed exact one requires the two to be measured over the same span,
and that a single daily sample of a sliding window is too small to do it for flow
metrics. Making the comparison meaningful for trips and earnings would need the
speed view accumulated across the day rather than sampled once — which is future
work, and is named as such rather than quietly omitted.

---

## 6. Defects found by running the system

None of these raised an exception. All would have survived to a demo.

| defect | how it surfaced |
|---|---|
| `zone_id` missing from the master dataset | Backfill crashed — the lake stores raw events; zone is derived at read time by the shared `with_zone`, the same function the speed layer calls |
| Speed layer started only 2 of 5 queries | A day-4 default of `master,dlq` survived into production: a fresh `make up` produced **no speed view at all**, so Redis stayed empty and the merge had nothing approximate to serve |
| Airflow branch skipped the entire batch path | `run_zone_hourly` has two upstreams and used the default `ALL_SUCCESS`; the branch skipping a sibling skipped it too. The DAG went amber, not red, and the only durable symptom was a watermark that never advanced |
| `streaming-job` metrics unscraped | The dashboard's whole STORE column would have been empty while everything else looked healthy |
| `RESUME=1` could not reach the container | The init guard's own error message advertised a flag that compose never passed through |
| Future dates reported as "the batch layer is behind" | A misleading explanation sends someone hunting a fault that does not exist |
| `test_dag_integrity` passed vacuously | Hard-coded path found no DAGs in the container, and an empty DagBag reports no import errors |
| `/api/v1/vehicles/unprofitable` returned 503 | An untyped NULL in `%s IS NULL OR col = %s` — Postgres cannot infer the type. Fixed with `::text` casts |
| A SQL bug was reported as a database outage | `readers.py` wrapped **every** exception as `StoreUnavailableError`, so the endpoint answering the assignment's headline question was dead while blaming a healthy Postgres. Failures are now classified: only connection-shaped ones degrade |
| `/api/v1/vehicles/{id}` returned 404 for every vehicle | `SpeedView.write_vehicle_states` existed since day 3 and was **never called by anything** — dead code, an empty keyspace, and no error anywhere. Now wired as a sixth streaming query (`q_vehicles`) |

---

## 7. Post-fix confirmation

| check | result |
|---|---|
| `GET /api/v1/vehicles/unprofitable?limit=5` | **200**, 5 rows, ordered ascending by 7-day rolling average |
| same with `classification=WATCH` | **200** |
| `fleet:vehicle:*:state` keys in Redis | **149** (of 150 — the key TTL is 30 simulated minutes, ≈6 real seconds at 288x, so a vehicle that has not pinged in the last few seconds is legitimately absent) |
| `GET /api/v1/vehicles/V113` | **200** with live status, position, zone and driver |
| streaming queries running | 6: `master, dlq, activity, earnings, idle, vehicles` |
| unit suite | **385 passed, 1 skipped** |

Worth noting for the report: with the endpoint fixed, **V113 ranks worst by rolling
7-day average profit** without anything being told to look for it. That is the
scripted deteriorating-vehicle narrative emerging from the batch layer on its own,
which is the demonstration the business question asks for.


---

## 8. The report

`docs/report/main.tex` -> `main.pdf`, built with `make report`.

**19 pages total; the body is pages 3-17, exactly 15** -- the top of the brief's
recommended 8-15 range, with cover, contents, references and two appendices outside
it. Nine figures (four vector TikZ diagrams, five verified screenshots) and ten
tables. Zero LaTeX errors, no undefined references.

Every quantitative claim in the report traces to a measurement in this document.
Nothing is estimated: where a figure was previously eyeballed (the "3-4 minutes per
DAG run" estimate) it was replaced with the instrument's own number (Airflow's mean
of 1 min 51 s).

### What the report does not claim

Three things are stated as not implemented rather than left for a marker to discover:
**distributed tracing** (OpenTelemetry and Jaeger were designed and cut for time),
the **generated PDF daily report** (the provisioned dashboards discharge the brief's
"report *or* dashboard" requirement), and **log aggregation**. The technology-stack
table in `plan/02` lists tracing as chosen; the report corrects that rather than
repeating it.

### Cover page

Author names are filled in (Jathur, Shamil, Rifath); index numbers, group number and
submission date are `\newcommand` placeholders collected at the top of `main.tex`.
`make report-check` **fails the build** while any placeholder remains, so the report
cannot be submitted with `EG/20XX/XXXX` on the cover by accident.
