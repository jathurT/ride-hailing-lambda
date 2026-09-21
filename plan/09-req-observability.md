# 09 — Requirement 4: Observability

> **PDF minimum:** *"Structured logging across ingestion, processing, and storage stages. At least one basic alert or health-check rule (e.g. no data received in N minutes, error rate above threshold)."*
>
> **Rubric weight: 10 marks** — *"**Logging, metrics and tracing** across pipeline stages to detect and diagnose pipeline failures."* The rubric asks for all three pillars, so we build all three.
>
> **Honesty requirement:** Prometheus, Grafana, Alertmanager, OpenTelemetry and Loki were **not taught in this module**. The report introduces them in a clearly-labelled paragraph as deliberate extensions beyond lecture scope. The module's own observability content is the Airflow Web UI and Kafka's "Operational Metrics"/"Log Aggregation" use cases, and we use those too.

---

## 1. The measurement plan — what, where, why

We instrument **four stages**, matching the PDF's wording plus serving.

| Stage | Question it must answer | Primary signal |
|---|---|---|
| **Ingestion** | Is data arriving, at the expected rate, without errors? | `events_produced_total` rate |
| **Processing** | Is the stream keeping up, and is state bounded? | consumer lag + batch duration + watermark lag |
| **Storage** | Are writes landing, and is the master dataset growing? | `sink_write_*` + Parquet file count |
| **Serving** | Are queries fast, correct and non-degraded? | request latency + `degraded` rate + watermark age |

The design question we answer explicitly in the report: **"if this pipeline broke at 3 a.m., which single dashboard would tell you where?"** Answer: the Pipeline Health dashboard, which lays the four stages out left to right so a break shows as the point where throughput drops to zero.

---

## 2. Structured logging

`structlog` → JSON → stdout. One configuration module (`common/logging.py`) imported by producers, Spark driver, Airflow tasks and the API.

**Mandatory field schema** — every log line, everywhere:

```json
{
  "ts": "2026-08-04T18:22:07.412Z",
  "level": "info",
  "service": "telemetry-producer",
  "stage": "ingest",
  "event": "batch_flushed",
  "sim_date": "2026-03-02",
  "sim_time": "2026-03-02T14:31:00Z",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "correlation_id": "V042",
  "duration_ms": 12.4,
  "record_count": 240
}
```

Rules enforced by a test (`test_logging_schema.py`):
- `stage` ∈ `{ingest, process, store, serve, orchestrate}` — this is what makes "logging **across** stages" verifiable rather than asserted.
- `correlation_id` is the `vehicle_id` wherever one exists, so grepping one vehicle's journey across all services works.
- `trace_id`/`span_id` are injected automatically from the active OTel context — logs and traces are joinable.
- Errors always carry `error.type`, `error.message`, `error.stack`.
- **No f-strings in log messages.** `log.info("batch_flushed", record_count=n)` not `log.info(f"flushed {n}")` — the whole point of structured logging is machine-queryable fields.

**Aggregation (stretch goal):** Promtail → Loki → a Grafana log panel, so logs and metrics sit on one screen and a spike in the error-rate graph can be clicked through to the actual lines. First item on the cut list (`15`) if time is short; `docker compose logs` remains the fallback and satisfies the PDF's minimum.

---

## 3. Metrics

### 3.1 Ingestion (producer `/metrics`, ports 8001/8002)

| Metric | Type | Labels |
|---|---|---|
| `events_produced_total` | counter | `topic`, `status=ok\|failed` |
| `producer_send_duration_seconds` | histogram | `topic` |
| `producer_errors_total` | counter | `error_type` |
| `producer_backpressure_events_total` | counter | — |
| `defects_injected_total` | counter | `defect_type` |
| `sim_clock_day_index` | gauge | — |
| `sim_clock_drift_seconds` | gauge | Difference between this process's simulated time and the shared anchor. **Should be ~0; a non-zero value means the clock design is broken**, which is a bug class we specifically instrument for after `03 §3`. |
| `expense_files_written_total` | counter | `restatement=true\|false` |

### 3.2 Kafka

`danielqsj/kafka-exporter` on `:9308`:
- `kafka_consumergroup_lag{group,topic,partition}` — for consumers that commit offsets to Kafka.
- `kafka_topic_partition_current_offset`, `kafka_brokers`, `kafka_topic_partitions`

> #### ⚠ Correction, found on day 4: consumer lag does NOT measure the Spark queries
>
> This line originally called `kafka_consumergroup_lag` *"the single most important
> metric in the system"*. That is **wrong for Spark Structured Streaming**, and the
> correction matters because the wrong metric would have read healthy while the
> pipeline fell behind.
>
> Structured Streaming tracks offsets in **its own checkpoint** and never commits them
> to a Kafka consumer group. Setting `kafka.group.id` does not change that — it only
> affects group membership, so a lag series for those groups is permanently stale.
>
> Worse, setting it is actively harmful: every micro-batch joins and leaves the group,
> churning the coordinator. On this stack it killed the DLQ query after ~4,500
> micro-batches with `TimeoutException: Timeout of 60000ms expired before the position
> for partition fleet.telemetry.v1-0 could be determined`. Spark emits a warning about
> this at query start; it is easy to scroll past.
>
> **Streaming progress comes from `StreamingQueryProgress`** instead, via the listener:
>
> | Signal | Source |
> |---|---|
> | Backlog (the real "lag") | `sources[].latestOffset − sources[].endOffset` per partition |
> | Keeping up? | `processedRowsPerSecond` vs `inputRowsPerSecond` |
> | Alive at all? | age of `stream_last_progress_timestamp` |
> | State growth | `stateOperators[].numRowsTotal` |
>
> `kafka_consumergroup_lag` stays useful for anything that *does* commit offsets — the
> Airflow lab-file producer in the sibling project, and any plain consumer — so the
> exporter remains. It simply is not the Spark queries' health signal.
>
> **Related fix:** every streaming query now sets an explicit `trigger(processingTime=…)`.
> The default trigger fires as soon as the previous batch completes, which on a
> low-volume path such as the DLQ means thousands of near-empty micro-batches per hour,
> each opening Kafka consumers. An unset trigger is not a neutral default.

### 3.3 Processing (Spark) — and the honest engineering choice

Structured Streaming does not expose Prometheus metrics from PySpark out of the box. Two options were evaluated:

| Option | Verdict |
|---|---|
| Spark's native `PrometheusServlet` on the driver UI (`spark.ui.prometheus.enabled=true`) | Exposes JVM/executor metrics but **not** Structured Streaming progress detail (input rate, watermark, state rows) in a usable form, and the driver's port must be scrapeable — awkward in compose when the driver is submitted per job. |
| **`StreamingQueryListener` → Prometheus Pushgateway** ✅ | We control exactly which fields are exported, works identically for the long-lived streaming driver and the short-lived batch job, and needs no scrape-target discovery. |

**Chosen: the listener + Pushgateway.** We note in the report that Pushgateway is generally an anti-pattern for long-lived services (it holds the last value forever, so a dead job looks alive) — and we mitigate exactly that by also exporting `spark_streaming_last_progress_timestamp` and alerting on its **age**, not on its presence. Naming the anti-pattern and showing the mitigation is a better answer than using it naively.

```python
class PrometheusStreamingListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        p = event.progress
        push_metrics({
            "spark_streaming_input_rows_per_second":     p.inputRowsPerSecond,
            "spark_streaming_processed_rows_per_second": p.processedRowsPerSecond,
            "spark_streaming_batch_duration_seconds":    p.durationMs["triggerExecution"] / 1000,
            "spark_streaming_state_rows":                sum(s.numRowsTotal for s in p.stateOperators),
            "spark_streaming_watermark_lag_seconds":     _watermark_lag(p.eventTime),
            "spark_streaming_last_progress_timestamp":   time.time(),
        }, labels={"query": p.name})
    def onQueryTerminated(self, event):
        log.error("streaming_query_terminated", query_id=str(event.id), reason=event.exception)
```

Plus in-pipeline business counters: `events_validated_total{result}`, `events_dlq_total{reason}`, `events_deduplicated_total`, `business_alerts_emitted_total{type,severity}`.

### 3.4 Storage

- `redis_exporter` → memory, keyspace, evictions, connected clients
- `postgres_exporter` → connections, transaction rate, table sizes, replication lag (n/a here but scraped)
- App-level: `sink_write_duration_seconds{sink}`, `sink_write_errors_total{sink}`, `master_dataset_files_written_total`, `master_dataset_bytes_total`

### 3.5 Serving

`prometheus-fastapi-instrumentator` gives request rate, latency histograms and status codes per route for free. Plus:
- `serving_batch_watermark_age_sim_days` — how stale the batch view is
- `serving_degraded_responses_total{missing_store}` — how often we fell back to one store
- `serving_merge_boundary_crossings_total` — how often a request actually spanned both layers (proves the merge path is exercised, rather than being dead code)

### 3.6 Orchestration

Airflow StatsD → `statsd-exporter` → Prometheus: DAG duration, task failures, scheduler heartbeat, task queue depth.

---

## 4. Alerting — two kinds, deliberately separated

**This distinction is a report point in its own right.** Conflating them is a common design failure: a dispatcher paged about Kafka consumer lag is noise; an SRE paged about a vehicle being idle is negligence.

### 4.1 Pipeline-health alerts (Prometheus → Alertmanager)

`observability/prometheus/alerts.yml`:

| Alert | Expression (simplified) | For | Severity |
|---|---|---|---|
| **`NoTelemetryIngested`** | `rate(events_produced_total{status="ok"}[2m]) == 0` | 3m | critical |
| **`HighDLQRate`** | `rate(events_dlq_total[5m]) / rate(events_produced_total[5m]) > 0.05` | 5m | warning |
| `ConsumerLagGrowing` | `kafka_consumergroup_lag > 10000` | 5m | warning |
| `ConsumerLagCritical` | `kafka_consumergroup_lag > 50000` | 2m | critical |
| `StreamingQueryStalled` | `time() - spark_streaming_last_progress_timestamp > 120` | 1m | critical |
| `MasterDatasetWriteStalled` | same, for the `q1_master` query | 1m | **critical (highest)** |
| `WatermarkLagHigh` | `spark_streaming_watermark_lag_seconds > 3600` | 5m | warning |
| `StateStoreGrowing` | `deriv(spark_streaming_state_rows[15m]) > 100` | 15m | warning |
| `SinkWriteErrors` | `rate(sink_write_errors_total[5m]) > 0` | 2m | warning |
| `AirflowDagFailed` | `airflow_dag_run_failed > 0` | 1m | warning |
| `BatchWatermarkStale` | `serving_batch_watermark_age_sim_days > 2` | 5m | warning |
| `ServingLayerDegraded` | `rate(serving_degraded_responses_total[5m]) > 0` | 5m | warning |
| `ApiHighLatency` | `histogram_quantile(0.95, ...) > 0.5` | 5m | warning |
| `SimClockDrift` | `abs(sim_clock_drift_seconds) > 5` | 2m | critical |

The first two are the PDF's explicitly requested rules ("no data received in N minutes", "error rate above threshold"). The rest are ours.

Alertmanager routes by severity to a webhook receiver (`observability/alert_sink/`) that logs alerts as structured JSON and exposes them at `/api/v1/alerts/pipeline`, so firing alerts are visible in the demo without needing email or Slack.

### 4.2 Business alerts (emitted by the pipeline)

Produced into `fleet.alerts.v1` by the speed layer and surfaced via the API and dashboard:

| Alert | Rule | Source |
|---|---|---|
| `VEHICLE_IDLE_PROLONGED` | idle ≥ 45 simulated minutes (WARNING) / 90 (CRITICAL) | Speed layer, stateful (`05 §3.5`) |
| `VEHICLE_UNPROFITABLE` | 7-day rolling profit ≤ 0 | Batch layer |
| `VEHICLE_DETERIORATING` | classification = `WATCH` (positive profit, negative slope) | Batch layer |
| `ZONE_UNDERSERVED` | idle_ratio < 0.1 and demand high — no spare capacity | Speed layer |
| `EXPENSE_RECONCILIATION_EXCEPTION` | `MISSING_TELEMETRY` rows present | Batch layer |

### 4.3 Proving the alerts work

An alert rule that has never fired is untested code. `make chaos` provides four scenarios, each with an expected alert and a target detection time:

| Command | Injects | Expect |
|---|---|---|
| `make chaos-kill-producer` | Stops the telemetry producer | `NoTelemetryIngested` within 3 min |
| `make chaos-bad-data` | Sets `DEFECT_RATE=0.30` | `HighDLQRate` within 5 min |
| `make chaos-pause-consumer` | Pauses the Spark container | `ConsumerLagGrowing` + `StreamingQueryStalled` |
| `make chaos-kill-redis` | Stops Redis | `ServingLayerDegraded`; API stays up returning batch-only |

**These are run on camera in the demo video.** Showing an alert fire and then recover is far more convincing than a screenshot of a rules file.

---

## 5. Distributed tracing — and an honest limitation

### 5.1 What we do

- **Producers:** one span per flush batch; `traceparent` (W3C) injected into Kafka message headers, either via `opentelemetry-instrumentation-kafka-python` or manually into the header list.
- **FastAPI:** auto-instrumented; spans for Redis and Postgres calls, so a slow endpoint shows *which store* was slow.
- **Airflow:** native OTel traces if the version supports it, else manual spans in `@task` bodies — giving a trace per DAG run with a span per task.
- **Spark:** a span per **micro-batch**, created on the driver inside `foreachBatch`, carrying `batch_id`, input row count, watermark, and per-sink child spans. From a **sample** of the batch's Kafka message headers we extract the incoming `traceparent` values and attach them as **span links**.
- **Collector:** OTel Collector → Jaeger all-in-one (`:16686`).

Result: for a sampled event we can show a waterfall of `producer.send` → (link) → `spark.micro_batch` → `spark.sink.redis` → `api.request` → `api.redis.get`.

### 5.2 The limitation, stated plainly

**True per-event distributed tracing through a micro-batch engine is not achievable at reasonable cost, and we do not claim it.** Reasons:
1. A micro-batch collapses thousands of events into one unit of work — there is no per-event span to nest.
2. PySpark executors are separate processes; a tracer initialised per partition produces disconnected fragments, and the JVM-side shuffle is invisible to a Python tracer entirely.
3. Emitting a span per event at 240 events/second would generate more telemetry than data.

**So we trace at micro-batch granularity and link sampled events.** This is what production streaming systems actually do, and the report says so with that framing. A marker who knows the space will recognise this as the correct answer; claiming full per-event tracing would be recognised as false.

The limitation goes in **both** §7 (observability design) and §9 (limitations) of the report.

---

## 6. Health checks

| Endpoint / mechanism | Checks |
|---|---|
| `GET /health/live` | Process responsive |
| `GET /health/ready` | Redis + Postgres connections established |
| `GET /health/deep` | Actually queries each dependency (Kafka metadata, Redis PING, Postgres `SELECT 1`, MinIO bucket list); returns per-dependency status and latency |
| `GET /api/v1/pipeline/status` | The one-stop view: high-water-mark, last batch run, streaming liveness, consumer lag, simulated clock |
| Docker `healthcheck` | On every service; compose `depends_on: condition: service_healthy` orders start-up |
| `fleet_pipeline_healthcheck` DAG | Independent probe every 2 real minutes (`08 §5`) |

---

## 7. Grafana dashboards

Provisioned from `observability/grafana/provisioning/` — datasources and dashboard JSON in the repo, so a fresh clone gets working dashboards. This is a reproducibility mark, not just convenience.

**1. Pipeline Health** — laid out left-to-right by stage so a break is visually located:
```
INGEST                PROCESS                  STORE              SERVE
events/s              consumer lag/group       sink duration      req rate
producer errors       batch duration           sink errors        p95 latency
DLQ rate by reason    watermark lag            parquet files/hr   degraded rate
sim clock drift       state rows               redis memory       hwm age
```
Plus: firing alerts table, Airflow DAG status, error-log rate.

**2. Fleet Operations** (business) — active vehicles, idle ratio, trips/hour, earnings by zone (bar), zone geomap, earnings by time-of-day (the diurnal curve), live idle alerts table.

**3. Batch Reconciliation** — daily net profit, top 10 unprofitable, classification distribution, **speed-vs-batch divergence** (`07 §5.4`), high-water-mark age, expense join match rate, DAG run history.

**4. Traces & Logs** — Jaeger-linked trace search, latency exemplars, Loki panel filtered by `stage` and `level` (if Loki is included).

---

## 8. Testing observability

| Test | Asserts |
|---|---|
| `test_logging_schema.py` | Every log call emits the mandatory fields; `stage` is from the allowed set |
| `test_metrics_registry.py` | No duplicate metric names; every metric has HELP text; naming follows Prometheus conventions (`_total` on counters, `_seconds` on durations) |
| `test_alert_rules.py` | `promtool check rules` passes in CI; every rule has `severity`, `summary` and `description` annotations |
| `test_alert_firing.py` (integration) | Feed synthetic series into a test Prometheus; assert each rule fires and resolves |
| `test_trace_propagation.py` | A `traceparent` header written by the producer is readable and valid on the consumer side |
| `test_health_deep.py` | Degrades gracefully with one dependency down |

---

## 9. What the report must show (§7)

- The four-stage measurement table from §1, with the *why* column filled in — the rubric asks *"what is measured, how, and why"*.
- The log schema with a real example line.
- The full alert rule table, **split into pipeline-health and business** with the rationale for splitting.
- The Pushgateway choice with the anti-pattern acknowledged and mitigated.
- The tracing approach **with the micro-batch limitation stated**.
- Screenshots: Pipeline Health dashboard mid-incident; Alertmanager with `NoTelemetryIngested` firing; a Jaeger waterfall; Kafka UI consumer lag; Airflow grid.
- The `make chaos` table as evidence the alerts were actually verified.
