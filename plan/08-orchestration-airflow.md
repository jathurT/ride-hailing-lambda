# 08 — Orchestration with Apache Airflow

> **PDF preferred stack:** *"Orchestration: Apache Airflow (for managing batch jobs or reporting pipelines)."*
>
> **The governing principle, in the module's own words:** *"Airflow only orchestrates. NOT a data processing engine, NOT a database, NOT a distributed computing framework."*

---

## 1. The rule we hold ourselves to

**No transformation logic in any DAG file.** DAG files may contain: sensing, validation *invocation*, `spark-submit` calls, SQL that manages watermarks or dimensions, report rendering *invocation*, branching, and failure callbacks. They may **not** contain aggregation, joins, or business calculations — those live in `src/fleet/transforms/` and `src/fleet/batch_layer/` and execute inside Spark.

This is checkable and we make it checkable: a lint test (`tests/unit/test_dag_purity.py`) parses every DAG file's AST and fails if it imports pandas, or calls `groupby`/`merge`/`agg`. **A test that enforces an architectural principle is a strong thing to show in a viva.**

---

## 2. DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `fleet_daily_reconciliation` | `*/5 * * * *` (real) = **once per simulated day** | The batch layer. The main DAG. |
| `fleet_restatement_watcher` | `*/1 * * * *` | Detects restatement markers and re-triggers reconciliation for a past simulated date |
| `fleet_maintenance` | `0 * * * *` (real, hourly) | Parquet compaction, retention enforcement, `dim_vehicle` SCD2 refresh |
| `fleet_pipeline_healthcheck` | `*/2 * * * *` | Independent liveness probe (see §5) |

### 2.1 Why `*/5 * * * *` and not `@daily`

Airflow schedules in **real** time; our business day is **simulated**. One simulated day = 5 real minutes, so a real 5-minute cron *is* a daily schedule in the simulation. The DAG resolves its target `sim_date` from `simclock` (the same shared module the producers use), **not** from Airflow's `logical_date`, and pushes it to XCom for downstream tasks.

**This is a genuine design subtlety and the report should call it out.** Using `logical_date` would bind us to real time and the whole pipeline would target the wrong day. The mapping is stated in the README and asserted by a test.

`catchup=False` and `max_active_runs=1` — we never want a backlog of simulated days piling up, and two concurrent runs would race on the upsert.

---

## 3. `fleet_daily_reconciliation` — task graph

```
        resolve_sim_date
               │
               ▼
      wait_for_expense_file          (S3KeySensor, poke, timeout 90s)
               │
        ┌──────┴───────┐
     found          timed out
        │                │
        ▼                ▼
 validate_expense   handle_missing_expense_file
        │            (log + alert + mark day incomplete)
   ┌────┴────┐                │
 valid    invalid             │
   │          │               │
   │          ▼               │
   │    quarantine_file       │
   │    (+ alert)             │
   │          │               │
   ▼          │               │
run_spark_profitability       │
   │          │               │
   ▼          │               │
run_dq_checks │               │
   │          │               │
   ├──────────┴───────────────┤
   ▼                          │
refresh_dimensions            │
   │                          │
   ▼                          │
compute_reconciliation_delta  │
   │                          │
   ▼                          │
advance_batch_watermark   ◀── never runs on the incomplete path
   │
   ▼
generate_daily_report
   │
   ▼
publish_report  (MinIO + Prometheus "report ready" gauge)
   │
   ▼
notify_success  (trigger_rule=ALL_SUCCESS)
```

Plus `notify_failure` with `trigger_rule=ONE_FAILED` posting to the Alertmanager webhook.

### 3.1 Task detail

| Task | Operator | Notes |
|---|---|---|
| `resolve_sim_date` | `@task` (TaskFlow) | Reads `simclock`; honours a `sim_date` DAG param for manual/restatement runs; pushes to XCom |
| `wait_for_expense_file` | `S3KeySensor` | `mode="reschedule"` (frees the worker slot between pokes — important on a small stack), `poke_interval=10`, `timeout=90`, `soft_fail=False`. **The module names "file arrival" as the canonical sensor use case.** |
| `validate_expense_file` | `@task.branch` | Verifies the `.md5` sidecar, header, dtypes, non-negative costs, no duplicate `vehicle_id`. Returns the next task id → branching |
| `quarantine_file` | `@task` | Moves to `quarantine/`, writes a reason file, raises a pipeline alert |
| `handle_missing_expense_file` | `@task` | Marks the simulated day incomplete in `mart.dq_run_log`; alerts; **does not advance the watermark** |
| `run_spark_profitability` | `SparkSubmitOperator` (or `BashOperator` → `spark-submit`) | Passes `--sim-date`, `--job-run-id`; `retries=2`, `retry_delay=30s`, `retry_exponential_backoff=True` |
| `run_dq_checks` | `@task` | Executes the checks from `06 §4`; pushes results to Postgres + Pushgateway; fails the DAG only on the duplicate-PK check |
| `refresh_dimensions` | `SQLExecuteQueryOperator` | SCD2 maintenance on `dim_vehicle`; upserts `dim_date` |
| `compute_reconciliation_delta` | `@task` | Compares the speed view's stored values for the day against the batch result (`07 §5.4`) |
| `advance_batch_watermark` | `SQLExecuteQueryOperator` | **Single-row UPDATE. The last data task. Everything about the serving-layer contract depends on this running only after a successful, committed upsert.** |
| `generate_daily_report` | `@task` | Jinja2 → HTML → WeasyPrint → PDF |
| `publish_report` | `@task` | Uploads to MinIO; sets `report_last_generated_timestamp` gauge |

### 3.2 Airflow features used — all explicitly taught

The report can tick these off against the lecture:

| Taught concept | Where we use it |
|---|---|
| **DAG / DAG Run** | Four DAGs; each run targets one simulated day |
| **Operators** | `S3KeySensor`, `SparkSubmitOperator`, `SQLExecuteQueryOperator`, `TriggerDagRunOperator`, `EmptyOperator` |
| **Sensors** | `wait_for_expense_file` — the module's own "file arrival" example |
| **TaskFlow `@task`** | Every Python task; the module names the decorator explicitly |
| **Dependencies declared second** | We define all tasks, then wire them at the bottom of the file — the module's stated style |
| **Retries** | 2 retries with exponential backoff on Spark and DB tasks |
| **Scheduling** | Real cron mapped to simulated days |
| **XCom** | `sim_date`, `job_run_id`, row counts passed between tasks |
| **Branching** | `@task.branch` on validation; missing-file path |
| **Trigger Rules** | `ONE_FAILED` for the failure notifier; `NONE_FAILED_MIN_ONE_SUCCESS` for the join after branching |
| **Depends On Past** | On `advance_batch_watermark`, so the watermark can never skip a simulated day |
| **Web UI** | The primary orchestration observability surface |

Not used, and we say why if asked: **Latest Only** (we deliberately *do* want to re-run past days — that is the restatement feature), and **Celery/Kubernetes executors** (LocalExecutor is right for a single-machine demo).

---

## 4. `fleet_restatement_watcher`

Runs every real minute. Lists `restatements/` in MinIO for markers not yet processed (tracked in a small `mart.restatement_log` table), and for each one uses `TriggerDagRunOperator` to fire `fleet_daily_reconciliation` with `conf={"sim_date": "...", "reason": "restatement"}`.

Guards: a marker is processed once (idempotent); `max_active_runs=1` on the target DAG serialises the re-runs; the watermark is not advanced on a restatement run (it is already past that date — see `06 §3`).

**This DAG is what makes the reprocessing argument in `01 §2.7` demonstrable rather than theoretical.**

---

## 5. `fleet_pipeline_healthcheck` — orchestrated observability

A deliberately independent probe, because *"the monitoring failed silently"* is the most dangerous failure mode. Every 2 real minutes it checks:

| Check | Alert on breach |
|---|---|
| Events produced in the last 2 minutes > 0 | `NoTelemetryIngested` |
| Streaming query last-progress age < 120 s | `StreamingQueryStalled` |
| Consumer lag on all three groups < 10,000 | `ConsumerLagGrowing` |
| High-water-mark age < 2 simulated days | `BatchWatermarkStale` |
| New Parquet files in the last simulated day > 0 | `MasterDatasetNotGrowing` |
| API `/health/deep` returns 200 | `ServingLayerUnhealthy` |

Results are pushed as Prometheus gauges so Alertmanager owns the alerting; this DAG only *measures*. Its own failure is itself alertable via the DAG-failure callback — so there is no unwatched watcher.

---

## 6. Configuration

| Item | Approach |
|---|---|
| Executor | `LocalExecutor` (two processes, not Celery — appropriate for one machine, and stated as such) |
| Metadata DB | Its own `postgres:16-alpine` container, **separate from the mart**. Mixing Airflow's metadata with the data warehouse is a real anti-pattern and keeping them apart is worth a sentence in the report. |
| Connections | Defined in `.env` as `AIRFLOW_CONN_*` URIs so they are version-controlled shape and secret-free value. No connections created by hand in the UI — that would break reproducibility. |
| Variables | `sim_day_seconds`, `fleet_size`, thresholds — set by an init container from `.env` |
| DAG folder | `airflow/dags/`, bind-mounted |
| Shared code | `src/` mounted and on `PYTHONPATH`, so DAGs import the same `simclock` and `transforms` the producers and Spark jobs use. **One definition of simulated time across the whole system.** |
| Logging | `[logging] json_format = True` so Airflow's own logs match the pipeline's structured format |
| Metrics | `[metrics] statsd_on = True` → `statsd-exporter` → Prometheus |
| Tracing | Airflow 2.10+ OTel traces if the version allows; otherwise manual spans in `@task` bodies |

---

## 7. Testing DAGs

| Test | Asserts |
|---|---|
| `test_dag_integrity.py` | Every DAG imports without error, has no cycles, has `owner`/`retries` defaults, and `catchup=False` |
| `test_dag_purity.py` | **AST check: no pandas import, no `groupby`/`merge`/`agg` calls in any DAG file.** Enforces "Airflow only orchestrates" |
| `test_sim_date_resolution.py` | `resolve_sim_date` returns the simulated date, not Airflow's `logical_date`; honours the manual param |
| `test_branching.py` | Valid file → spark path; invalid → quarantine; missing → incomplete path |
| `test_watermark_task.py` | Advances by exactly one simulated day; refuses to skip; never moves backwards |

---

## 8. What the reviewer sees (the observability deliverable)

The Airflow UI at `:8082` is a graded artefact, not a convenience:

- **Grid view** — a column per simulated day, green squares marching left to right in real time. This single screenshot demonstrates that the batch layer is genuinely running on a schedule.
- **Graph view** — the branching structure, with the taken path highlighted.
- **Task logs** — structured JSON, per task, including the `spark-submit` output.
- **A deliberately failed run** — we trigger the missing-expense-file scenario so the demo shows a red task, the branch to the recovery path, and the alert. **Showing a failure handled correctly is worth more than showing only green.**

---

## 9. What the report must show (§6.2 / §7)

- The task graph from §3.
- The taught-features table from §3.2 — direct evidence of applying the module's material.
- The real-cron-to-simulated-day mapping and why `logical_date` was not used.
- The "Airflow only orchestrates" principle and the AST test that enforces it.
- Screenshots: grid view across several simulated days; graph view with branching; a failed run recovering.
