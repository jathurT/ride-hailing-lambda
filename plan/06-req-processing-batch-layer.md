# 06 — Requirement 2: Batch Layer Processing

> Answers the **second half** of the business question: *"which vehicles are becoming unprofitable once yesterday's fuel/maintenance costs are factored in?"*
>
> This is where the **join between the two sources** happens — the transformation the PDF explicitly names.

---

## 1. The master dataset

The batch layer's input is the immutable Parquet lake written by speed-layer query Q1.

```
s3://fleet-lake/
├── raw/telemetry/
│   ├── sim_date=2026-03-01/part-*.snappy.parquet
│   ├── sim_date=2026-03-02/part-*.snappy.parquet
│   └── sim_date=2026-03-03/...
├── landing/expenses/
│   ├── expenses_2026-03-01.csv
│   ├── expenses_2026-03-02.csv          ← overwritten by the day-3 restatement
│   └── .md5 sidecars
├── quarantine/                          ← failed-validation expense files
├── restatements/2026-03-02.json         ← restatement markers
├── reports/fleet_daily_report_2026-03-02.pdf
└── _checkpoints/                        ← Structured Streaming checkpoints
```

**Properties that make the architecture argument work** (worth restating in the report because this is *why* Lambda won):
- **Immutable** — nothing ever rewrites a telemetry partition. Recomputation is always from the same bytes, so it is deterministic.
- **Addressable** — "recompute 2026-03-02" is a path, not a query.
- **Partition-pruned** — the 7-day trend reads 7 directories, not the whole lake.
- **Column-pruned** — the profitability job projects 7 of 14 columns.

This is the module's **data lake** ("stores all your data, regardless of format or size") feeding a **data mart** ("pre-joining and aggregating"). Use those terms.

---

## 2. The job — `batch_layer/daily_profitability.py`

Invoked by Airflow via `spark-submit` with `--sim-date {{ params.sim_date }}`.

```
read telemetry partition (sim_date = D)
        │
        ├── reconstruct trips        (group consecutive pings by trip_id)
        ├── exact distance           (haversine over consecutive positions)
        ├── exact revenue            (final fare per completed trip)
        ├── utilization              (on_trip time ÷ total reported time)
        └── idle hours               (idle time in simulated hours)
        │
        ▼
  per-vehicle daily telemetry summary   (150 rows)
        │
        │           read expenses_D.csv → validate → 150 rows
        │                    │
        └────────► FULL OUTER JOIN on vehicle_id ◄──────┘
                            │
                  reconcile & flag mismatches
                            │
                  net_profit = revenue − fuel − maintenance
                  profit_per_km, margin_pct
                            │
                  read previous 6 days from the mart
                            │
                  7-day rolling profit + trend slope
                            │
                  classify: healthy / watch / unprofitable / deteriorating
                            │
                            ▼
              UPSERT mart.fact_vehicle_daily_pnl on (vehicle_id, sim_date)
```

### 2.1 Trip reconstruction

Telemetry is per-ping; profitability is per-trip. We reconstruct trips with a window function — the canonical Spark batch operation, and a nice demonstration of the DataFrame API the module taught:

```python
w = Window.partitionBy("vehicle_id").orderBy("event_time")
trips = (events
    .withColumn("prev_status", lag("status").over(w))
    .withColumn("prev_lat", lag("lat").over(w))
    .withColumn("prev_lon", lag("lon").over(w))
    .withColumn("segment_km", haversine_km(col("prev_lat"), col("prev_lon"),
                                           col("lat"), col("lon")))
    .withColumn("segment_sec", unix_timestamp("event_time") - unix_timestamp(lag("event_time").over(w))))
```

`haversine_km` is a **Spark Column expression** built from `pyspark.sql.functions` (`sin`, `cos`, `asin`, `sqrt`, `radians`) — **not a Python UDF**. Same reasoning as `05 §3.2`: it stays in the JVM and Catalyst can optimise it. Being able to explain that in the viva is worth more than the code itself.

This `lag`/`Window` step is a **wide transformation** requiring a shuffle — exactly what the module taught (*"Wide Transformations: require data to be shuffled across the network"*). Point at it in the report and in the Spark UI's DAG visualisation.

### 2.2 The join — the requirement the PDF names explicitly

```python
result = telemetry_summary.join(expenses, on="vehicle_id", how="full_outer")
```

**`full_outer`, deliberately**, because both mismatch directions are real business signals and dropping either would hide a problem:

| Case | Meaning | Handling |
|---|---|---|
| Telemetry, no expense row | Vehicle drove but the partner did not invoice it | `expense_status = 'MISSING_EXPENSE'`; costs treated as null (not zero — a null profit is honest, a zero-cost profit is a lie); counted in `dq_missing_expense_rows` |
| Expense row, no telemetry | Invoiced for a vehicle that never reported — possible fraud, or a dead telematics unit | `expense_status = 'MISSING_TELEMETRY'`; **raises a data-quality alert**; the row is retained in the mart for investigation |
| Both | Normal | `expense_status = 'MATCHED'` |

An inner join would silently discard both anomalies. **In a financial reconciliation pipeline, silently discarding unmatched rows is the classic serious bug**, and choosing `full_outer` with explicit statuses is exactly the kind of thing a marker looks for under "correctness of transformation logic".

### 2.3 The distance reconciliation

Two independent measures of the same quantity:
- `telemetry_distance_km` — our haversine sum over GPS pings
- `partner_distance_km` — from the expense file (deliberately ±3% different, per `03 §5.1`)

We compute `distance_variance_pct` and flag rows above 10%. The report presents this as a genuine finding: **a reconciliation pipeline's job is to surface disagreement between sources, not to hide it by picking one.** We use our own measure for `profit_per_km` (we trust our GPS more than a partner's odometer) and expose both, plus the variance, in the daily report.

### 2.4 "Becoming" unprofitable — the trend

The business question says *becoming*, not *is*. One-day profit is not the answer.

```sql
AVG(net_profit) OVER (
    PARTITION BY vehicle_id ORDER BY sim_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_7d_avg_profit
```

Plus a simple least-squares slope over the same 7 points (`regr_slope(net_profit, day_index)`), giving:

| Classification | Rule |
|---|---|
| `HEALTHY` | 7-day average profit > 0 and slope ≥ 0 |
| `WATCH` | 7-day average > 0 but slope < 0 (**"becoming unprofitable"** — the answer to the question) |
| `UNPROFITABLE` | 7-day average ≤ 0 |
| `CRITICAL` | 7-day average ≤ 0 **and** slope < 0 |
| `INSUFFICIENT_DATA` | fewer than 4 days of history |

`INSUFFICIENT_DATA` matters: on simulated days 1–3 there is no trend yet, and reporting a slope from two points would be misleading. Saying "we don't know yet" is the correct engineering answer and the report should note it.

`V113` (the scripted outskirts vehicle, `03 §4.4`) reliably reaches `UNPROFITABLE` by simulated day 3 — the report's headline finding, reproducible on every run.

---

## 3. Idempotency and restatement

**Every task in the DAG is safe to re-run.** This is a hard requirement, because the restatement scenario re-runs a past day while the pipeline keeps running.

| Mechanism | Detail |
|---|---|
| Upsert, not insert | `INSERT … ON CONFLICT (vehicle_id, sim_date) DO UPDATE SET …` |
| Restatement audit | `restated_at` timestamp and `restatement_count` incremented on the conflict path — so the mart records *that* a figure was restated, which an auditor would want |
| Delete-then-insert avoided | Never `DELETE FROM … WHERE sim_date = D` before inserting — a crash between the two would leave the day missing. Upsert is atomic per row. |
| Input immutability | Telemetry partitions are never rewritten, so recomputation is deterministic |
| Transactional watermark | `batch_high_water_mark` advances only in the final task, after the upsert commits |

**Restatement flow:**
1. `expense_dropper` writes `expenses_2026-03-02.csv` (corrected) + `restatements/2026-03-02.json`.
2. A short Airflow DAG `fleet_restatement_watcher` (every 1 real minute) senses restatement markers.
3. It triggers `fleet_daily_reconciliation` with `params={"sim_date": "2026-03-02", "reason": "restatement"}`.
4. The job recomputes from the *unchanged* telemetry partition + the *new* expense file.
5. Upsert overwrites; `restated_at` is set.
6. Downstream trend calculations for days 3+ are recomputed for affected vehicles.
7. The daily report PDF for that date is regenerated and versioned as `..._v2.pdf`.

**The high-water-mark does not move backwards** during a restatement — day 2 was already batch-complete; its *values* changed, not its completeness. Getting this right is subtle and it is exactly the kind of detail a viva probes.

---

## 4. Data quality checks

A dedicated `dq_checks` task, results written to `mart.dq_run_log` and pushed to Prometheus:

| Check | Threshold | Action on breach |
|---|---|---|
| Telemetry row count for the day | within 40% of the trailing 3-day median | Warn |
| Expense row count | ≥ 80% of active vehicles | Warn |
| Join match rate | ≥ 90% `MATCHED` | **Alert** |
| Null rate in key columns | ≤ 1% | Alert |
| Negative net profit fleet-wide | not more than 30% of vehicles | Warn (sanity check on the cost model) |
| Distance variance | ≤ 10% for ≥ 90% of vehicles | Warn |
| Duplicate `(vehicle_id, sim_date)` in the source | zero | **Fail the DAG** |

Failing loudly on a duplicate primary key rather than silently upserting one over the other is the right call and worth stating.

---

## 5. The consolidated daily report

The PDF requires *"a consolidated (daily/hourly) report or dashboard... that answers the business question"*. We produce **both** — Grafana for live, and a generated file for the daily reconciliation.

`batch_layer/report_render.py`: Jinja2 → HTML → **WeasyPrint** → PDF, written to `s3://fleet-lake/reports/fleet_daily_report_<sim_date>.pdf`.

Contents:
1. **Header** — simulated date, real generation time, data-completeness statement, restatement banner if applicable
2. **Fleet summary** — vehicles active, trips, revenue, total costs, net profit, fleet margin %
3. **The answer, part 1** — utilization and earnings by zone and by time-of-day (a small chart)
4. **The answer, part 2** — per-vehicle profitability table sorted ascending by 7-day rolling profit, with the classification column, top 10 highlighted
5. **`WATCH` and `CRITICAL` vehicles** — the "becoming unprofitable" list, with the trend sparkline
6. **Reconciliation exceptions** — `MISSING_EXPENSE` / `MISSING_TELEMETRY` rows, distance variance outliers
7. **Data-quality summary** — every check with pass/fail
8. **Footer** — job run ID, input paths and row counts, code version. *Provenance, so the number is traceable — the module's lineage/audit-trail material.*

An HTML copy is also written so it can be linked from Grafana.

---

## 6. Performance notes

At demo scale the job is trivial (72,000 rows), so it will finish in ~20 seconds, most of which is Spark start-up. Rather than pretend it is a big-data job, the report states the honest numbers **and** the scaling analysis:

| Scale | Rows/day | Expected runtime | Notes |
|---|---|---|---|
| Demo (150 vehicles) | 72k | ~20 s (mostly JVM start-up) | Single executor is plenty |
| Realistic (5,000 vehicles, 30 s pings) | 14.4M | ~3–5 min on 4 executors | Shuffle on the `Window` becomes the cost centre |
| Large (50,000 vehicles) | 144M | Needs partition-count tuning, `spark.sql.shuffle.partitions` raised from 200, likely broadcast-join the expenses side | |

Tuning applied even at demo scale, with comments explaining why:
- `spark.sql.shuffle.partitions = 8` (the 200 default creates 200 tiny tasks and dominates runtime at this scale — a very common mistake worth showing we avoided)
- `broadcast(expenses)` — 150 rows, always broadcast
- Adaptive Query Execution on (default in Spark 3.x), noted as doing the coalescing for us

---

## 7. Maintenance tasks

Two housekeeping DAG tasks, both good practice and both cheap:

- **`compact_parquet`** — the 10-second streaming trigger creates many small files (`05 §4.1`). Once per simulated day, rewrite the previous day's partition into ~4 files. The small-file problem is real and naming it shows operational awareness.
- **`enforce_retention`** — delete telemetry partitions older than 30 simulated days. Ties to the module's **hot/warm/cold tiering and retention policy** material; in production this would be a lifecycle rule moving cold partitions to cheaper storage rather than deleting.

---

## 8. Testing

| Test | Approach |
|---|---|
| `test_haversine.py` | Known coordinate pairs → known distances (±0.5%) |
| `test_trip_reconstruction.py` | A synthetic ping sequence → correct trip count, duration, distance |
| `test_profitability.py` | Fixed telemetry + expense DataFrames → exact expected profit; covers all three join cases |
| `test_classification.py` | Table-driven over (avg, slope, days_of_history) → expected class, including `INSUFFICIENT_DATA` |
| `test_idempotency.py` | Run the job twice against a test Postgres → identical rows, `restatement_count` incremented once |
| `test_restatement.py` | Run day 2, then re-run with changed expenses → profit changes, `restated_at` set, high-water-mark unchanged |
| `test_dq_checks.py` | Each check triggered independently by a crafted input |
| `test_shared_transforms.py` | **The key test:** assert `compute_zone_utilization()` gives identical results on a static DataFrame whether called from the batch path or the streaming path's `foreachBatch`. This is the executable proof of the `01 §2.3` mitigation. |

---

## 9. What the report must show (§6.3 / §8)

- The dataflow diagram from §2.
- The `full_outer` join decision with the three-case table — this is the strongest "correctness of transformation logic" evidence.
- The trend classification rules, and why `INSUFFICIENT_DATA` exists.
- The idempotency mechanism and the restatement sequence.
- **A screenshot of the same vehicle's profitability before and after the restatement.**
- The Spark UI DAG for the batch job, pointing at the shuffle boundary.
- The honest performance table from §6.
- A page of the generated daily report PDF.
