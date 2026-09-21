# 05 — Requirement 2: Speed Layer Processing

> **PDF requirement:** *"Processing must be consistent with your chosen architecture. Transformations should be **meaningful** for the use case (not just pass-through): cleaning, enrichment, joins between the two sources, aggregation, or windowing."*
>
> **Rubric weight: 15 marks** (shared with `06`) — *"correctness of transformation logic; appropriate use of streaming and/or batch processing consistent with the declared architecture."*

---

## 1. What the speed layer is responsible for

It answers **only the first half** of the business question — *"What is fleet utilization and earnings by area/time-of-day right now?"* — plus the idle alerts. It is knowingly approximate. It never computes profitability, because profitability requires the expense file, which does not exist yet.

Being clear about that boundary is itself an architecture point: **the speed layer's scope is defined by what data exists in real time, not by what would be nice to have.**

---

## 2. Three queries, not one

`src/fleet/speed_layer/streaming_job.py` starts three independent Structured Streaming queries against the same Kafka topic. Rationale in `04 §4` — separate consumer groups, separate checkpoints, separate failure domains.

```
                     fleet.telemetry.v1
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
 ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐
 │ Q1          │    │ Q2           │    │ Q3               │
 │ master      │    │ zone         │    │ idle detector    │
 │ dataset     │    │ aggregates   │    │ (stateful)       │
 │             │    │              │    │                  │
 │ validate    │    │ validate     │    │ validate         │
 │   ↓         │    │   ↓          │    │   ↓              │
 │ append      │    │ enrich       │    │ groupByKey       │
 │ Parquet     │    │   ↓          │    │  (vehicle_id)    │
 │             │    │ watermark    │    │   ↓              │
 │ append mode │    │   ↓          │    │ flatMapGroups    │
 │ checkpoint  │    │ window+agg   │    │  WithState       │
 │             │    │   ↓          │    │   ↓              │
 │             │    │ foreachBatch │    │ alerts → Kafka   │
 │             │    │  → Redis     │    │  + Redis         │
 │ update mode │    │              │    │                  │
 └──────┬──────┘    └──────┬───────┘    └────────┬─────────┘
        ▼                  ▼                     ▼
    MinIO Parquet       Redis              fleet.alerts.v1
   (master dataset)  (speed view)            + Redis ZSET
```

Invalid events from any query are routed to the DLQ; in practice Q1 owns DLQ emission so a bad event is dead-lettered exactly once.

---

## 3. The transformation chain

All of this lives in `src/fleet/transforms/` as **pure DataFrame → DataFrame functions**. No `readStream`, no `writeStream`, no Redis, no side effects. That is what makes them unit-testable without Spark streaming and reusable by the batch layer — the mitigation promised in `01 §2.3`.

### 3.1 Cleaning / validation — `transforms/validate.py`

```python
def validate_telemetry(df: DataFrame) -> DataFrame:
    """Adds `is_valid` (bool) and `rejection_reason` (string, null when valid)."""
```

| Check | Rule | Reason code |
|---|---|---|
| Coordinates present | `lat IS NOT NULL AND lon IS NOT NULL` | `NULL_COORDINATES` |
| Coordinates in bounds | within the city bounding box | `COORDINATES_OUT_OF_BOUNDS` |
| Fare non-negative | `fare IS NULL OR fare >= 0` | `NEGATIVE_FARE` |
| Speed plausible | `0 <= speed_kmh <= 200` | `IMPLAUSIBLE_SPEED` |
| Timestamp not in future | `event_time <= sim_now() + 5 min` | `FUTURE_TIMESTAMP` |
| Status/trip consistency | `trip_id IS NULL` **iff** `status = 'idle'` | `STATUS_TRIP_MISMATCH` |
| Fare/status consistency | `fare IS NOT NULL` when `status = 'on_trip'` | `MISSING_FARE_ON_TRIP` |

Then `df.filter(col("is_valid"))` continues; `df.filter(~col("is_valid"))` goes to the DLQ. **Nothing is dropped silently** — every rejection is counted and dead-lettered.

Deduplication: `dropDuplicates(["event_id"])` with a watermark, so the state bounded. Counted as `events_deduplicated_total`.

### 3.2 Enrichment — `transforms/enrich.py`

Two enrichments, and the implementation choice matters:

**(a) Zone assignment from lat/lon.** The obvious implementation is a Python UDF. **We do not use one**, and the report explains why: a Python UDF forces per-row serialisation between the JVM and a Python worker, defeats Catalyst optimisation, and the module specifically taught that Spark's advantage comes from the Catalyst optimiser and whole-stage codegen. Instead:

```python
# Zones are 12 lat/lon bounding boxes → a broadcast join with a range condition,
# which stays inside the JVM and is optimised by Catalyst.
zones_df = broadcast(spark.createDataFrame(ZONE_BOXES))
enriched = events.join(
    zones_df,
    (events.lat >= zones_df.lat_min) & (events.lat < zones_df.lat_max) &
    (events.lon >= zones_df.lon_min) & (events.lon < zones_df.lon_max),
    how="left",
)
```

This is a **stream-static broadcast join**, and being able to explain why it beats a UDF is a strong viva answer.

**(b) Vehicle registry join.** Read `fleet.vehicle.registry` (compacted) as a static DataFrame, deduplicate to the latest record per key, broadcast, left-join on `vehicle_id`. Brings in `fuel_type`, `model`, `home_zone`. Refreshed every 200 micro-batches (registry changes are rare); refresh count is a metric.

> **Note the phrasing for the report:** the PDF asks for *"joins between the two sources"*. In our Lambda design that join — telemetry ⨝ expenses — is the **batch layer's** job (`06 §4`), because expenses do not exist in real time. The speed layer's joins are enrichment joins (zones, registry). Both are real joins; be precise about which is which rather than blurring them.

### 3.3 Windowed aggregation — `transforms/utilization.py`

```python
.withWatermark("event_time", "30 minutes")        # simulated → 6.25 real seconds (see 03 §3.3)
.groupBy(
    window(col("event_time"), "15 minutes", "5 minutes"),   # SLIDING window
    col("zone_id"),
)
.agg(
    approx_count_distinct("vehicle_id").alias("active_vehicles"),
    approx_count_distinct(when(col("status") == "on_trip", col("trip_id"))).alias("trips"),
    sum(when(col("status") == "on_trip", col("fare_delta")).otherwise(0)).alias("earnings"),
    avg("speed_kmh").alias("avg_speed"),
    (sum(when(col("status") == "idle", 1).otherwise(0)) / count("*")).alias("idle_ratio"),
)
```

**Why a sliding window** (15-simulated-minute width, 5-simulated-minute slide) rather than tumbling: the dashboard needs a *smoothly updating* current picture. A tumbling window would make the "trips per hour" panel jump discontinuously every 15 simulated minutes and show a nearly-empty bucket at the start of each window. A sliding window updates every 5 simulated minutes with a full 15 simulated minutes of context. The module taught all four window types; we choose sliding and say why — *"an event can belong to multiple windows"* is the property we want here.

**Why `approx_count_distinct`** rather than `countDistinct`: exact distinct counting is a stateful shuffle that grows with cardinality. HyperLogLog is ~2% error, which is invisible in a "how many vehicles are active" number, and bounded in memory. **This is the speed layer's approximation being a deliberate design choice rather than an accident** — the module's phrase for the speed layer is *"immediate but less accurate"*, and this is us implementing that trade-off knowingly. The batch layer uses exact `countDistinct`.

**Output mode: `update`.** Only changed windows are emitted each micro-batch. `complete` would re-emit every window ever seen (unbounded and pointless); `append` would emit only on window close (too late — the whole point is a live view of the *current* window).

### 3.4 Fare deltas — a correctness detail worth writing down

Telemetry carries the **accumulated** fare on a trip, not the increment. Naively `sum(fare)` over a window would massively over-count, because each ping repeats the running total.

We compute `fare_delta` per vehicle as the increase since its previous ping, using a per-key state (in Q3's state store) or, more simply and defensibly, by summing **only the final fare at dropoff** (`status` transitions `on_trip → idle` with a non-null `fare`). We take the second approach:

```python
# Earnings recognised at trip completion only.
completed = df.filter((col("status") == "idle") & col("prev_status").eqNullSafe("on_trip"))
```

This means earnings lag by up to one trip duration in the live view — an approximation we **document in the API response and the report** rather than hide. The batch layer recognises revenue exactly, per trip, from the master dataset.

This is a good example to raise in the viva: it shows we found a real correctness trap and made an explicit, documented trade-off.

### 3.5 Stateful idle detection — `speed_layer/idle_detector.py`

The PDF's suggested output: *"Threshold-based alerts when a vehicle is considerably idle for a longer period of time."*

```python
def track_idle(vehicle_id, events_iter, state: GroupState[IdleState]) -> Iterator[IdleAlert]:
    """
    State per vehicle: (idle_since_sim, last_status, alert_level_emitted).
    - status leaves 'idle'      → clear state
    - status enters/stays 'idle'→ if now - idle_since >= threshold and not yet
                                   alerted at this level, emit and record the level
    Escalation: 45 sim-min = WARNING, 90 sim-min = CRITICAL.
    Timeout: GroupStateTimeout.EventTimeTimeout at watermark + 2h, so a vehicle
    that stops reporting entirely does not leak state forever.
    """
```

Applied with `df.groupByKey(lambda r: r.vehicle_id).flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(track_idle)`.

Alert IDs are `sha1(vehicle_id + idle_since_sim)` so the same idle episode always yields the same ID — this is what makes at-least-once delivery safe downstream (see `04 §5`).

Emitted to `fleet.alerts.v1` **and** written to a Redis ZSET for the API.

> **This is the highest-risk component in the project.** `flatMapGroupsWithState` in PySpark is fiddly, state encoding is easy to get wrong, and it is hard to debug. See §6 for the fallback.

---

## 4. Sinks

### 4.1 Q1 → MinIO Parquet (the master dataset)

```python
(validated
   .withColumn("sim_date", to_date(col("event_time")))
   .writeStream
   .format("parquet")
   .option("path", "s3a://fleet-lake/raw/telemetry/")
   .option("checkpointLocation", "s3a://fleet-lake/_checkpoints/q1_master/")
   .partitionBy("sim_date")
   .outputMode("append")
   .trigger(processingTime="10 seconds")
   .start())
```

- **Partitioned by `sim_date`** — the batch job's partition pruning depends on it.
- **10-second trigger**, not the default. The default (as-fast-as-possible) would produce hundreds of tiny Parquet files, and the small-file problem would make the batch job slow. 10 seconds ≈ 2,400 events per file — a reasonable file size. Airflow runs a small compaction task nightly (`06 §7`).
- **This query must be the most reliable thing in the system.** It has its own checkpoint, its own consumer group, and a dedicated `StreamingQueryStalled` alert at higher severity than the others.

### 4.2 Q2 → Redis (the speed view)

Via `foreachBatch`, because Redis has no native Spark sink:

```python
def write_zone_aggregates(batch_df: DataFrame, batch_id: int) -> None:
    with tracer.start_as_current_span("speed.sink.redis") as span:
        span.set_attribute("batch_id", batch_id)
        rows = batch_df.collect()          # safe: ≤ 12 zones × a few open windows
        pipe = redis.pipeline()
        for r in rows:
            key = f"fleet:zone:{r.zone_id}"
            pipe.hset(key, mapping={...})  # HSET, never HINCRBY — see 04 §5
            pipe.expire(key, TTL_SPEED_VIEW)
        pipe.hset("fleet:snapshot", mapping=_rollup(rows))
        pipe.execute()
        sink_write_duration.observe(...)
```

`collect()` is safe here **only because the aggregate is tiny** (12 zones). The code carries a comment saying so, because `collect()` in a streaming sink is otherwise a red flag a marker would rightly question.

**`HSET`, never `HINCRBY`** — the aggregate is recomputed from scratch each micro-batch, so overwriting makes the sink idempotent under micro-batch replay. This is the single most important line of reasoning in the sink and it is commented in the code.

### 4.3 Q3 → Kafka + Redis

Alerts are written to `fleet.alerts.v1` (native Kafka sink, exactly-once-capable) and mirrored to a Redis ZSET `fleet:alerts:idle` scored by simulated timestamp, trimmed to the most recent 200.

---

## 5. Checkpointing and recovery

| Query | Checkpoint location | On restart |
|---|---|---|
| Q1 | `s3a://fleet-lake/_checkpoints/q1_master/` | Resumes from committed offset; the `_spark_metadata` log prevents duplicate file commits |
| Q2 | `s3a://fleet-lake/_checkpoints/q2_agg/` | Resumes; re-runs the last uncommitted batch → idempotent `HSET` makes this safe |
| Q3 | `s3a://fleet-lake/_checkpoints/q3_idle/` | **Restores the per-vehicle state store**, so idle timers survive a restart |

**A trap to document:** changing the aggregation logic or the state schema invalidates a checkpoint — Spark will refuse to start or behave unpredictably. `make reset-checkpoints` exists and the README warns about it. This is worth mentioning in limitations, because it is a genuine operational cost of stateful streaming that the module did not cover.

---

## 6. Risk and fallback for the stateful detector

`flatMapGroupsWithState` is the riskiest piece. If it consumes more than half a day on days 4–5, **switch to the fallback** rather than pushing on:

**Fallback:** track idle state in `foreachBatch` against Redis instead of Spark state.
```
for each vehicle in the micro-batch:
    if status == 'idle':
        SETNX fleet:vehicle:{id}:idle_since <sim_time>   # only sets if absent
        if sim_now - idle_since >= 45 min and not already alerted:
            emit alert; SET alerted flag
    else:
        DEL fleet:vehicle:{id}:idle_since, alerted flag
```

Trade-off to state honestly in the report if used: state lives in Redis rather than in a checkpointed Spark state store, so it is not recovered from the checkpoint on restart and it is not exactly-once. In exchange it is ~15 lines, trivially testable, and easy to defend line-by-line. **At our scale it is arguably the better engineering choice anyway** — that is a legitimate position, not an excuse, provided we say it deliberately.

---

## 7. Metrics emitted by the speed layer

Via a `StreamingQueryListener` (details in `09 §3.3`) plus in-sink counters:

| Metric | Type | Labels |
|---|---|---|
| `spark_streaming_input_rows_per_second` | gauge | `query` |
| `spark_streaming_processed_rows_per_second` | gauge | `query` |
| `spark_streaming_batch_duration_seconds` | histogram | `query` |
| `spark_streaming_state_rows` | gauge | `query` |
| `spark_streaming_watermark_lag_seconds` | gauge | `query` |
| `spark_streaming_last_progress_timestamp` | gauge | `query` |
| `events_validated_total` | counter | `result=valid\|invalid` |
| `events_dlq_total` | counter | `reason` |
| `events_deduplicated_total` | counter | — |
| `sink_write_duration_seconds` | histogram | `sink=redis\|parquet\|kafka` |
| `sink_write_errors_total` | counter | `sink` |
| `business_alerts_emitted_total` | counter | `type`, `severity` |
| `registry_refresh_total` | counter | — |

---

## 8. Testing

Because the transforms are pure DataFrame functions, most tests need only a **local batch** `SparkSession` — no streaming, no Kafka, no Docker.

| Test | Approach |
|---|---|
| `test_validate.py` | Build a DataFrame with one row per defect type; assert `is_valid` and `rejection_reason` |
| `test_enrich_zones.py` | Known lat/lon → known zone; a point outside all boxes → null zone (and is counted, not dropped) |
| `test_utilization.py` | Fixed input DataFrame → assert exact aggregate values. **Same function the batch layer calls**, so this test protects both layers. |
| `test_fare_delta.py` | A full trip sequence → earnings recognised once, at dropoff, with the right amount |
| `test_idle_state.py` | Drive `track_idle` directly as a plain Python function with a fake `GroupState` — no Spark needed. Covers: enters idle, stays idle past threshold, escalates, leaves idle, resumes idle later (new alert ID) |
| `test_windowing.py` | Use `MemoryStream` (or batch equivalence) to assert sliding-window boundaries and watermark drop behaviour |
| `test_sink_idempotence.py` | Run the same micro-batch through `write_zone_aggregates` twice against a fake Redis; assert identical final state |

---

## 9. What the report must show (§6.3)

- The three-query diagram from §2.
- The windowing choice with its justification (sliding vs tumbling) — tying back to the taught window taxonomy.
- The watermark calculation in **simulated** time, with the 6.25-real-second figure.
- The `approx_count_distinct` decision as a deliberate instance of the module's *"immediate but less accurate"* speed layer.
- The `HSET`-not-`HINCRBY` idempotence argument.
- A Spark UI screenshot of the Structured Streaming tab showing input rate, processing rate and batch duration.
- Honest statement of the fare-recognition lag.
