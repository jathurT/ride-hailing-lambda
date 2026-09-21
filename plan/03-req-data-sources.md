# 03 — Requirement 1: Simulated Data Sources

> **PDF requirement:** *"Your system must ingest from simulated sources, built as Python scripts: a **streaming source** that continuously emits events every few seconds... a **batch source** that uploads/drops a new file once per simulated day... You may compress simulated time. **State your simulated clock clearly in the report.**"*
>
> **Rubric weight: 15 marks** — *"correctness and **robustness** of simulated sources."* Robustness is the word to optimise for: retries, graceful shutdown, delivery confirmation, deterministic seeding.

---

## 1. What we are building

| File | Role | Runs as |
|---|---|---|
| `src/fleet/ingestion/telemetry_producer.py` | **Streaming source.** 150 vehicles emitting GPS/telemetry to Kafka | Long-lived container, `restart: unless-stopped` |
| `src/fleet/ingestion/expense_dropper.py` | **Daily-batch source.** Writes one expense CSV per simulated day to MinIO | Long-lived container, sleeps between simulated days |
| `src/fleet/ingestion/registry_producer.py` | Seeds the log-compacted vehicle reference topic (runs once at start-up, then idles) | One-shot init container |
| `src/fleet/common/simclock.py` | **The shared simulated clock.** Imported by producers, Spark jobs, Airflow DAGs and the API | Library |

---

## 2. Fleet model

A believable fleet, defined once in `data/fleet_seed.json` and consumed by both producers so they agree on vehicle identity.

```
150 vehicles      V001 … V150
150 drivers       D001 … D150  (1:1 assignment, stable for the demo)
12 zones          Z01 … Z12    (a 4×3 grid over a fictional city)
```

**Vehicle registry record** (published to the compacted topic, joined in as enrichment):

| Field | Values | Why it exists |
|---|---|---|
| `vehicle_id` | `V001`–`V150` | Key |
| `model` | Corolla / Prius / Vitz / Wagon-R / Alto | Cosmetic realism |
| `fuel_type` | `petrol` \| `hybrid` \| `electric` (60/30/10%) | **Load-bearing:** drives fuel cost per km in the expense generator, so hybrids and EVs really do show better profitability. Makes the daily report *say something*. |
| `capacity` | 4 or 6 | Affects fare band |
| `home_zone` | `Z01`–`Z12` | Vehicles idle preferentially near home |
| `onboarded_sim_date` | date | SCD2 dimension seed |

**Zone model** — 12 zones with different characters, so the "earnings by area" answer is non-uniform:

| Zone class | Zones | Demand weight | Surge behaviour |
|---|---|---|---|
| CBD / business | Z01, Z02 | 1.8× | Strong morning + evening peaks |
| Airport | Z03 | 1.4× | Flat, high fares, long trips |
| Residential | Z04–Z08 | 1.0× | Inverse of CBD (outbound morning) |
| Industrial | Z09, Z10 | 0.6× | Shift-change spikes at 06:00/14:00/22:00 |
| Outskirts | Z11, Z12 | 0.3× | Low demand — these are where vehicles go unprofitable |

Zone assignment is by **lat/lon bounding box**, implemented as a broadcast lookup table (`common/zones.py`), *not* a Python UDF — see `05 §3.2` for why.

---

## 3. The simulated clock — the part that is easy to get wrong

**This is the subtlest piece of the whole project.** Four independent processes (producer, expense dropper, Spark, Airflow) must agree on what simulated day it is, or the join silently produces nothing and you lose a day debugging it.

### 3.1 Design

```python
# src/fleet/common/simclock.py

SIM_EPOCH_WALL: datetime   # real UTC instant when the simulation started
SIM_EPOCH_SIM:  datetime   # simulated datetime that instant corresponds to
SIM_DAY_SECONDS: int = 300 # 1 simulated day = 5 real minutes
SPEEDUP: float = 86400 / SIM_DAY_SECONDS   # = 288.0

def sim_now() -> datetime:
    elapsed_real = (utcnow() - SIM_EPOCH_WALL).total_seconds()
    return SIM_EPOCH_SIM + timedelta(seconds=elapsed_real * SPEEDUP)

def sim_date() -> date:          return sim_now().date()
def sim_day_index() -> int:      return (sim_now() - SIM_EPOCH_SIM).days
def real_to_sim(seconds) -> float:  return seconds * SPEEDUP
def sim_to_real(seconds) -> float:  return seconds / SPEEDUP
```

**Anchoring — the critical rule.** `SIM_EPOCH_WALL` is decided **once**, by an init container at stack start-up, and written to `redis: sim:epoch_wall` *and* to a bind-mounted file `./state/sim_epoch.json`. Every other process **reads** it; nobody computes it locally. Redis is the primary source (fast, shared); the file is the fallback for Spark executors that may not have a Redis client.

Why both: a Spark executor deserialising a closure must be able to resolve simulated time without a network round-trip per row, so the epoch is **broadcast** into the job at submission time as a plain value.

`SIM_EPOCH_SIM` is fixed at `2026-03-01T00:00:00Z` so the demo always shows the same dates and screenshots stay consistent across runs.

### 3.2 Dual timestamps on every event — do not skip this

Every telemetry record carries **two** timestamps:

| Field | Type | Value | Used for |
|---|---|---|---|
| `event_time` | timestamp | **simulated** time the ping occurred | Spark windowing and watermarks; Parquet partitioning; all business aggregation |
| `ingest_time` | timestamp | **real** wall-clock time the producer sent it | Prometheus latency histograms; end-to-end pipeline lag; trace timing |

Mixing these up is the classic failure. A dashboard panel showing "end-to-end latency" must use real time; a window computing "earnings in the last hour" must use simulated time.

### 3.3 Watermarks must be expressed in SIMULATED time

At 288×, **one real second = 4.8 simulated minutes.** So:

- A watermark of `"10 minutes"` in Spark (which reads `event_time`, a simulated timestamp) tolerates only **2.08 real seconds** of lateness.
- Real network jitter, a Kafka rebalance, or a GC pause easily exceeds that → events silently dropped → windows empty → hours lost debugging.

**Rule: set the watermark generously in simulated terms.** We use **`withWatermark("event_time", "30 minutes")`** = 6.25 real seconds of tolerance. Documented inline with this exact calculation:

```python
# 30 simulated minutes ÷ 288× speedup = 6.25 real seconds of lateness tolerance.
# Chosen to survive Kafka rebalances and GC pauses; see plan/03 §3.3.
.withWatermark("event_time", "30 minutes")
```

A unit test asserts `sim_to_real(WATERMARK_SIM_SECONDS) >= 5.0` so that changing `SIM_DAY_SECONDS` without revisiting the watermark fails CI. **This test is worth mentioning in the viva** — it shows the coupling was understood, not stumbled into.

### 3.4 Alignment table — what maps to what

| Simulated | Real | Notes |
|---|---|---|
| 1 simulated day | 5 minutes | The assignment's own suggested compression |
| 1 simulated hour | 12.5 seconds | |
| 3 simulated minutes (ping interval) | 0.625 s | Per vehicle |
| 30 simulated minutes (watermark) | 6.25 s | Lateness tolerance |
| 15 simulated minutes (window size) | 3.1 s | Sliding window, 5-simulated-minute slide |
| 45 simulated minutes (idle threshold) | 9.4 s | Business alert trigger |
| 3 simulated days (full demo) | 15 minutes | Enough to show batch + restatement |
| 7 simulated days (rolling trend) | 35 minutes | Full trend demo; optional longer run |

---

## 4. Streaming source — `telemetry_producer.py`

### 4.1 Event schema (Avro, `schemas/telemetry.v1.avsc`)

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Idempotency / dedup key |
| `trip_id` | string, nullable | `null` when the vehicle is `idle` |
| `driver_id` | string | |
| `vehicle_id` | string | **Kafka message key** |
| `lat`, `lon` | double | |
| `speed_kmh` | double | |
| `status` | enum `idle \| enroute \| on_trip` | |
| `fare` | double, nullable | Accumulated fare so far on the trip; `null` when not `on_trip` |
| `event_time` | timestamp-millis | **Simulated** |
| `ingest_time` | timestamp-millis | **Real** |
| `producer_id` | string | Which simulator instance — useful when we scale to 2 producers |
| `schema_version` | int | Belt-and-braces alongside the registry |

> **Deviation from the PDF's field list, declared in the report:** we add `event_id`, `ingest_time`, `producer_id` and `schema_version`. The PDF permits adaptation (*"You may adapt scope (e.g. add/change fields as required)"*). Each addition has an operational reason — idempotency, latency measurement, multi-producer attribution, evolution safety — and the report states them.

### 4.2 Vehicle state machine

Each vehicle is an independent object stepping through:

```
        ┌──────────────────────────────────────────┐
        │                                          │
        ▼                                          │
    ┌────────┐  demand draw  ┌─────────┐  pickup  ┌─────────┐
    │  IDLE  │ ─────────────▶│ ENROUTE │ ────────▶│ ON_TRIP │
    └────────┘               └─────────┘          └─────────┘
        ▲                                              │ dropoff
        └──────────────────────────────────────────────┘

  IDLE     dwell ~ LogNormal(μ=8, σ=0.6) simulated minutes, scaled by 1/zone_demand(hour)
  ENROUTE  2–8 simulated minutes to reach the pickup point
  ON_TRIP  8–35 simulated minutes; airport trips 25–50
```

While in any state the vehicle emits a ping every **3 simulated minutes** containing its current position, speed and status.

### 4.3 Making it look real, not uniform-random

This matters for the 15 marks and for the dashboard being worth looking at.

**Diurnal demand curve.** `demand(sim_hour)` is a sum of two Gaussians plus a floor:
```
morning peak  N(μ=8.0,  σ=1.2) × 1.0
evening peak  N(μ=18.0, σ=1.5) × 1.2
night floor   0.15
```
Trip starts per zone per simulated minute are drawn `Poisson(λ = base_λ × demand(hour) × zone_weight)`. Result: the "earnings by time-of-day" panel shows a genuine twin-peak shape, and the "utilization by hour" panel shows idle ratio inverting overnight.

**Movement.** A trip is a straight-line interpolation from pickup to dropoff in lat/lon with:
- Gaussian jitter (σ ≈ 0.0004°, ~45 m) so the GPS track looks noisy rather than ruler-straight
- Speed drawn from `N(μ = 45 − 20×congestion(zone, hour), σ = 8)` clamped to `[0, 90]`
- Position advanced by `speed × elapsed_sim_time`, so distance and speed are self-consistent (important — the batch job recomputes distance from positions and it must roughly agree with reported speed)

**Fares.** `fare = base(2.50) + per_km(0.85)×km + per_min(0.15)×min`, multiplied by `surge(zone, hour)` which rises to 1.8× at peak in CBD zones. Airport trips add a flat 3.00 pickup levy. Fares accumulate during `on_trip` and are finalised at dropoff.

**Idle placement.** After a dropoff, a vehicle drifts toward its `home_zone` with probability 0.6, otherwise stays put. This produces realistic zone imbalance — outskirt zones accumulate idle vehicles, which is what makes the "zone underserved" and "vehicle unprofitable" signals meaningful rather than random.

### 4.4 Determinism and scripted anomalies

Everything is seeded: `--seed 42` (default, and set in compose). Two vehicles run **scripted narratives** so the demo shows the same thing every time:

| Vehicle | Script | Demonstrates |
|---|---|---|
| **`V007`** | Goes `idle` at simulated 10:00 on simulated day 1 and stays idle for **90 simulated minutes** (≈19 real seconds) | Triggers the `VEHICLE_IDLE_PROLONGED` business alert on cue, in the speed layer |
| **`V113`** | Operates almost exclusively in outskirt zones Z11/Z12, low fares, high distance | Becomes the top **unprofitable** vehicle in the daily report — the batch layer's headline finding |

Both are documented in the README and in the demo script, so the reviewer knows to watch for them.

### 4.5 Deliberate bad data — exercising the cleaning path

Roughly **1%** of emitted events are intentionally malformed, on a seeded schedule. This is not laziness — the PDF wants *"cleaning"* to be a meaningful transformation, and we cannot demonstrate cleaning without dirt.

| Defect | Rate | What the pipeline must do |
|---|---|---|
| `lat`/`lon` null | 0.3% | Route to DLQ, reason `NULL_COORDINATES` |
| `lat`/`lon` outside the city bounding box (GPS glitch) | 0.2% | DLQ, `COORDINATES_OUT_OF_BOUNDS` |
| Negative `fare` | 0.2% | DLQ, `NEGATIVE_FARE` |
| `speed_kmh` > 200 (sensor fault) | 0.2% | DLQ, `IMPLAUSIBLE_SPEED` |
| `event_time` 2 simulated hours in the future (clock skew) | 0.1% | DLQ, `FUTURE_TIMESTAMP` |
| Duplicate `event_id` re-sent | 0.05% | Deduplicated by the streaming job, counted |

Each defect is counted in `events_dlq_total{reason="…"}` so the DLQ dashboard panel has a real breakdown, and the `HighDLQRate` alert has something to threshold against. We can also raise the rate at runtime (`DEFECT_RATE=0.20`) to **make the alert fire on demand during the demo**.

### 4.6 Robustness — this is where the 15 marks live

| Concern | Implementation |
|---|---|
| **Delivery guarantee** | `confluent-kafka` producer with `enable.idempotence=true`, `acks=all`, `retries=10`, `max.in.flight.requests.per.connection=5`. The module taught idempotent producers and exactly-once semantics — we use them and say so. |
| **Delivery confirmation** | Async `delivery_report` callback increments `events_produced_total{status="ok"|"failed"}` and logs failures with the full error. We never fire-and-forget. |
| **Broker unavailable at start-up** | Retry loop with exponential backoff (1 s → 30 s cap) and a clear log line each attempt. The container must not crash-loop when Kafka is still starting — compose `depends_on: service_healthy` helps but is not sufficient. |
| **Backpressure** | Bounded local queue (`queue.buffering.max.messages=100000`); on `BufferError` we block rather than drop, increment `producer_backpressure_events_total`, and log a warning. Dropping telemetry silently would be a correctness bug. |
| **Graceful shutdown** | `SIGTERM`/`SIGINT` handler → stop generating → `producer.flush(timeout=10)` → close → log final counts. Ensures `docker compose down` doesn't lose buffered events mid-demo. |
| **Rate control** | A token-bucket paced against real time, so we hit ≈240 events/s rather than spinning as fast as the CPU allows. Configurable `TARGET_EPS`. |
| **Health** | `/health` (liveness) and `/metrics` on port 8001; compose healthcheck curls `/health`. |
| **Observability** | Structured JSON logs with `stage="ingest"`, `sim_date`, `vehicle_id` as `correlation_id`, `trace_id`. One OTel span per flush batch with `traceparent` injected into Kafka headers. |
| **Config** | `pydantic-settings` from env; no magic numbers in code. `--dry-run` prints events to stdout without Kafka, for development. |

---

## 5. Daily-batch source — `expense_dropper.py`

### 5.1 What it produces

One CSV per simulated day, written to MinIO at:
```
s3://fleet-lake/landing/expenses/expenses_2026-03-14.csv
```

| Column | Type | Generation rule |
|---|---|---|
| `vehicle_id` | string | One row per vehicle that was active that simulated day |
| `fuel_cost` | decimal(10,2) | `distance_covered × rate(fuel_type) × (1 + N(0, 0.08))` — petrol 0.42/km, hybrid 0.26/km, electric 0.11/km |
| `maintenance_cost` | decimal(10,2) | Usually small (0–15); spikes to 150–600 when `service_flag = true` |
| `distance_covered` | decimal(10,2) | The simulator's *own* record of km driven that simulated day, ± 3% noise (partners measure slightly differently than GPS) |
| `service_flag` | boolean | 4% of vehicles per day |
| `submitted_at` | timestamp | Real wall time — used to detect late submissions |
| `partner_id` | string | `GARAGE_A`…`GARAGE_D` — lets the report break down data quality by partner |

**The ±3% distance discrepancy is deliberate.** The batch job computes distance independently from GPS (haversine over consecutive pings) and compares. The difference becomes a **data-quality metric** (`expense_distance_variance_pct`) and a talking point in the report: real reconciliation pipelines always have two sources that disagree slightly, and a good pipeline surfaces the disagreement rather than silently picking one.

### 5.2 The restatement — the architecture argument, executed

On **simulated day 3**, the dropper additionally writes:
```
s3://fleet-lake/landing/expenses/expenses_2026-03-02.csv     ← overwrites day 2
```
with corrected figures for ~8 vehicles (a fuel card mis-assignment, restated). It also writes a marker `restatements/2026-03-02.json` describing what changed.

Airflow's DAG detects the marker and triggers a **re-run of the day-2 reconciliation**, the batch job recomputes that Parquet partition, upserts the fact table, and the dashboard figures visibly change.

**This is the single most valuable 30 seconds of the demo video.** It converts the reprocessing argument from `01 §2.7` into something the marker watches happen.

### 5.3 Robustness

| Concern | Implementation |
|---|---|
| **Atomic drop** | Write to `landing/expenses/.tmp/expenses_<date>.csv` then `copy_object` to the final key and delete the temp. Prevents Airflow's sensor firing on a half-written file — a real and very common bug. |
| **Late arrival simulation** | With probability 0.15 the file is delayed by a random 20–80 real seconds after the simulated day boundary, so the Airflow sensor's `poke`/timeout behaviour is genuinely exercised rather than always succeeding instantly. |
| **Missing-file simulation** | On simulated day 5 (optional, `SIMULATE_MISSING_DAY=5`) no file is dropped at all → the sensor times out → the DAG branches to `handle_missing_expense_file` → raises a pipeline alert → the day is marked incomplete rather than silently producing a wrong report. Demonstrates branching and trigger rules. |
| **Schema drift simulation** | On simulated day 4 (optional) a column is renamed → validation fails → file quarantined to `quarantine/` → alert. Demonstrates the validation branch. |
| **Idempotency** | Re-running for the same simulated date overwrites the same key; downstream upserts are keyed, so no duplicates. |
| **Checksums** | An `.md5` sidecar is written and verified by the DAG before parsing. |
| **Metrics/logs** | `/metrics` on 8002: `expense_files_written_total`, `expense_rows_written_total`, `expense_restatements_total`. JSON logs with `stage="ingest"`, `source="expenses"`. |

---

## 6. Testing the sources

| Test | File | Asserts |
|---|---|---|
| Sim clock conversions round-trip | `tests/unit/test_simclock.py` | `sim_to_real(real_to_sim(x)) == x`; `sim_day_index` advances exactly once per 300 real seconds |
| **Watermark safety** | `tests/unit/test_simclock.py` | `sim_to_real(WATERMARK_SIM_SECONDS) >= 5.0` — fails CI if someone changes the speed-up without revisiting the watermark |
| Zone lookup | `tests/unit/test_zones.py` | Known lat/lon → known zone; out-of-bounds → `None` |
| Fare calculation | `tests/unit/test_fares.py` | Base + distance + time + surge, table-driven |
| State machine | `tests/unit/test_vehicle_state.py` | Only legal transitions occur over 10,000 steps; `trip_id` is null iff `status == "idle"` |
| Determinism | `tests/unit/test_determinism.py` | Same seed → byte-identical first 1,000 events. **Protects the scripted demo narratives.** |
| Defect injection rate | `tests/unit/test_defects.py` | Over 100k events, each defect type lands within ±20% of its configured rate |
| Expense generation | `tests/unit/test_expenses.py` | Every active vehicle gets exactly one row; costs are non-negative; EV fuel cost < petrol for equal distance |
| Avro round-trip | `tests/contract/test_schemas.py` | Serialise → deserialise → identical; and a v1-record deserialises under the v2 reader schema |

---

## 7. What the report must state (§6.1)

A short, explicit block — the PDF asks for the simulated clock to be *"stated clearly"* and for *"any assumptions, simplifications, or simulated-time compression"* to be declared:

> **Simulated clock.** One simulated day is compressed to **5 real minutes** (a speed-up of **288×**), matching the compression suggested in the assignment brief. The simulation begins at simulated `2026-03-01T00:00:00Z`, anchored to a single wall-clock epoch established at stack start-up and shared by all components via Redis and a mounted state file. Every event carries both a simulated `event_time` (used for all windowing, watermarking and partitioning) and a real `ingest_time` (used for latency measurement). Stream watermarks are expressed in simulated time: a 30-simulated-minute watermark corresponds to 6.25 real seconds of lateness tolerance.
>
> **Assumptions and simplifications.** 150 vehicles over 12 zones in a fictional city; straight-line trip paths with Gaussian GPS jitter rather than road-network routing; fares from a simple base + distance + time + surge model; expenses generated from simulated distance with a deliberate ±3% measurement discrepancy to model real partner reconciliation; approximately 1% of events deliberately malformed to exercise the cleaning and dead-letter paths; two vehicles (`V007`, `V113`) follow scripted narratives so demonstration outcomes are reproducible under a fixed random seed.
