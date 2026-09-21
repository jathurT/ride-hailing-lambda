# 07 — Requirement 3: Storage & Serving Layer

> **PDF requirement:** *"Processed results must land in a queryable store suited to your serving needs. The final deliverable of the running system is a consolidated (daily/hourly) report or dashboard... that answers the business question."*
>
> **Rubric weight: 10 marks** — *"correctness of implementation of the serving layer."*
>
> **§5 of this document is the most important section in the whole plan.** It is where Lambda's stated weakness — *"reconciling data between systems"* — is answered in code.

---

## 1. Three stores, three jobs

| Store | Holds | Written by | Read by | Durability |
|---|---|---|---|---|
| **MinIO / Parquet** | The immutable master dataset + landed files + reports | Speed-layer Q1; expense dropper; report renderer | Batch job; humans via console | Permanent |
| **Redis** | The speed view — approximate, current, TTL'd | Speed-layer Q2 & Q3 | FastAPI; Grafana | **Disposable by design** |
| **PostgreSQL** | The batch view — exact, historical, star schema | Airflow/Spark batch job | FastAPI; Grafana | Permanent, ACID |

The three-way split is the Lambda architecture made physical, and each store's durability profile matches its layer's role.

---

## 2. PostgreSQL — the batch-layer data mart

Schema `mart`. Star schema, per the module's OLAP material.

### 2.1 Dimensions

```sql
CREATE TABLE mart.dim_vehicle (
    vehicle_key      SERIAL PRIMARY KEY,
    vehicle_id       TEXT        NOT NULL,
    model            TEXT        NOT NULL,
    fuel_type        TEXT        NOT NULL CHECK (fuel_type IN ('petrol','hybrid','electric')),
    capacity         SMALLINT    NOT NULL,
    home_zone        TEXT        NOT NULL,
    valid_from       DATE        NOT NULL,      -- SCD Type 2
    valid_to         DATE,                      -- NULL = current
    is_current       BOOLEAN     NOT NULL DEFAULT TRUE,
    UNIQUE (vehicle_id, valid_from)
);

CREATE TABLE mart.dim_zone (
    zone_key    SERIAL PRIMARY KEY,
    zone_id     TEXT UNIQUE NOT NULL,
    zone_name   TEXT NOT NULL,
    zone_class  TEXT NOT NULL,      -- cbd | airport | residential | industrial | outskirts
    demand_weight NUMERIC(4,2) NOT NULL
);

CREATE TABLE mart.dim_date (
    date_key    SERIAL PRIMARY KEY,
    sim_date    DATE UNIQUE NOT NULL,
    sim_dow     SMALLINT NOT NULL,
    is_weekend  BOOLEAN  NOT NULL,
    sim_day_index INT    NOT NULL
);

CREATE TABLE mart.dim_driver (
    driver_key  SERIAL PRIMARY KEY,
    driver_id   TEXT UNIQUE NOT NULL,
    joined_on   DATE NOT NULL
);
```

**SCD Type 2 on `dim_vehicle`** so that if a vehicle is re-assigned to a different home zone mid-history, past profitability rows still join to the attributes that were true *at the time*. This is a real warehousing concept the module's OLAP section implies, and it costs about 15 lines to implement.

### 2.2 Facts

```sql
CREATE TABLE mart.fact_vehicle_daily_pnl (
    vehicle_key            INT  NOT NULL REFERENCES mart.dim_vehicle(vehicle_key),
    date_key               INT  NOT NULL REFERENCES mart.dim_date(date_key),
    vehicle_id             TEXT NOT NULL,          -- denormalised for upsert convenience
    sim_date               DATE NOT NULL,

    -- from telemetry (exact, recomputed from the master dataset)
    trips                  INT           NOT NULL DEFAULT 0,
    revenue                NUMERIC(12,2) NOT NULL DEFAULT 0,
    telemetry_distance_km  NUMERIC(10,2) NOT NULL DEFAULT 0,
    on_trip_hours          NUMERIC(6,2)  NOT NULL DEFAULT 0,
    idle_hours             NUMERIC(6,2)  NOT NULL DEFAULT 0,
    utilization_pct        NUMERIC(5,2),

    -- from the expense file
    fuel_cost              NUMERIC(12,2),
    maintenance_cost       NUMERIC(12,2),
    partner_distance_km    NUMERIC(10,2),
    service_flag           BOOLEAN,
    partner_id             TEXT,

    -- reconciliation
    expense_status         TEXT NOT NULL CHECK (expense_status IN
                             ('MATCHED','MISSING_EXPENSE','MISSING_TELEMETRY')),
    distance_variance_pct  NUMERIC(6,2),

    -- derived
    net_profit             NUMERIC(12,2),
    profit_per_km          NUMERIC(10,4),
    margin_pct             NUMERIC(6,2),
    rolling_7d_avg_profit  NUMERIC(12,2),
    profit_trend_slope     NUMERIC(12,4),
    classification         TEXT NOT NULL,          -- HEALTHY|WATCH|UNPROFITABLE|CRITICAL|INSUFFICIENT_DATA

    -- provenance
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    restated_at            TIMESTAMPTZ,
    restatement_count      INT NOT NULL DEFAULT 0,
    job_run_id             TEXT NOT NULL,

    PRIMARY KEY (vehicle_id, sim_date)     -- ← the upsert conflict target
);

CREATE INDEX ON mart.fact_vehicle_daily_pnl (sim_date);
CREATE INDEX ON mart.fact_vehicle_daily_pnl (classification, sim_date);

CREATE TABLE mart.fact_zone_hourly (
    zone_key        INT NOT NULL REFERENCES mart.dim_zone(zone_key),
    date_key        INT NOT NULL REFERENCES mart.dim_date(date_key),
    sim_hour        SMALLINT NOT NULL,
    trips           INT NOT NULL,
    earnings        NUMERIC(12,2) NOT NULL,
    active_vehicles INT NOT NULL,
    idle_ratio      NUMERIC(5,4) NOT NULL,
    avg_speed_kmh   NUMERIC(6,2),
    PRIMARY KEY (zone_key, date_key, sim_hour)
);
```

`fact_zone_hourly` is the **exact** version of what the speed layer approximates. Having both is what makes the reconciliation chart in §5.4 possible.

### 2.3 The boundary table — small, and load-bearing

```sql
CREATE TABLE mart.batch_high_water_mark (
    id                  BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),  -- single-row table
    batch_complete_thru DATE        NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_job_run_id     TEXT        NOT NULL
);
```

The `CHECK (id)` single-row trick guarantees there can never be two conflicting watermarks. Advanced **only** by the final task of the DAG, after the fact upsert commits.

### 2.4 Supporting tables

```sql
CREATE TABLE mart.dq_run_log (
    run_id TEXT, sim_date DATE, check_name TEXT, status TEXT,
    observed NUMERIC, threshold NUMERIC, checked_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE mart.reconciliation_delta (   -- see §5.4
    sim_date DATE, zone_id TEXT, metric TEXT,
    speed_value NUMERIC, batch_value NUMERIC, delta_pct NUMERIC,
    recorded_at TIMESTAMPTZ DEFAULT now()
);
```

---

## 3. Redis — the speed view

| Key | Type | Fields | TTL |
|---|---|---|---|
| `fleet:zone:{zone_id}` | HASH | `active_vehicles, trips, idle_ratio, avg_speed_kmh, window_start, window_end, updated_at_sim` (activity writer) + `earnings, completed_trips, earnings_window_end` (earnings writer) | 2 sim-hours |
| `fleet:snapshot` | HASH | Fleet-wide roll-up: `total_active, total_trips, fleet_idle_ratio, zones_reporting, updated_at_sim` | 2 sim-hours |
| `fleet:vehicle:{vehicle_id}:state` | HASH | `status, status_since_sim, lat, lon, zone_id, current_trip_id` | 30 sim-min |
| `fleet:alerts:idle` | ZSET | member = alert JSON, score = sim epoch seconds | trimmed to 200 |
| `fleet:ts:zone:{zone_id}:earnings` | LIST | Capped at 96 points, for dashboard sparklines | 6 sim-hours |
| `sim:epoch_wall` | STRING | The shared simulated-clock anchor (`03 §3.1`) | none |

> **Corrected on day 7 against the running code.** This table originally predicted
> `avg_speed` and a `fleet:snapshot` of `total_active, total_idle, idle_ratio,
> trips_per_hour, earnings_current_hour`. The sinks in
> `speed_layer/sinks/redis_sink.py` write what is shown above. Where a plan written
> in advance and shipped code disagree about a field name, the code is the fact; the
> serving layer reads these names directly rather than translating, because a
> translation layer would let the two drift further apart silently.
>
> The two writers on the zone hash are **deliberately disjoint**. An earlier version
> of the earnings writer also carried `earnings=0.0` and clobbered the activity
> query's fields on every window. A test now enforces the disjoint field sets.

Design notes worth defending:
- **All TTLs are expressed in simulated seconds converted to real seconds** via `simclock.sim_to_real()`. A 2-simulated-hour TTL is 25 real seconds. Hard-coding real seconds here would be a bug that only appears when the speed-up changes.
- **No `INCR` anywhere on aggregate values** — every write is a full overwrite of a recomputed value, which is what makes micro-batch replay safe (`04 §5`).
- Redis runs with **persistence disabled** (`--appendonly no --save ""`). Deliberate: the data is disposable, and disabling persistence removes fork-based snapshot pauses. Stated in the report as a conscious choice, not an oversight.

---

## 4. MinIO layout

Covered in `06 §1`. Additional serving-relevant points:
- `reports/` is browsable in the MinIO console (`:9001`) during the demo — the daily PDF is visibly *there*, not just claimed.
- Bucket versioning is **enabled on `landing/`** so an expense-file restatement does not destroy the original submission. That is the audit-trail argument in practice, and it costs one config line.

---

## 5. The serving layer — FastAPI, and the reconciliation

### 5.1 Why this is the hard part

The module's stated Lambda weakness is *"reconciling data between systems"*. Most student projects hand-wave this: they query one store or the other and never define what happens at the seam. **Our answer is an explicit, tested, documented boundary.**

The core idea:

```
      simulated time ──────────────────────────────────────────────▶

      ├──────── batch view (PostgreSQL) ────────┤├── speed view (Redis) ──┤
      │           exact, complete               ││   approximate, live    │
      └─────────────────────────────────────────┘└────────────────────────┘
                                                 ▲
                                    batch_high_water_mark
                              (last sim_date the batch layer
                               has fully processed and committed)
```

Half-open intervals: batch serves `[from, hwm]`, speed serves `(hwm, now]`. The boundary date belongs to **exactly one** side. No overlap → no double counting. No gap → no missing interval.

### 5.2 `serving/merge.py`

```python
@dataclass(frozen=True)
class MergedResult:
    rows: list[MetricRow]          # each row carries source="batch" | "speed"
    as_of_sim: datetime
    batch_complete_thru: date | None
    consistency: str               # human-readable contract statement
    degraded: bool                 # True if one store was unavailable

async def merged_utilization(from_date, to_date) -> MergedResult:
    hwm = await get_high_water_mark()          # may be None: DAG has never run

    if hwm is None:
        batch_rows, speed_from = [], from_date
        consistency = "no batch data available; all values approximate"
    else:
        batch_end = min(hwm, to_date)
        batch_rows = await pg.fetch_zone_daily(from_date, batch_end) if from_date <= batch_end else []
        speed_from = hwm + timedelta(days=1)
        consistency = f"batch-complete-through {hwm.isoformat()}"

    speed_rows = await redis.fetch_zone_current() if speed_from <= to_date else []

    return MergedResult(
        rows=[*_tag(batch_rows, "batch"), *_tag(speed_rows, "speed")],
        as_of_sim=simclock.sim_now(),
        batch_complete_thru=hwm,
        consistency=consistency,
        degraded=...,
    )
```

**Cases the implementation must handle, each with a test:**

| Case | Behaviour |
|---|---|
| `hwm is None` (DAG never ran) | Serve everything from the speed view; `consistency` says so; `degraded=False` (this is a valid cold-start state, not a failure) |
| `to_date <= hwm` | Pure batch. Redis is not queried at all. |
| `from_date > hwm` | Pure speed. Postgres is not queried. |
| Range spans the boundary | Both, split at `hwm`, no overlap |
| Redis unavailable | Serve batch only, `degraded=True`, `X-Data-Degraded: true` header, warning log, metric incremented — **do not 500.** A partial answer with an honest label beats an error page. |
| Postgres unavailable | Serve speed only, `degraded=True`, same treatment |
| Both unavailable | 503 with a clear body |

**The degradation behaviour is itself an architecture argument:** because the two layers are independent, the system stays partially useful when one fails. A single-path system has no such property. Say this in the report.

### 5.3 Endpoints

| Method | Path | Source | Notes |
|---|---|---|---|
| GET | `/api/v1/fleet/live` | **Speed** | The PDF's "real-time fleet utilization metrics": active vehicles, idle ratio, trips/hour, earnings by zone. Response documents itself as approximate. |
| GET | `/api/v1/fleet/zones` | Speed | Per-zone current stats |
| GET | `/api/v1/fleet/zones/{zone_id}/history?hours=24` | **Merged** | Crosses the boundary |
| GET | `/api/v1/fleet/utilization?from=&to=` | **Merged** | The flagship endpoint |
| GET | `/api/v1/vehicles/{id}` | Speed | Current state and position |
| GET | `/api/v1/vehicles/{id}/profitability?days=7` | **Batch** | Exact. Includes classification and trend. |
| GET | `/api/v1/vehicles/unprofitable?limit=10&classification=WATCH` | **Batch** | The "becoming unprofitable" answer |
| GET | `/api/v1/reports/daily/{sim_date}` | Batch + MinIO | Metadata + presigned URL to the PDF |
| GET | `/api/v1/alerts/idle?limit=50` | Speed | Business alerts |
| GET | `/api/v1/alerts/business` | Speed | All business alerts |
| GET | `/api/v1/pipeline/status` | All | **Observability endpoint**: high-water-mark, last batch run, streaming query liveness, consumer lag, sim clock. One place to see whether the pipeline is healthy. |
| GET | `/metrics` | — | Prometheus |
| GET | `/health/live` | — | Process alive |
| GET | `/health/ready` | — | Dependencies reachable |
| GET | `/health/deep` | — | Actually queries Kafka, Redis, Postgres, MinIO; returns per-dependency status and latency |

### 5.4 Quantifying divergence — an observability bonus

When a simulated date transitions from speed-served to batch-served, we can compare what the speed layer said with what the batch layer computed. A small Airflow task does this and writes to `mart.reconciliation_delta`.

A Grafana panel charts `delta_pct` over time per metric. This turns the module's abstract "reconciling data between systems" problem into **a number we monitor**, which is a genuinely strong report point: we do not merely acknowledge the weakness, we instrument it.

Expected divergence at demo scale: 1–3% on `active_vehicles` (HyperLogLog error), up to 8% on `earnings` (fare recognition lag, `05 §3.4`). **Publishing the expected range and then showing the observed range is exactly the honesty the rubric rewards.**

### 5.5 Response shape

```json
{
  "as_of_sim": "2026-03-03T14:22:00Z",
  "batch_complete_thru": "2026-03-02",
  "consistency": "batch-complete-through 2026-03-02",
  "degraded": false,
  "rows": [
    {"sim_date": "2026-03-01", "zone_id": "Z01", "trips": 412, "earnings": 3841.20,
     "utilization_pct": 68.4, "source": "batch",  "exact": true},
    {"sim_date": "2026-03-03", "zone_id": "Z01", "trips": 118, "earnings": 1102.75,
     "utilization_pct": 71.2, "source": "speed", "exact": false,
     "approximation_note": "distinct counts are HyperLogLog (~2% error); earnings recognised at trip completion"}
  ]
}
```

Every row is self-describing. A consumer never has to guess whether a number is exact. **Screenshot `/docs` showing this model for the report.**

---

## 6. Grafana dashboards

Provisioned as code in `observability/grafana/` (datasources + dashboard JSON), so a fresh clone gets working dashboards.

| Dashboard | Datasource | Key panels |
|---|---|---|
| **Fleet Operations** (the business dashboard) | Redis + Postgres | Active vehicles gauge; idle ratio; trips/hour; earnings by zone (bar); zone map (geomap from lat/lon); earnings by time-of-day (the diurnal curve); live idle-alert table |
| **Batch Reconciliation** | Postgres | Daily net profit fleet-wide; top 10 unprofitable; classification distribution; **speed-vs-batch divergence** (§5.4); high-water-mark age; expense join match rate |
| **Pipeline Health** | Prometheus | Covered in `09 §7` |
| **Traces & Logs** | Jaeger + Loki | Covered in `09 §7` |

Grafana reads Redis via the **Redis datasource plugin**; if that proves awkward, the fallback is the **Infinity (JSON) datasource** pointed at our own FastAPI endpoints — which has the side benefit of making the dashboard exercise the serving layer rather than bypassing it. **Recommend the Infinity/FastAPI route as the default**: it is one less plugin, and it means the dashboard and the API cannot disagree.

---

## 7. Testing

| Test | Asserts |
|---|---|
| `test_merge_boundary.py` | **Table-driven over every case in §5.2.** The most important test in the repo. |
| `test_merge_degraded.py` | Redis down → batch-only + `degraded=True`, not a 500. Same for Postgres. |
| `test_hwm.py` | Single-row constraint holds; watermark never moves backwards |
| `test_upsert.py` | Double-run → one row, `restatement_count=1` |
| `test_redis_keys.py` | TTLs are computed from `simclock`, not hard-coded |
| `test_api_contract.py` | Every response includes `source`, `as_of_sim`, `consistency`; OpenAPI schema snapshot test |
| `test_scd2.py` | A vehicle attribute change closes the old row and opens a new one; historical facts still join correctly |
| `test_health_deep.py` | Reports per-dependency status; degrades rather than crashing when one is down |

---

## 8. What the report must show (§6.4 / §6.5)

- The star-schema diagram.
- The **boundary diagram from §5.1** — this should be one of the report's headline figures.
- The endpoint table with a `source` column, making the Lambda split visible at the API level.
- A `/docs` screenshot showing the self-describing response model.
- The divergence chart from §5.4 with the observed numbers.
- The degradation behaviour, framed as a benefit of layer independence.
