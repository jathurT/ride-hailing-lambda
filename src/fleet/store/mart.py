"""Writing and reading the batch-layer data mart.

Two things in here carry architectural weight:

`upsert_pnl` is idempotent by construction. Re-running the batch job for a simulated
date must produce the identical result, because that is exactly what a restatement
does - and a restatement is the scenario the whole Lambda-over-Kappa argument rests
on (plan/01 section 2.7). The conflict target is the natural key `(vehicle_id,
sim_date)`, and the DELETE-then-INSERT alternative is deliberately avoided: a crash
between the two would leave the day missing entirely.

`advance_high_water_mark` is what the serving layer's correctness depends on. It
must be called ONLY after the fact upsert has committed, because the watermark is
the promise that everything up to that date is complete (ADR-005).
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date
from typing import Any

UPSERT_COLUMNS = (
    "vehicle_id",
    "sim_date",
    "trips",
    "revenue",
    "telemetry_distance_km",
    "paid_distance_km",
    "deadhead_distance_km",
    "on_trip_hours",
    "idle_hours",
    "utilization_pct",
    "fuel_cost",
    "maintenance_cost",
    "partner_distance_km",
    "service_flag",
    "partner_id",
    "expense_status",
    "distance_variance_pct",
    "net_profit",
    "profit_per_km",
    "margin_pct",
    "rolling_7d_avg_profit",
    "profit_trend_slope",
    "classification",
    "job_run_id",
)

# Everything except the natural key and the provenance counters is overwritten.
_UPDATE_SET = ",\n            ".join(
    f"{c} = EXCLUDED.{c}" for c in UPSERT_COLUMNS if c not in {"vehicle_id", "sim_date"}
)

UPSERT_PNL_SQL = f"""
INSERT INTO mart.fact_vehicle_daily_pnl ({", ".join(UPSERT_COLUMNS)})
VALUES ({", ".join(["%s"] * len(UPSERT_COLUMNS))})
ON CONFLICT (vehicle_id, sim_date) DO UPDATE SET
            {_UPDATE_SET},
            computed_at = now(),
            -- A row that is written twice is a restatement. Recording that fact is
            -- what lets an auditor see that a figure changed, and when.
            restated_at = CASE WHEN mart.fact_vehicle_daily_pnl.job_run_id
                                    IS DISTINCT FROM EXCLUDED.job_run_id
                               THEN now() ELSE mart.fact_vehicle_daily_pnl.restated_at END,
            restatement_count = mart.fact_vehicle_daily_pnl.restatement_count
                              + CASE WHEN mart.fact_vehicle_daily_pnl.job_run_id
                                          IS DISTINCT FROM EXCLUDED.job_run_id
                                     THEN 1 ELSE 0 END
"""

ZONE_HOURLY_COLUMNS = (
    "zone_id",
    "sim_date",
    "sim_hour",
    "trips",
    "earnings",
    "active_vehicles",
    "idle_ratio",
    "avg_speed_kmh",
    "job_run_id",
)

_ZONE_HOURLY_SET = ",\n            ".join(
    f"{c} = EXCLUDED.{c}"
    for c in ZONE_HOURLY_COLUMNS
    if c not in {"zone_id", "sim_date", "sim_hour"}
)

# Idempotent for the same reason `upsert_pnl` is: the batch layer must be safe to
# re-run for a simulated date. There is no restatement bookkeeping here because a
# zone-hour carries no provenance the report cites - the per-vehicle P&L is the
# audited figure, and that is where `restated_at` lives.
UPSERT_ZONE_HOURLY_SQL = f"""
INSERT INTO mart.fact_zone_hourly ({", ".join(ZONE_HOURLY_COLUMNS)})
VALUES ({", ".join(["%s"] * len(ZONE_HOURLY_COLUMNS))})
ON CONFLICT (zone_id, sim_date, sim_hour) DO UPDATE SET
            {_ZONE_HOURLY_SET}
"""

ADVANCE_HWM_SQL = """
INSERT INTO mart.batch_high_water_mark (id, batch_complete_thru, last_job_run_id)
VALUES (TRUE, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    -- Never moves backwards. A restatement changes a past day's *values*, not the
    -- fact that it was already complete.
    batch_complete_thru = GREATEST(mart.batch_high_water_mark.batch_complete_thru,
                                   EXCLUDED.batch_complete_thru),
    updated_at = now(),
    last_job_run_id = EXCLUDED.last_job_run_id
"""

READ_HWM_SQL = "SELECT batch_complete_thru FROM mart.batch_high_water_mark WHERE id"


@dataclass(frozen=True, slots=True)
class PnlRow:
    vehicle_id: str
    sim_date: date
    job_run_id: str
    trips: int = 0
    revenue: float = 0.0
    telemetry_distance_km: float = 0.0
    paid_distance_km: float = 0.0
    deadhead_distance_km: float = 0.0
    on_trip_hours: float = 0.0
    idle_hours: float = 0.0
    utilization_pct: float | None = None
    fuel_cost: float | None = None
    maintenance_cost: float | None = None
    partner_distance_km: float | None = None
    service_flag: bool | None = None
    partner_id: str | None = None
    expense_status: str = "MATCHED"
    distance_variance_pct: float | None = None
    net_profit: float | None = None
    profit_per_km: float | None = None
    margin_pct: float | None = None
    rolling_7d_avg_profit: float | None = None
    profit_trend_slope: float | None = None
    classification: str = "INSUFFICIENT_DATA"

    def as_tuple(self) -> tuple[Any, ...]:
        by_name = {f.name: getattr(self, f.name) for f in fields(self)}
        return tuple(by_name[c] for c in UPSERT_COLUMNS)


def upsert_pnl(cursor: Any, rows: list[PnlRow]) -> int:
    cursor.executemany(UPSERT_PNL_SQL, [r.as_tuple() for r in rows])
    return len(rows)


@dataclass(frozen=True, slots=True)
class ZoneHourlyRow:
    """The batch view's exact zone facts - the row `fact_zone_hourly` stores.

    `sim_hour` is the hour of the SIMULATED day, 0-23, not a wall-clock hour. The
    DDL's CHECK constraint enforces the range, so an off-by-one in the hour
    extraction fails loudly at insert rather than quietly landing in hour 24.
    """

    zone_id: str
    sim_date: date
    sim_hour: int
    job_run_id: str
    trips: int = 0
    earnings: float = 0.0
    active_vehicles: int = 0
    idle_ratio: float | None = None
    avg_speed_kmh: float | None = None

    def as_tuple(self) -> tuple[Any, ...]:
        by_name = {f.name: getattr(self, f.name) for f in fields(self)}
        return tuple(by_name[c] for c in ZONE_HOURLY_COLUMNS)


def upsert_zone_hourly(cursor: Any, rows: list[ZoneHourlyRow]) -> int:
    cursor.executemany(UPSERT_ZONE_HOURLY_SQL, [r.as_tuple() for r in rows])
    return len(rows)


def advance_high_water_mark(cursor: Any, thru: date, job_run_id: str) -> None:
    cursor.execute(ADVANCE_HWM_SQL, (thru, job_run_id))


def read_high_water_mark(cursor: Any) -> date | None:
    """None means the batch layer has never run - a valid cold-start state that the
    serving layer must handle, not an error (plan/07 section 5.2)."""
    cursor.execute(READ_HWM_SQL)
    row = cursor.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Read side.
#
# Everything above is written by the batch layer; everything below is read by the
# serving layer (plan/07 section 5). They are deliberately in one module: the SQL
# that writes a table and the SQL that reads it should not be able to drift apart
# unnoticed, and a column rename now breaks both in the same file.
#
# These stay synchronous and cursor-based, matching `upsert_pnl` above. FastAPI
# runs them in its threadpool. An async pool would be the right answer under real
# load; at 12 zones and 150 vehicles it would be ceremony, and mixing two database
# idioms in one codebase costs more than it buys.
# ---------------------------------------------------------------------------


def _rows(cursor: Any) -> list[dict[str, Any]]:
    """Cursor rows as dicts, using the cursor's own column names.

    Reading names off `cursor.description` rather than hand-writing them at each
    call site means a SELECT and its unpacking cannot disagree - the class of bug
    where a column is added in the middle and every field shifts by one.
    """
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


FETCH_ZONE_DAILY_SQL = """
SELECT zone_id,
       sim_date,
       SUM(trips)                        AS trips,
       SUM(earnings)                     AS earnings,
       MAX(active_vehicles)              AS active_vehicles,
       AVG(idle_ratio)                   AS idle_ratio,
       AVG(avg_speed_kmh)                AS avg_speed_kmh,
       COUNT(*)                          AS hours_reported
  FROM mart.fact_zone_hourly
 WHERE sim_date BETWEEN %s AND %s
 GROUP BY zone_id, sim_date
 ORDER BY sim_date, zone_id
"""


def fetch_zone_daily(cursor: Any, from_date: date, to_date: date) -> list[dict[str, Any]]:
    """Per-zone daily rollup - the BATCH half of the merged utilization endpoint.

    Two of these aggregates are not the obvious one, and both would be silently
    wrong the obvious way:

    `active_vehicles` is MAX across the day's hours, not SUM. The hourly figure is
    already a distinct count, so summing it counts a vehicle once per hour it was
    seen - a vehicle working an eight-hour shift would arrive as eight vehicles.
    MAX is the day's peak concurrent count, which is a real quantity and the
    closest analogue to what the speed layer's windowed distinct count reports, so
    the two sides of the merge stay comparable.

    `idle_ratio` is an unweighted mean over reported hours. Weighting by ping count
    would be more correct, but `fact_zone_hourly` does not store one; the error is
    small because the ping interval is fixed, and stating it here is better than an
    unexplained number in the report.

    `hours_reported` is returned so a caller can tell a genuinely quiet zone from a
    partially-processed day. A zone with 3 hours reported is not a zone that was
    idle for 21 hours.
    """
    cursor.execute(FETCH_ZONE_DAILY_SQL, (from_date, to_date))
    return _rows(cursor)


FETCH_VEHICLE_PNL_SQL = """
SELECT vehicle_id, sim_date, trips, revenue, telemetry_distance_km, paid_distance_km,
       deadhead_distance_km, on_trip_hours, idle_hours, utilization_pct,
       fuel_cost, maintenance_cost, expense_status, distance_variance_pct,
       net_profit, profit_per_km, margin_pct, rolling_7d_avg_profit,
       profit_trend_slope, classification, computed_at, restated_at, restatement_count
  FROM mart.fact_vehicle_daily_pnl
 WHERE vehicle_id = %s
 ORDER BY sim_date DESC
 LIMIT %s
"""


def fetch_vehicle_profitability(cursor: Any, vehicle_id: str, days: int) -> list[dict[str, Any]]:
    """One vehicle's trailing daily P&L, newest first.

    `restated_at` and `restatement_count` come back with every row on purpose. A
    figure that has changed since it was first published is exactly what an auditor
    asks about, and the serving layer can only surface that provenance if the read
    carries it (plan/07 section 5.5).
    """
    cursor.execute(FETCH_VEHICLE_PNL_SQL, (vehicle_id, days))
    return _rows(cursor)


FETCH_UNPROFITABLE_SQL = """
SELECT p.vehicle_id, p.sim_date, p.net_profit, p.profit_per_km, p.margin_pct,
       p.rolling_7d_avg_profit, p.profit_trend_slope, p.classification,
       p.revenue, p.fuel_cost, p.maintenance_cost, p.trips, p.utilization_pct
  FROM mart.fact_vehicle_daily_pnl p
  JOIN (SELECT MAX(sim_date) AS d FROM mart.fact_vehicle_daily_pnl) latest
    ON p.sim_date = latest.d
 -- The ::text casts are required, not decorative. psycopg sends an untyped NULL
 -- for the unfiltered case, and Postgres cannot infer a type for `$1 IS NULL`
 -- on its own: it raises "could not determine data type of parameter $1".
 -- Worse, `readers.py` normalises every exception to StoreUnavailableError, so
 -- this SQL bug surfaced to clients as a 503 "batch view unavailable: postgres"
 -- while Postgres was perfectly healthy.
 WHERE (%s::text IS NULL OR p.classification = %s::text)
 ORDER BY p.rolling_7d_avg_profit ASC NULLS LAST, p.net_profit ASC
 LIMIT %s
"""


def fetch_unprofitable(
    cursor: Any, limit: int, classification: str | None = None
) -> list[dict[str, Any]]:
    """The worst vehicles on the most recent complete simulated day.

    Ordered by the 7-day rolling average rather than by today's net profit. The
    business question is which vehicles are BECOMING unprofitable; a single bad day
    is noise, and sorting on it would put a vehicle that had one long deadhead
    above a vehicle that has lost money every day for a week.

    The `%s IS NULL OR` pattern lets one statement serve both the filtered and
    unfiltered case, so there is one query plan and one place for the ordering rule
    to live. The parameter is passed twice because psycopg binds positionally.
    """
    cursor.execute(FETCH_UNPROFITABLE_SQL, (classification, classification, limit))
    return _rows(cursor)


FETCH_PIPELINE_STATUS_SQL = """
SELECT h.batch_complete_thru,
       h.last_job_run_id,
       h.updated_at                       AS watermark_updated_at,
       (SELECT MAX(sim_date) FROM mart.fact_vehicle_daily_pnl)      AS latest_fact_sim_date,
       (SELECT COUNT(*) FROM mart.fact_vehicle_daily_pnl)           AS pnl_rows,
       (SELECT COUNT(*) FROM mart.fact_zone_hourly)                 AS zone_hourly_rows,
       (SELECT COUNT(*) FROM mart.fact_vehicle_daily_pnl
         WHERE restatement_count > 0)                               AS restated_rows
  FROM mart.batch_high_water_mark h
 WHERE h.id
"""


def fetch_pipeline_status(cursor: Any) -> dict[str, Any]:
    """Batch-layer state for `/api/v1/pipeline/status`.

    Returns an empty dict when the batch layer has never run. That is a cold start,
    not an error - the same distinction `read_high_water_mark` makes, and the
    serving layer must render it as "no batch data yet" rather than as a failure.
    """
    cursor.execute(FETCH_PIPELINE_STATUS_SQL)
    rows = _rows(cursor)
    return rows[0] if rows else {}


# ---------------------------------------------------------------------------
# Speed/batch reconciliation (plan/07 section 5.4).
#
# Lambda's advertised weakness is "reconciling data between two systems". This is
# where that stops being a discussion point and becomes a number in a table.
#
# The orchestrator snapshots the speed view once per simulated day, and later - once
# the batch layer has computed the same day exactly - the two are compared. The
# comparison cannot be done retrospectively from Redis, whose TTL is two simulated
# hours, which is why the snapshot table exists at all.
#
# An expected divergence is published in plan/07 section 5.4: 1-3% on active_vehicles
# (HyperLogLog is ~2%) and up to 8% on earnings (the speed view recognises revenue at
# trip completion inside a sliding window, the batch layer over the whole day). A
# delta of EXACTLY zero would not be a triumph - it would be evidence that one side
# is reading the other rather than computing it independently.
# ---------------------------------------------------------------------------

SPEED_SNAPSHOT_COLUMNS = (
    "sim_date",
    "zone_id",
    "sim_hour",
    "captured_at_sim",
    "window_minutes",
    "active_vehicles",
    "trips",
    "earnings",
    "idle_ratio",
    "avg_speed_kmh",
)

_SNAPSHOT_SET = ",\n            ".join(
    f"{c} = EXCLUDED.{c}"
    for c in SPEED_SNAPSHOT_COLUMNS
    if c not in {"sim_date", "zone_id", "sim_hour"}
)

UPSERT_SPEED_SNAPSHOT_SQL = f"""
INSERT INTO mart.speed_view_snapshot ({", ".join(SPEED_SNAPSHOT_COLUMNS)})
VALUES ({", ".join(["%s"] * len(SPEED_SNAPSHOT_COLUMNS))})
ON CONFLICT (sim_date, zone_id, sim_hour) DO UPDATE SET
            {_SNAPSHOT_SET}
"""


def upsert_speed_snapshot(cursor: Any, rows: list[tuple[Any, ...]]) -> int:
    """Rows must be in SPEED_SNAPSHOT_COLUMNS order.

    Last write per simulated day wins. The alternative - keeping every capture - would
    need a decision about which one to compare against, and "the speed view's final
    word on that day" is both defensible and the one a live dashboard would have been
    showing at the end of it.
    """
    cursor.executemany(UPSERT_SPEED_SNAPSHOT_SQL, rows)
    return len(rows)


# WHY THIS NORMALISES INSTEAD OF SUBTRACTING
#
# The first version of this query compared the speed view's numbers against the
# batch view's directly and reported ~99% divergence on every metric. That was not
# approximation error - it was a unit mismatch. The speed view aggregates a
# 15-simulated-minute SLIDING window; the batch view aggregates a 1-hour TUMBLING
# one, rolled up to a day. Comparing them raw compares a quarter of an hour against
# twenty-four of them, and the answer is arithmetic, not accuracy.
#
# So FLOW metrics (trips, earnings) are converted to a per-minute rate on both sides
# before the comparison: speed / window_minutes against batch_hour / 60. Those are
# the same quantity in the same units, and the residual difference is the real thing
# plan/07 section 5.4 is interested in.
#
# `active_vehicles` is a LEVEL, not a flow, so a rate makes no sense for it - but the
# window still matters, because a 15-minute window sees fewer distinct vehicles than
# an hour does. It is compared raw and labelled `active_vehicles_window_biased` so
# nobody reads it as a pure HyperLogLog error figure. The bias is one-directional and
# expected: speed should read LOWER.
#
# NULLIF guards every division: a zone with no batch activity in that hour would
# otherwise raise a division-by-zero and fail the DAG over a quiet 4am hour, which is
# data, not a fault. The delta comes back NULL and reads as "not comparable".
RECONCILE_SQL = """
INSERT INTO mart.reconciliation_delta (sim_date, zone_id, metric, speed_value, batch_value, delta_pct)
SELECT s.sim_date,
       s.zone_id,
       m.metric,
       ROUND(m.speed_value, 4),
       ROUND(m.batch_value, 4),
       ROUND(100.0 * (m.speed_value - m.batch_value) / NULLIF(ABS(m.batch_value), 0), 3)
  FROM mart.speed_view_snapshot s
  JOIN mart.fact_zone_hourly b
    ON b.sim_date = s.sim_date
   AND b.zone_id  = s.zone_id
   AND b.sim_hour = s.sim_hour
 CROSS JOIN LATERAL (VALUES
        ('trips_per_min',
         s.trips::numeric    / NULLIF(s.window_minutes, 0),
         b.trips::numeric    / 60.0),
        ('earnings_per_min',
         s.earnings          / NULLIF(s.window_minutes, 0),
         b.earnings          / 60.0),
        ('idle_ratio',
         s.idle_ratio,
         b.idle_ratio),
        ('active_vehicles_window_biased',
         s.active_vehicles::numeric,
         b.active_vehicles::numeric)
 ) AS m(metric, speed_value, batch_value)
 WHERE s.sim_date = %s
"""

# The same comparison rolled up across all 12 zones, written with zone_id = 'ALL'.
#
# This is the one to quote. The per-zone rows above are honest but statistically
# noisy: a snapshot captures ONE 15-simulated-minute sliding window out of a
# 1,440-minute day - roughly a 1% sample - and at zone granularity that window
# contains one or two trips. A zone that happened to see zero trips in the sampled
# window reports a 100% divergence against an hour that averaged three, which is
# sampling variance, not a pipeline defect.
#
# Summing across zones multiplies the sample by twelve and the variance falls
# accordingly. The limitation is stated rather than tuned away, because a
# reconciliation figure that was massaged until it looked good would be worth
# nothing - and the honest version is still evidence the two paths compute
# independently, which is what plan/07 section 5.4 is actually asking.
RECONCILE_FLEET_SQL = """
INSERT INTO mart.reconciliation_delta (sim_date, zone_id, metric, speed_value, batch_value, delta_pct)
SELECT agg.sim_date,
       'ALL',
       m.metric,
       ROUND(m.speed_value, 4),
       ROUND(m.batch_value, 4),
       ROUND(100.0 * (m.speed_value - m.batch_value) / NULLIF(ABS(m.batch_value), 0), 3)
  FROM (
        SELECT s.sim_date,
               SUM(s.trips)::numeric    / NULLIF(MAX(s.window_minutes), 0) AS speed_trips_pm,
               SUM(s.earnings)          / NULLIF(MAX(s.window_minutes), 0) AS speed_earn_pm,
               AVG(s.idle_ratio)                                           AS speed_idle,
               SUM(b.trips)::numeric    / 60.0                             AS batch_trips_pm,
               SUM(b.earnings)          / 60.0                             AS batch_earn_pm,
               AVG(b.idle_ratio)                                           AS batch_idle
          FROM mart.speed_view_snapshot s
          JOIN mart.fact_zone_hourly b
            ON b.sim_date = s.sim_date AND b.zone_id = s.zone_id AND b.sim_hour = s.sim_hour
         WHERE s.sim_date = %s
         GROUP BY s.sim_date
       ) agg
 CROSS JOIN LATERAL (VALUES
        ('fleet_trips_per_min',    agg.speed_trips_pm, agg.batch_trips_pm),
        ('fleet_earnings_per_min', agg.speed_earn_pm,  agg.batch_earn_pm),
        ('fleet_idle_ratio',       agg.speed_idle,     agg.batch_idle)
 ) AS m(metric, speed_value, batch_value)
"""

DELETE_RECONCILE_SQL = "DELETE FROM mart.reconciliation_delta WHERE sim_date = %s"


def compute_reconciliation(cursor: Any, sim_date: date) -> int:
    """Measure speed-vs-batch divergence for one simulated date. Returns rows written.

    Delete-then-insert rather than upsert, because `reconciliation_delta` has a
    surrogate key and no natural one. That is safe here in a way it is NOT safe for
    the fact tables: this is a derived measurement that can be recomputed from two
    surviving sources at any time, so a crash between the delete and the insert loses
    nothing permanent. The fact tables deliberately avoid the same pattern.
    """
    cursor.execute(DELETE_RECONCILE_SQL, (sim_date,))
    cursor.execute(RECONCILE_SQL, (sim_date,))
    written = cursor.rowcount
    cursor.execute(RECONCILE_FLEET_SQL, (sim_date,))
    return written + cursor.rowcount


READ_RECONCILE_SQL = """
SELECT zone_id, metric, speed_value, batch_value, delta_pct
  FROM mart.reconciliation_delta
 WHERE sim_date = %s
 ORDER BY metric, zone_id
"""


def fetch_reconciliation(cursor: Any, sim_date: date) -> list[dict[str, Any]]:
    cursor.execute(READ_RECONCILE_SQL, (sim_date,))
    return _rows(cursor)
