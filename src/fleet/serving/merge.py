"""Reconciling the batch view and the speed view behind one answer.

★ This is the module the architecture argument rests on. The module notes give
"managing and reconciling data between two systems" as Lambda's principal weakness;
this file is that reconciliation written down, and everything else in the serving
layer is plumbing around it (plan/07 section 5).

THE RULE - half-open intervals, one owner per date

    |<------------- batch view ------------->|<---- speed view ---->|
    from                                    hwm                    now

    batch serves  [from, min(hwm, to)]        inclusive both ends
    speed serves  (hwm, now]                  hwm EXCLUDED

The boundary date `hwm` belongs to exactly one side - the batch side - so a date can
never be counted twice and never be dropped. That single choice is what makes the
merged total add up; the alternative (both sides serving the boundary) produces a
day that is silently double-counted, which is precisely the class of bug this
project has been bitten by before and which no exception would report.

THE GAP - and why it is reported rather than hidden

The speed view is Redis, and Redis holds the CURRENT window only. It is not a
per-date history. So when the batch layer falls behind by more than one simulated
day, the dates strictly between the watermark and today are in neither store: the
batch view has not reached them, and the speed view has already forgotten them. The
master dataset on MinIO still has them, but serving from Parquet at request time is
not the speed layer's job.

A merge that quietly returned fewer rows than the caller asked for would make a
pipeline falling behind look like a quiet week. So the gap is computed, named in
`consistency`, and listed in `uncovered_dates`. "We cannot answer for these dates
and here is why" is a correct answer; silence is not.

SYNCHRONOUS, deliberately

plan/07 section 5.1 sketches this as `async`. It is written synchronously because
both DAOs are synchronous, and FastAPI runs a plain `def` endpoint in its threadpool
anyway. An async merge over sync drivers would need a second database idiom in the
codebase to buy nothing at 12 zones and 150 vehicles. The deviation is recorded here
rather than left for a reader to notice.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from fleet.common.logging import get_logger
from fleet.serving.readers import PostgresReader, RedisReader, StoreUnavailableError

log = get_logger()

SPEED_APPROXIMATION_NOTE = (
    "distinct counts are HyperLogLog (~2% error); earnings recognised at trip completion"
)


@dataclass(frozen=True, slots=True)
class Interval:
    """Which store answers for which dates. Pure data, produced without any IO."""

    batch_from: date | None
    batch_to: date | None
    speed_date: date | None
    uncovered: tuple[date, ...]
    consistency: str

    @property
    def needs_batch(self) -> bool:
        return self.batch_from is not None

    @property
    def needs_speed(self) -> bool:
        return self.speed_date is not None


def split_interval(
    hwm: date | None,
    from_date: date,
    to_date: date,
    sim_today: date,
) -> Interval:
    """Decide who answers for what. No IO, no stores, no clock - all four inputs given.

    Taking `sim_today` as a parameter rather than reading the simulated clock is what
    makes every case in plan/07 section 5.2 testable as a table of dates, which is
    also why `test_merge_boundary.py` needs neither Redis nor Postgres running.

    Not querying a store at all when it owns no part of the range is a behaviour, not
    an optimisation: `to_date <= hwm` must leave Redis untouched, so a request wholly
    inside the batch view keeps answering while Redis is down.
    """
    if to_date < from_date:
        raise ValueError(f"to_date {to_date} precedes from_date {from_date}")

    # The speed view can only ever speak for the simulated day it is inside.
    speed_date: date | None = None
    if from_date <= sim_today <= to_date and (hwm is None or sim_today > hwm):
        speed_date = sim_today

    batch_from: date | None = None
    batch_to: date | None = None
    if hwm is not None:
        end = min(hwm, to_date)
        if from_date <= end:
            batch_from, batch_to = from_date, end

    # Everything in the requested range that neither side owns.
    covered: set[date] = set()
    if batch_from is not None and batch_to is not None:
        span = (batch_to - batch_from).days + 1
        covered |= {batch_from + timedelta(days=i) for i in range(span)}
    if speed_date is not None:
        covered.add(speed_date)
    requested = {from_date + timedelta(days=i) for i in range((to_date - from_date).days + 1)}
    uncovered = tuple(sorted(requested - covered))

    if hwm is None:
        consistency = "no batch data available; all values approximate"
    elif batch_from is None:
        consistency = f"speed view only; batch complete through {hwm.isoformat()}"
    elif speed_date is None:
        consistency = f"batch view only; complete through {hwm.isoformat()}"
    else:
        consistency = f"batch-complete-through {hwm.isoformat()}"

    if uncovered:
        consistency += (
            f"; {len(uncovered)} date(s) in neither view "
            f"({uncovered[0].isoformat()}..{uncovered[-1].isoformat()}) - "
            "the batch layer is behind the simulated clock"
        )

    return Interval(
        batch_from=batch_from,
        batch_to=batch_to,
        speed_date=speed_date,
        uncovered=uncovered,
        consistency=consistency,
    )


@dataclass(frozen=True, slots=True)
class MetricRow:
    """One zone-day. Self-describing: a client can tell exact from approximate."""

    sim_date: date
    zone_id: str
    source: str  # "batch" | "speed"
    exact: bool
    trips: int | None = None
    earnings: float | None = None
    active_vehicles: int | None = None
    idle_ratio: float | None = None
    utilization_pct: float | None = None
    avg_speed_kmh: float | None = None
    hours_reported: int | None = None
    approximation_note: str | None = None


@dataclass(frozen=True, slots=True)
class MergedResult:
    rows: list[MetricRow]
    as_of_sim: datetime
    batch_complete_thru: date | None
    consistency: str
    degraded: bool
    uncovered_dates: list[date] = field(default_factory=list)
    missing_stores: list[str] = field(default_factory=list)


def _utilization(idle_ratio: float | None) -> float | None:
    if idle_ratio is None:
        return None
    return round((1.0 - idle_ratio) * 100.0, 2)


def _batch_row(r: dict[str, Any]) -> MetricRow:
    idle = float(r["idle_ratio"]) if r.get("idle_ratio") is not None else None
    return MetricRow(
        sim_date=r["sim_date"],
        zone_id=r["zone_id"],
        source="batch",
        exact=True,
        trips=int(r["trips"]) if r.get("trips") is not None else None,
        earnings=float(r["earnings"]) if r.get("earnings") is not None else None,
        active_vehicles=(
            int(r["active_vehicles"]) if r.get("active_vehicles") is not None else None
        ),
        idle_ratio=idle,
        utilization_pct=_utilization(idle),
        avg_speed_kmh=(float(r["avg_speed_kmh"]) if r.get("avg_speed_kmh") is not None else None),
        hours_reported=int(r["hours_reported"]) if r.get("hours_reported") is not None else None,
    )


def _speed_row(r: dict[str, Any], sim_date: date) -> MetricRow:
    idle = r.get("idle_ratio")
    return MetricRow(
        sim_date=sim_date,
        zone_id=r["zone_id"],
        source="speed",
        exact=False,
        trips=r.get("trips"),
        earnings=r.get("earnings"),
        active_vehicles=r.get("active_vehicles"),
        idle_ratio=idle,
        utilization_pct=_utilization(idle),
        avg_speed_kmh=r.get("avg_speed_kmh"),
        approximation_note=SPEED_APPROXIMATION_NOTE,
    )


def merged_utilization(
    from_date: date,
    to_date: date,
    sim_now: datetime,
    pg: PostgresReader,
    redis: RedisReader,
) -> MergedResult:
    """Zone utilization across the batch/speed boundary - the flagship endpoint.

    Degradation is per store and never fatal on its own. If Postgres is unreachable
    the speed half still answers and the result says so; if Redis is unreachable the
    batch half still answers. Only when BOTH are unreachable is there nothing to
    return, and the caller turns that into a 503 - the one case plan/07 section 5.2
    allows to fail.

    The watermark read is itself a Postgres call, so losing Postgres means losing the
    watermark too. `hwm=None` then stands for "unknown", not "cold start", and the
    two are distinguished in `missing_stores` so a client cannot mistake a database
    outage for a pipeline that has never run.
    """
    missing: list[str] = []

    try:
        hwm = pg.high_water_mark()
        pg_up = True
    except StoreUnavailableError as exc:
        hwm, pg_up = None, False
        missing.append(exc.store)

    plan = split_interval(hwm, from_date, to_date, sim_now.date())

    rows: list[MetricRow] = []

    if pg_up and plan.needs_batch:
        try:
            assert plan.batch_from is not None and plan.batch_to is not None
            rows.extend(_batch_row(r) for r in pg.zone_daily(plan.batch_from, plan.batch_to))
        except StoreUnavailableError as exc:
            pg_up = False
            missing.append(exc.store)

    if plan.needs_speed:
        try:
            assert plan.speed_date is not None
            zones = redis.zones()
            rows.extend(_speed_row(z, plan.speed_date) for z in zones if z.get("reporting", True))
        except StoreUnavailableError as exc:
            missing.append(exc.store)

    degraded = bool(missing)
    consistency = plan.consistency
    if missing:
        consistency += f"; DEGRADED - unreachable: {', '.join(sorted(set(missing)))}"
    if not pg_up:
        consistency += "; watermark unknown while postgres is unreachable"

    log.info(
        "merge_served",
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        hwm=hwm.isoformat() if hwm else None,
        batch_rows=sum(1 for r in rows if r.source == "batch"),
        speed_rows=sum(1 for r in rows if r.source == "speed"),
        uncovered=len(plan.uncovered),
        degraded=degraded,
    )

    return MergedResult(
        rows=rows,
        as_of_sim=sim_now,
        batch_complete_thru=hwm,
        consistency=consistency,
        degraded=degraded,
        uncovered_dates=list(plan.uncovered),
        missing_stores=sorted(set(missing)),
    )


def all_stores_down(result: MergedResult) -> bool:
    """The single case that justifies a 503: nothing was reachable, so nothing is known."""
    return not result.rows and len(result.missing_stores) >= 2
