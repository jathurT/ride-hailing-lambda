"""The serving layer: FastAPI over the batch view and the speed view.

Structure is deliberately flat - the interesting code is in `merge.py`, and an
endpoint here should be readable as "ask a store, shape the answer, say how good it
is". Anything longer than that belongs in the merge module or a DAO.

THE DEGRADED CONTRACT (plan/07 section 5.2)

    one store down   -> 200, X-Data-Degraded: true, partial rows, consistency says why
    both stores down -> 503, with a body that names them
    never            -> 500

A dashboard that goes blank when Redis restarts teaches nobody anything. A dashboard
that keeps showing yesterday's exact figures and says "speed view unavailable" is the
architecture doing its job, and it is the thing worth demonstrating live.

Endpoints are plain `def`, not `async def`, so FastAPI runs them in its threadpool.
The DAOs underneath are synchronous psycopg and redis-py; see the note in `merge.py`.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.common.simclock import SimClock, read_anchor
from fleet.serving import merge as merge_mod
from fleet.serving.models import (
    DeepHealth,
    FleetLive,
    IdleAlert,
    MergedEnvelope,
    MetricRowOut,
    PipelineStatus,
    StoreHealth,
    UnprofitableVehicle,
    VehiclePnlRow,
    VehicleProfitability,
    ZoneLive,
)
from fleet.serving.readers import PostgresReader, RedisReader, StoreUnavailableError

log = get_logger()

app = FastAPI(
    title="Fleet Operations API",
    version="1.0.0",
    description=(
        "Serving layer of a Lambda-architecture pipeline. Every response states "
        "which view answered it and how far the batch layer has got, so a client "
        "can tell an exact figure from an approximate one without knowing the "
        "architecture."
    ),
)

_pg = PostgresReader()
_redis = RedisReader()
_clock: SimClock | None = None


def clock() -> SimClock:
    """The simulated clock, read once from the anchor the init container wrote.

    Cached because the anchor is immutable for the life of a run - re-reading it per
    request would turn a demo into a file-system benchmark. If the anchor is missing
    the API must still start: the clock is needed to interpret dates, not to report
    that the pipeline is down, and /health must keep answering either way.
    """
    global _clock
    if _clock is None:
        _clock = read_anchor(Path(config.sim().state_path))
    return _clock


def pg() -> PostgresReader:
    return _pg


def rd() -> RedisReader:
    return _redis


def _degrade(response: Response, degraded: bool) -> None:
    if degraded:
        response.headers[config.serving().degraded_header] = "true"


# ---------------------------------------------------------------------------
# Speed view
# ---------------------------------------------------------------------------


@app.get("/api/v1/fleet/live", response_model=FleetLive, tags=["speed view"])
def fleet_live(response: Response, redis: RedisReader = Depends(rd)) -> FleetLive:
    """Fleet-wide snapshot from the speed view. Approximate by design, seconds old."""
    try:
        snap = redis.snapshot()
    except StoreUnavailableError as exc:
        raise HTTPException(503, f"speed view unavailable: {exc.store}") from exc
    _degrade(response, False)
    return FleetLive(
        as_of_sim=clock().sim_now(),
        approximation_note=merge_mod.SPEED_APPROXIMATION_NOTE,
        **snap,
    )


@app.get("/api/v1/fleet/zones", response_model=list[ZoneLive], tags=["speed view"])
def fleet_zones(redis: RedisReader = Depends(rd)) -> list[ZoneLive]:
    """Current per-zone aggregates.

    Zones with no data still appear, with `reporting=false`. A zone that has gone
    quiet and a zone that has disappeared from the pipeline look identical if you
    only return the rows Redis happens to hold.
    """
    try:
        return [ZoneLive(**z) for z in redis.zones()]
    except StoreUnavailableError as exc:
        raise HTTPException(503, f"speed view unavailable: {exc.store}") from exc


# NOTE ON ORDER: this route MUST be declared before "/api/v1/vehicles/{vehicle_id}".
# FastAPI matches in declaration order, so with the parameterised route first, a GET
# of /api/v1/vehicles/unprofitable binds vehicle_id="unprofitable", misses in Redis
# and returns 404 - a working endpoint that is simply unreachable, with nothing in
# any log to say so. `test_api_contract.py` pins the behaviour, not the ordering,
# so the test still passes if this is ever restructured correctly some other way.
@app.get(
    "/api/v1/vehicles/unprofitable",
    response_model=list[UnprofitableVehicle],
    tags=["batch view"],
)
def unprofitable(
    limit: int = Query(10, gt=0),
    classification: str | None = Query(
        None, description="WATCH | UNPROFITABLE | HEALTHY | INSUFFICIENT_DATA"
    ),
    postgres: PostgresReader = Depends(pg),
) -> list[UnprofitableVehicle]:
    """Worst vehicles on the latest complete simulated day, by 7-day rolling average.

    Sorted on the rolling average, not on today's profit: the question is which
    vehicles are BECOMING unprofitable, and one bad day is noise.
    """
    limit = min(limit, config.serving().max_page_size)
    try:
        rows = postgres.unprofitable(limit, classification)
    except StoreUnavailableError as exc:
        raise HTTPException(503, f"batch view unavailable: {exc.store}") from exc
    return [UnprofitableVehicle(**r) for r in rows]


@app.get("/api/v1/vehicles/{vehicle_id}", tags=["speed view"])
def vehicle_state(vehicle_id: str, redis: RedisReader = Depends(rd)) -> dict[str, Any]:
    try:
        state = redis.vehicle(vehicle_id)
    except StoreUnavailableError as exc:
        raise HTTPException(503, f"speed view unavailable: {exc.store}") from exc
    if state is None:
        raise HTTPException(404, f"no live state for vehicle {vehicle_id}")
    return {"vehicle_id": vehicle_id, "source": "speed", "exact": False, "state": state}


@app.get("/api/v1/alerts/idle", response_model=list[IdleAlert], tags=["speed view"])
def idle_alerts(
    limit: int = Query(50, gt=0),
    redis: RedisReader = Depends(rd),
) -> list[IdleAlert]:
    limit = min(limit, config.serving().max_page_size)
    try:
        return [IdleAlert(**a) for a in redis.idle_alerts(limit)]
    except StoreUnavailableError as exc:
        raise HTTPException(503, f"speed view unavailable: {exc.store}") from exc


# ---------------------------------------------------------------------------
# Batch view
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/vehicles/{vehicle_id}/profitability",
    response_model=VehicleProfitability,
    tags=["batch view"],
)
def vehicle_profitability(
    vehicle_id: str,
    days: int = Query(7, gt=0),
    postgres: PostgresReader = Depends(pg),
) -> VehicleProfitability:
    """One vehicle's trailing daily P&L, newest first, with restatement provenance."""
    days = min(days, config.serving().max_page_size)
    try:
        rows = postgres.vehicle_profitability(vehicle_id, days)
        hwm = postgres.high_water_mark()
    except StoreUnavailableError as exc:
        raise HTTPException(503, f"batch view unavailable: {exc.store}") from exc
    if not rows:
        raise HTTPException(404, f"no batch rows for vehicle {vehicle_id}")
    return VehicleProfitability(
        vehicle_id=vehicle_id,
        batch_complete_thru=hwm,
        row_count=len(rows),
        rows=[VehiclePnlRow(**r) for r in rows],
    )


# ---------------------------------------------------------------------------
# Merged - the flagship
# ---------------------------------------------------------------------------


@app.get("/api/v1/fleet/utilization", response_model=MergedEnvelope, tags=["merged"])
def utilization(
    response: Response,
    from_date: date = Query(..., alias="from", description="inclusive, simulated date"),
    to_date: date = Query(..., alias="to", description="inclusive, simulated date"),
    postgres: PostgresReader = Depends(pg),
    redis: RedisReader = Depends(rd),
) -> MergedEnvelope:
    """Zone utilization across the batch/speed boundary.

    ★ The endpoint the architecture argument is made in. The batch view answers for
    every date up to and including the watermark; the speed view answers for the
    simulated day in progress; the boundary date belongs to exactly one of them.

    Returns 200 with partial data and `X-Data-Degraded: true` when one store is
    unreachable. Only a total outage - neither store answering - is a 503.
    """
    if to_date < from_date:
        raise HTTPException(422, f"to ({to_date}) precedes from ({from_date})")

    result = merge_mod.merged_utilization(from_date, to_date, clock().sim_now(), postgres, redis)
    if merge_mod.all_stores_down(result):
        raise HTTPException(
            503,
            f"no serving store reachable ({', '.join(result.missing_stores)}); "
            "the pipeline may still be healthy - this is the serving layer failing",
        )

    _degrade(response, result.degraded)
    return MergedEnvelope(
        as_of_sim=result.as_of_sim,
        batch_complete_thru=result.batch_complete_thru,
        consistency=result.consistency,
        degraded=result.degraded,
        missing_stores=result.missing_stores,
        uncovered_dates=result.uncovered_dates,
        row_count=len(result.rows),
        rows=[MetricRowOut(**asdict(r)) for r in result.rows],
    )


# ---------------------------------------------------------------------------
# Status and health
# ---------------------------------------------------------------------------


@app.get("/api/v1/pipeline/status", response_model=PipelineStatus, tags=["status"])
def pipeline_status(
    postgres: PostgresReader = Depends(pg),
    redis: RedisReader = Depends(rd),
) -> PipelineStatus:
    """Where the pipeline actually is: watermark, its age, and the live speed view.

    `watermark_age_sim_days` is the number that matters. A watermark that stops
    advancing is the batch layer having quietly died, and it looks exactly like a
    healthy pipeline from every other angle.
    """
    now = clock().sim_now()
    missing: list[str] = []
    status: dict[str, Any] = {}
    speed: dict[str, Any] = {}

    try:
        status = postgres.pipeline_status()
    except StoreUnavailableError as exc:
        missing.append(exc.store)
    try:
        speed = redis.snapshot()
    except StoreUnavailableError as exc:
        missing.append(exc.store)

    hwm = status.get("batch_complete_thru")
    age = (now.date() - hwm).days if isinstance(hwm, date) else None

    return PipelineStatus(
        as_of_sim=now,
        sim_day_index=clock().sim_day_index(),
        batch_complete_thru=hwm,
        watermark_age_sim_days=age,
        last_job_run_id=status.get("last_job_run_id"),
        watermark_updated_at=status.get("watermark_updated_at"),
        latest_fact_sim_date=status.get("latest_fact_sim_date"),
        pnl_rows=status.get("pnl_rows"),
        zone_hourly_rows=status.get("zone_hourly_rows"),
        restated_rows=status.get("restated_rows"),
        speed_view=speed,
        degraded=bool(missing),
        missing_stores=sorted(set(missing)),
    )


@app.get("/health/live", tags=["health"])
def health_live() -> dict[str, str]:
    """Is the process up. Touches nothing - a liveness probe that queries a database
    restarts the API when the database blips, which is the opposite of the point."""
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
def health_ready(response: Response) -> dict[str, Any]:
    """Can this instance serve anything at all. One store is enough to be useful."""
    up = {}
    for name, reader in (("postgres", _pg), ("redis", _redis)):
        try:
            reader.ping()
            up[name] = True
        except StoreUnavailableError:
            up[name] = False
    ready = any(up.values())
    if not ready:
        response.status_code = 503
    return {"status": "ready" if ready else "not-ready", "stores": up}


@app.get("/health/deep", response_model=DeepHealth, tags=["health"])
def health_deep(response: Response) -> DeepHealth:
    """Actually queries each dependency and times it (plan/09 section 6)."""
    import time

    stores: dict[str, StoreHealth] = {}
    for name, reader in (("postgres", _pg), ("redis", _redis)):
        started = time.perf_counter()
        try:
            reader.ping()
            stores[name] = StoreHealth(
                status="up", latency_ms=round((time.perf_counter() - started) * 1000, 2)
            )
        except StoreUnavailableError as exc:
            stores[name] = StoreHealth(status="down", detail=str(exc.cause)[:200])

    ok = all(s.status == "up" for s in stores.values())
    if not ok:
        response.status_code = 503
    return DeepHealth(status="ok" if ok else "degraded", as_of_sim=clock().sim_now(), stores=stores)


def _setup() -> None:
    obs = config.observability()
    api = config.serving()
    configure(service="serving-api", stage="serve", level=obs.log_level, json_output=obs.log_json)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=api.cors_origin_list,
        allow_methods=["GET"],
        allow_headers=["*"],
        expose_headers=[api.degraded_header],
    )
    try:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator().instrument(app).expose(app, endpoint="/metrics", tags=["health"])
    except ImportError:  # pragma: no cover - metrics are optional at import time
        log.warning("instrumentator_missing", detail="/metrics not exposed")


_setup()
