"""Read adapters over the two serving stores.

Both stores were write-only until now: `store/speed_view.py` writes Redis from the
speed layer, `store/mart.py` writes Postgres from the batch layer. The serving layer
is the first reader of either, and this module is the whole of that read side.

The one idea worth stating: **every failure here surfaces as `StoreUnavailableError`.**
psycopg raises `OperationalError`, redis-py raises `ConnectionError`, a timeout
raises something else again, and a `merge.py` that had to know all of them would
grow a store-specific except clause per call site. Normalising at the boundary is
what lets the merge express its rule once - "if this store is unreachable, serve what
the other one has and say so" - which is the degraded behaviour plan/07 section 5.2
requires and the demo is meant to show.

That means a bare `except Exception` in each adapter, and it is CLASSIFIED rather
than blanket - see `_is_unreachable`. Only connection-shaped failures become
`StoreUnavailableError` and degrade the response. Bad SQL, a missing column or a type
error propagates untouched and becomes a 500, because a programming error is a fault
in us, not a degraded dependency, and reporting it as "postgres unavailable" sends
whoever is debugging it to the wrong machine entirely.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from fleet.common import config
from fleet.common.logging import get_logger
from fleet.common.zones import ZONES
from fleet.store import mart, redis_keys

log = get_logger()


class StoreUnavailableError(RuntimeError):
    """A serving store could not answer. Carries which one, for the degraded note."""

    def __init__(self, store: str, cause: BaseException | None = None) -> None:
        super().__init__(f"{store} unavailable: {cause}")
        self.store = store
        self.cause = cause


def _is_unreachable(exc: BaseException) -> bool:
    """Is this store DOWN, or did we send it something broken?

    The distinction decides whether the serving layer degrades or reports a fault,
    and getting it wrong is how a bug hides.

    Observed live: `fetch_unprofitable` sent an untyped NULL that Postgres could not
    infer a type for ("could not determine data type of parameter $1"). Because this
    module originally wrapped EVERY exception as StoreUnavailableError, the endpoint
    answered 503 "batch view unavailable: postgres" while Postgres was healthy and
    every other endpoint was serving from it happily. The message pointed at the
    infrastructure; the bug was in our own SQL.

    So: connection-shaped failures degrade, because the store really is unreachable
    and the other half of the architecture should carry the request. Everything else
    - bad SQL, a missing table, a type error - propagates as a 500. A programming
    error is not a degraded service.
    """
    import socket

    if isinstance(exc, TimeoutError | socket.timeout | socket.gaierror | ConnectionError):
        return True

    # Matched by class NAME so this module need not import psycopg or redis merely
    # to classify, and so it keeps working when either is absent.
    unreachable = {
        "OperationalError",  # psycopg: connection refused, server closed, timeout
        "InterfaceError",
        "PoolTimeout",  # psycopg_pool: no connection free in time
        "ConnectionError",  # redis
        "TimeoutError",
        "BusyLoadingError",
    }
    return any(cls.__name__ in unreachable for cls in type(exc).__mro__)


# ---------------------------------------------------------------------------
# Postgres - the batch view. Exact, ACID, complete only up to the watermark.
# ---------------------------------------------------------------------------


class PostgresReader:
    """Thin, synchronous wrapper around the read DAOs in `store/mart.py`.

    A connection pool rather than a connection per request: at 150 vehicles the
    dashboard polls more often than it reads much, so connection setup would
    dominate. `psycopg_pool` is used if present and a plain connect is the fallback,
    because the pool is a deployment nicety and its absence should degrade
    performance, not availability.
    """

    def __init__(self, dsn: str | None = None, timeout: float | None = None) -> None:
        cfg = config.storage()
        api = config.serving()
        self.dsn = dsn or cfg.postgres_dsn
        self.timeout = timeout if timeout is not None else api.store_timeout_seconds
        self._pool: Any = None

    def _get_pool(self) -> Any:
        """Open the pool on first use, not at import.

        Constructing it in __init__ would make importing this module require a
        reachable database - so `fleet.serving.app` could not be imported to generate
        the OpenAPI schema, and the contract tests would need Postgres running.

        Measured on the live stack: a fresh connection per request costs 30-65 ms for
        a SELECT 1, against 1-2 ms once pooled. That is the whole of the API's
        latency budget spent on connection setup.
        """
        if self._pool is None:
            from psycopg_pool import ConnectionPool

            self._pool = ConnectionPool(
                self.dsn,
                min_size=1,
                max_size=4,
                timeout=self.timeout,
                # Do not block start-up waiting for Postgres. If it is down the API
                # must still come up and degrade to the speed view, which is the
                # behaviour the whole serving layer is designed around.
                open=True,
                check=ConnectionPool.check_connection,
            )
        return self._pool

    def _query(self, fn: Any, *args: Any) -> Any:
        try:
            try:
                pool = self._get_pool()
            except ImportError:
                # psycopg_pool is a separate distribution. Its absence should cost
                # latency, not availability - so fall back to a direct connection
                # rather than failing the request.
                import psycopg

                with (
                    psycopg.connect(self.dsn, connect_timeout=int(max(self.timeout, 1))) as conn,
                    conn.cursor() as cur,
                ):
                    return fn(cur, *args)

            with pool.connection(timeout=self.timeout) as conn, conn.cursor() as cur:
                return fn(cur, *args)
        except Exception as exc:
            if not _is_unreachable(exc):
                log.error("postgres_query_failed", error=str(exc), error_type=type(exc).__name__)
                raise
            log.warning("store_unavailable", store="postgres", error=str(exc))
            raise StoreUnavailableError("postgres", exc) from exc

    def high_water_mark(self) -> date | None:
        """None means the batch layer has never run - a cold start, not a failure."""
        return self._query(mart.read_high_water_mark)

    def zone_daily(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        return self._query(mart.fetch_zone_daily, from_date, to_date)

    def vehicle_profitability(self, vehicle_id: str, days: int) -> list[dict[str, Any]]:
        return self._query(mart.fetch_vehicle_profitability, vehicle_id, days)

    def unprofitable(self, limit: int, classification: str | None) -> list[dict[str, Any]]:
        return self._query(mart.fetch_unprofitable, limit, classification)

    def pipeline_status(self) -> dict[str, Any]:
        return self._query(mart.fetch_pipeline_status)

    def ping(self) -> None:
        self._query(lambda cur: cur.execute("SELECT 1"))


# ---------------------------------------------------------------------------
# Redis - the speed view. Approximate, TTL'd, disposable.
# ---------------------------------------------------------------------------


class RedisReader:
    """The read side of the keys written by `speed_layer/sinks/redis_sink.py`.

    Field names here are taken from the SINK, not from plan/07 section 3, which
    predicted a different set. Where the plan and the running code disagree about
    what is in a hash, the code is the fact; the plan doc has been corrected rather
    than a translation layer invented to paper over it.

    Zones are enumerated from `common/zones.py` rather than by SCAN-ing
    `fleet:zone:*`. The zone grid is static and ordered, so the static list gives a
    stable ordering, costs no round trips, and - the real reason - a zone that has
    gone quiet still appears, as a zone with no data. A SCAN would silently drop it,
    and "the zone vanished from the dashboard" is indistinguishable from "the zone
    is idle" exactly when it matters.
    """

    def __init__(self, url: str | None = None, timeout: float | None = None) -> None:
        cfg = config.storage()
        api = config.serving()
        self.url = url or cfg.redis_url
        self.timeout = timeout if timeout is not None else api.store_timeout_seconds
        self._client: Any = None

    def _conn(self) -> Any:
        if self._client is None:
            import redis

            self._client = redis.from_url(
                self.url,
                decode_responses=True,
                socket_timeout=self.timeout,
                socket_connect_timeout=self.timeout,
            )
        return self._client

    def _call(self, fn: Any) -> Any:
        try:
            return fn(self._conn())
        except Exception as exc:
            if not _is_unreachable(exc):
                log.error("redis_query_failed", error=str(exc), error_type=type(exc).__name__)
                raise
            log.warning("store_unavailable", store="redis", error=str(exc))
            self._client = None  # force a reconnect on the next call
            raise StoreUnavailableError("redis", exc) from exc

    def zones(self) -> list[dict[str, Any]]:
        """Current per-zone aggregates, one row per zone in the grid.

        Two writers populate each hash - activity and earnings - and they are
        deliberately disjoint (a previous version of the earnings writer carried
        `earnings=0.0` and clobbered the activity query's fields). A hash may
        therefore legitimately hold activity fields and no earnings yet, so every
        field is read defensively and a missing one reports as None rather than 0.
        Reporting 0 would make "no trip has completed in this window yet"
        indistinguishable from "this zone earned nothing".
        """

        def _read(client: Any) -> list[dict[str, Any]]:
            pipe = client.pipeline()
            for z in ZONES:
                pipe.hgetall(redis_keys.zone(z.zone_id))
            raw = pipe.execute()
            out = []
            for z, h in zip(ZONES, raw, strict=True):
                out.append(
                    {
                        "zone_id": z.zone_id,
                        "active_vehicles": _as_int(h.get("active_vehicles")),
                        "trips": _as_int(h.get("trips")),
                        "earnings": _as_float(h.get("earnings")),
                        "completed_trips": _as_int(h.get("completed_trips")),
                        "idle_ratio": _as_float(h.get("idle_ratio")),
                        "avg_speed_kmh": _as_float(h.get("avg_speed_kmh")),
                        "window_start": h.get("window_start"),
                        "window_end": h.get("window_end"),
                        "updated_at_sim": h.get("updated_at_sim"),
                        "reporting": bool(h),
                    }
                )
            return out

        return self._call(_read)

    def snapshot(self) -> dict[str, Any]:
        def _read(client: Any) -> dict[str, Any]:
            h = client.hgetall(redis_keys.SNAPSHOT)
            return {
                "total_active": _as_int(h.get("total_active")),
                "total_trips": _as_int(h.get("total_trips")),
                "fleet_idle_ratio": _as_float(h.get("fleet_idle_ratio")),
                "zones_reporting": _as_int(h.get("zones_reporting")),
                "updated_at_sim": h.get("updated_at_sim"),
            }

        return self._call(_read)

    def vehicle(self, vehicle_id: str) -> dict[str, Any] | None:
        def _read(client: Any) -> dict[str, Any] | None:
            h = client.hgetall(redis_keys.vehicle_state(vehicle_id))
            return dict(h) if h else None

        return self._call(_read)

    def idle_alerts(self, limit: int) -> list[dict[str, Any]]:
        """Newest-first idle alerts from the ZSET the alert sink maintains."""

        def _read(client: Any) -> list[dict[str, Any]]:
            members = client.zrevrange(redis_keys.IDLE_ALERTS, 0, max(limit - 1, 0))
            out = []
            for m in members:
                try:
                    out.append(json.loads(m))
                except (TypeError, ValueError):
                    # One malformed member must not take down the alert feed. It is
                    # counted in the log rather than raised, because an alerts
                    # endpoint that 500s during an incident is the worst time for it.
                    log.warning("idle_alert_unparseable", member=str(m)[:200])
            return out

        return self._call(_read)

    def zone_earnings_series(self, zone_id: str, limit: int) -> list[dict[str, Any]]:
        """The LPUSH'd sparkline: elements are `"{window_end}:{earnings}"` strings."""

        def _read(client: Any) -> list[dict[str, Any]]:
            raw = client.lrange(redis_keys.zone_earnings_series(zone_id), 0, max(limit - 1, 0))
            out = []
            for item in raw:
                window_end, _, earnings = str(item).rpartition(":")
                if window_end:
                    out.append({"window_end": window_end, "earnings": _as_float(earnings)})
            return out

        return self._call(_read)

    def ping(self) -> None:
        self._call(lambda client: client.ping())


def _as_int(v: Any) -> int | None:
    try:
        return int(float(v)) if v is not None else None
    except (TypeError, ValueError):
        return None


def _as_float(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
