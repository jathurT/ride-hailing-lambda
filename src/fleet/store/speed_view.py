"""Writing the speed view to Redis.

Every write here is an **overwrite of a recomputed value**, never an increment.
Structured Streaming may re-run a micro-batch after a failure, so an INCR would
double-count on replay. Recomputing the aggregate and HSET-ing it makes the sink
idempotent under at-least-once delivery - which is the whole reason we can claim
the at-least-once path is safe (plan/04 section 5).

If you ever find yourself reaching for HINCRBY in this file, the aggregate belongs
in Spark, not in Redis.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fleet.common import config
from fleet.common.simclock import SimClock
from fleet.store import redis_keys as keys


@dataclass(frozen=True, slots=True)
class ZoneAggregate:
    """Windowed ACTIVITY for one zone.

    Note what is absent: `earnings`. Activity and earnings are produced by two
    different streaming queries (a query may hold only one aggregation, and revenue
    must come from a trip-deduplicated stream). Both write into the SAME Redis hash,
    so they must own disjoint field sets - otherwise whichever writes last wins and
    silently zeroes the other's work.

    That is not hypothetical: carrying `earnings=0.0` on this record made the
    activity writer clobber every earnings value on every micro-batch, and the
    dashboard showed 0.00 revenue across all zones with nothing failing.
    """

    zone_id: str
    active_vehicles: int
    trips: int
    idle_ratio: float
    avg_speed_kmh: float
    window_start: str
    window_end: str
    updated_at_sim: str

    def as_mapping(self) -> dict[str, str]:
        """Activity fields only. Must never include a field the earnings writer owns."""
        return {
            "active_vehicles": str(self.active_vehicles),
            "trips": str(self.trips),
            "idle_ratio": f"{self.idle_ratio:.4f}",
            "avg_speed_kmh": f"{self.avg_speed_kmh:.1f}",
            "window_start": self.window_start,
            "window_end": self.window_end,
            "updated_at_sim": self.updated_at_sim,
        }


# Fields owned by the earnings query. Asserted disjoint from ZoneAggregate's in
# tests, so the clobbering bug cannot come back.
EARNINGS_FIELDS = frozenset({"earnings", "completed_trips", "earnings_window_end"})


class SpeedView:
    """Thin wrapper over Redis so TTL arithmetic lives in exactly one place."""

    def __init__(self, client: Any, clock: SimClock) -> None:
        self.client = client
        self.clock = clock
        s = config.storage()
        self.zone_ttl = keys.ttl_seconds(clock, sim_hours=s.speed_view_ttl_sim_hours)
        self.vehicle_ttl = keys.ttl_seconds(clock, sim_minutes=s.vehicle_state_ttl_sim_minutes)

    def write_zone_aggregates(self, aggregates: list[ZoneAggregate]) -> int:
        """Upsert one activity hash per zone, in a single pipeline round-trip.

        Writes ONLY the activity fields. The earnings query owns `earnings`,
        `completed_trips` and the earnings sparkline series - see the note on
        ZoneAggregate about why the two writers must not overlap.
        """
        pipe = self.client.pipeline()
        for agg in aggregates:
            key = keys.zone(agg.zone_id)
            # HSET, never HINCRBY - see the module docstring.
            pipe.hset(key, mapping=agg.as_mapping())
            pipe.expire(key, self.zone_ttl)
        pipe.execute()
        return len(aggregates)

    def push_zone_earnings(self, zone_id: str, window_end: str, earnings: float) -> None:
        """Append to a zone's earnings sparkline series. Owned by the earnings query."""
        series = keys.zone_earnings_series(zone_id)
        pipe = self.client.pipeline()
        pipe.lpush(series, f"{window_end}:{earnings:.2f}")
        pipe.ltrim(series, 0, keys.EARNINGS_SERIES_MAX - 1)
        pipe.expire(series, self.zone_ttl)
        pipe.execute()

    def write_snapshot(self, mapping: dict[str, Any]) -> None:
        pipe = self.client.pipeline()
        pipe.hset(keys.SNAPSHOT, mapping={k: str(v) for k, v in mapping.items()})
        pipe.expire(keys.SNAPSHOT, self.zone_ttl)
        pipe.execute()

    def write_vehicle_states(self, states: list[dict[str, Any]]) -> int:
        pipe = self.client.pipeline()
        for st in states:
            key = keys.vehicle_state(str(st["vehicle_id"]))
            pipe.hset(key, mapping={k: str(v) for k, v in st.items()})
            pipe.expire(key, self.vehicle_ttl)
        pipe.execute()
        return len(states)

    def push_idle_alert(self, alert_json: str, score: float) -> None:
        """Sorted set scored by simulated time, trimmed to a bounded length.

        Trimming matters: without it the alert feed grows unbounded in a store that
        has no persistence and a fixed maxmemory.
        """
        pipe = self.client.pipeline()
        pipe.zadd(keys.IDLE_ALERTS, {alert_json: score})
        pipe.zremrangebyrank(keys.IDLE_ALERTS, 0, -(keys.IDLE_ALERTS_MAX + 1))
        pipe.expire(keys.IDLE_ALERTS, self.zone_ttl)
        pipe.execute()
