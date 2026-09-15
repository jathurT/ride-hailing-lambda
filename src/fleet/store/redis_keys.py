"""Redis key design for the speed view.

Two rules, both load-bearing:

1. **Every TTL is derived from the simulated clock**, never hard-coded in real
   seconds. A 2-simulated-hour TTL is 25 real seconds at 288x; writing `25` here
   would silently become wrong the moment SIM_DAY_SECONDS changed.

2. **Values are overwritten, never incremented.** Structured Streaming may re-run a
   micro-batch after failure, so an INCR would double-count on replay. Recomputing
   the aggregate and HSET-ing it makes the sink idempotent, which is what lets us
   claim at-least-once delivery is safe (plan/04 section 5).

The speed view being TTL'd and disposable is also what makes the Lambda
fault-tolerance argument true: Redis can be flushed and nothing permanent is lost.
"""

from __future__ import annotations

from fleet.common.simclock import SimClock

ZONE = "fleet:zone:{zone_id}"
SNAPSHOT = "fleet:snapshot"
VEHICLE_STATE = "fleet:vehicle:{vehicle_id}:state"
IDLE_ALERTS = "fleet:alerts:idle"
ZONE_EARNINGS_SERIES = "fleet:ts:zone:{zone_id}:earnings"
SIM_EPOCH = "sim:epoch_wall"

IDLE_ALERTS_MAX = 200
EARNINGS_SERIES_MAX = 96


def zone(zone_id: str) -> str:
    return ZONE.format(zone_id=zone_id)


def vehicle_state(vehicle_id: str) -> str:
    return VEHICLE_STATE.format(vehicle_id=vehicle_id)


def zone_earnings_series(zone_id: str) -> str:
    return ZONE_EARNINGS_SERIES.format(zone_id=zone_id)


def ttl_seconds(clock: SimClock, sim_hours: float = 0.0, sim_minutes: float = 0.0) -> int:
    """Convert a simulated duration into the real seconds Redis expects.

    Always at least 1 second: a TTL of 0 means "no expiry" in Redis, so rounding a
    very short simulated duration down to 0 would leak keys forever.
    """
    sim_seconds = sim_hours * 3600 + sim_minutes * 60
    return max(int(clock.sim_to_real(sim_seconds)), 1)
