"""Prolonged-idle detection: the per-vehicle state machine.

The PDF's suggested output: *"Threshold-based alerts when a vehicle is considerably
idle for a longer period of time."*

This is the only genuinely STATEFUL piece of the speed layer. "Idle for 45 minutes"
cannot be answered from a single event or from one window - it needs memory of when
the vehicle last stopped being idle, carried across micro-batches.

★ The state machine lives here as plain Python, deliberately, so that:

  * it is testable without Spark, Kafka or Docker - the tests below run in
    milliseconds and cover the sequences that matter;
  * the choice of execution engine (Spark's state store vs an external store) is a
    deployment decision, not a rewrite. Both call the same `advance` function.

Ordering is a correctness requirement, not a nicety: the machine must see a
vehicle's status transitions in the order they happened. That is exactly why the
telemetry topic is keyed by `vehicle_id` (ADR-002).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

IDLE = "idle"

WARNING = "WARNING"
CRITICAL = "CRITICAL"

# Escalation order, lowest first. An alert is re-emitted only when the level rises.
_LEVELS = (WARNING, CRITICAL)


@dataclass(frozen=True, slots=True)
class IdleState:
    """What we must remember about one vehicle between micro-batches.

    Deliberately tiny and JSON-serialisable: it has to survive being encoded into a
    Spark state store or a Redis hash, and a fat state object is what makes stateful
    streaming expensive.
    """

    idle_since_sim: datetime | None = None
    last_status: str | None = None
    alerted_level: str | None = None

    @property
    def is_idle(self) -> bool:
        return self.idle_since_sim is not None


@dataclass(frozen=True, slots=True)
class IdleAlert:
    alert_id: str
    vehicle_id: str
    severity: str
    idle_minutes: float
    idle_since_sim: datetime
    raised_at_sim: datetime
    zone_id: str | None


@dataclass(frozen=True, slots=True)
class IdleEvent:
    """The only fields of a telemetry event this machine cares about."""

    vehicle_id: str
    status: str
    event_time: datetime
    zone_id: str | None = None


def alert_id(vehicle_id: str, idle_since_sim: datetime) -> str:
    """A stable id for one idle EPISODE.

    Every alert about the same continuous idle period gets the same id, so
    at-least-once delivery is safe: a replayed micro-batch re-emits an alert the
    consumer has already seen and can discard. Including a timestamp or a batch id
    here would make every retry look like a new incident.
    """
    return hashlib.sha1(
        f"{vehicle_id}|{idle_since_sim.isoformat()}".encode(), usedforsecurity=False
    ).hexdigest()[:16]


def _level_for(idle_minutes: float, warn_after: float, critical_after: float) -> str | None:
    if idle_minutes >= critical_after:
        return CRITICAL
    if idle_minutes >= warn_after:
        return WARNING
    return None


def _escalated(previous: str | None, current: str | None) -> bool:
    """True only when the level has RISEN.

    Without this, a vehicle idle for an hour emits an alert on every micro-batch -
    hundreds of duplicates for one incident. Alert fatigue is a real operational
    failure, not a cosmetic one.
    """
    if current is None:
        return False
    if previous is None:
        return True
    return _LEVELS.index(current) > _LEVELS.index(previous)


def advance(
    state: IdleState,
    events: list[IdleEvent],
    warn_after_sim_minutes: float,
    critical_after_sim_minutes: float,
) -> tuple[IdleState, list[IdleAlert]]:
    """Feed one vehicle's events through the machine.

    Returns the new state and any alerts to emit. Pure: no clock, no I/O, no
    globals - the caller supplies everything, which is what makes the sequences
    below testable exhaustively.

    Events must be in event-time order; the caller sorts them, because a micro-batch
    does not guarantee ordering within a group.
    """
    alerts: list[IdleAlert] = []

    for event in events:
        if event.status != IDLE:
            # Any non-idle status ends the episode. The next idle period is a NEW
            # episode with a new id, so a vehicle that goes idle, takes a trip and
            # goes idle again is correctly two incidents rather than one.
            state = IdleState(idle_since_sim=None, last_status=event.status, alerted_level=None)
            continue

        if not state.is_idle:
            state = IdleState(idle_since_sim=event.event_time, last_status=IDLE, alerted_level=None)
            continue

        assert state.idle_since_sim is not None
        idle_minutes = (event.event_time - state.idle_since_sim).total_seconds() / 60
        level = _level_for(idle_minutes, warn_after_sim_minutes, critical_after_sim_minutes)

        if _escalated(state.alerted_level, level):
            alerts.append(
                IdleAlert(
                    alert_id=alert_id(event.vehicle_id, state.idle_since_sim),
                    vehicle_id=event.vehicle_id,
                    severity=level or WARNING,
                    idle_minutes=round(idle_minutes, 1),
                    idle_since_sim=state.idle_since_sim,
                    raised_at_sim=event.event_time,
                    zone_id=event.zone_id,
                )
            )
            state = replace(state, alerted_level=level, last_status=IDLE)
        else:
            state = replace(state, last_status=IDLE)

    return state, alerts


def is_stale(state: IdleState, now_sim: datetime, ttl_sim_minutes: float) -> bool:
    """Whether a vehicle's state can be discarded.

    A vehicle that stops reporting entirely - a dead telematics unit - would
    otherwise keep its state forever and the store would grow without bound. Spark
    expresses this as a state timeout; the Redis implementation expresses it as a
    key TTL. Same rule either way.
    """
    reference = state.idle_since_sim
    if reference is None:
        return True
    return now_sim - reference > timedelta(minutes=ttl_sim_minutes)
