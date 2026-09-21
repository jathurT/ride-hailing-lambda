"""The prolonged-idle state machine.

Pure Python: no Spark, no Kafka, no Docker. That is the point of keeping the state
machine separate from the execution engine - these sequences are the ones that
actually matter, and they run in milliseconds.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from fleet.transforms.idle import (
    CRITICAL,
    WARNING,
    IdleEvent,
    IdleState,
    advance,
    alert_id,
    is_stale,
)

T0 = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)
WARN_AFTER = 45.0
CRIT_AFTER = 90.0


def ev(minutes: float, status: str = "idle", zone: str | None = "Z11") -> IdleEvent:
    return IdleEvent("V007", status, T0 + timedelta(minutes=minutes), zone)


def run(events, state: IdleState | None = None):
    return advance(state or IdleState(), events, WARN_AFTER, CRIT_AFTER)


class TestNoAlertBeforeTheThreshold:
    def test_a_single_idle_ping_alerts_nothing(self):
        state, alerts = run([ev(0)])
        assert alerts == []
        assert state.is_idle

    def test_idle_for_44_minutes_is_below_threshold(self):
        _, alerts = run([ev(0), ev(20), ev(44)])
        assert alerts == []

    def test_a_busy_vehicle_never_alerts(self):
        _, alerts = run([ev(i * 5, "on_trip") for i in range(40)])
        assert alerts == []


class TestAlerting:
    def test_crossing_45_minutes_raises_a_warning(self):
        _, alerts = run([ev(0), ev(30), ev(46)])
        assert len(alerts) == 1
        assert alerts[0].severity == WARNING
        assert alerts[0].idle_minutes == pytest.approx(46.0)

    def test_crossing_90_minutes_escalates_to_critical(self):
        _, alerts = run([ev(0), ev(46), ev(91)])
        assert [a.severity for a in alerts] == [WARNING, CRITICAL]

    def test_the_alert_carries_the_zone(self):
        """Ops needs to know WHERE a vehicle is idling; a bare id is not actionable."""
        _, alerts = run([ev(0), ev(50, zone="Z12")])
        assert alerts[0].zone_id == "Z12"

    def test_going_straight_past_critical_still_reports_it(self):
        """A gap in reporting must not let a vehicle skip the escalation."""
        _, alerts = run([ev(0), ev(120)])
        assert [a.severity for a in alerts] == [CRITICAL]


class TestNoAlertStorm:
    """A sustained condition must produce ONE alert per level, not one per event.

    Without this a vehicle idle for an hour emits hundreds of duplicates for a
    single incident. Alert fatigue is an operational failure, not a cosmetic one.
    """

    def test_sustained_idling_emits_one_warning(self):
        events = [ev(0)] + [ev(46 + i) for i in range(30)]
        _, alerts = run(events)
        assert len([a for a in alerts if a.severity == WARNING]) == 1

    def test_a_long_episode_emits_exactly_two_alerts(self):
        events = [ev(0)] + [ev(i) for i in range(1, 180)]
        _, alerts = run(events)
        assert [a.severity for a in alerts] == [WARNING, CRITICAL]

    def test_alerts_never_de_escalate(self):
        _, alerts = run([ev(0), ev(91), ev(95), ev(120)])
        assert [a.severity for a in alerts] == [CRITICAL]


class TestEpisodeBoundaries:
    def test_a_trip_ends_the_episode(self):
        state, _ = run([ev(0), ev(50), ev(55, "on_trip")])
        assert not state.is_idle
        assert state.alerted_level is None

    def test_idling_again_after_a_trip_is_a_new_incident(self):
        """Two separate idle periods must be two alerts with different ids -
        otherwise the second incident is silently deduplicated away."""
        events = [ev(0), ev(50), ev(55, "on_trip"), ev(60), ev(110)]
        _, alerts = run(events)
        assert len(alerts) == 2
        assert alerts[0].alert_id != alerts[1].alert_id

    def test_enroute_also_ends_the_episode(self):
        state, _ = run([ev(0), ev(30), ev(35, "enroute")])
        assert not state.is_idle


class TestAlertIdStability:
    """The id must identify the EPISODE, so at-least-once delivery is safe: a
    replayed micro-batch re-emits an alert the consumer already has."""

    def test_same_episode_gives_the_same_id(self):
        assert alert_id("V007", T0) == alert_id("V007", T0)

    def test_different_episodes_differ(self):
        assert alert_id("V007", T0) != alert_id("V007", T0 + timedelta(minutes=1))

    def test_different_vehicles_differ(self):
        assert alert_id("V007", T0) != alert_id("V008", T0)

    def test_replaying_a_batch_reproduces_the_same_alert_id(self):
        events = [ev(0), ev(50)]
        _, first = run(events)
        _, second = run(events)
        assert first[0].alert_id == second[0].alert_id


class TestStateIsCarriedAcrossBatches:
    """The whole reason this component is stateful: the threshold is crossed in a
    LATER micro-batch than the one where idling began."""

    def test_idling_begins_in_one_batch_and_alerts_in_the_next(self):
        state, alerts = run([ev(0), ev(20)])
        assert alerts == []
        _, alerts = run([ev(46)], state=state)
        assert len(alerts) == 1 and alerts[0].severity == WARNING

    def test_the_episode_start_survives_across_batches(self):
        state, _ = run([ev(0)])
        _, alerts = run([ev(50)], state=state)
        assert alerts[0].idle_since_sim == T0

    def test_state_is_small_enough_to_checkpoint(self):
        """A fat state object is what makes stateful streaming expensive."""
        from dataclasses import fields

        assert len(fields(IdleState)) <= 4


class TestStateExpiry:
    """A vehicle that stops reporting - a dead telematics unit - must not keep its
    state forever, or the store grows without bound."""

    def test_a_long_silent_vehicle_is_stale(self):
        state, _ = run([ev(0)])
        assert is_stale(state, T0 + timedelta(hours=8), ttl_sim_minutes=240)

    def test_a_recently_seen_vehicle_is_not_stale(self):
        state, _ = run([ev(0)])
        assert not is_stale(state, T0 + timedelta(minutes=30), ttl_sim_minutes=240)

    def test_a_non_idle_vehicle_holds_no_state_worth_keeping(self):
        state, _ = run([ev(0, "on_trip")])
        assert is_stale(state, T0, ttl_sim_minutes=240)


class TestTheScriptedDemoNarrative:
    """V007 is held idle for 90 simulated minutes from simulated day 1 at 10:00 so
    the alert fires on cue during the demo. If this test fails, the demo is broken."""

    def test_v007_reaches_warning_and_then_critical(self):
        # a ping every 3 simulated minutes across a 90-minute forced idle
        events = [ev(3 * i) for i in range(31)]
        _, alerts = run(events)
        assert [a.severity for a in alerts] == [WARNING, CRITICAL]
        assert alerts[0].vehicle_id == "V007"

    def test_the_warning_lands_within_one_ping_of_the_threshold(self):
        events = [ev(3 * i) for i in range(31)]
        _, alerts = run(events)
        assert WARN_AFTER <= alerts[0].idle_minutes < WARN_AFTER + 3
