"""The vehicle state machine.

These invariants are load-bearing: the stream validator downstream rejects events
that break them (STATUS_TRIP_MISMATCH, MISSING_FARE_ON_TRIP), so if the simulator
broke them the DLQ would fill with our own bugs rather than with injected defects.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from random import Random

import pytest

from fleet.common.zones import ZoneClass
from fleet.ingestion.generators.vehicle import Status, Vehicle

START = datetime(2026, 3, 1, tzinfo=UTC)
PING = timedelta(minutes=3)


def make(seed: int = 42, **kw) -> Vehicle:
    v = Vehicle(
        vehicle_id=kw.pop("vehicle_id", "V001"),
        driver_id="D001",
        home_zone_id=kw.pop("home_zone_id", "Z05"),
        fuel_type="petrol",
        model="Corolla",
        capacity=4,
        rng=Random(seed),
        **kw,
    )
    v.place_at_home(START)
    return v


def drive(v: Vehicle, steps: int) -> list[Vehicle]:
    for i in range(steps):
        v.advance_to(START + PING * i)
    return v


class TestInvariants:
    @pytest.mark.parametrize("seed", [7, 42, 99, 123])
    def test_trip_id_is_null_iff_idle(self, seed):
        """The stream validator enforces exactly this; the simulator must not be the
        thing that violates it."""
        v = make(seed)
        for i in range(1500):
            v.advance_to(START + PING * i)
            assert (v.trip is None) == (v.status is Status.IDLE)

    @pytest.mark.parametrize("seed", [7, 42])
    def test_fare_is_present_iff_on_trip(self, seed):
        v = make(seed)
        for i in range(1500):
            v.advance_to(START + PING * i)
            assert (v.current_fare() is None) == (v.status is not Status.ON_TRIP)

    def test_speed_is_zero_when_idle(self):
        v = make()
        for i in range(1000):
            v.advance_to(START + PING * i)
            if v.status is Status.IDLE:
                assert v.speed_kmh == 0.0

    def test_speed_never_implausible(self):
        """Any speed above 200 in the stream must be an *injected* defect, never a
        simulator artefact - otherwise the DLQ rate is meaningless."""
        v = make()
        for i in range(2000):
            v.advance_to(START + PING * i)
            assert 0.0 <= v.speed_kmh <= 90.0

    def test_whole_fleet_stays_inside_the_city_the_validator_enforces(self):
        """Every real position must pass the stream's bounds check.

        The first version of this test checked one vehicle and allowed 0.01 degrees
        (about 1.1 km) outside the box, so it passed while the live stack
        dead-lettered real events as COORDINATES_OUT_OF_BOUNDS: 176 of them in the
        first few minutes of the rerun, all near the city edge, and the lake (which
        keeps only valid events) lost those trips too.
        """
        from fleet.common.zones import in_city_bounds
        from fleet.ingestion.generators.fleet import build_fleet

        fleet = build_fleet(150, 42, START)
        outside = 0
        for i in range(480):  # one simulated day of 3-minute pings
            for v in fleet:
                v.advance_to(START + PING * i)
                outside += not in_city_bounds(v.lat, v.lon)
        assert outside == 0, f"{outside} real positions fall outside the city bounds"


class TestDeterminism:
    def _trace(self, seed: int, n: int = 300) -> list[tuple]:
        v = make(seed)
        out = []
        for i in range(n):
            v.advance_to(START + PING * i)
            out.append(
                (
                    round(v.lat, 6),
                    round(v.lon, 6),
                    v.status.value,
                    v.trip.trip_id if v.trip else None,
                )
            )
        return out

    def test_same_seed_reproduces_exactly(self):
        """Protects the scripted demo narratives and every report screenshot."""
        assert self._trace(42) == self._trace(42)

    def test_different_seed_diverges(self):
        assert self._trace(42) != self._trace(7)


class TestEconomics:
    def _day(self, seed: int, **kw) -> Vehicle:
        v = make(seed, **kw)
        return drive(v, 480)  # 24 simulated hours

    def test_deadhead_is_tracked_separately_from_paid_distance(self):
        """Deadhead - unpaid kilometres driven to reach a pickup - is the mechanism
        by which a real vehicle becomes unprofitable. Without it the daily report's
        finding would be arbitrary."""
        v = self._day(42)
        assert v.day_paid_km > 0
        assert v.day_deadhead_km > 0
        assert v.day_distance_km == pytest.approx(v.day_paid_km + v.day_deadhead_km)

    def test_outskirts_vehicle_has_a_worse_deadhead_ratio(self):
        out = self._day(113, home_zone_id="Z11", prefers_zone_class=ZoneClass.OUTSKIRTS)
        normal = self._day(113, home_zone_id="Z05")
        assert out.day_deadhead_ratio > normal.day_deadhead_ratio

    def test_outskirts_vehicle_earns_less_per_km_driven(self):
        """The V113 narrative, asserted rather than hoped for."""
        losses = 0
        for seed in range(12):
            out = self._day(
                seed, vehicle_id="V113", home_zone_id="Z11", prefers_zone_class=ZoneClass.OUTSKIRTS
            )
            normal = self._day(seed, home_zone_id="Z05")
            if out.day_revenue / max(out.day_distance_km, 1) < normal.day_revenue / max(
                normal.day_distance_km, 1
            ):
                losses += 1
        assert losses >= 10, f"outskirts vehicle only underperformed in {losses}/12 seeds"

    def test_accumulators_reset_each_simulated_day(self):
        v = make()
        drive(v, 480)
        assert v.day_trips > 0
        v.advance_to(START + timedelta(days=1, hours=1))
        assert v.day_trips == 0 and v.day_paid_km == 0.0


class TestScriptedNarratives:
    def test_forced_idle_holds_the_vehicle_long_enough_to_alert(self):
        """V007 must exceed the 45-simulated-minute alert threshold on cue."""
        idle_from = START + timedelta(days=1, hours=10)
        v = make(
            7,
            forced_idle_from_sim=idle_from,
            forced_idle_until_sim=idle_from + timedelta(minutes=90),
        )
        peak = 0.0
        for i in range(1000):
            now = START + PING * i
            v.advance_to(now)
            if idle_from <= now < idle_from + timedelta(minutes=95):
                peak = max(peak, v.idle_minutes(now))
        assert peak >= 45.0

    def test_zone_preference_actually_biases_destinations(self):
        v = make(5, home_zone_id="Z11", prefers_zone_class=ZoneClass.OUTSKIRTS)
        seen: Counter[str] = Counter()
        for i in range(2000):
            v.advance_to(START + PING * i)
            if v.trip:
                seen[v.trip.dest_zone_id] += 1
        outskirt_share = sum(n for z, n in seen.items() if z in {"Z11", "Z12"}) / max(
            sum(seen.values()), 1
        )
        assert outskirt_share > 0.5


class TestStallRecovery:
    """A producer starved of CPU falls behind in SIMULATED time very fast: at 288x,
    fourteen real seconds is over an hour of simulated time. That is normal on a
    loaded laptop and must not be fatal.

    It was: the catch-up guard raised, crashing the producer roughly hourly. Docker's
    restart policy restarted it, so throughput looked healthy and the crashes went
    unnoticed for hours.
    """

    def test_a_long_stall_resyncs_instead_of_raising(self):
        v = make()
        v.advance_to(START + PING)
        v.advance_to(START + timedelta(days=3))  # ~3 simulated days of stall
        assert v.resyncs == 1
        assert v.status is Status.IDLE

    def test_the_vehicle_keeps_working_after_a_resync(self):
        """Recovery must be real, not just survival."""
        v = make()
        v.advance_to(START + timedelta(days=3))
        for i in range(400):
            v.advance_to(START + timedelta(days=3) + PING * i)
        assert v.day_trips > 0

    def test_a_short_gap_is_replayed_not_skipped(self):
        """Ordinary catch-up must still step through states - skipping them would
        lose trips and understate utilization."""
        v = make()
        v.advance_to(START + timedelta(minutes=90))
        assert v.resyncs == 0

    def test_resync_does_not_replay_the_missed_window(self):
        """Those simulated minutes produced no events, so no consumer saw them.
        Reconstructing them would only make the producer slower and further behind."""
        v = make()
        for i in range(200):
            v.advance_to(START + PING * i)
        trips_before = v.day_trips
        v.advance_to(START + timedelta(days=2))
        assert v.day_trips <= trips_before  # day roll resets; never inflates

    def test_a_state_that_never_ends_still_raises(self):
        """The resync path must not mask a genuine state-machine bug.

        A small gap plus a transition that never advances `state_until_sim` is the
        signature of a broken state machine, as opposed to a stall - and it must
        still fail loudly rather than spin.
        """

        class StuckVehicle(Vehicle):
            def _transition(self, at):  # never advances state_until_sim
                return None

        v = StuckVehicle(
            vehicle_id="VSTUCK",
            driver_id="D",
            home_zone_id="Z05",
            fuel_type="petrol",
            model="Corolla",
            capacity=4,
            rng=Random(1),
        )
        v.place_at_home(START)
        v.state_until_sim = START + PING
        with pytest.raises(RuntimeError, match="not scheduling its end"):
            v.advance_to(START + PING + timedelta(minutes=1))


class TestEmittedGpsMatchesInternalAccounting:
    """The simulator's internal counters and its emitted GPS track must tell the
    same story, because the batch layer only ever sees the GPS.

    They did not. During ENROUTE the vehicle set a speed but never moved, so a
    vehicle reported "30 km/h" with an unchanging position. The batch layer
    recomputes distance from consecutive positions and measured ~8% deadhead for
    EVERY vehicle, against internal figures of 22-45% - erasing the economic
    difference between a CBD vehicle and an outskirts one.

    Found by the batch layer disagreeing with the simulator: two independent
    measurements of one quantity is exactly what a reconciliation pipeline is for.
    """

    def gps_distances(self, v: Vehicle, steps: int = 480) -> tuple[float, float]:
        """Distance as the BATCH LAYER computes it: from emitted positions."""
        from fleet.common.geo import haversine_km

        prev = None
        paid = dead = 0.0
        for i in range(steps):
            v.advance_to(START + PING * i)
            if prev is not None:
                seg = haversine_km(prev[0], prev[1], v.lat, v.lon)
                if prev[2] is Status.ON_TRIP:
                    paid += seg
                elif prev[2] is Status.ENROUTE:
                    dead += seg
            prev = (v.lat, v.lon, v.status)
        return paid, dead

    def test_a_vehicle_actually_moves_while_enroute(self, seed=42):
        v = make(seed)
        _, dead = self.gps_distances(v)
        assert dead > 1.0, "no GPS movement during enroute - position was never updated"

    def test_internal_and_measured_deadhead_agree(self):
        """Allowing for GPS jitter, which legitimately adds a little distance."""
        v = make(42)
        paid, dead = self.gps_distances(v)
        measured_ratio = dead / (paid + dead)
        assert measured_ratio == pytest.approx(v.day_deadhead_ratio, abs=0.10)

    def test_outskirts_deadhead_is_visible_in_the_gps_not_just_the_counter(self):
        """The economic mechanism must survive into the emitted data, or the daily
        report cannot explain WHY a vehicle is unprofitable."""
        out = make(113, home_zone_id="Z11", prefers_zone_class=ZoneClass.OUTSKIRTS)
        normal = make(113, home_zone_id="Z05")
        p_out, d_out = self.gps_distances(out)
        p_norm, d_norm = self.gps_distances(normal)
        assert d_out / (p_out + d_out) > d_norm / (p_norm + d_norm) + 0.05

    def test_reported_speed_is_consistent_with_movement(self):
        """A vehicle claiming 30 km/h while stationary is the bug this class exists
        for. Over an enroute leg, some movement must accompany a non-zero speed."""
        v = make(7)
        moved_while_enroute = False
        prev = None
        for i in range(600):
            v.advance_to(START + PING * i)
            if (
                prev
                and prev[2] is Status.ENROUTE
                and v.status is Status.ENROUTE
                and (v.lat, v.lon) != (prev[0], prev[1])
            ):
                moved_while_enroute = True
                break
            prev = (v.lat, v.lon, v.status)
        assert moved_while_enroute
