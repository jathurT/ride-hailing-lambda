"""The per-vehicle state machine that drives the telemetry simulator.

    IDLE ---demand draw---> ENROUTE ---pickup---> ON_TRIP ---dropoff---> IDLE

Every vehicle is an independent object stepped by simulated time. Randomness comes
from an injected `random.Random`, so a fixed seed reproduces a run byte-for-byte -
which is what makes the scripted demo narratives (V007 going idle, V113 becoming
unprofitable) fire at the same simulated moment on every run, and what
`tests/unit/test_determinism.py` asserts.

Why a state machine rather than emitting independent random pings: the idle detector
downstream is itself a per-vehicle state machine reading status transitions in order.
If the simulator produced uncorrelated statuses there would be nothing coherent for
it to detect, and the alert would be meaningless.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from random import Random

from fleet.common.geo import haversine_km, interpolate
from fleet.common.zones import BY_ID, ZONES, Zone, ZoneClass
from fleet.ingestion.generators.demand import (
    demand_multiplier,
    industrial_multiplier,
    surge_multiplier,
)
from fleet.ingestion.generators.fares import compute_fare

GPS_JITTER_DEG = 0.0004  # ~45 m
MAX_SPEED_KMH = 90.0
FREE_FLOW_SPEED_KMH = 45.0
MAX_CATCHUP_TRANSITIONS = 400
RESYNC_THRESHOLD_SIM_MINUTES = 720
"""Half a simulated day. Beyond this a vehicle is not 'a bit behind', it missed a
stall - replaying its history event by event would be both slow and pointless,
since nothing downstream saw those minutes anyway."""


class Status(StrEnum):
    IDLE = "idle"
    ENROUTE = "enroute"
    ON_TRIP = "on_trip"


@dataclass(slots=True)
class Trip:
    trip_id: str
    origin: tuple[float, float]
    destination: tuple[float, float]
    start_sim: datetime
    duration_sim_minutes: float
    straight_km: float
    is_airport: bool
    surge: float
    dest_zone_id: str


@dataclass(slots=True)
class Vehicle:
    """One simulated vehicle. Mutable by design - it *is* the state machine."""

    vehicle_id: str
    driver_id: str
    home_zone_id: str
    fuel_type: str
    model: str
    capacity: int
    rng: Random

    status: Status = Status.IDLE
    lat: float = 0.0
    lon: float = 0.0
    speed_kmh: float = 0.0
    trip: Trip | None = None

    state_since_sim: datetime | None = None
    state_until_sim: datetime | None = None

    # Per-simulated-day accumulators, reset at midnight. The expense generator reads
    # distance from here so the partner's figure is derived from the same physical
    # journey we report - then perturbed by ~3%, which is the reconciliation
    # discrepancy the batch layer is meant to surface (plan/03 section 5.1).
    day_paid_km: float = 0.0
    """Kilometres driven with a passenger aboard. These earn."""
    day_deadhead_km: float = 0.0
    """Kilometres driven to reach a pickup. These cost fuel and earn nothing.

    Deadhead is the mechanism by which a real vehicle becomes unprofitable, and
    modelling it is what makes the daily report's finding meaningful rather than
    arbitrary: a vehicle working a low-demand zone drives further per fare, so its
    fuel bill rises while its revenue does not."""
    day_revenue: float = 0.0
    day_trips: int = 0
    _current_day: int = -1

    # Scripted-narrative overrides, applied by the fleet builder.
    forced_idle_from_sim: datetime | None = None
    forced_idle_until_sim: datetime | None = None
    prefers_zone_class: ZoneClass | None = None

    # Pickup point chosen when ENROUTE begins, consumed when the trip starts.
    # Declared as a field because the dataclass uses slots=True.
    _pending_pickup: tuple[float, float] | None = None
    _enroute_origin: tuple[float, float] | None = None
    _deadhead_leg_km: float = 0.0
    _last_resync_minutes: float = 0.0

    @property
    def day_distance_km(self) -> float:
        """Total kilometres, paid and unpaid. This is what burns fuel."""
        return self.day_paid_km + self.day_deadhead_km

    @property
    def day_deadhead_ratio(self) -> float:
        return self.day_deadhead_km / self.day_distance_km if self.day_distance_km else 0.0

    # -- lifecycle ---------------------------------------------------------

    resyncs: int = 0
    """How many times this vehicle skipped forward after a producer stall. Exported
    as a metric: a rising count means the host cannot keep up with the configured
    event rate, which is a capacity signal rather than a correctness one."""

    def resync(self, now: datetime, behind_minutes: float) -> None:
        """Abandon a missed interval and rejoin the simulation at `now`.

        Deliberately does NOT replay the gap. Those simulated minutes produced no
        events, so no consumer ever saw them; reconstructing them would only make
        the producer slower and further behind.
        """
        self.resyncs += 1
        self._pending_pickup = None
        self._deadhead_leg_km = 0.0
        self._roll_day(now)
        self._enter_idle(now)
        self._last_resync_minutes = behind_minutes

    def place_at_home(self, now: datetime) -> None:
        z = BY_ID[self.home_zone_id]
        self.lat, self.lon = self._jittered(*z.centre)
        self._enter_idle(now)

    def advance_to(self, now: datetime) -> None:
        """Step the machine until its scheduled transition time is in the future."""
        # Two different things can put a vehicle behind, and they need opposite
        # responses:
        #
        #   a transient STALL - the producer was starved of CPU while simulated time
        #   ran on at 288x. Fourteen real seconds of stall is over an hour of
        #   simulated time. This is normal on a loaded laptop and must NOT be fatal:
        #   the vehicle resynchronises to the present and carries on.
        #
        #   a genuine BUG - a state that never schedules its own end. That loops
        #   forever and must be caught loudly.
        #
        # They are told apart by the size of the gap, not by the transition count.
        # Raising on the first case crashed the producer roughly hourly under load;
        # Docker's restart policy hid it behind healthy-looking throughput.
        if self.state_until_sim is not None:
            behind_minutes = (now - self.state_until_sim).total_seconds() / 60
            if behind_minutes > RESYNC_THRESHOLD_SIM_MINUTES:
                self.resync(now, behind_minutes)
                return

        guard = 0
        while self.state_until_sim is not None and now >= self.state_until_sim:
            self._transition(self.state_until_sim)
            guard += 1
            if guard > MAX_CATCHUP_TRANSITIONS:
                # The gap was small but we still cannot advance - that is a state
                # machine that is not scheduling its own transitions.
                raise RuntimeError(
                    f"{self.vehicle_id}: {guard} transitions without advancing past "
                    f"{self.state_until_sim} - a state is not scheduling its end"
                )
        self._roll_day(now)
        self._update_position(now)

    # -- transitions -------------------------------------------------------

    def _transition(self, at: datetime) -> None:
        match self.status:
            case Status.IDLE:
                self._begin_enroute(at)
            case Status.ENROUTE:
                self._begin_trip(at)
            case Status.ON_TRIP:
                self._complete_trip(at)

    def _enter_idle(self, at: datetime) -> None:
        self.status = Status.IDLE
        self.trip = None
        self.speed_kmh = 0.0
        self.state_since_sim = at

        # Scripted narrative: hold this vehicle idle across a fixed simulated window
        # so the prolonged-idle alert fires on cue during the demo.
        if (
            self.forced_idle_from_sim
            and self.forced_idle_until_sim
            and self.forced_idle_from_sim <= at < self.forced_idle_until_sim
        ):
            self.state_until_sim = self.forced_idle_until_sim
            return

        zone = self.current_zone() or BY_ID[self.home_zone_id]
        hour = at.hour + at.minute / 60
        pressure = (
            industrial_multiplier(hour)
            if zone.zone_class is ZoneClass.INDUSTRIAL
            else demand_multiplier(hour)
        )
        # Busier zone + busier hour => shorter wait. Lognormal gives a realistic
        # long tail: most waits are short, a few are very long.
        mean_wait = 8.0 / max(zone.demand_weight * pressure, 0.08)
        dwell = min(self.rng.lognormvariate(math.log(mean_wait), 0.6), 240.0)
        self.state_until_sim = at + timedelta(minutes=dwell)

    def _begin_enroute(self, at: datetime) -> None:
        """Accept a booking and head for the pickup point.

        The trip is created HERE, not at pickup. A driver who is enroute has already
        been assigned a specific booking, so `trip_id` exists from this moment. That
        is what makes the downstream invariant hold - `trip_id` is null **iff** the
        vehicle is idle - which the stream validator relies on
        (rejection reason STATUS_TRIP_MISMATCH, plan/05 section 3.1).
        """
        pickup = self._pick_pickup_point()
        dest_zone = self._pick_destination_zone()
        destination = self._jittered(*dest_zone.centre)
        is_airport = dest_zone.zone_class is ZoneClass.AIRPORT
        origin_zone = self.current_zone() or BY_ID[self.home_zone_id]

        self.trip = Trip(
            trip_id=f"T{uuid.UUID(int=self.rng.getrandbits(128)).hex[:10]}",
            origin=pickup,
            destination=destination,
            # start_sim is set at pickup - the meter does not run while enroute.
            start_sim=at,
            duration_sim_minutes=self.rng.uniform(25, 50)
            if is_airport
            else self.rng.uniform(8, 35),
            straight_km=haversine_km(*pickup, *destination),
            is_airport=is_airport,
            surge=surge_multiplier(at.hour + at.minute / 60, str(origin_zone.zone_class)),
            dest_zone_id=dest_zone.zone_id,
        )

        # A low-demand zone means the nearest waiting passenger is further away, so
        # the unpaid leg is longer. This is what separates a CBD vehicle from an
        # outskirts one economically.
        reach = max(origin_zone.demand_weight, 0.3)
        enroute_minutes = self.rng.uniform(2, 8) / reach
        # The straight-line distance the vehicle will ACTUALLY travel to the
        # pickup. It must match what the emitted GPS track implies, or the
        # simulator's own accounting and the batch layer's independent measurement
        # disagree - which is precisely the discrepancy that exposed this bug.
        self._deadhead_leg_km = haversine_km(self.lat, self.lon, *pickup)

        self._enroute_origin = (self.lat, self.lon)
        self.status = Status.ENROUTE
        self.state_since_sim = at
        self.state_until_sim = at + timedelta(minutes=enroute_minutes)
        self._pending_pickup = pickup

    def _begin_trip(self, at: datetime) -> None:
        """Passenger picked up. The meter starts now."""
        if self.trip is None:  # defensive: ENROUTE always creates a trip
            self._enter_idle(at)
            return
        self.lat, self.lon = self._pending_pickup or (self.lat, self.lon)
        self.day_deadhead_km += self._deadhead_leg_km
        self._deadhead_leg_km = 0.0
        self.trip.start_sim = at
        self.status = Status.ON_TRIP
        self.state_since_sim = at
        self.state_until_sim = at + timedelta(minutes=self.trip.duration_sim_minutes)

    def _complete_trip(self, at: datetime) -> None:
        if self.trip is not None:
            t = self.trip
            self.lat, self.lon = t.destination
            self.day_paid_km += t.straight_km
            self.day_revenue += self.current_fare() or 0.0
            self.day_trips += 1
        self._enter_idle(at)

    # -- position and derived values --------------------------------------

    def _update_position(self, now: datetime) -> None:
        if self.status is Status.ON_TRIP and self.trip and self.state_since_sim:
            elapsed = (now - self.state_since_sim).total_seconds() / 60
            fraction = min(elapsed / max(self.trip.duration_sim_minutes, 1e-6), 1.0)
            lat, lon = interpolate(*self.trip.origin, *self.trip.destination, fraction)
            self.lat, self.lon = self._jittered(lat, lon)
            zone = self.current_zone()
            congestion = zone.congestion if zone else 0.4
            # Speed is derived from congestion, and position is derived from the
            # trip's schedule, so reported speed and covered distance stay roughly
            # consistent - the batch layer recomputes distance from positions and
            # would otherwise disagree with itself.
            mean = FREE_FLOW_SPEED_KMH - 20 * congestion
            self.speed_kmh = round(min(max(self.rng.gauss(mean, 8), 0.0), MAX_SPEED_KMH), 1)
        elif self.status is Status.ENROUTE and self._enroute_origin and self.state_since_sim:
            # Move toward the pickup, do not merely claim a speed.
            #
            # This originally set speed_kmh and left the position untouched, so a
            # vehicle reported "30 km/h" while its GPS never moved. The batch layer
            # recomputes distance from consecutive positions and so measured ~8%
            # deadhead for every vehicle, against the simulator's own 22-45%. Two
            # independent measurements of one quantity disagreeing is exactly what a
            # reconciliation pipeline exists to surface - here it caught a bug in
            # the simulator rather than in the partner's data.
            leg_minutes = (
                max((self.state_until_sim - self.state_since_sim).total_seconds() / 60, 1e-6)
                if self.state_until_sim
                else 1.0
            )
            elapsed = (now - self.state_since_sim).total_seconds() / 60
            fraction = min(elapsed / leg_minutes, 1.0)
            target = self._pending_pickup or self._enroute_origin
            lat, lon = interpolate(*self._enroute_origin, *target, fraction)
            self.lat, self.lon = self._jittered(lat, lon)
            self.speed_kmh = round(min(max(self.rng.gauss(30, 8), 0.0), MAX_SPEED_KMH), 1)
        else:
            self.speed_kmh = 0.0

    def current_zone(self) -> Zone | None:
        from fleet.common.zones import zone_for

        return zone_for(self.lat, self.lon)

    def current_fare(self) -> float | None:
        """Fare accrued so far on the current trip. None when not on a trip.

        NOTE: this is the *accumulated* total, not an increment. Summing it across a
        window double-counts - the trap documented in plan/05 section 3.4.
        """
        if self.status is not Status.ON_TRIP or self.trip is None:
            return None
        t = self.trip
        return compute_fare(t.straight_km, t.duration_sim_minutes, t.surge, t.is_airport).total

    def idle_minutes(self, now: datetime) -> float:
        if self.status is not Status.IDLE or self.state_since_sim is None:
            return 0.0
        return (now - self.state_since_sim).total_seconds() / 60

    # -- helpers -----------------------------------------------------------

    def _jittered(self, lat: float, lon: float) -> tuple[float, float]:
        return (
            lat + self.rng.gauss(0, GPS_JITTER_DEG),
            lon + self.rng.gauss(0, GPS_JITTER_DEG),
        )

    def _pick_pickup_point(self) -> tuple[float, float]:
        """Where the next passenger is waiting.

        In a low-demand zone the nearest waiting passenger is further away, so the
        unpaid leg is longer. This is the economic mechanism separating a CBD
        vehicle from an outskirts one, and it must be expressed in the POSITION -
        the batch layer only ever sees GPS, never an internal counter.
        """
        zone = self.current_zone() or BY_ID[self.home_zone_id]
        spread = 0.004 / max(zone.demand_weight, 0.3)
        return (
            self.lat + self.rng.gauss(0, spread),
            self.lon + self.rng.gauss(0, spread),
        )

    def _pick_destination_zone(self) -> Zone:
        """Destination weighted by zone demand, with a scripted override.

        V113 is pinned to outskirt zones so it reliably accumulates distance without
        fares and becomes the top unprofitable vehicle in the daily report.
        """
        if self.prefers_zone_class is not None and self.rng.random() < 0.85:
            candidates = [z for z in ZONES if z.zone_class is self.prefers_zone_class]
            if candidates:
                return self.rng.choice(candidates)
        weights = [z.demand_weight for z in ZONES]
        return self.rng.choices(ZONES, weights=weights, k=1)[0]

    def _roll_day(self, now: datetime) -> None:
        day = now.toordinal()
        if self._current_day != day:
            self._current_day = day
            self.day_paid_km = 0.0
            self.day_deadhead_km = 0.0
            self.day_revenue = 0.0
            self.day_trips = 0
