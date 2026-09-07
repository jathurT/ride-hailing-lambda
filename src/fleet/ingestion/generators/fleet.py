"""Build the simulated fleet, including the scripted demo narratives.

The fleet is generated deterministically from a seed rather than checked in as a
fixture, so `FLEET_SIZE` is a real knob. The scripted vehicles are pinned by id so
the demo and the report screenshots always show the same characters.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from random import Random

from fleet.common.zones import ZONES, ZoneClass
from fleet.ingestion.generators.vehicle import Vehicle

MODELS_BY_FUEL: dict[str, tuple[str, ...]] = {
    "petrol": ("Corolla", "Vitz", "Wagon-R", "Alto"),
    "hybrid": ("Prius", "Aqua", "Corolla"),
    "electric": ("Leaf", "Dolphin"),
}
FUEL_MIX: tuple[tuple[str, float], ...] = (("petrol", 0.60), ("hybrid", 0.30), ("electric", 0.10))

# --- scripted narratives (plan/03 section 4.4) -----------------------------
# Both are documented in the README and the demo script so a reviewer knows what
# to watch for. Seeded, so they fire at the same simulated moment every run.

IDLE_NARRATIVE_VEHICLE = "V007"
"""Held idle for 90 simulated minutes from simulated day 1 at 10:00, so the
prolonged-idle alert (threshold 45 sim-min) fires on cue during the demo."""
IDLE_NARRATIVE_DAY = 1
IDLE_NARRATIVE_HOUR = 10
IDLE_NARRATIVE_DURATION_MIN = 90

UNPROFITABLE_NARRATIVE_VEHICLE = "V113"
"""Pinned to outskirt zones. Low demand means long unpaid legs to reach a pickup,
so its deadhead ratio runs ~45% against a fleet norm of ~22%: it burns fuel without
earning, and reliably lands at the top of the daily report's unprofitable list."""


def build_fleet(
    size: int, seed: int, epoch_sim: datetime, start_at: datetime | None = None
) -> list[Vehicle]:
    """Create `size` vehicles, deterministically.

    Args:
        epoch_sim: when the simulation began. Scripted narratives are scheduled
            relative to this, so they fire on the same simulated day every run.
        start_at: the simulated time to place vehicles at. Defaults to `epoch_sim`.

    The two are separate because a producer that restarts mid-run must not replay
    the whole simulation to catch up: it places its vehicles at the *current*
    simulated time while the narratives stay pinned to their scheduled day.
    """
    rng = Random(seed)
    fuels = [f for f, _ in FUEL_MIX]
    weights = [w for _, w in FUEL_MIX]
    zone_ids = [z.zone_id for z in ZONES]

    idle_from = epoch_sim + timedelta(days=IDLE_NARRATIVE_DAY, hours=IDLE_NARRATIVE_HOUR)

    vehicles: list[Vehicle] = []
    for i in range(1, size + 1):
        vid = f"V{i:03d}"
        fuel = rng.choices(fuels, weights=weights, k=1)[0]
        v = Vehicle(
            vehicle_id=vid,
            driver_id=f"D{i:03d}",
            home_zone_id=rng.choice(zone_ids),
            fuel_type=fuel,
            model=rng.choice(MODELS_BY_FUEL[fuel]),
            capacity=rng.choice((4, 4, 4, 6)),
            # Each vehicle gets its own RNG stream so adding or removing a vehicle
            # does not perturb every other vehicle's behaviour - without this,
            # changing FLEET_SIZE would silently invalidate every screenshot.
            rng=Random(seed * 1_000_003 + i),
        )

        if vid == IDLE_NARRATIVE_VEHICLE:
            v.forced_idle_from_sim = idle_from
            v.forced_idle_until_sim = idle_from + timedelta(minutes=IDLE_NARRATIVE_DURATION_MIN)
        if vid == UNPROFITABLE_NARRATIVE_VEHICLE:
            v.prefers_zone_class = ZoneClass.OUTSKIRTS
            v.home_zone_id = "Z11"

        v.place_at_home(start_at or epoch_sim)
        vehicles.append(v)

    return vehicles


def registry_records(vehicles: list[Vehicle], onboarded: date) -> list[dict[str, object]]:
    """Reference records for the log-compacted registry topic."""
    epoch_day = date(1970, 1, 1)
    return [
        {
            "vehicle_id": v.vehicle_id,
            "model": v.model,
            "fuel_type": v.fuel_type,
            "capacity": v.capacity,
            "home_zone": v.home_zone_id,
            "driver_id": v.driver_id,
            "onboarded_sim_date": (onboarded - epoch_day).days,
            "updated_at": int(datetime.now(UTC).timestamp() * 1000),
        }
        for v in vehicles
    ]
