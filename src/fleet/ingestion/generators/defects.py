"""Deliberate data defects.

The PDF asks for *cleaning* as a meaningful transformation, and cleaning cannot be
demonstrated without dirt. Roughly 1% of emitted events are corrupted on a seeded
schedule, each corresponding to a real telematics failure mode and to a specific
rejection reason the stream validator will produce.

`DEFECT_RATE` is runtime-configurable so `make chaos-bad-data` can raise it and fire
the HighDLQRate alert on demand during the demo (plan/09 section 4.3).
"""

from __future__ import annotations

from datetime import timedelta
from random import Random
from typing import Any

# defect name -> share of the total defect budget
DEFECT_MIX: dict[str, float] = {
    "null_coordinates": 0.30,  # dropped GPS fix
    "coordinates_out_of_bounds": 0.20,  # GPS glitch, position outside the city
    "negative_fare": 0.20,  # meter fault
    "implausible_speed": 0.20,  # wheel-sensor fault
    "future_timestamp": 0.10,  # device clock skew
}

_NAMES = list(DEFECT_MIX)
_WEIGHTS = list(DEFECT_MIX.values())


def maybe_corrupt(
    event: dict[str, Any], rng: Random, defect_rate: float
) -> tuple[dict, str | None]:
    """Return (event, defect_name). `defect_name` is None when left intact.

    The event is mutated to be *invalid*, not merely unusual: an abnormal-but-
    possible value is a real signal and must survive to the aggregates. Only
    physically impossible values are injected here.
    """
    if defect_rate <= 0 or rng.random() >= defect_rate:
        return event, None

    defect = rng.choices(_NAMES, weights=_WEIGHTS, k=1)[0]
    e = dict(event)

    match defect:
        case "null_coordinates":
            e["lat"] = None
            e["lon"] = None
        case "coordinates_out_of_bounds":
            e["lat"] = rng.uniform(20.0, 60.0)
            e["lon"] = rng.uniform(-40.0, 10.0)
        case "negative_fare":
            e["fare"] = -abs(rng.uniform(1, 200))
        case "implausible_speed":
            e["speed_kmh"] = rng.uniform(220, 400)
        case "future_timestamp":
            e["event_time"] = e["event_time"] + timedelta(hours=2)

    return e, defect
