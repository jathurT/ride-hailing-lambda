"""Diurnal demand.

Uniform-random trip starts would make the "earnings by time-of-day" panel a flat
line and the dashboard worthless. Real ride-hailing demand has two peaks, so the
model is a sum of two Gaussians over a floor.

The shape matters for the report: the twin-peak curve is visible evidence that the
simulated clock is genuinely driving the physiology of the simulation, rather than
events being sprayed at a constant rate.
"""

from __future__ import annotations

import math

MORNING_PEAK_HOUR, MORNING_SIGMA, MORNING_AMPLITUDE = 8.0, 1.2, 1.0
EVENING_PEAK_HOUR, EVENING_SIGMA, EVENING_AMPLITUDE = 18.0, 1.5, 1.2
NIGHT_FLOOR = 0.15

# Shift-change spikes for industrial zones, in simulated hours.
SHIFT_CHANGES = (6.0, 14.0, 22.0)
SHIFT_SIGMA, SHIFT_AMPLITUDE = 0.5, 0.9


def _gaussian(x: float, mu: float, sigma: float) -> float:
    return math.exp(-((x - mu) ** 2) / (2 * sigma**2))


def demand_multiplier(sim_hour: float) -> float:
    """Relative demand at a simulated hour of day (0.0-23.99).

    Returns roughly 0.15 at 03:00 and ~1.35 at the evening peak.
    """
    h = sim_hour % 24.0
    return (
        NIGHT_FLOOR
        + MORNING_AMPLITUDE * _gaussian(h, MORNING_PEAK_HOUR, MORNING_SIGMA)
        + EVENING_AMPLITUDE * _gaussian(h, EVENING_PEAK_HOUR, EVENING_SIGMA)
    )


def industrial_multiplier(sim_hour: float) -> float:
    """Industrial zones spike at shift changes rather than following commuter peaks."""
    h = sim_hour % 24.0
    spikes = sum(_gaussian(h, c, SHIFT_SIGMA) for c in SHIFT_CHANGES)
    return NIGHT_FLOOR + SHIFT_AMPLITUDE * spikes


def surge_multiplier(sim_hour: float, zone_class: str) -> float:
    """Fare surge. Rises to ~1.8x at peak in the CBD, flat at the airport.

    Capped so a single trip can never dominate a day's earnings - an uncapped surge
    makes the profitability figures noisy for no modelling benefit.
    """
    if zone_class == "airport":
        return 1.0
    base = demand_multiplier(sim_hour)
    scale = {"cbd": 0.62, "residential": 0.40, "industrial": 0.25, "outskirts": 0.15}.get(
        zone_class, 0.3
    )
    return round(min(1.0 + scale * base, 1.8), 2)
