"""Fare calculation.

Deliberately simple and fully deterministic given its inputs, so the batch layer's
revenue figure can be checked by hand against a trip's distance and duration - which
matters when the number feeds a profitability report the report claims is *exact*.
"""

from __future__ import annotations

from dataclasses import dataclass

BASE_FARE = 2.50
PER_KM = 0.85
PER_MINUTE = 0.15
AIRPORT_LEVY = 3.00
MINIMUM_FARE = 3.50


@dataclass(frozen=True, slots=True)
class FareBreakdown:
    """Kept as a breakdown rather than a single number so a disputed fare can be
    explained, and so tests assert on components rather than on a total that could
    be right for the wrong reasons."""

    base: float
    distance_component: float
    time_component: float
    levy: float
    surge_multiplier: float
    total: float


def compute_fare(
    distance_km: float,
    duration_sim_minutes: float,
    surge: float = 1.0,
    is_airport: bool = False,
) -> FareBreakdown:
    if distance_km < 0 or duration_sim_minutes < 0:
        raise ValueError("distance and duration must be non-negative")

    distance_component = PER_KM * distance_km
    time_component = PER_MINUTE * duration_sim_minutes
    levy = AIRPORT_LEVY if is_airport else 0.0

    # Surge applies to the metered portion, not to the fixed airport levy - which is
    # how real levies work, and it stops airport trips inflating during peaks.
    metered = (BASE_FARE + distance_component + time_component) * surge
    total = max(round(metered + levy, 2), MINIMUM_FARE)

    return FareBreakdown(
        base=BASE_FARE,
        distance_component=round(distance_component, 2),
        time_component=round(time_component, 2),
        levy=levy,
        surge_multiplier=surge,
        total=total,
    )
