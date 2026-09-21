"""The demand curve and the fare model."""

from __future__ import annotations

import pytest

from fleet.ingestion.generators.demand import (
    demand_multiplier,
    industrial_multiplier,
    surge_multiplier,
)
from fleet.ingestion.generators.fares import (
    AIRPORT_LEVY,
    BASE_FARE,
    MINIMUM_FARE,
    PER_KM,
    compute_fare,
)


class TestDemandCurve:
    def test_morning_and_evening_peaks_exist(self):
        assert demand_multiplier(8) > demand_multiplier(12)
        assert demand_multiplier(18) > demand_multiplier(12)

    def test_evening_peak_is_the_larger(self):
        assert demand_multiplier(18) > demand_multiplier(8)

    def test_night_is_quietest(self):
        assert demand_multiplier(3) < demand_multiplier(8)
        assert demand_multiplier(3) < 0.3

    def test_curve_wraps_around_midnight(self):
        assert demand_multiplier(24.0) == pytest.approx(demand_multiplier(0.0))
        assert demand_multiplier(25.0) == pytest.approx(demand_multiplier(1.0))

    def test_always_positive(self):
        assert all(demand_multiplier(h / 4) > 0 for h in range(96))

    def test_industrial_peaks_at_shift_changes_not_commute(self):
        """Industrial zones must not simply mirror the commuter curve, or the zone
        breakdown carries no information."""
        assert industrial_multiplier(6) > industrial_multiplier(8)
        assert industrial_multiplier(22) > industrial_multiplier(18)


class TestSurge:
    def test_cbd_surges_at_peak(self):
        assert surge_multiplier(8, "cbd") > surge_multiplier(3, "cbd")

    def test_airport_never_surges(self):
        assert all(surge_multiplier(h, "airport") == 1.0 for h in range(24))

    def test_capped(self):
        assert all(surge_multiplier(h / 4, "cbd") <= 1.8 for h in range(96))

    def test_never_below_one(self):
        assert all(
            surge_multiplier(h / 4, c) >= 1.0
            for h in range(96)
            for c in ("cbd", "residential", "outskirts")
        )

    def test_cbd_surges_harder_than_outskirts(self):
        assert surge_multiplier(18, "cbd") > surge_multiplier(18, "outskirts")


class TestFares:
    def test_components_are_additive(self):
        f = compute_fare(10.0, 20.0, surge=1.0)
        assert f.distance_component == pytest.approx(PER_KM * 10.0)
        assert f.total == pytest.approx(
            BASE_FARE + f.distance_component + f.time_component, abs=0.01
        )

    def test_surge_scales_the_metered_portion(self):
        a, b = compute_fare(10.0, 20.0, surge=1.0), compute_fare(10.0, 20.0, surge=2.0)
        assert b.total == pytest.approx(a.total * 2, abs=0.02)

    def test_airport_levy_is_not_surged(self):
        """Real levies are fixed. Surging them would let airport trips distort the
        profitability figures during peaks."""
        plain = compute_fare(10.0, 20.0, surge=2.0, is_airport=False)
        airport = compute_fare(10.0, 20.0, surge=2.0, is_airport=True)
        assert airport.total - plain.total == pytest.approx(AIRPORT_LEVY, abs=0.01)

    def test_minimum_fare_applies(self):
        assert compute_fare(0.1, 0.5).total == MINIMUM_FARE

    def test_longer_trips_cost_more(self):
        assert compute_fare(20.0, 30.0).total > compute_fare(5.0, 30.0).total

    @pytest.mark.parametrize("km,mins", [(-1.0, 10.0), (5.0, -1.0)])
    def test_negative_inputs_rejected(self, km, mins):
        with pytest.raises(ValueError, match="non-negative"):
            compute_fare(km, mins)
