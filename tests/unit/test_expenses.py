"""The daily-batch source."""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from random import Random

import pytest

from fleet.common.simclock import SimClock
from fleet.ingestion.expense_dropper import (
    FIELDNAMES,
    FUEL_RATE_PER_KM,
    ExpenseDropper,
    build_rows,
    to_csv,
)

NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)
CLOCK = SimClock(epoch_wall=NOW, epoch_sim=datetime(2026, 3, 1, tzinfo=UTC), day_seconds=300)


def distances(n: int = 50, fuel: str = "petrol") -> dict[str, tuple[str, float]]:
    return {f"V{i:03d}": (fuel, 100.0 + i) for i in range(1, n + 1)}


class TestRowGeneration:
    def test_one_row_per_active_vehicle(self):
        assert len(build_rows(distances(50), Random(1), NOW)) == 50

    def test_vehicles_that_did_not_move_are_omitted(self):
        """A vehicle with no distance has no fuel bill; inventing a row for it would
        create a phantom cost in the profitability report."""
        d = distances(10)
        d["V003"] = ("petrol", 0.0)
        assert all(r.vehicle_id != "V003" for r in build_rows(d, Random(1), NOW))

    def test_costs_are_never_negative(self):
        rows = build_rows(distances(200), Random(9), NOW)
        assert all(r.fuel_cost >= 0 and r.maintenance_cost >= 0 for r in rows)

    @pytest.mark.parametrize("fuel", ["petrol", "hybrid", "electric"])
    def test_fuel_cost_recovers_the_configured_rate(self, fuel):
        rows = build_rows(distances(400, fuel), Random(3), NOW)
        mean = sum(r.fuel_cost / r.distance_covered for r in rows) / len(rows)
        assert mean == pytest.approx(FUEL_RATE_PER_KM[fuel], rel=0.05)

    def test_electric_is_cheaper_to_run_than_petrol(self):
        """Fuel type must visibly matter, or the daily report's breakdown says
        nothing."""
        e = build_rows(distances(300, "electric"), Random(4), NOW)
        p = build_rows(distances(300, "petrol"), Random(4), NOW)
        assert sum(r.fuel_cost for r in e) < sum(r.fuel_cost for r in p) / 2

    def test_partner_distance_disagrees_with_ours_by_about_three_percent(self):
        """Two independent measurements of one journey. Surfacing the disagreement
        is the batch layer's job; hiding it by picking one would defeat the
        reconciliation (plan/06 §2.3)."""
        d = distances(500)
        rows = build_rows(d, Random(7), NOW)
        deltas = [abs(r.distance_covered - d[r.vehicle_id][1]) / d[r.vehicle_id][1] for r in rows]
        mean_abs = sum(deltas) / len(deltas)
        assert 0.01 < mean_abs < 0.05
        assert max(deltas) < 0.20

    def test_service_events_are_occasional_and_expensive(self):
        rows = build_rows(distances(2000), Random(11), NOW)
        serviced = [r for r in rows if r.service_flag]
        assert 0.02 <= len(serviced) / len(rows) <= 0.07
        assert all(150 <= r.maintenance_cost <= 600 for r in serviced)
        assert all(r.maintenance_cost <= 15 for r in rows if not r.service_flag)

    def test_restatement_reduces_the_fuel_figure(self):
        """The scripted correction models a mis-assigned fuel card, so the restated
        total must be lower - that visible drop is the demo's payoff."""
        d = distances(300)
        original = sum(r.fuel_cost for r in build_rows(d, Random(5), NOW))
        restated = sum(r.fuel_cost for r in build_rows(d, Random(5), NOW, restatement=True))
        assert restated < original


class TestCsvRendering:
    def test_header_matches_the_declared_contract(self):
        rows = build_rows(distances(3), Random(1), NOW)
        assert next(csv.reader(io.StringIO(to_csv(rows)))) == FIELDNAMES

    def test_round_trips_through_csv(self):
        rows = build_rows(distances(20), Random(1), NOW)
        parsed = list(csv.DictReader(io.StringIO(to_csv(rows))))
        assert len(parsed) == 20
        assert parsed[0]["vehicle_id"] == rows[0].vehicle_id
        assert float(parsed[0]["fuel_cost"]) == pytest.approx(rows[0].fuel_cost)

    def test_booleans_render_lowercase_for_downstream_parsing(self):
        rows = build_rows(distances(50), Random(2), NOW)
        assert {r["service_flag"] for r in csv.DictReader(io.StringIO(to_csv(rows)))} <= {
            "true",
            "false",
        }

    def test_drift_renames_a_column(self):
        """Exercises the DAG's validation-failure branch."""
        rows = build_rows(distances(3), Random(1), NOW)
        header = next(csv.reader(io.StringIO(to_csv(rows, drift=True))))
        assert "distance_km" in header and "distance_covered" not in header


class TestAtomicDrop:
    def test_final_file_appears_and_staging_is_left_empty(self, tmp_path):
        """The sensor downstream must never observe a partial write - a real and
        very common bug."""
        dropper = ExpenseDropper(CLOCK, tmp_path / "expenses")
        rows = build_rows(distances(20), Random(1), NOW)
        path = dropper.drop(NOW.date(), rows)

        assert path.exists()
        assert path.name == f"expenses_{NOW.date().isoformat()}.csv"
        assert list((tmp_path / "expenses" / ".tmp").iterdir()) == []

    def test_dropped_file_is_complete_and_parseable(self, tmp_path):
        dropper = ExpenseDropper(CLOCK, tmp_path / "expenses")
        rows = build_rows(distances(40), Random(1), NOW)
        path = dropper.drop(NOW.date(), rows)
        assert len(list(csv.DictReader(path.open()))) == 40

    def test_redropping_the_same_day_overwrites_cleanly(self, tmp_path):
        """A restatement reuses the same key; downstream upserts handle the rest."""
        dropper = ExpenseDropper(CLOCK, tmp_path / "expenses")
        dropper.drop(NOW.date(), build_rows(distances(10), Random(1), NOW))
        p = dropper.drop(NOW.date(), build_rows(distances(20), Random(1), NOW))
        assert len(list(csv.DictReader(p.open()))) == 20
        assert len(list((tmp_path / "expenses").glob("*.csv"))) == 1
