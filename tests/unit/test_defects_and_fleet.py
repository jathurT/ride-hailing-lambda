"""Defect injection and fleet construction."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from random import Random

import pytest

from fleet.ingestion.generators.defects import DEFECT_MIX, maybe_corrupt
from fleet.ingestion.generators.fleet import (
    IDLE_NARRATIVE_VEHICLE,
    UNPROFITABLE_NARRATIVE_VEHICLE,
    build_fleet,
    registry_records,
)

EPOCH = datetime(2026, 3, 1, tzinfo=UTC)


def sample_event() -> dict:
    return {
        "event_id": "e1",
        "vehicle_id": "V001",
        "driver_id": "D001",
        "trip_id": "T1",
        "lat": 6.92,
        "lon": 79.86,
        "speed_kmh": 40.0,
        "status": "on_trip",
        "fare": 120.0,
        "event_time": EPOCH,
        "ingest_time": EPOCH,
        "producer_id": "sim-1",
        "schema_version": 1,
    }


class TestDefectInjection:
    def test_zero_rate_never_corrupts(self):
        rng = Random(1)
        assert all(maybe_corrupt(sample_event(), rng, 0.0)[1] is None for _ in range(500))

    def test_rate_is_honoured(self):
        rng = Random(1)
        hits = sum(maybe_corrupt(sample_event(), rng, 0.10)[1] is not None for _ in range(20_000))
        assert 0.09 <= hits / 20_000 <= 0.11

    def test_mix_proportions_hold(self):
        rng = Random(1)
        seen = Counter(
            d
            for _ in range(40_000)
            if (d := maybe_corrupt(sample_event(), rng, 1.0)[1]) is not None
        )
        for name, expected in DEFECT_MIX.items():
            assert seen[name] / sum(seen.values()) == pytest.approx(expected, abs=0.03)

    def test_every_defect_produces_an_invalid_event(self):
        """A defect must be *impossible*, not merely unusual. An abnormal-but-real
        value is a signal the pipeline must keep."""
        rng = Random(3)
        checks = {
            "null_coordinates": lambda e: e["lat"] is None,
            "coordinates_out_of_bounds": lambda e: not (6.85 <= (e["lat"] or 0) < 6.99),
            "negative_fare": lambda e: (e["fare"] or 0) < 0,
            "implausible_speed": lambda e: (e["speed_kmh"] or 0) > 200,
            "future_timestamp": lambda e: e["event_time"] > EPOCH,
        }
        found: set[str] = set()
        for _ in range(5000):
            e, d = maybe_corrupt(sample_event(), rng, 1.0)
            if d:
                assert checks[d](e), f"{d} did not make the event invalid"
                found.add(d)
        assert found == set(DEFECT_MIX)

    def test_original_event_is_not_mutated(self):
        original = sample_event()
        snapshot = dict(original)
        maybe_corrupt(original, Random(1), 1.0)
        assert original == snapshot


class TestFleetConstruction:
    def test_size_is_honoured(self):
        assert len(build_fleet(37, 42, EPOCH)) == 37

    def test_ids_are_stable_and_padded(self):
        f = build_fleet(150, 42, EPOCH)
        assert f[0].vehicle_id == "V001" and f[-1].vehicle_id == "V150"

    def test_deterministic(self):
        a, b = build_fleet(50, 42, EPOCH), build_fleet(50, 42, EPOCH)
        assert [(v.vehicle_id, v.model, v.fuel_type, v.home_zone_id) for v in a] == [
            (v.vehicle_id, v.model, v.fuel_type, v.home_zone_id) for v in b
        ]

    def test_changing_fleet_size_does_not_perturb_existing_vehicles(self):
        """Each vehicle owns its RNG stream. Without this, raising FLEET_SIZE would
        silently change every other vehicle's behaviour and invalidate every
        screenshot already captured."""
        small = {v.vehicle_id: (v.model, v.home_zone_id) for v in build_fleet(150, 42, EPOCH)}
        large = {v.vehicle_id: (v.model, v.home_zone_id) for v in build_fleet(300, 42, EPOCH)}
        assert all(small[k] == large[k] for k in small)

    def test_fuel_mix_is_roughly_as_configured(self):
        mix = Counter(v.fuel_type for v in build_fleet(2000, 42, EPOCH))
        total = sum(mix.values())
        assert mix["petrol"] / total == pytest.approx(0.60, abs=0.05)
        assert mix["electric"] / total == pytest.approx(0.10, abs=0.04)

    def test_model_matches_fuel_type(self):
        from fleet.ingestion.generators.fleet import MODELS_BY_FUEL

        assert all(v.model in MODELS_BY_FUEL[v.fuel_type] for v in build_fleet(300, 42, EPOCH))


class TestScriptedVehiclesArePinned:
    def test_idle_narrative_vehicle_is_scheduled(self):
        v = next(v for v in build_fleet(150, 42, EPOCH) if v.vehicle_id == IDLE_NARRATIVE_VEHICLE)
        assert v.forced_idle_from_sim is not None
        assert (v.forced_idle_until_sim - v.forced_idle_from_sim).total_seconds() / 60 == 90

    def test_unprofitable_narrative_vehicle_is_pinned_to_outskirts(self):
        v = next(
            v for v in build_fleet(150, 42, EPOCH) if v.vehicle_id == UNPROFITABLE_NARRATIVE_VEHICLE
        )
        assert v.home_zone_id == "Z11"
        assert v.prefers_zone_class is not None


class TestRegistryRecords:
    def test_one_record_per_vehicle_with_registry_schema_fields(self):
        recs = registry_records(build_fleet(150, 42, EPOCH), onboarded=EPOCH.date())
        assert len(recs) == 150
        assert {
            "vehicle_id",
            "model",
            "fuel_type",
            "capacity",
            "home_zone",
            "driver_id",
            "onboarded_sim_date",
            "updated_at",
        } == set(recs[0])

    def test_onboarded_date_is_days_since_epoch(self):
        """Avro logicalType `date` is days since 1970-01-01, not a string."""
        recs = registry_records(build_fleet(1, 42, EPOCH), onboarded=datetime(2025, 1, 1).date())
        assert recs[0]["onboarded_sim_date"] == 20089
