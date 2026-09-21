"""Avro contract tests.

The registry enforces compatibility at deploy time; these catch a broken schema at
commit time, which is cheaper.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import fastavro
import pytest

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "src" / "fleet" / "common" / "schemas"
ALL_SCHEMAS = sorted(SCHEMA_DIR.glob("*.avsc"))


def load(name: str) -> dict:
    return json.loads((SCHEMA_DIR / name).read_text())


@pytest.mark.parametrize("path", ALL_SCHEMAS, ids=lambda p: p.name)
def test_every_schema_parses(path):
    fastavro.parse_schema(json.loads(path.read_text()))


@pytest.mark.parametrize("path", ALL_SCHEMAS, ids=lambda p: p.name)
def test_every_schema_is_documented(path):
    """A schema is a contract with future maintainers; an undocumented one is a
    contract nobody can read."""
    schema = json.loads(path.read_text())
    assert schema.get("doc"), f"{path.name} has no record-level doc"


def roundtrip(schema: dict, record: dict) -> dict:
    parsed = fastavro.parse_schema(schema)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    buf.seek(0)
    return fastavro.schemaless_reader(buf, parsed)


class TestTelemetry:
    def sample(self, **over) -> dict:
        now_ms = int(datetime(2026, 3, 1, 8, 30, tzinfo=UTC).timestamp() * 1000)
        rec = {
            "event_id": "8f14e45f-ea8a-4c2b-9f1e-1a2b3c4d5e6f",
            "vehicle_id": "V007",
            "driver_id": "D007",
            "trip_id": "T00042",
            "lat": 6.9271,
            "lon": 79.8612,
            "speed_kmh": 42.5,
            "status": "on_trip",
            "fare": 350.0,
            "event_time": now_ms,
            "ingest_time": now_ms,
            "producer_id": "sim-1",
            "schema_version": 1,
        }
        rec.update(over)
        return rec

    def test_round_trips(self):
        schema = load("telemetry.v1.avsc")
        assert roundtrip(schema, self.sample())["vehicle_id"] == "V007"

    def test_idle_event_with_null_trip_and_fare(self):
        """status=idle carries no trip_id and no fare - the validator enforces
        'trip_id is null iff idle', so the schema must permit it."""
        schema = load("telemetry.v1.avsc")
        out = roundtrip(schema, self.sample(status="idle", trip_id=None, fare=None))
        assert out["trip_id"] is None and out["fare"] is None

    def test_null_coordinates_are_representable(self):
        """A dropped GPS fix must survive serialisation so it can reach the DLQ
        with a reason code, rather than failing at the producer."""
        schema = load("telemetry.v1.avsc")
        out = roundtrip(schema, self.sample(lat=None, lon=None))
        assert out["lat"] is None

    def test_status_enum_is_closed(self):
        schema = load("telemetry.v1.avsc")
        with pytest.raises(ValueError):
            roundtrip(schema, self.sample(status="teleporting"))

    def test_carries_both_time_bases(self):
        """event_time is simulated (windowing); ingest_time is real (latency).
        Losing either makes a whole class of question unanswerable."""
        fields = {f["name"] for f in load("telemetry.v1.avsc")["fields"]}
        assert {"event_time", "ingest_time"} <= fields


class TestRegistry:
    def test_round_trips(self):
        schema = load("vehicle_registry.v1.avsc")
        rec = {
            "vehicle_id": "V007",
            "model": "Prius",
            "fuel_type": "hybrid",
            "capacity": 4,
            "home_zone": "Z04",
            "driver_id": "D007",
            "onboarded_sim_date": 20150,
            "updated_at": 1772323800000,
        }
        assert roundtrip(schema, rec)["fuel_type"] == "hybrid"

    def test_fuel_type_drives_the_cost_model(self):
        symbols = next(
            f["type"]["symbols"]
            for f in load("vehicle_registry.v1.avsc")["fields"]
            if f["name"] == "fuel_type"
        )
        assert symbols == ["petrol", "hybrid", "electric"]


class TestDlq:
    def test_carries_partition_and_offset(self):
        """Without these a dead-lettered event cannot be traced back to its exact
        position in the source log - see plan/04 §6."""
        fields = {f["name"] for f in load("dlq.v1.avsc")["fields"]}
        assert {"source_topic", "source_partition", "source_offset"} <= fields

    def test_round_trips(self):
        schema = load("dlq.v1.avsc")
        rec = {
            "original_payload": b"\x00\x01\x02",
            "rejection_reason": "IMPLAUSIBLE_SPEED",
            "rejection_detail": "speed_kmh=247.3 exceeds max 200",
            "validator": "speed_range_check",
            "vehicle_id": "V042",
            "rejected_at_sim": 1772323800000,
            "rejected_at_real": 1754331727412,
            "source_topic": "fleet.telemetry.v1",
            "source_partition": 3,
            "source_offset": 184722,
            "trace_id": None,
        }
        assert roundtrip(schema, rec)["source_offset"] == 184722


class TestAlerts:
    def test_records_which_lambda_layer_raised_it(self):
        """Idle alerts come from the speed layer, profitability from the batch
        layer. Under Lambda, provenance is part of the contract."""
        symbols = next(
            f["type"]["symbols"]
            for f in load("alert.v1.avsc")["fields"]
            if f["name"] == "source_layer"
        )
        assert symbols == ["speed", "batch"]
