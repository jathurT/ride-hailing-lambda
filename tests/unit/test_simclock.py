"""The simulated clock. Pure functions, no I/O, so no Docker and no fixtures needed."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from fleet.common.simclock import (
    SECONDS_PER_DAY,
    SimClock,
    anchor_exists,
    read_anchor,
    write_anchor,
)

EPOCH_WALL = datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC)
EPOCH_SIM = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)


def clock(day_seconds: int = 300) -> SimClock:
    return SimClock(epoch_wall=EPOCH_WALL, epoch_sim=EPOCH_SIM, day_seconds=day_seconds)


class TestSpeedup:
    def test_project_default_is_288x(self):
        assert clock(300).speedup == 288.0

    def test_one_real_second_is_4_point_8_sim_minutes(self):
        assert clock(300).real_to_sim(1) == pytest.approx(288.0)
        assert clock(300).real_to_sim(1) / 60 == pytest.approx(4.8)

    @pytest.mark.parametrize("day_seconds", [60, 300, 900, 3600, SECONDS_PER_DAY])
    def test_conversions_round_trip(self, day_seconds):
        c = clock(day_seconds)
        for seconds in (0.0, 1.0, 42.5, 3600.0):
            assert c.sim_to_real(c.real_to_sim(seconds)) == pytest.approx(seconds)

    def test_no_compression_at_86400(self):
        assert clock(SECONDS_PER_DAY).speedup == 1.0


class TestSimNow:
    def test_at_epoch_sim_now_is_epoch_sim(self):
        assert clock().sim_now(now=EPOCH_WALL) == EPOCH_SIM

    def test_one_sim_day_elapses_per_300_real_seconds(self):
        c = clock(300)
        after = c.sim_now(now=EPOCH_WALL + timedelta(seconds=300))
        assert after - EPOCH_SIM == timedelta(days=1)

    @pytest.mark.parametrize(
        ("real_seconds", "expected_day"),
        [(0, 0), (299, 0), (300, 1), (601, 2), (900, 3)],
    )
    def test_sim_day_index_advances_once_per_300_real_seconds(self, real_seconds, expected_day):
        c = clock(300)
        assert c.sim_day_index(now=EPOCH_WALL + timedelta(seconds=real_seconds)) == expected_day

    def test_sim_date_is_the_parquet_partition_key(self):
        c = clock(300)
        assert c.sim_date(now=EPOCH_WALL + timedelta(seconds=600)).isoformat() == "2026-03-03"

    def test_real_instant_of_inverts_sim_now(self):
        c = clock(300)
        target_sim = EPOCH_SIM + timedelta(days=2, hours=10)
        real = c.real_instant_of(target_sim)
        assert c.sim_now(now=real) == pytest.approx(target_sim, abs=timedelta(milliseconds=1))


class TestWatermarkSafety:
    """The project's subtlest coupling, guarded in CI.

    A watermark is expressed in SIMULATED minutes but has to survive REAL-world
    jitter - a Kafka rebalance, a GC pause. At 288x, 30 simulated minutes buys only
    6.25 real seconds. Halve SIM_DAY_SECONDS without revisiting the watermark and
    events start being dropped silently. This test fails the build first.

    See plan/03 §3.3.
    """

    WATERMARK_SIM_MINUTES = 30
    MIN_REAL_SECONDS = 5.0

    def test_default_watermark_survives_real_jitter(self):
        real = clock(300).sim_to_real(self.WATERMARK_SIM_MINUTES * 60)
        assert real == pytest.approx(6.25)
        assert real >= self.MIN_REAL_SECONDS

    def test_the_guard_would_catch_a_faster_clock(self):
        """Documents the failure mode rather than only the happy path."""
        real = clock(150).sim_to_real(self.WATERMARK_SIM_MINUTES * 60)
        assert real == pytest.approx(3.125)
        assert real < self.MIN_REAL_SECONDS


class TestAnchor:
    def test_write_then_read_round_trips(self, tmp_path):
        path = tmp_path / "sim_epoch.json"
        written = write_anchor(EPOCH_SIM, 300, path=path, now=EPOCH_WALL)
        assert anchor_exists(path)
        loaded = read_anchor(path)
        assert loaded == written

    def test_anchor_is_readable_json(self, tmp_path):
        path = tmp_path / "sim_epoch.json"
        write_anchor(EPOCH_SIM, 300, path=path, now=EPOCH_WALL)
        assert set(json.loads(path.read_text())) == {"epoch_wall", "epoch_sim", "day_seconds"}

    def test_missing_anchor_says_what_to_do(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="init container"):
            read_anchor(tmp_path / "nope.json")

    def test_two_processes_loading_the_same_anchor_do_not_drift(self, tmp_path):
        """The whole point of a shared anchor."""
        path = tmp_path / "sim_epoch.json"
        write_anchor(EPOCH_SIM, 300, path=path, now=EPOCH_WALL)
        assert read_anchor(path).drift_seconds(read_anchor(path)) == 0.0


class TestValidation:
    def test_naive_datetimes_rejected(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            SimClock(datetime(2026, 8, 4), EPOCH_SIM, 300)

    @pytest.mark.parametrize("bad", [0, -1])
    def test_non_positive_day_seconds_rejected(self, bad):
        with pytest.raises(ValueError, match="positive"):
            SimClock(EPOCH_WALL, EPOCH_SIM, bad)
