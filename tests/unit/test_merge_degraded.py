"""Degradation: one store down is a 200 with a warning, not a 500.

This is the behaviour plan/07 section 5.2 specifies and the one worth showing live.
A dashboard that goes blank when Redis restarts demonstrates nothing; a dashboard
that keeps serving yesterday's exact figures and says "speed view unavailable" is
the architecture earning its complexity.

The stores are faked rather than mocked with `unittest.mock`, so a signature change
in `PostgresReader` or `RedisReader` breaks these tests loudly instead of letting a
`Mock` happily accept the wrong call.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from fleet.serving.merge import all_stores_down, merged_utilization
from fleet.serving.readers import StoreUnavailableError

SIM_NOW = datetime(2026, 3, 8, 14, 30)
HWM = date(2026, 3, 7)


class FakePg:
    """Answers unless `down`, in which case it fails the way the real one does."""

    def __init__(self, down: bool = False, hwm: date | None = HWM) -> None:
        self.down = down
        self._hwm = hwm
        self.zone_daily_calls = 0

    def _guard(self) -> None:
        if self.down:
            raise StoreUnavailableError("postgres", ConnectionError("connection refused"))

    def high_water_mark(self) -> date | None:
        self._guard()
        return self._hwm

    def zone_daily(self, from_date: date, to_date: date) -> list[dict]:
        self._guard()
        self.zone_daily_calls += 1
        return [
            {
                "zone_id": "Z01",
                "sim_date": from_date,
                "trips": 40,
                "earnings": 900.0,
                "active_vehicles": 12,
                "idle_ratio": 0.25,
                "avg_speed_kmh": 31.0,
                "hours_reported": 24,
            }
        ]


class FakeRedis:
    def __init__(self, down: bool = False) -> None:
        self.down = down
        self.zones_calls = 0

    def zones(self) -> list[dict]:
        if self.down:
            raise StoreUnavailableError("redis", ConnectionError("Error 111 connecting"))
        self.zones_calls += 1
        return [
            {
                "zone_id": "Z01",
                "active_vehicles": 11,
                "trips": 6,
                "earnings": 130.0,
                "idle_ratio": 0.3,
                "avg_speed_kmh": 29.0,
                "reporting": True,
            }
        ]


def _merge(pg: FakePg, rd: FakeRedis, frm=date(2026, 3, 6), to=date(2026, 3, 8)):
    return merged_utilization(frm, to, SIM_NOW, pg, rd)  # type: ignore[arg-type]


def test_both_up_serves_both_sides_and_is_not_degraded():
    res = _merge(FakePg(), FakeRedis())
    assert not res.degraded
    assert res.missing_stores == []
    assert {r.source for r in res.rows} == {"batch", "speed"}
    assert res.batch_complete_thru == HWM


def test_redis_down_still_serves_the_batch_half():
    rd = FakeRedis(down=True)
    res = _merge(FakePg(), rd)
    assert res.degraded
    assert res.missing_stores == ["redis"]
    assert res.rows, "batch rows must still be returned"
    assert all(r.source == "batch" for r in res.rows)
    assert "DEGRADED" in res.consistency
    assert not all_stores_down(res)


def test_postgres_down_still_serves_the_speed_half():
    res = _merge(FakePg(down=True), FakeRedis())
    assert res.degraded
    assert res.missing_stores == ["postgres"]
    assert res.rows and all(r.source == "speed" for r in res.rows)
    assert res.batch_complete_thru is None
    assert "watermark unknown" in res.consistency, (
        "an unreachable database must not be reported as a cold start"
    )


def test_both_down_is_the_only_fatal_case():
    res = _merge(FakePg(down=True), FakeRedis(down=True))
    assert res.rows == []
    assert sorted(res.missing_stores) == ["postgres", "redis"]
    assert all_stores_down(res)


def test_a_range_inside_the_batch_view_never_touches_redis():
    """Not an optimisation - it is why a pure-batch query survives Redis being down."""
    rd = FakeRedis(down=True)
    res = merged_utilization(date(2026, 3, 2), date(2026, 3, 4), SIM_NOW, FakePg(), rd)  # type: ignore[arg-type]
    assert not res.degraded, "Redis was down but was never asked, so nothing is degraded"
    assert rd.zones_calls == 0


def test_a_range_after_the_watermark_never_touches_postgres():
    pg = FakePg()
    res = merged_utilization(date(2026, 3, 8), date(2026, 3, 8), SIM_NOW, pg, FakeRedis())  # type: ignore[arg-type]
    assert pg.zone_daily_calls == 0
    assert res.rows and all(r.source == "speed" for r in res.rows)


def test_cold_start_is_not_degraded():
    """No batch data yet is a valid state, not a failure (plan/07 section 5.2)."""
    res = _merge(FakePg(hwm=None), FakeRedis())
    assert not res.degraded
    assert res.batch_complete_thru is None
    assert "no batch data available" in res.consistency


def test_speed_rows_carry_the_approximation_note_and_batch_rows_do_not():
    res = _merge(FakePg(), FakeRedis())
    speed = [r for r in res.rows if r.source == "speed"]
    batch = [r for r in res.rows if r.source == "batch"]
    assert all(r.approximation_note and not r.exact for r in speed)
    assert all(r.approximation_note is None and r.exact for r in batch)


@pytest.mark.parametrize("idle,expected", [(0.25, 75.0), (0.0, 100.0), (1.0, 0.0), (None, None)])
def test_utilization_is_the_complement_of_idle_ratio(idle, expected):
    from fleet.serving.merge import _utilization

    assert _utilization(idle) == expected
