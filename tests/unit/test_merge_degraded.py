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


# --- instrumentation -------------------------------------------------------
#
# A dashboard panel is only as honest as the counter behind it. These assert the
# serving metrics move when the thing they describe actually happens - otherwise a
# flat line on the Grafana SERVE column means nothing at all.


def _counter_value(counter, **labels):
    c = counter.labels(**labels) if labels else counter
    return c._value.get()


def test_boundary_crossing_is_counted_only_when_both_views_contribute():
    from fleet.common import metrics

    before = _counter_value(metrics.serving_merge_boundary_crossings_total)
    # Wholly inside the batch view - no crossing.
    merged_utilization(date(2026, 3, 2), date(2026, 3, 4), SIM_NOW, FakePg(), FakeRedis())  # type: ignore[arg-type]
    assert _counter_value(metrics.serving_merge_boundary_crossings_total) == before

    # Spans the watermark - this is the reconciliation actually happening.
    _merge(FakePg(), FakeRedis())
    assert _counter_value(metrics.serving_merge_boundary_crossings_total) == before + 1


def test_watermark_age_gauge_tracks_how_far_behind_the_batch_layer_is():
    from fleet.common import metrics

    _merge(FakePg(), FakeRedis())
    # SIM_NOW is 2026-03-08, HWM is 2026-03-07.
    assert metrics.serving_batch_watermark_age_sim_days._value.get() == 1


def test_degraded_responses_are_counted_per_missing_store():
    from fleet.common import metrics

    before = _counter_value(metrics.serving_degraded_responses_total, missing_store="redis")
    _merge(FakePg(), FakeRedis(down=True))
    assert (
        _counter_value(metrics.serving_degraded_responses_total, missing_store="redis")
        == before + 1
    )


def test_uncovered_dates_are_counted_so_a_lagging_batch_layer_is_visible():
    from fleet.common import metrics

    before = _counter_value(metrics.serving_uncovered_dates_total)
    # Watermark on 3-02 with today 3-08 leaves 3-03..3-07 in neither view.
    merged_utilization(
        date(2026, 3, 1),
        date(2026, 3, 8),
        SIM_NOW,
        FakePg(hwm=date(2026, 3, 2)),  # type: ignore[arg-type]
        FakeRedis(),  # type: ignore[arg-type]
    )
    assert _counter_value(metrics.serving_uncovered_dates_total) == before + 5


# --- what counts as "unavailable" -----------------------------------------
#
# Degrading is for a store that is DOWN. A programming error must not be dressed up
# as an outage: it sends whoever is debugging to the wrong machine, and it hides a
# fault behind a 503 that looks like expected behaviour.


def test_connection_failures_are_classified_as_unreachable():
    import socket

    from fleet.serving.readers import _is_unreachable

    assert _is_unreachable(ConnectionRefusedError("refused"))
    assert _is_unreachable(TimeoutError("timed out"))
    assert _is_unreachable(socket.gaierror("name resolution"))


def test_programming_errors_are_not_classified_as_unreachable():
    """★ The regression guard.

    A SQL bug - `could not determine data type of parameter $1` - was reported to
    clients as 503 "batch view unavailable: postgres" while Postgres was healthy and
    every other endpoint served from it. The endpoint that answers the assignment's
    headline question was dead and the error blamed the database.
    """
    from fleet.serving.readers import _is_unreachable

    assert not _is_unreachable(ValueError("could not determine data type of parameter $1"))
    assert not _is_unreachable(KeyError("missing column"))
    assert not _is_unreachable(TypeError("bad argument"))


def test_a_programming_error_propagates_instead_of_degrading(monkeypatch):
    """It must reach the caller as itself, not wrapped as StoreUnavailableError."""
    from fleet.serving import readers as readers_mod

    reader = readers_mod.PostgresReader(dsn="postgresql://unused")

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return self

    # Make the pool path unavailable so `_query` takes its documented fallback,
    # then have the connection itself raise a PROGRAMMING error.
    def _no_pool():
        raise ImportError("psycopg_pool not installed")

    monkeypatch.setattr(reader, "_get_pool", _no_pool)

    def _connect(*_a, **_kw):
        raise ValueError("could not determine data type of parameter $1")

    import psycopg

    monkeypatch.setattr(psycopg, "connect", _connect)

    with pytest.raises(ValueError, match="could not determine data type"):
        reader._query(lambda cur: None)


def test_a_connection_failure_still_degrades(monkeypatch):
    """The other half of the contract: a real outage must still become the soft error."""
    from fleet.serving import readers as readers_mod

    reader = readers_mod.PostgresReader(dsn="postgresql://unused")
    monkeypatch.setattr(reader, "_get_pool", lambda: (_ for _ in ()).throw(ImportError("no pool")))

    import psycopg

    def _connect(*_a, **_kw):
        raise ConnectionRefusedError("connection refused")

    monkeypatch.setattr(psycopg, "connect", _connect)

    with pytest.raises(readers_mod.StoreUnavailableError):
        reader._query(lambda cur: None)


def test_the_classifier_matches_driver_exceptions_by_class_name():
    """psycopg and redis are matched by NAME, so this pins that mechanism.

    `_is_unreachable` deliberately avoids importing either driver just to classify.
    That keeps the module importable without them - but it means a driver renaming
    its exception class would silently stop degradation working, so the names are
    asserted here rather than trusted.
    """
    from fleet.serving.readers import _is_unreachable

    class OperationalError(Exception):
        """The psycopg name."""

    class PoolTimeout(Exception):  # noqa: N818 - must match psycopg_pool's real name
        """The psycopg_pool name."""

    class BusyLoadingError(Exception):
        """The redis name."""

    for exc_type in (OperationalError, PoolTimeout, BusyLoadingError):
        assert _is_unreachable(exc_type("down")), f"{exc_type.__name__} must degrade"


def test_latest_ping_per_vehicle_wins_regardless_of_row_order():
    """Backs GET /api/v1/vehicles/{id}.

    A micro-batch holds several pings per vehicle in no guaranteed order. Writing
    them all leaves whichever was written last, which is not the same as the most
    recent - and the symptom is a live map showing a vehicle where it was two
    minutes ago, with nothing reporting a fault.
    """
    from fleet.speed_layer.sinks.redis_sink import latest_per_vehicle

    rows = [
        {"vehicle_id": "V1", "event_time": 300, "status": "on_trip"},
        {"vehicle_id": "V1", "event_time": 100, "status": "idle"},
        {"vehicle_id": "V1", "event_time": 200, "status": "enroute"},
        {"vehicle_id": "V2", "event_time": 50, "status": "idle"},
        {"vehicle_id": None, "event_time": 999, "status": "idle"},
    ]
    newest = latest_per_vehicle(rows)
    assert set(newest) == {"V1", "V2"}, "a null vehicle_id must be dropped, not keyed"
    assert newest["V1"]["status"] == "on_trip"
    assert newest["V1"]["event_time"] == 300
