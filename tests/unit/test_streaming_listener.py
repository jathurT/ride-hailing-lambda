"""The gauge derivation for the speed layer's progress metrics.

`progress_gauges` is pure, so it is tested without Spark. What it protects is the
only signal that tells a stalled streaming query from an idle one - which matters
because consumer lag cannot do it (plan/09 section 3) and because a wrong unit or a
NaN here produces a panel that looks fine while the pipeline is dead.
"""

from __future__ import annotations

from types import SimpleNamespace

from fleet.speed_layer.listener import _num, _watermark_lag, progress_gauges

NAN = float("nan")


def _progress(**kw):
    base = {
        "name": "q_master",
        "inputRowsPerSecond": 240.0,
        "processedRowsPerSecond": 238.5,
        "batchDuration": 4300,
        "stateOperators": [],
        "eventTime": {},
    }
    base.update(kw)
    return SimpleNamespace(**base)


def test_batch_duration_is_converted_from_milliseconds():
    """Spark reports milliseconds; the metric name says seconds.

    Pushing the raw value is off by 1000 with nothing to reveal it - a panel reading
    "4,300 s" looks like a stall instead of 4.3 seconds.
    """
    g = progress_gauges(_progress(batchDuration=4300), now=100.0)
    assert g["spark_streaming_batch_duration_seconds"] == 4.3


def test_rates_are_passed_through():
    g = progress_gauges(_progress(), now=100.0)
    assert g["spark_streaming_input_rows_per_second"] == 240.0
    assert g["spark_streaming_processed_rows_per_second"] == 238.5


def test_nan_rates_are_dropped_not_pushed():
    """Spark reports NaN when a micro-batch had no rows.

    NaN reaches Grafana as a gap, and a gap is indistinguishable from a failed
    scrape. Omitting the gauge leaves the previous value standing, which is honest.
    """
    g = progress_gauges(_progress(inputRowsPerSecond=NAN, processedRowsPerSecond=NAN), now=100.0)
    assert "spark_streaming_input_rows_per_second" not in g
    assert "spark_streaming_processed_rows_per_second" not in g


def test_progress_timestamp_is_always_present():
    """The liveness signal. Every other gauge is optional; this one is not.

    Alerting is on the AGE of this timestamp, because a query that has stopped stops
    emitting progress - which is the only way to tell "stalled" from "idle".
    """
    for kwargs in ({}, {"inputRowsPerSecond": NAN}, {"batchDuration": None}, {"eventTime": None}):
        assert "spark_streaming_last_progress_timestamp" in progress_gauges(
            _progress(**kwargs), now=100.0
        )


def test_state_rows_are_summed_across_operators():
    g = progress_gauges(
        _progress(
            stateOperators=[
                SimpleNamespace(numRowsTotal=150),
                SimpleNamespace(numRowsTotal=12),
            ]
        ),
        now=100.0,
    )
    assert g["spark_streaming_state_rows"] == 162.0


def test_state_rows_reported_as_zero_when_stateless():
    """Zero, not absent: a stateless query must draw a flat line, not a gap."""
    g = progress_gauges(_progress(stateOperators=[]), now=100.0)
    assert g["spark_streaming_state_rows"] == 0.0


def test_watermark_lag_is_the_gap_between_max_event_time_and_the_watermark():
    g = progress_gauges(
        _progress(
            eventTime={"watermark": "2026-03-08T14:00:00.000Z", "max": "2026-03-08T14:30:00.000Z"}
        ),
        now=100.0,
    )
    assert g["spark_streaming_watermark_lag_seconds"] == 1800.0


def test_a_malformed_progress_object_still_yields_liveness():
    """A listener that raises inside its own callback takes down all speed-layer
    visibility - the exact failure this module exists to prevent."""
    g = progress_gauges(SimpleNamespace(), now=100.0)
    assert g == {
        "spark_streaming_last_progress_timestamp": 100.0,
        "spark_streaming_state_rows": 0.0,
    }


def test_num_helper_rejects_nan_and_junk():
    assert _num(1.5) == 1.5
    assert _num(NAN) is None
    assert _num(None) is None
    assert _num("not a number") is None


def test_watermark_lag_returns_none_rather_than_guessing():
    assert _watermark_lag(None, "2026-03-08T14:00:00Z") is None
    assert _watermark_lag("2026-03-08T14:00:00Z", None) is None
    assert _watermark_lag("garbage", "2026-03-08T14:00:00Z") is None


def test_an_idle_query_refreshes_its_own_liveness_not_an_unnamed_one(monkeypatch):
    """Spark 3.5's idle event has the query id but no name. Before the fix every idle
    heartbeat was pushed as query="unnamed", so the real query still looked stalled
    and a phantom "unnamed" series raised a false alert once data resumed."""
    import pytest

    pytest.importorskip("pyspark")
    from fleet.speed_layer import listener as mod

    pushed = []
    monkeypatch.setattr(
        mod.metrics, "push_gauges", lambda job, g, grouping: pushed.append(grouping)
    )
    lst = mod.build_listener()
    lst.onQueryStarted(SimpleNamespace(id="id-1", runId="r-1", name="q_dlq", timestamp="t"))
    lst.onQueryIdle(SimpleNamespace(id="id-1", runId="r-1", timestamp="t"))
    assert pushed == [{"query": "q_dlq"}]
