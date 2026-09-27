"""Regression tests for the stale-simulated-clock bug.

A `sim_now` captured once at job start is correct in a batch job and catastrophic
in a stream. At a 288x speed-up the captured value is 4.8 simulated minutes stale
after one real second, so against a 5-simulated-minute future tolerance every event
produced more than about a second after start is dead-lettered as FUTURE_TIMESTAMP.

Observed on the live stack before the fix: 47,049 events rejected against 11,083
deliberately injected defects, and widening. Nothing errored - the DLQ just filled
with valid data.

These tests exist so that reintroducing it fails the build instead of the demo.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from fleet.common.simclock import SimClock
from fleet.transforms.clock import sim_now_column, sim_now_literal
from fleet.transforms.validate import validate_telemetry

pytestmark = pytest.mark.spark

EPOCH_SIM = datetime(2026, 3, 1, tzinfo=UTC)

TELEMETRY_SCHEMA = (
    "event_id string, vehicle_id string, driver_id string, trip_id string, "
    "lat double, lon double, speed_kmh double, status string, fare double, "
    "event_time timestamp, ingest_time timestamp"
)


def clock_started_ago(real_seconds: float, day_seconds: int = 300) -> SimClock:
    """A clock anchored `real_seconds` in the past - i.e. a job that has been up
    that long."""
    return SimClock(
        epoch_wall=datetime.now(UTC) - timedelta(seconds=real_seconds),
        epoch_sim=EPOCH_SIM,
        day_seconds=day_seconds,
    )


def event_at(sim_time: datetime, **over):
    base = {
        "event_id": "e1",
        "vehicle_id": "V001",
        "driver_id": "D001",
        "trip_id": "T1",
        "lat": 6.9271,
        "lon": 79.8612,
        "speed_kmh": 42.0,
        "status": "on_trip",
        "fare": 120.0,
        "event_time": sim_time,
        # ingest_time is REAL wall time in the live topic; "sent just now".
        "ingest_time": datetime.now(UTC),
    }
    base.update(over)
    return tuple(base[c.split()[0]] for c in TELEMETRY_SCHEMA.split(", "))


class TestSimNowColumn:
    def test_tracks_the_clock_rather_than_a_fixed_instant(self, spark):
        """The derived column must agree with the Python clock.

        Compared INSIDE Spark rather than by collecting a datetime: collect()
        renders timestamps in the JVM's default timezone, so on a machine set to
        anything other than UTC a correct value comes back shifted and the
        assertion tests the wrong thing. (This test originally failed that way,
        reporting 5h30m of "drift" that was really Asia/Colombo.)
        """
        clock = clock_started_ago(30)
        drift = (
            spark.range(1)
            .select(
                (sim_now_column(clock).cast("double") - F.lit(clock.sim_now().timestamp())).alias(
                    "drift"
                )
            )
            .collect()[0]["drift"]
        )
        # Allowed slack is in SIMULATED seconds; a second of Spark start-up is
        # 288 simulated seconds at this speed-up.
        assert abs(drift) < 5 * clock.speedup

    def test_advances_as_real_time_passes(self, spark):
        """Two clocks anchored 10 real seconds apart must differ by 10*speedup
        simulated seconds - i.e. the column is a function of now, not of anchor.

        Both evaluated in ONE query so they share a single current_timestamp().
        """
        older, newer = clock_started_ago(60), clock_started_ago(50)
        row = (
            spark.range(1)
            .select(
                sim_now_column(older).cast("double").alias("old"),
                sim_now_column(newer).cast("double").alias("new"),
            )
            .collect()[0]
        )
        assert (row["old"] - row["new"]) == pytest.approx(10 * older.speedup, rel=0.01)

    def test_sub_second_precision_is_kept(self, spark):
        """unix_timestamp() truncates to the whole second; at 288x that discards up
        to 4.8 simulated minutes, comparable to the tolerance being tested. The
        implementation must cast to double instead.

        Detected by whether the result carries a fractional part at all.
        """
        clock = clock_started_ago(0.5)
        value = (
            spark.range(1).select(sim_now_column(clock).cast("double").alias("t")).collect()[0]["t"]
        )
        assert value % 1 != 0.0, "no fractional seconds - precision was truncated"


class TestValidatorAgainstALiveClock:
    def frame(self, spark, rows):
        return spark.createDataFrame(rows, schema=TELEMETRY_SCHEMA)

    def test_a_stale_captured_instant_rejects_valid_events(self, spark):
        """THE BUG, reproduced. A job started 60 real seconds ago sees events whose
        simulated time is far beyond the instant it captured at startup."""
        clock = clock_started_ago(60)
        captured_at_startup = clock.epoch_sim  # what sim_now() returned at t=0
        current_event = event_at(clock.sim_now())

        out = validate_telemetry(self.frame(spark, [current_event]), captured_at_startup).collect()[
            0
        ]
        assert not out["is_valid"]
        assert out["rejection_reason"] == "FUTURE_TIMESTAMP"

    def test_a_live_clock_accepts_the_same_event(self, spark):
        """THE FIX. Same event, same job age, reference derived per row."""
        clock = clock_started_ago(60)
        out = validate_telemetry(self.frame(spark, [event_at(clock.sim_now())]), clock).collect()[0]
        assert out["is_valid"], out["rejection_reason"]

    @pytest.mark.parametrize("job_age_seconds", [1, 10, 60, 300])
    def test_valid_events_stay_valid_however_long_the_job_has_run(self, spark, job_age_seconds):
        """The property that actually matters: acceptance must not decay with
        uptime."""
        clock = clock_started_ago(job_age_seconds)
        out = validate_telemetry(self.frame(spark, [event_at(clock.sim_now())]), clock).collect()[0]
        assert out["is_valid"], f"rejected after {job_age_seconds}s uptime"

    def test_genuinely_future_events_are_still_rejected(self, spark):
        """The fix must not disable the check it was fixing."""
        clock = clock_started_ago(60)
        far_future = clock.sim_now() + timedelta(hours=2)
        out = validate_telemetry(self.frame(spark, [event_at(far_future)]), clock).collect()[0]
        assert not out["is_valid"]
        assert out["rejection_reason"] == "FUTURE_TIMESTAMP"

    def test_an_honest_event_is_valid_however_far_behind_the_batch_clock_is(self, spark):
        """Spark fixes a micro-batch's clock before it fetches the records, so the
        batch clock can be behind the data. Here it is two simulated hours behind the
        event; the event was sent just now and is honest. The old check (batch clock
        plus a tolerance) rejected it."""
        clock = clock_started_ago(60)
        # Sent 25 real seconds (two simulated hours) after the batch clock was fixed.
        sent = datetime.now(UTC) + timedelta(seconds=25)
        event = event_at(clock.sim_now(sent), ingest_time=sent)
        out = validate_telemetry(self.frame(spark, [event]), clock).collect()[0]
        assert out["is_valid"], out["rejection_reason"]

    def test_injected_skew_is_caught_however_late_the_processing_is(self, spark):
        """The injected fault puts event_time two hours after the send time. If the
        stream processes it late enough, the batch clock has caught up and the old
        check let it through: 15 of 480 on a clean run."""
        clock = clock_started_ago(3600)  # the stream is an hour of real time on
        sent = clock.epoch_wall + timedelta(seconds=10)  # sent long ago
        skewed = event_at(clock.sim_now(sent) + timedelta(hours=2), ingest_time=sent)
        out = validate_telemetry(self.frame(spark, [skewed]), clock).collect()[0]
        assert out["rejection_reason"] == "FUTURE_TIMESTAMP"

    def test_a_fixed_datetime_is_still_supported_for_batch(self, spark):
        """The batch layer genuinely wants a fixed reference, so both forms work."""
        fixed = datetime(2026, 3, 5, 12, tzinfo=UTC)
        out = validate_telemetry(
            self.frame(spark, [event_at(fixed - timedelta(hours=1))]), fixed
        ).collect()[0]
        assert out["is_valid"]

    def test_literal_and_clock_helpers_produce_columns(self, spark):
        row = (
            spark.range(1)
            .select(
                sim_now_literal(datetime(2026, 3, 1, tzinfo=UTC)).alias("lit"),
                sim_now_column(clock_started_ago(10)).alias("live"),
            )
            .collect()[0]
        )
        assert row["lit"] is not None and row["live"] is not None
