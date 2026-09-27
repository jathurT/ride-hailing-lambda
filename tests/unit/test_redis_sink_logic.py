"""The non-trivial logic inside the Redis sinks, tested without Redis or Spark."""

from __future__ import annotations

import pytest

from fleet.speed_layer.sinks.redis_sink import newest_window_per_zone


def row(zone: str | None, window_end: str, **extra):
    return {"zone_id": zone, "window_end": window_end, **extra}


class TestNewestWindowPerZone:
    def test_keeps_the_latest_window_for_each_zone(self):
        """A sliding-window batch emits overlapping windows; only the newest is the
        current view."""
        rows = [
            row("Z01", "2026-03-01T10:00:00", n=1),
            row("Z01", "2026-03-01T10:05:00", n=2),
            row("Z01", "2026-03-01T09:55:00", n=3),
            row("Z02", "2026-03-01T10:00:00", n=4),
        ]
        out = newest_window_per_zone(rows)
        assert set(out) == {"Z01", "Z02"}
        assert out["Z01"]["n"] == 2

    def test_order_of_arrival_does_not_matter(self):
        """Micro-batch rows arrive in arbitrary order; the result must not depend
        on it, or the dashboard flickers between window generations."""
        newest = row("Z01", "2026-03-01T10:05:00", n=99)
        older = row("Z01", "2026-03-01T10:00:00", n=1)
        assert newest_window_per_zone([older, newest])["Z01"]["n"] == 99
        assert newest_window_per_zone([newest, older])["Z01"]["n"] == 99

    def test_null_zone_rows_are_dropped(self):
        """Events outside the city bounding box belong to no zone. They are counted
        as a data-quality signal upstream, but must not be written as a zone."""
        out = newest_window_per_zone([row(None, "2026-03-01T10:00:00"), row("Z01", "x")])
        assert set(out) == {"Z01"}

    def test_empty_input(self):
        assert newest_window_per_zone([]) == {}

    def test_every_zone_survives(self):
        rows = [row(f"Z{i:02d}", "2026-03-01T10:00:00") for i in range(1, 13)]
        assert len(newest_window_per_zone(rows)) == 12


class TestWriterFieldOwnership:
    """Activity and earnings are produced by two separate streaming queries that
    write into the SAME Redis hash. If their field sets overlap, whichever runs last
    silently zeroes the other's work.

    This actually happened: ZoneAggregate carried `earnings=0.0`, so every activity
    micro-batch clobbered the earnings the other query had just written. The
    dashboard showed 0.00 revenue in every zone and nothing failed.
    """

    def aggregate(self):
        from fleet.store.speed_view import ZoneAggregate

        return ZoneAggregate(
            zone_id="Z01",
            active_vehicles=10,
            trips=4,
            idle_ratio=0.63,
            avg_speed_kmh=13.2,
            window_start="s",
            window_end="e",
            updated_at_sim="u",
        )

    def test_activity_and_earnings_fields_are_disjoint(self):
        from fleet.store.speed_view import EARNINGS_FIELDS

        assert not (set(self.aggregate().as_mapping()) & EARNINGS_FIELDS)

    def test_activity_mapping_does_not_mention_earnings(self):
        assert "earnings" not in self.aggregate().as_mapping()

    def test_activity_mapping_carries_what_it_owns(self):
        m = self.aggregate().as_mapping()
        assert {"active_vehicles", "trips", "idle_ratio", "avg_speed_kmh"} <= set(m)
        assert m["active_vehicles"] == "10"


class TestIdleAlertFeedDurability:
    """The idle-alert feed must survive a gap between alerts.

    It used to carry the speed-view TTL. A Redis TTL applies to the whole KEY, not to
    individual members, and 2 simulated hours is 25 REAL seconds at a 288x clock - so
    the entire feed vanished 25 seconds after the last push, taking all 200 alerts
    with it, then reappeared on the next one. Observed live as ZCARD oscillating
    between 200 and 0 while alerts streamed steadily into Kafka throughout.

    The trim already bounds the key at 200 members permanently, which is the only
    thing the TTL was there to do.
    """

    class _FakePipe:
        def __init__(self):
            self.calls = []

        def zadd(self, key, mapping):
            self.calls.append(("zadd", key, mapping))
            return self

        def zremrangebyrank(self, key, start, stop):
            self.calls.append(("zremrangebyrank", key, start, stop))
            return self

        def expire(self, key, ttl):
            self.calls.append(("expire", key, ttl))
            return self

        def execute(self):
            return []

    class _FakeClient:
        def __init__(self, pipe):
            self._pipe = pipe

        def pipeline(self):
            return self._pipe

    def _view(self, pipe):
        from datetime import UTC, datetime

        from fleet.common.simclock import SimClock
        from fleet.store.speed_view import SpeedView

        clock = SimClock(epoch_wall=datetime.now(UTC), epoch_sim=datetime.now(UTC), day_seconds=300)
        return SpeedView(self._FakeClient(pipe), clock)

    def test_the_feed_is_never_given_an_expiry(self):
        pipe = self._FakePipe()
        self._view(pipe).push_idle_alert('{"alert_id":"a1"}', score=1.0)
        assert not [c for c in pipe.calls if c[0] == "expire"], (
            "an EXPIRE on the alert ZSET deletes the whole feed, not old members"
        )

    def test_the_feed_is_still_bounded_by_trimming(self):
        """Removing the TTL must not remove the memory bound."""
        from fleet.store import redis_keys as keys

        pipe = self._FakePipe()
        self._view(pipe).push_idle_alert('{"alert_id":"a1"}', score=1.0)
        trims = [c for c in pipe.calls if c[0] == "zremrangebyrank"]
        assert trims, "the feed must still be trimmed or it grows unbounded"
        assert trims[0][3] == -(keys.IDLE_ALERTS_MAX + 1)

    def test_the_alert_is_scored_so_newest_first_reads_work(self):
        pipe = self._FakePipe()
        self._view(pipe).push_idle_alert('{"alert_id":"a1"}', score=12345.0)
        zadds = [c for c in pipe.calls if c[0] == "zadd"]
        assert zadds and list(zadds[0][2].values()) == [12345.0]


class TestARedisOutageDoesNotStopTheSpeedLayer:
    """An exception in foreachBatch terminates the query; the job then stops every
    query and restarts, in a loop, until Redis is back. Seen live: stopping Redis
    stopped the lake writes too. The writers now skip the batch and count the error.
    """

    UNREACHABLE = "redis://127.0.0.1:1/0"  # nothing listens on port 1

    class _Batch:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

        def isEmpty(self):
            return not self._rows

    @staticmethod
    def _clock():
        from datetime import UTC, datetime

        from fleet.common.simclock import SimClock

        return SimClock(
            epoch_wall=datetime(2026, 9, 27, tzinfo=UTC),
            epoch_sim=datetime(2026, 3, 1, tzinfo=UTC),
            day_seconds=300,
        )

    @staticmethod
    def _errors(sink):
        from fleet.common import metrics

        return metrics.sink_write_errors_total.labels(sink=sink)._value.get()

    def test_the_zone_writer_skips_the_batch_and_counts_it(self):
        pytest.importorskip("pyspark")
        from fleet.speed_layer.sinks.redis_sink import make_activity_writer

        row = {
            "zone_id": "Z01",
            "active_vehicles": 3,
            "trips": 1,
            "idle_ratio": 0.5,
            "avg_speed_kmh": 20.0,
            "window_start": "2026-03-01 10:00:00",
            "window_end": "2026-03-01 10:15:00",
        }
        before = self._errors("redis_activity")
        make_activity_writer(self.UNREACHABLE, self._clock())(self._Batch([row]), 7)
        assert self._errors("redis_activity") == before + 1

    def test_the_vehicle_writer_skips_the_batch_and_counts_it(self):
        pytest.importorskip("pyspark")
        from datetime import datetime

        from fleet.speed_layer.sinks.redis_sink import make_vehicle_state_writer

        row = {
            "vehicle_id": "V001",
            "status": "idle",
            "lat": 6.9,
            "lon": 79.86,
            "speed_kmh": 0.0,
            "zone_id": "Z01",
            "trip_id": None,
            "driver_id": "D001",
            "event_time": datetime(2026, 3, 1, 10),
        }
        before = self._errors("redis_vehicle_state")
        make_vehicle_state_writer(self.UNREACHABLE, self._clock())(self._Batch([row]), 7)
        assert self._errors("redis_vehicle_state") == before + 1
