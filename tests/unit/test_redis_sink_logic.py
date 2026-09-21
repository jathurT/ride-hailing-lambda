"""The non-trivial logic inside the Redis sinks, tested without Redis or Spark."""

from __future__ import annotations

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
