"""The shared transforms, exercised on static DataFrames.

Everything here runs on a local SparkSession with no Kafka, no Redis and no Docker.
That is possible precisely because the transforms are pure - and it is why the same
functions can be reused by the batch layer.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from fleet.transforms.enrich import latest_per_key, with_zone, zone_table
from fleet.transforms.geo import haversine_km
from fleet.transforms.utilization import (
    deduplicate_trips,
    zone_activity,
    zone_earnings,
)
from fleet.transforms.validate import split_valid, validate_telemetry

pytestmark = pytest.mark.spark

SIM_NOW = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)

TELEMETRY_SCHEMA = (
    "event_id string, vehicle_id string, driver_id string, trip_id string, "
    "lat double, lon double, speed_kmh double, status string, fare double, "
    "event_time timestamp, ingest_time timestamp"
)


def event(**over):
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
        "event_time": SIM_NOW,
        "ingest_time": SIM_NOW,
    }
    base.update(over)
    return tuple(base[c.split()[0]] for c in TELEMETRY_SCHEMA.split(", "))


def frame(spark, rows):
    return spark.createDataFrame(rows, schema=TELEMETRY_SCHEMA)


class TestValidation:
    def validated(self, spark, **over):
        df = validate_telemetry(frame(spark, [event(**over)]), SIM_NOW)
        return df.collect()[0]

    def test_a_clean_event_passes(self, spark):
        r = self.validated(spark)
        assert r["is_valid"] and r["rejection_reason"] is None

    @pytest.mark.parametrize(
        ("over", "reason"),
        [
            ({"lat": None, "lon": None}, "NULL_COORDINATES"),
            ({"lat": 51.5, "lon": -0.12}, "COORDINATES_OUT_OF_BOUNDS"),
            ({"fare": -30.0}, "NEGATIVE_FARE"),
            ({"speed_kmh": 247.3}, "IMPLAUSIBLE_SPEED"),
            ({"event_time": SIM_NOW + timedelta(hours=2)}, "FUTURE_TIMESTAMP"),
            ({"status": "idle", "trip_id": "T1", "fare": None}, "STATUS_TRIP_MISMATCH"),
            ({"status": "on_trip", "fare": None}, "MISSING_FARE_ON_TRIP"),
        ],
    )
    def test_each_defect_gets_its_own_reason_code(self, spark, over, reason):
        """Each injected defect type must map to a distinct, diagnosable code -
        otherwise the DLQ breakdown says nothing useful."""
        r = self.validated(spark, **over)
        assert not r["is_valid"]
        assert r["rejection_reason"] == reason

    def test_abnormal_but_possible_values_are_kept(self, spark):
        """The heart of the cleaning philosophy: reject the impossible, never the
        merely unusual. A vehicle crawling at 3 km/h is a real observation."""
        assert self.validated(spark, speed_kmh=3.0)["is_valid"]
        assert self.validated(spark, speed_kmh=0.0, status="idle", trip_id=None, fare=None)[
            "is_valid"
        ]
        assert self.validated(spark, fare=0.0)["is_valid"]

    def test_idle_event_with_null_trip_and_fare_is_valid(self, spark):
        assert self.validated(spark, status="idle", trip_id=None, fare=None)["is_valid"]

    def test_micro_batch_scheduling_jitter_is_not_the_future(self, spark):
        """Spark stamps a micro-batch's time before it fetches the batch's Kafka
        offsets, so an event can look up to about 11 real seconds (about 54
        simulated minutes) newer than "now". With a 5-minute tolerance the rerun
        dead-lettered 98 real events as FUTURE_TIMESTAMP."""
        assert self.validated(spark, event_time=SIM_NOW + timedelta(minutes=55))["is_valid"]

    def test_the_tolerance_still_catches_the_injected_clock_skew(self):
        """The injected defect is two hours ahead; the tolerance must stay below it."""
        from fleet.transforms.validate import FUTURE_TOLERANCE_SIM_MINUTES

        assert FUTURE_TOLERANCE_SIM_MINUTES < 120

    def test_speed_at_the_exact_limit_is_kept(self, spark):
        assert self.validated(spark, speed_kmh=200.0)["is_valid"]
        assert not self.validated(spark, speed_kmh=200.1)["is_valid"]

    def test_split_partitions_without_loss(self, spark):
        """Nothing may be silently dropped - every rejection must stay countable."""
        rows = [event(), event(lat=None, lon=None), event(fare=-1.0), event(speed_kmh=5.0)]
        good, bad = split_valid(validate_telemetry(frame(spark, rows), SIM_NOW))
        assert good.count() == 2
        assert bad.count() == 2
        assert good.count() + bad.count() == len(rows)


class TestZoneEnrichment:
    def test_known_point_resolves_to_a_zone(self, spark):
        out = with_zone(frame(spark, [event()]), zone_table(spark)).collect()[0]
        assert out["zone_id"] is not None
        assert out["zone_class"] is not None

    def test_out_of_bounds_keeps_the_row_with_a_null_zone(self, spark):
        """LEFT join on purpose: dropping the row would hide a data-quality problem
        and quietly understate fleet totals."""
        out = with_zone(frame(spark, [event(lat=51.5, lon=-0.12)]), zone_table(spark))
        assert out.count() == 1
        assert out.collect()[0]["zone_id"] is None

    def test_no_row_is_duplicated_by_the_range_join(self, spark):
        """Zones tile without overlap, so each point matches exactly one - if this
        fails, every downstream count is inflated."""
        rows = [
            event(event_id=f"e{i}", lat=6.86 + i * 0.01, lon=79.83 + i * 0.008) for i in range(12)
        ]
        assert with_zone(frame(spark, rows), zone_table(spark)).count() == len(rows)

    def test_zone_table_has_twelve_rows(self, spark):
        assert zone_table(spark).count() == 12


class TestLatestPerKey:
    def test_highest_offset_per_key_wins(self, spark):
        """A compacted topic can still hold several versions of a key until the
        compactor runs; taking the latest makes the join correct regardless."""
        df = spark.createDataFrame(
            [("V001", "petrol", 1), ("V001", "electric", 7), ("V002", "hybrid", 3)],
            "vehicle_id string, fuel_type string, offset long",
        )
        out = {r["vehicle_id"]: r["fuel_type"] for r in latest_per_key(df, "vehicle_id").collect()}
        assert out == {"V001": "electric", "V002": "hybrid"}


class TestHaversine:
    def test_matches_the_python_implementation(self, spark):
        """The simulator computes distance in Python; the batch layer computes it in
        Spark. If they disagree, reported distance and recomputed distance diverge
        and the reconciliation is meaningless."""
        from fleet.common.geo import haversine_km as py_haversine

        pairs = [(6.90, 79.85, 6.95, 79.90), (6.85, 79.82, 6.99, 79.94), (6.9, 79.9, 6.9, 79.9)]
        df = spark.createDataFrame(pairs, "a double, b double, c double, d double")
        got = df.select(
            haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("km")
        ).collect()
        for (lat1, lon1, lat2, lon2), row in zip(pairs, got, strict=True):
            assert row["km"] == pytest.approx(py_haversine(lat1, lon1, lat2, lon2), rel=1e-9)

    def test_identical_points_are_zero(self, spark):
        df = spark.createDataFrame(
            [(6.9, 79.9, 6.9, 79.9)], "a double, b double, c double, d double"
        )
        assert df.select(haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d"))).collect()[0][
            0
        ] == pytest.approx(0.0, abs=1e-9)


class TestRevenueTrap:
    """The single most dangerous bug in the aggregation layer.

    `fare` is the trip's fare repeated on every ping, so a naive sum multiplies
    revenue by the ping count. Measured against live data this inflates revenue
    ~8.5x - and nothing raises.
    """

    def trip_pings(self, n: int, trip_id: str, fare: float, zone_lat: float = 6.9271):
        return [
            event(
                event_id=f"{trip_id}-{i}",
                trip_id=trip_id,
                fare=fare,
                lat=zone_lat,
                event_time=SIM_NOW + timedelta(minutes=3 * i),
            )
            for i in range(n)
        ]

    def test_naive_sum_would_overcount(self, spark):
        """Documents the wrong answer, so a future refactor that reintroduces it
        fails loudly here."""
        df = frame(spark, self.trip_pings(8, "T1", 100.0))
        assert df.agg(F.sum("fare")).collect()[0][0] == pytest.approx(800.0)

    def test_deduplicate_trips_counts_each_trip_once(self, spark):
        df = frame(spark, self.trip_pings(8, "T1", 100.0) + self.trip_pings(5, "T2", 60.0))
        deduped = deduplicate_trips(df, "30 minutes")
        assert deduped.count() == 2
        assert deduped.agg(F.sum("fare")).collect()[0][0] == pytest.approx(160.0)

    def test_idle_pings_contribute_no_revenue(self, spark):
        rows = [
            *self.trip_pings(4, "T1", 100.0),
            event(event_id="i1", trip_id=None, status="idle", fare=None),
        ]
        assert deduplicate_trips(frame(spark, rows), "30 minutes").count() == 1

    def test_earnings_are_correct_after_dedup(self, spark):
        rows = self.trip_pings(8, "T1", 100.0) + self.trip_pings(6, "T2", 45.5)
        enriched = with_zone(frame(spark, rows), zone_table(spark))
        out = zone_earnings(
            deduplicate_trips(enriched, "30 minutes"), "30 minutes", "60 minutes", "60 minutes"
        ).collect()
        assert sum(r["earnings"] for r in out) == pytest.approx(145.5)
        assert sum(r["completed_trips"] for r in out) == 2


class TestTripDedupStateIsBounded:
    """The streaming dedup must forget old trips once the watermark passes them.

    Before the fix `dropDuplicates(["trip_id"])` never evicted anything (the key has
    no event-time column), so the speed layer's earnings query kept every trip since
    start-up in its state store.
    """

    def test_old_trips_are_evicted_from_state(self, spark, tmp_path):
        src = tmp_path / "in"
        src.mkdir()

        def drop_file(name, rows):
            frame(spark, rows).write.parquet((src / name).as_uri())

        schema = frame(spark, [event()]).schema
        stream = spark.readStream.schema(schema).option("recursiveFileLookup", "true")
        deduped = deduplicate_trips(stream.parquet(src.as_uri()), "30 minutes")
        query = (
            deduped.writeStream.format("memory")
            .queryName(f"dedup_{tmp_path.name}")
            .outputMode("append")
            .start()
        )
        try:
            old = [event(event_id=f"o{i}", trip_id=f"OLD{i}") for i in range(10)]
            drop_file("a", old)
            query.processAllAvailable()
            later = SIM_NOW + timedelta(hours=5)
            drop_file("b", [event(event_id="n1", trip_id="NEW1", event_time=later)])
            query.processAllAvailable()
            drop_file("c", [event(event_id="n2", trip_id="NEW2", event_time=later)])
            query.processAllAvailable()
            state = query.lastProgress["stateOperators"][0]["numRowsTotal"]
        finally:
            query.stop()
        assert state <= 2, f"{state} trips still held in state; the old ten were never evicted"

    def test_a_trip_longer_than_the_pipeline_watermark_is_still_one_trip(self, spark):
        """Trips run up to 50 simulated minutes, beyond the 30 minute watermark."""
        rows = [
            event(event_id=f"t{m}", event_time=SIM_NOW + timedelta(minutes=m))
            for m in range(0, 51, 3)
        ]
        assert deduplicate_trips(frame(spark, rows), "30 minutes").count() == 1


class TestZoneActivity:
    def test_counts_distinct_vehicles_not_pings(self, spark):
        rows = [
            event(event_id=f"e{i}", vehicle_id=f"V{i % 3:03d}", trip_id=f"T{i % 3}")
            for i in range(30)
        ]
        enriched = with_zone(frame(spark, rows), zone_table(spark))
        out = zone_activity(enriched, "30 minutes", "60 minutes", "60 minutes").collect()
        assert sum(r["active_vehicles"] for r in out) == 3
        assert sum(r["pings"] for r in out) == 30

    def test_idle_ratio_is_the_share_of_idle_pings(self, spark):
        rows = [event(event_id=f"i{i}", status="idle", trip_id=None, fare=None) for i in range(3)]
        rows += [event(event_id=f"t{i}", status="on_trip") for i in range(1)]
        enriched = with_zone(frame(spark, rows), zone_table(spark))
        out = zone_activity(enriched, "30 minutes", "60 minutes", "60 minutes").collect()[0]
        assert out["idle_ratio"] == pytest.approx(0.75)

    def test_sliding_window_places_an_event_in_multiple_windows(self, spark):
        """The property that makes the dashboard refresh smoothly instead of
        jumping once per window width."""
        enriched = with_zone(frame(spark, [event()]), zone_table(spark))
        out = zone_activity(enriched, "30 minutes", "15 minutes", "5 minutes").collect()
        assert len(out) == 3  # 15-minute window advancing every 5 minutes


class TestEnrouteRowsDoNotZeroRevenue:
    """A vehicle heading to a pickup has a trip_id but no fare - the meter has not
    started. If dedup keeps that row instead of an on-trip one, the trip contributes
    a null fare and revenue silently becomes zero.

    Observed live before the fix: every zone reported 0.00 earnings alongside a
    non-zero completed-trip count.
    """

    def rows_for_one_trip(self, trip_id="T1", fare=100.0):
        return [
            # enroute: trip assigned, meter not started
            event(event_id=f"{trip_id}-e0", trip_id=trip_id, status="enroute", fare=None),
            event(event_id=f"{trip_id}-e1", trip_id=trip_id, status="enroute", fare=None),
            # on_trip: the fare, repeated on every ping
            event(event_id=f"{trip_id}-t0", trip_id=trip_id, status="on_trip", fare=fare),
            event(event_id=f"{trip_id}-t1", trip_id=trip_id, status="on_trip", fare=fare),
        ]

    def test_dedup_keeps_a_row_that_has_a_fare(self, spark):
        out = deduplicate_trips(frame(spark, self.rows_for_one_trip()), "30 minutes").collect()
        assert len(out) == 1
        assert out[0]["fare"] == pytest.approx(100.0)

    def test_revenue_is_the_trip_fare_not_zero(self, spark):
        rows = [*self.rows_for_one_trip("T1", 100.0), *self.rows_for_one_trip("T2", 45.5)]
        deduped = deduplicate_trips(frame(spark, rows), "30 minutes")
        total = deduped.agg(F.sum("fare")).collect()[0][0]
        assert total == pytest.approx(145.5)

    def test_a_trip_that_is_still_enroute_contributes_nothing_yet(self, spark):
        """Correct behaviour: revenue is recognised when the meter runs, not when
        the booking is accepted."""
        rows = [
            event(event_id="e0", trip_id="T9", status="enroute", fare=None),
            event(event_id="e1", trip_id="T9", status="enroute", fare=None),
        ]
        assert deduplicate_trips(frame(spark, rows), "30 minutes").count() == 0
