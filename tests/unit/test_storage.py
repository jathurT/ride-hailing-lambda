"""Storage layout, key design and the mart's idempotency contract.

These run against real Postgres and Redis when the stack is up, and skip cleanly
when it is not - so `make test` stays a no-docker command while still covering the
SQL that the restatement scenario depends on.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime

import pytest

from fleet.common.simclock import SimClock
from fleet.store import redis_keys as keys
from fleet.store.lake import PREFIXES, LakePaths
from fleet.store.mart import UPSERT_COLUMNS, PnlRow

CLOCK = SimClock(
    epoch_wall=datetime(2026, 8, 4, tzinfo=UTC),
    epoch_sim=datetime(2026, 3, 1, tzinfo=UTC),
    day_seconds=300,
)


class TestLakeLayout:
    def test_partition_path_is_hive_style(self):
        """Spark reads `sim_date` back as a real column and can prune partitions
        from a WHERE clause only if the directory is named this way."""
        p = LakePaths("fleet-lake").telemetry_partition(date(2026, 3, 2))
        assert p == "s3a://fleet-lake/raw/telemetry/sim_date=2026-03-02"

    def test_scheme_is_switchable(self):
        """Spark uses s3a://; boto3 and the console use plain keys."""
        lp = LakePaths("fleet-lake")
        assert lp.telemetry_root("s3").startswith("s3://")
        assert lp.telemetry_root().startswith("s3a://")

    def test_expense_key_matches_what_the_dropper_writes(self):
        """A mismatch here fails silently: the sensor waits forever, or Spark reads
        an empty partition and reports zeros."""
        assert (
            LakePaths("b").expense_key(date(2026, 3, 1))
            == "landing/expenses/expenses_2026-03-01.csv"
        )

    def test_report_versioning_for_restatements(self):
        lp = LakePaths("b")
        assert lp.report_key(date(2026, 3, 2)).endswith("2026-03-02.pdf")
        assert lp.report_key(date(2026, 3, 2), version=2).endswith("2026-03-02_v2.pdf")

    def test_all_prefixes_declared(self):
        assert set(PREFIXES) >= {"raw/telemetry", "landing/expenses", "reports", "_checkpoints"}


class TestRedisKeyDesign:
    def test_ttls_derive_from_the_simulated_clock(self):
        """A 2-simulated-hour TTL is 25 real seconds at 288x. Hard-coding 25 here
        would silently become wrong the moment SIM_DAY_SECONDS changed."""
        assert keys.ttl_seconds(CLOCK, sim_hours=2) == 25

    def test_ttl_is_never_zero(self):
        """Redis treats a TTL of 0 as 'no expiry', so rounding a short simulated
        duration down to 0 would leak keys forever."""
        assert keys.ttl_seconds(CLOCK, sim_minutes=0.01) >= 1

    def test_ttl_scales_with_the_clock(self):
        slow = SimClock(CLOCK.epoch_wall, CLOCK.epoch_sim, day_seconds=3600)
        assert keys.ttl_seconds(slow, sim_hours=2) > keys.ttl_seconds(CLOCK, sim_hours=2)

    def test_keys_are_namespaced(self):
        assert keys.zone("Z01").startswith("fleet:")
        assert keys.vehicle_state("V007").startswith("fleet:")

    def test_alert_feed_is_bounded(self):
        assert keys.IDLE_ALERTS_MAX > 0


class TestMartSql:
    def test_upsert_targets_the_natural_key(self):
        from fleet.store.mart import UPSERT_PNL_SQL

        assert "ON CONFLICT (vehicle_id, sim_date) DO UPDATE" in UPSERT_PNL_SQL

    def test_upsert_never_deletes(self):
        """DELETE-then-INSERT would leave the day missing if the job crashed between
        the two. Upsert is atomic per row."""
        from fleet.store.mart import UPSERT_PNL_SQL

        assert "DELETE" not in UPSERT_PNL_SQL.upper()

    def test_watermark_cannot_move_backwards(self):
        from fleet.store.mart import ADVANCE_HWM_SQL

        assert "GREATEST" in ADVANCE_HWM_SQL

    def test_row_serialises_in_column_order(self):
        row = PnlRow(vehicle_id="V001", sim_date=date(2026, 3, 1), job_run_id="run-1")
        t = row.as_tuple()
        assert len(t) == len(UPSERT_COLUMNS)
        assert t[UPSERT_COLUMNS.index("vehicle_id")] == "V001"
        assert t[UPSERT_COLUMNS.index("job_run_id")] == "run-1"

    def test_defaults_are_safe_for_a_vehicle_with_no_expense_row(self):
        """Costs default to None, not 0. A null profit is honest; a zero-cost profit
        is a lie (plan/06 section 2.2)."""
        row = PnlRow(vehicle_id="V1", sim_date=date(2026, 3, 1), job_run_id="r")
        assert row.fuel_cost is None and row.maintenance_cost is None
        assert row.classification == "INSUFFICIENT_DATA"


# --------------------------------------------------------------------------
# Integration: real Postgres / Redis. Skipped unless the stack is up.


def _pg_dsn() -> str:
    return os.environ.get("TEST_POSTGRES_DSN", "postgresql://fleet:fleet@localhost:5442/fleet_mart")


def _pg_available() -> bool:
    try:
        import psycopg

        with psycopg.connect(_pg_dsn(), connect_timeout=2):
            return True
    except Exception:
        return False


def _redis_available() -> bool:
    try:
        import redis

        redis.from_url(
            os.environ.get("TEST_REDIS_URL", "redis://localhost:6389/0"), socket_connect_timeout=2
        ).ping()
        return True
    except Exception:
        return False


pg = pytest.mark.skipif(not _pg_available(), reason="postgres not running (make up)")
rd = pytest.mark.skipif(not _redis_available(), reason="redis not running (make up)")


@pg
class TestMartAgainstRealPostgres:
    @pytest.fixture
    def cur(self):
        import psycopg

        with psycopg.connect(_pg_dsn(), autocommit=True) as conn, conn.cursor() as c:
            c.execute("DELETE FROM mart.fact_vehicle_daily_pnl WHERE vehicle_id LIKE 'TEST%'")
            yield c
            c.execute("DELETE FROM mart.fact_vehicle_daily_pnl WHERE vehicle_id LIKE 'TEST%'")

    def row(self, **kw) -> PnlRow:
        base = {
            "vehicle_id": "TEST01",
            "sim_date": date(2026, 3, 1),
            "job_run_id": "run-1",
            "revenue": 100.0,
            "net_profit": 40.0,
        }
        return PnlRow(**{**base, **kw})

    def test_insert_then_read(self, cur):
        from fleet.store.mart import upsert_pnl

        upsert_pnl(cur, [self.row()])
        cur.execute("SELECT revenue FROM mart.fact_vehicle_daily_pnl WHERE vehicle_id='TEST01'")
        assert float(cur.fetchone()[0]) == 100.0

    def test_rerunning_the_same_job_is_a_no_op(self, cur):
        """Re-running the batch job must not manufacture a phantom restatement."""
        from fleet.store.mart import upsert_pnl

        upsert_pnl(cur, [self.row()])
        upsert_pnl(cur, [self.row()])
        cur.execute(
            "SELECT count(*), max(restatement_count) FROM mart.fact_vehicle_daily_pnl "
            "WHERE vehicle_id='TEST01'"
        )
        count, restatements = cur.fetchone()
        assert count == 1 and restatements == 0

    def test_a_new_job_run_records_a_restatement(self, cur):
        """The restatement scenario: same day recomputed by a different run."""
        from fleet.store.mart import upsert_pnl

        upsert_pnl(cur, [self.row(job_run_id="run-1", net_profit=40.0)])
        upsert_pnl(cur, [self.row(job_run_id="run-2", net_profit=95.0)])
        cur.execute(
            "SELECT net_profit, restatement_count, restated_at IS NOT NULL "
            "FROM mart.fact_vehicle_daily_pnl WHERE vehicle_id='TEST01'"
        )
        profit, count, restated = cur.fetchone()
        assert float(profit) == 95.0
        assert count == 1 and restated

    def test_watermark_is_single_row_and_monotonic(self, cur):
        """Save and restore the real watermark.

        This table is deliberately single-row and shared with the running batch
        layer, so a test that writes to it without restoring would corrupt the
        serving layer's completeness contract - and, because GREATEST() keeps the
        larger date, a live watermark ahead of the test's dates makes the assertion
        fail for reasons that have nothing to do with the code.
        """
        from fleet.store.mart import advance_high_water_mark, read_high_water_mark

        cur.execute("SELECT batch_complete_thru, last_job_run_id FROM mart.batch_high_water_mark")
        saved = cur.fetchone()
        try:
            cur.execute("DELETE FROM mart.batch_high_water_mark")
            advance_high_water_mark(cur, date(2026, 3, 5), "run-a")
            advance_high_water_mark(cur, date(2026, 3, 2), "run-b")  # a restatement
            assert read_high_water_mark(cur) == date(2026, 3, 5)
            cur.execute("SELECT count(*) FROM mart.batch_high_water_mark")
            assert cur.fetchone()[0] == 1
        finally:
            cur.execute("DELETE FROM mart.batch_high_water_mark")
            if saved:
                advance_high_water_mark(cur, saved[0], saved[1])

    def test_classification_is_constrained(self, cur):
        import psycopg

        from fleet.store.mart import upsert_pnl

        with pytest.raises(psycopg.errors.CheckViolation):
            upsert_pnl(cur, [self.row(classification="VIBES")])

    def test_zone_dimension_was_seeded(self, cur):
        cur.execute("SELECT count(*) FROM mart.dim_zone")
        assert cur.fetchone()[0] == 12


@rd
class TestSpeedViewAgainstRealRedis:
    @pytest.fixture
    def view(self):
        import redis

        from fleet.store.speed_view import SpeedView

        client = redis.from_url(
            os.environ.get("TEST_REDIS_URL", "redis://localhost:6389/0"), decode_responses=True
        )
        for k in client.scan_iter("fleet:zone:TEST*"):
            client.delete(k)
        yield SpeedView(client, CLOCK), client
        for k in client.scan_iter("fleet:zone:TEST*"):
            client.delete(k)

    def agg(self, trips: int = 5):
        from fleet.store.speed_view import ZoneAggregate

        # No `earnings` field: that belongs to the earnings query, which writes into
        # the same hash. Overlapping field sets silently zeroed revenue in the live
        # stack - see TestWriterFieldOwnership.
        return ZoneAggregate("TESTZ", 12, trips, 0.25, 38.5, "s", "e", "2026-03-01T00:00:00Z")

    def test_write_and_read_back(self, view):
        sv, client = view
        sv.write_zone_aggregates([self.agg()])
        assert client.hget(keys.zone("TESTZ"), "trips") == "5"

    def test_activity_write_does_not_disturb_earnings(self, view):
        """The regression: the earnings query writes revenue, then an activity
        micro-batch must leave it alone."""
        sv, client = view
        client.hset(keys.zone("TESTZ"), mapping={"earnings": "42.50"})
        sv.write_zone_aggregates([self.agg()])
        assert client.hget(keys.zone("TESTZ"), "earnings") == "42.50"

    def test_ttl_is_applied_and_short(self, view):
        sv, client = view
        sv.write_zone_aggregates([self.agg()])
        ttl = client.ttl(keys.zone("TESTZ"))
        assert 0 < ttl <= 25

    def test_rewriting_a_batch_overwrites_rather_than_accumulates(self, view):
        """The idempotency guarantee: a replayed micro-batch must not double-count."""
        sv, client = view
        sv.write_zone_aggregates([self.agg(trips=5)])
        sv.write_zone_aggregates([self.agg(trips=5)])
        assert client.hget(keys.zone("TESTZ"), "trips") == "5"
