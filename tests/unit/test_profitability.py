"""The batch layer's transforms.

The full-outer join and its three reconciliation cases are the strongest
"correctness of transformation logic" evidence in the project, so they are tested
case by case rather than only on the happy path.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from fleet.transforms.profitability import (
    DISTANCE_VARIANCE_ALERT_PCT,
    MIN_DAYS_FOR_TREND,
    classify,
    compute_profit,
    daily_revenue,
    join_with_expenses,
    reconstruct_trips,
    with_trend,
)

pytestmark = pytest.mark.spark

D0 = date(2026, 3, 1)
T0 = datetime(2026, 3, 1, 8, 0, tzinfo=UTC)

EVENT_SCHEMA = (
    "vehicle_id string, trip_id string, lat double, lon double, "
    "status string, fare double, event_time timestamp, sim_date date"
)


def ping(minute: int, status="on_trip", trip="T1", fare=100.0, lat=6.90, lon=79.85, vid="V001"):
    return (vid, trip, lat, lon, status, fare, T0 + timedelta(minutes=minute), D0)


def events(spark, rows):
    return spark.createDataFrame(rows, schema=EVENT_SCHEMA)


def telemetry_row(spark, **over):
    base = {
        "vehicle_id": "V001",
        "sim_date": D0,
        "trips": 5,
        "telemetry_distance_km": 100.0,
        "paid_distance_km": 70.0,
        "deadhead_distance_km": 30.0,
        "on_trip_hours": 4.0,
        "idle_hours": 4.0,
        "pings": 480,
        "revenue": 300.0,
    }
    base.update(over)
    return spark.createDataFrame([base])


def expense_row(spark, **over):
    base = {
        "vehicle_id": "V001",
        "sim_date": D0,
        "fuel_cost": 42.0,
        "maintenance_cost": 8.0,
        "partner_distance_km": 103.0,
        "service_flag": False,
        "partner_id": "GARAGE_A",
    }
    base.update(over)
    return spark.createDataFrame([base])


class TestTripReconstruction:
    def test_distance_accumulates_across_pings(self, spark):
        rows = [ping(0, lat=6.90), ping(3, lat=6.91), ping(6, lat=6.92)]
        out = reconstruct_trips(events(spark, rows)).collect()[0]
        assert out["telemetry_distance_km"] > 0

    def test_first_ping_contributes_no_distance(self, spark):
        """There is no previous position to measure from - it must not be treated
        as (0,0), which would produce a ~7000 km segment."""
        out = reconstruct_trips(events(spark, [ping(0)])).collect()[0]
        assert out["telemetry_distance_km"] == pytest.approx(0.0)

    def test_paid_and_deadhead_distance_are_separated(self, spark):
        """Deadhead kilometres burn fuel and earn nothing - the mechanism by which a
        vehicle becomes unprofitable."""
        rows = [
            ping(0, status="enroute", fare=None),
            ping(3, status="enroute", fare=None, lat=6.91),
            ping(6, status="on_trip", lat=6.92),
            ping(9, status="on_trip", lat=6.93),
        ]
        out = reconstruct_trips(events(spark, rows)).collect()[0]
        assert out["deadhead_distance_km"] > 0
        assert out["paid_distance_km"] > 0

    def test_idle_hours_are_measured(self, spark):
        rows = [
            ping(0, status="idle", trip=None, fare=None),
            ping(60, status="idle", trip=None, fare=None),
        ]
        out = reconstruct_trips(events(spark, rows)).collect()[0]
        assert out["idle_hours"] == pytest.approx(1.0, abs=0.01)

    def test_trip_count_is_exact_not_approximate(self, spark):
        """The batch layer uses exact countDistinct where the speed layer uses
        HyperLogLog - the module's "high accuracy" vs "immediate but less accurate"."""
        rows = [ping(i * 3, trip=f"T{i % 4}") for i in range(20)]
        assert reconstruct_trips(events(spark, rows)).collect()[0]["trips"] == 4


class TestRevenue:
    def test_repeated_fares_are_counted_once(self, spark):
        """The 8.5x revenue trap, in the batch layer."""
        rows = [ping(i * 3, trip="T1", fare=100.0) for i in range(8)]
        assert daily_revenue(events(spark, rows)).collect()[0]["revenue"] == pytest.approx(100.0)

    def test_enroute_pings_do_not_zero_revenue(self, spark):
        rows = [
            ping(0, status="enroute", trip="T1", fare=None),
            ping(3, status="on_trip", trip="T1", fare=100.0),
        ]
        assert daily_revenue(events(spark, rows)).collect()[0]["revenue"] == pytest.approx(100.0)

    def test_several_trips_sum(self, spark):
        rows = [ping(0, trip="T1", fare=100.0), ping(30, trip="T2", fare=45.5)]
        assert daily_revenue(events(spark, rows)).collect()[0]["revenue"] == pytest.approx(145.5)


class TestTheTwoSourceJoin:
    """FULL OUTER with three explicit statuses. An inner join would silently discard
    both mismatch directions, which in a financial reconciliation is the classic
    serious bug."""

    def test_matched(self, spark):
        out = join_with_expenses(telemetry_row(spark), expense_row(spark)).collect()[0]
        assert out["expense_status"] == "MATCHED"

    def test_vehicle_drove_but_was_never_invoiced(self, spark):
        empty = expense_row(spark).filter("1=0")
        out = join_with_expenses(telemetry_row(spark), empty).collect()[0]
        assert out["expense_status"] == "MISSING_EXPENSE"
        assert out["revenue"] == pytest.approx(300.0)

    def test_invoiced_for_a_vehicle_that_never_reported(self, spark):
        """Possible fraud or a dead telematics unit. Must be retained, not dropped."""
        empty = telemetry_row(spark).filter("1=0")
        out = join_with_expenses(empty, expense_row(spark)).collect()[0]
        assert out["expense_status"] == "MISSING_TELEMETRY"
        assert out["fuel_cost"] == pytest.approx(42.0)

    def test_no_row_is_lost(self, spark):
        """The whole reason for full_outer."""
        t = telemetry_row(spark).union(telemetry_row(spark, vehicle_id="V002"))
        e = expense_row(spark).union(expense_row(spark, vehicle_id="V003"))
        assert join_with_expenses(t, e).count() == 3

    def test_distance_variance_is_surfaced(self, spark):
        """Two independent measurements of one journey. The job is to surface the
        disagreement, not hide it by picking one."""
        out = join_with_expenses(
            telemetry_row(spark, telemetry_distance_km=100.0),
            expense_row(spark, partner_distance_km=112.0),
        ).collect()[0]
        assert out["distance_variance_pct"] == pytest.approx(12.0)
        assert out["distance_variance_pct"] > DISTANCE_VARIANCE_ALERT_PCT


class TestProfit:
    def joined(self, spark, **over):
        t_over = {
            k: v
            for k, v in over.items()
            if k in {"revenue", "telemetry_distance_km", "on_trip_hours", "idle_hours"}
        }
        e_over = {k: v for k, v in over.items() if k in {"fuel_cost", "maintenance_cost"}}
        return compute_profit(
            join_with_expenses(telemetry_row(spark, **t_over), expense_row(spark, **e_over))
        ).collect()[0]

    def test_net_profit_is_revenue_minus_costs(self, spark):
        out = self.joined(spark, revenue=300.0, fuel_cost=42.0, maintenance_cost=8.0)
        assert out["net_profit"] == pytest.approx(250.0)

    def test_a_service_event_can_push_a_day_negative(self, spark):
        out = self.joined(spark, revenue=180.0, fuel_cost=60.0, maintenance_cost=420.0)
        assert out["net_profit"] < 0

    def test_missing_costs_give_a_null_profit_not_an_inflated_one(self, spark):
        """A null profit is honest; revenue-minus-nothing invents a healthy vehicle
        out of missing data."""
        empty = expense_row(spark).filter("1=0")
        out = compute_profit(join_with_expenses(telemetry_row(spark), empty)).collect()[0]
        assert out["net_profit"] is None

    def test_utilization_is_on_trip_over_total_time(self, spark):
        out = self.joined(spark, on_trip_hours=6.0, idle_hours=2.0)
        assert out["utilization_pct"] == pytest.approx(75.0)


class TestTrendAndClassification:
    def history(self, spark, profits: list[float]):
        rows = [
            {"vehicle_id": "V001", "sim_date": D0 + timedelta(days=i), "net_profit": p}
            for i, p in enumerate(profits)
        ]
        return classify(with_trend(spark.createDataFrame(rows)))

    def last(self, spark, profits):
        return self.history(spark, profits).orderBy(F.desc("sim_date")).collect()[0]

    def test_too_little_history_says_so_rather_than_guessing(self, spark):
        """A slope through two points is not a trend."""
        for n in range(1, MIN_DAYS_FOR_TREND):
            out = self.last(spark, [100.0] * n)
            assert out["classification"] == "INSUFFICIENT_DATA"

    def test_steady_profit_is_healthy(self, spark):
        assert self.last(spark, [100.0] * 7)["classification"] == "HEALTHY"

    def test_profitable_but_declining_is_WATCH(self, spark):
        """The class the business question actually asks for: 'BECOMING
        unprofitable' - still in the black, but trending down."""
        out = self.last(spark, [200.0, 180.0, 160.0, 140.0, 120.0, 100.0, 80.0])
        assert out["classification"] == "WATCH"
        assert out["rolling_7d_avg_profit"] > 0
        assert out["profit_trend_slope"] < 0

    def test_sustained_losses_are_UNPROFITABLE(self, spark):
        assert self.last(spark, [-50.0] * 7)["classification"] == "UNPROFITABLE"

    def test_losing_money_and_getting_worse_is_CRITICAL(self, spark):
        out = self.last(spark, [10.0, 0.0, -20.0, -40.0, -60.0, -80.0, -100.0])
        assert out["classification"] == "CRITICAL"

    def test_the_rolling_window_is_seven_days(self, spark):
        """An eighth day must not drag the average - the window must slide."""
        out = self.last(spark, [1000.0] + [10.0] * 7)
        assert out["rolling_7d_avg_profit"] == pytest.approx(10.0)
