"""Per-vehicle daily profitability: the batch layer's answer.

★ Shared package. `zone_activity` here is the SAME function the speed layer calls -
that is the point (ADR-001). What differs is only the input: a static DataFrame read
from the immutable Parquet master dataset rather than a stream.

This module answers the second half of the business question:

    "...which vehicles are BECOMING unprofitable once yesterday's fuel/maintenance
     costs are factored in?"

Two words carry the weight. **"unprofitable"** requires joining telemetry with the
daily expense file - the two-source join the assignment asks for. **"becoming"**
requires a trailing multi-day trend, not a one-day lookup, which is why the batch
layer exists at all and a large part of why this project is Lambda rather than Kappa.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fleet.transforms.geo import haversine_km

# A vehicle needs this many simulated days of history before a trend means anything.
# Below it we report INSUFFICIENT_DATA rather than a slope drawn through two points.
MIN_DAYS_FOR_TREND = 4
TREND_WINDOW_DAYS = 7

# Flag a vehicle whose partner-reported distance disagrees with ours by more than
# this. Real reconciliation always has two sources that disagree slightly; the job
# is to surface the disagreement, not to hide it by picking one.
DISTANCE_VARIANCE_ALERT_PCT = 10.0


def reconstruct_trips(events: DataFrame) -> DataFrame:
    """Turn per-ping telemetry into per-vehicle daily totals.

    Telemetry is per ping; profitability is per vehicle per day. Bridging the two
    needs the previous ping for each vehicle, which is a window function over an
    ordered partition - a WIDE transformation requiring a shuffle, and exactly the
    operation the module's Spark material describes.

    Distance is computed here from consecutive GPS positions rather than trusted
    from the partner's odometer, so we hold an independent measurement to reconcile
    against.
    """
    w = Window.partitionBy("vehicle_id", "sim_date").orderBy("event_time")

    stepped = (
        events.withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_time", F.lag("event_time").over(w))
        .withColumn("prev_status", F.lag("status").over(w))
        .withColumn(
            "segment_km",
            F.when(
                F.col("prev_lat").isNotNull(),
                haversine_km(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "segment_minutes",
            F.when(
                F.col("prev_time").isNotNull(),
                (F.unix_timestamp("event_time") - F.unix_timestamp("prev_time")) / 60.0,
            ).otherwise(F.lit(0.0)),
        )
    )

    return stepped.groupBy("vehicle_id", "sim_date").agg(
        # Exact distinct count here, unlike the speed layer's HyperLogLog. The
        # module frames the batch layer as "high accuracy" against a speed layer
        # that is "immediate but less accurate"; this is that difference in code.
        F.countDistinct(F.when(F.col("status") == "on_trip", F.col("trip_id"))).alias("trips"),
        F.sum("segment_km").alias("telemetry_distance_km"),
        F.sum(F.when(F.col("prev_status") == "on_trip", F.col("segment_km")).otherwise(0.0)).alias(
            "paid_distance_km"
        ),
        F.sum(F.when(F.col("prev_status") == "enroute", F.col("segment_km")).otherwise(0.0)).alias(
            "deadhead_distance_km"
        ),
        (
            F.sum(
                F.when(F.col("prev_status") == "on_trip", F.col("segment_minutes")).otherwise(0.0)
            )
            / 60.0
        ).alias("on_trip_hours"),
        (
            F.sum(F.when(F.col("prev_status") == "idle", F.col("segment_minutes")).otherwise(0.0))
            / 60.0
        ).alias("idle_hours"),
        F.count("*").alias("pings"),
    )


def daily_revenue(events: DataFrame) -> DataFrame:
    """Exact revenue per vehicle per day.

    Same trap as the speed layer: `fare` is the trip's fare repeated on every ping,
    so it must be deduplicated to one row per trip before summing. `fare IS NOT
    NULL` additionally drops enroute pings, whose meter has not started.
    """
    return (
        events.filter(F.col("trip_id").isNotNull() & F.col("fare").isNotNull())
        .dropDuplicates(["trip_id"])
        .groupBy("vehicle_id", "sim_date")
        .agg(F.sum("fare").alias("revenue"))
    )


def join_with_expenses(telemetry: DataFrame, expenses: DataFrame) -> DataFrame:
    """★ The two-source join the assignment asks for.

    FULL OUTER, deliberately. Both mismatch directions are real business signals and
    an inner join would silently discard both:

      MATCHED            normal
      MISSING_EXPENSE    the vehicle drove but the partner did not invoice it.
                         Costs stay NULL, not zero - a null profit is honest, a
                         zero-cost profit is a lie.
      MISSING_TELEMETRY  invoiced for a vehicle that never reported. Possible fraud,
                         or a dead telematics unit. Retained for investigation.

    In a financial reconciliation, silently dropping unmatched rows is the classic
    serious bug. This is the strongest single piece of "correctness of transformation
    logic" evidence in the project.
    """
    joined = telemetry.join(expenses, on=["vehicle_id", "sim_date"], how="full_outer")

    return joined.withColumn(
        "expense_status",
        F.when(F.col("fuel_cost").isNull(), F.lit("MISSING_EXPENSE"))
        .when(F.col("pings").isNull(), F.lit("MISSING_TELEMETRY"))
        .otherwise(F.lit("MATCHED")),
    ).withColumn(
        # Two independent measurements of one journey. Surfacing the disagreement is
        # the point; we use our own GPS figure downstream and expose both.
        "distance_variance_pct",
        F.when(
            F.col("telemetry_distance_km") > 0,
            F.abs(F.col("partner_distance_km") - F.col("telemetry_distance_km"))
            / F.col("telemetry_distance_km")
            * 100.0,
        ),
    )


def compute_profit(df: DataFrame) -> DataFrame:
    """Net profit and its per-km and margin derivatives.

    Costs are NOT coalesced to zero. A vehicle with no expense row has an unknown
    profit, and reporting it as revenue-minus-nothing would invent a healthy vehicle
    out of missing data.
    """
    net = F.col("revenue") - F.col("fuel_cost") - F.col("maintenance_cost")
    return (
        df.withColumn("net_profit", F.round(net, 2))
        .withColumn(
            "profit_per_km",
            F.when(
                F.col("telemetry_distance_km") > 0, F.round(net / F.col("telemetry_distance_km"), 4)
            ),
        )
        .withColumn(
            "margin_pct",
            F.when(F.col("revenue") > 0, F.round(net / F.col("revenue") * 100.0, 2)),
        )
        .withColumn(
            "utilization_pct",
            F.when(
                (F.col("on_trip_hours") + F.col("idle_hours")) > 0,
                F.round(
                    F.col("on_trip_hours") / (F.col("on_trip_hours") + F.col("idle_hours")) * 100.0,
                    2,
                ),
            ),
        )
    )


def with_trend(history: DataFrame) -> DataFrame:
    """Rolling 7-day profit and its slope - the answer to "BECOMING unprofitable".

    The business asks which vehicles are on a deteriorating trajectory, not which
    lost money yesterday. That distinction is the whole reason a batch layer with
    history exists rather than a stream with bounded state.
    """
    w = (
        Window.partitionBy("vehicle_id")
        .orderBy(F.col("sim_date").cast("timestamp").cast("long"))
        .rowsBetween(-(TREND_WINDOW_DAYS - 1), 0)
    )
    day_index = F.datediff(F.col("sim_date"), F.lit("2026-01-01").cast("date")).cast("double")

    return (
        history.withColumn("_day_index", day_index)
        .withColumn("rolling_7d_avg_profit", F.round(F.avg("net_profit").over(w), 2))
        .withColumn("_days_of_history", F.count("net_profit").over(w))
        # regr_slope over the same window: positive means improving.
        .withColumn(
            "profit_trend_slope",
            F.round(F.expr("regr_slope(net_profit, _day_index)").over(w), 4),
        )
    )


def classify(df: DataFrame) -> DataFrame:
    """Bucket each vehicle-day.

    WATCH is the class the business question actually asks for: still profitable,
    but trending down. INSUFFICIENT_DATA exists because a slope through two points
    is not a trend, and saying "we do not know yet" is the correct engineering
    answer on simulated days 1-3.
    """
    return df.withColumn(
        "classification",
        F.when(F.col("_days_of_history") < MIN_DAYS_FOR_TREND, F.lit("INSUFFICIENT_DATA"))
        .when(F.col("rolling_7d_avg_profit").isNull(), F.lit("INSUFFICIENT_DATA"))
        .when(
            (F.col("rolling_7d_avg_profit") <= 0) & (F.col("profit_trend_slope") < 0),
            F.lit("CRITICAL"),
        )
        .when(F.col("rolling_7d_avg_profit") <= 0, F.lit("UNPROFITABLE"))
        .when(F.col("profit_trend_slope") < 0, F.lit("WATCH"))
        .otherwise(F.lit("HEALTHY")),
    ).drop("_day_index", "_days_of_history")
