"""Fleet utilization and earnings by zone and time window.

★ Shared by BOTH layers. The speed layer calls these on a stream; the batch layer
calls the same functions on a static DataFrame read from the Parquet master
dataset. That is what makes "one codebase, two entry points" a fact rather than a
claim (ADR-001).

--------------------------------------------------------------------------------
THE REVENUE TRAP - read before changing anything here.

`fare` on a telemetry ping is the **trip's fare**, repeated unchanged on every ping
of that trip. It is NOT an increment. So:

    df.groupBy(window, zone).agg(F.sum("fare"))      # WRONG

multiplies revenue by however many pings the trip happened to emit - roughly 8x at
a 3-simulated-minute ping interval. Nothing errors; the number is just silently
several times too large, which is the worst kind of bug in a figure that feeds a
profitability report.

Revenue is therefore computed from a **trip-deduplicated** stream, so each trip
contributes its fare exactly once (`deduplicate_trips` below). Because a single
streaming query may contain only one aggregation, activity metrics and earnings are
computed by two separate queries over the same source. That is a real cost of the
correct answer, and it is stated in the report rather than hidden.
--------------------------------------------------------------------------------
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_trips(df: DataFrame, watermark_sim: str) -> DataFrame:
    """One row per trip, so `fare` can be summed without double counting.

    Two filters, and BOTH are required:

    `trip_id IS NOT NULL` drops idle pings, which have no trip and no revenue.

    `fare IS NOT NULL` drops ENROUTE pings. A vehicle heading to a pickup already
    has a trip assigned - that is what makes `trip_id null IFF idle` hold - but the
    meter has not started, so its fare is null. Without this filter,
    `dropDuplicates` keeps an arbitrary row per trip and frequently keeps the
    enroute one, whose null fare sums to zero. Observed live: every zone reported
    0.00 earnings alongside a non-zero completed-trip count, with nothing failing.

    Since `fare` is constant across a trip's on-trip pings, any surviving row gives
    the correct value.

    `dropDuplicatesWithinWatermark`, not `dropDuplicates`. The plain version only
    evicts state when the event-time column is part of the key, and here the key is
    `trip_id` alone, so the watermark bounded nothing: the state store held every
    trip since start-up. Seen live on the pipeline-health dashboard as the one query
    whose state rows only ever went up (25,000 after 40 minutes).

    Spark only guarantees a duplicate is dropped if it is within the delay of the
    first row, so the delay is at least TRIP_DEDUP_MIN_SIM_MINUTES, longer than the
    longest simulated trip. With the 30 minute pipeline watermark a long trip's later
    pings could be counted a second time.

    A static DataFrame has no state to bound (and Spark refuses the watermark
    variant on one), so it takes the plain dedup, with the same result.
    """
    priced = df.filter(F.col("trip_id").isNotNull() & F.col("fare").isNotNull())
    if not df.isStreaming:
        return priced.dropDuplicates(["trip_id"])
    delay = max(_minutes(watermark_sim), TRIP_DEDUP_MIN_SIM_MINUTES)
    return priced.withWatermark("event_time", f"{delay} minutes").dropDuplicatesWithinWatermark(
        ["trip_id"]
    )


# Longer than the longest simulated trip (50 minutes, generators/vehicle.py).
TRIP_DEDUP_MIN_SIM_MINUTES = 60


def _minutes(delay: str) -> int:
    """'30 minutes' -> 30. The pipeline states every delay in simulated minutes."""
    value, unit = delay.split()
    if not unit.startswith("minute"):
        raise ValueError(f"expected a delay in minutes, got {delay!r}")
    return int(value)


def zone_activity(
    df: DataFrame,
    watermark_sim: str,
    window_sim: str,
    slide_sim: str,
) -> DataFrame:
    """Windowed activity per zone: how busy is each area right now?

    A **sliding** window, not tumbling. A tumbling window would only refresh a zone
    once per window width, so the dashboard would jump every 15 simulated minutes
    and show a nearly-empty bucket at the start of each one. Sliding gives a full
    window of context refreshed every 5 simulated minutes. The module taught all
    four window types; this is why we picked this one.

    `approx_count_distinct` (HyperLogLog, ~2% error) rather than exact counting is a
    DELIBERATE approximation. Exact distinct counts are a stateful shuffle that
    grows with cardinality. The module describes a Lambda speed layer as giving
    "immediate but less accurate results" - this is us implementing that trade-off
    knowingly. The batch layer uses exact `countDistinct` over the same data.
    """
    return (
        df.withWatermark("event_time", watermark_sim)
        .groupBy(F.window(F.col("event_time"), window_sim, slide_sim), F.col("zone_id"))
        .agg(
            F.approx_count_distinct("vehicle_id").alias("active_vehicles"),
            F.approx_count_distinct(F.when(F.col("status") == "on_trip", F.col("trip_id"))).alias(
                "trips"
            ),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            (F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)) / F.count("*")).alias(
                "idle_ratio"
            ),
            F.count("*").alias("pings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "active_vehicles",
            "trips",
            F.round("avg_speed_kmh", 1).alias("avg_speed_kmh"),
            F.round("idle_ratio", 4).alias("idle_ratio"),
            "pings",
        )
    )


def zone_earnings(
    df: DataFrame,
    watermark_sim: str,
    window_sim: str,
    slide_sim: str,
) -> DataFrame:
    """Windowed earnings per zone, from a trip-deduplicated input.

    The caller MUST pass a DataFrame that has been through `deduplicate_trips`,
    or revenue will be inflated by the ping count. See the module docstring.
    """
    return (
        df.groupBy(F.window(F.col("event_time"), window_sim, slide_sim), F.col("zone_id"))
        .agg(
            F.sum("fare").alias("earnings"),
            F.count("*").alias("completed_trips"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            F.round("earnings", 2).alias("earnings"),
            "completed_trips",
        )
    )


def trip_completions(df: DataFrame) -> DataFrame:
    """One row per trip, chosen deterministically: the LAST on-trip ping.

    The batch counterpart of `deduplicate_trips`, and it differs in two ways that
    both matter.

    It takes no watermark. A watermark bounds state on an unbounded stream; a batch
    DataFrame is finite and already complete, so `withWatermark` here would be
    decoration at best and a silent row-dropper at worst.

    It picks the last ping rather than an arbitrary one. `dropDuplicates` is fine in
    `daily_revenue`, which groups by `vehicle_id` - a field that cannot change
    within a trip. It is NOT fine here, because we group by `zone_id` and by hour,
    and a trip crosses both. Picking arbitrarily would attribute a fare to whichever
    zone the shuffle happened to surface, so the same input could produce different
    zone totals on different runs. The last ping is the trip's completion, which is
    also the revenue-recognition rule the serving layer publishes to clients
    (plan/07 section 5.5: "earnings recognised at trip completion").

    The fleet-wide total is unaffected either way - the same trips, the same fares -
    so `sum(fact_zone_hourly.earnings)` must equal `sum(fact_vehicle_daily_pnl.revenue)`
    to the penny for a simulated date. Two independent paths to one number; if they
    disagree, one of them is wrong.
    """
    ordered = Window.partitionBy("trip_id").orderBy(F.col("event_time").desc())
    return (
        df.filter(F.col("trip_id").isNotNull() & F.col("fare").isNotNull())
        .withColumn("_rank", F.row_number().over(ordered))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def zone_hourly_exact(df: DataFrame) -> DataFrame:
    """Per-zone, per-hour facts for the batch view - the EXACT counterpart of what
    `zone_activity` and `zone_earnings` approximate on the stream.

    Three deliberate differences from the speed-layer pair above, and they are the
    Lambda accuracy trade-off made visible in one file rather than argued in prose:

    1. `countDistinct`, not `approx_count_distinct`. The speed layer accepts ~2%
       HyperLogLog error to keep a bounded state store; the batch layer has the
       whole day in hand and no state to bound, so it pays the shuffle and returns
       the true count.
    2. Tumbling hours, not sliding windows. A sliding window exists to keep a live
       dashboard smooth. A daily report wants each hour counted once.
    3. Activity and earnings are computed here and joined. On a stream they must be
       two queries, because Structured Streaming permits one aggregation per query.
       A batch job has no such restriction, and the join is cheap at 12 zones x 24
       hours.

    A `left` join, not `inner`: an hour can have telemetry and no completed trips -
    a quiet 4am hour with vehicles idling is real data, not a missing row. Inner
    would drop it and understate the fleet's idle time, which is precisely the
    figure the utilization report turns on.
    """
    keys = [F.col("zone_id"), F.hour("event_time").alias("sim_hour")]

    activity = df.groupBy(*keys).agg(
        F.countDistinct("vehicle_id").alias("active_vehicles"),
        F.countDistinct(F.when(F.col("status") == "on_trip", F.col("trip_id"))).alias("trips"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        (F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)) / F.count("*")).alias(
            "idle_ratio"
        ),
    )

    earnings = trip_completions(df).groupBy(*keys).agg(F.sum("fare").alias("earnings"))

    return activity.join(earnings, on=["zone_id", "sim_hour"], how="left").select(
        "zone_id",
        F.col("sim_hour").cast("int").alias("sim_hour"),
        "active_vehicles",
        "trips",
        F.round(F.coalesce(F.col("earnings"), F.lit(0.0)), 2).alias("earnings"),
        F.round("idle_ratio", 4).alias("idle_ratio"),
        F.round("avg_speed_kmh", 1).alias("avg_speed_kmh"),
    )


def fleet_snapshot(activity: DataFrame) -> DataFrame:
    """Roll zone-level activity up to one fleet-wide row per window."""
    return (
        activity.groupBy("window_start", "window_end")
        .agg(
            F.sum("active_vehicles").alias("total_active"),
            F.sum("trips").alias("total_trips"),
            F.avg("idle_ratio").alias("fleet_idle_ratio"),
            F.avg("avg_speed_kmh").alias("fleet_avg_speed_kmh"),
        )
        .select(
            "window_start",
            "window_end",
            "total_active",
            "total_trips",
            F.round("fleet_idle_ratio", 4).alias("fleet_idle_ratio"),
            F.round("fleet_avg_speed_kmh", 1).alias("fleet_avg_speed_kmh"),
        )
    )
