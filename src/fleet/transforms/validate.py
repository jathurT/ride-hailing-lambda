"""Validation: separating events we can trust from events we cannot.

★ This package is the answer to the module's stated Lambda weakness - "managing
multiple codebases". Every function here is a pure DataFrame -> DataFrame
transformation with no streaming or batch concepts inside it, so the SAME tested
function is imported by `speed_layer/streaming_job.py` and by
`batch_layer/daily_profitability.py`. Two entry points over one library, not two
implementations. See ADR-001 and plan/01 §2.3.

The rule that keeps that true: nothing in `fleet.transforms` may import Kafka,
Redis, psycopg, boto3 or `pyspark.sql.streaming`. `test_transforms_are_pure`
enforces it.

Validation philosophy: we reject the **impossible**, never the merely unusual. A
vehicle reporting 3 km/h in traffic is a real observation; a vehicle reporting
247 km/h is a broken sensor. An over-eager filter would delete signal.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fleet.common.simclock import SimClock
from fleet.common.zones import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN
from fleet.transforms.clock import as_sim_now_column

MAX_PLAUSIBLE_SPEED_KMH = 200.0
# 60 simulated minutes is 12.5 real seconds at 288x. Spark stamps a micro-batch's
# time before it fetches the batch's Kafka offsets, so under load an event can look
# up to about 11 real seconds newer than "now". The first value, 5 minutes (about
# one real second), dead-lettered 98 real events on the rerun. The injected
# clock-skew defect is 120 simulated minutes ahead, so it is still caught.
FUTURE_TOLERANCE_SIM_MINUTES = 60

# Checked in order; the first match wins, so the reported reason is the most
# specific one that applies rather than whichever happened to be evaluated last.
REJECTION_REASONS = (
    "NULL_COORDINATES",
    "COORDINATES_OUT_OF_BOUNDS",
    "NEGATIVE_FARE",
    "IMPLAUSIBLE_SPEED",
    "FUTURE_TIMESTAMP",
    "STATUS_TRIP_MISMATCH",
    "MISSING_FARE_ON_TRIP",
)


def validate_telemetry(df: DataFrame, sim_now: datetime | SimClock | Column) -> DataFrame:
    """Add `is_valid` and `rejection_reason` columns.

    Args:
        df: decoded telemetry with the v1 schema's columns.
        sim_now: the simulated "now" to compare event times against. Pass a
            **SimClock** in a streaming job so the reference advances with the
            stream; pass a **datetime** in a batch job or a test where a fixed
            reference is what you want. Passing a fixed datetime to a long-running
            stream rejects everything - see `fleet.transforms.clock`.

    Nothing is dropped here. The caller splits on `is_valid` and routes the
    remainder to the dead-letter queue, so every rejection stays countable and
    traceable (plan/04 §6).
    """
    future_cutoff = as_sim_now_column(sim_now) + F.expr(
        f"INTERVAL {FUTURE_TOLERANCE_SIM_MINUTES} MINUTES"
    )

    reason = (
        F.when(F.col("lat").isNull() | F.col("lon").isNull(), F.lit("NULL_COORDINATES"))
        .when(
            ~F.col("lat").between(LAT_MIN, LAT_MAX) | ~F.col("lon").between(LON_MIN, LON_MAX),
            F.lit("COORDINATES_OUT_OF_BOUNDS"),
        )
        .when(F.col("fare").isNotNull() & (F.col("fare") < 0), F.lit("NEGATIVE_FARE"))
        .when(
            F.col("speed_kmh").isNotNull()
            & (~F.col("speed_kmh").between(0.0, MAX_PLAUSIBLE_SPEED_KMH)),
            F.lit("IMPLAUSIBLE_SPEED"),
        )
        .when(F.col("event_time") > future_cutoff, F.lit("FUTURE_TIMESTAMP"))
        # trip_id must be null exactly when idle. The simulator guarantees this, so a
        # violation here means either a producer bug or a genuinely corrupt message -
        # both worth surfacing rather than silently accepting.
        .when(
            F.col("trip_id").isNull() != (F.col("status") == "idle"),
            F.lit("STATUS_TRIP_MISMATCH"),
        )
        .when(
            (F.col("status") == "on_trip") & F.col("fare").isNull(),
            F.lit("MISSING_FARE_ON_TRIP"),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    return df.withColumn("rejection_reason", reason).withColumn(
        "is_valid", F.col("rejection_reason").isNull()
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition a validated DataFrame into (accepted, rejected)."""
    return df.filter(F.col("is_valid")), df.filter(~F.col("is_valid"))


def deduplicate(df: DataFrame, watermark_sim: str) -> DataFrame:
    """Drop repeat `event_id`s within the watermark.

    The producer buffers locally and replays on reconnect, so genuine duplicates
    occur. Bounding the dedup state by the watermark is what stops it growing
    without limit in a long-running stream.
    """
    return df.withWatermark("event_time", watermark_sim).dropDuplicates(["event_id"])
