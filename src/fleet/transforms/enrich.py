"""Enrichment: attaching zone and vehicle context to raw telemetry.

Both joins here are **broadcast** joins against small static tables. Broadcasting
sends one copy of the table to every executor and turns the join into a local
lookup with no shuffle - correct when one side is tiny (12 zones, 150 vehicles) and
essential when the other side is a stream.

Zone assignment in particular is a *range* join on a bounding box, not an equality
join. Expressing it as a range condition over a broadcast table keeps it in the JVM;
a Python UDF doing the same lookup would serialise every row across the JVM/Python
boundary and defeat Catalyst (plan/05 §3.2).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fleet.common.zones import as_broadcast_rows

ZONE_SCHEMA = (
    "zone_id string, zone_name string, zone_class string, "
    "lat_min double, lat_max double, lon_min double, lon_max double, demand_weight double"
)


def zone_table(spark: SparkSession) -> DataFrame:
    """The 12 zones as a DataFrame ready to broadcast."""
    return spark.createDataFrame(as_broadcast_rows(), schema=ZONE_SCHEMA)


def with_zone(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach `zone_id`, `zone_name`, `zone_class` by lat/lon bounding box.

    LEFT join on purpose: a point outside every zone keeps its row with a null
    `zone_id` rather than vanishing. Silently dropping out-of-bounds events would
    hide a data-quality problem and quietly understate fleet totals.
    """
    z = F.broadcast(zones)
    condition = (
        (df["lat"] >= z["lat_min"])
        & (df["lat"] < z["lat_max"])
        & (df["lon"] >= z["lon_min"])
        & (df["lon"] < z["lon_max"])
    )
    return df.join(z, condition, how="left").drop("lat_min", "lat_max", "lon_min", "lon_max")


def with_vehicle_registry(df: DataFrame, registry: DataFrame) -> DataFrame:
    """Attach `fuel_type`, `model`, `home_zone` from the compacted registry topic.

    LEFT join for the same reason: a vehicle absent from the registry is a real
    operational signal (an unregistered vehicle reporting telemetry), so it is
    counted rather than discarded.
    """
    r = F.broadcast(
        registry.select(
            F.col("vehicle_id").alias("reg_vehicle_id"),
            "fuel_type",
            "model",
            F.col("home_zone").alias("registry_home_zone"),
        )
    )
    return df.join(r, df["vehicle_id"] == r["reg_vehicle_id"], how="left").drop("reg_vehicle_id")


def latest_per_key(df: DataFrame, key: str, order_by: str = "offset") -> DataFrame:
    """Collapse a compacted topic read to one row per key.

    Compaction guarantees only the *latest* value survives eventually; until the
    compactor runs, several versions of a key can still be present in the log. Taking
    the highest offset per key makes the read correct regardless of compaction
    timing - a subtlety that otherwise produces intermittently duplicated joins.
    """
    from pyspark.sql.window import Window

    w = Window.partitionBy(key).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
