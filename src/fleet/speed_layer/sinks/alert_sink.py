"""Q3 sink - business alerts to Kafka, and to Redis for the API.

These are **business** alerts, distinct from pipeline-health alerts, which go to
Alertmanager instead. Conflating the two is a design failure: a dispatcher paged
about Kafka consumer lag is noise, and an SRE paged about a vehicle idling is
negligence. The distinction is a report point (plan/09 §4).

Written to Kafka in Confluent wire format so they are readable in Kafka UI and by
any registry-aware consumer, and mirrored into a Redis sorted set so the serving
API can answer "most recent idle alerts" without touching Kafka.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.streaming import StreamingQuery

from fleet.common import config, metrics
from fleet.common.logging import get_logger
from fleet.common.serialization import load_schema
from fleet.common.simclock import SimClock
from fleet.common.spark_avro import confluent_envelope
from fleet.speed_layer.source import QuerySpec

ALERT_SCHEMA_FILE = "alert.v1.avsc"
log = get_logger()


def to_alert_envelope(alerts: DataFrame) -> DataFrame:
    """Shape idle alerts into the shared business-alert schema."""
    return alerts.select(
        F.col("vehicle_id").alias("_key"),
        F.struct(
            F.col("alert_id"),
            F.col("vehicle_id"),
            F.lit("VEHICLE_IDLE_PROLONGED").alias("alert_type"),
            F.col("severity"),
            F.concat(
                F.lit("idle for "),
                F.round("idle_minutes", 0).cast("int").cast("string"),
                F.lit(" simulated minutes since "),
                F.date_format("idle_since_sim", "yyyy-MM-dd HH:mm"),
            ).alias("detail"),
            F.col("zone_id"),
            F.col("idle_minutes").alias("value"),
            F.lit(None).cast("double").alias("threshold"),
            F.col("raised_at_sim"),
            F.current_timestamp().alias("raised_at_real"),
            # Which Lambda layer raised it. Idle alerts come from the speed layer;
            # profitability alerts will come from the batch layer. Under Lambda,
            # provenance is part of the contract.
            F.lit("speed").alias("source_layer"),
        ).alias("envelope"),
    )


def make_alert_writer(schema_id: int, redis_url: str, clock: SimClock) -> Any:
    """One foreachBatch writing alerts to BOTH Kafka and Redis.

    Deliberately ONE query rather than two `writeStream` calls on the same
    DataFrame. Two writeStreams would each re-run the upstream plan - including the
    stateful detector - giving two independent state stores computing the same thing
    from the same input. Wasteful, and a needless second place for state to live.

    Kafka is written FIRST because it is the durable record; Redis is a serving
    cache that can be rebuilt from it. If Redis fails after Kafka succeeds, the
    batch retries and re-writes both - harmless, because `alert_id` identifies the
    episode rather than the delivery, so a repeated alert is recognisably the same
    incident (see transforms/idle.py).
    """
    k = config.kafka()
    payload = to_avro(F.col("envelope"), load_schema(ALERT_SCHEMA_FILE))

    def write(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df = batch_df.persist()  # a static DataFrame here; two sinks read it
        try:
            (
                to_alert_envelope(batch_df)
                .select(
                    F.col("_key").cast("string").alias("key"),
                    confluent_envelope(payload, schema_id).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", k.bootstrap)
                .option("topic", k.alerts_topic)
                .save()
            )
            _mirror_to_redis(batch_df, batch_id, redis_url, clock)
        finally:
            batch_df.unpersist()

    return write


def start_alert_query(
    alerts: DataFrame, spec: QuerySpec, writer: Any, trigger_seconds: int
) -> StreamingQuery:
    return (
        alerts.writeStream.queryName(spec.name)
        .outputMode("append")
        .foreachBatch(writer)
        .option("checkpointLocation", spec.checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def _mirror_to_redis(batch_df: DataFrame, batch_id: int, redis_url: str, clock: SimClock) -> None:
    """Mirror alerts into Redis so the serving API can answer "recent idle alerts"
    without reading Kafka."""
    import redis

    from fleet.store.speed_view import SpeedView

    rows = batch_df.collect()  # safe: alerts are rare by construction
    if not rows:
        return

    view = SpeedView(redis.from_url(redis_url, decode_responses=True), clock)
    for r in rows:
        view.push_idle_alert(
            json.dumps(
                {
                    "alert_id": r["alert_id"],
                    "vehicle_id": r["vehicle_id"],
                    "severity": r["severity"],
                    "idle_minutes": float(r["idle_minutes"]),
                    "zone_id": r["zone_id"],
                    "raised_at_sim": str(r["raised_at_sim"]),
                }
            ),
            score=r["raised_at_sim"].timestamp(),
        )

    # Counted per severity, not as one lump. A batch of 40 WARNING alerts and a
    # batch of 40 CRITICAL ones are the same number on a single counter, and the
    # second is the one somebody has to act on.
    for severity, n in Counter(r["severity"] for r in rows).items():
        metrics.business_alerts_emitted_total.labels(
            type="VEHICLE_IDLE_PROLONGED", severity=severity
        ).inc(n)
    log.warning(
        "idle_alerts_raised",
        batch_id=batch_id,
        count=len(rows),
        vehicles=[r["vehicle_id"] for r in rows][:5],
    )
