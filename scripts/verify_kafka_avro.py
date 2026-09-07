"""Prove Spark can read the producer's Confluent-framed Avro from Kafka.

Settled on day 4, before any streaming logic, for the same reason s3a was settled on
day 3: when this is wrong the symptom is a column of nulls and empty aggregates, not
an exception - so debugging it alongside brand-new windowing code would be miserable.

    make verify-kafka
"""

from __future__ import annotations

import sys

from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.common.serialization import load_schema
from fleet.common.spark_avro import deserialize_avro_value
from fleet.store.spark_s3 import build_session

configure(service="verify-kafka-avro", stage="process", json_output=False)
log = get_logger()


def main() -> int:
    k = config.kafka()
    spark = build_session("verify-kafka-avro")
    spark.sparkContext.setLogLevel("WARN")

    # Batch read of a bounded slice - simplest possible thing that exercises the
    # whole decode path without involving triggers, watermarks or checkpoints.
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", k.bootstrap)
        .option("subscribe", k.telemetry_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .limit(2000)
    )
    total = raw.count()
    log.info("kafka_batch_read", topic=k.telemetry_topic, rows=total)
    if total == 0:
        log.error("no_messages", hint="is the telemetry producer running?")
        return 1

    decoded = deserialize_avro_value(raw, load_schema("telemetry.v1.avsc")).selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "partition",
        "payload.*",
    )
    decoded.cache()

    nulls = decoded.filter("vehicle_id IS NULL").count()
    ok = decoded.count() - nulls
    log.info("avro_decoded", decoded=ok, failed=nulls)

    log.info("sample")
    decoded.select(
        "kafka_key",
        "partition",
        "vehicle_id",
        "status",
        "lat",
        "lon",
        "speed_kmh",
        "fare",
        "event_time",
    ).show(5, truncate=False)

    # The message KEY must equal the vehicle_id inside the payload. If it does not,
    # per-vehicle ordering is not what we think it is and the idle detector is wrong.
    mismatched = decoded.filter("kafka_key != vehicle_id").count()
    log.info("key_matches_payload", mismatched=mismatched)

    # Both time bases must survive the round trip.
    from pyspark.sql.functions import max as smax
    from pyspark.sql.functions import min as smin

    span = decoded.select(
        smin("event_time").alias("first_sim"), smax("event_time").alias("last_sim")
    ).collect()[0]
    log.info("event_time_span", first=str(span["first_sim"]), last=str(span["last_sim"]))

    statuses = {
        r["status"]: r["n"]
        for r in decoded.groupBy("status").count().withColumnRenamed("count", "n").collect()
    }
    log.info("status_distribution", **statuses)

    healthy = nulls == 0 and mismatched == 0 and ok > 0
    log.info(
        "kafka_avro_verified" if healthy else "kafka_avro_FAILED",
        decoded=ok,
        decode_failures=nulls,
        key_mismatches=mismatched,
    )
    spark.stop()
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
