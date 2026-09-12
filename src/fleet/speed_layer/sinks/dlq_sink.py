"""The dead-letter path.

Cleaning that silently drops data is not observable and not good engineering. Every
rejected event is written here with enough context to trace it back to its exact
position in the source log, so the DLQ is a diagnostic tool rather than a bin.

Written back to Kafka in Confluent wire format, not bare Avro, so the envelopes stay
readable in Kafka UI. A DLQ nobody can inspect is worse than no DLQ, because it
creates the appearance of diligence without the substance.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.streaming import StreamingQuery

from fleet.common import config
from fleet.common.serialization import load_schema
from fleet.common.spark_avro import confluent_envelope
from fleet.speed_layer.source import QuerySpec

DLQ_SCHEMA_FILE = "dlq.v1.avsc"


def build_envelopes(rejected: DataFrame, source_topic: str) -> DataFrame:
    """Shape rejected rows into the DLQ envelope schema."""
    return rejected.select(
        F.col("vehicle_id").alias("_key"),
        F.struct(
            # The original bytes are not recoverable at this point in the pipeline
            # (we decoded before validating), so we store the decoded event's
            # identity instead and say so, rather than storing an empty field that
            # implies we kept something we did not.
            F.col("event_id").cast("binary").alias("original_payload"),
            F.col("rejection_reason").alias("rejection_reason"),
            F.concat_ws(
                " ",
                F.lit("event_id="),
                F.col("event_id"),
                F.lit("status="),
                F.col("status"),
                F.lit("lat="),
                F.col("lat").cast("string"),
                F.lit("speed="),
                F.col("speed_kmh").cast("string"),
                F.lit("fare="),
                F.col("fare").cast("string"),
            ).alias("rejection_detail"),
            F.lit("validate_telemetry").alias("validator"),
            F.col("vehicle_id").alias("vehicle_id"),
            F.col("event_time").alias("rejected_at_sim"),
            F.current_timestamp().alias("rejected_at_real"),
            F.lit(source_topic).alias("source_topic"),
            F.col("source_partition").cast("int").alias("source_partition"),
            F.col("source_offset").cast("long").alias("source_offset"),
            F.lit(None).cast("string").alias("trace_id"),
        ).alias("envelope"),
    )


def write_dlq(rejected: DataFrame, spec: QuerySpec, schema_id: int) -> StreamingQuery:
    k = config.kafka()
    envelopes = build_envelopes(rejected, k.telemetry_topic)
    payload = to_avro(F.col("envelope"), load_schema(DLQ_SCHEMA_FILE))

    return (
        envelopes.select(
            F.col("_key").cast("string").alias("key"),
            confluent_envelope(payload, schema_id).alias("value"),
        )
        .writeStream.format("kafka")
        .queryName(spec.name)
        .option("kafka.bootstrap.servers", k.bootstrap)
        .option("topic", k.dlq_topic)
        .option("checkpointLocation", spec.checkpoint)
        .outputMode("append")
        .start()
    )
