"""The shared read path for every speed-layer query.

All three queries start from the same Kafka topic but run as INDEPENDENT streaming
queries with their own consumer groups and checkpoints. That is deliberate: one
query with three sinks would couple three failure domains, so a Redis timeout would
stall the master-dataset write - and the master dataset is the system of record that
the entire batch layer, and therefore the Lambda argument, depends on.

The cost is that the topic is read three times. At 240 events/second that is
irrelevant, and it buys three separate consumer-lag series in Grafana so a lagging
stage can actually be identified. See plan/04 §4.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fleet.common import config
from fleet.common.serialization import load_schema
from fleet.common.simclock import SimClock
from fleet.common.spark_avro import deserialize_avro_value
from fleet.transforms.validate import validate_telemetry

TELEMETRY_SCHEMA_FILE = "telemetry.v1.avsc"


@dataclass(frozen=True, slots=True)
class QuerySpec:
    """Identity of one streaming query: its consumer group and its checkpoint.

    Kept together because they must never be reused across queries - sharing a
    checkpoint between two queries corrupts both in ways that are very hard to
    diagnose.
    """

    name: str
    consumer_group: str
    checkpoint: str


def read_telemetry(
    spark: SparkSession,
    spec: QuerySpec,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int = 50_000,
) -> DataFrame:
    """Raw Kafka rows for one query."""
    _ = spec  # every query is still identified by its spec; kept in the signature
    k = config.kafka()
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", k.bootstrap)
        .option("subscribe", k.telemetry_topic)
        .option("startingOffsets", starting_offsets)
        # Deliberately NOT setting kafka.group.id.
        #
        # It looks useful - one group per query would give a per-query lag series in
        # kafka-exporter - but it is wrong twice over. Structured Streaming tracks
        # offsets in its OWN checkpoint and never commits them to a consumer group,
        # so that lag series would be permanently stale and misleading. And setting
        # it makes every micro-batch join and leave the group, churning the group
        # coordinator; this killed the DLQ query after ~4,500 micro-batches with
        # "Timeout expired before the position for partition ... could be
        # determined". Spark's own startup warning says as much.
        #
        # Progress is measured instead from StreamingQueryProgress
        # (sources[].startOffset vs latestOffset) via the listener - see plan/09.
        #
        # failOnDataLoss=false: the demo runs a deliberately short retention
        # (plan/04 section 1.4), so a restart after a pause should skip forward
        # rather than kill the job.
        .option("failOnDataLoss", "false")
        # Bound how much one micro-batch may pull. Without a cap, a query starting
        # from `earliest` against a large backlog builds one enormous batch.
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .load()
    )


def decode_telemetry(raw: DataFrame) -> DataFrame:
    """Confluent-framed Avro -> flat columns, keeping Kafka's own metadata.

    `source_partition` and `source_offset` are carried through so a dead-lettered
    event can be traced back to its exact position in the log (plan/04 §6).
    """
    return (
        deserialize_avro_value(raw, load_schema(TELEMETRY_SCHEMA_FILE))
        .select(
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("payload.*"),
        )
        # Avro enums decode as a struct-ish type in some Spark versions; casting to
        # string here means every downstream comparison is against a plain string.
        .withColumn("status", F.col("status").cast("string"))
    )


def validated_telemetry(raw: DataFrame, sim_now: datetime | SimClock) -> DataFrame:
    """Decode and validate in one step. Nothing is filtered - the caller splits.

    Streaming callers MUST pass the SimClock, not `clock.sim_now()`. A captured
    instant goes stale at 288x within about a second and dead-letters the entire
    stream. See `fleet.transforms.clock`.
    """
    return validate_telemetry(decode_telemetry(raw), sim_now)
