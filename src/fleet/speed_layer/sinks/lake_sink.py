"""Q1 - the master dataset writer.

★ The most important query in the system. Everything the batch layer computes, and
therefore the entire "Lambda beats Kappa for reprocessing" argument, rests on this
Parquet lake being complete and immutable. Its alert (`MasterDatasetWriteStalled`)
carries the highest severity in the alert rules for that reason.

Two settings that are not defaults and matter:

`partitionBy("sim_date")` - Hive-style partitioning is what lets the batch job read
one simulated day without scanning the lake, and what makes "recompute 2026-03-02" a
path rather than a query.

`trigger(processingTime="10 seconds")` - the default trigger fires as fast as it can
and would produce hundreds of tiny Parquet files an hour. The small-file problem
makes the batch job slow and the lake unpleasant to inspect. Ten seconds is roughly
2,400 events per file at our rate.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from fleet.speed_layer.source import QuerySpec

# Kafka bookkeeping and validation flags are not part of the master dataset: it
# holds the events as they were observed, not our opinion of them.
DROP_COLUMNS = ("is_valid", "rejection_reason", "kafka_timestamp")


def write_master_dataset(
    validated: DataFrame, path: str, spec: QuerySpec, trigger_seconds: int = 10
) -> StreamingQuery:
    """Append valid events to the Parquet lake, partitioned by simulated date."""
    to_write = (
        validated.filter(F.col("is_valid"))
        .withColumn("sim_date", F.to_date(F.col("event_time")))
        .drop(*DROP_COLUMNS)
    )
    return (
        to_write.writeStream.format("parquet")
        .queryName(spec.name)
        .option("path", path)
        .option("checkpointLocation", spec.checkpoint)
        .partitionBy("sim_date")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
