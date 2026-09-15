"""Reading the immutable master dataset back out of the lake.

Shared by every batch job, because both of them ask the lake the same question and
a second copy of `spark.read.parquet(...)` is a second place for the partition
layout to be wrong. `daily_profitability` re-exports these names so the job reads
as one piece.

Nothing here writes. The master dataset is append-only by design (ADR-002): the
batch layer recomputes FROM it and never edits it, which is what makes a
restatement a recomputation rather than a repair.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fleet.store.lake import LakePaths

EXPENSE_SCHEMA = (
    "vehicle_id string, fuel_cost double, maintenance_cost double, "
    "distance_covered double, service_flag boolean, submitted_at string, partner_id string"
)


def read_telemetry(spark: Any, paths: LakePaths, sim_date: date) -> Any:
    """One simulated day out of the master dataset.

    Reading the partition path directly rather than filtering the whole lake is the
    partition pruning the layout exists for: the batch job touches one directory,
    not the entire history.

    `sim_date` is added back as a column because it lives in the directory name and
    Spark will not infer it when the partition path is addressed directly.
    """
    from pyspark.sql import functions as F

    df = spark.read.parquet(paths.telemetry_partition(sim_date))
    return df.withColumn("sim_date", F.lit(sim_date).cast("date"))


def read_expenses(spark: Any, paths: LakePaths, sim_date: date) -> Any:
    """The partner's daily expense CSV - the second, independent source.

    The schema is declared rather than inferred. Inference would read the file
    twice and, worse, would let a day where every `fuel_cost` happens to be whole
    arrive as a long column, so the join silently changes type between days.
    """
    from pyspark.sql import functions as F

    return (
        spark.read.option("header", "true")
        .schema(EXPENSE_SCHEMA)
        .csv(paths.expense_uri(sim_date))
        .withColumn("sim_date", F.lit(sim_date).cast("date"))
        .withColumnRenamed("distance_covered", "partner_distance_km")
        .drop("submitted_at")
    )
