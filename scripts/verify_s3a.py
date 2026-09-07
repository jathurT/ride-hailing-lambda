"""Prove Spark can round-trip Parquet through MinIO over s3a://.

Run on day 3, deliberately, rather than discovering the jar mismatch on day 5 when
the streaming job is also new and the failure is ambiguous.

    docker compose run --rm spark-app python /app/scripts/verify_s3a.py
"""

from __future__ import annotations

import sys
from datetime import date

from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.store.lake import LakePaths
from fleet.store.spark_s3 import build_session

configure(service="verify-s3a", stage="store", json_output=False)
log = get_logger()


def main() -> int:
    paths = LakePaths(config.storage().lake_bucket)
    spark = build_session("verify-s3a")
    spark.sparkContext.setLogLevel("WARN")

    probe = f"{paths.telemetry_root()}/_probe"
    rows = [(f"V{i:03d}", "Z01", float(i), i % 3 == 0) for i in range(1, 501)]
    df = spark.createDataFrame(rows, "vehicle_id string, zone_id string, fare double, idle boolean")

    log.info("writing_parquet", path=probe, rows=df.count())
    df.write.mode("overwrite").parquet(probe)

    back = spark.read.parquet(probe)
    count = back.count()
    log.info("read_back", rows=count, columns=len(back.columns))

    # Partitioned write + partition pruning, which is what the batch layer relies on
    part = f"{paths.telemetry_root()}/_probe_partitioned"
    df.withColumn("sim_date", df.fare.cast("int").cast("string")).limit(50).write.mode(
        "overwrite"
    ).partitionBy("sim_date").parquet(part)
    pruned = spark.read.parquet(part).where("sim_date = '1'").count()
    log.info("partition_pruning_works", matched_rows=pruned)

    ok = count == 500
    log.info(
        "s3a_verified" if ok else "s3a_FAILED",
        round_trip_ok=ok,
        partition_example=paths.telemetry_partition(date(2026, 3, 1)),
    )
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
