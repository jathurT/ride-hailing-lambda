"""The batch layer: exact per-zone, per-hour facts.

The counterpart of the speed layer's Redis zone view. Same source data, same shared
transform module, different accuracy contract:

    Parquet master dataset (one sim_date partition)
              |
    zone_hourly_exact()   <- countDistinct, tumbling hours
              |
    UPSERT into mart.fact_zone_hourly

This job is what makes the merged utilization endpoint possible. Without it the
batch half of `/api/v1/fleet/utilization` has no rows and the serving layer can only
ever answer from the speed view - which would make the whole merge argument
theoretical (plan/07 section 5).

It also makes the divergence measurement in plan/07 section 5.4 possible: two
independent computations of the same quantity, one approximate and one exact, over
identical input. A speed-vs-batch delta of exactly zero would be evidence that one
of them is reading the other rather than computing it.

Deliberately NOT part of `daily_profitability`. They fail independently - a zone
rollup that breaks should not block the P&L that the report is built on - and the
watermark belongs to the P&L job alone, because that is the table the serving-layer
contract is written against.

Run:
    python -m fleet.batch_layer.zone_hourly --sim-date 2026-03-02
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import date

from fleet.batch_layer.reader import read_telemetry
from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.store.lake import LakePaths
from fleet.store.mart import ZoneHourlyRow, upsert_zone_hourly
from fleet.store.spark_s3 import build_session
from fleet.transforms.enrich import with_zone, zone_table
from fleet.transforms.utilization import zone_hourly_exact

log = get_logger()


def run(sim_date: date, job_run_id: str) -> int:
    storage = config.storage()
    paths = LakePaths(storage.lake_bucket)
    spark = build_session(f"fleet-zone-hourly-{sim_date}")
    spark.sparkContext.setLogLevel("WARN")

    import psycopg

    try:
        events = read_telemetry(spark, paths, sim_date)

        # The master dataset stores RAW validated events - `zone_id` is not in it.
        # Zone is derived from lat/lon at read time, by the SAME `with_zone` the speed
        # layer calls in `_valid_enriched`. That is the "one codebase, two entry
        # points" claim being true rather than asserted: identical enrichment,
        # identical zone boundaries, one place to change them.
        #
        # Deriving zone at query time rather than storing it is also what makes the
        # master dataset re-interpretable: if the zone grid is ever redrawn, every
        # historical day can be recomputed under the new boundaries. A zone_id baked
        # into Parquet would freeze the old grid into the raw data for ever.
        enriched = with_zone(events, zone_table(spark))

        # 12 zones x 24 hours is 288 rows at most. Collecting through the driver is
        # the same call made and justified in `daily_profitability`: the upsert needs
        # ON CONFLICT, which Spark's JDBC writer cannot express.
        computed = zone_hourly_exact(enriched).collect()

        rows = [
            ZoneHourlyRow(
                zone_id=r["zone_id"],
                sim_date=sim_date,
                sim_hour=int(r["sim_hour"]),
                job_run_id=job_run_id,
                trips=int(r["trips"] or 0),
                earnings=float(r["earnings"] or 0.0),
                active_vehicles=int(r["active_vehicles"] or 0),
                idle_ratio=r["idle_ratio"],
                avg_speed_kmh=r["avg_speed_kmh"],
            )
            for r in computed
        ]

        with psycopg.connect(storage.postgres_dsn, autocommit=False) as conn, conn.cursor() as cur:
            upsert_zone_hourly(cur, rows)
            conn.commit()

        # Totals are logged, not just the row count. A row count only says the job
        # ran; the earnings total is checkable against the P&L job's revenue for the
        # same date, and that comparison is what would catch a dedup regression.
        log.info(
            "zone_hourly_complete",
            sim_date=sim_date.isoformat(),
            rows_upserted=len(rows),
            zones=len({r.zone_id for r in rows}),
            hours=len({r.sim_hour for r in rows}),
            trips_total=sum(r.trips for r in rows),
            earnings_total=round(sum(r.earnings for r in rows), 2),
            job_run_id=job_run_id,
        )
        return 0
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Exact per-zone hourly batch view")
    parser.add_argument("--sim-date", required=True, help="simulated date, YYYY-MM-DD")
    parser.add_argument("--job-run-id", default=None)
    args = parser.parse_args(argv)

    obs = config.observability()
    configure(
        service="batch-zone-hourly",
        stage="process",
        level=obs.log_level,
        json_output=obs.log_json,
    )

    return run(
        date.fromisoformat(args.sim_date),
        args.job_run_id or f"manual-{uuid.uuid4().hex[:8]}",
    )


if __name__ == "__main__":
    sys.exit(main())
