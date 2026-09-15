"""The batch layer: per-vehicle daily profitability reconciliation.

Answers the second half of the business question - which vehicles are BECOMING
unprofitable once yesterday's fuel and maintenance costs are factored in.

    Parquet master dataset (one sim_date partition)
              +
    daily expense CSV from the partner
              |
         FULL OUTER JOIN on vehicle_id
              |
    net profit -> 7-day rolling trend -> classification
              |
         UPSERT into mart.fact_vehicle_daily_pnl
              |
         advance mart.batch_high_water_mark   <- LAST, and only on success

★ All business logic is imported from `fleet.transforms` - the SAME functions the
speed layer calls. That is the answer to the module's "managing multiple codebases"
criticism of Lambda, and it is checkable: `test_layer_imports_from_transforms`.

Spark reads the lake; psycopg reads and writes the mart. The upsert needs
ON CONFLICT, which Spark's JDBC writer cannot express, and at 150 vehicles x 7 days
the result set is ~1,000 rows - small enough to move through the driver, and
commented as such at the point where it happens.

Run:
    python -m fleet.batch_layer.daily_profitability --sim-date 2026-03-02
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import date, timedelta
from typing import Any

from fleet.batch_layer.reader import read_expenses, read_telemetry
from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.store.lake import LakePaths
from fleet.store.mart import PnlRow, advance_high_water_mark, upsert_pnl
from fleet.store.spark_s3 import build_session
from fleet.transforms.profitability import (
    TREND_WINDOW_DAYS,
    classify,
    compute_profit,
    daily_revenue,
    join_with_expenses,
    reconstruct_trips,
    with_trend,
)

log = get_logger()


def read_recent_history(cur: Any, sim_date: date, days: int) -> list[dict]:
    """Trailing net-profit history from the mart, for the trend calculation.

    The trend is recomputed across the whole trailing window rather than only for
    today, so that a RESTATEMENT of a past day propagates into the trend of every
    later day - which is the point of having a batch layer that can recompute.
    """
    cur.execute(
        """SELECT vehicle_id, sim_date, net_profit
             FROM mart.fact_vehicle_daily_pnl
            WHERE sim_date BETWEEN %s AND %s
              AND net_profit IS NOT NULL""",
        (sim_date - timedelta(days=days - 1), sim_date - timedelta(days=1)),
    )
    return [{"vehicle_id": v, "sim_date": d, "net_profit": float(p)} for v, d, p in cur.fetchall()]


def run(sim_date: date, job_run_id: str, advance_watermark: bool = True) -> int:
    """Compute and upsert one simulated day.

    `advance_watermark=False` is the restatement path (plan/08 section 4). Re-running
    an old date must change that date's VALUES without touching the promise about
    how far the batch layer has got. The GREATEST() guard in `ADVANCE_HWM_SQL` already
    stops the watermark moving backwards; this flag is what stops a backfill of day 2,
    run after day 9 has completed, from being treated as fresh progress.
    """
    storage = config.storage()
    paths = LakePaths(storage.lake_bucket)
    spark = build_session(f"fleet-batch-{sim_date}")
    spark.sparkContext.setLogLevel("WARN")

    import psycopg

    try:
        events = read_telemetry(spark, paths, sim_date)
        expenses = read_expenses(spark, paths, sim_date)

        telemetry = reconstruct_trips(events).join(
            daily_revenue(events), on=["vehicle_id", "sim_date"], how="left"
        )
        joined = compute_profit(join_with_expenses(telemetry, expenses))
        today = joined.collect()  # ~150 rows; see the module docstring
        log.info(
            "day_computed",
            sim_date=sim_date.isoformat(),
            vehicles=len(today),
            matched=sum(1 for r in today if r["expense_status"] == "MATCHED"),
            missing_expense=sum(1 for r in today if r["expense_status"] == "MISSING_EXPENSE"),
            missing_telemetry=sum(1 for r in today if r["expense_status"] == "MISSING_TELEMETRY"),
        )

        with psycopg.connect(storage.postgres_dsn, autocommit=False) as conn, conn.cursor() as cur:
            history = read_recent_history(cur, sim_date, TREND_WINDOW_DAYS)

            profit_rows = [
                {
                    "vehicle_id": r["vehicle_id"],
                    "sim_date": r["sim_date"],
                    "net_profit": float(r["net_profit"]),
                }
                for r in today
                if r["net_profit"] is not None
            ]
            trend_by_vehicle: dict[tuple[str, date], Any] = {}
            if profit_rows:
                combined = spark.createDataFrame(history + profit_rows)
                for r in classify(with_trend(combined)).collect():
                    trend_by_vehicle[(r["vehicle_id"], r["sim_date"])] = r

            rows = []
            for r in today:
                trend = trend_by_vehicle.get((r["vehicle_id"], r["sim_date"]))
                rows.append(
                    PnlRow(
                        vehicle_id=r["vehicle_id"],
                        sim_date=r["sim_date"],
                        job_run_id=job_run_id,
                        trips=int(r["trips"] or 0),
                        revenue=float(r["revenue"] or 0.0),
                        telemetry_distance_km=float(r["telemetry_distance_km"] or 0.0),
                        paid_distance_km=float(r["paid_distance_km"] or 0.0),
                        deadhead_distance_km=float(r["deadhead_distance_km"] or 0.0),
                        on_trip_hours=float(r["on_trip_hours"] or 0.0),
                        idle_hours=float(r["idle_hours"] or 0.0),
                        utilization_pct=r["utilization_pct"],
                        fuel_cost=r["fuel_cost"],
                        maintenance_cost=r["maintenance_cost"],
                        partner_distance_km=r["partner_distance_km"],
                        service_flag=r["service_flag"],
                        partner_id=r["partner_id"],
                        expense_status=r["expense_status"],
                        distance_variance_pct=r["distance_variance_pct"],
                        net_profit=r["net_profit"],
                        profit_per_km=r["profit_per_km"],
                        margin_pct=r["margin_pct"],
                        rolling_7d_avg_profit=trend["rolling_7d_avg_profit"] if trend else None,
                        profit_trend_slope=trend["profit_trend_slope"] if trend else None,
                        classification=trend["classification"] if trend else "INSUFFICIENT_DATA",
                    )
                )

            upsert_pnl(cur, rows)
            # The watermark advances LAST, inside the same transaction as the facts.
            # It is the promise that everything up to this date is complete, so it
            # must never be visible before the data it describes (ADR-005).
            if advance_watermark:
                advance_high_water_mark(cur, sim_date, job_run_id)
            conn.commit()

        classes: dict[str, int] = {}
        for r in rows:
            classes[r.classification] = classes.get(r.classification, 0) + 1
        log.info(
            "batch_complete",
            sim_date=sim_date.isoformat(),
            rows_upserted=len(rows),
            job_run_id=job_run_id,
            watermark_advanced=advance_watermark,
            **classes,
        )
        return 0
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily per-vehicle profitability")
    parser.add_argument("--sim-date", required=True, help="simulated date, YYYY-MM-DD")
    parser.add_argument("--job-run-id", default=None)
    parser.add_argument(
        "--no-advance-watermark",
        action="store_true",
        help="recompute this date without claiming batch progress (restatement/backfill)",
    )
    args = parser.parse_args(argv)

    obs = config.observability()
    configure(
        service="batch-profitability",
        stage="process",
        level=obs.log_level,
        json_output=obs.log_json,
    )

    return run(
        date.fromisoformat(args.sim_date),
        args.job_run_id or f"manual-{uuid.uuid4().hex[:8]}",
        advance_watermark=not args.no_advance_watermark,
    )


if __name__ == "__main__":
    sys.exit(main())
