"""Run the batch layer over a range of completed simulated days.

Why this exists at all: the batch layer's interesting outputs are the ones that need
HISTORY. `profit_trend_slope` and `classification` are computed over a trailing
window, so a single day's run can only ever report INSUFFICIENT_DATA - which is the
correct answer, and a useless demonstration. Four simulated days is the minimum for a
trend (`MIN_DAYS_FOR_TREND`), seven is the full window.

Order is not a detail. The jobs run ASCENDING by simulated date because
`read_recent_history` builds each day's trend from the rows already in the mart. Run
day 5 before day 4 and day 5's slope is drawn through a window with a hole in it -
no error, no warning, just a wrong number in the report. The loop below is
deliberately sequential for the same reason; there is no parallel mode to add later.

Only COMPLETE days are backfilled. The day the simulated clock is currently inside
is still being written to by the speed layer, so its Parquet partition is a moving
target; including it would produce a figure that changes on re-run and break the
idempotency check that the whole batch layer is justified by.

The watermark is advanced only by the LAST day processed, and only when that day is
the most recent complete one. Backfilling day 2 after day 9 has already been
published must not re-announce day 2 as the frontier of batch progress.

Run:
    python scripts/backfill.py --days 7
    python scripts/backfill.py --from 2026-03-02 --to 2026-03-08
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

from fleet.batch_layer import daily_profitability, zone_hourly
from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.common.simclock import read_anchor

log = get_logger()


def complete_sim_dates(days: int, state_path: Path) -> list[date]:
    """The last `days` simulated dates that are finished, oldest first.

    "Finished" means strictly before the simulated date the clock is in right now.
    On the first simulated day there is nothing complete yet and this returns an
    empty list - a real state during the first five real minutes of a fresh stack,
    and one the caller must report rather than crash on.
    """
    clock = read_anchor(state_path)
    today = clock.sim_date()
    first_complete = clock.epoch_sim.date()
    latest_complete = today - timedelta(days=1)
    if latest_complete < first_complete:
        return []
    start = max(first_complete, latest_complete - timedelta(days=days - 1))
    span = (latest_complete - start).days + 1
    return [start + timedelta(days=i) for i in range(span)]


def run_day(sim_date: date, job_run_id: str, advance_watermark: bool) -> tuple[int, int]:
    """Both batch jobs for one simulated date. Returns their two exit codes.

    The zone rollup runs first because it is the cheaper of the two and shares the
    same input partition; if the partition is missing or unreadable, failing here
    costs seconds rather than a full profitability run.
    """
    zone_rc = zone_hourly.run(sim_date, job_run_id)
    pnl_rc = daily_profitability.run(sim_date, job_run_id, advance_watermark=advance_watermark)
    return zone_rc, pnl_rc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill the batch layer over sim dates")
    parser.add_argument("--days", type=int, default=7, help="how many complete sim days")
    parser.add_argument("--from", dest="from_date", default=None, help="explicit start, YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=None, help="explicit end, YYYY-MM-DD")
    parser.add_argument(
        "--job-run-id",
        default=None,
        help="reuse one id to make a re-run idempotent; a NEW id marks every row restated",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="keep going past a failed day instead of stopping at the gap",
    )
    args = parser.parse_args(argv)

    obs = config.observability()
    configure(service="backfill", stage="process", level=obs.log_level, json_output=obs.log_json)

    if bool(args.from_date) != bool(args.to_date):
        log.error("range_incomplete", detail="--from and --to must be given together")
        return 2

    if args.from_date:
        start, end = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)
        if end < start:
            log.error("range_inverted", start=start.isoformat(), end=end.isoformat())
            return 2
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        dates = complete_sim_dates(args.days, Path(config.sim().state_path))

    if not dates:
        log.error(
            "nothing_to_backfill",
            detail="no complete simulated day yet - let the stack run one SIM_DAY_SECONDS",
        )
        return 1

    # One id across the whole backfill. Re-running with the same id leaves
    # `restatement_count` untouched, which is exactly the idempotency assertion the
    # verification step makes; a fresh id would mark all 1,050 rows restated and the
    # check would fail for a reason that has nothing to do with correctness.
    job_run_id = args.job_run_id or f"backfill-{uuid.uuid4().hex[:8]}"
    log.info(
        "backfill_starting",
        days=len(dates),
        first=dates[0].isoformat(),
        last=dates[-1].isoformat(),
        job_run_id=job_run_id,
    )

    # An explicit --from/--to range is a RESTATEMENT: it recomputes days that have
    # already been published, so it must change their values without touching the
    # promise about how far the batch layer has got (plan/08 section 4). Only a
    # clock-derived backfill, which by construction ends at the newest complete day,
    # is allowed to move the watermark - and then only on its final day.
    may_advance = args.from_date is None

    failed: list[str] = []
    started = time.monotonic()
    for i, sim_date in enumerate(dates):
        is_last = i == len(dates) - 1
        try:
            zone_rc, pnl_rc = run_day(
                sim_date, job_run_id, advance_watermark=is_last and may_advance
            )
        except Exception as exc:
            log.error("day_failed", sim_date=sim_date.isoformat(), error=str(exc))
            failed.append(sim_date.isoformat())
            if not args.continue_on_error:
                break
            continue

        if zone_rc or pnl_rc:
            log.error("day_nonzero_exit", sim_date=sim_date.isoformat(), zone=zone_rc, pnl=pnl_rc)
            failed.append(sim_date.isoformat())
            if not args.continue_on_error:
                break

    log.info(
        "backfill_complete",
        requested=len(dates),
        failed=len(failed),
        failed_dates=failed,
        job_run_id=job_run_id,
        elapsed_seconds=round(time.monotonic() - started, 1),
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
