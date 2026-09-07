"""The daily-batch source: one vehicle-expense CSV per simulated day.

This is the second of the two ingestion paths the assignment requires. It is
deliberately NOT published to Kafka: a CSV that a garage uploads once a day is not
an event stream, and forcing it into the log would add a producer, a topic and a
serialization contract while buying nothing that an object-store landing zone plus a
file sensor does not already give. That choice is part of the Lambda argument
(plan/01 section 4.2).

Three behaviours here exist to exercise the pipeline rather than to model reality
faithfully, and all three are declared in the report:

  * an **atomic drop** (write to .tmp, then move), so the Airflow sensor can never
    fire on a half-written file - a real and very common bug;
  * a **restatement** on simulated day 3 that corrects simulated day 2, which is the
    scenario the whole Lambda-over-Kappa argument rests on;
  * an optional **missing day** and **schema drift**, so the DAG's branching and
    quarantine paths are genuinely exercised rather than always taking the happy path.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import signal
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from random import Random
from types import FrameType

from fleet.common import config, metrics
from fleet.common.logging import configure, get_logger
from fleet.common.simclock import SimClock, read_anchor
from fleet.ingestion.generators.fleet import build_fleet

FIELDNAMES = [
    "vehicle_id",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
    "submitted_at",
    "partner_id",
]

# Cost per kilometre by fuel type. The spread is what makes the daily report's
# fuel-type breakdown say something: an electric vehicle covering the same distance
# costs a quarter as much to run as a petrol one.
FUEL_RATE_PER_KM = {"petrol": 0.42, "hybrid": 0.26, "electric": 0.11}

PARTNERS = ("GARAGE_A", "GARAGE_B", "GARAGE_C", "GARAGE_D")
SERVICE_PROBABILITY = 0.04
RESTATEMENT_SIM_DAY = 3
RESTATES_SIM_DAY = 2


@dataclass(frozen=True, slots=True)
class ExpenseRow:
    vehicle_id: str
    fuel_cost: float
    maintenance_cost: float
    distance_covered: float
    service_flag: bool
    submitted_at: str
    partner_id: str

    def as_dict(self) -> dict[str, object]:
        return {
            "vehicle_id": self.vehicle_id,
            "fuel_cost": f"{self.fuel_cost:.2f}",
            "maintenance_cost": f"{self.maintenance_cost:.2f}",
            "distance_covered": f"{self.distance_covered:.2f}",
            "service_flag": str(self.service_flag).lower(),
            "submitted_at": self.submitted_at,
            "partner_id": self.partner_id,
        }


def build_rows(
    vehicle_distances: dict[str, tuple[str, float]],
    rng: Random,
    submitted_at: datetime,
    restatement: bool = False,
) -> list[ExpenseRow]:
    """One row per vehicle that was active on the simulated day.

    `distance_covered` is the *partner's* measurement, deliberately 3% off our own
    GPS-derived figure. Real reconciliation always has two sources that disagree
    slightly, and surfacing that disagreement is the batch layer's job - hiding it
    by picking one would defeat the purpose (plan/06 section 2.3).
    """
    rows: list[ExpenseRow] = []
    for vehicle_id, (fuel_type, our_km) in sorted(vehicle_distances.items()):
        if our_km <= 0:
            continue

        partner_km = our_km * (1 + rng.gauss(0, 0.03))
        rate = FUEL_RATE_PER_KM[fuel_type]
        fuel = partner_km * rate * (1 + rng.gauss(0, 0.08))

        serviced = rng.random() < SERVICE_PROBABILITY
        maintenance = rng.uniform(150, 600) if serviced else rng.uniform(0, 15)

        # A restatement corrects a mis-assigned fuel card: the original figure was
        # too high for some vehicles, so the correction reduces it.
        if restatement:
            fuel *= rng.uniform(0.55, 0.8)

        rows.append(
            ExpenseRow(
                vehicle_id=vehicle_id,
                fuel_cost=round(max(fuel, 0.0), 2),
                maintenance_cost=round(maintenance, 2),
                distance_covered=round(max(partner_km, 0.0), 2),
                service_flag=serviced,
                submitted_at=submitted_at.isoformat(),
                partner_id=rng.choice(PARTNERS),
            )
        )
    return rows


def to_csv(rows: list[ExpenseRow], drift: bool = False) -> str:
    """Render rows as CSV. `drift` renames a column to exercise the DAG's
    validation-failure branch."""
    fields = list(FIELDNAMES)
    if drift:
        fields[3] = "distance_km"  # renamed upstream without warning
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    for r in rows:
        d = r.as_dict()
        if drift:
            d["distance_km"] = d.pop("distance_covered")
        writer.writerow(d)
    return buf.getvalue()


class S3ExpenseDropper:
    """Drops the daily expense file into the object store.

    NOTE on atomicity: the filesystem version below stages in a sibling directory
    and renames, because a POSIX reader can observe a partially-written file. S3
    needs no such dance - `put_object` is atomic, an object appears complete or not
    at all, and a multipart upload is invisible until it completes. Carrying the
    .tmp pattern over to S3 would be cargo-culting a fix for a problem that does not
    exist there.
    """

    def __init__(self, clock: SimClock, bucket: str, prefix: str = "landing/expenses") -> None:
        import boto3

        s = config.storage()
        self.clock = clock
        self.bucket = bucket
        self.prefix = prefix
        self.client = boto3.client(
            "s3",
            endpoint_url=s.minio_endpoint,
            aws_access_key_id=s.minio_access_key,
            aws_secret_access_key=s.minio_secret_key,
            region_name="us-east-1",
        )
        self.log = get_logger()
        self.fleet_cfg = config.fleet()
        self.sim = config.sim()
        self.rng = Random(self.fleet_cfg.random_seed ^ 0xE7DE45)
        self._running = True
        self._dropped: set[date] = set()

    def drop(self, sim_date: date, rows: list[ExpenseRow], restatement: bool = False) -> str:
        key = f"{self.prefix}/expenses_{sim_date.isoformat()}.csv"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=to_csv(rows).encode())

        if restatement:
            # A marker the restatement watcher DAG polls for, kept separate from the
            # CSV so a corrected file is distinguishable from an original arriving late.
            self.client.put_object(
                Bucket=self.bucket,
                Key=f"restatements/{sim_date.isoformat()}.json",
                Body=json.dumps(
                    {
                        "sim_date": sim_date.isoformat(),
                        "reason": "fuel card mis-assignment corrected by partner",
                        "rows": len(rows),
                    }
                ).encode(),
            )

        metrics.expense_files_written_total.labels(restatement=str(restatement).lower()).inc()
        metrics.expense_rows_written_total.inc(len(rows))
        self.log.info(
            "expense_file_dropped",
            bucket=self.bucket,
            key=key,
            sim_date=sim_date.isoformat(),
            rows=len(rows),
            restatement=restatement,
            total_fuel=round(sum(r.fuel_cost for r in rows), 2),
            serviced=sum(r.service_flag for r in rows),
        )
        return key

    def request_stop(self, signum: int, _f: FrameType | None) -> None:
        self.log.info("shutdown_requested", signal=signal.Signals(signum).name)
        self._running = False


class ExpenseDropper:
    def __init__(self, clock: SimClock, landing_dir: Path) -> None:
        self.clock = clock
        self.landing = landing_dir
        self.tmp = landing_dir / ".tmp"
        self.landing.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.log = get_logger()
        self.fleet_cfg = config.fleet()
        self.sim = config.sim()
        self.rng = Random(self.fleet_cfg.random_seed ^ 0xE7DE45)
        self._running = True
        self._dropped: set[date] = set()

    def drop(self, sim_date: date, rows: list[ExpenseRow], restatement: bool = False) -> Path:
        """Write atomically: a half-written file must never be visible.

        The sensor downstream polls for the final name; if it could observe a
        partial write it would hand a truncated CSV to the batch job. `Path.replace`
        is atomic within a filesystem, which is why we stage in a sibling directory
        rather than a system temp dir.
        """
        name = f"expenses_{sim_date.isoformat()}.csv"
        staged = self.tmp / name
        final = self.landing / name

        staged.write_text(to_csv(rows))
        staged.replace(final)

        metrics.expense_files_written_total.labels(restatement=str(restatement).lower()).inc()
        metrics.expense_rows_written_total.inc(len(rows))
        self.log.info(
            "expense_file_dropped",
            path=str(final),
            sim_date=sim_date.isoformat(),
            rows=len(rows),
            restatement=restatement,
            total_fuel=round(sum(r.fuel_cost for r in rows), 2),
            serviced=sum(r.service_flag for r in rows),
        )
        return final

    def request_stop(self, signum: int, _f: FrameType | None) -> None:
        self.log.info("shutdown_requested", signal=signal.Signals(signum).name)
        self._running = False


def _simulate_day_distances(sim_date: date, seed: int, size: int, epoch_sim: datetime) -> dict:
    """Replay one simulated day to obtain each vehicle's distance.

    The dropper runs as its own process, so it cannot read the live producer's
    accumulators. It re-simulates the same seeded day instead - which yields the
    same journeys, because the fleet is deterministic.
    """
    from datetime import UTC, timedelta

    day_start = datetime.combine(sim_date, datetime.min.time(), tzinfo=UTC)
    vehicles = build_fleet(size, seed, epoch_sim, start_at=day_start)
    for i in range(480):  # 24 sim-hours at the 3-sim-minute ping interval
        now = day_start + timedelta(minutes=3 * i)
        for v in vehicles:
            v.advance_to(now)
    return {v.vehicle_id: (v.fuel_type, v.day_distance_km) for v in vehicles}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily vehicle-expense file dropper")
    parser.add_argument(
        "--landing-dir",
        default="/landing/expenses",
        help="filesystem path; ignored when --bucket is given",
    )
    parser.add_argument(
        "--bucket", default=None, help="write into this MinIO bucket instead of the filesystem"
    )
    parser.add_argument("--once", action="store_true", help="drop today's file and exit")
    parser.add_argument(
        "--missing-day",
        type=int,
        default=None,
        help="simulated day to skip, exercising the DAG's missing-file branch",
    )
    parser.add_argument(
        "--drift-day",
        type=int,
        default=None,
        help="simulated day to emit with a renamed column, exercising quarantine",
    )
    args = parser.parse_args(argv)

    obs = config.observability()
    configure(
        service="expense-dropper", stage="ingest", level=obs.log_level, json_output=obs.log_json
    )
    log = get_logger()

    sim, fleet_cfg = config.sim(), config.fleet()
    clock = read_anchor(Path(sim.state_path))
    dropper = (
        S3ExpenseDropper(clock, args.bucket)
        if args.bucket
        else ExpenseDropper(clock, Path(args.landing_dir))
    )
    signal.signal(signal.SIGTERM, dropper.request_stop)
    signal.signal(signal.SIGINT, dropper.request_stop)

    if not args.once:
        metrics.serve(obs.metrics_port)
    log.info(
        "expense_dropper_started",
        sink=f"s3://{args.bucket}" if args.bucket else args.landing_dir,
        sim_day_real_minutes=clock.day_seconds / 60,
    )

    while dropper._running:
        day_index = clock.sim_day_index()
        # A day's expenses are submitted after that day ends, so we always drop for
        # the day just completed.
        target_day = day_index - 1
        if target_day >= 0:
            sim_date = sim.epoch_sim.date().toordinal() + target_day
            sim_date = date.fromordinal(sim_date)

            if sim_date not in dropper._dropped:
                if args.missing_day == target_day:
                    log.warning(
                        "expense_file_skipped",
                        sim_date=sim_date.isoformat(),
                        reason="SIMULATE_MISSING_DAY",
                    )
                else:
                    distances = _simulate_day_distances(
                        sim_date, fleet_cfg.random_seed, fleet_cfg.size, sim.epoch_sim
                    )
                    rows = build_rows(distances, dropper.rng, datetime.now(sim.epoch_sim.tzinfo))
                    dropper.drop(sim_date, rows)
                dropper._dropped.add(sim_date)

                # The restatement: on simulated day 3, resubmit day 2 corrected.
                if target_day == RESTATEMENT_SIM_DAY:
                    restated = date.fromordinal(sim.epoch_sim.date().toordinal() + RESTATES_SIM_DAY)
                    distances = _simulate_day_distances(
                        restated, fleet_cfg.random_seed, fleet_cfg.size, sim.epoch_sim
                    )
                    rows = build_rows(
                        distances,
                        dropper.rng,
                        datetime.now(sim.epoch_sim.tzinfo),
                        restatement=True,
                    )
                    dropper.drop(restated, rows, restatement=True)
                    log.info(
                        "restatement_issued",
                        restates_sim_date=restated.isoformat(),
                        reason="fuel card mis-assignment corrected by partner",
                    )

        if args.once:
            break
        time.sleep(max(clock.day_seconds / 10, 1.0))

    log.info("expense_dropper_stopped", files=len(dropper._dropped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
