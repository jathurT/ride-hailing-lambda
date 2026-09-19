"""The shared simulated clock.

Four independent processes — the telemetry producer, the expense dropper, the Spark
jobs and the Airflow DAGs — must agree on what *simulated* day it is. If they drift,
the daily join silently matches nothing and the failure is very hard to diagnose
because nothing raises.

The rule that prevents this: the anchor (`epoch_wall`) is decided **once**, by the
init container at stack start-up, and written to a shared file. Every other process
*reads* it. Nobody computes it locally.

Units are the other hazard in this module. Two different time bases are in play:

    real time       wall-clock seconds. Used for latency metrics and traces.
    simulated time  the clock the business sees. Used for windowing, watermarking,
                    Parquet partitioning and every business aggregation.

Every function below states which base it takes and returns. Mixing them is the
classic bug: a "last hour of earnings" window must use simulated time, while an
"end-to-end latency" histogram must use real time.

See plan/03 §3 for the full design and the watermark arithmetic.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

SECONDS_PER_DAY = 86_400

# Where the init container writes the anchor, and everyone else reads it.
DEFAULT_STATE_PATH = Path(os.environ.get("SIM_STATE_PATH", "/state/sim_epoch.json"))


def utcnow() -> datetime:
    """Real wall-clock time, timezone-aware. Wrapped so tests can freeze it."""
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class SimClock:
    """Maps real wall-clock time onto simulated time.

    Deliberately holds no I/O and no globals: construct it with an explicit epoch and
    every method becomes a pure function of its inputs, so the whole module is
    testable without Docker, Redis or a filesystem.

    Args:
        epoch_wall: the real UTC instant at which the simulation started.
        epoch_sim:  the simulated datetime corresponding to that instant.
        day_seconds: how many *real* seconds one *simulated* day lasts.
    """

    epoch_wall: datetime
    epoch_sim: datetime
    day_seconds: int

    def __post_init__(self) -> None:
        if self.day_seconds <= 0:
            raise ValueError("day_seconds must be positive")
        for name in ("epoch_wall", "epoch_sim"):
            value: datetime = getattr(self, name)
            if value.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware")

    # -- conversions -------------------------------------------------------

    @property
    def speedup(self) -> float:
        """How many simulated seconds pass per real second.

        At the project default (1 simulated day = 300 real seconds) this is 288.0,
        i.e. one real second is 4.8 simulated minutes.
        """
        return SECONDS_PER_DAY / self.day_seconds

    def real_to_sim(self, real_seconds: float) -> float:
        """Real seconds -> simulated seconds."""
        return real_seconds * self.speedup

    def sim_to_real(self, sim_seconds: float) -> float:
        """Simulated seconds -> real seconds.

        Used to sanity-check watermarks: a watermark expressed in simulated minutes
        buys far less real-world lateness tolerance than it appears to.
        """
        return sim_seconds / self.speedup

    # -- current simulated time --------------------------------------------

    def sim_now(self, now: datetime | None = None) -> datetime:
        """The current simulated datetime."""
        elapsed_real = ((now or utcnow()) - self.epoch_wall).total_seconds()
        return self.epoch_sim + timedelta(seconds=self.real_to_sim(elapsed_real))

    def sim_date(self, now: datetime | None = None) -> date:
        """The current simulated calendar date — the Parquet partition key."""
        return self.sim_now(now).date()

    def sim_day_index(self, now: datetime | None = None) -> int:
        """Simulated days elapsed since the simulation began. Day 0 is the first day."""
        return (self.sim_now(now) - self.epoch_sim).days

    # -- going the other way -----------------------------------------------

    def real_instant_of(self, sim_time: datetime) -> datetime:
        """The real instant at which a given simulated time occurs (or occurred).

        Lets a producer schedule a scripted event ("V007 goes idle at simulated
        10:00 on day 1") without polling.
        """
        sim_elapsed = (sim_time - self.epoch_sim).total_seconds()
        return self.epoch_wall + timedelta(seconds=self.sim_to_real(sim_elapsed))

    def drift_seconds(self, other: SimClock) -> float:
        """Simulated-time disagreement between two clocks, in simulated seconds.

        Should always be 0.0 — every process loads the same anchor. Exported as the
        `sim_clock_drift_seconds` gauge so a broken anchor is caught by an alert
        rather than by a confusing empty window three hours later.
        """
        now = utcnow()
        return abs((self.sim_now(now) - other.sim_now(now)).total_seconds())


# -- the shared anchor -----------------------------------------------------


def write_anchor(
    epoch_sim: datetime,
    day_seconds: int,
    path: Path = DEFAULT_STATE_PATH,
    now: datetime | None = None,
) -> SimClock:
    """Establish the anchor. Called **once**, by the init container.

    Writing this file is what makes a run a run. `make clean` removes it, which is
    how the init container detects a previous run and refuses to start on top of it
    (see plan/10 §4.3).
    """
    clock = SimClock(epoch_wall=now or utcnow(), epoch_sim=epoch_sim, day_seconds=day_seconds)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "epoch_wall": clock.epoch_wall.isoformat(),
                "epoch_sim": clock.epoch_sim.isoformat(),
                "day_seconds": clock.day_seconds,
            },
            indent=2,
        )
    )
    return clock


def read_anchor(path: Path = DEFAULT_STATE_PATH) -> SimClock:
    """Load the shared anchor. Called by every process except the init container."""
    if not path.exists():
        raise FileNotFoundError(
            f"No simulated-clock anchor at {path}. The init container writes it at "
            f"start-up; if you are running outside the stack, call write_anchor() first."
        )
    raw = json.loads(path.read_text())
    return SimClock(
        epoch_wall=datetime.fromisoformat(raw["epoch_wall"]),
        epoch_sim=datetime.fromisoformat(raw["epoch_sim"]),
        day_seconds=int(raw["day_seconds"]),
    )


def anchor_exists(path: Path = DEFAULT_STATE_PATH) -> bool:
    """Whether a previous run left an anchor behind. Used by the init container."""
    return path.exists()
