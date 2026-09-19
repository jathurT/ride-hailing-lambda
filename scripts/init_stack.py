"""Bootstrap the stack. Runs once, then exits.

Order matters:

    1. guard against starting a new run on top of an old one
    2. anchor the simulated clock   <- everything else depends on this
    3. create topics
    4. register schemas

Step 1 is the one worth explaining. Every run starts the simulated clock at day 1.
If a previous run's data is still present, a new run writes a *second* day 1 over
the top of the first. Nothing errors - the dates and trends just go quietly wrong,
and the screenshots are worthless. So we refuse, loudly, and print the two valid
options. See plan/10 section 4.3.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from fleet.common import config
from fleet.common.logging import configure, get_logger
from fleet.common.simclock import anchor_exists, read_anchor, write_anchor

configure(service="init", stage="orchestrate")
log = get_logger()


def guard_previous_run(state_path: Path) -> bool:
    """Return True if we should proceed. Exits the process if we should not.

    Returns True for a fresh start, and True (in resume mode) when the caller has
    explicitly asked to continue an existing run.
    """
    resume = os.environ.get("RESUME", "").strip() in {"1", "true", "yes"}

    if not anchor_exists(state_path):
        return True

    if resume:
        clock = read_anchor(state_path)
        log.warning(
            "resuming_previous_run",
            epoch_wall=clock.epoch_wall.isoformat(),
            sim_now=clock.sim_now().isoformat(),
            sim_day=clock.sim_day_index(),
        )
        return True

    existing = read_anchor(state_path)
    sys.stderr.write(
        "\n"
        "ERROR: existing simulation data found.\n"
        f"       anchor at {state_path}, started {existing.epoch_wall.isoformat()},\n"
        f"       currently at simulated day {existing.sim_day_index()} "
        f"({existing.sim_now().date().isoformat()}).\n"
        "\n"
        "Starting a new run over old data produces incoherent dates and trends,\n"
        "and nothing will report an error.\n"
        "\n"
        "  make clean && make up     start fresh (recommended)\n"
        "  RESUME=1 make up          continue the previous run, reusing its clock\n"
        "\n"
    )
    return False


def main() -> int:
    s = config.sim()
    state_path = Path(s.state_path)

    if not guard_previous_run(state_path):
        return 2

    # --- 2. anchor the simulated clock ---
    if anchor_exists(state_path):
        clock = read_anchor(state_path)
    else:
        clock = write_anchor(epoch_sim=s.epoch_sim, day_seconds=s.day_seconds, path=state_path)
        log.info(
            "sim_clock_anchored",
            epoch_wall=clock.epoch_wall.isoformat(),
            epoch_sim=clock.epoch_sim.isoformat(),
            day_seconds=clock.day_seconds,
            speedup=round(clock.speedup, 1),
            state_path=str(state_path),
        )

    # Surface the coupling that costs the most time when it is wrong.
    p = config.processing()
    log.info(
        "sim_clock_derived_values",
        watermark_sim_minutes=p.watermark_sim_minutes,
        watermark_real_seconds=round(clock.sim_to_real(p.watermark_sim_minutes * 60), 2),
        window_sim_minutes=p.window_sim_minutes,
        window_real_seconds=round(clock.sim_to_real(p.window_sim_minutes * 60), 2),
        sim_day_real_minutes=round(clock.day_seconds / 60, 1),
    )

    # --- 3 & 4. topics and schemas ---
    from create_topics import main as create_topics
    from register_schemas import main as register_schemas

    if rc := create_topics():
        return rc
    if rc := register_schemas():
        return rc

    # --- 5. storage: lake buckets, mart schema, dimensions, speed view ---
    from init_storage import main as init_storage

    if rc := init_storage():
        return rc

    log.info("init_complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
