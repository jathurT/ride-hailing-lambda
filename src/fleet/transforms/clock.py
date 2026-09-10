"""Simulated time as a Spark Column.

★ This module exists because of a real bug, and the comment is the point.

The obvious way to write a "not from the future" validation is to read the
simulated clock once and pass the value in:

    sim_now = clock.sim_now()                       # captured ONCE
    df.filter(F.col("event_time") <= sim_now + ...) # WRONG in a stream

In a batch job that is correct. In a **stream** it is badly wrong, because the job
runs for hours while simulated time advances at 288x. One real second later that
captured value is already 4.8 simulated minutes stale; against a 5-simulated-minute
tolerance, every event produced more than ~1 real second after job start looks like
it came from the future.

Measured on the live stack before the fix: 47,049 events dead-lettered against
11,083 deliberately injected defects, and the gap was widening.

The fix is to derive simulated now *per row* from `current_timestamp()`, so it
advances with the stream:

    sim_now = epoch_sim + (now_real - epoch_wall) * speedup

Kept as a pure function of the clock's parameters, so `fleet.transforms` stays
free of I/O.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column
from pyspark.sql import functions as F

from fleet.common.simclock import SimClock


def sim_now_column(clock: SimClock) -> Column:
    """Simulated 'now', recomputed on every row from the real clock.

    Use this in any long-running query. Use `sim_now_literal` only for a batch job
    or a test, where a fixed reference is what you actually want.
    """
    # cast to double keeps sub-second precision; unix_timestamp() truncates to the
    # second, which at 288x is a 4.8-simulated-minute rounding error.
    elapsed_real = F.current_timestamp().cast("double") - F.lit(clock.epoch_wall.timestamp())
    return F.timestamp_seconds(
        F.lit(clock.epoch_sim.timestamp()) + elapsed_real * F.lit(clock.speedup)
    )


def sim_now_literal(sim_now: datetime) -> Column:
    """A fixed simulated instant. Correct for batch jobs and deterministic tests."""
    return F.lit(sim_now).cast("timestamp")


def as_sim_now_column(sim_now: datetime | SimClock | Column) -> Column:
    """Accept whichever form the caller has, so the validator has one signature."""
    if isinstance(sim_now, SimClock):
        return sim_now_column(sim_now)
    if isinstance(sim_now, datetime):
        return sim_now_literal(sim_now)
    return sim_now
