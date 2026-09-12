"""Q3 - prolonged-idle detection, running on Spark's state store.

The state machine itself lives in `fleet.transforms.idle` as plain Python. This
module is only the adapter that runs it inside Structured Streaming.

`applyInPandasWithState` is PySpark's ONLY stateful streaming API -
`flatMapGroupsWithState` is Scala/Java. It is awkward (an iterator of pandas
DataFrames, state encoded as a tuple) which is precisely why the business logic
stays out of it: this file can be wrong without the state machine being wrong, and
the state machine's tests do not need Spark.

Why state at all: "idle for 45 minutes" cannot be answered from one event or one
window. The threshold is usually crossed in a LATER micro-batch than the one where
idling started, so the episode's start must survive between batches.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fleet.transforms.idle import IdleEvent, IdleState, advance

# What each invocation emits: zero or more alerts.
ALERT_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("idle_minutes", DoubleType(), False),
        StructField("idle_since_sim", TimestampType(), False),
        StructField("raised_at_sim", TimestampType(), False),
        StructField("zone_id", StringType(), True),
    ]
)

# What Spark checkpoints per vehicle. Kept to three scalars: state size is the main
# cost of stateful streaming, and this is written on every batch for every key.
STATE_SCHEMA = StructType(
    [
        StructField("idle_since_sim", TimestampType(), True),
        StructField("last_status", StringType(), True),
        StructField("alerted_level", StringType(), True),
    ]
)

# Discard a vehicle's state after this much simulated silence. A dead telematics
# unit would otherwise hold state forever.
STATE_TTL_SIM_MINUTES = 24 * 60


def _load(state: GroupState) -> IdleState:
    if not state.exists:
        return IdleState()
    idle_since, last_status, alerted = state.get
    return IdleState(idle_since_sim=idle_since, last_status=last_status, alerted_level=alerted)


def _save(state: GroupState, value: IdleState) -> None:
    state.update((value.idle_since_sim, value.last_status, value.alerted_level))


def make_idle_detector(warn_after: float, critical_after: float) -> Any:
    """Build the applyInPandasWithState function.

    Thresholds are closed over rather than read from config inside the function:
    the function is shipped to executors, and reading configuration there would
    make behaviour depend on executor environment rather than on the driver's.
    """

    def detect(key: tuple, pdfs: Iterator[Any], state: GroupState) -> Iterator[Any]:
        import pandas as pd

        vehicle_id = key[0]

        # A timed-out group gets no data - just the chance to clean up.
        if state.hasTimedOut:
            state.remove()
            return
            yield  # pragma: no cover - makes this a generator

        rows: list[IdleEvent] = []
        for pdf in pdfs:
            for row in pdf.itertuples(index=False):
                rows.append(
                    IdleEvent(
                        vehicle_id=vehicle_id,
                        status=row.status,
                        event_time=row.event_time.to_pydatetime(),
                        zone_id=getattr(row, "zone_id", None),
                    )
                )

        if not rows:
            return
            yield  # pragma: no cover

        # A micro-batch does NOT guarantee ordering within a group, and the state
        # machine's correctness depends on seeing transitions in order. Kafka gives
        # us per-partition ordering; this restores it per group after the shuffle.
        rows.sort(key=lambda e: e.event_time)

        current, alerts = advance(_load(state), rows, warn_after, critical_after)
        _save(state, current)

        # Expire the group a simulated day after its last event, so vehicles that
        # stop reporting do not accumulate state forever.
        latest = rows[-1].event_time
        state.setTimeoutTimestamp(
            int(latest.timestamp() * 1000) + STATE_TTL_SIM_MINUTES * 60 * 1000
        )

        if alerts:
            yield pd.DataFrame(
                [
                    {
                        "alert_id": a.alert_id,
                        "vehicle_id": a.vehicle_id,
                        "severity": a.severity,
                        "idle_minutes": float(a.idle_minutes),
                        "idle_since_sim": a.idle_since_sim,
                        "raised_at_sim": a.raised_at_sim,
                        "zone_id": a.zone_id,
                    }
                    for a in alerts
                ]
            )

    return detect


def detect_idle(
    enriched: DataFrame,
    watermark_sim: str,
    warn_after_sim_minutes: float,
    critical_after_sim_minutes: float,
) -> DataFrame:
    """Stream of prolonged-idle alerts, one row per escalation."""
    return (
        enriched.select("vehicle_id", "status", "event_time", "zone_id")
        .withWatermark("event_time", watermark_sim)
        .groupBy("vehicle_id")
        .applyInPandasWithState(
            func=make_idle_detector(warn_after_sim_minutes, critical_after_sim_minutes),
            outputStructType=ALERT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )
