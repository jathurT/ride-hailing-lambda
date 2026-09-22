"""Making the speed layer visible.

Without this module the streaming queries are a black box. They run inside the Spark
driver, expose no scrape endpoint, and - the trap this project already fell into -
their health CANNOT be inferred from Kafka consumer lag. Structured Streaming
checkpoints its own offsets and never commits to a consumer group, so lag for these
queries reads as zero whether they are keeping up or dead. Setting `kafka.group.id`
to make lag work killed the DLQ query after roughly 4,500 micro-batches
(plan/09 section 3).

`StreamingQueryProgress` is the only source of truth about a streaming query, so the
listener below turns each progress event into gauges and pushes them.

WHAT TO ALERT ON

Not the rates. A query processing 0 rows/s might be healthy and idle. The signal is
`spark_streaming_last_progress_timestamp` going STALE: a query that has stopped stops
emitting progress, and the age of that timestamp is the only thing that distinguishes
"nothing to do" from "died four hours ago". Everything else here is for the dashboard.
"""

from __future__ import annotations

import time
from typing import Any

from fleet.common import metrics
from fleet.common.logging import get_logger

log = get_logger()

PUSH_JOB = "fleet_speed_layer"


def _num(value: Any) -> float | None:
    """Progress fields arrive as float, None, or NaN. NaN is the interesting one.

    Spark reports NaN for a rate when a micro-batch processed no rows, and NaN
    serialises into the Prometheus exposition format as `NaN`, which Grafana then
    draws as a gap. A gap is indistinguishable from a scrape failure, so NaN is
    dropped here and the previous value is simply not overwritten.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f  # NaN is the only value not equal to itself


def progress_gauges(progress: Any, now: float | None = None) -> dict[str, float]:
    """Turn one `StreamingQueryProgress` into the gauge set. Pure, so it is testable.

    Every field is read defensively. The Python wrapper around the progress object
    has changed shape across Spark versions, and a listener that raises inside its
    own callback takes all speed-layer visibility down with it - the failure mode
    this whole module exists to prevent.
    """
    out: dict[str, float] = {
        "spark_streaming_last_progress_timestamp": now if now is not None else time.time()
    }

    rate_in = _num(getattr(progress, "inputRowsPerSecond", None))
    if rate_in is not None:
        out["spark_streaming_input_rows_per_second"] = rate_in

    rate_out = _num(getattr(progress, "processedRowsPerSecond", None))
    if rate_out is not None:
        out["spark_streaming_processed_rows_per_second"] = rate_out

    # Spark reports the batch duration in MILLIseconds; the metric name says seconds.
    # Pushing the raw value would be off by 1000 with nothing to reveal it - a panel
    # reading "batch duration 4,300s" looks like a stall rather than 4.3 seconds.
    duration_ms = _num(getattr(progress, "batchDuration", None))
    if duration_ms is not None:
        out["spark_streaming_batch_duration_seconds"] = duration_ms / 1000.0

    # Rows IN and OUT for this micro-batch.
    #
    # The ratio of one query's output to another's is what makes a dead-letter RATE
    # expressible without touching the DLQ sink. That sink writes straight to Kafka
    # with no `foreachBatch`, so adding a counter inside it would mean restructuring
    # the query and invalidating its checkpoint - a real cost for a metric that can be
    # derived from progress events we already receive.
    #
    #   dlq rate = output_rows{query="q_dlq"} / output_rows{query="q_master"}
    #
    # q_master writes the events that validated; q_dlq writes the ones that did not.
    rows_in = _num(getattr(progress, "numInputRows", None))
    if rows_in is not None:
        out["spark_streaming_input_rows_last_batch"] = rows_in

    sink = getattr(progress, "sink", None)
    rows_out = _num(getattr(sink, "numOutputRows", None)) if sink is not None else None
    if rows_out is not None and rows_out >= 0:
        # Spark reports -1 when a sink cannot count its own output. Pushing that would
        # put a negative on a rows-written panel, which reads as a bug in the pipeline
        # rather than a gap in the instrumentation.
        out["spark_streaming_output_rows_last_batch"] = rows_out

    state_rows = 0.0
    for op in getattr(progress, "stateOperators", None) or []:
        rows = _num(getattr(op, "numRowsTotal", None))
        if rows is not None:
            state_rows += rows
    out["spark_streaming_state_rows"] = state_rows

    # Watermark lag: how far behind the stream's own event time the watermark sits.
    # This is in SIMULATED seconds, because event_time is simulated. At a 288x
    # speed-up a lag of 1,800 simulated seconds is 6 real seconds, so the number
    # means nothing without that context - it is stated in plan/09 and in the panel.
    event_time = getattr(progress, "eventTime", None) or {}
    if isinstance(event_time, dict):
        watermark, latest = event_time.get("watermark"), event_time.get("max")
        lag = _watermark_lag(watermark, latest)
        if lag is not None:
            out["spark_streaming_watermark_lag_seconds"] = lag

    return out


def _watermark_lag(watermark: str | None, latest: str | None) -> float | None:
    from datetime import datetime

    if not watermark or not latest:
        return None
    try:
        w = datetime.fromisoformat(str(watermark).replace("Z", "+00:00"))
        m = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (m - w).total_seconds()


def build_listener() -> Any:
    """Construct the listener. Imported lazily so this module is testable without Spark."""
    from pyspark.sql.streaming import StreamingQueryListener

    class _PushListener(StreamingQueryListener):  # type: ignore[misc]
        def onQueryStarted(self, event: Any) -> None:  # noqa: N802 - Spark's interface
            log.info("query_started", query=event.name, query_id=str(event.id))

        def onQueryProgress(self, event: Any) -> None:  # noqa: N802 - Spark's interface
            progress = event.progress
            name = getattr(progress, "name", None) or "unnamed"
            try:
                gauges = progress_gauges(progress)
            except Exception as exc:
                log.warning("progress_gauges_failed", query=name, error=str(exc))
                return
            metrics.push_gauges(PUSH_JOB, gauges, grouping={"query": name})

        def onQueryIdle(self, event: Any) -> None:  # noqa: N802 - Spark's interface
            """Spark 3.5 fires this instead of progress when a query has no data.

            It still counts as liveness, so the timestamp is refreshed. Without this
            an idle query would look stalled after 120 seconds and fire a false
            critical alert in the middle of a demo.
            """
            metrics.push_gauges(
                PUSH_JOB,
                {"spark_streaming_last_progress_timestamp": time.time()},
                grouping={"query": getattr(event, "name", None) or "unnamed"},
            )

        def onQueryTerminated(self, event: Any) -> None:  # noqa: N802 - Spark's interface
            log.error(
                "query_terminated",
                query_id=str(event.id),
                exception=str(getattr(event, "exception", None)),
            )

    return _PushListener()
