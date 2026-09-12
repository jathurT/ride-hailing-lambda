"""Q2 / Q2b - the speed view sinks.

Redis has no native Spark sink, so these run inside `foreachBatch`. Two things in
here are load-bearing and easy to "simplify" into bugs later:

**`collect()` is safe here, and only here.** The aggregate is at most 12 zones times
a few open windows - tens of rows. `collect()` on a raw event stream would pull the
whole micro-batch to the driver and is a red flag anywhere else in this codebase.

**Writes overwrite, never increment.** `foreachBatch` may re-run a micro-batch after
a failure, so `HINCRBY` would double-count on replay. Recomputing the aggregate in
Spark and `HSET`-ing the result makes the sink idempotent, which is what lets the
at-least-once delivery path be safe (plan/04 §5). If you find yourself reaching for
an increment here, the aggregate belongs upstream in Spark.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from fleet.common import metrics
from fleet.common.logging import get_logger
from fleet.common.simclock import SimClock
from fleet.speed_layer.source import QuerySpec
from fleet.store.speed_view import SpeedView, ZoneAggregate

log = get_logger()


def newest_window_per_zone(rows: list[Any]) -> dict[str, Any]:
    """Keep only the most recent window for each zone.

    A sliding-window micro-batch emits several overlapping windows per zone. Only
    the newest is "the current view"; writing the older ones too makes the dashboard
    flicker between window generations as they arrive in arbitrary order.

    Rows with a null `zone_id` are dropped here rather than earlier: they are events
    outside the city bounding box, which are counted as a data-quality signal but
    belong to no zone.
    """
    newest: dict[str, Any] = {}
    for r in rows:
        zone = r["zone_id"]
        if zone is None:
            continue
        current = newest.get(zone)
        if current is None or r["window_end"] > current["window_end"]:
            newest[zone] = r
    return newest


def _redis_client(url: str) -> Any:
    """Built inside the executor/driver call, not captured from the enclosing scope.

    A client created at job-submission time would have to be pickled and shipped
    with the closure; connections are not picklable, and even when they appear to
    work they break on reconnect.
    """
    import redis

    return redis.from_url(url, decode_responses=True, socket_timeout=5)


def make_activity_writer(redis_url: str, clock: SimClock) -> Any:
    """Build the foreachBatch function for windowed zone activity."""

    def write(batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()  # safe: <= 12 zones x a few open windows
        if not rows:
            return
        view = SpeedView(_redis_client(redis_url), clock)

        newest = newest_window_per_zone(rows)

        aggregates = [
            ZoneAggregate(
                zone_id=r["zone_id"],
                active_vehicles=int(r["active_vehicles"] or 0),
                trips=int(r["trips"] or 0),
                idle_ratio=float(r["idle_ratio"] or 0.0),
                avg_speed_kmh=float(r["avg_speed_kmh"] or 0.0),
                window_start=str(r["window_start"]),
                window_end=str(r["window_end"]),
                updated_at_sim=str(r["window_end"]),
            )
            for r in newest.values()
        ]
        view.write_zone_aggregates(aggregates)
        view.write_snapshot(
            {
                "total_active": sum(a.active_vehicles for a in aggregates),
                "total_trips": sum(a.trips for a in aggregates),
                "fleet_idle_ratio": round(
                    sum(a.idle_ratio for a in aggregates) / max(len(aggregates), 1), 4
                ),
                "zones_reporting": len(aggregates),
                "updated_at_sim": max((a.window_end for a in aggregates), default=""),
            }
        )
        metrics.sink_writes_total.labels(sink="redis_activity").inc(len(aggregates))
        log.info("speed_view_written", batch_id=batch_id, zones=len(aggregates))

    return write


def make_earnings_writer(redis_url: str, clock: SimClock) -> Any:
    """Build the foreachBatch function for windowed zone earnings.

    Separate from activity because a streaming query may contain only one
    aggregation, and earnings must be computed from a trip-deduplicated stream to
    avoid the 8.5x revenue inflation documented in `transforms/utilization.py`.
    """

    def write(batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            return
        client = _redis_client(redis_url)
        view = SpeedView(client, clock)

        newest = newest_window_per_zone(rows)

        pipe = client.pipeline()
        from fleet.store import redis_keys as keys

        for zone, r in newest.items():
            key = keys.zone(zone)
            # HSET of a recomputed value, not HINCRBY - see the module docstring.
            pipe.hset(
                key,
                mapping={
                    "earnings": f"{float(r['earnings'] or 0.0):.2f}",
                    "completed_trips": str(int(r["completed_trips"] or 0)),
                    "earnings_window_end": str(r["window_end"]),
                },
            )
            pipe.expire(key, view.zone_ttl)
        pipe.execute()

        for zone, r in newest.items():
            view.push_zone_earnings(zone, str(r["window_end"]), float(r["earnings"] or 0.0))

        metrics.sink_writes_total.labels(sink="redis_earnings").inc(len(newest))
        log.info(
            "earnings_written",
            batch_id=batch_id,
            zones=len(newest),
            total=round(sum(float(r["earnings"] or 0) for r in newest.values()), 2),
        )

    return write


def start_foreach_batch(
    df: DataFrame, spec: QuerySpec, writer: Any, trigger_seconds: int
) -> StreamingQuery:
    return (
        df.writeStream.queryName(spec.name)
        .outputMode("update")  # only changed windows; complete would re-emit everything
        .foreachBatch(writer)
        .option("checkpointLocation", spec.checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
