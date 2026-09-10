"""The speed layer.

Runs several INDEPENDENT Structured Streaming queries over one Kafka topic. See
`source.py` for why they are independent rather than one query with several sinks.

    Q1  master dataset   validated events -> Parquet on MinIO, partitioned by sim_date
    Q2  zone activity    windowed aggregates -> Redis
    Q2b zone earnings    trip-deduplicated windowed revenue -> Redis
    Q3  idle detector    per-vehicle state -> alerts topic          (day 5)
    DLQ rejected events  -> Kafka, in Confluent wire format

Run:
    python -m fleet.speed_layer.streaming_job --queries master,dlq
    python -m fleet.speed_layer.streaming_job            # all implemented queries
"""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from types import FrameType

from fleet.common import config, metrics
from fleet.common.logging import configure, get_logger
from fleet.common.simclock import read_anchor
from fleet.common.spark_avro import registered_schema_id
from fleet.speed_layer.listener import build_listener
from fleet.speed_layer.source import QuerySpec, read_telemetry, validated_telemetry
from fleet.store.lake import LakePaths
from fleet.store.spark_s3 import build_session

ALL_QUERIES = ("master", "dlq", "activity", "earnings", "idle")


def build_specs(bucket: str) -> dict[str, QuerySpec]:
    """One consumer group and one checkpoint per query. Never shared."""
    paths = LakePaths(bucket)
    return {
        name: QuerySpec(
            name=f"q_{name}",
            consumer_group=f"fleet-{name}",
            checkpoint=paths.checkpoint(f"q_{name}"),
        )
        for name in ALL_QUERIES
    }


class SpeedLayer:
    def __init__(self, queries: list[str]) -> None:
        self.log = get_logger()
        self.queries = queries
        self.sim = config.sim()
        self.kafka_cfg = config.kafka()
        self.proc = config.processing()
        self.storage = config.storage()
        self.clock = read_anchor(Path(self.sim.state_path))
        self.paths = LakePaths(self.storage.lake_bucket)
        self.specs = build_specs(self.storage.lake_bucket)
        self.spark = build_session("fleet-speed-layer")
        self.spark.sparkContext.setLogLevel("WARN")
        # Registered BEFORE any query starts, or the first micro-batches of each
        # query produce no metrics and the dashboard opens on an empty panel that
        # looks exactly like a broken pipeline.
        try:
            self.spark.streams.addListener(build_listener())
        except Exception as exc:
            self.log.warning("listener_registration_failed", error=str(exc))
        self._started: list = []
        self._running = True

    def start(self) -> None:
        watermark = f"{self.proc.watermark_sim_minutes} minutes"
        self.log.info(
            "speed_layer_starting",
            queries=self.queries,
            watermark_sim=watermark,
            watermark_real_seconds=round(
                self.clock.sim_to_real(self.proc.watermark_sim_minutes * 60), 2
            ),
            window_sim=f"{self.proc.window_sim_minutes} minutes",
            slide_sim=f"{self.proc.window_slide_sim_minutes} minutes",
        )

        if "master" in self.queries:
            self._start_master()
        if "dlq" in self.queries:
            self._start_dlq()
        if "activity" in self.queries:
            self._start_activity()
        if "earnings" in self.queries:
            self._start_earnings()
        if "idle" in self.queries:
            self._start_idle()

        if not self._started:
            self.log.error("no_queries_started", requested=self.queries)
            raise SystemExit(1)

        self.log.info("speed_layer_running", queries=[q.name for q in self._started])

    def _start_master(self) -> None:
        from fleet.speed_layer.sinks.lake_sink import write_master_dataset

        spec = self.specs["master"]
        raw = read_telemetry(self.spark, spec, starting_offsets="earliest")
        validated = validated_telemetry(raw, self.clock)
        q = write_master_dataset(
            validated,
            path=self.paths.telemetry_root(),
            spec=spec,
            trigger_seconds=self.proc.trigger_seconds,
        )
        self._started.append(q)
        self.log.info(
            "query_started", query=spec.name, sink="parquet", path=self.paths.telemetry_root()
        )

    def _start_dlq(self) -> None:
        from fleet.speed_layer.sinks.dlq_sink import write_dlq

        spec = self.specs["dlq"]
        schema_id = registered_schema_id(
            self.kafka_cfg.schema_registry_url, f"{self.kafka_cfg.dlq_topic}-value"
        )
        raw = read_telemetry(self.spark, spec, starting_offsets="earliest")
        validated = validated_telemetry(raw, self.clock)
        rejected = validated.filter("NOT is_valid")
        q = write_dlq(rejected, spec, schema_id)
        self._started.append(q)
        self.log.info(
            "query_started",
            query=spec.name,
            sink="kafka",
            topic=self.kafka_cfg.dlq_topic,
            schema_id=schema_id,
        )

    def _valid_enriched(self, spec: QuerySpec):
        """Valid, zone-enriched telemetry - the common input to both aggregations."""
        from fleet.transforms.enrich import with_zone, zone_table

        raw = read_telemetry(self.spark, spec, starting_offsets="latest")
        validated = validated_telemetry(raw, self.clock)
        return with_zone(validated.filter("is_valid"), zone_table(self.spark))

    def _start_activity(self) -> None:
        """Q2 - windowed activity per zone -> Redis."""
        from fleet.speed_layer.sinks.redis_sink import (
            make_activity_writer,
            start_foreach_batch,
        )
        from fleet.transforms.utilization import zone_activity

        spec = self.specs["activity"]
        agg = zone_activity(
            self._valid_enriched(spec),
            watermark_sim=f"{self.proc.watermark_sim_minutes} minutes",
            window_sim=f"{self.proc.window_sim_minutes} minutes",
            slide_sim=f"{self.proc.window_slide_sim_minutes} minutes",
        )
        q = start_foreach_batch(
            agg,
            spec,
            make_activity_writer(self.storage.redis_url, self.clock),
            self.proc.trigger_seconds,
        )
        self._started.append(q)
        self.log.info("query_started", query=spec.name, sink="redis", view="zone_activity")

    def _start_earnings(self) -> None:
        """Q2b - windowed earnings per zone, from a TRIP-DEDUPLICATED stream.

        Separate query because a streaming query permits only one aggregation, and
        summing `fare` without deduplicating first inflates revenue ~8.5x
        (measured). See transforms/utilization.py.
        """
        from fleet.speed_layer.sinks.redis_sink import (
            make_earnings_writer,
            start_foreach_batch,
        )
        from fleet.transforms.utilization import deduplicate_trips, zone_earnings

        spec = self.specs["earnings"]
        watermark = f"{self.proc.watermark_sim_minutes} minutes"
        deduped = deduplicate_trips(self._valid_enriched(spec), watermark)
        agg = zone_earnings(
            deduped,
            watermark_sim=watermark,
            window_sim=f"{self.proc.window_sim_minutes} minutes",
            slide_sim=f"{self.proc.window_slide_sim_minutes} minutes",
        )
        q = start_foreach_batch(
            agg,
            spec,
            make_earnings_writer(self.storage.redis_url, self.clock),
            self.proc.trigger_seconds,
        )
        self._started.append(q)
        self.log.info("query_started", query=spec.name, sink="redis", view="zone_earnings")

    def _start_idle(self) -> None:
        """Q3 - prolonged-idle detection on Spark's state store.

        Two queries off one detector: the alerts go to Kafka (the durable record)
        and to Redis (what the serving API reads). Splitting them means a Redis
        outage cannot lose an alert from the log.
        """
        from fleet.speed_layer.idle_detector import detect_idle
        from fleet.speed_layer.sinks.alert_sink import make_alert_writer, start_alert_query

        spec = self.specs["idle"]
        alerts = detect_idle(
            self._valid_enriched(spec),
            watermark_sim=f"{self.proc.watermark_sim_minutes} minutes",
            warn_after_sim_minutes=float(self.proc.idle_alert_sim_minutes),
            critical_after_sim_minutes=float(self.proc.idle_critical_sim_minutes),
        )

        schema_id = registered_schema_id(
            self.kafka_cfg.schema_registry_url, f"{self.kafka_cfg.alerts_topic}-value"
        )
        # ONE query writing to both sinks. Two writeStreams on this DataFrame would
        # each re-run the stateful detector with its own state store.
        writer = make_alert_writer(schema_id, self.storage.redis_url, self.clock)
        self._started.append(start_alert_query(alerts, spec, writer, self.proc.trigger_seconds))

        self.log.info(
            "query_started",
            query=spec.name,
            sink="kafka+redis (one query)",
            warn_after_sim_minutes=self.proc.idle_alert_sim_minutes,
            critical_after_sim_minutes=self.proc.idle_critical_sim_minutes,
        )

    def await_termination(self) -> None:
        while self._running and self._started:
            for q in self._started:
                if not q.isActive:
                    self.log.error("query_terminated", query=q.name, exception=str(q.exception()))
                    self._running = False
            self.spark.streams.awaitAnyTermination(5000)
            self.spark.streams.resetTerminated()

    def request_stop(self, signum: int, _f: FrameType | None) -> None:
        self.log.info("shutdown_requested", signal=signal.Signals(signum).name)
        self._running = False
        for q in self._started:
            q.stop()

    def stop(self) -> None:
        for q in self._started:
            if q.isActive:
                q.stop()
        self.spark.stop()
        self.log.info("speed_layer_stopped")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Speed layer streaming job")
    parser.add_argument(
        "--queries", default="master,dlq", help=f"comma-separated subset of {','.join(ALL_QUERIES)}"
    )
    args = parser.parse_args(argv)

    obs = config.observability()
    configure(service="speed-layer", stage="process", level=obs.log_level, json_output=obs.log_json)

    layer = SpeedLayer([q.strip() for q in args.queries.split(",") if q.strip()])
    signal.signal(signal.SIGTERM, layer.request_stop)
    signal.signal(signal.SIGINT, layer.request_stop)

    metrics.serve(obs.metrics_port)
    try:
        layer.start()
        layer.await_termination()
    finally:
        layer.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
