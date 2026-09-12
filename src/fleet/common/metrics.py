"""Prometheus metrics.

Named and typed per plan/09 section 3. Two conventions enforced by
`tests/unit/test_metrics_registry.py`: counters end in `_total`, durations end in
`_seconds`, and every metric carries HELP text.
"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# --- ingestion ------------------------------------------------------------

events_produced_total = Counter(
    "events_produced_total",
    "Telemetry events handed to Kafka, by outcome of the delivery callback.",
    ["topic", "status"],
)
producer_send_duration_seconds = Histogram(
    "producer_send_duration_seconds",
    "Wall-clock time from produce() to the delivery callback.",
    ["topic"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)
producer_errors_total = Counter(
    "producer_errors_total", "Producer errors by class.", ["error_type"]
)
producer_backpressure_events_total = Counter(
    "producer_backpressure_events_total",
    "Times the local produce queue was full and the producer blocked. "
    "Blocking is deliberate: dropping telemetry silently would be a correctness bug.",
)
defects_injected_total = Counter(
    "defects_injected_total",
    "Deliberately corrupted events, by defect type. Compare against events_dlq_total "
    "to confirm the stream validator catches everything the simulator injects.",
    ["defect_type"],
)
expense_files_written_total = Counter(
    "expense_files_written_total",
    "Daily expense files dropped, split by whether the file restates a previous day.",
    ["restatement"],
)
expense_rows_written_total = Counter(
    "expense_rows_written_total", "Expense rows written across all files."
)

# --- processing / sinks ---------------------------------------------------

sink_writes_total = Counter("sink_writes_total", "Rows written by a speed-layer sink.", ["sink"])
sink_write_errors_total = Counter("sink_write_errors_total", "Failed sink writes.", ["sink"])

# --- the simulated clock --------------------------------------------------

sim_clock_day_index = Gauge("sim_clock_day_index", "Simulated days elapsed since the run started.")
sim_clock_drift_seconds = Gauge(
    "sim_clock_drift_seconds",
    "Simulated-time disagreement between this process and the shared anchor. "
    "Should be 0; a non-zero value means the shared-clock design is broken, which "
    "otherwise surfaces only as confusing empty windows hours later.",
)

# --- fleet state (useful for sanity-checking the simulator itself) --------

business_alerts_emitted_total = Counter(
    "business_alerts_emitted_total",
    "Business alerts emitted by the pipeline (about vehicles), as distinct from "
    "pipeline-health alerts (about the system), which go to Alertmanager. "
    "Labelled by severity as well as type so a dashboard can show CRITICAL "
    "separately - an alert count that mixes the two hides a deterioration.",
    ["type", "severity"],
)

vehicle_resyncs_total = Counter(
    "vehicle_resyncs_total",
    "Times a simulated vehicle skipped forward after the producer stalled. "
    "A rising count means the host cannot sustain the configured event rate - a "
    "capacity signal, not a correctness one. Previously this condition crashed the "
    "producer, and Docker's restart policy hid it behind healthy throughput.",
)

fleet_vehicles_by_status = Gauge(
    "fleet_vehicles_by_status", "Simulated vehicles currently in each status.", ["status"]
)


class _Handler(BaseHTTPRequestHandler):
    """Serves /metrics and /health from one port.

    Compose healthchecks need a liveness endpoint and Prometheus needs a scrape
    endpoint; running two servers per container to get both is not worth it.
    """

    def do_GET(self) -> None:
        if self.path.startswith("/metrics"):
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        elif self.path.startswith("/health"):
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b"not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        """Silence the default stderr access log - it is unstructured and would
        pollute the JSON log stream."""


def serve(port: int) -> ThreadingHTTPServer:
    """Expose /metrics and /health on a daemon thread. Called once at start-up."""
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    Thread(target=server.serve_forever, daemon=True, name="metrics").start()
    return server


# --- pushgateway ----------------------------------------------------------
#
# Spark's Structured Streaming queries cannot be scraped. They run inside the driver
# with no HTTP endpoint of their own, and - the part that actually matters - consumer
# lag does NOT measure them: the queries checkpoint their own offsets and never
# commit to a consumer group. Setting `kafka.group.id` to make lag work killed the
# DLQ query after ~4,500 micro-batches (plan/09 §3, the correction block).
#
# So the driver PUSHES. Pushgateway is a known anti-pattern for exactly one reason -
# a pushed metric outlives the process that pushed it, so a dead job looks alive
# forever. That is mitigated by pushing `..._last_progress_timestamp` and alerting on
# its AGE rather than on the values themselves: a query that has stopped stops
# updating the timestamp, and the timestamp going stale is the signal.


def push_gauges(job: str, values: dict[str, float], grouping: dict[str, str] | None = None) -> bool:
    """Push a set of gauge values to the Pushgateway. Returns whether it worked.

    Never raises. This is called from inside a `StreamingQueryListener` callback on
    the Spark driver; an exception escaping there would take down the listener and,
    with it, all visibility into the speed layer - trading the entire observability
    story for a transient HTTP error. A failed push is logged and counted, and the
    next micro-batch tries again.
    """
    from prometheus_client import CollectorRegistry, push_to_gateway
    from prometheus_client import Gauge as _Gauge

    from fleet.common import config
    from fleet.common.logging import get_logger

    registry = CollectorRegistry()
    for name, value in values.items():
        _Gauge(name, f"{name} (pushed from the Spark driver)", registry=registry).set(value)

    try:
        push_to_gateway(
            config.observability().pushgateway_url,
            job=job,
            registry=registry,
            grouping_key=grouping or {},
            timeout=5,
        )
        return True
    except Exception as exc:
        get_logger().warning("pushgateway_failed", job=job, error=str(exc))
        return False
