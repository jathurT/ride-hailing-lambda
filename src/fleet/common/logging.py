"""Structured logging.

The PDF requires "structured logging across ingestion, processing, and storage
stages". The word doing the work there is *across*: a log line is only useful for
diagnosing a pipeline if you can tell which stage produced it and which entity it
concerns. So every line carries a fixed schema, enforced by `tests/unit/test_logging_schema.py`:

    ts, level, service, stage, event, sim_date, sim_time,
    trace_id, span_id, correlation_id, ...

Two rules that matter:

1. `stage` must be one of the five pipeline stages. That is what makes "logging
   across stages" checkable rather than asserted.
2. **No f-strings in messages.** `log.info("batch_flushed", rows=n)` — never
   `log.info(f"flushed {n}")`. The entire point of structured logging is that the
   fields are machine-queryable; interpolating them into prose destroys that.

See plan/09 §2.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

import structlog

Stage = Literal["ingest", "process", "store", "serve", "orchestrate"]

VALID_STAGES: frozenset[str] = frozenset({"ingest", "process", "store", "serve", "orchestrate"})


def _add_sim_time(_logger: Any, _name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Stamp every line with simulated time as well as real time.

    Real `ts` answers "when did this happen on my laptop"; `sim_time` answers "when
    did this happen in the business's day". Debugging a windowing problem needs the
    second one, and reconstructing it after the fact from `ts` is painful.

    Silently skipped when no anchor exists yet — during start-up, the init container
    logs before it has written one.
    """
    try:
        from fleet.common import config
        from fleet.common.simclock import read_anchor

        clock = read_anchor(__import__("pathlib").Path(config.sim().state_path))
        now = clock.sim_now()
        event_dict.setdefault("sim_time", now.isoformat())
        event_dict.setdefault("sim_date", now.date().isoformat())
    except Exception:
        pass
    return event_dict


def _add_trace_context(_logger: Any, _name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Attach the active OpenTelemetry trace/span ids so logs and traces join up.

    No-op until tracing is enabled (the `full` compose profile).
    """
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx.is_valid:
            event_dict.setdefault("trace_id", format(ctx.trace_id, "032x"))
            event_dict.setdefault("span_id", format(ctx.span_id, "016x"))
    except Exception:
        pass
    return event_dict


def _validate_stage(_logger: Any, _name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Fail loudly on a typo'd stage rather than producing unqueryable logs."""
    stage = event_dict.get("stage")
    if stage is not None and stage not in VALID_STAGES:
        raise ValueError(f"invalid log stage {stage!r}; expected one of {sorted(VALID_STAGES)}")
    return event_dict


def configure(
    service: str,
    stage: Stage,
    level: str = "INFO",
    json_output: bool = True,
    cache: bool = True,
) -> None:
    """Configure structlog once, at process start-up.

    Args:
        service: which container this is (`telemetry-producer`, `api`, ...).
        stage:   which pipeline stage it belongs to. Bound to every line.
        level:   standard logging level name.
        json_output: JSON for containers; pretty console for local development.
        cache: cache the bound logger on first use. True in production (it is a
            real hot-path saving). Tests pass False because caching would pin the
            first stdout the logger ever saw, and pytest swaps stdout per test.
    """
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[
            _reject_reserved_fields,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
            _validate_stage,
            _add_sim_time,
            _add_trace_context,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=cache,
    )
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service, stage=stage)


def get_logger(**initial: Any) -> Any:
    """A logger with optional permanently-bound fields.

    Bind `correlation_id` to the entity a component is about — `vehicle_id` here —
    so one vehicle's journey can be grepped across every service.
    """
    return structlog.get_logger().bind(**initial)


# structlog and our own processors own these keys. Passing one as a kwarg silently
# overwrites the real value - which is how `level=` on a schema-compatibility log
# line produced an empty field instead of "BACKWARD".
RESERVED_LOG_FIELDS: frozenset[str] = frozenset(
    {"event", "level", "ts", "timestamp", "logger", "exception", "exc_info", "stack"}
)


def _reject_reserved_fields(_logger: Any, _name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Fail loudly when a caller shadows a reserved field.

    Silently losing a logged value is worse than crashing: the line still appears,
    it just carries the wrong data, so nothing ever reveals the mistake.
    """
    # Runs FIRST in the chain, so `event` (the positional message) is the only key
    # structlog has put here yet - everything else came from the caller.
    if clash := (set(event_dict) - {"event"}) & RESERVED_LOG_FIELDS:
        raise ValueError(
            f"log field(s) {sorted(clash)} are reserved and would be overwritten; rename them"
        )
    return event_dict
