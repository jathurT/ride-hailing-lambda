"""Structured logging.

'Logging across ingestion, processing and storage stages' is a graded requirement.
These tests make 'across stages' checkable rather than asserted.
"""

from __future__ import annotations

import json

import pytest
import structlog

from fleet.common.logging import VALID_STAGES, configure, get_logger


@pytest.fixture
def logs(capsys):
    configure(service="test-service", stage="process", json_output=True, cache=False)
    yield capsys
    structlog.contextvars.clear_contextvars()


def last_line(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


class TestMandatoryFields:
    def test_every_line_carries_the_schema(self, logs):
        get_logger().info("batch_flushed", record_count=240)
        entry = last_line(logs)
        assert {"ts", "level", "event", "service", "stage"} <= set(entry)
        assert entry["service"] == "test-service"
        assert entry["stage"] == "process"
        assert entry["event"] == "batch_flushed"

    def test_fields_stay_structured_not_interpolated(self, logs):
        """The whole point: fields must be machine-queryable, not baked into prose."""
        get_logger().info("batch_flushed", record_count=240)
        entry = last_line(logs)
        assert entry["record_count"] == 240
        assert "240" not in entry["event"]

    def test_correlation_id_binds_for_grepping_one_vehicle(self, logs):
        get_logger(correlation_id="V007").info("idle_detected")
        assert last_line(logs)["correlation_id"] == "V007"


class TestStageValidation:
    def test_five_pipeline_stages(self):
        assert {"ingest", "process", "store", "serve", "orchestrate"} == VALID_STAGES

    @pytest.mark.parametrize("stage", sorted(VALID_STAGES))
    def test_each_valid_stage_is_accepted(self, capsys, stage):
        configure(service="s", stage=stage, json_output=True, cache=False)
        get_logger().info("ok")
        assert last_line(capsys)["stage"] == stage
        structlog.contextvars.clear_contextvars()

    def test_a_typo_raises_rather_than_producing_unqueryable_logs(self, logs):
        with pytest.raises(ValueError, match="invalid log stage"):
            get_logger().info("oops", stage="proccess")


class TestOutputFormat:
    def test_json_output_is_parseable(self, logs):
        get_logger().info("hello", answer=42)
        assert last_line(logs)["answer"] == 42

    def test_levels_are_recorded(self, logs):
        get_logger().warning("slow_batch", duration_ms=9000)
        assert last_line(logs)["level"] == "warning"


class TestReservedFields:
    """A kwarg that shadows a structlog field is silently overwritten - the line
    still appears, carrying wrong data, so nothing ever reveals the mistake.
    This actually happened: `level=` on a schema-compatibility line logged the log
    level instead of "BACKWARD". Now it raises.
    """

    @pytest.mark.parametrize("field", ["level", "ts", "logger", "exception"])
    def test_reserved_kwargs_raise(self, logs, field):
        with pytest.raises(ValueError, match="reserved"):
            get_logger().info("some_event", **{field: "value"})

    def test_ordinary_fields_still_work(self, logs):
        get_logger().info("ok", compatibility="BACKWARD", schema_id=1)
        entry = last_line(logs)
        assert entry["compatibility"] == "BACKWARD"
        assert entry["level"] == "info"
