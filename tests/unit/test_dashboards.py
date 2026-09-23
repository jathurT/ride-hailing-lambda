"""The provisioned Grafana dashboards are checked like code, because they are code.

The failure this guards against is the quiet one. A panel whose PromQL names a metric
that does not exist does not error - it draws a flat line, which is exactly what a
healthy-but-idle pipeline also draws. During a live demo the two are
indistinguishable, and the panel will be believed.

So every metric named in every expression must either exist in the application's own
Prometheus registry, or be listed below as deliberately external with a reason.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from prometheus_client import REGISTRY

import fleet.common.metrics  # noqa: F401 - imported for the side effect of registering

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "observability/grafana/dashboards"
PROVISIONING = Path(__file__).resolve().parents[2] / "observability/grafana/provisioning"

# Metrics that come from outside this codebase. Each one needs a source, so that a
# typo cannot hide in here disguised as a third-party metric.
EXTERNAL_METRICS = {
    # Pushed by the Spark driver via StreamingQueryListener (fleet/speed_layer/listener.py).
    # Not in the app registry because they are built in a throwaway CollectorRegistry
    # at push time.
    "spark_streaming_input_rows_per_second",
    "spark_streaming_processed_rows_per_second",
    "spark_streaming_batch_duration_seconds",
    "spark_streaming_state_rows",
    "spark_streaming_watermark_lag_seconds",
    "spark_streaming_last_progress_timestamp",
    # danielqsj/kafka-exporter
    "kafka_consumergroup_lag",
    # prometheus-fastapi-instrumentator, registered inside the API process
    "http_requests_total",
    "http_request_duration_seconds_bucket",
    # Synthesised by Prometheus itself for every configured scrape target. It is the
    # one series that is never ABSENT when a target dies - it goes to 0 instead of
    # vanishing - which is precisely why `ProducerTargetDown` is written against it.
    # An application metric disappears with its process, and a rule written against a
    # disappeared series cannot fire.
    "up",
}

# PromQL functions and keywords that look like metric names to a regex.
PROMQL_KEYWORDS = {
    "sum",
    "rate",
    "irate",
    "max",
    "min",
    "avg",
    "count",
    "by",
    "without",
    "time",
    "histogram_quantile",
    "increase",
    "deriv",
    "topk",
    "bottomk",
    "delta",
    "abs",
    "clamp_max",
    "clamp_min",
    "round",
    "le",
    "on",
    "ignoring",
    "group_left",
    "group_right",
    "offset",
    "bool",
    "and",
    "or",
    "unless",
}

METRIC_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?=[\{\[\)\s]|$)")


def _registry_metric_names() -> set[str]:
    names: set[str] = set()
    for metric in REGISTRY.collect():
        names.add(metric.name)
        for sample in metric.samples:
            names.add(sample.name)
        # Counters are exposed as `<name>_total` but collect() reports `<name>`.
        names.add(f"{metric.name}_total")
        names.add(f"{metric.name}_bucket")
    return names


def _dashboards() -> list[Path]:
    return sorted(DASHBOARD_DIR.glob("*.json"))


def _panels(dash: dict) -> list[dict]:
    out = []
    for p in dash.get("panels", []):
        out.append(p)
        out.extend(p.get("panels", []))
    return out


def test_there_is_at_least_one_dashboard():
    assert _dashboards(), f"no provisioned dashboards found in {DASHBOARD_DIR}"


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_dashboard_is_valid_json_with_a_uid_and_title(path):
    dash = json.loads(path.read_text())
    assert dash.get("uid"), "a dashboard without a uid gets a new one on every reload"
    assert dash.get("title")
    assert dash.get("panels")


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_panel_ids_are_unique(path):
    ids = [p["id"] for p in _panels(json.loads(path.read_text()))]
    duplicates = {i for i in ids if ids.count(i) > 1}
    assert not duplicates, f"duplicate panel ids {duplicates} - Grafana will drop panels"


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_panels_do_not_overlap_on_the_grid(path):
    """Overlapping panels render stacked on top of each other and hide data."""
    occupied: dict[tuple[int, int], int] = {}
    for panel in _panels(json.loads(path.read_text())):
        g = panel["gridPos"]
        assert g["x"] + g["w"] <= 24, f"panel {panel['id']} runs off the 24-column grid"
        for dx in range(g["w"]):
            for dy in range(g["h"]):
                cell = (g["x"] + dx, g["y"] + dy)
                assert cell not in occupied, (
                    f"panels {occupied[cell]} and {panel['id']} overlap at {cell}"
                )
                occupied[cell] = panel["id"]


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_every_metric_referenced_actually_exists(path):
    """★ The one that matters. A panel querying a metric that does not exist draws a
    flat line, which is what a healthy idle pipeline draws too."""
    known = _registry_metric_names() | EXTERNAL_METRICS | PROMQL_KEYWORDS
    unknown: dict[str, str] = {}

    for panel in _panels(json.loads(path.read_text())):
        for tgt in panel.get("targets", []):
            expr = tgt.get("expr", "")
            # Strip label matchers, then grouping clauses. Both contain bare
            # identifiers that are LABEL names, not metric names, and counting them
            # as metrics makes this test fail on a perfectly correct dashboard.
            stripped = re.sub(r"\{[^}]*\}", "", expr)
            stripped = re.sub(r"\b(?:by|without|on|ignoring)\s*\([^)]*\)", "", stripped)
            for name in METRIC_RE.findall(stripped):
                if name not in known and not name.isdigit():
                    unknown[name] = panel.get("title", str(panel["id"]))

    assert not unknown, "dashboard references metrics that are never emitted: " + ", ".join(
        f"{m} (panel: {p!r})" for m, p in sorted(unknown.items())
    )


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_every_panel_has_a_description(path):
    """A panel nobody can interpret is a panel nobody will act on - and these are
    going into the report as figures, where the caption has to come from somewhere."""
    missing = [
        p.get("title", p["id"])
        for p in _panels(json.loads(path.read_text()))
        if p["type"] != "row" and not p.get("description")
    ]
    assert not missing, f"panels without a description: {missing}"


def test_datasource_uids_used_by_panels_are_provisioned():
    provisioned = set()
    import yaml

    for f in (PROVISIONING / "datasources").glob("*.yml"):
        for ds in yaml.safe_load(f.read_text())["datasources"]:
            provisioned.add(ds["uid"])

    used = set()
    for path in _dashboards():
        for panel in _panels(json.loads(path.read_text())):
            ds = panel.get("datasource")
            if isinstance(ds, dict) and ds.get("uid"):
                used.add(ds["uid"])

    assert used <= provisioned, (
        f"panels reference unprovisioned datasources: {sorted(used - provisioned)}"
    )


# ---------------------------------------------------------------------------
# Alert rules, checked the same way as the dashboards.
#
# An alert that names a metric nobody emits never fires. It sits in the file looking
# like coverage, and the incident it was written for passes unnoticed - which is
# strictly worse than having no rule, because someone believed they were covered.
# ---------------------------------------------------------------------------

ALERTS_FILE = Path(__file__).resolve().parents[2] / "observability/prometheus/alerts.yml"


def _alert_rules() -> list[dict]:
    import yaml

    doc = yaml.safe_load(ALERTS_FILE.read_text())
    return [r for g in doc.get("groups", []) for r in g.get("rules", []) if "alert" in r]


def test_the_alerts_file_exists_and_parses():
    """The assignment requires at least one alert or health-check rule as a minimum."""
    assert ALERTS_FILE.is_file(), f"no alert rules at {ALERTS_FILE}"
    assert _alert_rules(), "the alerts file defines no alerts"


@pytest.mark.parametrize("rule", _alert_rules(), ids=lambda r: r["alert"])
def test_every_alert_expression_references_only_real_metrics(rule):
    """★ The same guard as the dashboard test, for the same reason."""
    known = (
        _registry_metric_names()
        | EXTERNAL_METRICS
        | PROMQL_KEYWORDS
        | {
            # Pushed by the StreamingQueryListener alongside the other spark_streaming_*
            # gauges; used to express the dead-letter rate as a ratio of two queries'
            # output rows, because the DLQ sink writes straight to Kafka and cannot be
            # instrumented without resetting its checkpoint.
            "spark_streaming_input_rows_last_batch",
            "spark_streaming_output_rows_last_batch",
            "clamp_min",
            "clamp_max",
        }
    )
    stripped = re.sub(r"\{[^}]*\}", "", rule["expr"])
    stripped = re.sub(r"\b(?:by|without|on|ignoring)\s*\([^)]*\)", "", stripped)
    unknown = {
        name for name in METRIC_RE.findall(stripped) if name not in known and not name.isdigit()
    }
    assert not unknown, f"alert {rule['alert']} references metrics never emitted: {sorted(unknown)}"


@pytest.mark.parametrize("rule", _alert_rules(), ids=lambda r: r["alert"])
def test_every_alert_has_severity_and_a_runbook(rule):
    """An alert with no severity cannot be routed, and one with no runbook wakes
    somebody up with a number and no next action."""
    assert rule.get("labels", {}).get("severity") in {"critical", "warning"}
    annotations = rule.get("annotations", {})
    assert annotations.get("summary"), f"{rule['alert']} has no summary"
    assert annotations.get("description"), f"{rule['alert']} has no description"
    assert annotations.get("runbook"), f"{rule['alert']} has no runbook"


@pytest.mark.parametrize("rule", _alert_rules(), ids=lambda r: r["alert"])
def test_every_alert_waits_before_firing(rule):
    """A rule with no `for:` fires on a single scrape.

    At a 5-second scrape interval that turns one slow micro-batch into a page. Every
    rule here describes a SUSTAINED condition, so every rule must have a duration.
    """
    assert rule.get("for"), f"{rule['alert']} has no `for:` duration"


def test_the_assignment_minimum_rule_is_present():
    """The brief's own example: "no data received in N minutes"."""
    names = {r["alert"] for r in _alert_rules()}
    assert "NoTelemetryIngested" in names
