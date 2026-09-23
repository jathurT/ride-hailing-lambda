"""The DAG actually loads, and its shape is what the plan says it is.

Skipped when Airflow is not installed, which is the normal state of the dev venv -
Airflow is a ~200 MB dependency that only the scheduler container needs, and making
the whole unit suite drag it in to check four task relationships is a bad trade.
It runs inside the airflow container and in CI.

`pytest -m ''` from within the airflow image picks these up.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# NOTE: importorskip("airflow") does NOT work here. The repository has its own
# `airflow/` directory for DAG files, and under PEP 420 that resolves as a namespace
# package - so the import succeeds, the skip never fires, and the test fails later
# with a confusing ModuleNotFoundError on `airflow.models`. Probing a real submodule
# is what actually distinguishes the installed package from our folder.
pytest.importorskip(
    "airflow.models", reason="apache-airflow is installed only in the scheduler image"
)

# Overridable because these tests are meant to run in two places with different
# layouts: the repo (where the DAGs are ../../airflow/dags) and inside the scheduler
# image (where they are mounted at /opt/airflow/dags). Hard-coding the repo-relative
# path made the whole file pass vacuously in the container - the DagBag pointed at a
# directory that did not exist, found no DAGs, and reported no import errors.
DAG_DIR = Path(
    os.environ.get("FLEET_DAG_DIR", Path(__file__).resolve().parents[2] / "airflow/dags")
)
DAG_ID = "fleet_daily_reconciliation"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    # Asserted, not assumed. An empty DagBag has no import errors and no DAGs, so
    # every assertion below would either pass for the wrong reason or fail with a
    # confusing KeyError instead of saying "you pointed me at the wrong folder".
    assert DAG_DIR.is_dir(), f"DAG folder not found: {DAG_DIR} (set FLEET_DAG_DIR)"
    assert list(DAG_DIR.glob("*.py")), f"no DAG files in {DAG_DIR}"

    bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    assert bag.dags, f"DagBag loaded nothing from {DAG_DIR}: {bag.import_errors}"
    return bag


def test_no_import_errors(dagbag):
    """An import error in Airflow surfaces only as a red banner in the UI - the DAG
    simply is not there, and nothing fails loudly."""
    assert not dagbag.import_errors, dagbag.import_errors


def test_the_dag_is_present(dagbag):
    assert DAG_ID in dagbag.dags


def test_schedule_is_one_simulated_day(dagbag):
    """5 real minutes at SIM_DAY_SECONDS=300. If either changes, so must the other."""
    assert dagbag.dags[DAG_ID].schedule_interval == "*/5 * * * *"


def test_catchup_is_off_and_runs_are_serialised(dagbag):
    """Two concurrent runs would race on the same upsert and the same watermark."""
    dag = dagbag.dags[DAG_ID]
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_expected_tasks_exist(dagbag):
    expected = {
        "resolve_sim_date",
        "snapshot_speed_view",
        "validate_expense_file",
        "handle_missing_expense_file",
        "quarantine_file",
        "run_zone_hourly",
        "run_spark_profitability",
        "verify_watermark_advanced",
        "compute_reconciliation_delta",
        "notify_success",
        "notify_failure",
    }
    assert expected <= set(dagbag.dags[DAG_ID].task_ids)


def test_profitability_runs_after_the_zone_rollup(dagbag):
    """Ordered so a missing or unreadable partition fails on the cheap job first."""
    dag = dagbag.dags[DAG_ID]
    assert "run_spark_profitability" in dag.get_task("run_zone_hourly").downstream_task_ids


def test_reconciliation_runs_after_the_watermark_is_verified(dagbag):
    """The delta is only meaningful once the batch result for the day has committed."""
    dag = dagbag.dags[DAG_ID]
    assert (
        "compute_reconciliation_delta"
        in dag.get_task("verify_watermark_advanced").downstream_task_ids
    )


def test_snapshot_happens_before_validation(dagbag):
    """The speed view must be captured while it still remembers the day - its TTL is
    two simulated hours, and the batch path takes longer than that to finish."""
    dag = dagbag.dags[DAG_ID]
    assert "validate_expense_file" in dag.get_task("snapshot_speed_view").downstream_task_ids


def test_there_is_no_separate_watermark_task(dagbag):
    """ADR-005: the watermark moves inside the fact upsert's transaction.

    A separate Airflow task would either duplicate that or open a window where the
    watermark claims a day whose facts have not committed - exactly the inconsistency
    the serving layer's contract forbids. plan/08 lists such a task; this asserts the
    deliberate deviation, so nobody "fixes" it back.
    """
    assert "advance_batch_watermark" not in dagbag.dags[DAG_ID].task_ids


def test_failure_notification_is_reachable_from_the_quarantine_path(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert "notify_failure" in dag.get_task("quarantine_file").downstream_task_ids


def test_branch_join_uses_none_failed_min_one_success(dagbag):
    """The Airflow branch trap, pinned.

    `run_zone_hourly` is reachable two ways: directly from the branch when the
    expense file is fine, and via `handle_missing_expense_file` when it is absent
    but the telemetry is still valid. Under the DEFAULT trigger rule (ALL_SUCCESS)
    the branch skipping its other outcomes marks this task skipped as well, and the
    entire batch path quietly does not run.

    Observed live before the fix: the DAG reported four tasks skipped and one
    failure, the batch layer produced nothing, and the only durable symptom was a
    watermark that stayed null.
    """
    from airflow.utils.trigger_rule import TriggerRule

    task = dagbag.dags[DAG_ID].get_task("run_zone_hourly")
    assert task.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    assert len(task.upstream_task_ids) >= 2, (
        "if this task ever has a single upstream the rule above is no longer needed - "
        "but check why the missing-file path disappeared"
    )
