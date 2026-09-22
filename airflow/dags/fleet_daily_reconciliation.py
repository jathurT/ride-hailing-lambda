"""fleet_daily_reconciliation - the batch layer, orchestrated.

Runs every 5 real minutes, which at SIM_DAY_SECONDS=300 is once per simulated day.

    resolve_sim_date
          |
    snapshot_speed_view          <- captures what the speed view says about TODAY
          |
    wait_for_expense_file        <- S3KeySensor, reschedule mode
          |
    validate_expense_file        <- branch
       /      |        \\
 quarantine  handle_    run_zone_hourly
   _file     missing          |
       \\      |        run_spark_profitability
        \\     |              |
         \\    |        verify_watermark_advanced
          \\   |              |
           \\  |        compute_reconciliation_delta
            \\ |              |
             notify_success / notify_failure

RULE: no transformation logic in this file (plan/08 section 1). Every task is a call
into `fleet.*` or a shell-out to a job module. `tests/unit/test_dag_purity.py`
enforces it with an AST check - no pandas, no groupby, no aggregation here. A DAG
that grows business logic becomes a second, untested copy of the pipeline.

sim_date comes from the SIMULATED clock, not Airflow's `logical_date`. The two have
nothing to do with each other: Airflow's is wall-clock, ours advances 288x faster.
Using `logical_date` would silently process the wrong partition every single run.

TWO DELIBERATE DEVIATIONS FROM plan/08 section 3
------------------------------------------------
1. There is no `advance_batch_watermark` task. plan/08 lists one, but ADR-005
   requires the watermark to move inside the SAME transaction as the fact upsert, so
   it can never be visible before the data it describes. `daily_profitability` does
   that already. A separate Airflow task would either duplicate it or, worse, open a
   window where the watermark claims a day the facts have not committed. The task
   here VERIFIES the advance instead.

2. `snapshot_speed_view` is new. The reconciliation in plan/07 section 5.4 compares
   what the speed view said about a day against what the batch layer computed for it,
   and that is impossible to do retrospectively - Redis has a 2-simulated-hour TTL and
   has forgotten the day by the time the batch runs. Without a snapshot the
   divergence figure could only be asserted, never measured.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "fleet",
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=30),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="fleet_daily_reconciliation",
    description="Batch layer for one simulated day: zone rollup, P&L, reconciliation",
    schedule="*/5 * * * *",  # one simulated day at SIM_DAY_SECONDS=300
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # a missed simulated day is gone; the lake partition is what it is
    max_active_runs=1,  # two runs would race on the same upsert and watermark
    default_args=DEFAULT_ARGS,
    tags=["fleet", "batch", "lambda"],
    params={"sim_date": None},  # set manually to re-run a past day (restatement)
)
def fleet_daily_reconciliation() -> None:
    @task
    def resolve_sim_date(params: dict | None = None) -> str:
        """The most recent COMPLETE simulated day, or an explicit override.

        The day the clock is currently inside is still being written to by the speed
        layer, so batching it would produce a figure that changes on re-run and break
        the idempotency the whole batch layer is justified by.
        """
        from pathlib import Path

        from fleet.common import config
        from fleet.common.simclock import read_anchor

        override = (params or {}).get("sim_date")
        if override:
            return str(override)

        clock = read_anchor(Path(config.sim().state_path))
        latest_complete = clock.sim_date() - pendulum.duration(days=1)
        if latest_complete < clock.epoch_sim.date():
            raise AirflowSkipException(
                "no complete simulated day yet - the stack has been up for less than "
                "one SIM_DAY_SECONDS"
            )
        return latest_complete.isoformat()

    @task
    def snapshot_speed_view(sim_date: str) -> int:
        """Capture what the speed view says, before it forgets (2 sim-hour TTL).

        `sim_date` is accepted but deliberately unused for the row key: the speed
        view speaks for the day the clock is currently IN, which is the day after the
        one being batched. The parameter keeps this task ordered behind
        `resolve_sim_date`, which is what guarantees a snapshot exists for every day
        the batch layer will later try to reconcile.
        """
        from datetime import datetime
        from pathlib import Path

        import psycopg

        from fleet.common import config
        from fleet.common.simclock import read_anchor
        from fleet.serving.readers import RedisReader, StoreUnavailableError
        from fleet.store.mart import upsert_speed_snapshot

        clock = read_anchor(Path(config.sim().state_path))
        try:
            zones = RedisReader().zones()
        except StoreUnavailableError:
            # Not a failure. The speed view being down costs us the reconciliation
            # figure for this day; it does not invalidate the batch result, and
            # failing here would stop the P&L from being computed at all.
            raise AirflowSkipException("speed view unreachable - no snapshot taken") from None

        captured = clock.sim_now()
        # The window the speed view was reporting over, recorded alongside the values.
        # Without it the reconciliation compares a 15-simulated-minute sliding window
        # against a whole batch hour and reports a "divergence" that is a unit
        # mismatch, not an accuracy measurement.
        window_minutes = config.processing().window_sim_minutes

        rows = []
        for z in zones:
            if not z.get("reporting") or not z.get("window_end"):
                continue

            # ★ The row is keyed on the window the DATA describes, not on when this
            # task happened to run. The speed view lags the clock - measured live at
            # 93 simulated minutes, which is only ~19 real seconds at a 288x speed-up
            # but is more than an hour in simulated time, so it routinely crosses an
            # hour boundary and sometimes a day boundary.
            #
            # Keying on capture time therefore joined one hour's speed reading against
            # a different hour's batch figure. The symptom was not an error: it was a
            # systematic one-directional bias (speed reading ~80% low on trips) that
            # looked exactly like the speed layer under-counting.
            try:
                window_end = datetime.fromisoformat(str(z["window_end"]))
            except ValueError:
                continue

            rows.append(
                (
                    window_end.date(),
                    z["zone_id"],
                    window_end.hour,
                    captured,
                    window_minutes,
                    z["active_vehicles"],
                    z["trips"],
                    z["earnings"],
                    z["idle_ratio"],
                    z["avg_speed_kmh"],
                )
            )
        if not rows:
            raise AirflowSkipException("speed view reported no zones")

        dsn = config.storage().postgres_dsn
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            n = upsert_speed_snapshot(cur, rows)
            conn.commit()
        return n

    @task.branch
    def validate_expense_file(sim_date: str) -> str:
        """Structural checks only - is this file usable at all.

        Deliberately NOT a data-quality check. This decides whether the pipeline can
        proceed; whether the numbers look sensible is a separate concern measured
        after the fact, not a gate that blocks the run.
        """
        from fleet.batch_layer.validation import classify_expense_file

        verdict = classify_expense_file(sim_date)
        return {
            "ok": "run_zone_hourly",
            "missing": "handle_missing_expense_file",
            "corrupt": "quarantine_file",
        }[verdict]

    @task
    def handle_missing_expense_file(sim_date: str) -> None:
        """Record the day as incomplete and carry on.

        A missing partner file is a real, expected condition - the expense dropper
        simulates it. The telemetry for the day is still valid, so the P&L is still
        computed; those vehicles land as MISSING_EXPENSE rather than vanishing.
        """
        from fleet.batch_layer.validation import log_dq

        log_dq(sim_date, "expense_file_present", "FAIL", detail="no file at the expected key")

    @task
    def quarantine_file(sim_date: str) -> None:
        from fleet.batch_layer.validation import quarantine

        quarantine(sim_date)
        raise AirflowFailException(
            f"expense file for {sim_date} was malformed and has been quarantined"
        )

    # Both jobs run INSIDE the spark-app container rather than in the scheduler.
    # Airflow orchestrates; it does not host a Spark driver. Putting pyspark in the
    # scheduler image would add a second 1.5 GB driver on a host that has ~11 GB for
    # all of Docker, and would duplicate an image that already exists. The cost is a
    # mounted docker socket, which is stated in docker/airflow/Dockerfile.
    run_zone_hourly = BashOperator(
        task_id="run_zone_hourly",
        # NONE_FAILED_MIN_ONE_SUCCESS, not the default ALL_SUCCESS. This task has TWO
        # upstreams - the branch (when the file is fine) and handle_missing_expense_file
        # (when it is absent but the telemetry is still valid). Under ALL_SUCCESS the
        # branch skipping its other outcomes marks this SKIPPED too, so the whole
        # batch path silently never runs: the DAG goes amber, not red, and the only
        # visible symptom is a watermark that never advances.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        bash_command=(
            "docker exec fleet-spark-app "
            "python -m fleet.batch_layer.zone_hourly "
            "--sim-date {{ ti.xcom_pull(task_ids='resolve_sim_date') }} "
            "--job-run-id airflow-{{ run_id }}"
        ),
    )

    run_spark_profitability = BashOperator(
        task_id="run_spark_profitability",
        bash_command=(
            "docker exec fleet-spark-app "
            "python -m fleet.batch_layer.daily_profitability "
            "--sim-date {{ ti.xcom_pull(task_ids='resolve_sim_date') }} "
            "--job-run-id airflow-{{ run_id }}"
            # A manually-triggered run of a PAST date is a restatement: it corrects
            # that day's values without re-announcing it as the frontier of batch
            # progress (plan/08 section 4).
            "{{ ' --no-advance-watermark' if params.sim_date else '' }}"
        ),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def verify_watermark_advanced(sim_date: str, params: dict | None = None) -> str:
        """The watermark is moved by the job, in its own transaction. Check it landed.

        This is the assertion that the serving layer's contract still holds. If the
        upsert committed but the watermark did not, the API would keep serving the
        speed view for a day the batch layer has actually finished - stale numbers
        with nothing anywhere reporting a fault.
        """
        from datetime import date as _date

        import psycopg

        from fleet.common import config
        from fleet.store.mart import read_high_water_mark

        with psycopg.connect(config.storage().postgres_dsn) as conn, conn.cursor() as cur:
            hwm = read_high_water_mark(cur)

        target = _date.fromisoformat(sim_date)
        if (params or {}).get("sim_date"):
            # Restatement: the watermark must NOT have moved to this past date.
            if hwm is not None and hwm < target:
                raise AirflowFailException(
                    f"restatement of {sim_date} moved the watermark backwards to {hwm}"
                )
            return f"restatement ok; watermark unchanged at {hwm}"

        if hwm is None or hwm < target:
            raise AirflowFailException(
                f"facts for {sim_date} committed but the watermark is at {hwm}; "
                "the serving layer will keep answering from the speed view"
            )
        return f"watermark at {hwm}"

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def compute_reconciliation_delta(sim_date: str) -> int:
        """Speed view vs batch view for the same day - Lambda's weakness, measured.

        Expected divergence is published in plan/07 section 5.4: 1-3% on
        active_vehicles, up to 8% on earnings. A delta of exactly zero would be
        evidence that one side is reading the other rather than computing it.
        """
        from datetime import date as _date

        import psycopg

        from fleet.common import config
        from fleet.store.mart import compute_reconciliation

        with psycopg.connect(config.storage().postgres_dsn) as conn, conn.cursor() as cur:
            n = compute_reconciliation(cur, _date.fromisoformat(sim_date))
            conn.commit()
        return n

    notify_success = EmptyOperator(task_id="notify_success", trigger_rule=TriggerRule.ALL_SUCCESS)
    notify_failure = EmptyOperator(task_id="notify_failure", trigger_rule=TriggerRule.ONE_FAILED)

    sim_date = resolve_sim_date()
    snapshot = snapshot_speed_view(sim_date)
    branch = validate_expense_file(sim_date)

    missing = handle_missing_expense_file(sim_date)
    corrupt = quarantine_file(sim_date)

    sim_date >> snapshot >> branch
    branch >> [run_zone_hourly, missing, corrupt]
    missing >> run_zone_hourly
    run_zone_hourly >> run_spark_profitability

    verified = verify_watermark_advanced(sim_date)
    delta = compute_reconciliation_delta(sim_date)

    run_spark_profitability >> verified >> delta >> [notify_success, notify_failure]
    corrupt >> notify_failure


fleet_daily_reconciliation()
