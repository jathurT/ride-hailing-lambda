"""Input validation and data-quality logging for the batch layer.

Lives here rather than in the DAG for one reason: `tests/unit/test_dag_purity.py`
forbids logic in DAG files (plan/08 section 1), and a DAG that grows business rules
becomes a second, untested copy of the pipeline. Everything in this module is
importable and testable without Airflow.

THE DISTINCTION THAT MATTERS

`classify_expense_file` decides whether the pipeline can PROCEED. It is structural:
is the file there, does it parse, are the columns the ones we agreed. It gates the
run.

`log_dq` records whether the numbers look SENSIBLE. It does not gate anything. A day
where fuel costs are oddly high is data worth flagging and still worth processing;
blocking on it would mean the one interesting day never reaches the report.

Conflating the two is how a pipeline ends up either refusing to run over a trivial
anomaly, or silently importing a corrupt file because the quality check was advisory.
"""

from __future__ import annotations

import csv
import io
from datetime import date
from typing import Any

from fleet.common import config
from fleet.common.logging import get_logger
from fleet.store.lake import LakePaths

log = get_logger()

# The columns the partner agreed to send. A file with different columns is not a
# quality problem to be noted - it is a different file, and the join would produce
# nulls rather than an error.
REQUIRED_COLUMNS = frozenset(
    {
        "vehicle_id",
        "fuel_cost",
        "maintenance_cost",
        "distance_covered",
        "service_flag",
        "submitted_at",
        "partner_id",
    }
)

OK, MISSING, CORRUPT = "ok", "missing", "corrupt"


def _s3() -> Any:
    import boto3

    s = config.storage()
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_access_key,
        aws_secret_access_key=s.minio_secret_key,
        region_name="us-east-1",
    )


def inspect_csv(body: str) -> tuple[str, str]:
    """Classify a CSV's CONTENT. Pure - no network - so every branch is testable.

    Returns (verdict, detail).
    """
    try:
        reader = csv.DictReader(io.StringIO(body))
        header = set(reader.fieldnames or [])
    except csv.Error as exc:
        return CORRUPT, f"unparseable csv: {exc}"

    if not header:
        return CORRUPT, "file has no header row"

    absent = REQUIRED_COLUMNS - header
    if absent:
        return CORRUPT, f"missing columns: {sorted(absent)}"

    seen: set[str] = set()
    rows = 0
    for row in reader:
        rows += 1
        vid = (row.get("vehicle_id") or "").strip()
        if not vid:
            return CORRUPT, f"row {rows} has no vehicle_id"
        if vid in seen:
            # A duplicate vehicle would double-count that vehicle's costs in the
            # join. This is the one content check severe enough to stop the run,
            # because the resulting P&L would be wrong rather than incomplete.
            return CORRUPT, f"duplicate vehicle_id {vid!r}"
        seen.add(vid)

        for money in ("fuel_cost", "maintenance_cost"):
            raw = row.get(money)
            if raw in (None, ""):
                continue
            try:
                if float(raw) < 0:
                    return CORRUPT, f"negative {money} for {vid}: {raw}"
            except ValueError:
                return CORRUPT, f"non-numeric {money} for {vid}: {raw!r}"

    if rows == 0:
        return CORRUPT, "file has a header but no rows"

    return OK, f"{rows} rows, {len(seen)} vehicles"


def classify_expense_file(sim_date: str | date) -> str:
    """Fetch the day's expense file and classify it: ok | missing | corrupt.

    A MISSING file is not an error. The expense dropper simulates late and absent
    partner files on purpose, and the telemetry for the day is still perfectly valid -
    those vehicles land as MISSING_EXPENSE in the P&L rather than disappearing from
    it. Treating absence as failure would lose a day of real data to a partner's
    scheduling.
    """
    d = date.fromisoformat(str(sim_date))
    paths = LakePaths(config.storage().lake_bucket)
    key = paths.expense_key(d)
    client = _s3()

    try:
        obj = client.get_object(Bucket=paths.bucket, Key=key)
    except Exception as exc:
        if "NoSuchKey" in type(exc).__name__ or "404" in str(exc):
            log.warning("expense_file_missing", sim_date=d.isoformat(), key=key)
            log_dq(d, "expense_file_present", "FAIL", detail=f"no object at {key}")
            return MISSING
        log.error("expense_file_unreadable", sim_date=d.isoformat(), key=key, error=str(exc))
        return CORRUPT

    verdict, detail = inspect_csv(obj["Body"].read().decode("utf-8"))
    log_dq(
        d,
        "expense_file_structure",
        "PASS" if verdict == OK else "FAIL",
        detail=detail,
    )
    log.info("expense_file_classified", sim_date=d.isoformat(), verdict=verdict, detail=detail)
    return verdict


def quarantine(sim_date: str | date) -> str:
    """Move a malformed file out of the landing area and leave a reason beside it.

    Copied then deleted rather than left in place, so a re-run cannot pick the same
    bad file up again and fail identically forever. The reason file is written first:
    if the delete fails, we would rather have an explained file in two places than an
    unexplained one in neither.
    """
    d = date.fromisoformat(str(sim_date))
    paths = LakePaths(config.storage().lake_bucket)
    src = paths.expense_key(d)
    dest = f"quarantine/expenses_{d.isoformat()}.csv"
    client = _s3()

    client.put_object(
        Bucket=paths.bucket,
        Key=f"{dest}.reason.txt",
        Body=f"quarantined {d.isoformat()}: failed structural validation\n".encode(),
    )
    client.copy_object(
        Bucket=paths.bucket, Key=dest, CopySource={"Bucket": paths.bucket, "Key": src}
    )
    client.delete_object(Bucket=paths.bucket, Key=src)
    log.error("expense_file_quarantined", sim_date=d.isoformat(), src=src, dest=dest)
    log_dq(d, "expense_file_quarantined", "FAIL", detail=dest)
    return dest


INSERT_DQ_SQL = """
INSERT INTO mart.dq_run_log (job_run_id, sim_date, check_name, status, observed, threshold, detail)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def log_dq(
    sim_date: str | date,
    check_name: str,
    status: str,
    observed: float | None = None,
    threshold: float | None = None,
    detail: str | None = None,
    job_run_id: str = "validation",
) -> None:
    """Append one data-quality result to `mart.dq_run_log`.

    Never raises. A quality LOG that can fail the run it is describing is worse than
    no log at all - the pipeline would go down over its own bookkeeping, and the
    failure would be attributed to the batch job.
    """
    if status not in {"PASS", "WARN", "FAIL"}:
        raise ValueError(f"status must be PASS, WARN or FAIL, not {status!r}")

    d = date.fromisoformat(str(sim_date))
    try:
        import psycopg

        with psycopg.connect(config.storage().postgres_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                INSERT_DQ_SQL,
                (job_run_id, d, check_name, status, observed, threshold, detail),
            )
            conn.commit()
    except Exception as exc:
        log.warning("dq_log_failed", check=check_name, sim_date=d.isoformat(), error=str(exc))
