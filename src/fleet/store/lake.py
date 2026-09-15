"""The master dataset: object storage layout and bucket bootstrap.

The lake is the batch layer's input and the reason the Lambda argument holds. Three
properties make the reprocessing story work (plan/01 section 2.7):

  immutable      nothing ever rewrites a telemetry partition, so recomputation is
                 deterministic - the same bytes always give the same answer;
  addressable    "recompute 2026-03-02" is a path, not a query;
  partitioned    the batch job scans one directory, not the whole lake.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

RAW_TELEMETRY_PREFIX = "raw/telemetry"
LANDING_EXPENSES_PREFIX = "landing/expenses"
QUARANTINE_PREFIX = "quarantine"
RESTATEMENTS_PREFIX = "restatements"
REPORTS_PREFIX = "reports"
CHECKPOINTS_PREFIX = "_checkpoints"

PREFIXES = (
    RAW_TELEMETRY_PREFIX,
    LANDING_EXPENSES_PREFIX,
    QUARANTINE_PREFIX,
    RESTATEMENTS_PREFIX,
    REPORTS_PREFIX,
    CHECKPOINTS_PREFIX,
)


@dataclass(frozen=True, slots=True)
class LakePaths:
    """Path construction in one place, so the writer and the reader cannot drift.

    A mismatch here fails silently: Spark reads an empty partition and produces a
    report full of zeros rather than raising.
    """

    bucket: str

    def _root(self, scheme: str) -> str:
        return f"{scheme}://{self.bucket}"

    # Hive-style partitioning: Spark reads `sim_date` back as a real column and can
    # prune partitions from a WHERE clause without scanning them.
    def telemetry_partition(self, sim_date: date, scheme: str = "s3a") -> str:
        return f"{self._root(scheme)}/{RAW_TELEMETRY_PREFIX}/sim_date={sim_date.isoformat()}"

    def telemetry_root(self, scheme: str = "s3a") -> str:
        return f"{self._root(scheme)}/{RAW_TELEMETRY_PREFIX}"

    def expense_key(self, sim_date: date) -> str:
        return f"{LANDING_EXPENSES_PREFIX}/expenses_{sim_date.isoformat()}.csv"

    def expense_uri(self, sim_date: date, scheme: str = "s3a") -> str:
        return f"{self._root(scheme)}/{self.expense_key(sim_date)}"

    def restatement_key(self, sim_date: date) -> str:
        return f"{RESTATEMENTS_PREFIX}/{sim_date.isoformat()}.json"

    def report_key(self, sim_date: date, version: int = 1) -> str:
        suffix = "" if version == 1 else f"_v{version}"
        return f"{REPORTS_PREFIX}/fleet_daily_report_{sim_date.isoformat()}{suffix}.pdf"

    def checkpoint(self, query: str, scheme: str = "s3a") -> str:
        return f"{self._root(scheme)}/{CHECKPOINTS_PREFIX}/{query}"


def ensure_bucket(client: object, bucket: str) -> bool:
    """Create the bucket and its prefix skeleton if absent. Idempotent.

    Returns True when the bucket was created by this call.
    """
    from botocore.exceptions import ClientError  # imported lazily: init-only dep

    created = False
    try:
        client.head_bucket(Bucket=bucket)  # type: ignore[attr-defined]
    except ClientError:
        client.create_bucket(Bucket=bucket)  # type: ignore[attr-defined]
        created = True

    # Object stores have no real directories, but a zero-byte marker per prefix
    # makes the layout visible in the MinIO console, which matters for the demo.
    for prefix in PREFIXES:
        client.put_object(Bucket=bucket, Key=f"{prefix}/.keep", Body=b"")  # type: ignore[attr-defined]

    return created
