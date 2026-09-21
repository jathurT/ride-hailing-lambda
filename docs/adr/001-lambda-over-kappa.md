# ADR-001 — Lambda architecture, not Kappa

**Status:** Accepted · **Date:** 2026-08-18 · **Full argument:** [`plan/01`](../../plan/01-architecture-decision.md)

## Context

The business question is two questions bolted together. *"Fleet utilization and earnings by
area/time-of-day right now"* needs an answer in seconds and tolerates approximation. *"Which
vehicles are becoming unprofitable once yesterday's fuel/maintenance costs are factored in"*
needs an answer once a day, must be exactly right because it feeds driver settlements, and
cannot exist earlier than T+1 — the expense file does not exist until the day ends.

## Decision

Lambda. Speed layer (Structured Streaming → Redis) for the first question; batch layer (Spark
batch over an immutable Parquet master dataset → PostgreSQL mart) for the second; a serving
layer that merges them.

Walking the module's eight criteria, six favour Lambda. The two decisive ones:

- **Historical data analysis.** "*Becoming* unprofitable" is a multi-day trend, not a one-day
  lookup. That needs a queryable historical store, not bounded stream state.
- **Data reprocessing.** Garages restate invoices routinely. Under Lambda a correction re-reads
  one immutable Parquet partition. Under Kappa it means replaying a full day of telemetry —
  cost proportional to the *largest* dataset in the system to fix a handful of expense rows.

## Consequences

**Accepted costs**, both named by the module as Lambda's weaknesses:

- *"Managing multiple codebases"* — mitigated by `src/fleet/transforms/`, pure DataFrame
  functions imported by **both** layers and unit-tested once. Two entry points over one library.
- *"Reconciling data between systems"* — mitigated by an explicit `batch_high_water_mark`
  boundary (ADR-005). Half-open intervals, so no overlap and no gap.

Also accepted: more containers, two failure domains, higher storage cost. In exchange the
financial record is independent of streaming state — the speed layer is disposable by design.

## Rejected

Kappa, including a genuinely considered design in which expenses are published to a compacted
topic and joined stream-to-stream. Rejected on reprocessing asymmetry, the auditability of a
financial figure, and Kafka retention being the most expensive per-byte tier for a 30-day
history. We would revisit if costs became a real-time stream and the historical window shrank
to hours.
