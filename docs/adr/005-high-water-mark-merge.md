# ADR-005 — An explicit high-water-mark for the serving-layer merge

**Status:** Accepted · **Date:** 2026-08-18 · **Full argument:** [`plan/07 §5`](../../plan/07-req-storage-serving.md)

## Context

The module names *"reconciling data between systems"* as one of Lambda's two headline weaknesses.
Most implementations hand-wave it: they query one store or the other and never define what
happens at the seam, which produces either double-counted or missing intervals.

## Decision

A single-row `mart.batch_high_water_mark` table recording the last simulated date the batch
layer has **fully processed and committed**. The serving layer reads it per request and serves:

    [from, hwm]   from PostgreSQL   (exact)
    (hwm, now]    from Redis        (approximate)

Half-open intervals, so the boundary date belongs to exactly one side. **Provably no overlap and
no gap.** Every row in the response carries `"source": "batch" | "speed"`, and the envelope
carries `"consistency": "batch-complete-through <date>"`.

The watermark is advanced **only** by the final task of the batch DAG, after the fact upsert
commits — which is why the mart must be transactional.

## Consequences

- The client can always tell which numbers are exact and which are approximate. It never has
  to guess.
- If one store is unavailable the API **degrades rather than failing** — it serves the other
  half with `degraded: true`. That partial usefulness is a property of layer independence that
  a single-path system does not have.
- Divergence between the two views is *measured*, not assumed: when a date transitions from
  speed-served to batch-served we record the delta in `mart.reconciliation_delta` and chart it.
  The module's abstract reconciliation problem becomes a number we monitor.

`tests/unit/test_merge_boundary.py` is table-driven over every case, including cold start
(`hwm is None`) and both single-store outages.
