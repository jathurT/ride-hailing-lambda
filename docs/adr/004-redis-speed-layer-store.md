# ADR-004 — Redis as the speed-layer store

**Status:** Accepted · **Date:** 2026-08-18 · **Full argument:** [`plan/02 §4`](../../plan/02-technology-stack.md)

## Context

The module maps Lambda's speed layer onto **NoSQL databases**, and defines key–value stores as
*"simple key→value access for ultra-fast lookups"*. Our speed-layer access pattern is entirely
point lookups by known key — no joins, no ad-hoc queries, no aggregation (Spark did all of it).

## Decision

Redis, with persistence **disabled** (`--appendonly no --save ""`).

The strongest reason is Lambda-specific: **the speed layer's job is to cover the window since
the batch layer last ran, and to be discarded afterwards.** Redis TTL makes that automatic —
every key carries a 2-simulated-hour TTL, so the transient view expires itself. We never write
cleanup code and it cannot grow without bound. In a Kappa system this would be a liability; in a
Lambda speed layer it is exactly right.

Disabling persistence is deliberate, not an oversight: the data is disposable, and it removes
fork-based snapshot pauses.

## Rejected

- **Cassandra** — the module's canonical wide-column store and a fine speed-layer choice, but
  our working set is tiny (12 zones, 150 vehicles) and entirely transient. Its strengths buy us
  nothing while costing ~2 GB and a 90-second start-up. *(Note: the sibling hospital project
  chose Cassandra correctly, because its serving store is durable and write-heavy. Same
  taxonomy, different answer, driven by the workload.)*
- **PostgreSQL (reuse the mart)** — collapses the two layers into one store, destroying the
  architectural separation the report argues for and coupling their failure domains.

## Consequences

Redis is not durable. If it dies the live dashboard is blank until the next micro-batch
repopulates it (~seconds), and nothing permanent is lost. That disposability is what makes the
fault-tolerance argument in ADR-001 true.
