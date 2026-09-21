# ADR-002 — Partition the telemetry topic by `vehicle_id`

**Status:** Accepted · **Date:** 2026-08-18 · **Full argument:** [`plan/04 §1.2`](../../plan/04-req-ingestion-kafka.md)

## Context

Kafka guarantees ordering *within* a partition, not across partitions. The idle detector is a
per-vehicle state machine: it observes `on_trip → idle → idle → idle` and times how long a
vehicle has been continuously idle.

## Decision

Key every telemetry message by `vehicle_id`. Six partitions.

**This is a correctness requirement of the business logic, not a load-balancing choice.** If a
vehicle's pings were spread across partitions they could be consumed out of order, the state
machine would see phantom transitions, and the idle timer would keep resetting.

Six partitions because: it caps consumer parallelism (4 Spark tasks today, headroom to 6 without
re-partitioning a live topic), and 150 keys spread ~25 per partition so nothing hotspots.

## Rejected

- `zone_id` — 12 values, heavily skewed toward CBD zones → hotspotting, which the module warns
  about explicitly. Also breaks per-vehicle ordering.
- `trip_id` — excellent distribution, but idle pings have `trip_id = null`, so idle events could
  not be co-located with the trip events preceding them.
- `null` (round-robin) — best balance, zero ordering guarantees, incorrect detector.
- `driver_id` — works today (1:1 with vehicle) but breaks the moment a driver swaps vehicles,
  which is a normal event. Key on the entity the state is *about*.

## Consequences

Per-vehicle ordering holds. Verified by a key-distribution test and by a Kafka UI screenshot
showing near-equal per-partition message counts.
