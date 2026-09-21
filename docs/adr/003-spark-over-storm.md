# ADR-003 — PySpark Structured Streaming, not Apache Storm

**Status:** Accepted · **Date:** 2026-08-18 · **Full argument:** [`plan/02 §3`](../../plan/02-technology-stack.md)

## Context

The assignment permits *"Apache Spark (Structured Streaming) OR Apache Storm"*. The module
taught Storm as its streaming engine and Spark as its batch engine, so choosing Spark for the
stream needs a defence.

## Decision

PySpark Structured Streaming, for both layers.

1. **We need one engine for both layers.** Our answer to Lambda's "multiple codebases" weakness
   (ADR-001) is a shared `transforms/` package of DataFrame functions called by the streaming
   job *and* the batch job. That only works if both run on the same engine and API. Choosing
   Storm would force two implementations of every transformation — **manufacturing the exact
   weakness we are mitigating.** Storm is architecturally incompatible with our headline answer.
2. **The module taught the theory; Spark implements it.** Event time vs processing time,
   watermarks, tumbling/sliding/session/global windows were all taught — in the Storm deck, as
   concepts with no code. `withWatermark()` and `window()` express them most directly.
3. **Throughput over per-event latency is right here.** Our budget is seconds; a dispatcher
   cannot perceive 200 ms vs 2 s on a wall dashboard.
4. **The module gave a PySpark path and no Storm-in-Python path.** The only external API link
   in the deck set is the PySpark docs. Storm was taught without code, grouping types or Trident.

## Consequences

~1–2 s micro-batch latency floor. Checkpoint invalidation when aggregation logic changes.

Conceded honestly in the report: if the requirement were *"alert within 50 ms of a GPS ping"*,
Storm's per-event model would win outright.
