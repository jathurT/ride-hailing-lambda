# ADR-006 — Spark runs in local mode by default

**Status:** Accepted · **Date:** 2026-08-19 · **Full argument:** [`plan/10 §1.3`](../../plan/10-infrastructure-docker.md)

## Context

A machine audit found the host has **15.6 GB** of physical RAM. WSL takes 14 GB of it and
Windows needs 3–4 GB back, leaving Docker roughly **11 GB**. The stack as originally specced
needed ~12.7 GB and would not fit.

## Decision

Run the Structured Streaming job as `SparkSession.builder.master("local[4]")` inside the app
container (1.5 GB) instead of a separate `spark-master` plus two `spark-worker` containers
(~3.5 GB). Compose is organised into `core` / `run` / `obs` / `full` profiles; the standalone
cluster appears only under `full`.

## Consequences

**What is unchanged** — and this is the point:

- Structured Streaming semantics are identical: same windowing, watermarking, checkpointing,
  state handling, `foreachBatch` behaviour and output modes.
- The Spark UI still serves on 4040, so the Structured Streaming screenshot is unaffected.

**What is lost:** the live demonstration of tasks distributed across separate executor nodes.
Recovered via `make up-full` for one screenshot during the day-11 capture pass, which is marked
non-optional in the schedule precisely because the profile split makes it easy to forget.

**Framing for the report:** at 240 events/second the demo was never a distributed-scale
demonstration in *any* configuration — a two-worker cluster processing this volume is theatre.
It demonstrates architectural correctness and pipeline behaviour; the scaling analysis in
report §8 addresses scale instead. Stating that is more honest than a cluster that OOMs on
camera.
