# Final clean run: measurements for the report

Every number in the report comes from one clean run: `make clean`, then
`COMPOSE_PROFILES=obs docker compose up -d --no-build`, started 17:51 UTC on
27 September 2026, simulated clock anchored at 2026-03-01 (`SIM_DAY_SECONDS=300`,
288 times real time). The report reads these values from
`docs/report/measurements.tex`, which is generated from the raw outputs in
[`final-run/`](final-run/) (one file per check; `steps.log` has the order and times).

| Measurement | Value | Source and method |
|---|---|---|
| Ingestion rate | 239.9 events/s | Prometheus, `sum(rate(events_produced_total[5m]))` |
| Live view freshness p50 / p95 | 9.1 s / 23.2 s (real) | 30 samples 2 s apart of `as_of_sim` minus `updated_at_sim` on `/api/v1/fleet/live`, divided by 288 |
| API latency p50 / p95 / p99 | 6 / 18 / 29 ms | Prometheus, `histogram_quantile` over `http_request_duration_highr_seconds_bucket[20m]` |
| Both batch jobs for one day | 53 to 56 s | wall time of `scripts/backfill.py --from D --to D`, two runs |
| Airflow run that batches a day, mean / longest | 91 s and 105 s | `airflow dags list-runs`, successful runs longer than 30 s |
| First alert firing | 76 s after the producer stopped | Prometheus `/api/v1/alerts`, polled every second (`ProducerTargetDown`) |
| "No data received" alert firing | 181 s | same, `NoTelemetryIngested` |
| All alerts resolved | 19 s after `make chaos-heal` | same |
| Alertmanager during the outage | only ProducerTargetDown, with NoTelemetryIngested inhibited as a consequence | Alertmanager `/api/v2/alerts` |
| Memory, whole stack | 4.8 GiB | `docker stats --no-stream`, sum over `fleet-*` |
| Automated tests | 461 passed | `make test` against the running stack |
| High-water mark at yesterday | 81 of 113 (72 %) of samples | `/api/v1/pipeline/status` every 10 s for 20 minutes |
| Streaming job restarts during the Redis stop | no | `docker inspect fleet-streaming-job` |

## Revenue: two independent paths (`make batch-check`)

| Simulated date | P&L revenue | Zone earnings | Difference |
|---|---|---|---|
| 2026-03-01 | 47,664.77 | 47,664.77 | 0.00 |
| 2026-03-02 | 49,644.29 | 49,644.29 | 0.00 |
| 2026-03-03 | 49,969.45 | 49,969.45 | 0.00 |
| 2026-03-04 | 50,707.43 | 50,707.43 | 0.00 |
| 2026-03-05 | 50,456.36 | 50,456.36 | 0.00 |

P&L rows: 750 after 5 simulated days (150 per day).

## Dead-letter control (producer stopped, so both counts are fixed)

Injected: the producer's `defects_injected_total`, read just before the stop.
Dead-lettered: every record on `fleet.telemetry.dlq`, grouped by `rejection_reason`.

| Fault type | Injected | Dead-lettered |
|---|---|---|
| Missing coordinates | 1,415 | 1,415 |
| Position outside the city | 922 | 922 |
| Negative fare | 974 | 975 |
| Impossible speed | 907 | 907 |
| Timestamp two hours ahead | 474 | 475 |
| Total | 4,692 | 4,694 |

## Merge boundary

`/api/v1/fleet/utilization?from=2026-03-02&to=2026-03-05` with the high-water
mark at 2026-03-04: 48 rows, 36 batch and
12 speed, every date from exactly one side.

## Degradation (stores stopped with `docker compose stop`)

| Situation | HTTP | Answer |
|---|---|---|
| Both stores up | 200 | 36 batch, 12 speed rows, not degraded |
| Redis stopped | 200 | 36 batch rows, X-Data-Degraded: true, Redis named as missing |
| Both stopped | 503 | names both stores; never a 500 |
| PostgreSQL stopped | 200 | 12 speed rows, X-Data-Degraded: true, PostgreSQL named as missing; high-water mark reported as unknown |
| Both restarted | 200 | 36 batch, 12 speed rows, not degraded |

## Restatement of 2026-03-02 (high-water mark later throughout)

| Run | Job run id | Sum of restatement_count |
|---|---|---|
| before |  | 0 |
| A | new | 150 (all 150 rows recorded as corrected) |
| B | same as A | 150 (no change) |

## Micro-batch duration, 20 real minutes (Prometheus `quantile_over_time`)

| Query | p50 (s) | p95 (s) |
|---|---|---|
| q_master | 1.5 | 3.4 |
| q_dlq | 0.6 | 3.2 |
| q_vehicles | 2.0 | 4.8 |
| q_activity | 3.7 | 6.2 |
| q_earnings | 3.7 | 7.9 |
| q_idle | 5.9 | 12.8 |

## Speed versus batch reconciliation (fleet level)

Idle ratio within 10.9 %; trips per minute 70 to 79 % low;
earnings per minute 24 to 43 % low, over 3 days
(`mart.reconciliation_delta`, `zone_id = 'ALL'`).
