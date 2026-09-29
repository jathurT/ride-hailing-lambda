# Demo video — Person 3: Serving the answer, corrections, failures and alerts

**Part 3 of 3 · 6:04–9:17 · clips C14–C19** · EC8202 Big Data Analytics · Ride-Hailing Fleet Operations

This file is everything Person 3 needs: where the part fits, the words to say, what is shown on screen at each moment, and the text-to-speech version of the same words.
The recorder's setup (fresh run, terminal helpers, browser tabs, recording order) is in `complete-script.md`, Part D.

## Where your part fits

| Clip | Time | Voice | What happens | Screen |
|---|---|---|---|---|
| C01 | 0:00–0:17 | Person 1 | Opening | Report p.1 (cover) |
| C02 | 0:17–0:45 | Person 1 | One question, two opposite needs | Report p.3 (§1 The problem) |
| C03 | 0:45–1:16 | Person 1 | Lambda or Kappa | Report p.4 (Table 2), then p.5 (§3.3) |
| C04 | 1:16–1:48 | Person 1 | The architecture | Report p.6 (Figure 2) |
| C05 | 1:48–2:08 | Person 1 | Live: the running system | Terminal |
| C06 | 2:08–2:42 | Person 1 | Live: streaming ingestion in Kafka | Kafka UI (tab 2) |
| C07 | 2:42–3:14 | Person 1 | Live: the daily file, and a structured log | Terminal |
| C08 | 3:14–3:44 | Person 2 | Live: the speed layer in Spark | Spark UI (tab 3) |
| C09 | 3:44–4:08 | Person 2 | Live: one shared codebase | Terminal |
| C10 | 4:08–4:39 | Person 2 | Live: question A answered, and idle alerts | Grafana Operations (tab 4) |
| C11 | 4:39–4:56 | Person 2 | Live: the master dataset | Terminal |
| C12 | 4:56–5:35 | Person 2 | Live: the batch layer in Airflow | Airflow (tab 5) |
| C13 | 5:35–6:04 | Person 2 | Live: proving the batch is right | Terminal |
| **C14** | **6:04–6:37** | **Person 3** | **The merge, live** | **Report p.7 → FastAPI /docs (tab 6)** |
| **C15** | **6:37–7:01** | **Person 3** | **Live: question B answered** | **Grafana Batch Reconciliation (tab 7)** |
| **C16a** | **7:01–7:25** | **Person 3** | **Live correction, part 1: before** | **Terminal** |
| **C16b** | **7:25–7:43** | **Person 3** | **Live correction, part 2: after** | **Terminal** |
| **C17** | **7:43–8:11** | **Person 3** | **Live: a store fails, the answer degrades** | **Terminal → Grafana Pipeline Health (tab 8)** |
| **C18a** | **8:11–8:24** | **Person 3** | **Live alerting, part 1: break it** | **Grafana Pipeline Health (tab 8) → Terminal** |
| **C18b** | **8:24–8:52** | **Person 3** | **Live alerting, part 2: one clear alert** | **Pipeline Health → Prometheus (tab 9) → Alertmanager (tab 10) → Terminal** |
| **C19** | **8:52–9:17** | **Person 3** | **Honest limits, and close** | **Report p.16 → p.17** |

## Your words, in one piece (script only)

`C14` The two layers meet in one place: the serving layer. The batch layer keeps a high-water mark, the last day it fully processed. Batch answers up to that day, and the speed layer only after it, so no day is counted twice. Here is one request up to today. Past days come from the batch view, marked exact, and today comes from the speed view, marked approximate.

`C15` And this dashboard answers question B: the ten vehicles with the worst seven-day rolling profit. Vehicle V113, our outskirts vehicle, is at or near the top: almost half its kilometres are unpaid, so it earns the least. The label beside it shows whether its trend is healthy or falling.

`C16a` Now the case that decided our architecture. A partner corrected the cost file for the third of March, because a fuel card was charged to the wrong vehicle. Here is vehicle V113 on that day, before the correction. We ask Airflow to recompute only that day.

`C16b` The run has finished. The fuel cost is lower, the profit is higher, and the row records that it was corrected, and when. Only one stored day was re-read, and the high-water mark did not move.

`C17` What if a store fails? We stop Redis, the speed view. The same request still returns status two hundred, with every exact batch row, and the answer is flagged as degraded and names the missing store. The streaming job keeps running, and this panel counts the skipped writes. Then we bring Redis back.

`C18a` Finally, alerting, which the brief asks for. This dashboard shows one health number per stage. Now we stop the telemetry producer, on purpose.

`C18b` A few minutes later, ingestion is at zero, and Prometheus is firing two alerts: the producer is down, and no telemetry has arrived. Alertmanager shows only the cause, and inhibits the second alert as a consequence, so whoever is on call sees one clear problem. We restart the producer, and the alerts clear within seconds.

`C19` We are open about our limits: there is no distributed tracing, no PDF daily report, and corrections start with one command, not automatically. To sum up: two sources, a speed path and a batch path sharing one library, one clear merge boundary, and a system that shows its own health. Thank you for watching.

## Your part, clip by clip (show + script + text-to-speech)

#### C14 · 6:04–6:37 · Person 3 · The merge, live

**Screen:** Report p.7 → FastAPI /docs (tab 6)

**Before this clip:** Run `status`: **watermark_age_sim_days must be 1**. If it is 2, wait (at most about 2 minutes) until the next Airflow batch finishes. Run `today` and note the date.

**Show / do:**
1. First 8 s: report page 7, the box **The merge rule** and **Figure 3** (panel a).
2. At “here is one request”: tab 6 → **GET /api/v1/fleet/utilization** → **Try it out** → `from` = `2026-03-02`, `to` = the date from `today` → **Execute**.
3. Scroll the response: point at `"consistency": "batch-complete-through …"`, then rows with `"source": "batch", "exact": true`, and at the bottom rows with `"source": "speed", "exact": false` and their `approximation_note`.

**Say:**

> The two layers meet in one place: the serving layer. The batch layer keeps a high-water mark, the last day it fully processed. Batch answers up to that day, and the speed layer only after it, so no day is counted twice. Here is one request up to today. Past days come from the batch view, marked exact, and today comes from the speed view, marked approximate.

**Text-to-speech** (`C14.mp3`, 66 words, about 27 s):

```text
The two layers meet in one place, the serving layer. The batch layer keeps a high-water mark, the last day it has fully processed. Batch answers up to that day, and speed answers only after it, so no day is counted twice. Here is one request, up to today. Past days come from the batch view, marked exact. Today comes from the speed view, marked approximate.
```

#### C15 · 6:37–7:01 · Person 3 · Live: question B answered

**Screen:** Grafana Batch Reconciliation (tab 7)

**Show / do:**
1. Grafana **Fleet · Batch Reconciliation (daily)**: table **Worst 10 by 7-day rolling average profit**. Point at **V113**, its label (HEALTHY or WATCH) and its trend.
2. Last seconds: glance over the stats row (watermark age, rows in the batch view, restated rows, merge boundary crossings).

**Say:**

> And this dashboard answers question B: the ten vehicles with the worst seven-day rolling profit. Vehicle V113, our outskirts vehicle, is at or near the top: almost half its kilometres are unpaid, so it earns the least. The label beside it shows whether its trend is healthy or falling.

**Text-to-speech** (`C15.mp3`, 55 words, about 22 s):

```text
And this dashboard answers question B. It ranks the ten vehicles with the worst seven-day rolling profit. Vehicle V one one three, our outskirts vehicle, is at or near the top. Almost half of its kilometres are unpaid, so it earns the least. The label beside it shows whether its trend is healthy, or falling.
```

#### C16a · 7:01–7:25 · Person 3 · Live correction, part 1: before

**Screen:** Terminal

**Before this clip:** ONE-SHOT. `pnl V113 2026-03-03` must show `restatement_count: 0` before this take.

**Show / do:**
1. Terminal: `clear`, then `pnl V113 2026-03-03` — the values before the correction.
2. At “we ask Airflow”: `make dag-trigger DATE=2026-03-03 | tail -1`

**You will see:**
- Before (rehearsal): `fuel_cost 56.11`, `net_profit 112.38`, `restatement_count 0`, `restated_at null`.
- `triggered as a RESTATEMENT - the watermark must not move to 2026-03-03`

**Say:**

> Now the case that decided our architecture. A partner corrected the cost file for the third of March, because a fuel card was charged to the wrong vehicle. Here is vehicle V113 on that day, before the correction. We ask Airflow to recompute only that day.

**Text-to-speech** (`C16a.mp3`, 50 words, about 20 s):

```text
Now, the case that decided our architecture. A partner corrected the cost file for the third of March, because a fuel card was charged to the wrong vehicle. Here is vehicle V one one three on that day, before the correction. We ask Airflow to recompute only that one day.
```

**⏸ Cut:** STOP recording. Wait until the manual run is green in the Airflow Grid (rehearsal: about 25 seconds; up to a minute if a scheduled run was busy). Then record C16b.

#### C16b · 7:25–7:43 · Person 3 · Live correction, part 2: after

**Screen:** Terminal

**Show / do:**
1. Terminal: `pnl V113 2026-03-03` — the values after the correction.
2. At “the high-water mark”: `status` — `batch_complete_thru` is still the latest day.

**You will see:**
- After (rehearsal): `fuel_cost 30.12`, `net_profit 137.41`, `restatement_count 1`, `restated_at` set.

**Say:**

> The run has finished. The fuel cost is lower, the profit is higher, and the row records that it was corrected, and when. Only one stored day was re-read, and the high-water mark did not move.

**Text-to-speech** (`C16b.mp3`, 37 words, about 15 s):

```text
The run has finished. The fuel cost is lower, the profit is higher, and the row records that it was corrected, and when. Only one stored day was read again, and the high-water mark did not move.
```

#### C17 · 7:43–8:11 · Person 3 · Live: a store fails, the answer degrades

**Screen:** Terminal → Grafana Pipeline Health (tab 8)

**Before this clip:** Run `status`: **watermark_age_sim_days must be 1** (otherwise the answer also lists a missing day). Keep Redis stopped for less than a minute.

**Show / do:**
1. Terminal: `clear`, then `docker stop fleet-redis`, then `util`.
2. At “this panel”: tab 8, **Pipeline Health**: the stat **STORE · sink write errors/sec** is now above zero.
3. At “we bring Redis back”: terminal `make chaos-heal`.

**You will see:**
- `HTTP/1.1 200 OK` · `x-data-degraded: true` · `consistency: "batch-complete-through …; DEGRADED - unreachable: redis"` · `missing_stores: ["redis"]` · `rows_by_source: {"batch": …}`
- Rehearsal: the streaming job did not restart; sink write errors rose to about 0.2 per second; Parquet files kept being written.

**Say:**

> What if a store fails? We stop Redis, the speed view. The same request still returns status two hundred, with every exact batch row, and the answer is flagged as degraded and names the missing store. The streaming job keeps running, and this panel counts the skipped writes. Then we bring Redis back.

**Text-to-speech** (`C17.mp3`, 54 words, about 22 s):

```text
What happens if a store fails? We stop Redis, the speed view. The same request still returns status two hundred, with every exact batch row. The answer is flagged as degraded, and it names the missing store. The streaming job keeps running, and this panel counts the skipped writes. Then, we bring Redis back.
```

#### C18a · 8:11–8:24 · Person 3 · Live alerting, part 1: break it

**Screen:** Grafana Pipeline Health (tab 8) → Terminal

**Before this clip:** At least 1 minute after C17's `make chaos-heal`. `curl -s localhost:9090/api/v1/alerts | jq -r '.data.alerts[].labels.alertname'` prints nothing.

**Show / do:**
1. Tab 8, **Pipeline Health**: point along the top row — **INGEST · events/sec** (about 240), **PROCESS**, **STORE**, **SERVE**.
2. At “we stop the telemetry producer”: terminal `make chaos-kill-producer`.

**Say:**

> Finally, alerting, which the brief asks for. This dashboard shows one health number per stage. Now we stop the telemetry producer, on purpose.

**Text-to-speech** (`C18a.mp3`, 24 words, about 10 s):

```text
Finally, alerting, which the brief asks for. This dashboard shows one health number for each stage. Now, we stop the telemetry producer, on purpose.
```

**⏸ Cut:** STOP recording. Wait about 3½ minutes: `ProducerTargetDown` fires after about 1¼ minutes and `NoTelemetryIngested` after about 3 minutes. Then record C18b.

#### C18b · 8:24–8:52 · Person 3 · Live alerting, part 2: one clear alert

**Screen:** Pipeline Health → Prometheus (tab 9) → Alertmanager (tab 10) → Terminal

**Show / do:**
1. Tab 8: **INGEST · events/sec** shows **0**.
2. At “Prometheus is firing”: tab 9, Alerts — **ProducerTargetDown** and **NoTelemetryIngested** are **FIRING**.
3. At “Alertmanager”: tab 10 — only **ProducerTargetDown** is shown. Tick **Inhibited**: **NoTelemetryIngested** appears, marked inhibited.
4. At “we restart the producer”: terminal `make chaos-heal`.

**You will see:**
- Rehearsal on this machine: ProducerTargetDown fired 75 s after the stop, NoTelemetryIngested at 178 s; StreamingQueryStalled did not fire.
- Alertmanager: **ProducerTargetDown = active**, **NoTelemetryIngested = suppressed (inhibited)**.
- After `make chaos-heal`: the live view was back within seconds and every alert cleared within about 30 s.

**Say:**

> A few minutes later, ingestion is at zero, and Prometheus is firing two alerts: the producer is down, and no telemetry has arrived. Alertmanager shows only the cause, and inhibits the second alert as a consequence, so whoever is on call sees one clear problem. We restart the producer, and the alerts clear within seconds.

**Text-to-speech** (`C18b.mp3`, 55 words, about 22 s):

```text
A few minutes later, ingestion is at zero, and Prometheus is firing two alerts. The producer is down, and no telemetry has arrived. Alertmanager shows only the cause. It inhibits the second alert as a consequence, so whoever is on call sees one clear problem. We restart the producer, and the alerts clear within seconds.
```

#### C19 · 8:52–9:17 · Person 3 · Honest limits, and close

**Screen:** Report p.16 → p.17

**Show / do:**
1. Report page 16, **9.4 What is not implemented**: point at *Distributed tracing*, *A PDF daily report*, *Automatic corrections*.
2. At “to sum up”: page 17, **10 Conclusion**. Hold to the end (optionally fade to the cover, page 1).

**Say:**

> We are open about our limits: there is no distributed tracing, no PDF daily report, and corrections start with one command, not automatically. To sum up: two sources, a speed path and a batch path sharing one library, one clear merge boundary, and a system that shows its own health. Thank you for watching.

**Text-to-speech** (`C19.mp3`, 56 words, about 23 s):

```text
We are open about our limits. There is no distributed tracing, no P D F daily report, and corrections start with one command, not automatically. To sum up. Two sources, a speed path and a batch path sharing one library, one clear merge boundary, and a system that shows its own health. Thank you for watching.
```

## Handover

This is the end of the video.

## Likely questions for you

- **Why a high-water mark?** Every date gets exactly one owner, so nothing is counted twice or dropped; the mark moves in the same transaction as the facts (report §4.1).
- **What if the batch layer falls behind?** Redis only holds the current window, so the missing dates are listed in uncovered_dates and named in the consistency text (report Figure 3b).
- **Is a correction idempotent?** Yes: an upsert on (vehicle_id, sim_date). The same job id changes nothing; a new job id records a restatement; the watermark never moves back (report Table 11).
- **Why inhibition?** One cause raises several alerts; Alertmanager shows the cause and hides the consequences (report §7.3).
- **Why not Kafka consumer lag?** Structured Streaming keeps offsets in its checkpoint and never commits to a consumer group, so lag reads zero; we alert on the age of the last progress report instead (report §7.1).
- **Do the speed and batch views agree?** The idle ratio agrees within about 11%; counts read low because a daily snapshot catches a half-full window. The real correctness check is the two revenue paths agreeing to the cent (report §9.3).
