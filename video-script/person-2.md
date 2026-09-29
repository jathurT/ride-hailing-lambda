# Demo video — Person 2: Processing: the speed layer and the batch layer

**Part 2 of 3 · 3:14–6:04 · clips C08–C13** · EC8202 Big Data Analytics · Ride-Hailing Fleet Operations

This file is everything Person 2 needs: where the part fits, the words to say, what is shown on screen at each moment, and the text-to-speech version of the same words.
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
| **C08** | **3:14–3:44** | **Person 2** | **Live: the speed layer in Spark** | **Spark UI (tab 3)** |
| **C09** | **3:44–4:08** | **Person 2** | **Live: one shared codebase** | **Terminal** |
| **C10** | **4:08–4:39** | **Person 2** | **Live: question A answered, and idle alerts** | **Grafana Operations (tab 4)** |
| **C11** | **4:39–4:56** | **Person 2** | **Live: the master dataset** | **Terminal** |
| **C12** | **4:56–5:35** | **Person 2** | **Live: the batch layer in Airflow** | **Airflow (tab 5)** |
| **C13** | **5:35–6:04** | **Person 2** | **Live: proving the batch is right** | **Terminal** |
| C14 | 6:04–6:37 | Person 3 | The merge, live | Report p.7 → FastAPI /docs (tab 6) |
| C15 | 6:37–7:01 | Person 3 | Live: question B answered | Grafana Batch Reconciliation (tab 7) |
| C16a | 7:01–7:25 | Person 3 | Live correction, part 1: before | Terminal |
| C16b | 7:25–7:43 | Person 3 | Live correction, part 2: after | Terminal |
| C17 | 7:43–8:11 | Person 3 | Live: a store fails, the answer degrades | Terminal → Grafana Pipeline Health (tab 8) |
| C18a | 8:11–8:24 | Person 3 | Live alerting, part 1: break it | Grafana Pipeline Health (tab 8) → Terminal |
| C18b | 8:24–8:52 | Person 3 | Live alerting, part 2: one clear alert | Pipeline Health → Prometheus (tab 9) → Alertmanager (tab 10) → Terminal |
| C19 | 8:52–9:17 | Person 3 | Honest limits, and close | Report p.16 → p.17 |

## Your words, in one piece (script only)

`C08` Processing starts with the speed layer. One Spark Structured Streaming job runs six independent queries every ten seconds, each with its own checkpoint: the raw lake writer, the dead-letter writer, zone activity, zone earnings, vehicle state and the idle detector. Windows and watermarks use simulated time, and each trip's fare is counted once, not once per reading.

`C09` These queries share their logic with the batch layer. Lambda is often criticised for needing two codebases, but we have one: all business rules live in one shared transforms package, imported by both the streaming job and the batch jobs, and a test fails if either layer adds its own logic.

`C10` The result is this dashboard, which answers question A: active vehicles, trips, the idle ratio and earnings by zone, refreshed every ten seconds through our own API. At the bottom are the idle alerts. A stateful detector follows each vehicle and raises a warning after forty-five simulated minutes idle, and a critical alert after ninety. These business alerts also go to their own Kafka topic.

`C11` Meanwhile, every valid event is stored unchanged in the lake, as Parquet, with one folder per simulated day. This is the master dataset. It is never edited, so any day can be recomputed exactly.

`C12` That dataset feeds the batch layer, which is run by Airflow. It checks every minute, and when a simulated day is complete, it batches that day, using our simulated clock. The graph shows the steps: snapshot the live view, check the expense file, then two Spark jobs. The profitability job joins each vehicle's telemetry with its costs, and computes net profit, a seven-day rolling average and a trend. Its log line is structured, with service, stage and counts.

`C13` How do we know the batch is right? We check it against numbers we already know. There is one row per vehicle per day. And revenue is computed twice, per vehicle and per zone, by different code, and the two totals agree to the cent on every day. Now let us see how both layers reach the user.

## Your part, clip by clip (show + script + text-to-speech)

#### C08 · 3:14–3:44 · Person 2 · Live: the speed layer in Spark

**Screen:** Spark UI (tab 3)

**Show / do:**
1. Spark UI → **Structured Streaming**: **Active Streaming Queries (6)** — `q_master`, `q_dlq`, `q_activity`, `q_earnings`, `q_idle`, `q_vehicles`. Point at each name as it is spoken.
2. At “windows and watermarks”: click **q_activity** → its charts (Input Rate, Process Rate, Batch Duration). Then go back.

**Say:**

> Processing starts with the speed layer. One Spark Structured Streaming job runs six independent queries every ten seconds, each with its own checkpoint: the raw lake writer, the dead-letter writer, zone activity, zone earnings, vehicle state and the idle detector. Windows and watermarks use simulated time, and each trip's fare is counted once, not once per reading.

**Text-to-speech** (`C08.mp3`, 63 words, about 26 s):

```text
Processing starts with the speed layer. One Spark Structured Streaming job runs six independent queries, every ten seconds, and each one has its own checkpoint. They are the raw data writer, the dead-letter writer, zone activity, zone earnings, vehicle state, and the idle detector. Windows and watermarks use simulated time. And each trip's fare is counted only once, not once for every reading.
```

#### C09 · 3:44–4:08 · Person 2 · Live: one shared codebase

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `grep -rl "fleet.transforms" src/fleet/speed_layer src/fleet/batch_layer`
2. Optional: `ls src/fleet/transforms/`

**You will see:**
- Five files: three in `speed_layer` (`idle_detector.py`, `streaming_job.py`, `source.py`) and two in `batch_layer` (`zone_hourly.py`, `daily_profitability.py`).

**Say:**

> These queries share their logic with the batch layer. Lambda is often criticised for needing two codebases, but we have one: all business rules live in one shared transforms package, imported by both the streaming job and the batch jobs, and a test fails if either layer adds its own logic.

**Text-to-speech** (`C09.mp3`, 52 words, about 21 s):

```text
These queries share their logic with the batch layer. Lambda is often criticised for needing two codebases, but we have only one. All business rules live in one shared transforms package. Both the streaming job and the batch jobs import it, and a test fails if either layer adds its own logic.
```

#### C10 · 4:08–4:39 · Person 2 · Live: question A answered, and idle alerts

**Screen:** Grafana Operations (tab 4)

**Show / do:**
1. Grafana **Fleet · Operations (live)**: hold on the top row — **Active vehicles, Trips in window, Idle ratio, Zones reporting (12)**.
2. Point along **Earnings by zone**.
3. At “idle alerts”: scroll to **Vehicles idle beyond threshold**; point at a **CRITICAL** row if one is there.

**Say:**

> The result is this dashboard, which answers question A: active vehicles, trips, the idle ratio and earnings by zone, refreshed every ten seconds through our own API. At the bottom are the idle alerts. A stateful detector follows each vehicle and raises a warning after forty-five simulated minutes idle, and a critical alert after ninety. These business alerts also go to their own Kafka topic.

**Text-to-speech** (`C10.mp3`, 70 words, about 28 s):

```text
The result is this dashboard, which answers question A. It shows active vehicles, trips, the idle ratio, and earnings by zone, refreshed every ten seconds through our own A P I. At the bottom are the idle alerts. A stateful detector follows each vehicle. It raises a warning after forty-five simulated minutes of idling, and a critical alert after ninety. These business alerts also go to their own Kafka topic.
```

#### C11 · 4:39–4:56 · Person 2 · Live: the master dataset

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `lake ls l/fleet-lake/raw/telemetry/` — `_spark_metadata/`, then one `sim_date=2026-03-0X/` folder per day.

**Say:**

> Meanwhile, every valid event is stored unchanged in the lake, as Parquet, with one folder per simulated day. This is the master dataset. It is never edited, so any day can be recomputed exactly.

**Text-to-speech** (`C11.mp3`, 37 words, about 15 s):

```text
Meanwhile, every valid event is stored, unchanged, in the data lake, as Parquet files, with one folder for each simulated day. This is the master dataset. It is never edited, so any day can be recomputed exactly.
```

#### C12 · 4:56–5:35 · Person 2 · Live: the batch layer in Airflow

**Screen:** Airflow (tab 5)

**Show / do:**
1. Airflow → **fleet_daily_reconciliation** → **Grid**: one run every minute; most stop at the first task (skipped), and one full green run per simulated day.
2. At “the graph shows the steps”: click **Graph** and point along `resolve_sim_date` → `snapshot_speed_view` → `validate_expense_file` (branch: `quarantine_file` / `handle_missing_expense_file` / `run_zone_hourly`) → `run_spark_profitability` → `verify_watermark_advanced` → `compute_reconciliation_delta`.
3. At “its log line”: back to **Grid**, click the green **run_spark_profitability** square of the latest full run → **Logs** → Ctrl+F `batch_complete`.

**You will see:**
- `batch_complete  HEALTHY=… WATCH=… rows_upserted=150  service=batch-profitability  sim_date=2026-03-0X  stage=process  watermark_advanced=True`

**Say:**

> That dataset feeds the batch layer, which is run by Airflow. It checks every minute, and when a simulated day is complete, it batches that day, using our simulated clock. The graph shows the steps: snapshot the live view, check the expense file, then two Spark jobs. The profitability job joins each vehicle's telemetry with its costs, and computes net profit, a seven-day rolling average and a trend. Its log line is structured, with service, stage and counts.

**Text-to-speech** (`C12.mp3`, 83 words, about 34 s):

```text
That dataset feeds the batch layer, which is run by Airflow. It checks every minute, and when a simulated day is complete, it batches that day, using our simulated clock. The graph shows the steps. It snapshots the live view, checks the expense file, and runs two Spark jobs. The profitability job joins each vehicle's telemetry with its costs, and computes net profit, a seven-day rolling average, and a trend. Its log line is structured, with the service, the stage, and the counts.
```

#### C13 · 5:35–6:04 · Person 2 · Live: proving the batch is right

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `make batch-check | sed -n '1,6p;/revenue vs zone/,$p'`. Point at `pnl rows`, then at the **delta** column.

**You will see:**
- Summary: `watermark`, `pnl rows` (= simulated days × 150), `distinct sim_dates`, `restated rows : 0`, `zone-hourly rows`.
- Table *revenue vs zone earnings (two independent paths to one number - must match)* with **0.00** in every row.

**Say:**

> How do we know the batch is right? We check it against numbers we already know. There is one row per vehicle per day. And revenue is computed twice, per vehicle and per zone, by different code, and the two totals agree to the cent on every day. Now let us see how both layers reach the user.

**Text-to-speech** (`C13.mp3`, 62 words, about 25 s):

```text
How do we know the batch results are right? We check them against numbers we already know. There is exactly one row per vehicle, per day. And the revenue is computed twice, once per vehicle and once per zone, by different code. The two totals agree to the cent, on every day. Now, let us see how both layers reach the user.
```

## Handover

Person 3 continues with the serving layer (C14).

## Likely questions for you

- **Why are activity and earnings separate queries?** A streaming query allows one aggregation, and earnings need one row per trip first (report §6.2).
- **What happens to late events?** Events later than the 30-simulated-minute watermark are left out of the live windows but still reach the lake, so the batch layer counts them.
- **How does idle detection keep state?** applyInPandasWithState keyed by vehicle; the start of an idle episode survives between micro-batches; WARNING at 45 and CRITICAL at 90 simulated minutes.
- **Why does Airflow run every minute?** A simulated midnight falls at any offset, so a 5-minute timer could miss a day; checking every minute batches each day about a minute after it ends, and the other runs skip.
- **Why no logic in the DAG?** An AST test forbids it; logic that only runs under a scheduler escapes the unit tests (report §6.4).
