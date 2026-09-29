# Demo video script — Ride-Hailing Fleet Operations

**EC8202 Big Data Analytics · mini project · Lambda architecture** · video length about **9:17** · three voices (Person 1, Person 2, Person 3)

This file holds the whole video in four forms:

- **Part A — Script only:** just the words, in order, for reading or rehearsing.
- **Part B — Show + script:** for every clip, what to show on screen and what is said.
- **Part C — Text-to-speech:** the same words rewritten for a TTS voice (numbers and short forms spelled out), one block per audio file.
- **Part D — Recording guide:** how to prepare this machine and record against the audio.
- **Part E — Likely questions** for the viva.

Each person also has their own file (`person-1.md`, `person-2.md`, `person-3.md`) with only their clips.
The words are identical in every file.

## Timeline

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
| C14 | 6:04–6:37 | Person 3 | The merge, live | Report p.7 → FastAPI /docs (tab 6) |
| C15 | 6:37–7:01 | Person 3 | Live: question B answered | Grafana Batch Reconciliation (tab 7) |
| C16a | 7:01–7:25 | Person 3 | Live correction, part 1: before | Terminal |
| C16b | 7:25–7:43 | Person 3 | Live correction, part 2: after | Terminal |
| C17 | 7:43–8:11 | Person 3 | Live: a store fails, the answer degrades | Terminal → Grafana Pipeline Health (tab 8) |
| C18a | 8:11–8:24 | Person 3 | Live alerting, part 1: break it | Grafana Pipeline Health (tab 8) → Terminal |
| C18b | 8:24–8:52 | Person 3 | Live alerting, part 2: one clear alert | Pipeline Health → Prometheus (tab 9) → Alertmanager (tab 10) → Terminal |
| C19 | 8:52–9:17 | Person 3 | Honest limits, and close | Report p.16 → p.17 |

Timings assume a voice speed of about 150 words per minute plus a few seconds per clip for clicks and typing. Waiting (the Airflow run in C16, the alert in C18) is cut out of the video.

## What the video proves (assignment coverage)

| Brief requirement / rubric criterion | Where it is shown |
|---|---|
| Architecture decision, Lambda vs Kappa (20) | C02, C03, and proved live in C16 |
| Technology stack and its justification (10) | C04, then every tool doing its job in C05–C18 |
| Data ingestion: streaming source and daily-batch source (15) | C05, C06, C07 |
| Processing: streaming and batch, windows, watermark, join, trend (15) | C08–C13 |
| Storage and serving layer (10) | C07, C11, C13, C14, C15, C17 |
| Observability: structured logs, metrics, an alert rule firing (10) | C07 and C12 (logs), C17 (sink errors), C18 (alerts, inhibition) |
| Consolidated dashboard answering the business question | C10 (question A), C15 (question B) |
| Simulated clock stated clearly | C05 (and report page 3) |
| Honest limitations | C19 |

---

## Part A — Script only

### Person 1 — The problem, the decision, and the data coming in (0:00–3:14)

`C01` Welcome to our mini project for EC8202, Big Data Analytics: Ride-Hailing Fleet Operations, built as a Lambda architecture. First we explain why, and then we show the whole pipeline running live.

`C02` Our use case asks one question: what is fleet utilization and earnings by area right now, and which vehicles are becoming unprofitable once yesterday's costs are counted? This is really two questions. Question A comes from dispatch: it needs an answer in seconds, and a small error is fine. Question B comes from finance: it must be exact, and the word becoming means it needs history.

`C03` So we compared Lambda and Kappa on the module's eight criteria. Six favour Lambda, and two decide it. Historical analysis: becoming unprofitable is a seven-day trend. And reprocessing: when a partner corrects a cost file, Lambda recomputes one stored day, while Kappa would replay a whole day of telemetry. Kappa wins on complexity and cost, and we accept both.

`C04` Here is the architecture, from top to bottom. Telemetry goes into Apache Kafka. In blue, the speed layer: Spark Structured Streaming keeps a live view in Redis, and stores every raw event in the MinIO lake. In green, the batch layer: Airflow runs Spark once per simulated day, and the exact results go into PostgreSQL. At the bottom, FastAPI merges both views for Grafana.

`C05` Now, the live system. One command started every service you see here, and the three setup containers ran once and finished. The clock is simulated: one day lasts five real minutes, two hundred and eighty-eight times faster than real time.

`C06` Data comes in from two sources. The streaming source simulates a hundred and fifty vehicles, sending about two hundred and forty events per second. The telemetry topic has six partitions, and each message is keyed by vehicle ID, so every vehicle's events stay in order for the idle detector. Messages are Avro, checked by the Schema Registry. About one percent are broken on purpose, and they land in the dead-letter topic with the reason.

`C07` The second source is the daily-batch file. After each simulated day, the partners' expense file lands in the lake as one CSV, with fuel, maintenance and distance per vehicle. It skips Kafka, because a daily file is not an event stream. Every service also writes structured logs, like this producer line, tagged with its stage. Next, let us see how this data is processed.

### Person 2 — Processing: the speed layer and the batch layer (3:14–6:04)

`C08` Processing starts with the speed layer. One Spark Structured Streaming job runs six independent queries every ten seconds, each with its own checkpoint: the raw lake writer, the dead-letter writer, zone activity, zone earnings, vehicle state and the idle detector. Windows and watermarks use simulated time, and each trip's fare is counted once, not once per reading.

`C09` These queries share their logic with the batch layer. Lambda is often criticised for needing two codebases, but we have one: all business rules live in one shared transforms package, imported by both the streaming job and the batch jobs, and a test fails if either layer adds its own logic.

`C10` The result is this dashboard, which answers question A: active vehicles, trips, the idle ratio and earnings by zone, refreshed every ten seconds through our own API. At the bottom are the idle alerts. A stateful detector follows each vehicle and raises a warning after forty-five simulated minutes idle, and a critical alert after ninety. These business alerts also go to their own Kafka topic.

`C11` Meanwhile, every valid event is stored unchanged in the lake, as Parquet, with one folder per simulated day. This is the master dataset. It is never edited, so any day can be recomputed exactly.

`C12` That dataset feeds the batch layer, which is run by Airflow. It checks every minute, and when a simulated day is complete, it batches that day, using our simulated clock. The graph shows the steps: snapshot the live view, check the expense file, then two Spark jobs. The profitability job joins each vehicle's telemetry with its costs, and computes net profit, a seven-day rolling average and a trend. Its log line is structured, with service, stage and counts.

`C13` How do we know the batch is right? We check it against numbers we already know. There is one row per vehicle per day. And revenue is computed twice, per vehicle and per zone, by different code, and the two totals agree to the cent on every day. Now let us see how both layers reach the user.

### Person 3 — Serving the answer, corrections, failures and alerts (6:04–9:17)

`C14` The two layers meet in one place: the serving layer. The batch layer keeps a high-water mark, the last day it fully processed. Batch answers up to that day, and the speed layer only after it, so no day is counted twice. Here is one request up to today. Past days come from the batch view, marked exact, and today comes from the speed view, marked approximate.

`C15` And this dashboard answers question B: the ten vehicles with the worst seven-day rolling profit. Vehicle V113, our outskirts vehicle, is at or near the top: almost half its kilometres are unpaid, so it earns the least. The label beside it shows whether its trend is healthy or falling.

`C16a` Now the case that decided our architecture. A partner corrected the cost file for the third of March, because a fuel card was charged to the wrong vehicle. Here is vehicle V113 on that day, before the correction. We ask Airflow to recompute only that day.

`C16b` The run has finished. The fuel cost is lower, the profit is higher, and the row records that it was corrected, and when. Only one stored day was re-read, and the high-water mark did not move.

`C17` What if a store fails? We stop Redis, the speed view. The same request still returns status two hundred, with every exact batch row, and the answer is flagged as degraded and names the missing store. The streaming job keeps running, and this panel counts the skipped writes. Then we bring Redis back.

`C18a` Finally, alerting, which the brief asks for. This dashboard shows one health number per stage. Now we stop the telemetry producer, on purpose.

`C18b` A few minutes later, ingestion is at zero, and Prometheus is firing two alerts: the producer is down, and no telemetry has arrived. Alertmanager shows only the cause, and inhibits the second alert as a consequence, so whoever is on call sees one clear problem. We restart the producer, and the alerts clear within seconds.

`C19` We are open about our limits: there is no distributed tracing, no PDF daily report, and corrections start with one command, not automatically. To sum up: two sources, a speed path and a batch path sharing one library, one clear merge boundary, and a system that shows its own health. Thank you for watching.

---

## Part B — Show + script

### Person 1 — The problem, the decision, and the data coming in (0:00–3:14)

#### C01 · 0:00–0:17 · Person 1 · Opening

**Screen:** Report p.1 (cover)

**Show / do:**
1. Tab 1 (report), page 1, zoom **Fit to page**: university logo, *Mini Project Report · EC8202 Big Data Analytics*, title *Ride-Hailing Fleet Operations*.
2. Hold still. At “Ride-Hailing Fleet Operations”, zoom slowly towards the title.

**Say:**

> Welcome to our mini project for EC8202, Big Data Analytics: Ride-Hailing Fleet Operations, built as a Lambda architecture. First we explain why, and then we show the whole pipeline running live.

#### C02 · 0:17–0:45 · Person 1 · One question, two opposite needs

**Screen:** Report p.3 (§1 The problem)

**Show / do:**
1. Go to page 3. Zoom about 150% on the grey box **Business question (use case 1)** while the question is read.
2. At “Question A”: scroll to the paragraph *Question A, “right now”* and point at **right now**.
3. At “Question B”: point at *Question B, “becoming unprofitable”* and at the word **becoming**.

**Say:**

> Our use case asks one question: what is fleet utilization and earnings by area right now, and which vehicles are becoming unprofitable once yesterday's costs are counted? This is really two questions. Question A comes from dispatch: it needs an answer in seconds, and a small error is fine. Question B comes from finance: it must be exact, and the word becoming means it needs history.

#### C03 · 0:45–1:16 · Person 1 · Lambda or Kappa

**Screen:** Report p.4 (Table 2), then p.5 (§3.3)

**Show / do:**
1. Go to page 4 and zoom to **Table 2 — The eight criteria from the module**.
2. At “historical analysis” point at the bold row **Historical analysis — Decisive**; at “reprocessing” point at **Reprocessing — Decisive**.
3. At “complexity and cost” point at the two rows that favour **Kappa**.
4. Optional, last 3 s: scroll to page 5, heading **3.3 The case for Kappa, and why it lost**.

**Say:**

> So we compared Lambda and Kappa on the module's eight criteria. Six favour Lambda, and two decide it. Historical analysis: becoming unprofitable is a seven-day trend. And reprocessing: when a partner corrects a cost file, Lambda recomputes one stored day, while Kappa would replay a whole day of telemetry. Kappa wins on complexity and cost, and we accept both.

#### C04 · 1:16–1:48 · Person 1 · The architecture

**Screen:** Report p.6 (Figure 2)

**Show / do:**
1. Go to page 6 and zoom until **Figure 2 — The layered architecture** fills the screen.
2. Trace with the pointer as it is named: **Telemetry producer → Apache Kafka → Spark Structured Streaming** (blue) → **Redis** and **MinIO lake** → **Spark batch** + **Airflow** (green) → **PostgreSQL** → **FastAPI** → **Grafana**.

**Say:**

> Here is the architecture, from top to bottom. Telemetry goes into Apache Kafka. In blue, the speed layer: Spark Structured Streaming keeps a live view in Redis, and stores every raw event in the MinIO lake. In green, the batch layer: Airflow runs Spark once per simulated day, and the exact results go into PostgreSQL. At the bottom, FastAPI merges both views for Grafana.

#### C05 · 1:48–2:08 · Person 1 · Live: the running system

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then type `svc` and press Enter. Hold while the list shows.
2. At “the clock is simulated”: type `status` and press Enter.

**You will see:**
- `svc`: 22 services; the rest **Up (healthy)**; `init`, `registry-producer`, `airflow-init` show **Exited (0)** — correct, they run once.
- `status`: `as_of_sim` (the simulated date), `sim_day_index`, `batch_complete_thru`.

**Say:**

> Now, the live system. One command started every service you see here, and the three setup containers ran once and finished. The clock is simulated: one day lasts five real minutes, two hundred and eighty-eight times faster than real time.

#### C06 · 2:08–2:42 · Person 1 · Live: streaming ingestion in Kafka

**Screen:** Kafka UI (tab 2)

**Show / do:**
1. Kafka UI → **Topics**: four topics (`fleet.alerts.v1`, `fleet.telemetry.dlq`, `fleet.telemetry.v1`, `fleet.vehicle.registry`).
2. At “six partitions”: click **fleet.telemetry.v1** → **Overview** (6 partitions with similar message counts).
3. At “messages are Avro”: open **Messages** and click the newest message. The value is decoded: `vehicle_id`, `trip_id`, `lat`, `lon`, `speed_kmh`, `status`, `fare`, `event_time`, `ingest_time`; the key is the vehicle id.
4. At “broken on purpose”: back to **Topics** → **fleet.telemetry.dlq** → **Messages**, expand one and point at **rejection_reason**.

**Say:**

> Data comes in from two sources. The streaming source simulates a hundred and fifty vehicles, sending about two hundred and forty events per second. The telemetry topic has six partitions, and each message is keyed by vehicle ID, so every vehicle's events stay in order for the idle detector. Messages are Avro, checked by the Schema Registry. About one percent are broken on purpose, and they land in the dead-letter topic with the reason.

#### C07 · 2:42–3:14 · Person 1 · Live: the daily file, and a structured log

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `lake ls l/fleet-lake/landing/expenses/` — one `expenses_<date>.csv` per simulated day.
2. Then `lake head -n 4 l/fleet-lake/landing/expenses/expenses_2026-03-05.csv` — header and first rows.
3. At “structured logs”: `docker logs --tail 1 fleet-telemetry-producer | jq -c '{service, stage, event, sim_date}'`

**You will see:**
- CSV header: `vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag,submitted_at,partner_id`; rows like `V001,43.75,8.83,182.28,false,…,GARAGE_D`.
- Log line: `{"service":"telemetry-producer","stage":"ingest","event":"producer_status","sim_date":"2026-03-…"}`.

**Say:**

> The second source is the daily-batch file. After each simulated day, the partners' expense file lands in the lake as one CSV, with fuel, maintenance and distance per vehicle. It skips Kafka, because a daily file is not an event stream. Every service also writes structured logs, like this producer line, tagged with its stage. Next, let us see how this data is processed.

### Person 2 — Processing: the speed layer and the batch layer (3:14–6:04)

#### C08 · 3:14–3:44 · Person 2 · Live: the speed layer in Spark

**Screen:** Spark UI (tab 3)

**Show / do:**
1. Spark UI → **Structured Streaming**: **Active Streaming Queries (6)** — `q_master`, `q_dlq`, `q_activity`, `q_earnings`, `q_idle`, `q_vehicles`. Point at each name as it is spoken.
2. At “windows and watermarks”: click **q_activity** → its charts (Input Rate, Process Rate, Batch Duration). Then go back.

**Say:**

> Processing starts with the speed layer. One Spark Structured Streaming job runs six independent queries every ten seconds, each with its own checkpoint: the raw lake writer, the dead-letter writer, zone activity, zone earnings, vehicle state and the idle detector. Windows and watermarks use simulated time, and each trip's fare is counted once, not once per reading.

#### C09 · 3:44–4:08 · Person 2 · Live: one shared codebase

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `grep -rl "fleet.transforms" src/fleet/speed_layer src/fleet/batch_layer`
2. Optional: `ls src/fleet/transforms/`

**You will see:**
- Five files: three in `speed_layer` (`idle_detector.py`, `streaming_job.py`, `source.py`) and two in `batch_layer` (`zone_hourly.py`, `daily_profitability.py`).

**Say:**

> These queries share their logic with the batch layer. Lambda is often criticised for needing two codebases, but we have one: all business rules live in one shared transforms package, imported by both the streaming job and the batch jobs, and a test fails if either layer adds its own logic.

#### C10 · 4:08–4:39 · Person 2 · Live: question A answered, and idle alerts

**Screen:** Grafana Operations (tab 4)

**Show / do:**
1. Grafana **Fleet · Operations (live)**: hold on the top row — **Active vehicles, Trips in window, Idle ratio, Zones reporting (12)**.
2. Point along **Earnings by zone**.
3. At “idle alerts”: scroll to **Vehicles idle beyond threshold**; point at a **CRITICAL** row if one is there.

**Say:**

> The result is this dashboard, which answers question A: active vehicles, trips, the idle ratio and earnings by zone, refreshed every ten seconds through our own API. At the bottom are the idle alerts. A stateful detector follows each vehicle and raises a warning after forty-five simulated minutes idle, and a critical alert after ninety. These business alerts also go to their own Kafka topic.

#### C11 · 4:39–4:56 · Person 2 · Live: the master dataset

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `lake ls l/fleet-lake/raw/telemetry/` — `_spark_metadata/`, then one `sim_date=2026-03-0X/` folder per day.

**Say:**

> Meanwhile, every valid event is stored unchanged in the lake, as Parquet, with one folder per simulated day. This is the master dataset. It is never edited, so any day can be recomputed exactly.

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

#### C13 · 5:35–6:04 · Person 2 · Live: proving the batch is right

**Screen:** Terminal

**Show / do:**
1. Terminal: `clear`, then `make batch-check | sed -n '1,6p;/revenue vs zone/,$p'`. Point at `pnl rows`, then at the **delta** column.

**You will see:**
- Summary: `watermark`, `pnl rows` (= simulated days × 150), `distinct sim_dates`, `restated rows : 0`, `zone-hourly rows`.
- Table *revenue vs zone earnings (two independent paths to one number - must match)* with **0.00** in every row.

**Say:**

> How do we know the batch is right? We check it against numbers we already know. There is one row per vehicle per day. And revenue is computed twice, per vehicle and per zone, by different code, and the two totals agree to the cent on every day. Now let us see how both layers reach the user.

### Person 3 — Serving the answer, corrections, failures and alerts (6:04–9:17)

#### C14 · 6:04–6:37 · Person 3 · The merge, live

**Screen:** Report p.7 → FastAPI /docs (tab 6)

**Before this clip:** Run `status`: **watermark_age_sim_days must be 1**. If it is 2, wait (at most about 2 minutes) until the next Airflow batch finishes. Run `today` and note the date.

**Show / do:**
1. First 8 s: report page 7, the box **The merge rule** and **Figure 3** (panel a).
2. At “here is one request”: tab 6 → **GET /api/v1/fleet/utilization** → **Try it out** → `from` = `2026-03-02`, `to` = the date from `today` → **Execute**.
3. Scroll the response: point at `"consistency": "batch-complete-through …"`, then rows with `"source": "batch", "exact": true`, and at the bottom rows with `"source": "speed", "exact": false` and their `approximation_note`.

**Say:**

> The two layers meet in one place: the serving layer. The batch layer keeps a high-water mark, the last day it fully processed. Batch answers up to that day, and the speed layer only after it, so no day is counted twice. Here is one request up to today. Past days come from the batch view, marked exact, and today comes from the speed view, marked approximate.

#### C15 · 6:37–7:01 · Person 3 · Live: question B answered

**Screen:** Grafana Batch Reconciliation (tab 7)

**Show / do:**
1. Grafana **Fleet · Batch Reconciliation (daily)**: table **Worst 10 by 7-day rolling average profit**. Point at **V113**, its label (HEALTHY or WATCH) and its trend.
2. Last seconds: glance over the stats row (watermark age, rows in the batch view, restated rows, merge boundary crossings).

**Say:**

> And this dashboard answers question B: the ten vehicles with the worst seven-day rolling profit. Vehicle V113, our outskirts vehicle, is at or near the top: almost half its kilometres are unpaid, so it earns the least. The label beside it shows whether its trend is healthy or falling.

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

#### C18a · 8:11–8:24 · Person 3 · Live alerting, part 1: break it

**Screen:** Grafana Pipeline Health (tab 8) → Terminal

**Before this clip:** At least 1 minute after C17's `make chaos-heal`. `curl -s localhost:9090/api/v1/alerts | jq -r '.data.alerts[].labels.alertname'` prints nothing.

**Show / do:**
1. Tab 8, **Pipeline Health**: point along the top row — **INGEST · events/sec** (about 240), **PROCESS**, **STORE**, **SERVE**.
2. At “we stop the telemetry producer”: terminal `make chaos-kill-producer`.

**Say:**

> Finally, alerting, which the brief asks for. This dashboard shows one health number per stage. Now we stop the telemetry producer, on purpose.

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

#### C19 · 8:52–9:17 · Person 3 · Honest limits, and close

**Screen:** Report p.16 → p.17

**Show / do:**
1. Report page 16, **9.4 What is not implemented**: point at *Distributed tracing*, *A PDF daily report*, *Automatic corrections*.
2. At “to sum up”: page 17, **10 Conclusion**. Hold to the end (optionally fade to the cover, page 1).

**Say:**

> We are open about our limits: there is no distributed tracing, no PDF daily report, and corrections start with one command, not automatically. To sum up: two sources, a speed path and a batch path sharing one library, one clear merge boundary, and a system that shows its own health. Thank you for watching.

---

## Part C — Text-to-speech version

Paste each block into the TTS tool as its own audio file, named as shown. The text is already written for speaking:
numbers, dates and short forms are spelled out, there are no symbols, and sentences are short so the voice breathes naturally.

- **Speed:** normal (about 150 words per minute). A slower voice only makes the clip longer; the screen just holds.
- **Voices:** one voice for all, or one per person (Person 1 = C01–C07, Person 2 = C08–C13, Person 3 = C14–C19). The wording works either way.
- **Gaps:** leave about half a second of silence at the start and end of each file.

If your voice mispronounces a word, change only that word:

| Word on screen | Write it for the voice as | Why |
|---|---|---|
| EC8202 | E C eight two zero two | digits are otherwise read as a number |
| MinIO | Min I O | often read as "minnio" |
| PostgreSQL | Postgres | the full name is long and often misread |
| FastAPI, API | Fast A P I, A P I | spoken as letters |
| CSV, PDF, ID | C S V, P D F, I D | spoken as letters |
| V113 | V one one three | a vehicle name, not "V one hundred thirteen" |
| Parquet | Parquet (or par-kay) | if the voice says "par-ket", write par-kay |
| Redis | Redis (or Reddis) | if the voice says "ree-dis", write Reddis |
| Avro | Avro | usually fine: AV-roh |

### Person 1 — The problem, the decision, and the data coming in

#### C01 · Opening — file `C01.mp3` · 38 words · about 16 s

```text
Welcome to our mini project for E C eight two zero two, Big Data Analytics. The project is Ride-Hailing Fleet Operations, built as a Lambda architecture. First, we explain why. Then, we show the whole pipeline running live.
```

#### C02 · One question, two opposite needs — file `C02.mp3` · 66 words · about 27 s

```text
Our use case asks one question. What is fleet utilization and earnings by area right now, and which vehicles are becoming unprofitable, once yesterday's costs are counted? This is really two questions. Question A comes from dispatch. It needs an answer in seconds, and a small error is fine. Question B comes from finance. It must be exact, and the word becoming means it needs history.
```

#### C03 · Lambda or Kappa — file `C03.mp3` · 72 words · about 29 s

```text
So we compared Lambda and Kappa, using the eight criteria from the module. Six of them favour Lambda, and two decide it. The first is historical analysis. Becoming unprofitable is a trend over seven days. The second is reprocessing. When a partner corrects a cost file, Lambda recomputes one stored day, while Kappa would have to replay a whole day of telemetry. Kappa wins on complexity and cost, and we accept both.
```

#### C04 · The architecture — file `C04.mp3` · 74 words · about 30 s

```text
Here is the architecture, from top to bottom. Telemetry goes into Apache Kafka. In blue is the speed layer. Spark Structured Streaming keeps a live view in Redis, and stores every raw event in the Min I O data lake. In green is the batch layer. Airflow runs Spark once per simulated day, and the exact results go into Postgres. At the bottom, the Fast A P I service merges both views for Grafana.
```

#### C05 · Live: the running system — file `C05.mp3` · 41 words · about 17 s

```text
Now, the live system. One command started every service you see here. The three setup containers ran once, and finished. The clock is simulated. One day lasts five real minutes, which is two hundred and eighty-eight times faster than real time.
```

#### C06 · Live: streaming ingestion in Kafka — file `C06.mp3` · 72 words · about 29 s

```text
Data comes in from two sources. The streaming source simulates one hundred and fifty vehicles, sending about two hundred and forty events every second. The telemetry topic has six partitions, keyed by vehicle I D, so each vehicle's events stay in order for the idle detector. The messages are Avro, checked by the Schema Registry. About one percent are broken on purpose, and they land in this dead-letter topic, with the reason.
```

#### C07 · Live: the daily file, and a structured log — file `C07.mp3` · 68 words · about 28 s

```text
The second source is the daily batch file. After each simulated day, the partners' expense file lands in the data lake as one C S V, with fuel, maintenance, and distance per vehicle. It skips Kafka, because a daily file is not an event stream. Every service also writes structured logs, like this producer line, tagged with its stage. Next, let us see how this data is processed.
```

### Person 2 — Processing: the speed layer and the batch layer

#### C08 · Live: the speed layer in Spark — file `C08.mp3` · 63 words · about 26 s

```text
Processing starts with the speed layer. One Spark Structured Streaming job runs six independent queries, every ten seconds, and each one has its own checkpoint. They are the raw data writer, the dead-letter writer, zone activity, zone earnings, vehicle state, and the idle detector. Windows and watermarks use simulated time. And each trip's fare is counted only once, not once for every reading.
```

#### C09 · Live: one shared codebase — file `C09.mp3` · 52 words · about 21 s

```text
These queries share their logic with the batch layer. Lambda is often criticised for needing two codebases, but we have only one. All business rules live in one shared transforms package. Both the streaming job and the batch jobs import it, and a test fails if either layer adds its own logic.
```

#### C10 · Live: question A answered, and idle alerts — file `C10.mp3` · 70 words · about 28 s

```text
The result is this dashboard, which answers question A. It shows active vehicles, trips, the idle ratio, and earnings by zone, refreshed every ten seconds through our own A P I. At the bottom are the idle alerts. A stateful detector follows each vehicle. It raises a warning after forty-five simulated minutes of idling, and a critical alert after ninety. These business alerts also go to their own Kafka topic.
```

#### C11 · Live: the master dataset — file `C11.mp3` · 37 words · about 15 s

```text
Meanwhile, every valid event is stored, unchanged, in the data lake, as Parquet files, with one folder for each simulated day. This is the master dataset. It is never edited, so any day can be recomputed exactly.
```

#### C12 · Live: the batch layer in Airflow — file `C12.mp3` · 83 words · about 34 s

```text
That dataset feeds the batch layer, which is run by Airflow. It checks every minute, and when a simulated day is complete, it batches that day, using our simulated clock. The graph shows the steps. It snapshots the live view, checks the expense file, and runs two Spark jobs. The profitability job joins each vehicle's telemetry with its costs, and computes net profit, a seven-day rolling average, and a trend. Its log line is structured, with the service, the stage, and the counts.
```

#### C13 · Live: proving the batch is right — file `C13.mp3` · 62 words · about 25 s

```text
How do we know the batch results are right? We check them against numbers we already know. There is exactly one row per vehicle, per day. And the revenue is computed twice, once per vehicle and once per zone, by different code. The two totals agree to the cent, on every day. Now, let us see how both layers reach the user.
```

### Person 3 — Serving the answer, corrections, failures and alerts

#### C14 · The merge, live — file `C14.mp3` · 66 words · about 27 s

```text
The two layers meet in one place, the serving layer. The batch layer keeps a high-water mark, the last day it has fully processed. Batch answers up to that day, and speed answers only after it, so no day is counted twice. Here is one request, up to today. Past days come from the batch view, marked exact. Today comes from the speed view, marked approximate.
```

#### C15 · Live: question B answered — file `C15.mp3` · 55 words · about 22 s

```text
And this dashboard answers question B. It ranks the ten vehicles with the worst seven-day rolling profit. Vehicle V one one three, our outskirts vehicle, is at or near the top. Almost half of its kilometres are unpaid, so it earns the least. The label beside it shows whether its trend is healthy, or falling.
```

#### C16a · Live correction, part 1: before — file `C16a.mp3` · 50 words · about 20 s

```text
Now, the case that decided our architecture. A partner corrected the cost file for the third of March, because a fuel card was charged to the wrong vehicle. Here is vehicle V one one three on that day, before the correction. We ask Airflow to recompute only that one day.
```

*After this clip: STOP recording. Wait until the manual run is green in the Airflow Grid (rehearsal: about 25 seconds; up to a minute if a scheduled run was busy). Then record C16b.*

#### C16b · Live correction, part 2: after — file `C16b.mp3` · 37 words · about 15 s

```text
The run has finished. The fuel cost is lower, the profit is higher, and the row records that it was corrected, and when. Only one stored day was read again, and the high-water mark did not move.
```

#### C17 · Live: a store fails, the answer degrades — file `C17.mp3` · 54 words · about 22 s

```text
What happens if a store fails? We stop Redis, the speed view. The same request still returns status two hundred, with every exact batch row. The answer is flagged as degraded, and it names the missing store. The streaming job keeps running, and this panel counts the skipped writes. Then, we bring Redis back.
```

#### C18a · Live alerting, part 1: break it — file `C18a.mp3` · 24 words · about 10 s

```text
Finally, alerting, which the brief asks for. This dashboard shows one health number for each stage. Now, we stop the telemetry producer, on purpose.
```

*After this clip: STOP recording. Wait about 3½ minutes: `ProducerTargetDown` fires after about 1¼ minutes and `NoTelemetryIngested` after about 3 minutes. Then record C18b.*

#### C18b · Live alerting, part 2: one clear alert — file `C18b.mp3` · 55 words · about 22 s

```text
A few minutes later, ingestion is at zero, and Prometheus is firing two alerts. The producer is down, and no telemetry has arrived. Alertmanager shows only the cause. It inhibits the second alert as a consequence, so whoever is on call sees one clear problem. We restart the producer, and the alerts clear within seconds.
```

#### C19 · Honest limits, and close — file `C19.mp3` · 56 words · about 23 s

```text
We are open about our limits. There is no distributed tracing, no P D F daily report, and corrections start with one command, not automatically. To sum up. Two sources, a speed path and a batch path sharing one library, one clear merge boundary, and a system that shows its own health. Thank you for watching.
```

---

## Part D — Recording guide (this machine, branch `main`)

### D1. About 45 minutes before recording: start a fresh run

```bash
cd ~/57-big-data/ride-hailing-lambda
git status                  # should say: On branch main
make clean && make up-obs   # deletes the old run, starts everything fresh
```

- `make clean` deletes the previous run's data. That is intended: the dates start again at 2026-03-01 and the correction scene (C16) needs a fresh run.
- The images are already built, so this takes about 2 minutes. Write down the time; call it **T**. Record from **T + 40 min** (8 simulated days of data; the corrected file for 2026-03-03 arrives at about T + 20).
- This machine needs its local file `docker-compose.override.yml` (MinIO image, Airflow Docker API version, speed-layer memory). It is already in place and git ignores it. Do not delete it.
- Keep the machine awake (no sleep, no screen lock) while the run is going.

### D2. Terminal (font 16–18 pt, dark theme, window maximised)

Paste this block once; it adds short commands used in the clips:

```bash
cd ~/57-big-data/ride-hailing-lambda
svc()    { docker compose --profile '*' ps -a --format 'table {{.Service}}\t{{.Status}}'; }
lake()   { docker exec fleet-minio sh -c "/opt/bitnami/minio-client/bin/mc alias set l http://localhost:9000 fleetadmin fleetadmin >/dev/null && /opt/bitnami/minio-client/bin/mc $*"; }
today()  { python3 -c "import json,datetime as d; a=json.load(open('state/sim_epoch.json')); w=d.datetime.fromisoformat(a['epoch_wall']); s=d.datetime.fromisoformat(a['epoch_sim']); print((s+(d.datetime.now(d.timezone.utc)-w)*(86400/a['day_seconds'])).date())"; }
status() { curl -s localhost:8000/api/v1/pipeline/status | jq '{as_of_sim, sim_day_index, batch_complete_thru, watermark_age_sim_days, pnl_rows, restated_rows}'; }
util()   { curl -s -D /tmp/h.txt -o /tmp/b.json "localhost:8000/api/v1/fleet/utilization?from=${1:-2026-03-02}&to=$(today)"; grep -iE '^HTTP|x-data-degraded' /tmp/h.txt; jq '{consistency, batch_complete_thru, degraded, missing_stores, rows_by_source: ([.rows[].source] | group_by(.) | map({(.[0]): length}) | add)}' /tmp/b.json; }
pnl()    { curl -s "localhost:8000/api/v1/vehicles/${1:-V113}/profitability?days=${3:-30}" | jq --arg d "${2:-2026-03-03}" '.rows[] | select(.sim_date==$d) | {sim_date, fuel_cost, net_profit, restatement_count, restated_at}'; }
clear
```

| Command | What it shows |
|---|---|
| `svc` | every service and its status |
| `status` | simulated date, batch high-water mark, row counts |
| `today` | today's *simulated* date (works even when Redis is down) |
| `util` | the merged answer from the API, summarised (status line, degraded header, rows per source) |
| `pnl V113 2026-03-03` | one vehicle's profit for one date |
| `lake …` | browse the MinIO data lake |

### D3. Browser (one Chrome window, maximised, bookmarks bar hidden, zoom 110–125%)

| Tab | Page | Address | Note |
|---|---|---|---|
| 1 | Report | `file:///home/sdvn_defense_sibil/57-big-data/ride-hailing-lambda/docs/report/ride-hailing-report-corrected.pdf` | 24 pages; "page" in this script = the page number Chrome shows |
| 2 | Kafka UI | http://localhost:8080 | open **Topics** |
| 3 | Spark UI | http://localhost:4040/StreamingQuery/ | |
| 4 | Grafana Operations | http://localhost:3000/d/fleet-operations?kiosk | `kiosk` hides Grafana menus (Esc leaves it) |
| 5 | Airflow | http://localhost:8082 | log in `admin` / `admin` → **fleet_daily_reconciliation** → **Grid** |
| 6 | FastAPI | http://localhost:8000/docs | |
| 7 | Grafana Batch Reconciliation | http://localhost:3000/d/fleet-batch-reconciliation?kiosk | |
| 8 | Grafana Pipeline Health | http://localhost:3000/d/fleet-pipeline-health?kiosk | |
| 9 | Prometheus alerts | http://localhost:9090/alerts | |
| 10 | Alertmanager | http://localhost:9093 | |

### D4. Readiness checks at T + 40 min

```bash
status                     # sim_day_index 8 or more; batch_complete_thru 2026-03-07 or later; restated_rows 0
pnl V113 2026-03-03        # restatement_count must be 0 (the correction has not been used yet)
lake cat l/fleet-lake/landing/expenses/expenses_2026-03-03.csv | grep ^V113   # fuel_cost here is LOWER than pnl shows
```

In our rehearsal: `pnl` showed fuel 56.11 and the corrected file 30.12. Also check the Airflow **Grid**: no red runs.

### D5. Recording with the text-to-speech audio

1. Generate one audio file per clip from Part C (`C01.mp3` … `C19.mp3`; C16 and C18 have parts **a** and **b**). Use the same voice settings for every clip of one person. The timings in this script assume about 150 words per minute.
2. For each clip: put the screen in its starting state, start the screen recorder **without the microphone**, play the clip's audio in headphones, and do each action when you hear its cue word (cues are quoted in *Show / do*). Stop recording 2 seconds after the audio ends.
3. In the editor, place the screen clip and drop its audio file at the start. If an action took longer than the audio, keep the extra video and let the screen finish; if the audio is longer, hold the last frame.
4. For **C16** and **C18**, record part **a**, stop, wait (see the ⏸ Cut note), then record part **b**. The waiting is not in the video.

### D6. Recording order and one-shot rules

| Step | Clips | Rule |
|---|---|---|
| 1 | C01–C15, C19 | Repeatable: retake as often as you like. |
| 2 | C16a → C16b | **One shot per run**: the correction of 2026-03-03 can be shown only once. Never run `make dag-trigger` before this take. |
| 3 | C17 | Stops Redis. Run `status` first (watermark age must be 1). Heal at the end of the clip. |
| 4 | C18a → C18b | Stops the producer. Record it last. Wait at least 1 minute after C17. |

The live clips C06, C08, C10, C14 and C15 need data flowing: record them before C17/C18, or at least 2 minutes after a `make chaos-heal`.

### D7. If something goes wrong

| Problem | Fix |
|---|---|
| `make up-obs` stops with an error about `init` | You skipped `make clean`. Run `make clean && make up-obs`. |
| A service shows **Restarting** or **unhealthy** in `svc` | Do not record. Check it with `make logs SVC=<name>`. |
| C14 or C17: `consistency` says "1 date(s) in neither view" | The batch for yesterday is not finished yet. Wait about 1–2 minutes (Airflow checks every minute) and retake. |
| C15: V113 is not first | The voice says "at or near the top", which still fits. Point at V113 wherever it is. |
| C16: `restatement_count` is already 1 | The correction was used. Start a fresh run (D1), or skip C16a and record only C16b. |
| C17: HTTP 503 | Postgres is also down. Run `make chaos-heal`, wait, retake. |
| Grafana panels show "No data" | Redis or the producer is down. Run `make chaos-heal`, wait 1–2 minutes. |
| After C18: a red Airflow run or a missing day | Harmless after the last live clip. `make backfill DAYS=3` repairs it if needed. |

---

## Part E — Likely questions

### Person 1

- **Why not Kappa?** History is the question itself (a seven-day trend), a correction should cost one stored day and not a day's replay, a stored file plus a deterministic job is better for auditing driver pay, and a daily CSV is not an event stream (report §3.3).
- **When would you choose Kappa?** If costs arrived continuously, the trend window shrank to hours, and nobody needed to audit the figures (report §3.3).
- **Why key Kafka by vehicle_id?** Kafka keeps order only inside a partition; the idle detector must see each vehicle's readings in order.
- **Why is the expense file not in Kafka?** It is a once-a-day file, not an event stream; a landing folder in object storage plus validation is simpler and loses nothing.
- **Why compress time 288 times?** The brief allows it; it lets us show a week in 35 minutes. Watermarks are in simulated time: 30 simulated minutes is 6.25 real seconds.

### Person 2

- **Why are activity and earnings separate queries?** A streaming query allows one aggregation, and earnings need one row per trip first (report §6.2).
- **What happens to late events?** Events later than the 30-simulated-minute watermark are left out of the live windows but still reach the lake, so the batch layer counts them.
- **How does idle detection keep state?** applyInPandasWithState keyed by vehicle; the start of an idle episode survives between micro-batches; WARNING at 45 and CRITICAL at 90 simulated minutes.
- **Why does Airflow run every minute?** A simulated midnight falls at any offset, so a 5-minute timer could miss a day; checking every minute batches each day about a minute after it ends, and the other runs skip.
- **Why no logic in the DAG?** An AST test forbids it; logic that only runs under a scheduler escapes the unit tests (report §6.4).

### Person 3

- **Why a high-water mark?** Every date gets exactly one owner, so nothing is counted twice or dropped; the mark moves in the same transaction as the facts (report §4.1).
- **What if the batch layer falls behind?** Redis only holds the current window, so the missing dates are listed in uncovered_dates and named in the consistency text (report Figure 3b).
- **Is a correction idempotent?** Yes: an upsert on (vehicle_id, sim_date). The same job id changes nothing; a new job id records a restatement; the watermark never moves back (report Table 11).
- **Why inhibition?** One cause raises several alerts; Alertmanager shows the cause and hides the consequences (report §7.3).
- **Why not Kafka consumer lag?** Structured Streaming keeps offsets in its checkpoint and never commits to a consumer group, so lag reads zero; we alert on the age of the last progress report instead (report §7.1).
- **Do the speed and batch views agree?** The idle ratio agrees within about 11%; counts read low because a daily snapshot catches a half-full window. The real correctness check is the two revenue paths agreeing to the cent (report §9.3).
