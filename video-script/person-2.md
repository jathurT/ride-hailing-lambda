# Demo video script — Person 2

## Part 2 of 3 · 3:09 – 6:07 · "Processing: the speed layer and the batch layer"

| | |
|---|---|
| **Voice** | Person 2 |
| **Screen** | The recorder records every frame of all three parts |
| **Marks this part earns** | Processing layer (15): streaming and batch, windows, watermarks, the two-source join · Storage (part of 10): the master dataset and the batch view · Code quality (5): one shared codebase · Observability (part of 10): structured logs |

> **How to read this file**
> - **Recorder:** follow **Screen** and **Do**. Hold each shot 1–2 s longer than the voice line.
> - **Person 2 (voice):** read only the **Say** lines. Record one audio file per frame (`P2-F01.wav`, …) and keep each close to its target length. Pronunciations and recording tips are in `person-1.md`, section G.
> - **Setup:** the stack, the terminal helpers (`svc`, `lake`, `status`, …) and the browser tabs are prepared once, in `person-1.md`, sections A–E.

---

## The whole video at a glance

| Frame | Time | Voice | What happens | Screen |
|---|---|---|---|---|
| P1-F01 → P1-F09 | 0:00–3:09 | P1 | The problem, the decision, the data coming in | Report p.1–11 → terminal → Kafka UI |
| **P2-F01** | **3:09–3:24** | **P2** | **The fork** | **Report p.10 (Figure 4)** |
| **P2-F02** | **3:24–3:58** | **P2** | **Live: six streaming queries** | **Spark UI** |
| **P2-F03** | **3:58–4:14** | **P2** | **Live: one shared codebase** | **Terminal** |
| **P2-F04** | **4:14–4:45** | **P2** | **Live: question A answered + idle alerts** | **Grafana *Operations*** |
| **P2-F05** | **4:45–4:59** | **P2** | **Live: the master dataset** | **Terminal** |
| **P2-F06** | **4:59–5:40** | **P2** | **Live: batch orchestration + structured log** | **Airflow** |
| **P2-F07** | **5:40–6:07** | **P2** | **Live: correctness → handover** | **Terminal + Report p.15** |
| P3-F01 → P3-F08 | 6:07–9:20 | P3 | Serving, the business answer, the restatement, observability, close | Report → FastAPI → Grafana → terminal → Prometheus / Alertmanager |

---

## Before recording Person 2's frames

- Record **after** Person 1's frames and **before** any of Person 3's chaos frames (P3-F06, P3-F07). Those deliberately break the pipeline, and the Airflow runs during them turn red.
- **Live data must be flowing:**
  - Run `svc`: the producers, `streaming-job` and `redis` should be **Up**.
  - Tab 3 should show **"Active Streaming Queries (6)"**.
- **Airflow:** logged in (tab 5), with the latest columns in the Grid green.
- **Terminal:** the helpers from `person-1.md` section C are pasted. Run `clear`.

---

## Person 2 — frame by frame

### P2-F01 · 3:09–3:24 (15 s) · The fork
- **Screen:** Report **p.10**, §4.2 *Event path*, **Figure 4**. The sequence diagram runs Vehicle simulator → Kafka → Spark micro-batch → **"THE FORK"** → Redis & MinIO → Airflow → Spark batch → FastAPI → Grafana.
- **Do:** Zoom until the diagram fills the screen.
  - 0–7 s: point at **"THE FORK"**.
  - 7–15 s: move down to **"re-read a whole simulated day later"** and the green **Airflow → Spark batch** box.
- **Say:** "One event's journey. After Kafka the path forks: the same event is aggregated approximately within seconds — and kept unchanged in the master dataset, so a simulated day later it can be recomputed exactly."
- **Caption:** *One event, two paths: seconds and approximate · a day later and exact*

### P2-F02 · 3:24–3:58 (34 s) · **Live:** the speed layer, six streaming queries
- **Screen:** Spark UI (tab 3), **Structured Streaming** page: **"Active Streaming Queries (6)"**. The six queries are `q_master`, `q_dlq`, `q_activity`, `q_earnings`, `q_idle` and `q_vehicles`, each **RUNNING**.
- **Do:**
  - 0–14 s: hold on the list and point at each name as it's spoken.
  - 14–30 s: click **`q_activity`** to open its statistics page (Input Rate, Process Rate, Batch Duration charts). Point at Input Rate against Process Rate: processing keeps ahead of input.
  - 30–34 s: go back.
- **Say:** "The speed layer is six independent Structured Streaming queries on one topic, each with its own checkpoint: the master-dataset writer, the dead-letter path, zone activity, zone earnings, vehicle state and the idle detector. Windows are fifteen simulated minutes, sliding every five, with a thirty-minute watermark — all in simulated time. And revenue is counted once per trip, not per ping, because every ping repeats the trip's fare."
- **Caption:** *6 queries · 15-min windows, 5-min slide, 30-min watermark (simulated time)*

### P2-F03 · 3:58–4:14 (16 s) · **Live:** one shared codebase
- **Screen:** Terminal.
- **Do:** `clear`, then:
  ```bash
  grep -rl "fleet.transforms" src/fleet/speed_layer src/fleet/batch_layer
  ```
  Hold 8 s. **You will see** five files: three from the speed layer (`idle_detector.py`, `streaming_job.py`, `source.py`) and two from the batch layer (`zone_hourly.py`, `daily_profitability.py`). Optionally follow with `ls src/fleet/transforms/`.
- **Say:** "Lambda's classic weakness is maintaining two codebases. We have one: pure transformation functions in fleet-dot-transforms, imported by both the streaming job and the batch jobs — and a unit test enforces that."
- **Caption:** *One library, two entry points · enforced by `test_transforms_purity.py`*

### P2-F04 · 4:14–4:45 (31 s) · **Live:** question A answered, and the idle alerts
- **Screen:** Grafana **"Fleet · Operations (live)"** (tab 4, kiosk mode).
  - Top row: **Active vehicles · Trips in window · Idle ratio · Zones reporting (12)**.
  - Middle: **Earnings by zone** (bar chart) and **Zone detail** (table).
  - Bottom: **Vehicles idle beyond threshold** (vehicle, severity WARNING or CRITICAL, idle minutes, zone).
- **Do:**
  - 0–9 s: hold on the top row.
  - 9–18 s: point along **Earnings by zone**.
  - 18–31 s: scroll to **Vehicles idle beyond threshold** and point at a **CRITICAL** row if there is one.
- **Say:** "This dashboard answers question A. Active vehicles, trips, idle ratio and earnings by zone, for the current window — refreshed every ten seconds and labelled approximate. At the bottom are the idle alerts: a stateful, per-vehicle detector raises a warning after forty-five simulated minutes idle, and a critical after ninety. These are business alerts, on their own Kafka topic — kept separate from pipeline alerts."
- **Caption:** *Question A answered · live, approximate · idle alert at 45 / 90 simulated minutes*
- **Note:** "Active vehicles" can read slightly above 150. It adds up each zone's count, and a vehicle that crossed zones in the window is counted in both. Don't comment on it in the video; the answer is in *Likely questions* below.

### P2-F05 · 4:45–4:59 (14 s) · **Live:** the master dataset
- **Screen:** Terminal.
- **Do:** `clear`, then:
  ```bash
  lake ls l/fleet-lake/raw/telemetry/
  ```
  Hold 4 s: `_spark_metadata/`, then **one `sim_date=2026-03-0X/` folder per simulated day**. Then:
  ```bash
  lake du l/fleet-lake/raw/telemetry/sim_date=2026-03-05/
  ```
  Hold 4 s. In our rehearsal: `2.9MiB  186 objects`, the Parquet files for that day.
- **Say:** "Every validated event is also written, unchanged, to Parquet on MinIO — partitioned by simulated date. That's the immutable master dataset: recomputing a day means re-reading one folder."
- **Caption:** *Master dataset · Parquet on MinIO · one folder per simulated day*

### P2-F06 · 4:59–5:40 (41 s) · **Live:** batch orchestration, and a structured log line
- **Screen:** Airflow (tab 5), DAG **`fleet_daily_reconciliation`**.
- **Do:**
  1. 0–6 s, **Grid:** columns of green squares, one column per run (a run every 5 real minutes).
  2. 6–22 s, click the **Graph** tab and point along the flow:
     - `resolve_sim_date` → `snapshot_speed_view` → `validate_expense_file`
     - the branch: `quarantine_file` | `handle_missing_expense_file` | `run_zone_hourly`
     - `run_spark_profitability` → `verify_watermark_advanced` → `compute_reconciliation_delta` → `notify_success` / `notify_failure`
  3. 22–41 s, back on the **Grid**: click the latest green square in the **`run_spark_profitability`** row, then the **Logs** tab. Press **Ctrl+F** and search `batch_complete`. The line reads:
     `batch_complete  HEALTHY=… WATCH=… rows_upserted=150  service=batch-profitability  sim_date=2026-03-0X  stage=process  watermark_advanced=True`
     Point at `stage=process` and hold 8 s.
- **Say:** "The batch layer is orchestrated by Airflow: one DAG run per simulated day, every five real minutes. It takes the date from our simulated clock, not from Airflow's own date. It validates the expense file and branches — quarantine a bad file, handle a missing one, or continue — then runs two Spark jobs. The profitability job joins each vehicle's telemetry with its expenses, computes net profit, a seven-day rolling average and a trend slope, and classifies every vehicle. Its log line is structured: service, stage and counts."
- **Caption:** *Airflow · one run = one simulated day · no business logic inside the DAG*

### P2-F07 · 5:40–6:07 (27 s) · **Live:** proving the batch layer is right → handover
- **Screen:** Terminal, then Report **p.15**, **Table 5**.
- **Do:** `clear`, then:
  ```bash
  make batch-check | sed -n '1,6p;/revenue vs zone/,$p'
  ```
  Hold 12 s. **You will see:**
  - the summary lines: watermark, `pnl rows` (= simulated days × 150), `distinct sim_dates`, `restated rows : 0`, `zone-hourly rows`;
  - the table **"revenue vs zone earnings (two independent paths to one number - must match)"**, with **`delta 0.00` on every row**. Point at the delta column.

  Then switch to tab 1 → Report **p.15**, zoom on **Table 5** (the same comparison, delta 0.00), and hold 5 s.
- **Say:** "Two checks show the batch layer is right. The row count is simulated days times a hundred and fifty vehicles. And revenue — computed per vehicle, and separately per zone, through different code — agrees to the cent, on every day. That's Table 5 in our report, reproduced live. Person 3 now shows how both layers are served."
- **Caption:** *Two independent computations · delta 0.00 every day*

---

## If something goes wrong (Person 2's frames)

| Problem | What to do |
|---|---|
| Spark UI shows fewer than 6 queries, or the page doesn't load | The streaming job is restarting, probably after a Redis stop. Wait 1–2 minutes, then check `svc`. |
| Grafana panels show "No data" | The producer or Redis is down (after a chaos frame). Run `make chaos-heal`, wait 1–2 minutes and retake. |
| The latest Airflow column is red | A chaos frame ran during that DAG run. Record Person 2's frames before Person 3's chaos frames. To repair after recording: `make backfill DAYS=3`. |
| `restated rows` is above 0 | The restatement (P3-F05) has already been recorded. That's fine; the voice line doesn't mention this number. |
| The `batch_complete` line isn't found | You opened the log of a different task. It must be **`run_spark_profitability`**. |

---

## Likely questions for Person 2 (viva / Q&A)

- **Why are activity and earnings separate queries?** Structured Streaming allows one aggregation per query, and earnings need a trip-deduplicated stream first (report §6.3).
- **What happens to late events?**
  - Beyond the 30-simulated-minute watermark, they're dropped from the windowed aggregates.
  - They still reach the master dataset, so the batch layer counts them.
- **Why is the speed layer approximate?**
  - Distinct counts use HyperLogLog (`approx_count_distinct`, about 2% error), where the batch layer uses exact `countDistinct` (report §6.4).
  - Earnings are recognised when a trip completes.
- **How does idle detection keep state?** `applyInPandasWithState`, keyed by vehicle. An idle episode's start survives across micro-batches. WARNING fires at 45 simulated minutes and CRITICAL at 90.
- **Why is there no logic in the DAG?** An AST test forbids it. Logic that only runs under a scheduler escapes the unit tests and becomes a second, untested copy of the pipeline (report §6.5).
- **What does the profitability join handle?** A `full_outer` join of telemetry and expenses per vehicle covers three cases: matched, expense missing, and telemetry missing (report §6.4).
- **Why can "Active vehicles" exceed 150?** It adds up each zone's count, and a vehicle that crossed zones in the window is counted in each zone.
