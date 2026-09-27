# Demo video script — Person 3

## Part 3 of 3 · 6:07 – 9:20 · "Serving the answer, the restatement, and observability"

| | |
|---|---|
| **Voice** | Person 3 |
| **Screen** | The recorder records every frame of all three parts |
| **Marks this part earns** | Storage & serving layer (10): the merge and graceful degradation · Architecture decision (20): the restatement *demonstrated* · Observability (10): health dashboard, an alert firing, inhibition · the consolidated dashboard answering the business question · Report (15): honest limitations |

> **How to read this file**
> - **Recorder:** follow **Screen** and **Do**. Hold each shot 1–2 s longer than the voice line. Some frames are recorded as **two clips with a jump cut**; the waiting is cut out.
> - **Person 3 (voice):** read only the **Say** lines. Record one audio file per frame (`P3-F01.wav`, …). Pronunciations and recording tips are in `person-1.md`, section G.
> - **Setup:** the stack, the terminal helpers (`svc`, `lake`, `today`, `status`, `util`, `pnl`) and the browser tabs are prepared once, in `person-1.md`, sections A–E.

---

## The whole video at a glance

| Frame | Time | Voice | What happens | Screen |
|---|---|---|---|---|
| P1-F01 → P1-F09 | 0:00–3:09 | P1 | The problem, the decision, the data coming in | Report p.1–11 → terminal → Kafka UI |
| P2-F01 → P2-F07 | 3:09–6:07 | P2 | Processing: speed layer and batch layer | Report p.10 → Spark UI → Grafana → Airflow → terminal |
| **P3-F01** | **6:07–6:27** | **P3** | **The merge rule** | **Report p.9 (Figure 3)** |
| **P3-F02** | **6:27–6:54** | **P3** | **Live: one answer from both layers** | **FastAPI `/docs`** |
| **P3-F03** | **6:54–7:19** | **P3** | **Live: question B answered** | **Grafana *Batch Reconciliation*** |
| **P3-F04** | **7:19–7:29** | **P3** | **Why restatements decide it** | **Report p.6** |
| **P3-F05** | **7:29–7:59** | **P3** | **★ Live: restatement of 2026-03-03** | **Terminal (+ Airflow)** |
| **P3-F06** | **7:59–8:41** | **P3** | **Observability → alert fires → inhibition → heal** | **Report p.13 → Grafana → Prometheus → Alertmanager** |
| **P3-F07** | **8:41–9:00** | **P3** | **Live: graceful degradation** | **Terminal** |
| **P3-F08** | **9:00–9:20** | **P3** | **Honest limits + close** | **Report p.18** |

---

## Recording order for Person 3's frames (important)

The video *shows* F01 → F08 in order, but they must be **recorded** in this order. F05, F06 and F07 change the live system.

| Step | Record | Why this position |
|---|---|---|
| 1 | **P3-F01, P3-F04, P3-F08** | Report pages. Any time, as often as you like. |
| 2 | **P3-F02, P3-F03** | Need live data and a current batch layer (checks below). Repeatable. |
| 3 | **P3-F05, the restatement** | **ONE-SHOT.** It can only be recorded once per run. |
| 4 | **P3-F07, the Redis stop** | Must come *before* F06. Start it just after an Airflow run has finished. |
| 5 | Cleanup, not recorded | Clears a leftover alert caused by step 4 (commands below). |
| 6 | **P3-F06, the producer stop** | **Last live frame.** It makes the next Airflow run fail, which is harmless once nothing else is left to record. |

---

## Person 3 — frame by frame

### P3-F01 · 6:07–6:27 (20 s) · The merge rule
- **Screen:** Report **p.9**, §4.1 *The merge boundary*: **Figure 3**, panel **(a) "Healthy — the batch layer is one simulated day behind"**, then the box **"The merge rule"**.
- **Do:**
  - 0–12 s: zoom on panel (a). Point at **hwm = d−1**, the green **BATCH view — exact** bar, and the blue **SPEED** bar.
  - 12–20 s: scroll to **"The merge rule"** box ("Batch serves [from, hwm] … Speed serves (hwm, now] …").
- **Say:** "Both layers meet in exactly one place. The batch layer records a high-water mark — the last day it has fully processed. Batch answers up to and including it; speed answers only after it. So every date has exactly one owner."
- **Caption:** *Batch serves [from, hwm] · Speed serves (hwm, now] · one owner per date*

### P3-F02 · 6:27–6:54 (27 s) · **Live:** one answer from both layers
- **Before this take:** in the terminal, run `status`. **`watermark_age_sim_days` must be 1.**
  - If it's 2, the next Airflow run hasn't finished yet. Wait (at most 5 minutes) and check again.
  - Run `today` and note the date it prints, e.g. `2026-03-10`.
- **Screen:** FastAPI (tab 6): **GET `/api/v1/fleet/utilization`** (section **merged**).
- **Do:**
  1. Expand **GET /api/v1/fleet/utilization**, click **Try it out**, and fill in **from** = `2026-03-02` and **to** = the date from `today`. Click **Execute**.
  2. Scroll to the response body. Near the top, point at **`"consistency": "batch-complete-through 2026-03-0X"`** and **`"degraded": false`**.
  3. Scroll through the rows to show **`"source": "batch", "exact": true`**.
  4. Scroll to the **bottom**: the last 12 rows are **`"source": "speed", "exact": false`**, each with an **`approximation_note`** (HyperLogLog). Point at them.
- **Say:** "Here is that rule, live. We ask for utilization from the second of March until today. Every completed day comes from the batch view, marked exact. Today comes from the speed view, marked approximate. And the consistency field says, in plain English, how far the exact data extends."
- **Caption:** *One request · batch rows (exact) + speed rows (approximate)*

### P3-F03 · 6:54–7:19 (25 s) · **Live:** question B answered
- **Screen:** Grafana **"Fleet · Batch Reconciliation (daily)"** (tab 7, kiosk):
  - the table **"Worst 10 by 7-day rolling average profit"**;
  - below it, the stats **Batch watermark age · Rows in the batch view · Restated rows · Merge boundary crossings**.
- **Do:**
  - 0–15 s: point at the **first row (V113)**, then its **classification (WATCH)**, its 7-day average and its trend slope.
  - 15–25 s: glance over the stats row (watermark age **1**, restated rows **0**).
- **Say:** "And this answers question B: the ten vehicles with the worst seven-day rolling profit. V113 is at the top — it works the outskirts, where nearly half its kilometres are unpaid. It's still profitable, but on the watch list; ranking by a seven-day average means one bad day is treated as noise."
- **Caption:** *Question B answered · ranked by 7-day rolling average profit*
- **Check first:**

  ```bash
  curl -s "localhost:8000/api/v1/vehicles/unprofitable?limit=3" | jq -r '.[] | "\(.vehicle_id) \(.classification) \(.rolling_7d_avg_profit)"'
  ```

  In our rehearsal V113 was first, and it was #1 on 82 of 111 simulated days. If it isn't first in your run, the voice says "V113, our scripted outskirts vehicle, is near the top" instead.

### P3-F04 · 7:19–7:29 (10 s) · Why restatements decide the architecture
- **Screen:** Report **p.6**, §3.4 **"Data reprocessing — decisive"**: the two bullets **Under Lambda** and **Under Kappa**.
- **Do:** Zoom on the two bullets. Point at "that one Parquet partition" and then at "replaying every telemetry event for that day".
- **Say:** "This is why restatements decided our architecture: under Lambda, a correction costs one partition; under Kappa, a full day's replay."
- **Caption:** *Reprocessing — the decisive criterion (report §3.4)*

### P3-F05 · 7:29–7:59 (30 s) · ★ **Live:** the restatement (ONE-SHOT)
- **What is happening:**
  - The expense simulator sends 2026-03-03's file at the end of that day, and Airflow processes it.
  - About 5 minutes later, together with the next day's file, the partner **resubmits 2026-03-03, corrected**: "fuel card mis-assignment", with fuel costs 20–45% lower.
  - Nothing re-runs automatically. We trigger the restatement by hand, exactly as an operator would.
- **Before this take (from `person-1.md`, E):** `pnl V113 2026-03-03` must show `restatement_count: 0`.
- **Screen:** Terminal. Optionally, a 3-second clip of the Airflow Grid.
- **Do.** Clip A, about 10 s:
  ```bash
  clear
  pnl V113 2026-03-03
  ```
  Hold 5 s. **You will see** the "before" values. In our rehearsal:
  ```json
  { "sim_date": "2026-03-03", "fuel_cost": 56.13, "net_profit": 136.35, "restatement_count": 0, "restated_at": null }
  ```
  ```bash
  make dag-trigger DATE=2026-03-03 | tail -1
  ```
  Hold 3 s: `triggered as a RESTATEMENT - the watermark must not move to 2026-03-03`.

  **Stop recording and wait** until the Airflow Grid shows the new manual run finished and green. In the rehearsal that took **about 25 seconds**. It can take 1–2 minutes if a scheduled run was busy.

  Optional clip: 3 s of the Airflow Grid showing the manual run.

  Clip B, about 15 s:
  ```bash
  pnl V113 2026-03-03
  ```
  Hold 6 s. **You will see** the "after" values. In our rehearsal:
  ```json
  { "sim_date": "2026-03-03", "fuel_cost": 30.13, "net_profit": 161.39, "restatement_count": 1, "restated_at": "2026-…Z" }
  ```
  ```bash
  status
  ```
  Hold 4 s. `batch_complete_thru` is still the latest day: the watermark did **not** move back to 2026-03-03.
- **Say:** "A day after sending it, the partner resubmitted the expenses for the third of March — a fuel card had been mis-assigned. Here is V113 for that day, before. We re-run the batch layer for that single date: a restatement. After: fuel cost is down, profit is up, and the row records that it was restated, and when. The watermark did not move backwards."
- **Caption:** *Restatement · one partition recomputed · idempotent upsert · watermark unchanged*

### P3-F06 · 7:59–8:41 (42 s) · Observability → an alert fires → inhibition → heal
**Record this LAST among the live frames** (step 6 in the order table).

- **Before this take:** make sure no alert is already firing:
  ```bash
  curl -s localhost:9090/api/v1/alerts | jq -r '.data.alerts[].labels.alertname'
  ```
  This should print **nothing**. See the cleanup after P3-F07.
- **Clip A, about 14 s:**
  1. Report **p.13**, **Table 4** "What is measured, how, and why" (4 s).
  2. Tab 8, Grafana **"Fleet · Pipeline Health"**. Point along the top row (8 s):
     - **INGEST · events/sec** (≈240);
     - **PROCESS · seconds since last Spark progress** (small);
     - **STORE · sink write errors/sec**;
     - **SERVE · batch watermark age**.
  3. Terminal (2 s):
     ```bash
     make chaos-kill-producer
     ```
     You will see `producer stopped at hh:mm:ss`.
- **Stop recording and wait about 3½ minutes.** Measured in the rehearsal:
  - `ProducerTargetDown` fires after about 1 minute.
  - `NoTelemetryIngested` and the six `StreamingQueryStalled` alerts fire after about 3 to 3¼ minutes. The "no data" rule looks back over 2 minutes, so it can't react sooner.
- **Clip B, about 28 s:**
  1. Tab 8, Pipeline Health: **INGEST · events/sec** now shows **0**, and the *Events produced /sec* line drops to zero (4 s).
  2. Tab 9, Prometheus **Alerts** (8 s). **Firing:**
     - `ProducerTargetDown` (telemetry-producer);
     - `NoTelemetryIngested`;
     - `StreamingQueryStalled` ×6 (q_master, q_dlq, q_activity, q_earnings, q_idle, q_vehicles).

     That's **8 firing**. If the page lists inactive rules too, untick **Inactive**.
  3. Tab 10, **Alertmanager**: only **one** alert is shown, **ProducerTargetDown** (severity critical) (5 s).
  4. Tick **Inhibited**: `NoTelemetryIngested` and the six `StreamingQueryStalled` now appear, marked **inhibited** (6 s).
  5. Terminal (3 s):
     ```bash
     make chaos-heal
     ```
     You will see `restored at …`. In the rehearsal the speed view was back in about 6 s and all alerts cleared in about 12 s.
- **Say:** "Observability was designed as a table of what, how, and why. This dashboard gives one health signal per stage: ingest, process, store and serve. Now we stop the telemetry producer, on purpose. *[cut]* Minutes later, ingestion is at zero, and Prometheus is firing eight critical alerts: the producer is down, no telemetry is arriving, and all six queries have stalled. But Alertmanager shows only one — the root cause. The other seven are inhibited, so whoever is on call sees one actionable alert, not a flood. Then we heal it."
- **Voice note:** read the sentence ending "…on purpose." as a separate audio file (`P3-F06a.wav`). The rest is `P3-F06b.wav`, so the editor can place it after the cut.
- **Captions:** clip B starts with *"≈ 3 minutes later"*. At the Alertmanager shot: *"8 alerts firing → 1 actionable (inhibition)"*.

### P3-F07 · 8:41–9:00 (19 s) · **Live:** graceful degradation
**Record this BEFORE P3-F06** (step 4 in the order table).

- **Before this take:** start it **just after an Airflow run has finished**. The newest Grid column is green, which means 1–3 minutes after a :x0 or :x5 wall-clock minute. Also run `status` and check `watermark_age_sim_days` is **1**. That way the response has no "dates in neither view" text and no Airflow run fails.
- **Screen:** Terminal.
- **Do:** keep Redis stopped for **less than 30 seconds**.
  ```bash
  clear
  docker stop fleet-redis
  util
  ```
  Hold 8 s on the output. **You will see** (from the rehearsal):
  ```
  HTTP/1.1 200 OK
  x-data-degraded: true
  {
    "consistency": "batch-complete-through 2026-03-0X; DEGRADED - unreachable: redis",
    "degraded": true,
    "missing_stores": [ "redis" ],
    "rows_by_source": { "batch": … }
  }
  ```
  Point at **200**, **x-data-degraded: true**, **missing_stores** and **"batch"**. Then heal immediately:
  ```bash
  make chaos-heal
  ```
- **Say:** "Finally, graceful degradation. We stop Redis, the speed view. The same request still returns 200: every exact batch row is there, and the response is flagged as degraded and names the missing store. Only losing both stores is an error."
- **Caption:** *Redis down → HTTP 200 · X-Data-Degraded: true · batch rows still served*
- **Why you must keep it short:** while Redis is down, the speed layer crashes and restarts about every 10 seconds; it recovers about 15 s after `make chaos-heal`. If an Airflow run starts during the outage, that run fails.

**Cleanup after P3-F07, before recording P3-F06 (not recorded).** Each crash leaves a stale "unnamed" entry in the Pushgateway, which would show up as a false `StreamingQueryStalled` alert in P3-F06.
1. Wait until `svc` shows `streaming-job` as **Up** for at least a minute.
2. Run:
   ```bash
   curl -s -X DELETE localhost:9091/metrics/job/fleet_speed_layer/query/unnamed
   ```
3. After about 15 seconds, this must print nothing:
   ```bash
   curl -s localhost:9090/api/v1/alerts | jq -r '.data.alerts[].labels.alertname'
   ```

### P3-F08 · 9:00–9:20 (20 s) · Honest limits, and close
- **Screen:** Report **p.18**, **§9.5 "What is not implemented"**, then scroll to **§10 Conclusion**.
- **Do:**
  - 0–10 s: zoom on §9.5 and point at "Distributed tracing was not implemented" and "The generated PDF daily report was not implemented".
  - 10–20 s: scroll to §10 and hold. Optionally fade to a closing card with the repository URL.
- **Say:** "We state our limits openly: distributed tracing and a rendered PDF report were not built, and delivery is at-least-once, made safe by idempotent writes. Two sources, two layers, one shared library, one explicit boundary — and a system that shows its own health. Thank you."
- **Caption / end card:** *Thank you · github.com/jathurT/ride-hailing-lambda*

---

## If something goes wrong (Person 3's frames)

| Problem | What to do |
|---|---|
| P3-F02 `consistency` says "…1 date(s) in neither view…" | The batch layer hasn't finished yesterday yet. Wait for the next Airflow run (at most 5 minutes) and retake. |
| P3-F02 shows no `"speed"` rows | Live data isn't flowing: the producer or Redis is down, or it's just after a chaos frame. Run `make chaos-heal`, wait 1–2 minutes and retake. |
| P3-F05 `restatement_count` is already 1 before the take | The restatement was used up. Use the after-values on screen and cut the "before" sentence. The voice then starts at "We re-run the batch layer…". |
| P3-F05 numbers don't change after the trigger | The batch had already used the corrected file. Narrate the `restated_at` and `restatement_count` change instead: "the row records that it was re-processed, and when". |
| P3-F06: an alert is firing *before* you stop the producer | Do the P3-F07 cleanup first. If `BatchWatermarkStale` is pending, an Airflow run failed; wait for the next green run. |
| P3-F06: Alertmanager shows several alerts, not one | You're viewing before the inhibition applies. Wait 30 s and refresh. |
| P3-F07 shows HTTP 503 | Postgres is down too. Run `make chaos-heal`, wait, and retake. |
| After everything: red Airflow runs or missing batch days | Expected after P3-F06. If you need the batch layer complete again, e.g. to re-capture the report figures, run `make backfill DAYS=3`. |

---

## Likely questions for Person 3 (viva / Q&A)

- **Why a high-water mark and not "whatever is newest"?** Half-open intervals give every date exactly one owner, so nothing is double-counted or silently dropped. The watermark moves in the same database transaction as the facts, so it can never promise a day that hasn't been committed (report §4.1, ADR-005).
- **What if the batch layer falls behind?** Redis only holds the current window, so the missing dates are in neither view. The API lists them in `uncovered_dates` and names the gap in `consistency`, rather than returning fewer rows silently (report Figure 3b).
- **Is the restatement idempotent?**
  - Yes: an upsert on `(vehicle_id, sim_date)`.
  - Re-running with the same job id changes nothing; a new job id records the restatement (report Table 8).
  - A restatement run never moves the watermark backwards.
- **Why stop the producer to demonstrate alerting?** The brief asks for "no data received in N minutes". Report §7.2 explains why the first version of that rule could never fire: *absent is not zero*. The fix is `or vector(0)` plus an `up == 0` rule.
- **Why inhibition?** One cause produced eight alerts. Inhibition shows on-call the root cause (`ProducerTargetDown`) and suppresses the seven consequences (report §7.3).
- **Why not consumer lag for the Spark queries?** They checkpoint their own offsets and never commit to a consumer group, so lag reads zero whether a query is healthy or dead. We alert on the age of the last progress timestamp instead (report §7.1).
- **Does the speed-vs-batch reconciliation match its prediction?** No, and the report says so (§9.4). A once-a-day snapshot catches a partly-filled sliding window, so flow metrics under-read. `idle_ratio`, a ratio, is the fair comparison.
- **What happens to the speed layer when Redis is down?** The API degrades correctly: HTTP 200, batch rows, flagged as degraded. The streaming job itself restarts every few seconds until Redis returns. Each restart still advances the Parquet master dataset, and the job recovers about 15 s after Redis is back.
