# Demo video script — Person 1

## Part 1 of 3 · 0:00 – 3:09 · "The problem, the decision, and the data coming in"

| | |
|---|---|
| **Voice** | Person 1 |
| **Screen** | The recorder records every frame of all three parts |
| **Marks this part earns** | Architecture decision (20) · Technology stack (10) · Data ingestion (15) · Simulated clock stated (a hard requirement of the brief) |

> **How to read this file**
> - **Recorder:** follow **Screen** and **Do**. Hold each shot 1–2 s longer than the voice line.
> - **Person 1 (voice):** read only the **Say** lines. Record one audio file per frame (`P1-F01.wav`, `P1-F02.wav`, …) and keep each close to its target length.
> - **Editor:** put the **Caption** text on screen as a small lower-third if you like. Captions are optional.

---

## The whole video at a glance

| Frame | Time | Voice | What happens | Screen |
|---|---|---|---|---|
| P1-F01 | 0:00–0:12 | P1 | Opening | Report p.1 (top half) |
| P1-F02 | 0:12–0:40 | P1 | One question, two tolerances | Report p.4 |
| P1-F03 | 0:40–0:56 | P1 | Requirements + simulated clock | Report p.5 |
| P1-F04 | 0:56–1:33 | P1 | Why Lambda; Kappa rejected honestly | Report p.7 |
| P1-F05 | 1:33–1:59 | P1 | Architecture walk-through | Report p.8 (Figure 2) |
| P1-F06 | 1:59–2:09 | P1 | Stack, justified for this scenario | Report p.11 (Table 3) |
| P1-F07 | 2:09–2:23 | P1 | **Live:** one command, 22 services, the clock | Terminal |
| P1-F08 | 2:23–2:53 | P1 | **Live:** streaming ingestion | Kafka UI |
| P1-F09 | 2:53–3:09 | P1 | **Live:** the daily file → handover | Terminal |
| P2-F01 | 3:09–3:24 | P2 | The fork | Report p.10 (Figure 4) |
| P2-F02 | 3:24–3:58 | P2 | **Live:** six streaming queries | Spark UI |
| P2-F03 | 3:58–4:14 | P2 | **Live:** one shared codebase | Terminal |
| P2-F04 | 4:14–4:45 | P2 | **Live:** question A answered + idle alerts | Grafana *Operations* |
| P2-F05 | 4:45–4:59 | P2 | **Live:** the master dataset | Terminal |
| P2-F06 | 4:59–5:40 | P2 | **Live:** batch orchestration + structured log | Airflow |
| P2-F07 | 5:40–6:07 | P2 | **Live:** correctness → handover | Terminal + Report p.15 |
| P3-F01 | 6:07–6:27 | P3 | The merge rule | Report p.9 (Figure 3) |
| P3-F02 | 6:27–6:54 | P3 | **Live:** one answer from both layers | FastAPI `/docs` |
| P3-F03 | 6:54–7:19 | P3 | **Live:** question B answered | Grafana *Batch Reconciliation* |
| P3-F04 | 7:19–7:29 | P3 | Why restatements decide it | Report p.6 |
| P3-F05 | 7:29–7:59 | P3 | ★ **Live:** restatement of 2026-03-03 | Terminal |
| P3-F06 | 7:59–8:41 | P3 | Observability → alert fires → inhibition | Report p.13 → Grafana → Prometheus → Alertmanager |
| P3-F07 | 8:41–9:00 | P3 | **Live:** graceful degradation | Terminal |
| P3-F08 | 9:00–9:20 | P3 | Honest limits + close | Report p.18 |

**Total: about 9:20.** The brief allows 5–10 minutes; our target band is 8:30–9:30. Waiting time (an Airflow run, the alert firing) is removed with jump cuts, so the voice never has to fill dead air.

---

## Before recording — EVERYONE (the recorder's master checklist)

### A. About 50 minutes before recording: start a fresh run

```bash
cd ~/57-big-data/ride-hailing-lambda
make clean && make up-obs
```

- `make clean` **deletes the current run's data**. That's intended: the dates restart at 2026-03-01, which the script relies on.
- The images are already built, so this takes 2–3 minutes. `make up-obs` waits for the services and then prints their status.
- Write down the start time and call it **T**. Recording starts at **T + 40 min** or later.
- **Keep the machine awake** (turn off sleep and screen lock). If it sleeps, the simulated clock jumps.
- Why 40 minutes: 1 simulated day = 5 real minutes. By T+40 the batch layer has about 8 days of data and the 7-day trend is full. The restatement Person 3 shows exists from T+20.

### B. Make a clean copy of the report (once)

The committed `docs/report/main.pdf` has two saved yellow highlights (on page 4 and page 5). This copy has none:

```bash
gs -q -o ~/report-clean.pdf -sDEVICE=pdfwrite -dShowAnnots=false -dPreserveAnnots=false docs/report/main.pdf
```

Open `file:///home/sdvn_defense_sibil/report-clean.pdf` in Chrome.

**Page numbers in this script are PDF page numbers**, the ones Chrome shows. The number printed at the bottom of each page is one lower.

**Never show these parts of the report:**
- **p.1 below the title:** the index numbers are still placeholders (`EG/20XX/XXXX`, "Group XX").
- **p.14, Figures 5 and 6:** Grafana screenshots showing "No data".
- **p.16, Figure 8:** shows the speed layer stale.
- **p.19:** placeholders again.

### C. Prepare the terminal

- Font 16–18 pt, dark theme, window maximised. Run `clear` before every take.
- Paste this block once. It creates short commands the script uses:

```bash
cd ~/57-big-data/ride-hailing-lambda
svc()    { docker compose --profile '*' ps -a --format 'table {{.Service}}\t{{.Status}}'; }
lake()   { docker exec fleet-minio sh -c "/opt/bitnami/minio-client/bin/mc alias set l http://localhost:9000 fleetadmin fleetadmin >/dev/null && /opt/bitnami/minio-client/bin/mc $*"; }
today()  { python3 -c "import json,datetime as d; a=json.load(open('state/sim_epoch.json')); w=d.datetime.fromisoformat(a['epoch_wall']); s=d.datetime.fromisoformat(a['epoch_sim']); print((s+(d.datetime.now(d.timezone.utc)-w)*(86400/a['day_seconds'])).date())"; }
status() { curl -s localhost:8000/api/v1/pipeline/status | jq '{as_of_sim, sim_day_index, batch_complete_thru, watermark_age_sim_days, pnl_rows, restated_rows}'; }
util()   { curl -s -D /tmp/h.txt -o /tmp/b.json "localhost:8000/api/v1/fleet/utilization?from=${1:-2026-03-02}&to=$(today)"; grep -iE '^HTTP|x-data-degraded' /tmp/h.txt; jq '{consistency, batch_complete_thru, degraded, missing_stores, rows_by_source: ([.rows[].source] | group_by(.) | map({(.[0]): length}) | add)}' /tmp/b.json; }
pnl()    { curl -s "localhost:8000/api/v1/vehicles/${1:-V113}/profitability?days=30" | jq --arg d "${2:-2026-03-03}" '.rows[] | select(.sim_date==$d) | {sim_date, fuel_cost, net_profit, restatement_count, restated_at}'; }
clear
```

| Command | What it does |
|---|---|
| `svc` | Every service and its status |
| `lake …` | Browse MinIO (the data lake) |
| `today` | Today's *simulated* date. Works even if Redis is down. |
| `status` | Pipeline status: simulated date, batch watermark, row counts |
| `util` | The merged utilization request, summarised |
| `pnl` | One vehicle's profit on one date |

### D. Prepare the browser

Use one Chrome window, maximised, with the bookmarks bar hidden and zoom at 110–125%. Open these tabs **in this order**:

| Tab | What | URL | Notes |
|---|---|---|---|
| 1 | Report | `file:///home/sdvn_defense_sibil/report-clean.pdf` | From step B |
| 2 | Kafka UI | http://localhost:8080 | Click **Topics** |
| 3 | Spark UI | http://localhost:4040/StreamingQuery/ | |
| 4 | Grafana *Operations* | http://localhost:3000/d/fleet-operations?kiosk | `kiosk` hides Grafana's menus; Esc exits |
| 5 | Airflow | http://localhost:8082 | Log in `admin` / `admin` → **fleet_daily_reconciliation** → **Grid** |
| 6 | FastAPI | http://localhost:8000/docs | |
| 7 | Grafana *Batch Reconciliation* | http://localhost:3000/d/fleet-batch-reconciliation?kiosk | |
| 8 | Grafana *Pipeline Health* | http://localhost:3000/d/fleet-pipeline-health?kiosk | |
| 9 | Prometheus alerts | http://localhost:9090/alerts | |
| 10 | Alertmanager | http://localhost:9093 | |

### E. Readiness check (at T + 40 min)

```bash
status
```
- `sim_day_index` should be **8 or more**.
- `batch_complete_thru` should be **2026-03-07 or later**.
- `restated_rows` should be **0**.

```bash
pnl V113 2026-03-03
```
- `restatement_count` must be **0**. If it's 1, the restatement has already been used up; see Person 3's fallback.

```bash
lake cat l/fleet-lake/landing/expenses/expenses_2026-03-03.csv | grep ^V113
```
- The `fuel_cost` here should be **lower** than the one `pnl` printed. That means the corrected file has arrived and Person 3's restatement will show a visible change.
- In our rehearsal it was 56.13 in the mart against 30.13 in the corrected file.

Finally, check the Airflow **Grid**: every column should be green.

### F. Recording rules (read before pressing record)

1. **One clip per frame,** named `P1-F01.mp4` … `P3-F08.mp4`.
   - Move the mouse slowly and use the pointer to point.
   - Read the voice lines quietly while you record, to pace yourself. That guide audio gets replaced later.
2. **Order:**
   - P1 and P2 frames first. These are repeatable, so retake them freely.
   - Then P3-F01 to P3-F04.
   - Then **P3-F05, the restatement, which is ONE-SHOT.** Never run `make dag-trigger` before this take.
   - Then **P3-F07 (stopping Redis)**, just after an Airflow run has finished.
   - Then the **cleanup** in `person-3.md`, not recorded.
   - Then **P3-F06 (stopping the producer) LAST.** It needs about 5 minutes including the wait, and it makes the next Airflow run fail, which is harmless at the end.
   - P3-F08 is a report page and can be recorded any time.
   - The final video still shows F06 before F07; only the recording order differs.
3. **Live data must be flowing** for P2-F02, P2-F04, P3-F02 and P3-F03. Record them before any chaos frame, or at least 2 minutes after `make chaos-heal`.
4. **Before P3-F02,** run `status` and check that `watermark_age_sim_days` is **1**. If it's 2, wait until the next Airflow run finishes (at most 5 minutes).

### G. Voice-recording guide (all three voices)

- **Pace:** calm, about **130 words per minute**. The word counts in this script are sized for that pace.
- **Files:** one audio file per frame (`P1-F03.wav`). Leave 1 s of silence at the start and end.
- **Sound:**
  - Record every take in the same room with the same microphone, about 20 cm from your mouth. A phone is fine.
  - Avoid echoey rooms.
  - Use WAV or high-quality MP3.
- **Pronunciation:**

  | Word | Say it as |
  |---|---|
  | Kafka | *KAHF-kuh* |
  | Avro | *AV-roh* |
  | Parquet | *par-KAY* |
  | MinIO | *min-EYE-oh* |
  | Redis | *RED-iss* |
  | PostgreSQL | *POST-gres* |
  | Grafana | *gruh-FAH-nuh* |
  | FastAPI | *fast A-P-I* |
  | idempotent | *eye-dem-POH-tent* |
  | Kappa | *KAP-uh* |
  | V113 | *vee one-one-three* |
  | 288× | *two hundred and eighty-eight times* |

---

## Person 1 — frame by frame

### P1-F01 · 0:00–0:12 (12 s) · Opening
- **Screen:** Report **p.1**, **top half only**: the university logo, "Mini Project Report · Applied Big Data Engineering · EC8203", and the title *"Ride-Hailing Fleet Operations: a Lambda Architecture Data Platform"*.
- **Do:** Tab 1, page 1. Zoom until the logo and title fill the screen. Stop **above** the names, because the index numbers are placeholders. Hold still for 13 s.
- **Say:** "This is our EC8203 mini project: Ride-Hailing Fleet Operations, a Lambda-architecture data platform. First, why we built it this way — then the system running, end to end."
- **Caption:** *EC8203 Applied Big Data Engineering · Mini Project · Use Case 1*

### P1-F02 · 0:12–0:40 (28 s) · One question, two tolerances
- **Screen:** Report **p.4** (§1): the grey box **"Business question (Use Case 1)"**, then the paragraphs **"Question A — right now"** and **"Question B — becoming unprofitable, given yesterday's costs"**.
- **Do:**
  - 0–10 s: zoom about 150% on the grey box.
  - 10–28 s: scroll slowly down to Question A and Question B. Point at **"right now"** when A is said, and at **"becoming"** when B is said.
- **Say:** "The use case asks one question: what is fleet utilization and earnings by area right now — and which vehicles are becoming unprofitable once yesterday's costs are factored in? That's really two questions. Question A is for dispatch: answer in seconds, approximation is fine. Question B is for finance: it needs yesterday's partner costs, it must be exact, and 'becoming' means it needs history."
- **Caption:** *Question A: "right now" · Question B: "becoming unprofitable"*

### P1-F03 · 0:40–0:56 (16 s) · Requirements and the simulated clock
- **Screen:** Report **p.5**: **Table 1** (FR-1 … NFR-5), then the box **"Simulated clock — stated explicitly, as the brief requires"**.
- **Do:** Show Table 1 for about 5 s, then scroll to the clock box and hold.
- **Say:** "We turned that into measurable requirements. And, as the brief asks, our simulated clock: one simulated day lasts five real minutes — two hundred and eighty-eight times faster — starting on the first of March, 2026."
- **Caption:** *1 simulated day = 5 real minutes (288×) · starts 2026-03-01*

### P1-F04 · 0:56–1:33 (37 s) · Why Lambda, and Kappa rejected honestly
- **Screen:** Report **p.7**: **Table 2** ("Six of eight criteria favour Lambda"), then **§3.9 "The rejected alternative: Kappa"**.
- **Do:**
  - 0–22 s: zoom to Table 2. Point at row **4 (Historical data analysis — Decisive)** and row **7 (Data reprocessing — Decisive)** as they are named.
  - 22–37 s: scroll to §3.9, past "The honest case for Kappa" to "Why it lost".
- **Say:** "We decided using the module's own eight criteria. Six favour Lambda, including the two decisive ones. Historical analysis: 'becoming unprofitable' is a seven-day trend, not a one-day lookup. And reprocessing: partners restate invoices — under Lambda that means re-reading one day of immutable data; under Kappa, replaying a full day of telemetry. We made the honest case for Kappa before rejecting it, and the two criteria it wins — complexity and cost — we concede, and mitigate in code."
- **Caption:** *6 of 8 criteria favour Lambda — historical analysis & reprocessing are decisive*

### P1-F05 · 1:33–1:59 (26 s) · The architecture
- **Screen:** Report **p.8**: **Figure 2**, the layered architecture.
- **Do:** Zoom until the diagram fills the width. Trace the path with the pointer as each part is named:
  1. telemetry simulator → **Kafka**;
  2. the **blue** box (Spark Structured Streaming → Redis);
  3. the grey **MinIO / S3 + Parquet** box;
  4. the **green** box (Spark batch jobs → PostgreSQL);
  5. the dashed **"daily CSV"** line from the expense dropper;
  6. the **orange** FastAPI box.
- **Say:** "Here is the design. Telemetry flows through Kafka into two paths. In blue, the speed layer: Spark Structured Streaming into Redis — fast and approximate. In green, the batch layer: an immutable Parquet master dataset on MinIO, recomputed by Spark into PostgreSQL — exact. The daily expense file skips Kafka. Both paths meet in one place: the FastAPI serving layer."
- **Caption:** *Blue = speed layer · Green = batch layer · Orange = serving layer*

### P1-F06 · 1:59–2:09 (10 s) · The stack, justified
- **Screen:** Report **p.11**: **Table 3**, "Technology stack with per-component justification".
- **Do:** Zoom to the table. Point at the row **Speed processing → Spark Structured Streaming → "Same engine as the batch layer"**. Hold.
- **Say:** "Every component is justified against this scenario — for example Spark, because the same engine runs both layers from one shared library."
- **Caption:** *Each technology justified for this use case — Table 3 of the report*

### P1-F07 · 2:09–2:23 (14 s) · **Live:** the running system
- **Screen:** Terminal.
- **Do:** `clear`, then type `svc`, press Enter and hold 6 s. Then type `status`, press Enter and hold 6 s.
- **You will see:**
  - `svc`: 22 rows. 19 show **Up** (most with **(healthy)**). `init`, `registry-producer` and `airflow-init` show **Exited (0)**; they are one-time setup containers, which is correct.
  - `status`: the simulated date (`as_of_sim`), `sim_day_index`, and `batch_complete_thru` (the last day the batch layer has finished).
- **Say:** "Now the live system. One command started all twenty-two services; the three setup containers finished cleanly. The status endpoint shows the simulated date, and how far the batch layer has got."
- **Caption:** *Live · started with one command: `make up-obs`*

### P1-F08 · 2:23–2:53 (30 s) · **Live:** streaming ingestion in Kafka
- **Screen:** Kafka UI (tab 2).
- **Do:**
  1. **Topics** page for 3 s. It shows 4 topics: `fleet.alerts.v1`, `fleet.telemetry.dlq`, `fleet.telemetry.v1`, `fleet.vehicle.registry`.
  2. Click **`fleet.telemetry.v1`** → **Overview**. It shows **6 partitions** with similar message counts. Hold 5 s.
  3. Open the **Messages** tab and click the newest message to expand it. The Avro message is decoded: key `V0..`; value with `vehicle_id`, `trip_id`, `lat`, `lon`, `speed_kmh`, `status`, `fare`, `event_time`, `ingest_time`. Hold 7 s.
  4. Go back to **Topics** → **`fleet.telemetry.dlq`** → **Messages**, and expand one. Point at **`rejection_reason`** (for example `IMPLAUSIBLE_SPEED`). Hold 5 s.
- **Say:** "The streaming source simulates a hundred and fifty vehicles at about two hundred and forty events per second, in Avro, registered in the Schema Registry. Six partitions, keyed by vehicle ID — not for balance, but for ordering: idle detection is a per-vehicle state machine. About one percent of events are corrupted on purpose. They land in the dead-letter topic, with the reason attached."
- **Caption:** *Kafka · 6 partitions · key = vehicle_id · Avro + Schema Registry · ~1% dead-lettered*

### P1-F09 · 2:53–3:09 (16 s) · **Live:** the daily-batch source → handover
- **Screen:** Terminal.
- **Do:** `clear`, then:
  ```bash
  lake ls l/fleet-lake/landing/expenses/
  ```
  Hold 3 s: one `expenses_2026-03-0X.csv` per simulated day. Then:
  ```bash
  lake head -n 4 l/fleet-lake/landing/expenses/expenses_2026-03-05.csv
  ```
  Hold 6 s: the header `vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag, submitted_at, partner_id`, then rows such as `V001,43.75,8.83,182.28,false,…,GARAGE_D`.
- **Say:** "The daily-batch source is the partners' expense file: one CSV per simulated day, written straight to object storage — not through Kafka, because a daily file isn't an event stream. Person 2 continues with the processing."
- **Caption:** *Daily-batch source → MinIO landing zone (not Kafka)*

---

## If something goes wrong (Person 1's frames)

| Problem | What to do |
|---|---|
| `svc` shows a container **Restarting** or **unhealthy** | Don't record. Run `make logs SVC=<name>` and fix it first. |
| Kafka UI messages look like garbled bytes | In the Messages tab, set **Value Serde** to **SchemaRegistry**. |
| `expenses_2026-03-05.csv` is missing | The run is younger than ~T+30. Wait. |
| The report shows yellow highlights | You opened `main.pdf`. Open `report-clean.pdf` (step B). |
| `make up-obs` ends with an error mentioning `init` | You skipped `make clean`. Run `make clean && make up-obs`. |

---

## Likely questions for Person 1 (viva / Q&A)

- **Why not Kappa?** Report §3.9 gives four reasons, most important first:
  1. **History:** "becoming" needs a multi-day trend. Holding weeks in stream state or in Kafka re-creates batch processing in a worse form.
  2. **Reprocessing cost:** a correction should cost one partition, not a full day's replay.
  3. **Auditability:** an immutable partition plus a deterministic job is a better audit story than "we replayed the log".
  4. **Modelling:** a daily CSV is not an event stream.
- **When would you switch to Kappa?** Report §3.9, "What would change our minds": all three would have to hold. Costs become real-time, the history window shrinks to hours, and auditability stops mattering.
- **Why key by `vehicle_id`?** Kafka orders only within a partition. Idle detection must see each vehicle's `on_trip → idle → idle` in order.
- **Why isn't the expense file in Kafka?** A once-a-day reference file isn't an event stream. An object-store landing zone plus validation is simpler and loses nothing.
- **Why compress time 288×?**
  - The brief allows it, so we can demo a week in 35 minutes.
  - Watermarks are in simulated time: 30 simulated minutes is only 6.25 real seconds.
  - `config.py` refuses to start below 5 seconds.
- **Why Avro and the Schema Registry?** A schema contract that can evolve (BACKWARD compatibility), and messages 40–60% smaller than JSON (report Table 3).
