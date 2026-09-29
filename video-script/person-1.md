# Demo video — Person 1: The problem, the decision, and the data coming in

**Part 1 of 3 · 0:00–3:14 · clips C01–C07** · EC8202 Big Data Analytics · Ride-Hailing Fleet Operations

This file is everything Person 1 needs: where the part fits, the words to say, what is shown on screen at each moment, and the text-to-speech version of the same words.
The recorder's setup (fresh run, terminal helpers, browser tabs, recording order) is in `complete-script.md`, Part D.

## Where your part fits

| Clip | Time | Voice | What happens | Screen |
|---|---|---|---|---|
| **C01** | **0:00–0:17** | **Person 1** | **Opening** | **Report p.1 (cover)** |
| **C02** | **0:17–0:45** | **Person 1** | **One question, two opposite needs** | **Report p.3 (§1 The problem)** |
| **C03** | **0:45–1:16** | **Person 1** | **Lambda or Kappa** | **Report p.4 (Table 2), then p.5 (§3.3)** |
| **C04** | **1:16–1:48** | **Person 1** | **The architecture** | **Report p.6 (Figure 2)** |
| **C05** | **1:48–2:08** | **Person 1** | **Live: the running system** | **Terminal** |
| **C06** | **2:08–2:42** | **Person 1** | **Live: streaming ingestion in Kafka** | **Kafka UI (tab 2)** |
| **C07** | **2:42–3:14** | **Person 1** | **Live: the daily file, and a structured log** | **Terminal** |
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

## Your words, in one piece (script only)

`C01` Welcome to our mini project for EC8202, Big Data Analytics: Ride-Hailing Fleet Operations, built as a Lambda architecture. First we explain why, and then we show the whole pipeline running live.

`C02` Our use case asks one question: what is fleet utilization and earnings by area right now, and which vehicles are becoming unprofitable once yesterday's costs are counted? This is really two questions. Question A comes from dispatch: it needs an answer in seconds, and a small error is fine. Question B comes from finance: it must be exact, and the word becoming means it needs history.

`C03` So we compared Lambda and Kappa on the module's eight criteria. Six favour Lambda, and two decide it. Historical analysis: becoming unprofitable is a seven-day trend. And reprocessing: when a partner corrects a cost file, Lambda recomputes one stored day, while Kappa would replay a whole day of telemetry. Kappa wins on complexity and cost, and we accept both.

`C04` Here is the architecture, from top to bottom. Telemetry goes into Apache Kafka. In blue, the speed layer: Spark Structured Streaming keeps a live view in Redis, and stores every raw event in the MinIO lake. In green, the batch layer: Airflow runs Spark once per simulated day, and the exact results go into PostgreSQL. At the bottom, FastAPI merges both views for Grafana.

`C05` Now, the live system. One command started every service you see here, and the three setup containers ran once and finished. The clock is simulated: one day lasts five real minutes, two hundred and eighty-eight times faster than real time.

`C06` Data comes in from two sources. The streaming source simulates a hundred and fifty vehicles, sending about two hundred and forty events per second. The telemetry topic has six partitions, and each message is keyed by vehicle ID, so every vehicle's events stay in order for the idle detector. Messages are Avro, checked by the Schema Registry. About one percent are broken on purpose, and they land in the dead-letter topic with the reason.

`C07` The second source is the daily-batch file. After each simulated day, the partners' expense file lands in the lake as one CSV, with fuel, maintenance and distance per vehicle. It skips Kafka, because a daily file is not an event stream. Every service also writes structured logs, like this producer line, tagged with its stage. Next, let us see how this data is processed.

## Your part, clip by clip (show + script + text-to-speech)

#### C01 · 0:00–0:17 · Person 1 · Opening

**Screen:** Report p.1 (cover)

**Show / do:**
1. Tab 1 (report), page 1, zoom **Fit to page**: university logo, *Mini Project Report · EC8202 Big Data Analytics*, title *Ride-Hailing Fleet Operations*.
2. Hold still. At “Ride-Hailing Fleet Operations”, zoom slowly towards the title.

**Say:**

> Welcome to our mini project for EC8202, Big Data Analytics: Ride-Hailing Fleet Operations, built as a Lambda architecture. First we explain why, and then we show the whole pipeline running live.

**Text-to-speech** (`C01.mp3`, 38 words, about 16 s):

```text
Welcome to our mini project for E C eight two zero two, Big Data Analytics. The project is Ride-Hailing Fleet Operations, built as a Lambda architecture. First, we explain why. Then, we show the whole pipeline running live.
```

#### C02 · 0:17–0:45 · Person 1 · One question, two opposite needs

**Screen:** Report p.3 (§1 The problem)

**Show / do:**
1. Go to page 3. Zoom about 150% on the grey box **Business question (use case 1)** while the question is read.
2. At “Question A”: scroll to the paragraph *Question A, “right now”* and point at **right now**.
3. At “Question B”: point at *Question B, “becoming unprofitable”* and at the word **becoming**.

**Say:**

> Our use case asks one question: what is fleet utilization and earnings by area right now, and which vehicles are becoming unprofitable once yesterday's costs are counted? This is really two questions. Question A comes from dispatch: it needs an answer in seconds, and a small error is fine. Question B comes from finance: it must be exact, and the word becoming means it needs history.

**Text-to-speech** (`C02.mp3`, 66 words, about 27 s):

```text
Our use case asks one question. What is fleet utilization and earnings by area right now, and which vehicles are becoming unprofitable, once yesterday's costs are counted? This is really two questions. Question A comes from dispatch. It needs an answer in seconds, and a small error is fine. Question B comes from finance. It must be exact, and the word becoming means it needs history.
```

#### C03 · 0:45–1:16 · Person 1 · Lambda or Kappa

**Screen:** Report p.4 (Table 2), then p.5 (§3.3)

**Show / do:**
1. Go to page 4 and zoom to **Table 2 — The eight criteria from the module**.
2. At “historical analysis” point at the bold row **Historical analysis — Decisive**; at “reprocessing” point at **Reprocessing — Decisive**.
3. At “complexity and cost” point at the two rows that favour **Kappa**.
4. Optional, last 3 s: scroll to page 5, heading **3.3 The case for Kappa, and why it lost**.

**Say:**

> So we compared Lambda and Kappa on the module's eight criteria. Six favour Lambda, and two decide it. Historical analysis: becoming unprofitable is a seven-day trend. And reprocessing: when a partner corrects a cost file, Lambda recomputes one stored day, while Kappa would replay a whole day of telemetry. Kappa wins on complexity and cost, and we accept both.

**Text-to-speech** (`C03.mp3`, 72 words, about 29 s):

```text
So we compared Lambda and Kappa, using the eight criteria from the module. Six of them favour Lambda, and two decide it. The first is historical analysis. Becoming unprofitable is a trend over seven days. The second is reprocessing. When a partner corrects a cost file, Lambda recomputes one stored day, while Kappa would have to replay a whole day of telemetry. Kappa wins on complexity and cost, and we accept both.
```

#### C04 · 1:16–1:48 · Person 1 · The architecture

**Screen:** Report p.6 (Figure 2)

**Show / do:**
1. Go to page 6 and zoom until **Figure 2 — The layered architecture** fills the screen.
2. Trace with the pointer as it is named: **Telemetry producer → Apache Kafka → Spark Structured Streaming** (blue) → **Redis** and **MinIO lake** → **Spark batch** + **Airflow** (green) → **PostgreSQL** → **FastAPI** → **Grafana**.

**Say:**

> Here is the architecture, from top to bottom. Telemetry goes into Apache Kafka. In blue, the speed layer: Spark Structured Streaming keeps a live view in Redis, and stores every raw event in the MinIO lake. In green, the batch layer: Airflow runs Spark once per simulated day, and the exact results go into PostgreSQL. At the bottom, FastAPI merges both views for Grafana.

**Text-to-speech** (`C04.mp3`, 74 words, about 30 s):

```text
Here is the architecture, from top to bottom. Telemetry goes into Apache Kafka. In blue is the speed layer. Spark Structured Streaming keeps a live view in Redis, and stores every raw event in the Min I O data lake. In green is the batch layer. Airflow runs Spark once per simulated day, and the exact results go into Postgres. At the bottom, the Fast A P I service merges both views for Grafana.
```

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

**Text-to-speech** (`C05.mp3`, 41 words, about 17 s):

```text
Now, the live system. One command started every service you see here. The three setup containers ran once, and finished. The clock is simulated. One day lasts five real minutes, which is two hundred and eighty-eight times faster than real time.
```

#### C06 · 2:08–2:42 · Person 1 · Live: streaming ingestion in Kafka

**Screen:** Kafka UI (tab 2)

**Show / do:**
1. Kafka UI → **Topics**: four topics (`fleet.alerts.v1`, `fleet.telemetry.dlq`, `fleet.telemetry.v1`, `fleet.vehicle.registry`).
2. At “six partitions”: click **fleet.telemetry.v1** → **Overview** (6 partitions with similar message counts).
3. At “messages are Avro”: open **Messages** and click the newest message. The value is decoded: `vehicle_id`, `trip_id`, `lat`, `lon`, `speed_kmh`, `status`, `fare`, `event_time`, `ingest_time`; the key is the vehicle id.
4. At “broken on purpose”: back to **Topics** → **fleet.telemetry.dlq** → **Messages**, expand one and point at **rejection_reason**.

**Say:**

> Data comes in from two sources. The streaming source simulates a hundred and fifty vehicles, sending about two hundred and forty events per second. The telemetry topic has six partitions, and each message is keyed by vehicle ID, so every vehicle's events stay in order for the idle detector. Messages are Avro, checked by the Schema Registry. About one percent are broken on purpose, and they land in the dead-letter topic with the reason.

**Text-to-speech** (`C06.mp3`, 72 words, about 29 s):

```text
Data comes in from two sources. The streaming source simulates one hundred and fifty vehicles, sending about two hundred and forty events every second. The telemetry topic has six partitions, keyed by vehicle I D, so each vehicle's events stay in order for the idle detector. The messages are Avro, checked by the Schema Registry. About one percent are broken on purpose, and they land in this dead-letter topic, with the reason.
```

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

**Text-to-speech** (`C07.mp3`, 68 words, about 28 s):

```text
The second source is the daily batch file. After each simulated day, the partners' expense file lands in the data lake as one C S V, with fuel, maintenance, and distance per vehicle. It skips Kafka, because a daily file is not an event stream. Every service also writes structured logs, like this producer line, tagged with its stage. Next, let us see how this data is processed.
```

## Handover

Person 2 continues with the processing (C08).

## Likely questions for you

- **Why not Kappa?** History is the question itself (a seven-day trend), a correction should cost one stored day and not a day's replay, a stored file plus a deterministic job is better for auditing driver pay, and a daily CSV is not an event stream (report §3.3).
- **When would you choose Kappa?** If costs arrived continuously, the trend window shrank to hours, and nobody needed to audit the figures (report §3.3).
- **Why key Kafka by vehicle_id?** Kafka keeps order only inside a partition; the idle detector must see each vehicle's readings in order.
- **Why is the expense file not in Kafka?** It is a once-a-day file, not an event stream; a landing folder in object storage plus validation is simpler and loses nothing.
- **Why compress time 288 times?** The brief allows it; it lets us show a week in 35 minutes. Watermarks are in simulated time: 30 simulated minutes is 6.25 real seconds.
