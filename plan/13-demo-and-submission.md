# 13 — Demo Video & Submission

> **PDF:** *"Include a short (5–10 minute) demo video OR be prepared to demo live, showing the pipeline running end-to-end and the **observability results**."*
>
> Note that "observability results" is called out specifically — a demo that only shows the business dashboard misses half the brief.

---

## 1. Demo script — 9 minutes

Run `make clean && make up` about 4 minutes before recording so the stack is warm and simulated day 1 is underway. Record at 1920×1080.

| Time | Segment | What to show | What to say |
|---|---|---|---|
| **0:00–0:45** | **The question** | The business question on a slide, split into its two halves | "One sentence, two questions: one needs an answer in seconds and tolerates approximation; the other needs an answer once a day and must be exactly right because it's money. That split is why we chose Lambda." |
| **0:45–1:30** | **Architecture** | Diagram D2 | Walk the three layers using the module's names. Point at where each question is answered. State the simulated clock: 1 simulated day = 5 real minutes, 288×. |
| **1:30–2:30** | **Ingestion** | Kafka UI: topics, 6 partitions, near-equal per-partition counts, three consumer groups with lag | "Keyed by `vehicle_id` — not for balance, for **ordering**. Idle detection is a per-vehicle state machine and Kafka only guarantees order within a partition." Show the compacted registry topic too. |
| **2:30–3:30** | **Speed layer** | Spark UI Structured Streaming tab (input rate, processing rate, batch duration), then the Grafana **Fleet Operations** dashboard updating live | Show the diurnal earnings curve and the zone breakdown. "This is question one, answered continuously — and knowingly approximate." |
| **3:30–4:15** | **A business alert** | `V007` idle alert appearing in the dashboard and at `/api/v1/alerts/idle` | "Scripted vehicle, seeded run — this fires at the same simulated time every run. Stateful streaming, `flatMapGroupsWithState` per vehicle." |
| **4:15–5:15** | **Batch layer** | MinIO console: `sim_date=` partitions filling; the expense CSV landing; Airflow grid view with green columns; graph view | "Kafka handles real-time ingestion writing to storage; Airflow picks it up in batch — that's the lecture's own prescription. No transformation logic in any DAG." |
| **5:15–6:00** | **The daily report** | Open the generated PDF from MinIO: fleet summary, per-vehicle profitability, `V113` at the top of the unprofitable list, reconciliation exceptions, DQ summary | "Question two, answered. `V113` runs the outskirts — high distance, low fares." |
| **6:00–6:45** | **★ The restatement ★** | `/api/v1/vehicles/V0xx/profitability` before; the restatement marker appearing; the watcher DAG firing; the reconciliation re-running; the same endpoint after, with changed numbers and `restated_at` set | **The most important 45 seconds.** "A garage restated an invoice. We re-read one immutable Parquet partition and upsert. Under Kappa this would mean replaying every telemetry event for that day. That asymmetry is the core of our architecture argument." |
| **6:45–7:30** | **The merge boundary** | `/docs`, then a merged response showing `source: batch` and `source: speed` rows with `consistency: batch-complete-through …` | "The lecture names 'reconciling data between systems' as Lambda's weakness. Here's our answer — an explicit high-water-mark, half-open intervals, and every row labelled with its provenance." |
| **7:30–8:30** | **Observability** | Pipeline Health dashboard; then `make chaos-kill-producer` live → the throughput cliff → `NoTelemetryIngested` firing in Alertmanager; restart → recovery. Then a Jaeger trace waterfall. | "Pipeline-health alerts protect the system; business alerts protect the user. We keep them separate on purpose." Mention the tracing limitation honestly: micro-batch granularity, sampled links. |
| **8:30–9:00** | **Reproducibility & close** | The README quick start; `make demo`; `make test` passing | "Fresh clone, five commands. Seeded, so the demo is identical every run." One honest sentence on the biggest limitation. |

### Recording notes
- **Rehearse once on day 11** and time each segment. The restatement and the alert both depend on simulated time, so know exactly when they will fire.
- Increase terminal font size; a marker watching on a laptop cannot read 10 pt.
- If a segment fails live, do not fight it — cut, fix, re-record that segment. A polished 9 minutes beats an authentic 14.
- Narrate *decisions*, not clicks. "I'm opening Kafka UI" is wasted breath; "keyed by vehicle_id for ordering, not balance" earns marks.

---

## 2. Submission checklist

**Codebase (git repository or zip)**
- [ ] All source: simulators, ingestion, speed layer, batch layer, serving
- [ ] Observability config: `prometheus.yml`, `alerts.yml`, `alertmanager.yml`, Grafana provisioning, OTel collector config
- [ ] `docker-compose.yml` + `.env.example`, all image tags pinned
- [ ] README (structure in `10 §7`)
- [ ] Tests, and `make test` green
- [ ] `plan/` and `docs/adr/` included — they evidence that decisions preceded code
- [ ] `.env` **not** committed; no credentials in history
- [ ] Tagged `v1.0-submission`

**Report**
- [ ] PDF, 8–15 pages, all mandated sections
- [ ] Diagrams vector and legible
- [ ] Screenshots from the real run
- [ ] Limitations honest
- [ ] Individual contributions statement (**required for group submissions**)

**Demo**
- [ ] 5–10 minute video (target 9), audio clear, text legible
- [ ] Shows end-to-end operation **and** observability results
- [ ] Uploaded and the link works from an incognito window

**Assumptions declaration** (PDF: *"clearly state any assumptions, simplifications, or simulated-time compression used"*)
- [ ] Simulated clock in the README **and** the report
- [ ] Seeded scenarios (`V007`, `V113`, the day-3 restatement) documented as scripted
- [ ] Retention reduction from the design value declared

---

## 3. Viva preparation

The notes say every architectural decision and *"every line of core pipeline logic"* must be defensible. Be able to open and explain, without hesitation:

1. `common/simclock.py` — and why watermarks are in simulated time
2. `transforms/utilization.py` — and show both layers importing it
3. `speed_layer/idle_detector.py` — the state machine and its timeout
4. `batch_layer/daily_profitability.py` — the `full_outer` join and its three cases
5. `serving/merge.py` — the boundary, every case, and the degraded path
6. `observability/prometheus/alerts.yml` — the first two rules and why they are the PDF's required ones
7. The `docker-compose.yml` `depends_on` graph and why `init` exists

Rehearse the attack/answer table in `01 §5` out loud. Also be ready for: *"what would you cut if you had one more week?"* — a good answer names Delta Lake for the master dataset and real per-event tracing, and explains why neither made this cut.
