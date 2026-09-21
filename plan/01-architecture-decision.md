# 01 — Architecture Decision: Lambda vs Kappa

> **Rubric weight: 20 marks — the single largest criterion.** Assessed on *"correctness and depth of the argument for the chosen architecture given the use case's latency, replay, cost and consistency requirements; honest discussion of trade-offs and rejected alternatives."*
>
> This document is the source material for §3 of the report. It is written to be argued, not just stated.

---

## 1. Start from the business question, not from the technology

> *"What is fleet utilization and earnings by area/time-of-day **right now**, and which vehicles are **becoming unprofitable** once **yesterday's** fuel/maintenance costs are factored in?"*

Decompose it:

| | Question A — "right now" | Question B — "unprofitable, given yesterday's costs" |
|---|---|---|
| **Who asks it** | Dispatch operations | Finance / fleet management |
| **How often** | Continuously, on a wall-mounted dashboard | Once a day |
| **Latency budget** | Seconds | Hours (it is *inherently* T+1 — the input file does not exist until the day ends) |
| **Accuracy demanded** | Approximate is fine. A utilization figure of 71% vs 72% changes no decision. | **Exact.** This feeds driver settlements and vehicle retirement decisions. Being wrong is a financial and possibly contractual problem. |
| **Tolerance for late data** | High — a GPS ping arriving 20 minutes late can be dropped without harm | **Zero** — a restated invoice must change the answer |
| **Input** | Streaming telemetry only | Streaming telemetry **joined with** a daily file |
| **Reprocessing need** | None. Yesterday's live dashboard is worthless. | **Frequent.** Garages restate invoices; the number must be recomputed. |

**These two rows have almost nothing in common.** One wants speed and forgives error; the other wants correctness and forgives delay. Any architecture that forces them down a single processing path must compromise one of them.

That is the entire argument. Everything below is the formal version of it.

---

## 2. The decision, applied against the module's own criteria

The module gives five explicit criteria (*"When to use the Lambda Architecture and when to use the Kappa Architecture"*) and three more in the comparison table. We walk all eight. **The report should use these as sub-headings verbatim** — it demonstrates the decision was made with the module's framework, not a blog post's.

### 2.1 Latency requirements

> *Module: "If your application requires real-time processing and low latency, the Kappa Architecture may be a better choice. However, if your application can tolerate higher latency for batch processing, the Lambda Architecture could be a good fit."*

**Verdict: Lambda.** Not because we don't need low latency — we do, for Question A — but because we need *two different latencies simultaneously*. The criterion as stated assumes one application with one latency budget. We have one application with two. Lambda's defining property is that it provides a low-latency path **and** a high-latency path from the same source. Question A is served in seconds by the speed layer; Question B is served T+1 by the batch layer, which is not a compromise because **its input data literally does not exist any earlier**.

This last point deserves emphasis in the report: *the batch layer's latency is not a limitation we accepted, it is a property of the business process we are modelling.* Fuel partners submit costs at end of day. No architecture can make a T+1 file available at T.

### 2.2 Data volume

> *Module: "The Lambda Architecture is better suited for processing large volumes of data, especially if it needs to be processed in batch mode. The Kappa Architecture is ideal for processing smaller data volumes that can be handled in real-time."*

**Verdict: Lambda.** At our demo scale: 150 vehicles × 480 pings/simulated-day = **72,000 events per simulated day**, ~240 events/second. At a realistic operator scale (5,000 vehicles, 30-second real ping interval) it is **14.4 million events per day** and ~167 events/second sustained. Answering "is this vehicle *becoming* unprofitable" requires a trailing window of at least 7–30 days — i.e. **100M–430M events at rest**, joined against expense records.

That is unambiguously a "process at rest, in batch" workload by the module's own definition, and it maps to the module's first named workload type: *"Batch processing of big data sources **at rest**"*.

> **Cross-project note worth making in the report:** the sibling use case (hospital ward vitals) generates ~3,840 events per simulated day — roughly **19× less**. The same criterion that pushes this project toward Lambda pushes that one toward Kappa. The criterion is doing real discriminating work here, not being cited decoratively.

### 2.3 Complexity

> *Module: "The Kappa Architecture is simpler and easier to manage since it eliminates the batch layer. The Lambda Architecture is more complex and requires managing multiple layers and technologies."*

**Verdict: this criterion favours Kappa. We concede it.**

Do not fudge this. The module names two specific Lambda drawbacks — *"managing multiple codebases"* and *"reconciling data between systems"* — and we have both. The report gains marks by naming them plainly and then showing what we did about them:

**Mitigation 1 — for "managing multiple codebases":** there is exactly **one** codebase. All business logic lives in `src/fleet/transforms/`, a package of pure DataFrame-in → DataFrame-out functions with no streaming or batch concepts inside them. The speed-layer entry point and the batch-layer entry point both import from it:

```python
# src/fleet/transforms/utilization.py  — imported by BOTH layers
def compute_zone_utilization(events: DataFrame) -> DataFrame: ...
def compute_idle_ratio(events: DataFrame) -> DataFrame: ...
def haversine_distance_km(df: DataFrame) -> DataFrame: ...
```

`speed_layer/streaming_job.py` calls these inside `foreachBatch`; `batch_layer/daily_profitability.py` calls the *same functions* on a static DataFrame. They are unit-tested once. **The lecturer's "multiple codebases" con is therefore reduced to "two entry points over one tested library"** — which is a genuine, demonstrable reduction, not a rhetorical one. In the viva, open the file and show the two import statements.

**Mitigation 2 — for "reconciling data between systems":** we do not hope the two views agree; we define a **boundary** and enforce it. The batch layer maintains a `mart.batch_high_water_mark` row recording the last simulated date it has fully processed. The serving layer reads it and serves `[from … hwm]` from PostgreSQL and `(hwm … now]` from Redis — never both for the same interval. Every row in the API response carries `"source": "batch" | "speed"`, and the envelope carries `"consistency": "batch-complete-through 2026-03-14"`. There is provably **no overlap and no gap**, and the client can see which numbers are exact and which are approximate. Design and code in `07-req-storage-serving.md §5`.

**What we still pay:** more containers, more moving parts, a longer README, two failure domains to monitor. We accept this and say so.

### 2.4 Historical data analysis

> *Module: "If your application requires historical data analysis, the Lambda Architecture may be a better choice since it provides batch processing and can store historical data for analysis. The Kappa Architecture focuses on real-time processing and may not be as suitable for historical data analysis."*

**Verdict: Lambda. This is the most decisive criterion for us.**

The word **"becoming"** in the business question is doing enormous work. The operator is not asking "which vehicles lost money yesterday" — that is a one-day lookup. They are asking which vehicles are on a **deteriorating trajectory**, which requires comparing a vehicle's cost/revenue ratio across a trailing window of simulated days and detecting a trend. Our implementation computes a **7-simulated-day rolling net profit** and a slope, and flags a vehicle when its trend crosses zero.

That is textbook historical analysis over data at rest, and it needs a queryable historical store — which is precisely what the batch layer plus master dataset provide and what a stream processor's bounded state does not.

### 2.5 Cost

> *Module: "The Lambda Architecture may require more resources to store and process batch data, which could increase costs. The Kappa Architecture can be more cost-effective since it only focuses on real-time processing and eliminates the need for batch processing."*

**Verdict: this criterion favours Kappa. We concede it, with a qualification.**

We do store the data twice in a sense (Kafka retains 7 days; MinIO retains the master dataset indefinitely). That is real cost. Our qualification is about *where* the cost lands:

- The master dataset is **columnar Parquet on object storage** — the cheapest durable tier, and the format the batch job can scan selectively (partition pruning by `sim_date`, column pruning by projection). A 30-day scan touches only the partitions it needs.
- The **speed layer holds almost nothing**: Redis stores only the current window's aggregates with a 2-simulated-hour TTL. Memory, the expensive tier, is deliberately kept tiny.
- Kafka retention is set to **7 days**, not infinite — because under Lambda the log does *not* have to be the historical store. MinIO is. **This is a real cost advantage of Lambda over Kappa that the module's criterion misses:** a Kappa system that wants 30 days of replayable history must pay for 30 days of broker-attached storage, which is far more expensive per byte than object storage.

So: yes, Lambda costs more in components. No, it does not obviously cost more in storage — for a workload with a long historical requirement, it may cost less. Say both.

### 2.6 Fault tolerance

> *Module (comparison table): Lambda — "Fault-tolerant, as batch processing ensures data accuracy." Kappa — "Fault-tolerant with real-time processing but depends on stream integrity."*

**Verdict: Lambda, and the property is real, not theoretical.**

If the Structured Streaming job crashes and loses in-flight state, the live dashboard shows stale or wrong numbers for as long as it takes to restart — an operational annoyance. **The financial record is untouched**, because it is derived from the immutable Parquet master dataset by a job that can be re-run at any time to produce the identical answer. The speed layer is *disposable by design*: we can `FLUSHDB` Redis and lose nothing permanent.

Under Kappa, the streaming job's state **is** the record of truth, so its corruption is a data-integrity incident, not an availability incident. Recovery means replay, and replay of a financial figure is exactly the thing we least want to have to trust.

Frame it as: **Lambda gives us a "system of record" (Parquet + Postgres) that is independent of the "system of engagement" (Redis).**

### 2.7 Data reprocessing

> *Module (comparison table): Lambda — "Batch layer allows accurate reprocessing of historical data." Kappa — "Reprocessing is done by replaying the stream in real-time."*

**Verdict: Lambda. This is the single strongest technical argument and it should lead §3.3 of the report.**

Here is the concrete scenario, and it is not hypothetical — it is how fuel and garage invoicing actually works. **A partner submits a corrected expense file for a previous day.** A fuel card was mis-assigned; a maintenance charge was billed to the wrong vehicle; a rebate was applied late. This happens routinely.

- **Under Lambda:** the corrected file lands, Airflow re-triggers `fleet_daily_reconciliation` for that simulated date, the Spark batch job re-reads *that one Parquet partition*, re-joins, and upserts `fact_vehicle_daily_pnl` on `(vehicle_id, sim_date)`. Cost: one partition scan, a few seconds. The answer is exact and the operation is idempotent — re-running it twice produces the same result.
- **Under Kappa:** the same correction requires replaying every telemetry event for that day through the streaming job to recompute the join. At realistic scale that is 14.4 million events re-processed to fix a handful of expense rows. The cost is proportional to the *telemetry* volume, not to the size of the correction.

**We build this into the demo.** The expense generator deliberately emits a **restatement of simulated day 2 during simulated day 3**, the DAG re-runs, and the reviewer watches the profitability figures for the affected vehicles change on the dashboard. The architecture argument is not merely asserted in the report — it is executed on camera.

### 2.8 Accuracy

> *Module (comparison table): Lambda — "Batch layer provides high accuracy, speed layer offers immediate but less accurate results." Kappa — "Provides consistent results, but may not match the accuracy of dedicated batch processing."*

**Verdict: Lambda, and the mapping is exact.**

The module's description of Lambda's accuracy profile — *immediate but less accurate* alongside *high accuracy* — is a **precise description of what this business question asks for**. Question A wants immediate and tolerates less accurate. Question B wants high accuracy and tolerates delay. We are not working around Lambda's accuracy split; we chose Lambda *because* the business has that split.

Concretely, the speed layer is knowingly approximate in three ways, all documented in the API response and the report:
1. Events arriving after the 10-simulated-minute watermark are dropped from windowed aggregates (they still reach the master dataset, so the batch layer sees them).
2. Aggregates are over sliding windows, so the newest window is partially filled.
3. Distance is estimated from GPS pings at 3-simulated-minute resolution; the batch layer recomputes it with the full-resolution haversine path.

The batch layer is exact and reconciles all three.

---

## 3. Summary table for the report

| # | Criterion (module's wording) | Favours | Weight for us | Note |
|---|---|---|---|---|
| 1 | Latency requirements | **Lambda** | High | We need two latencies at once; batch latency is inherent to the source |
| 2 | Data volume | **Lambda** | High | 14.4M events/day at scale; 7–30 day trailing window at rest |
| 3 | Complexity | *Kappa* | — | **Conceded.** Mitigated by shared `transforms/` + explicit merge boundary |
| 4 | Historical data analysis | **Lambda** | **Decisive** | "*becoming* unprofitable" = multi-day trend |
| 5 | Cost | *Kappa* | — | **Conceded** on components; contested on storage (object store beats broker storage for 30-day history) |
| 6 | Fault tolerance | **Lambda** | Medium | Financial record independent of streaming state; speed layer disposable |
| 7 | Data reprocessing | **Lambda** | **Decisive** | Restated invoices are routine; partition re-run vs full-day replay |
| 8 | Accuracy | **Lambda** | High | The business question *itself* has a split accuracy requirement |

**6 of 8 favour Lambda, including both decisive ones. The 2 that favour Kappa are conceded openly and mitigated in code.**

---

## 4. The rejected alternative: Kappa

The rubric explicitly asks for *"honest discussion of trade-offs and rejected alternatives"*. A rejection is only credible if the case for the rejected option is made properly first.

### 4.1 The honest case FOR Kappa here

It is a real case and the report must make it before dismissing it:

- **One processing path, one codebase, one set of semantics.** No risk of the speed layer and batch layer disagreeing, because there is nothing to disagree with. The module names this as Lambda's headline weakness.
- **Simpler operations.** Fewer containers, no MinIO, no Airflow-triggered Spark job, no high-water-mark bookkeeping, no star schema to maintain.
- **Lower infrastructure cost** in the module's terms — no separate batch compute.
- **The expense file could genuinely be modelled as a stream.** Publish each expense row to a log-compacted `fleet.expenses` topic keyed by `(vehicle_id, sim_date)`; a restatement is simply a new record with the same key, and compaction keeps the latest. A stream-stream join with a long watermark could then produce profitability continuously rather than daily. **This is a legitimate design and we should say so.**
- Our demo-scale volume (72k events/simulated day) is small enough that replay is cheap *at demo scale*.

### 4.2 Why it lost

1. **The historical requirement is not incidental, it is the question.** "Becoming unprofitable" needs a trailing multi-day window. Serving that from stream state means either holding 30 days of state in the streaming job (expensive, fragile, and a restart risk) or retaining 30 days in Kafka and replaying — both of which reintroduce, in a worse form, exactly the batch processing Kappa claims to eliminate. The module warns of this directly: Kappa *"may not be as suitable for historical data analysis"*.

2. **Reprocessing cost is asymmetric and points the wrong way.** Correcting a small expense restatement should cost work proportional to the correction. Under Kappa it costs work proportional to the day's *telemetry* volume — the largest dataset in the system — because the join must be recomputed. This is backwards, and it is a recurring operational cost, not a one-off.

3. **The auditability story is weaker.** Per-vehicle profitability feeds driver settlements. "Here is the immutable, addressable Parquet partition for 2026-03-14, and here is the deterministic job that produced the figure from it, re-runnable to the same answer" is a materially better answer to an auditor than "we replayed the log and this is what came out". The module's own governance material lists **audit trails** and **lineage** as pillars.

4. **It forces an awkward modelling of a genuinely batch source.** A CSV that a garage uploads once a day is not an event stream. Publishing it to Kafka to satisfy an architectural rule adds a producer, a topic, a serialization contract and a compaction policy, and buys us nothing we don't already get from an object-store landing zone plus a file sensor. Choosing an architecture that makes a natural batch source awkward is a signal the architecture is wrong for the problem.

5. **Kafka retention would have to expand to serve as the historical store**, which is the most expensive per-byte storage in the stack. The module's cost criterion penalises Lambda for extra storage; in this workload it is Kappa that would pay more.

6. **The module itself is sceptical.** *"[Kappa] hasn't seen widespread adoption due to streaming complexity and cost compared to traditional batch processing."* We are not obliged to agree with that, but choosing Kappa here would mean overriding both the module's stated volume guidance *and* its historical-analysis guidance *and* its adoption note, with a use case that does not require it.

### 4.3 What we would need to see to change our minds

Good reports state their own falsification conditions. We would switch to Kappa if:
- The profitability question became continuous rather than daily (e.g. costs streamed from telematics fuel sensors in real time rather than invoiced nightly), **and**
- The historical window shrank to something a streaming job can hold in state (hours, not weeks), **and**
- Auditability of the financial figure was not a requirement.

Absent all three, Lambda is the right call.

---

## 5. Viva defence — the attacks we expect and the answers

Prepare these. The notes say *"you must be able to explain and defend every architectural decision and every line of core pipeline logic"*.

| Attack | Answer |
|---|---|
| **"Your own lecture slides say Lambda means multiple codebases and reconciliation problems. You have both. Prove you handled it."** | Open `src/fleet/transforms/`. Show `compute_idle_ratio()`. Show `speed_layer/streaming_job.py:34` importing it and `batch_layer/daily_profitability.py:22` importing the same symbol. Show `tests/unit/test_transforms.py` testing it once. Then open `serving/merge.py` and walk the high-water-mark boundary. Two named weaknesses, two concrete answers in code. |
| **"Isn't this over-engineered? 72,000 events a day is nothing."** | Correct at demo scale, and we say so in the limitations section. The architecture is chosen for the production scale the scenario implies (a ride-hailing *operator*, i.e. thousands of vehicles → 14.4M events/day). We deliberately compressed the demo; we did not compress the design target. The alternative — designing for the demo and re-architecting at scale — is the failure the module's "consider big data architectures when…" slide warns about. |
| **"Why not just Kappa with a longer retention?"** | Three reasons, in order: reprocessing cost is proportional to telemetry volume rather than to the correction; 30 days of broker-attached retention costs far more than 30 days of object storage; and the audit trail for a financial figure is weaker. Then note we *did* consider the compacted-expenses-topic design and describe it — it is in §4.1. |
| **"Where exactly does the batch view end and the speed view begin? What stops you double-counting?"** | `mart.batch_high_water_mark`. The batch DAG advances it only as its final task, after the upsert commits. The serving layer reads it per request. `[from, hwm]` → Postgres; `(hwm, now]` → Redis. Half-open intervals, so the boundary date belongs to exactly one side. There is a table-driven unit test (`test_merge_boundary.py`) covering the boundary, the empty-batch case, and the case where the DAG has never run. |
| **"Your speed layer and batch layer will produce different numbers for the same day. Which is right?"** | The batch layer, always, and the API says so — every row is stamped with its source and the envelope carries `consistency: batch-complete-through <date>`. Divergence is expected and quantified: we log the delta when a simulated date transitions from speed-served to batch-served, and chart it on the Batch Reconciliation dashboard. That chart is itself a good observability artefact. |
| **"Airflow is orchestrating Spark. Isn't Airflow a processing engine here?"** | No — and the module is explicit: *"Airflow only orchestrates."* Our DAGs contain sensing, validation branching, `spark-submit` invocation, watermark advancement, report rendering and alert callbacks. No transformation logic lives in a DAG file. Grep the `airflow/dags/` directory for aggregation — there is none. |
| **"You used Spark Structured Streaming but the module taught Spark as batch and Storm for streaming. Justify."** | Covered in `02-technology-stack.md §3`. Short version: the module taught the stream *theory* (event time, watermarks, tumbling/sliding windows) in the Storm deck and taught Spark's engine in the Spark deck; Structured Streaming is where those two meet, and its `withWatermark`/`window` API is a 1:1 implementation of the taught concepts. Storm was rejected because the module presented no Python path for it and our entire stack is Python. |
| **"KRaft? The lecture taught ZooKeeper."** | The lecture correctly says Kafka used ZooKeeper *"before version 2.8"*. We run 3.x in KRaft mode, which removes the ZooKeeper ensemble. We note the version boundary in the report and in the README so it is clear this is a deliberate, informed divergence and not an oversight. |

---

## 6. What goes in the report (§3), and at what length

Target **≈ 3 pages** — the largest section, proportional to its 20 marks.

- **3.1 Candidate architectures** (~0.4 p) — both described fairly in the module's vocabulary, with the two diagrams.
- **3.2 Decision criteria applied** (~1.4 p) — the five module criteria as sub-headings (§2.1–2.5 above), then a table covering fault tolerance, reprocessing and accuracy (§2.6–2.8).
- **3.3 Decision** (~0.3 p) — the summary table from §3, then the single strongest paragraph: reprocessing asymmetry + the "becoming unprofitable" trend requirement.
- **3.4 Rejected alternative: Kappa** (~0.6 p) — §4.1 and §4.2, including the compacted-expenses design we considered.
- **3.5 Conceded weaknesses and mitigations** (~0.3 p) — complexity and cost, with the two code-level mitigations.

Do not pad this section with generic Lambda/Kappa background copied from the internet. Every paragraph should reference either the module's material or our specific use case. Generic content is exactly what the rubric's phrase *"justification tied to use-case constraints rather than generic popularity"* is written to penalise.
