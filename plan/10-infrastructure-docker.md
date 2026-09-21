# 10 — Infrastructure & Docker Compose

> **PDF:** *"A README with architecture summary, setup/run instructions, and how to reproduce results (**Docker Compose strongly recommended**)."*
>
> **Rubric:** Code Quality & Documentation — *"configuration management, and **reproducibility** (e.g. via Docker/Compose)."*

---

## 0. Prerequisites — verified against this machine (2026-08-18 audit)

### 0.1 The blocker: Docker WSL integration is OFF

Confirmed from Docker Desktop's own config (`AppData/Roaming/Docker/settings-store.json`):
`"IntegratedWslDistros": []`. Docker Desktop is installed but not integrated with this
Ubuntu distro, and was not running at audit time.

> Docker Desktop → **Settings → Resources → WSL Integration** → enable this Ubuntu distro → **Apply & Restart**

Verify: `docker run --rm hello-world` and `docker compose version`.

Good news: `docker_data.vhdx` is already **34 GB**, so images from previous use are cached
and the pulls will be faster than a cold start.

### 0.2 The real constraint: 15.6 GB of host RAM

| | |
|---|---|
| **Host physical RAM** | **15.6 GB** (measured, not assumed) |
| Host logical CPUs | 16 |
| WSL currently sees | 14 GB — no `.wslconfig` exists, so WSL has taken ~90% of the host |
| Windows itself needs | ~3–4 GB |
| **Realistically available to Docker** | **≈ 11 GB** |

Docker Desktop's WSL2 backend runs inside the **same** WSL VM memory pool, so the
`.wslconfig` cap governs Ubuntu *and* `docker-desktop` *and* every container combined.

Create `C:\Users\Munzib\.wslconfig`, then `wsl --shutdown` and reopen:

```ini
[wsl2]
memory=11GB
processors=12
swap=8GB
```

`swap=8GB` (raised from the usual 4) is deliberate: it is a safety net so a brief
over-commit degrades into slow rather than into an OOM kill mid-demo. **Do not plan to
run in swap** — Kafka and Cassandra behave badly there — but having it prevents a
catastrophic failure while recording.

### 0.3 Disk, network and CPU are not constraints

| Resource | Status |
|---|---|
| C: drive | 634 GB free |
| D: drive | 369 GB free |
| WSL ext4 | 853 GB free |
| Docker Hub / PyPI | reachable |
| CPU | 16 logical cores |

### 0.4 Host tooling already present

Verified installed: `pdflatex`, `lualatex`, `latexmk`, `texlive-latex-extra`,
`texlive-pictures`, `texlive-science` (so `tikz`, `pgfplots`, `tcolorbox`, `fontspec`,
`booktabs`, `hyperref` all resolve); **`drawio` 30.3.11** with WSLg running
(`DISPLAY=:0`) plus `xvfb-run` for headless export; cairo/pango/gdk-pixbuf for
WeasyPrint; Python 3.12.3 with `uv` 0.11.25; Node v20.20.2.

Missing and worth installing before day 12:

```bash
npm i -g @mermaid-js/mermaid-cli          # mmdc, for the sequence diagrams
sudo apt install -y texlive-fonts-extra   # inconsolata, Fira (cosmetic; will prompt for password)
```

Not needed on Linux: video capture — record the browser with Windows Game Bar (Win+G)
or OBS on the Windows side.

**No passwordless sudo** on this machine, so any `apt install` will prompt.
**No `gh` CLI and no GitHub SSH key** — the submission asks for a Git repository link,
so set up GitHub authentication or plan to submit a zip archive.

---

## 1. Service topology

Two compose files: `docker-compose.yml` (the pipeline) and `docker-compose.observability.yml` (metrics/traces/dashboards), combined via `COMPOSE_FILE` in `.env`. Splitting them means `make up-core` can run a lighter stack while developing.

### 1.1 Core pipeline

| Service | Image | Host port | Mem limit | Healthcheck |
|---|---|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.6.1` (KRaft) | 9092 | 1.5 G | `kafka-broker-api-versions` |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.1` | 8081 | 512 M | `curl /subjects` |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | **8080** | 384 M | `curl /actuator/health` |
| `minio` | `minio/minio:RELEASE.2024-06-13T22-53-53Z` | 9000, **9001** | 512 M | `mc ready local` |
| `postgres-mart` | `postgres:16-alpine` | **5442** | 512 M | `pg_isready` |
| `postgres-airflow` | `postgres:16-alpine` | 5433 | 256 M | `pg_isready` |
| `redis` | `redis:7.2-alpine` | **6389** | 256 M | `redis-cli ping` |
| `spark-master` *(`full` profile only)* | built from `python:3.11-slim-bookworm` + pyspark 3.5.1 | 7077, **8090** | 512 M | port check |
| `spark-worker-1/2` *(`full` profile only)* | same image | — | 1.5 G each | port check |
| `airflow-webserver` | `apache/airflow:2.9.3-python3.11` | **8082** | 768 M | `curl /health` |
| `airflow-scheduler` | `apache/airflow:2.9.3-python3.11` | — | 768 M | `airflow jobs check` |
| `telemetry-producer` | build `./docker/producer` | 8001 | 256 M | `curl /health` |
| `expense-dropper` | build `./docker/producer` | 8002 | 256 M | `curl /health` |
| `streaming-job` | build `./docker/spark-app` | **4040** | 1.5 G | driver port |
|  ↳ runs `master("local[4]")` by default; joins the cluster only under `make up-full` — see §1.3 | | | | |
| `api` | build `./docker/api` | **8000** | 256 M | `curl /health/live` |
| `init` (one-shot) | build `./docker/init` | — | 128 M | — |

`init` creates topics, registers schemas, creates MinIO buckets, runs SQL DDL, seeds the vehicle registry, and **writes the simulated-clock epoch**. It runs once and exits; everything else waits on it.

### 1.2 Observability

| Service | Image | Host port | Mem |
|---|---|---|---|
| `prometheus` | `prom/prometheus:v2.53.0` | **9090** | 512 M |
| `pushgateway` | `prom/pushgateway:v1.9.0` | 9091 | 128 M |
| `alertmanager` | `prom/alertmanager:v0.27.0` | **9093** | 128 M |
| `grafana` | `grafana/grafana:11.1.0` | **3000** | 384 M |
| `kafka-exporter` | `danielqsj/kafka-exporter:v1.7.0` | 9308 | 128 M |
| `postgres-exporter` | `prometheuscommunity/postgres-exporter:v0.15.0` | 9187 | 128 M |
| `redis-exporter` | `oliver006/redis_exporter:v1.62.0` | 9121 | 128 M |
| `statsd-exporter` | `prom/statsd-exporter:v0.27.0` | 9102 | 128 M |
| `otel-collector` | `otel/opentelemetry-collector-contrib:0.104.0` | 4317, 4318 | 256 M |
| `jaeger` | `jaegertracing/all-in-one:1.58` | **16686** | 384 M |
| `alert-sink` | build | 8003 | 128 M |
| `loki` + `promtail` *(stretch)* | `grafana/loki:3.0.0` | 3100 | 512 M |

### 1.3 Memory budget — profiles, because 11 GB is the ceiling

The full stack as originally specced is ~12.7 GB, which **does not fit** in the ~11 GB
Docker can realistically have on this 15.6 GB host (§0.2). The compose file is therefore
organised into **profiles**, and the default is deliberately not the maximum.

| Profile | Command | Services | RAM |
|---|---|---|---|
| **core** | `make up-core` | Kafka, Schema Registry, MinIO, Postgres-mart, Redis, producers, **Spark in local mode**, API, init | **≈ 5.6 G** |
| **run** (default) | `make up` | core + Kafka UI + Airflow (webserver, scheduler, metadata DB) | **≈ 8.2 G** |
| **obs** | `make up-obs` | run + Prometheus, Pushgateway, Alertmanager, Grafana, exporters | **≈ 9.9 G** |
| **full** | `make up-full` | obs + OTel Collector + Jaeger + a standalone Spark master/worker pair | **≈ 12.4 G** |

`make up-full` **exceeds the cap by design** and is intended to be run briefly, on its own,
to capture the Jaeger and Spark-cluster screenshots — then dropped back to `up-obs`.
Everything else runs comfortably inside the budget.

#### The change that buys the most: Spark in local mode

The single biggest saving is dropping the separate `spark-master` + two `spark-worker`
containers (~3.5 G) and running the Structured Streaming job as
`SparkSession.builder.master("local[4]")` **inside the app container** (1.5 G).

This is not a compromise that costs marks, and the report should say so plainly:

- Structured Streaming semantics are **identical** — the same windowing, watermarking,
  checkpointing and state handling. Nothing about the processing layer changes.
- The **Spark UI is still served on 4040**, so the Structured Streaming tab screenshot
  (input rate, batch duration, watermark) is unaffected.
- Distributed execution is demonstrated separately via `make up-full`, which brings up the
  real master/worker topology for one screenshot showing tasks distributed across executors.
- At 240 events/second, a 4-core local executor is not the bottleneck — the honest
  performance table in `06 §6` already says the demo-scale job is trivial.

Documenting *why* local mode is sufficient at demo scale, and showing you know exactly what
it does and does not demonstrate, reads better than a cluster that OOMs on camera.

#### Other trims applied to the defaults

| Trim | Saves | Cost |
|---|---|---|
| `KAFKA_HEAP_OPTS=-Xmx768m` (else the JVM sizes from host RAM) | ~700 M | none |
| Airflow `LocalExecutor`, webserver and scheduler as two lean containers | — | already planned |
| Loki/Promtail off by default (`COMPOSE_PROFILES`) | ~512 M | log aggregation is a stretch goal anyway |
| Jaeger + OTel Collector only in the `full` profile | ~640 M | tracing screenshots captured in one dedicated session |
| Exporters only in `obs` and above | ~380 M | none |

#### Non-negotiable operating rule

**Only one project's stack may be up at a time.** `make down` in this repo before
`make up` in `../hospital-vitals-kappa`. The two use distinct host port ranges
(§1.1) so a stray container fails loudly on a port conflict rather than silently
corrupting a demo — but they cannot coexist in memory.

`make mem` prints current container memory against the budget so drift is visible.

---

## 2. Start-up ordering

Compose `depends_on: condition: service_healthy` throughout. The order that matters:

```
kafka ─┬─▶ schema-registry ─┐
       └─▶ kafka-ui         │
minio ──────────────────────┤
postgres-mart ──────────────┼──▶ init ──┬──▶ telemetry-producer
redis ──────────────────────┘           ├──▶ expense-dropper
spark-master ──▶ spark-worker-* ────────┼──▶ streaming-job
postgres-airflow ──▶ airflow-* ─────────┼──▶ api
                                        └──▶ (observability)
```

`init` is the synchronisation point — nothing that needs a topic, a bucket, a table or the clock epoch starts before it has completed.

**Health-check-only ordering is still not enough for Kafka**, because a broker can be "healthy" a moment before it accepts producer connections. Producers therefore retry with backoff (`03 §4.6`) rather than relying on compose alone. This belt-and-braces approach is worth a sentence in the report — it is the difference between a stack that comes up reliably and one that needs `docker compose up` run twice.

---

## 3. WSL2-specific issues to expect

| Issue | Symptom | Fix |
|---|---|---|
| **Files on `/mnt/c/`** | Everything is 10–50× slower; Spark shuffle crawls | Keep the project in the Linux filesystem (`/home/munsif/…` — already correct). **Never** bind-mount from `/mnt/c`. |
| **Stale KRaft cluster id** | Kafka refuses to start: *"The Cluster ID … doesn't match"* | `make clean` removes the volume. Happens whenever `CLUSTER_ID` changes. |
| **`vmmem` balloon** | Whole machine stalls | `.wslconfig` memory cap (§0) |
| **Spark on host Java 21** | `UnsupportedClassVersionError` / obscure reflection failures | Never run Spark on the host JVM. `bitnami/spark:3.5.1` ships Java 17 internally. The host's Java 21 is irrelevant inside containers. |
| **PySpark ↔ Python version mismatch** | `Python in worker has different version` | The Spark image's Python must match the app image's. Pin both to 3.11 and set `PYSPARK_PYTHON`. |
| **Clock skew after laptop sleep** | Simulated clock jumps; watermarks drop everything | `SimClockDrift` alert catches it; `make restart-sim` re-anchors the epoch. Documented in the README troubleshooting section. |
| **Port already allocated** | Compose fails to start | Distinct port range per project (§1.3); `make ports` prints what is in use |
| **MinIO `s3a://` from Spark** | `ClassNotFoundException: S3AFileSystem` | Spark needs `hadoop-aws` + `aws-java-sdk-bundle` jars, plus `fs.s3a.path.style.access=true` and `fs.s3a.endpoint=http://minio:9000`. **This will cost an hour the first time — budget for it on day 3.** |

### 3.1 Findings from the day-3 build (verified on this machine)

Three infrastructure assumptions in this plan turned out to be wrong, and the
corrections are recorded here rather than silently applied:

| Assumption | Reality | Correction |
|---|---|---|
| `bitnami/spark:3.5.1` | **Withdrawn from Docker Hub.** The 3.5 tags no longer resolve. | Build from `python:3.11-slim-bookworm` + `pip install pyspark==3.5.1`. Also guarantees driver and worker Python versions match. |
| `apache/spark:3.5.1-python3` as the fallback | Ships **Python 3.8**, too old for this codebase (`StrEnum`, `datetime.UTC`, `match`, dataclass `slots`) | as above |
| `python:3.11-slim` | Now resolves to Debian 13 (trixie), which ships **only Java 21/25**; Spark 3.5 supports 8/11/17 | Pin `-slim-bookworm` |
| PostgreSQL on host port 5432, Redis on 6379 | **This machine runs native PostgreSQL and Redis on those ports.** Docker silently failed to publish rather than erroring, so host tools connected to the wrong database. | Publish on **5442** and **6389** |

The last one is the most dangerous of the four, because nothing failed: tests
connected to a different PostgreSQL and reported a clean skip. Non-default host
ports are now the rule for every service this project exposes.

**`s3a://` is verified working** as of day 3 (`make verify-s3a`): 500 rows written
to MinIO as Parquet and read back, with partition pruning confirmed. The jar
versions that make it work are pinned in `docker/spark-app/Dockerfile` with the
reason written next to them.

---

## 4. Data lifecycle — what persists, what expires, what to do between runs

**All data in this project is generated, not loaded.** There is no seed dataset; the producers
invent telemetry and expense records continuously while the stack runs. Nothing exists before
`make up` and everything is rebuildable by running again.

### 4.1 Where state lives

| Volume | Holds | Survives `make down`? | Survives `make clean`? |
|---|---|---|---|
| `kafka-data` | The event log | Yes | No |
| `minio-data` | Parquet master dataset, landed expense files, generated reports | Yes | No |
| `postgres-mart-data` | The star-schema data mart | Yes | No |
| `postgres-airflow-data` | Airflow DAG run history | Yes | No |
| `redis-data` | Nothing durable — persistence is **off** by design (`07 §3`) | n/a | n/a |
| `prometheus-data`, `grafana-data` | Metric history, dashboard state | Yes | No |
| `./state/sim_epoch.json` (bind mount) | The simulated-clock anchor | Yes | Removed by `make clean` |

### 4.2 Data that expires on purpose while running

This is designed behaviour, not leakage:

| What | Lifetime | Why |
|---|---|---|
| Kafka telemetry | `retention.ms` (1 h real in demo config, `04 §1.4`) | Kafka is a buffer here, not the historical store — MinIO is |
| Redis zone/snapshot keys | 2 simulated hours | The speed layer covers only the window since the last batch run; TTL makes it self-cleaning (`07 §3`) |
| Redis vehicle state | 30 simulated minutes | Same reason |
| MinIO telemetry partitions | 30 simulated days, enforced by `enforce_retention` (`06 §7`) | Bounded lake growth |
| PostgreSQL mart | **Never expires** | It is the financial record |

### 4.3 ★ The restart rule: every run starts fresh ★

**`make clean && make up` is the default way to start.** Not `make down && make up`.

The reason is the simulated clock. Every run begins at simulated `2026-03-01` (day 1). If the
previous run's data is still in the mart and the lake, a new run writes a *second* day 1, day 2
and day 3 over the top of the old ones. The upsert on `(vehicle_id, sim_date)` means rows are
overwritten rather than duplicated, but the trend calculations, the high-water-mark and the
Airflow run history all become incoherent, and the resulting screenshots are worthless.

**The init container enforces this.** On start-up it checks for evidence of a previous run
(`./state/sim_epoch.json` present, or rows in `mart.fact_vehicle_daily_pnl`) and:

```
ERROR: existing simulation data found (sim_epoch dated 2026-08-18T14:02:11Z,
       412 rows in mart.fact_vehicle_daily_pnl).

Starting a new run over old data produces incoherent dates and trends.

  make clean && make up     start fresh (recommended)
  RESUME=1 make up          continue the previous run, reusing the stored clock epoch
```

`RESUME=1` re-reads the stored epoch instead of writing a new one, so simulated time picks up
where it left off. **Use it only when restarting a single crashed service mid-session**, never
to begin a fresh demo.

Failing loudly here is deliberate: silently producing a second day 1 is the kind of bug that
costs an afternoon to diagnose because nothing errors — the numbers are just quietly wrong.

### 4.4 Why a fresh start costs you nothing

`RANDOM_SEED=42` is fixed, so **every fresh run generates byte-identical data**. `V007` goes
idle at the same simulated moment, `V113` becomes unprofitable on the same simulated day, and
the expense restatement lands at the same point every time.

That is what makes the demo and the report screenshots reproducible, and it is asserted by
`tests/unit/test_determinism.py` (`03 §6`). A marker re-running your submission sees exactly
what your report shows.

### 4.5 Practical guidance

| Situation | Command |
|---|---|
| Starting a demo, or capturing screenshots | `make clean && make up` |
| Recording the video | `make clean && make up`, wait ~4 min, then record |
| Restarting one crashed service mid-session | `docker compose restart <svc>` (clock untouched) |
| Restarting the whole stack mid-session, keeping progress | `make down && RESUME=1 make up` |
| Changing streaming aggregation logic | `make reset-checkpoint` first — checkpoints are invalidated by logic changes (`05 §5`) |
| Reclaiming disk after several runs | `make clean` then `docker system prune -f` |

Disk note: a full 3-simulated-day run produces roughly 60 MB of Parquet, a few MB of Postgres
and under 2 GB of Kafka log. Runs do not accumulate unless you avoid `make clean`.

---

## 5. Configuration management

**Single source of truth: `.env`**, with `.env.example` committed and `.env` git-ignored. No secrets and no hostnames anywhere in code.

```bash
# Simulation
SIM_DAY_SECONDS=300
SIM_EPOCH_SIM=2026-03-01T00:00:00Z
FLEET_SIZE=150
TARGET_EPS=240
DEFECT_RATE=0.01
RANDOM_SEED=42

# Kafka
KAFKA_BOOTSTRAP=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
TELEMETRY_TOPIC=fleet.telemetry.v1
TELEMETRY_PARTITIONS=6

# Processing
WATERMARK_SIM_MINUTES=30
WINDOW_SIM_MINUTES=15
WINDOW_SLIDE_SIM_MINUTES=5
IDLE_ALERT_SIM_MINUTES=45

# Storage
MINIO_ENDPOINT=http://minio:9000
LAKE_BUCKET=fleet-lake
POSTGRES_MART_DSN=postgresql://fleet:fleet@postgres-mart:5432/fleet_mart
REDIS_URL=redis://redis:6379/0

# Observability
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
PUSHGATEWAY_URL=http://pushgateway:9091
LOG_LEVEL=INFO
```

In Python, `pydantic-settings` gives typed, validated settings with fail-fast startup:

```python
class Settings(BaseSettings):
    sim_day_seconds: int = Field(300, gt=0)
    watermark_sim_minutes: int = Field(30, ge=1)
    ...
    @model_validator(mode="after")
    def watermark_must_survive_real_jitter(self):
        # 30 sim-min ÷ 288× = 6.25 real seconds. See plan/03 §3.3.
        real = self.watermark_sim_minutes * 60 / (86400 / self.sim_day_seconds)
        if real < 5.0:
            raise ValueError(f"Watermark is only {real:.2f} real seconds — too tight.")
        return self
```

**That validator is worth showing in the viva.** It encodes the project's subtlest coupling as a startup-time guard rather than a comment nobody reads.

---

## 6. Makefile

```
make setup            # check docker, copy .env.example → .env, pull images

make up-core          # ~5.6 G   pipeline only, Spark local mode
make up               # ~8.2 G   + Kafka UI + Airflow            (DEFAULT)
make up-obs           # ~9.9 G   + Prometheus/Grafana/Alertmanager
make up-full          # ~12.4 G  + Jaeger/OTel + Spark cluster   (screenshots only)
make mem              # current container memory vs the budget

make down             # stop, keep volumes
make clean            # stop and DELETE all data  <-- the normal way to start a run (§4.3)
make reset-checkpoint # clear Spark checkpoints only (after changing streaming logic)
make logs SVC=api     # follow one service's JSON logs
make ps               # status + health of every service
make ports            # what's listening where

make demo             # scripted end-to-end demo run (see 13)
make restart-sim      # re-anchor the simulated clock

make topics           # list topics + partition counts
make peek TOPIC=...   # Avro → JSON on the CLI
make lag              # consumer lag for all three groups
make psql             # psql into the mart
make redis-cli
make trigger-dag DAG=fleet_daily_reconciliation

make test             # unit tests (no docker)
make test-int         # integration smoke test (needs the stack up)
make lint             # ruff + ruff format --check + mypy
make chaos-kill-producer / chaos-bad-data / chaos-pause-consumer / chaos-kill-redis

make report           # regenerate the daily report for the latest simulated day
```

`make demo` is the single command the marker runs. It must work from a fresh clone.

---

## 7. Reproducibility checklist

Run this on a clean machine before submitting — it *is* the Code Quality mark:

- [ ] `git clone` → `make setup` → `make up` → healthy within 3 minutes
- [ ] No manual UI steps required (no hand-created Airflow connections, no Grafana dashboards added by hand)
- [ ] Every image tag is pinned — **no `:latest` anywhere**
- [ ] `.env.example` covers every variable the code reads
- [ ] `make clean && make up` works twice in a row
- [ ] Grafana dashboards and datasources appear automatically
- [ ] `make demo` produces a report PDF without intervention
- [ ] README's quick-start is 5 commands or fewer
- [ ] `RANDOM_SEED` fixed → the demo narrative (`V007` idle, `V113` unprofitable) reproduces identically
- [ ] Total disk footprint documented (~8 GB of images)

---

## 8. README structure

The README is graded. Sections, in order:

1. **What this is** — one paragraph + the architecture diagram
2. **The business question and how the system answers it**
3. **Architecture at a glance** — the three layers, one line each, linking to the report
4. **Quick start** — 5 commands
5. **What to look at** — the URL table from `00 §6`
6. **The demo** — what to watch and when (`V007` at simulated 10:00, the restatement on simulated day 3)
7. **Simulated clock** — the compression, stated clearly
8. **Repository layout** — annotated tree
9. **Configuration** — the `.env` table
10. **Running the tests**
11. **Troubleshooting** — the WSL2 table from §3
12. **Limitations** — pointer to the report
13. **Individual contributions** — required for group submissions
