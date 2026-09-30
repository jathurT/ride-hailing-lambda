# Ride-Hailing Fleet Operations: A Lambda Architecture Data Pipeline

EC8203 Applied Big Data Engineering · mini project · Use Case 1

A data platform that ingests continuous vehicle telemetry and a once-a-day partner expense
file, processes both, and answers one business question from two directions at once.

## Table of contents

- [Overview](#overview)
  - [The business question](#the-business-question)
  - [Architecture](#architecture)
  - [Simulated clock](#simulated-clock)
- [Getting started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Run the stack](#run-the-stack)
  - [Load batch history](#load-batch-history)
  - [Service URLs](#service-urls)
- [Usage](#usage)
  - [Service names and `SVC`](#service-names-and-svc)
  - [Terminal shortcuts](#terminal-shortcuts)
  - [Check processing and apply a corrected expense file](#check-processing-and-apply-a-corrected-expense-file)
  - [Trying things](#trying-things)
- [Project reference](#project-reference)
  - [Repository layout](#repository-layout)
  - [Development](#development)
  - [Documentation](#documentation)
- [Known limitations](#known-limitations)
- [Individual contributions](#individual-contributions)

---

## Overview

### The business question

> *"What is fleet utilization and earnings by area/time-of-day **right now**, and which vehicles
> are **becoming unprofitable** once **yesterday's** fuel/maintenance costs are factored in?"*

It contains two questions with opposite tolerances, and that is the whole reason the
architecture has two paths:

|  | "right now" | "becoming unprofitable" |
|---|---|---|
| Asked by | Dispatch operations | Finance |
| Latency budget | Seconds | Hours: the expense file does not exist until the day ends |
| Accuracy | Approximate is fine | Exact; it feeds driver settlements |
| Needs history | No | **Yes**: "becoming" is a multi-day trend |

One forgives error and demands speed. The other forgives delay and demands correctness.

### Architecture

| Layer | Technology | Answers |
|---|---|---|
| **Speed layer** | Spark Structured Streaming → **Redis** | *"right now"*: approximate, seconds old |
| **Batch layer** | Spark batch over **MinIO/Parquet** → **PostgreSQL** star-schema mart | *"unprofitable"*: exact, recomputable |
| **Serving layer** | **FastAPI**, merging both at an explicit high-water-mark | one API, every row labelled with its source |

Telemetry enters through Kafka (KRaft, Avro + Schema Registry), keyed by `vehicle_id` so each
vehicle's events stay ordered, which the stateful idle detector depends on. The daily expense
file is **not** streamed: it is written straight to object storage, because a once-a-day
reference feed is not an event stream.

**The merge rule.** The batch layer records the last simulated date it has fully processed.
The serving layer serves `[from, hwm]` from PostgreSQL and `(hwm, now]` from Redis. The
watermark date belongs to exactly one side, so no date is double-counted and none is dropped.
Every response states which view produced each row and how far the exact data extends.

`src/fleet/transforms/` holds pure DataFrame functions imported by **both** layers. That is the
answer to Lambda's stated weakness of "managing multiple codebases": one tested library, two
entry points, enforced by an architecture test.

### Simulated clock

One simulated day is compressed into **300 real seconds**, a **288×** speed-up. The simulation
starts at simulated date `2026-03-01`. Every container derives simulated time from one shared
anchor file written at start-up, so no two services can disagree about the date.

Durations are marked *real* or *simulated* throughout. A 30-simulated-minute watermark is
6.25 real seconds; a 2-simulated-hour TTL is 25 real seconds.

```bash
make simclock              # show the shared clock anchor and simulation rate
```

The `today` helper below returns the current simulated date from the running API.

---

## Getting started

### Prerequisites

Docker with the Compose plugin, Bash, and GNU Make. On Windows, enable
Docker Desktop's WSL integration and run the commands in a WSL terminal. The terminal helpers
below also need `curl` and `jq` on the host.

### Run the stack

Run commands from the repository root (the directory containing `Makefile` and
`docker-compose.yml`):

```bash
cd /path/to/ride-hailing-lambda   # replace with your checkout location
make setup                 # check docker, create .env
make clean && make up      # fresh simulation + full pipeline
make up-obs                # add Prometheus, Grafana, Alertmanager
```

Then open **<http://localhost:3000>** (Grafana) and **<http://localhost:8000/docs>** (the API).

> **Start every run with `make clean`.** The simulation always begins at simulated day 1, so
> restarting over retained data writes a second day 1 on top of the first. Nothing errors, the
> dates and trends just go quietly wrong. The init container detects this and refuses to start.
> To deliberately continue an existing run instead, use `RESUME=1 make up`.

### Load batch history

The batch layer needs a few complete simulated days before trends mean anything:

```bash
make backfill DAYS=7       # run the batch layer over the last complete sim days
make batch-check           # the numbers that prove it did what it claims
```

### Service URLs

| URL | What it shows |
|---|---|
| <http://localhost:8000/docs> | **The API**: every endpoint, and the merged response model |
| <http://localhost:3000> | **Grafana**: Fleet Operations, Batch Reconciliation, Pipeline Health |
| <http://localhost:8082> | **Airflow**: the daily reconciliation DAG (`admin`/`admin`) |
| <http://localhost:9090/alerts> | **Prometheus**: alert rules and their state |
| <http://localhost:9093> | **Alertmanager**: firing alerts, grouping and inhibition |
| <http://localhost:8080> | **Kafka UI**: topics, partitions, message counts |
| <http://localhost:4040> | **Spark UI**: Structured Streaming progress |
| <http://localhost:9001> | **MinIO**: the master dataset, partitioned by simulated date |
| <http://localhost:8081/subjects> | **Schema Registry**: registered Avro subjects |

Two dashboards answer the business question directly; the third shows whether the pipeline
answering it is healthy. The Grafana panels are driven through this project's own API rather
than by reading Redis and PostgreSQL directly, so a panel that breaks is telling the truth
about the serving layer.

---

## Usage

### Service names and `SVC`

`SVC` is short for **service**. In `make logs SVC=streaming-job`, it is a Makefile variable
that selects the Compose service whose logs you want. Set it on the command itself; there is
no separate definition or export required.

```bash
docker compose --profile '*' config --services   # available service names
make ps                                         # running services, health and ports
make logs SVC=streaming-job                      # follow Spark streaming logs
make logs SVC=airflow-scheduler                  # follow scheduler logs
make restart-svc SVC=telemetry-producer          # rebuild/recreate one service, preserving data
```

Use the Compose service name, such as `streaming-job`, for `SVC`. Commands such as
`docker exec` use the container name instead, such as `fleet-streaming-job`. Press `Ctrl+C`
to stop following logs; the service keeps running. `make restart-svc` uses `--no-deps` so it
does not rerun the initialization container over an existing simulation.

### Terminal shortcuts

#### Define the optional shortcuts

`svc` (lowercase) is a Bash function meaning **services**. It is separate from the uppercase
`SVC` Makefile variable. The following functions are shortcuts, so Bash will report
`command not found` until you define them. Paste the whole block into your Bash terminal
after starting the stack. Definitions last for that terminal session; paste them again in a
new terminal, or add them to `~/.bashrc` and run `source ~/.bashrc` to keep them available.

```bash
svc() {
  docker compose --profile '*' ps -a --format 'table {{.Service}}\t{{.Status}}'
}

lake() {
  docker exec fleet-minio sh -c '
    mc_bin=$(command -v mc || true)
    if [ -z "$mc_bin" ]; then
      mc_bin=/opt/bitnami/minio-client/bin/mc
    fi
    "$mc_bin" alias set l http://localhost:9000 \
      "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null || exit
    "$mc_bin" "$@"
  ' sh "$@"
}

today() (
  set -o pipefail
  curl -fsS http://localhost:8000/api/v1/pipeline/status |
    jq -er '.as_of_sim | split("T")[0]'
)

status() (
  set -o pipefail
  curl -fsS http://localhost:8000/api/v1/pipeline/status |
    jq '{as_of_sim, sim_day_index, batch_complete_thru,
         watermark_age_sim_days, pnl_rows, restated_rows, degraded, missing_stores}'
)

util() (
  set -o pipefail
  local sim_end response_dir
  sim_end=$(today) || return
  response_dir=$(mktemp -d) || return
  trap 'rm -rf "$response_dir"' EXIT
  curl -sS -D "$response_dir/headers" -o "$response_dir/body" \
    "http://localhost:8000/api/v1/fleet/utilization?from=${1:-2026-03-02}&to=$sim_end" || return
  grep -iE '^HTTP|^x-data-degraded:' "$response_dir/headers"
  jq '{consistency, batch_complete_thru, degraded, missing_stores,
       rows_by_source: ([.rows[]?.source] | group_by(.) |
         map({(.[0]): length}) | add // {}), detail}' "$response_dir/body"
)

pnl() (
  set -o pipefail
  curl -fsS "http://localhost:8000/api/v1/vehicles/${1:-V113}/profitability?days=${3:-30}" |
    jq --arg d "${2:-2026-03-03}" \
      '.rows[] | select(.sim_date == $d) |
       {sim_date, fuel_cost, net_profit, restatement_count, restated_at}'
)
```

`lake` runs the MinIO client inside the container, using its configured credentials. It
creates a client alias named `l` for the local object store, so `l/fleet-lake` means the
`fleet-lake` bucket on that store. It supports the client paths used by both the base MinIO
image and the Bitnami image in `docker-compose.override.yml`; no host MinIO client is needed.
The API helpers require the serving API on port 8000. `today` reads the simulated clock,
not your computer's date, and still works if Redis is unavailable but the API is running.

#### Use the shortcuts

Run `svc` from the repository root so Compose finds the project's configuration.

| Command | What it shows |
|---|---|
| `svc` | All created services, including stopped containers and completed initialization jobs |
| `status` | Simulated time, batch completion date, watermark age, row counts and unavailable stores |
| `today` | Current simulated date in `YYYY-MM-DD` format |
| `util` | HTTP status, degraded header and counts of batch/live rows from March 2 through today |
| `util 2026-03-04` | The same summary with a different starting simulated date |
| `pnl V113 2026-03-03` | Fuel cost, net profit and restatement details for that vehicle and date |
| `pnl V113 2026-03-03 60` | Search the last 60 simulated days for that vehicle/date instead of the default 30 |
| `lake ls l/fleet-lake/` | Top-level objects and prefixes in the lake |
| `lake ls l/fleet-lake/landing/expenses/` | Daily partner expense files |
| `lake cat l/fleet-lake/landing/expenses/expenses_2026-03-03.csv` | Contents of one daily expense file |

The example dates assume the default simulation start of March 1, 2026. Wait until the date
you request exists: each simulated day takes five real minutes. Profitability also needs the
batch layer to have processed that date. `pnl` prints nothing if the date is outside the
requested window or has no matching row; `status` shows how far the batch has completed.

### Check processing and apply a corrected expense file

```bash
make dag-check             # DAG availability, import errors and recent runs
make batch-check           # row counts, restatements and revenue reconciliation
make obs-check             # monitoring targets and dashboard provisioning
make alerts                # alert rules and their current state
```

Once a corrected expense file is present in the lake for an already processed date, trigger
that date again through Airflow. The trigger starts an asynchronous run; wait for it to
succeed in Airflow before comparing the result:

```bash
pnl V113 2026-03-03
lake cat l/fleet-lake/landing/expenses/expenses_2026-03-03.csv
make dag-trigger DATE=2026-03-03
make dag-check
# After the run succeeds:
pnl V113 2026-03-03
make batch-check
```

`restatement_count` and `restated_at` identify recomputed profitability rows. The batch
completion watermark should not move backward when an older date is corrected. For missing
completed days, use `make backfill DAYS=3` (adjust the number of days needed).

### Trying things

```bash
# Does the reconciliation actually happen?
curl 'localhost:8000/api/v1/fleet/utilization?from=2026-03-02&to=2026-03-08'

# Which vehicles are becoming unprofitable?
curl 'localhost:8000/api/v1/vehicles/unprofitable?limit=10'

# Graceful degradation: stop a store, the API keeps answering
docker stop fleet-redis
curl -i 'localhost:8000/api/v1/fleet/utilization?from=2026-03-02&to=2026-03-08'   # 200 + X-Data-Degraded
docker compose up -d --force-recreate redis   # NOT `docker start`, see below

# Does the alerting work? (fires in ~71 s, resolves in ~51 s)
make chaos-kill-producer
make alerts
make chaos-heal

# Speed view vs batch view for one simulated date
make reconcile DATE=2026-03-04
```

> **Bring a stopped container back with `docker compose up -d --force-recreate`, not
> `docker start`.** A container restarted with `docker start` can come up without its
> published host ports, and nothing reports it: everything inside the Docker network keeps
> working, so the pipeline looks healthy while connections from the host silently fail.
> `make chaos-heal` does this correctly.

`make ports` lists everything that is listening. `make help` lists every target.

---

## Project reference

### Repository layout

```
src/fleet/
├── common/          config, the shared simulated clock, logging, metrics
├── transforms/      ★ pure DataFrame functions, shared by BOTH layers
├── ingestion/       the two simulated sources
├── speed_layer/     Structured Streaming queries, sinks, progress listener
├── batch_layer/     Spark batch: zone rollup, profitability, validation
├── store/           data access for the lake, the mart and the speed view
└── serving/         FastAPI, including merge.py, the reconciliation
airflow/dags/        the daily reconciliation DAG
observability/       Prometheus scrape config and alert rules, provisioned Grafana
sql/ddl/             the mart schema
scripts/             stack bootstrap, backfill, figure capture
docs/                the report, its diagrams, and the measurement evidence
plan/                the design documents, one per requirement
tests/unit/          418 tests
```

### Development

```bash
make test       # full suite; store-backed tests skip if the stack is down
make lint       # ruff + format check + mypy
make fmt        # auto-format
```

Configuration is typed and centralised in `src/fleet/common/config.py`, one settings class per
layer, loaded from the environment. There are no magic numbers elsewhere in the codebase: a
threshold hard-coded in a `.py` file is treated as a bug. Several settings carry validators that
turn a misconfiguration into a start-up failure rather than a wrong number later.

### Documentation

| | |
|---|---|
| [`docs/report/`](docs/report/) | the submitted report; `make report` builds the PDF |
| [`docs/evidence/`](docs/evidence/) | every measurement quoted in the report, with how it was taken |
| [`docs/diagrams/`](docs/diagrams/) | the report's diagrams as draw.io files, generated by `build_drawio.py`; `make diagrams` exports them to PDF |
| [`plan/`](plan/README.md) | the design documents written before the build |

`make figures` re-captures the report's screenshots from the running stack. Both the diagrams
and the screenshots are regenerated from source, so the report cannot silently drift from the
system it describes.

---

## Known limitations

Stated here rather than left to be discovered; the report discusses each in full.

- **Distributed tracing was not implemented.** Observability rests on structured logging,
  metrics and alerting. A request cannot currently be followed across process boundaries.
- **A rendered PDF daily report was not implemented.** The provisioned dashboards discharge the
  requirement for a consolidated report *or* dashboard.
- **Exactly-once is not achieved.** The system is at-least-once with idempotent sinks, which is
  why every sink overwrites rather than increments.
- **Consumer lag cannot measure the streaming queries.** They checkpoint their own offsets and
  never commit to a consumer group, so lag reads zero whether a query is healthy or dead. The
  liveness signal is the age of the last progress timestamp instead.
- **Three of six streaming queries exceed their 10-second trigger at p95** on this hardware:
  the windowed and stateful ones. The system keeps up on average, not at the tail.
- **The expense file is generated from our own simulated distance**, so the two sources are only
  pseudo-independent and the reconciliation is weaker than it appears.
- **Single broker, `replication.factor=1`, no auth or TLS anywhere.** Demo scale only.

---

## Individual contributions

| Index number | Member | Contribution |
|---|---|---|
| EG/2021/4568 | JATHURSHAN T. | Architecture decision and justification; simulated clock and typed configuration; stateful idle detection; serving layer and the merge boundary |
| EG/2021/4810 | SHAMIL M.K.M. | Storage design: lake layout, star schema, speed-view keys; batch layer: zone rollup and per-vehicle profitability; restatement and idempotency |
| EG/2021/4760 | RIFATH M.F.M. | Simulated sources and ingestion; Avro and Schema Registry; speed-layer queries and sinks; observability: metrics, alert rules and dashboards |

All three contributed to testing, the demonstration, and the report.
