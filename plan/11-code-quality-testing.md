# 11 — Code Quality, Testing & Repository Structure

> **Rubric weight: 5 marks** — *"readability, modularity, README/setup instructions, configuration management, and reproducibility."*
>
> Small weight, but it is the cheapest 5 marks in the rubric and it is also what makes the other 95 marks defensible in a viva.

---

## 1. Repository structure

```
ride-hailing-lambda/
├── README.md
├── Makefile
├── docker-compose.yml
├── docker-compose.observability.yml
├── .env.example
├── .gitignore
├── pyproject.toml                  # uv / hatch; ruff + mypy + pytest config
├── plan/                           # ← this planning set
├── docs/
│   ├── adr/                        # architecture decision records
│   │   ├── 001-lambda-over-kappa.md
│   │   ├── 002-vehicle-id-partition-key.md
│   │   ├── 003-spark-over-storm.md
│   │   ├── 004-redis-speed-layer-store.md
│   │   └── 005-high-water-mark-merge.md
│   ├── diagrams/                   # draw.io sources + exported SVG/PDF
│   └── report/                     # LaTeX/Markdown source of the final report
│
├── src/fleet/                      # ← layered by ARCHITECTURE LAYER
│   ├── common/
│   │   ├── config.py               # pydantic-settings
│   │   ├── simclock.py             # THE shared simulated clock
│   │   ├── logging.py              # structlog JSON config
│   │   ├── metrics.py              # Prometheus registry + helpers
│   │   ├── tracing.py              # OTel bootstrap
│   │   ├── kafka_client.py
│   │   ├── serialization.py        # Avro
│   │   ├── dlq.py
│   │   ├── zones.py
│   │   └── schemas/*.avsc
│   │
│   ├── transforms/                 # ★ PURE FUNCTIONS — SHARED BY BOTH LAYERS ★
│   │   ├── validate.py
│   │   ├── enrich.py
│   │   ├── utilization.py
│   │   ├── geo.py                  # haversine as a Column expression
│   │   └── profitability.py
│   │
│   ├── ingestion/
│   │   ├── telemetry_producer.py
│   │   ├── expense_dropper.py
│   │   ├── registry_producer.py
│   │   └── generators/             # vehicle state machine, demand curve, fares
│   │
│   ├── speed_layer/
│   │   ├── streaming_job.py        # the 3 queries
│   │   ├── idle_detector.py        # flatMapGroupsWithState
│   │   ├── listener.py             # StreamingQueryListener → Pushgateway
│   │   └── sinks/{redis_sink,lake_sink,alert_sink}.py
│   │
│   ├── batch_layer/
│   │   ├── daily_profitability.py
│   │   ├── dq_checks.py
│   │   ├── report_render.py
│   │   └── templates/daily_report.html.j2
│   │
│   ├── serving/
│   │   ├── main.py
│   │   ├── merge.py                # ★ THE RECONCILIATION ★
│   │   ├── routers/{fleet,vehicles,alerts,reports,pipeline,health}.py
│   │   ├── models.py               # pydantic response models
│   │   └── deps.py
│   └── cli.py                      # typer entry point
│
├── airflow/dags/
├── sql/ddl/ + sql/seed/
├── observability/
│   ├── prometheus/{prometheus.yml,alerts.yml}
│   ├── alertmanager/alertmanager.yml
│   ├── grafana/provisioning/{datasources,dashboards}/
│   └── otel/otel-collector-config.yaml
├── tests/{unit,integration,contract,fixtures}/
├── scripts/                        # create_topics, register_schemas, smoke_test, chaos
├── docker/{producer,spark-app,api,init}/Dockerfile
└── .github/workflows/ci.yml
```

**The structure encodes the architecture.** `speed_layer/`, `batch_layer/` and `serving/` are the three layers the module names; `transforms/` sits above them as the shared logic. A marker opening this tree sees the Lambda architecture before reading a line of code — that is deliberate.

---

## 2. The modularity rule that matters

**`transforms/` must not import Spark streaming, Redis, Postgres, Kafka or MinIO.** Only `pyspark.sql.functions` and `DataFrame`. Enforced by a test:

```python
def test_transforms_have_no_io_dependencies():
    for module in walk_modules("src/fleet/transforms"):
        imports = collect_imports(module)
        forbidden = {"redis", "psycopg", "boto3", "confluent_kafka", "pyspark.sql.streaming"}
        assert not (imports & forbidden), f"{module} must stay I/O-free"
```

This is what makes the `01 §2.3` mitigation real rather than aspirational, and it is the single best thing to point at when asked *"how did you handle Lambda's multiple-codebases problem?"*

---

## 3. Testing strategy

### 3.1 The pyramid

| Level | Count | Runtime | Needs |
|---|---|---|---|
| **Unit — pure Python** | ~50 | < 2 s | nothing |
| **Unit — local Spark** | ~15 | ~40 s | `SparkSession(local[2])` |
| **Contract** | ~6 | < 5 s | nothing (schemas from disk) |
| **Integration / smoke** | ~8 | ~2 min | the compose stack up |

### 3.2 What is tested where

Full per-document test tables live in `03 §6`, `05 §8`, `06 §8`, `07 §7`, `08 §7`, `09 §8`. The three tests that carry the most argumentative weight:

1. **`test_merge_boundary.py`** — table-driven over every boundary case. Proves the reconciliation contract.
2. **`test_shared_transforms.py`** — the same function gives identical results in the streaming and batch paths. Proves the single-codebase claim.
3. **`test_watermark_safety.py`** — fails CI if the simulated-time speed-up and the watermark are changed inconsistently. Proves the clock coupling was understood.

### 3.3 Spark test fixture

```python
@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]").appName("tests")
            .config("spark.sql.shuffle.partitions", "2")   # 200 default = slow tests
            .config("spark.ui.enabled", "false")
            .getOrCreate())
```

Streaming logic is tested through the **same pure transform functions** on static DataFrames, so almost nothing needs `MemoryStream`. The one exception is `test_windowing.py`, which verifies watermark drop behaviour.

### 3.4 Integration smoke test

`scripts/smoke_test.py`, run by `make test-int` against a live stack:

1. Produce 500 synthetic telemetry events with a known signature
2. Poll `GET /api/v1/fleet/live` until they appear (timeout 60 s)
3. Assert Redis keys exist with sane TTLs
4. Assert Parquet files landed in MinIO under the right partition
5. Trigger the reconciliation DAG, poll for success
6. Assert `fact_vehicle_daily_pnl` has rows and the watermark advanced
7. Assert the report PDF exists in MinIO
8. Assert `/metrics` exposes the expected metric names

**Testcontainers was considered and rejected**: it would spin up Kafka/Postgres per test run, which on WSL2 costs minutes per invocation. A compose-based smoke test gives the same confidence for this project's scale. Noted in the report as a conscious trade-off.

---

## 4. Code standards

| Concern | Tool / rule |
|---|---|
| Format + lint | `ruff` and `ruff format` (line length 100) |
| Types | `mypy --strict` on `transforms/`, `serving/merge.py`, `common/simclock.py`; permissive elsewhere (Spark's stubs are poor) |
| Docstrings | Every public function: what it does, **why it exists**, and units (`sim_minutes` vs `real_seconds` — this project has a genuine units-confusion hazard) |
| Magic numbers | Banned. Every threshold comes from `config.py`. |
| Comments | Reserved for *why*, not *what*. The `HSET`-not-`HINCRBY` and `collect()`-is-safe-here comments are the model. |
| Errors | Typed exceptions in `common/errors.py`; never a bare `except:` |
| Imports | Absolute, `src` layout |

---

## 5. CI (GitHub Actions)

```yaml
jobs:
  quality:   ruff check · ruff format --check · mypy
  test:      pytest tests/unit tests/contract --cov=src/fleet
             coverage gate: 85% on transforms/ + serving/merge.py + common/simclock.py
  validate:  promtool check rules · python -c "import DAGs" · docker compose config
```

Deliberately **no Spark or compose in CI** — too slow and too flaky on hosted runners. Integration is run locally via `make test-int` and evidenced by a screenshot in the report. Stating that trade-off is better than a CI file that silently skips the real tests.

The coverage gate targets only the pure-logic modules. A global coverage number would be gamed by trivial tests on I/O wrappers; gating the modules that carry the architecture argument is more honest and more useful.

---

## 6. Git

- Branches: `main` + short-lived `feat/*`
- Conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`)
- **ADRs in `docs/adr/`** — five short records capturing each major decision at the moment it was made. These are the raw material for report §3 and §5, and they are strong viva evidence that decisions were reasoned about in advance rather than rationalised afterwards.
- Tag `v1.0-submission` at hand-in
- `.gitignore`: `.env`, `state/`, `__pycache__`, `.pytest_cache`, `spark-warehouse/`, `logs/`, `*.pdf` under `reports/` (generated artefacts are not source)

---

## 7. Structural differences from the sibling project

The hospital project (`../hospital-vitals-kappa`) is a separate group's submission and is deliberately different in shape, because its architecture is different:

| | This project (Lambda) | Hospital project (Kappa) |
|---|---|---|
| Package layout | `src/fleet/` (src layout) | `ward/` (flat package) |
| Directory taxonomy | By **architecture layer** (`speed_layer/`, `batch_layer/`, `serving/`) | By **pipeline stage** (`producers/`, `stream/`, `store/`, `api/`, `replay/`) |
| Compose file | `docker-compose.yml` | `compose.yaml` |
| Host ports | 8000/8080/8082/8090/9000/9090/3000/16686 | 8100/8180/8182/8190/9100/9190/3100/16786 |
| Orchestration dir | `airflow/` | `orchestration/` |
| Domain logic home | `transforms/` (shared by two layers) | `clinical/` (one rule, one path) |
| Serving store | Redis + PostgreSQL | Cassandra |

Both use Python, `confluent-kafka`, PySpark and `structlog` — because those are the *right* choices for both, and deliberately using a worse tool in one repo just to look different would be a bad decision that a viva would expose. The differentiation that matters is architectural, and it is total.
