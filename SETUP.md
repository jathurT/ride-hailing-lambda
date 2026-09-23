# How to Run the Project — Quick Setup Guide

Follow these steps on your machine to get the full pipeline running.

---

## 1. Prerequisites

You need these installed before anything else:

- **Docker Desktop** (running)
- **Git**
- **uv** (Python package manager — only needed if you want to run tests locally)

On Windows, make sure Docker Desktop has **WSL Integration enabled:**
Settings → Resources → WSL Integration → turn it on.

---

## 2. Clone the Repository

```bash
git clone https://github.com/jathurT/ride-hailing-lambda.git
cd ride-hailing-lambda
```

---

## 3. Create the .env File

The project needs a `.env` file with all the config values.
A ready-made example is included. Just copy it:

```bash
cp .env.example .env
```

You do not need to change anything in `.env` — the defaults work out of the box.

---

## 4. Start the Stack

Run this one command:

```bash
make up
```

This starts everything — Kafka, Spark, Redis, PostgreSQL, MinIO, Airflow, Kafka UI.
It takes 2-3 minutes the first time (Docker pulls the images).

> IMPORTANT: Always start with a clean state. If you have run this before, do:
>
> ```bash
> make clean && make up
> ```
>
> Never restart over old data — the simulation starts from day 1 every time and old data will corrupt the results.

---

## 5. Open the UIs

Once the stack is up, open these in your browser:

| URL                            | What you see                               |
| ------------------------------ | ------------------------------------------ |
| http://localhost:8080          | Kafka UI — topics, messages, consumer lag  |
| http://localhost:8081/subjects | Schema Registry — Avro schemas             |
| http://localhost:8082          | Airflow — batch DAG runs                   |
| http://localhost:8000/docs     | FastAPI — the serving layer API            |
| http://localhost:3000          | Grafana — live dashboards                  |
| http://localhost:9001          | MinIO — the master dataset (Parquet files) |
| http://localhost:9090          | Prometheus — raw metrics                   |

Default Grafana login: **admin / admin**
Default MinIO login: **fleetadmin / fleetadmin**

---

## 6. Wait and Watch

The simulation runs automatically. Every 5 real minutes = 1 simulated day.

**After 5 minutes:** Kafka UI shows telemetry flowing across 6 partitions.
**After 10 minutes:** The first expense CSV lands. Airflow DAG turns green.
**After 15 minutes:** Grafana Fleet Operations dashboard shows live data.
**After 35 minutes:** 7 simulated days have passed — enough to see vehicles classified as WATCH or UNPROFITABLE in the Batch Reconciliation dashboard.

To check the simulated clock:

```bash
make simclock
```

---

## 7. Stop the Stack

```bash
make down     # stops everything, keeps your data
make clean    # stops everything and wipes all data (use before next run)
```

---

## 8. Run Tests (No Docker Needed)

If you want to run the unit tests locally without Docker:

```bash
make venv
make test
```

All 5 tests should pass.

---

## 9. Common Issues

**Docker says "daemon unreachable":**
Open Docker Desktop and wait for it to fully start before running `make up`.

**Port already in use:**
Something else on your machine is using that port. Check `docker ps` to see if an old container is still running. Run `make clean` first.

**Airflow DAG not showing:**
Wait 2-3 minutes after `make up`. Airflow takes longer to start than the other services.

**Grafana shows "No data":**
The dashboards need at least 10-15 real minutes of data to show anything. Wait for the second or third simulated day.

---

## 10. Minimal Start (Low RAM)

If your machine has less than 10 GB free RAM, use:

```bash
make up-core
```

This starts just the pipeline (no Kafka UI, no Airflow UI). Everything still works, you just won't have the browser UIs. Uses ~5.6 GB instead of ~8.2 GB.

---

That is all. The whole pipeline starts from a single `make up`.
