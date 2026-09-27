# Ride-Hailing Fleet Operations - Lambda architecture pipeline
#
# Profiles exist because the host has 15.6 GB and Docker gets ~11 GB.
# See plan/10 section 1.3.

SHELL := /bin/bash
.DEFAULT_GOAL := help
COMPOSE := docker compose

# ---------------------------------------------------------------------------
##@ Setup

.PHONY: setup
setup: ## Check docker, create .env, pull images
	@command -v docker >/dev/null || { echo "docker not found - enable Docker Desktop WSL integration (plan/10 §0.1)"; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "docker daemon unreachable - is Docker Desktop running?"; exit 1; }
	@test -f .env || { cp .env.example .env; echo "created .env from .env.example"; }
	@echo "OK. Next: make up"

.PHONY: venv
venv: ## Create the local dev environment (for tests/linting on the host)
	uv venv --python 3.12 .venv
	uv pip install --python .venv/bin/python -e ".[dev]"
	@echo "OK. Activate with: source .venv/bin/activate"

# ---------------------------------------------------------------------------
##@ Running

.PHONY: up-core
up-core: ## ~5.6 G  pipeline only, Spark in local mode
	COMPOSE_PROFILES= $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait

.PHONY: up
up: ## ~8.2 G  + Kafka UI + Airflow            [DEFAULT]
	COMPOSE_PROFILES=run $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait

.PHONY: up-obs
up-obs: ## ~9.9 G  + Prometheus / Grafana / Alertmanager
	COMPOSE_PROFILES=obs $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait

.PHONY: up-full
up-full: ## Same as up-obs today (tracing was not implemented - see README)
	COMPOSE_PROFILES=full $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait

.PHONY: wait
wait: ## Block until every service reports healthy
	@echo "waiting for services to become healthy..."
	@for i in $$(seq 1 60); do \
	  unhealthy=$$($(COMPOSE) ps --format '{{.Service}} {{.Health}}' 2>/dev/null | awk '$$2!="healthy" && $$2!="" {print $$1}'); \
	  if [ -z "$$unhealthy" ]; then echo "all healthy"; break; fi; \
	  sleep 3; \
	done
	@$(MAKE) --no-print-directory ps

.PHONY: down
down: ## Stop, KEEP data
	$(COMPOSE) --profile "*" down

.PHONY: clean
clean: ## Stop and DELETE all data  <- the normal way to start a run (plan/10 §4.3)
	$(COMPOSE) --profile "*" down -v
	rm -f state/sim_epoch.json
	@echo "wiped. next 'make up' starts a fresh simulation from day 1"

.PHONY: restart-sim
restart-sim: ## Re-anchor the simulated clock (after laptop sleep)
	rm -f state/sim_epoch.json
	$(COMPOSE) up -d --force-recreate init

# ---------------------------------------------------------------------------
##@ Inspecting

.PHONY: ps
ps: ## Status and health of every service
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

.PHONY: logs
logs: ## Follow one service's JSON logs.  make logs SVC=init
	$(COMPOSE) logs -f --tail=100 $(SVC)

.PHONY: mem
mem: ## Container memory against the budget (plan/10 §1.3)
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' \
	  | grep -E 'fleet-|NAME' || true
	@echo ""
	@docker stats --no-stream --format '{{.MemUsage}}' | grep -o '^[0-9.]*[GM]iB' | \
	  awk '/GiB/{s+=$$1} /MiB/{s+=$$1/1024} END{printf "TOTAL: %.2f GiB (host gives docker ~11 GiB)\n", s}'

.PHONY: ports
ports: ## What is listening where
	@echo "  Fleet API    http://localhost:8000/docs      <- the serving layer"
	@echo "  Kafka UI     http://localhost:8080"
	@echo "  Airflow      http://localhost:8082            (admin/admin)"
	@echo "  Schema Reg   http://localhost:8081/subjects"
	@echo "  Kafka        localhost:9092"
	@echo "  MinIO console http://localhost:9001   (fleetadmin/fleetadmin)"
	@echo "  Postgres      localhost:5442          (fleet/fleet, db fleet_mart)"
	@echo "  Redis         localhost:6389"
	@echo ""
	@echo "  -- make up-obs --"
	@echo "  Grafana      http://localhost:3000/d/fleet-pipeline-health  <- the dashboard"
	@echo "  Prometheus   http://localhost:9090/targets"
	@echo "  Pushgateway  http://localhost:9091   (Spark streaming metrics land here)"
	@echo "  Alertmanager http://localhost:9093"
	@echo ""
	@echo "  NOTE: 5442/6389 are deliberate - this machine runs native"
	@echo "        PostgreSQL and Redis on the default 5432/6379."

# ---------------------------------------------------------------------------
##@ Batch layer

.PHONY: backfill
backfill: ## Run the batch layer over the last N complete sim days.  make backfill DAYS=7
	@docker exec fleet-spark-app python /app/scripts/backfill.py --days $${DAYS:-7}

.PHONY: batch-day
batch-day: ## Recompute ONE sim date without moving the watermark.  make batch-day DATE=2026-03-04
	@test -n "$(DATE)" || { echo "usage: make batch-day DATE=YYYY-MM-DD"; exit 2; }
	@docker exec fleet-spark-app python /app/scripts/backfill.py --from $(DATE) --to $(DATE)

.PHONY: batch-check
batch-check: ## The numbers that prove the backfill did what it claims
	@docker exec -e PGPASSWORD=fleet fleet-postgres-mart psql -U fleet -d fleet_mart -tA -c "\
	  SELECT 'pnl rows          : ' || count(*) FROM mart.fact_vehicle_daily_pnl \
	  UNION ALL SELECT 'distinct sim_dates: ' || count(DISTINCT sim_date) FROM mart.fact_vehicle_daily_pnl \
	  UNION ALL SELECT 'restated rows     : ' || count(*) FROM mart.fact_vehicle_daily_pnl WHERE restatement_count > 0 \
	  UNION ALL SELECT 'zone-hourly rows  : ' || count(*) FROM mart.fact_zone_hourly \
	  UNION ALL SELECT 'watermark         : ' || batch_complete_thru FROM mart.batch_high_water_mark;"
	@echo ""
	@echo "classification by sim_date:"
	@docker exec -e PGPASSWORD=fleet fleet-postgres-mart psql -U fleet -d fleet_mart -c "\
	  SELECT sim_date, classification, count(*) FROM mart.fact_vehicle_daily_pnl \
	  GROUP BY 1,2 ORDER BY 1,2;"
	@echo "revenue vs zone earnings (two independent paths to one number - must match):"
	@docker exec -e PGPASSWORD=fleet fleet-postgres-mart psql -U fleet -d fleet_mart -c "\
	  SELECT p.sim_date, round(p.revenue,2) AS pnl_revenue, round(z.earnings,2) AS zone_earnings, \
	         round(p.revenue - z.earnings, 2) AS delta \
	    FROM (SELECT sim_date, sum(revenue) revenue FROM mart.fact_vehicle_daily_pnl GROUP BY 1) p \
	    FULL JOIN (SELECT sim_date, sum(earnings) earnings FROM mart.fact_zone_hourly GROUP BY 1) z \
	      USING (sim_date) ORDER BY 1;"

# ---------------------------------------------------------------------------
.PHONY: dag-check
dag-check: ## Is the reconciliation DAG loaded, unpaused and succeeding?
	@docker exec fleet-airflow-scheduler airflow dags list 2>/dev/null \
	  | grep -E 'dag_id|fleet_' || echo "  scheduler not running - try 'make up'"
	@echo ""
	@echo "--- import errors (should be empty) ---"
	@docker exec fleet-airflow-scheduler airflow dags list-import-errors 2>/dev/null || true
	@echo ""
	@echo "--- last 5 runs ---"
	@docker exec fleet-airflow-scheduler \
	  airflow dags list-runs -d fleet_daily_reconciliation --no-backfill 2>/dev/null \
	  | head -9 || true

.PHONY: dag-trigger
dag-trigger: ## Run the DAG now for one sim date.  make dag-trigger DATE=2026-03-04
	@test -n "$(DATE)" || { echo "usage: make dag-trigger DATE=YYYY-MM-DD"; exit 2; }
	@docker exec fleet-airflow-scheduler airflow dags trigger \
	  fleet_daily_reconciliation --conf '{"sim_date":"$(DATE)"}'
	@echo "triggered as a RESTATEMENT - the watermark must not move to $(DATE)"

.PHONY: reconcile
reconcile: ## Speed-vs-batch divergence for a sim date.  make reconcile DATE=2026-03-04
	@test -n "$(DATE)" || { echo "usage: make reconcile DATE=YYYY-MM-DD"; exit 2; }
	@docker exec -e PGPASSWORD=fleet fleet-postgres-mart psql -U fleet -d fleet_mart -c "\
	  SELECT metric, count(*) AS zones, \
	         round(avg(abs(delta_pct)),2) AS avg_abs_delta_pct, \
	         round(max(abs(delta_pct)),2) AS worst_pct \
	    FROM mart.reconciliation_delta WHERE sim_date='$(DATE)' \
	   GROUP BY metric ORDER BY metric;"
	@echo "plan/07 section 5.4 expects 1-3% on active_vehicles, up to 8% on earnings."
	@echo "Exactly 0.00 would mean one side is reading the other, not computing it."

# ---------------------------------------------------------------------------
##@ Chaos - prove the alerts actually fire

.PHONY: chaos-kill-producer
chaos-kill-producer: ## Stop the telemetry producer. NoTelemetryIngested should fire in ~1 min.
	@docker stop fleet-telemetry-producer >/dev/null
	@echo "producer stopped at $$(date -u +%H:%M:%S)"
	@echo "watch:  http://localhost:9093          (alertmanager)"
	@echo "        http://localhost:9090/alerts   (pending -> firing)"
	@echo "restore with: make chaos-heal   (NOT 'docker start' - see that target)"

.PHONY: chaos-heal
chaos-heal: ## Restart whatever chaos stopped, and watch the alert resolve
	@# `docker compose up -d`, NOT `docker start`.
	@#
	@# A container brought back with `docker start` can come up WITHOUT its published
	@# host ports. Observed here: after stopping and starting postgres-mart and redis
	@# for the degradation demo, `docker port` listed nothing for either, while every
	@# other container kept its bindings. Nothing inside the Docker network noticed -
	@# the API talks to postgres-mart:5432 over the network and stayed healthy - so
	@# the pipeline looked fine and only host-originating connections broke. The
	@# symptom was six integration tests timing out against localhost:5442 while
	@# `make psql` (which uses docker exec) worked perfectly.
	@#
	@# `up -d` reconciles the container against the compose file and restores them.
	@RESUME=1 $(COMPOSE) up -d telemetry-producer redis postgres-mart >/dev/null 2>&1 || true
	@echo "restored at $$(date -u +%H:%M:%S) - alerts should resolve within ~1 min"
	@echo "host ports: $$(docker port fleet-postgres-mart | head -1 || echo 'NONE - run make up')"

.PHONY: alerts
alerts: ## Current alert state, and how long each has been firing
	@curl -s localhost:9090/api/v1/rules | python3 -c "import json,sys; gs=json.load(sys.stdin)['data']['groups']; rs=[r for g in gs for r in g['rules'] if r['type']=='alerting']; print(f'{len(rs)} rules loaded'); [print('  {:<26} {:<8} {}'.format(r['name'], r['state'], r['labels'].get('severity',''))) for r in rs]" || echo "  prometheus not reachable"
	@echo ""
	@curl -s localhost:9093/api/v2/alerts | python3 -c "import json,sys; a=json.load(sys.stdin); print(f'{len(a)} alert(s) in alertmanager'); [print('  ', x['labels'].get('alertname'), '->', x['status']['state']) for x in a]" 2>/dev/null || echo "  alertmanager has no active alerts"

.PHONY: report
report: ## Build the LaTeX report PDF
	@$(MAKE) --no-print-directory -C docs/report main.pdf

.PHONY: report-check
report-check: ## Build the report and refuse unfilled cover-page placeholders
	@$(MAKE) --no-print-directory -C docs/report check

.PHONY: diagrams
diagrams: ## Export the draw.io diagrams (D1-D6) to PDF
	@$(MAKE) --no-print-directory -C docs/diagrams all

.PHONY: figures
figures: ## Re-capture the report's screenshots from the running stack
	@.venv/bin/python scripts/capture_figures.py

.PHONY: obs-check
obs-check: ## Are the observability targets actually being scraped?
	@echo "--- prometheus targets ---"
	@curl -s localhost:9090/api/v1/targets | python3 -c "import json,sys; ts=json.load(sys.stdin)['data']['activeTargets']; [print('  {:<15} {:<6} {}'.format(t['labels'].get('job','?'), t['health'], t['scrapeUrl'])) for t in sorted(ts, key=lambda x: x['labels'].get('job',''))]" || echo "  prometheus not reachable - is the obs profile up?"
	@echo ""
	@echo "--- spark streaming metrics in the pushgateway ---"
	@echo "  $$(curl -s localhost:9091/metrics | grep -c '^spark_streaming_') spark_streaming_* series (0 means the listener is NOT pushing)"
	@echo "  $$(curl -s localhost:9091/metrics | grep -oE 'query="[a-z_]+"' | sort -u | wc -l) distinct queries reporting"
	@echo ""
	@echo "--- grafana ---"
	@if curl -sf -o /dev/null localhost:3000/api/dashboards/uid/fleet-pipeline-health; then \
	  echo "  dashboard provisioned: http://localhost:3000/d/fleet-pipeline-health"; \
	else \
	  echo "  dashboard NOT provisioned"; \
	fi
	@echo "  datasources: $$(curl -s localhost:3000/api/datasources | grep -oE '"name":"[^"]*"' | sed 's/"name"://' | tr '\n' ' ')"

.PHONY: topics
topics: ## List topics with partition counts and cleanup policy
	@docker exec fleet-kafka kafka-topics --bootstrap-server localhost:9092 --list
	@echo ""
	@for t in $$(docker exec fleet-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -v '^__'); do \
	  echo "--- $$t"; \
	  docker exec fleet-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $$t | head -1; \
	done

.PHONY: schemas
schemas: ## List registered schema subjects
	@curl -s http://localhost:8081/subjects | python3 -m json.tool

.PHONY: simclock
simclock: ## Show the current simulated time
	@cat state/sim_epoch.json 2>/dev/null | python3 -m json.tool || echo "no anchor yet - run make up"

# ---------------------------------------------------------------------------
##@ Quality

.PHONY: test
test: ## Full suite. Pure tests need no docker; store-backed ones skip without it
	.venv/bin/pytest tests/unit -v

.PHONY: lint
lint: ## ruff + format check + mypy
	.venv/bin/ruff check src tests scripts
	.venv/bin/ruff format --check src tests scripts
	.venv/bin/mypy src/fleet/common/simclock.py

.PHONY: fmt
fmt: ## Auto-format
	.venv/bin/ruff format src tests scripts
	.venv/bin/ruff check --fix src tests scripts

# ---------------------------------------------------------------------------
##@ Help

.PHONY: help
help:
	@awk 'BEGIN {FS=":.*##"; printf "\nUsage: make <target>\n"} \
	  /^[a-zA-Z_-]+:.*?##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2} \
	  /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0,5)}' $(MAKEFILE_LIST)
	@echo ""

.PHONY: psql
psql: ## psql into the mart
	@docker exec -it fleet-postgres-mart psql -U fleet -d fleet_mart

.PHONY: redis-cli
redis-cli: ## redis-cli into the speed view
	@docker exec -it fleet-redis redis-cli

.PHONY: verify-s3a
verify-s3a: ## Prove Spark can round-trip Parquet through MinIO
	@docker compose run --rm --no-deps spark-app python /app/scripts/verify_s3a.py

.PHONY: verify-kafka
verify-kafka: ## Prove Spark can decode the producer's Confluent-framed Avro
	@docker compose run --rm --no-deps spark-app python /app/scripts/verify_kafka_avro.py

.PHONY: restart-svc
restart-svc: ## Recreate one service without re-running init.  make restart-svc SVC=telemetry-producer
	@test -n "$(SVC)" || { echo "usage: make restart-svc SVC=<service>"; exit 1; }
	docker compose up -d --build --no-deps --force-recreate $(SVC)
	@echo "NOTE: --no-deps is required. Without it compose re-runs the init container,"
	@echo "      which refuses to start over an existing run (plan/10 §4.3)."
