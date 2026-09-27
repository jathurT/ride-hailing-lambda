"""Capture the report's screenshots from the running stack.

Screenshots are evidence, and evidence that cannot be regenerated is not much use:
if a figure is questioned during a viva, "run this script" is a better answer than
"we took it on Tuesday". Every shot here is reproducible from a running stack, so the
report's figures and the system it describes cannot silently drift apart.

Needs the `obs` profile up:

    make up-obs && python scripts/capture_figures.py [--only NAME]

Terminal images are the real output of the listed commands, run at capture time.
`terminal-api-merged` reads the date range from FROM and TO in the environment.

Grafana runs with anonymous admin, so the dashboards need no login. Airflow does; its
credentials come from the compose file and are the demo defaults, not secrets.
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from dataclasses import dataclass, field
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "docs/report/figures"

# 1920x1080 per plan/12 §3 - figures must stay legible when scaled into a PDF column.
VIEWPORT = {"width": 1920, "height": 1080}


@dataclass(frozen=True)
class Shot:
    name: str
    url: str
    proves: str
    wait_ms: int = 6000
    full_page: bool = False
    # Shorter than the default for pages whose content ends early (no white half page).
    height: int = VIEWPORT["height"]
    # A selector to wait for before shooting. Without one, dashboards are captured
    # mid-render and the panels come out as spinners - which looks exactly like a
    # broken dashboard in a report.
    wait_for: str | None = None
    auth: tuple[str, str] | None = None
    # Text that MUST appear on the captured page. Without it, a login form, an error
    # page or a spinner is indistinguishable from the real thing and the script
    # reports success either way - which it did, for an Airflow login screen.
    expect: str | None = None
    # Text that must NOT appear. Catches the unauthenticated case specifically.
    reject: tuple[str, ...] = ("Enter your login and password", "Invalid login")
    steps: list[str] = field(default_factory=list)


GRAFANA = "http://localhost:3000"
# A dashboard whose datasource plugin failed to load still renders every panel frame,
# so the page looks like a dashboard. Seen live: an unpinned plugin upgrade left the
# two business dashboards all "No data" and the capture reported ok.
GRAFANA_REJECT = ("No data", "plugin failed", "Enter your login and password")
AIRFLOW = ("admin", "admin")
SHOTS = [
    Shot(
        "grafana-fleet-operations",
        f"{GRAFANA}/d/fleet-operations/?kiosk&refresh=10s",
        "the live half of the business question: utilization and earnings by area",
        wait_ms=12000,
        wait_for="div.react-grid-item",
        reject=GRAFANA_REJECT,
    ),
    Shot(
        "grafana-batch-reconciliation",
        f"{GRAFANA}/d/fleet-batch-reconciliation/?kiosk",
        "the daily half: which vehicles are becoming unprofitable",
        wait_ms=12000,
        wait_for="div.react-grid-item",
        reject=GRAFANA_REJECT,
    ),
    Shot(
        "grafana-pipeline-health",
        f"{GRAFANA}/d/fleet-pipeline-health/?kiosk&from=now-30m&to=now",
        "observability across ingest, process, store and serve",
        wait_ms=14000,
        wait_for="div.react-grid-item",
        reject=GRAFANA_REJECT,
    ),
    Shot(
        "grafana-pipeline-outage",
        f"{GRAFANA}/d/fleet-pipeline-health/?kiosk&from=now-30m&to=now",
        "the same dashboard during the producer outage (take it during the chaos test)",
        wait_ms=14000,
        wait_for="div.react-grid-item",
        reject=("plugin failed",),  # panels go empty on purpose here
    ),
    Shot(
        "alertmanager",
        "http://localhost:9093/#/alerts",
        "the alert routed (take it during the chaos test)",
        wait_ms=5000,
        height=560,
    ),
    Shot(
        "prometheus-alerts",
        "http://localhost:9090/alerts?search=",
        "the rule set as Prometheus evaluates it",
        wait_ms=4000,
        expect="NoTelemetryIngested",
        height=520,
    ),
    Shot(
        "prometheus-targets",
        "http://localhost:9090/targets",
        "every scrape target up",
        wait_ms=4000,
        expect="streaming-job",
    ),
    Shot(
        "api-docs",
        "http://localhost:8000/docs",
        "the serving-layer contract",
        wait_ms=5000,
        expect="utilization",
    ),
    Shot(
        "kafka-topics",
        "http://localhost:8080/ui/clusters/fleet/all-topics?perPage=25",
        "topics with message counts",
        wait_ms=7000,
        expect="fleet.telemetry",
    ),
    Shot(
        "kafka-telemetry-messages",
        "http://localhost:8080/ui/clusters/fleet/all-topics/fleet.telemetry.v1/messages",
        "telemetry decoded from Avro through the Schema Registry, keyed by vehicle",
        wait_ms=8000,
        expect="vehicle_id",
        steps=["table tbody tr:first-child td:first-child"],
    ),
    Shot(
        "kafka-dlq-messages",
        "http://localhost:8080/ui/clusters/fleet/all-topics/fleet.telemetry.dlq/messages",
        "rejected events with their reason",
        wait_ms=8000,
        steps=["table tbody tr:first-child td:first-child"],
    ),
    Shot(
        "kafka-schemas",
        "http://localhost:8080/ui/clusters/fleet/schemas",
        "the Avro contracts",
        wait_ms=6000,
        expect="fleet.telemetry.v1-value",
    ),
    Shot(
        "spark-streaming",
        "http://localhost:4040/StreamingQuery/",
        "the six Structured Streaming queries of the speed layer",
        wait_ms=4000,
        expect="q_master",
        height=500,
    ),
    Shot(
        "airflow-dags",
        "http://localhost:8082/home",
        "the reconciliation DAG, loaded and scheduled",
        wait_ms=6000,
        auth=AIRFLOW,
        expect="fleet_daily_reconciliation",
    ),
    Shot(
        "airflow-grid",
        "http://localhost:8082/dags/fleet_daily_reconciliation/grid",
        "the batch layer running, one column per simulated day",
        wait_ms=9000,
        auth=AIRFLOW,
        expect="fleet_daily_reconciliation",
    ),
    Shot(
        "airflow-graph",
        "http://localhost:8082/dags/fleet_daily_reconciliation/grid?tab=graph",
        "the DAG shape, including the validation branch",
        wait_ms=9000,
        auth=AIRFLOW,
        expect="fleet_daily_reconciliation",
    ),
    Shot(
        "minio-lake",
        "http://localhost:9001/browser/fleet-lake/raw%2Ftelemetry%2F",
        "the master dataset on object storage",
        wait_ms=7000,
        auth=("fleetadmin", "fleetadmin"),
        expect="sim_date=",
        height=640,
    ),
]

# Real command output, rendered as a terminal image. Each runs at capture time.
PSQL = "docker exec -e PGPASSWORD=fleet fleet-postgres-mart psql -U fleet -d fleet_mart -c"
# (title, command run, command shown). The shown command is a short equivalent, so a
# long one-liner does not stretch the image until the output is unreadable in print.
TERMINALS: dict[str, tuple[str, ...]] = {
    "terminal-postgres-pnl": (
        "psql: the five least profitable vehicles on the latest batch day",
        f'{PSQL} "SELECT sim_date, vehicle_id, trips, round(revenue,2) AS revenue, '
        "round(fuel_cost+maintenance_cost,2) AS costs, round(net_profit,2) AS net_profit, "
        "classification FROM mart.fact_vehicle_daily_pnl WHERE sim_date = "
        '(SELECT max(sim_date) FROM mart.fact_vehicle_daily_pnl) ORDER BY net_profit LIMIT 5;"',
        "psql fleet_mart: the five lowest net_profit rows on the latest sim_date",
    ),
    "terminal-redis-speed-view": (
        "redis-cli: one zone in the speed view",
        'docker exec fleet-redis sh -c \'redis-cli --scan --pattern "fleet:zone:*" | head -5; '
        'echo; Z=$(redis-cli --scan --pattern "fleet:zone:*" | head -1); echo HGETALL $Z; '
        "redis-cli HGETALL $Z | paste - -'",
        "redis-cli --scan --pattern 'fleet:zone:*'; redis-cli HGETALL <first zone>",
    ),
    "terminal-api-merged": (
        "curl: the merged answer, with where each day came from",
        "curl -s 'http://localhost:8000/api/v1/fleet/utilization?from=${FROM}&to=${TO}' "
        '| python3 -c "import json, sys, collections; d = json.load(sys.stdin); '
        "[print(k + ':', d[k]) for k in ('batch_complete_thru', 'consistency', "
        "'uncovered_dates', 'degraded', 'row_count')]; "
        "c = collections.Counter((r['sim_date'], r['source'], r['exact']) for r in d['rows']); "
        "[print(f'  {k[0]}  source={k[1]:<5}  exact={k[2]!s:<5}  zones={n}') "
        'for k, n in sorted(c.items())]"',
        "curl -s '/api/v1/fleet/utilization?from=${FROM}&to=${TO}'  (rows counted per date and source)",
    ),
    "terminal-api-degraded": (
        "curl: the same request with Redis stopped",
        "curl -si 'http://localhost:8000/api/v1/fleet/utilization?from=${FROM}&to=${TO}' "
        "| tr -d '\\r' | sed -n '1p;/^x-data/Ip;/^$/q'; "
        "curl -s 'http://localhost:8000/api/v1/fleet/utilization?from=${FROM}&to=${TO}' "
        '| python3 -c "import json,sys; d=json.load(sys.stdin); '
        "[print(k + ':', v) for k, v in d.items() if k not in ('rows', 'as_of_sim')]\"",
        "curl -si '/api/v1/fleet/utilization?from=${FROM}&to=${TO}'  (status, header, envelope)",
    ),
    "terminal-batch-check": (
        "make batch-check: two independent paths to the same revenue",
        "make -s batch-check",
    ),
}


def terminal_html(title: str, command: str, output: str) -> str:
    import html

    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
      body {{ margin:0; background:#1e1e1e; }}
      .t {{ display:inline-block; font: 15px/1.45 'DejaVu Sans Mono', monospace; color:#e6e6e6;
            padding:18px 22px; white-space:pre; }}
      .p {{ color:#7ec699; }} .c {{ color:#9aa5b1; }}
    </style></head><body><div class="t"><span class="c">{html.escape(title)}</span>
<span class="p">$</span> {html.escape(command)}

{html.escape(output)}</div></body></html>"""


def capture_terminal(name: str, browser) -> tuple[str, str]:
    import os
    import subprocess

    title, command, *shown = TERMINALS[name]
    command = os.path.expandvars(command)
    display = os.path.expandvars(shown[0]) if shown else command
    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=Path(__file__).resolve().parents[1],
    )
    output = (result.stdout + result.stderr).rstrip()
    if result.returncode != 0 or not output:
        return "FAILED", f"exit {result.returncode}: {output[:120]}"
    ctx = browser.new_context(viewport=VIEWPORT, device_scale_factor=2)
    page = ctx.new_page()
    try:
        page.set_content(terminal_html(title, display, output))
        path = OUT / f"{name}.png"
        page.locator(".t").screenshot(path=str(path))
        return "ok", f"{path.name}"
    finally:
        ctx.close()


def capture(shot: Shot, browser) -> tuple[str, str]:
    from playwright.sync_api import TimeoutError as PWTimeout

    viewport = {"width": VIEWPORT["width"], "height": shot.height}
    ctx = browser.new_context(viewport=viewport, device_scale_factor=2)
    page = ctx.new_page()
    try:
        page.goto(shot.url, wait_until="domcontentloaded", timeout=45_000)

        if shot.auth:
            # Both Airflow and MinIO present a login form. Fill it if it is there and
            # carry on if it is not - a session may already be established.
            #
            # NOT `wait_for_load_state("networkidle")`: both UIs poll continuously so
            # networkidle never arrives, the wait times out, and because the timeout
            # was caught the navigation BACK to the target URL was skipped. The result
            # was a screenshot of the login form reported as "ok", which is why the
            # `expect`/`reject` checks below exist.
            try:
                user = page.locator(
                    "input[name='username'], input#accessKey, input[id='username']"
                ).first
                user.wait_for(timeout=4000)
                user.fill(shot.auth[0])
                page.locator(
                    "input[name='password'], input#secretKey, input[type='password']"
                ).first.fill(shot.auth[1])
                page.keyboard.press("Enter")
                page.wait_for_timeout(4000)
            except PWTimeout:
                pass
            # Always navigate to the target, whether or not a form was present.
            page.goto(shot.url, wait_until="domcontentloaded", timeout=45_000)

        if shot.wait_for:
            # Missing the selector is not fatal: a panel that never rendered is
            # itself worth seeing in the screenshot, and the settle below still runs.
            with contextlib.suppress(PWTimeout):
                page.locator(shot.wait_for).first.wait_for(timeout=25_000)

        # Dashboards keep animating; a fixed settle beats networkidle, which never
        # arrives on a page that polls every few seconds.
        page.wait_for_timeout(shot.wait_ms)

        # Clicks to make before the shot, such as opening one message so its decoded
        # fields are readable instead of cut off in a table cell.
        for selector in shot.steps:
            page.locator(selector).first.click(timeout=10_000)
            page.wait_for_timeout(2500)

        body = page.locator("body").inner_text(timeout=10_000)
        for bad in shot.reject:
            if bad in body:
                return "REJECTED", f"page shows {bad!r}"
        if shot.expect and shot.expect not in body:
            return "REJECTED", f"expected {shot.expect!r} not on the page"

        path = OUT / f"{shot.name}.png"
        page.screenshot(path=str(path), full_page=shot.full_page)
        size = path.stat().st_size // 1024
        return "ok", f"{path.name} ({size} KB)"
    except Exception as exc:
        return "FAILED", f"{type(exc).__name__}: {exc}"[:140]
    finally:
        ctx.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Capture report figures")
    parser.add_argument("--only", default=None, help="substring match on the shot name")
    args = parser.parse_args(argv)

    from playwright.sync_api import sync_playwright

    OUT.mkdir(parents=True, exist_ok=True)
    shots = [s for s in SHOTS if not args.only or args.only in s.name]
    terminals = [t for t in TERMINALS if not args.only or args.only in t]
    if not shots and not terminals:
        print(f"no shot matches {args.only!r}")
        return 2

    failures = 0
    with sync_playwright() as pw:
        browser = pw.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
        for shot in shots:
            status, detail = capture(shot, browser)
            if status != "ok":
                failures += 1
            print(f"  {status:<7} {shot.name:<28} {detail}")
            print(f"          proves: {shot.proves}")
        for name in terminals:
            status, detail = capture_terminal(name, browser)
            if status != "ok":
                failures += 1
            print(f"  {status:<7} {name:<28} {detail}")
        browser.close()

    total = len(shots) + len(terminals)
    print(f"\n{total - failures}/{total} captured into {OUT}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
