"""Capture the report's screenshots from the running stack.

Screenshots are evidence, and evidence that cannot be regenerated is not much use:
if a figure is questioned during a viva, "run this script" is a better answer than
"we took it on Tuesday". Every shot here is reproducible from a running stack, so the
report's figures and the system it describes cannot silently drift apart.

Needs the `obs` and `run` profiles up:

    make up && make up-obs && python scripts/capture_figures.py

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
SHOTS = [
    Shot(
        "R1-fleet-operations",
        f"{GRAFANA}/d/fleet-operations/?kiosk&refresh=10s",
        "the live half of the business question - utilization and earnings by area",
        wait_ms=12000,
        wait_for="div.react-grid-item",
    ),
    Shot(
        "R2-batch-reconciliation",
        f"{GRAFANA}/d/fleet-batch-reconciliation/?kiosk",
        "the daily half - which vehicles are becoming unprofitable",
        wait_ms=12000,
        wait_for="div.react-grid-item",
    ),
    Shot(
        "R10-pipeline-health",
        f"{GRAFANA}/d/fleet-pipeline-health/?kiosk&from=now-30m&to=now",
        "observability across all four stages: ingest, process, store, serve",
        wait_ms=14000,
        wait_for="div.react-grid-item",
    ),
    Shot(
        "R9-alertmanager",
        "http://localhost:9093/#/alerts",
        "alert rules firing - run `make chaos-kill-producer` first",
        wait_ms=5000,
    ),
    Shot(
        "R9b-prometheus-alerts",
        "http://localhost:9090/alerts?search=",
        "the rule set as Prometheus evaluates it",
        wait_ms=4000,
        full_page=True,
        expect="NoTelemetryIngested",
    ),
    Shot(
        "R12-api-docs",
        "http://localhost:8000/docs",
        "the serving-layer contract: every endpoint, and the merged response model",
        wait_ms=5000,
        full_page=True,
        expect="utilization",
    ),
    Shot(
        "R6-kafka-topics",
        "http://localhost:8080/ui/clusters/fleet/all-topics?perPage=25",
        "topics with per-partition counts - even key distribution, no hotspotting",
        wait_ms=7000,
        expect="fleet.telemetry",
    ),
    Shot(
        "R4-airflow-grid",
        "http://localhost:8082/dags/fleet_daily_reconciliation/grid",
        "the batch layer running on schedule, one column per simulated day",
        wait_ms=9000,
        auth=("admin", "admin"),
        expect="fleet_daily_reconciliation",
    ),
    Shot(
        "R5-airflow-graph",
        "http://localhost:8082/dags/fleet_daily_reconciliation/graph",
        "the DAG shape, including the validation branch",
        wait_ms=9000,
        auth=("admin", "admin"),
        # The graph draws task names inside SVG, invisible to inner_text; assert
        # on the header instead, which is real text.
        expect="fleet_daily_reconciliation",
    ),
    Shot(
        "R14-minio",
        "http://localhost:9001/browser/fleet-lake",
        "the master dataset on object storage, partitioned by sim_date",
        wait_ms=7000,
        auth=("fleetadmin", "fleetadmin"),
        expect="fleet-lake",
    ),
]


def capture(shot: Shot, browser) -> tuple[str, str]:
    from playwright.sync_api import TimeoutError as PWTimeout

    ctx = browser.new_context(viewport=VIEWPORT, device_scale_factor=2)
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

        body = page.locator("body").inner_text(timeout=10_000)
        for bad in shot.reject:
            if bad in body:
                return "REJECTED", f"page shows {bad!r} - not authenticated"
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
    if not shots:
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
        browser.close()

    print(f"\n{len(shots) - failures}/{len(shots)} captured into {OUT}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
