"""The serving layer's published contract, driven through the app with faked stores.

Runs without Docker, Redis or Postgres. What it pins is the CONTRACT - status codes,
the degraded header, the envelope fields and the route set - because those are what a
client and the report's screenshots depend on, and they are the things a refactor
breaks without any test of the merge logic noticing.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient

from fleet.common.simclock import SimClock
from fleet.serving import app as app_mod
from fleet.serving.readers import StoreUnavailableError

SIM_NOW = datetime(2026, 3, 8, 14, 30, tzinfo=UTC)
HWM = date(2026, 3, 7)


class FakePg:
    def __init__(self, down: bool = False) -> None:
        self.down = down

    def _guard(self) -> None:
        if self.down:
            raise StoreUnavailableError("postgres", ConnectionError("refused"))

    def high_water_mark(self):
        self._guard()
        return HWM

    def zone_daily(self, from_date, to_date):
        self._guard()
        return [
            {
                "zone_id": "Z01",
                "sim_date": from_date,
                "trips": 40,
                "earnings": 900.0,
                "active_vehicles": 12,
                "idle_ratio": 0.25,
                "avg_speed_kmh": 31.0,
                "hours_reported": 24,
            }
        ]

    def vehicle_profitability(self, vehicle_id, days):
        self._guard()
        return [
            {
                "sim_date": HWM,
                "trips": 18,
                "revenue": 410.5,
                "net_profit": -22.0,
                "classification": "UNPROFITABLE",
                "rolling_7d_avg_profit": -18.4,
                "restatement_count": 0,
            }
        ]

    def unprofitable(self, limit, classification):
        self._guard()
        return [
            {
                "vehicle_id": "V113",
                "sim_date": HWM,
                "classification": "UNPROFITABLE",
                "net_profit": -322.0,
                "rolling_7d_avg_profit": -280.0,
            }
        ]

    def pipeline_status(self):
        self._guard()
        return {
            "batch_complete_thru": HWM,
            "last_job_run_id": "backfill-abc12345",
            "watermark_updated_at": SIM_NOW,
            "latest_fact_sim_date": HWM,
            "pnl_rows": 1050,
            "zone_hourly_rows": 2016,
            "restated_rows": 0,
        }

    def ping(self) -> None:
        self._guard()


class FakeRedis:
    def __init__(self, down: bool = False) -> None:
        self.down = down

    def _guard(self) -> None:
        if self.down:
            raise StoreUnavailableError("redis", ConnectionError("Error 111"))

    def zones(self):
        self._guard()
        return [
            {
                "zone_id": "Z01",
                "active_vehicles": 11,
                "trips": 6,
                "earnings": 130.0,
                "completed_trips": 6,
                "idle_ratio": 0.3,
                "avg_speed_kmh": 29.0,
                "window_start": "2026-03-08T14:00:00",
                "window_end": "2026-03-08T14:15:00",
                "updated_at_sim": "2026-03-08T14:15:00",
                "reporting": True,
            }
        ]

    def snapshot(self):
        self._guard()
        return {
            "total_active": 141,
            "total_trips": 63,
            "fleet_idle_ratio": 0.28,
            "zones_reporting": 12,
            "updated_at_sim": "2026-03-08T14:15:00",
        }

    def vehicle(self, vehicle_id):
        self._guard()
        return {"status": "on_trip", "zone_id": "Z04"} if vehicle_id == "V001" else None

    def idle_alerts(self, limit):
        self._guard()
        return [
            {
                "alert_id": "a1",
                "vehicle_id": "V007",
                "severity": "CRITICAL",
                "idle_minutes": 95.0,
                "zone_id": "Z09",
                "raised_at_sim": "2026-03-08T13:40:00",
            }
        ]

    def ping(self) -> None:
        self._guard()


@pytest.fixture
def client(monkeypatch):
    """A client whose stores are fakes and whose clock is fixed.

    The clock is injected rather than read from `/state/sim_epoch.json` so the tests
    do not need a running stack - and so `as_of_sim` is deterministic in the
    snapshot assertions.
    """
    monkeypatch.setattr(
        app_mod,
        "_clock",
        SimClock(epoch_wall=datetime.now(UTC), epoch_sim=SIM_NOW, day_seconds=300),
    )
    pg, rd = FakePg(), FakeRedis()
    app_mod.app.dependency_overrides[app_mod.pg] = lambda: pg
    app_mod.app.dependency_overrides[app_mod.rd] = lambda: rd
    monkeypatch.setattr(app_mod, "_pg", pg)
    monkeypatch.setattr(app_mod, "_redis", rd)
    with TestClient(app_mod.app) as c:
        c.fake_pg, c.fake_redis = pg, rd  # type: ignore[attr-defined]
        yield c
    app_mod.app.dependency_overrides.clear()


def test_unprofitable_is_reachable_and_not_shadowed_by_the_vehicle_id_route(client):
    """The route-ordering trap: /vehicles/unprofitable must not bind vehicle_id.

    If the parameterised route is declared first, this returns 404 from the Redis
    vehicle lookup - a perfectly good endpoint that simply cannot be called, with
    nothing in any log to say so.
    """
    r = client.get("/api/v1/vehicles/unprofitable")
    assert r.status_code == 200, r.text
    assert r.json()[0]["vehicle_id"] == "V113"


def test_utilization_envelope_is_self_describing(client):
    r = client.get("/api/v1/fleet/utilization", params={"from": "2026-03-06", "to": "2026-03-08"})
    assert r.status_code == 200, r.text
    body = r.json()
    for key in ("as_of_sim", "batch_complete_thru", "consistency", "degraded", "rows"):
        assert key in body
    assert body["degraded"] is False
    assert app_mod.config.serving().degraded_header not in r.headers
    sources = {row["source"] for row in body["rows"]}
    assert sources == {"batch", "speed"}
    assert all("exact" in row for row in body["rows"])


def test_one_store_down_is_200_with_the_degraded_header(client):
    client.fake_redis.down = True
    r = client.get("/api/v1/fleet/utilization", params={"from": "2026-03-06", "to": "2026-03-08"})
    assert r.status_code == 200, "a single store outage must never fail the request"
    assert r.headers[app_mod.config.serving().degraded_header] == "true"
    body = r.json()
    assert body["degraded"] is True
    assert body["missing_stores"] == ["redis"]
    assert body["rows"], "the batch half must still be served"


def test_both_stores_down_is_503_not_500(client):
    client.fake_pg.down = True
    client.fake_redis.down = True
    r = client.get("/api/v1/fleet/utilization", params={"from": "2026-03-06", "to": "2026-03-08"})
    assert r.status_code == 503
    assert "postgres" in r.json()["detail"] and "redis" in r.json()["detail"]


def test_inverted_range_is_422(client):
    r = client.get("/api/v1/fleet/utilization", params={"from": "2026-03-08", "to": "2026-03-01"})
    assert r.status_code == 422


def test_pipeline_status_exposes_the_watermark_age(client):
    r = client.get("/api/v1/pipeline/status")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["batch_complete_thru"] == HWM.isoformat()
    assert body["watermark_age_sim_days"] == 1
    assert body["pnl_rows"] == 1050


def test_health_live_touches_nothing(client):
    client.fake_pg.down = True
    client.fake_redis.down = True
    assert client.get("/health/live").status_code == 200, (
        "liveness must not depend on a database, or a blip restarts the API"
    )


def test_health_ready_is_503_only_when_every_store_is_down(client):
    assert client.get("/health/ready").status_code == 200
    client.fake_redis.down = True
    assert client.get("/health/ready").status_code == 200, "one store is enough to serve"
    client.fake_pg.down = True
    assert client.get("/health/ready").status_code == 503


def test_health_deep_reports_per_store_latency(client):
    body = client.get("/health/deep").json()
    assert body["status"] == "ok"
    assert set(body["stores"]) == {"postgres", "redis"}
    assert body["stores"]["postgres"]["latency_ms"] is not None


def test_missing_vehicle_is_404(client):
    assert client.get("/api/v1/vehicles/V999").status_code == 404
    assert client.get("/api/v1/vehicles/V001").status_code == 200


EXPECTED_PATHS = {
    "/api/v1/fleet/live",
    "/api/v1/fleet/zones",
    "/api/v1/fleet/utilization",
    "/api/v1/vehicles/unprofitable",
    "/api/v1/vehicles/{vehicle_id}",
    "/api/v1/vehicles/{vehicle_id}/profitability",
    "/api/v1/alerts/idle",
    "/api/v1/pipeline/status",
    "/health/live",
    "/health/ready",
    "/health/deep",
}


def test_openapi_publishes_the_expected_route_set(client):
    """A snapshot of the contract. Adding a route is fine; silently losing one is not."""
    paths = set(client.get("/openapi.json").json()["paths"])
    missing = EXPECTED_PATHS - paths
    assert not missing, f"endpoints disappeared from the published API: {sorted(missing)}"
