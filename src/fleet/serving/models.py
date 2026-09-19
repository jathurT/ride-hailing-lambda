"""Response shapes for the serving layer.

Every merged row is **self-describing**: it carries where it came from and whether it
is exact. A client holding a row never has to know the architecture to know how much
to trust the number, which is the point of publishing the envelope at all
(plan/07 section 5.5).

The provenance fields on the batch P&L rows - `restated_at`, `restatement_count` -
are not in the plan's sketch and are included anyway. A figure that has changed since
it was first published is the first thing an auditor asks about, and the mart already
records it; dropping it at the API boundary would throw away the only evidence.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class MetricRowOut(BaseModel):
    sim_date: date
    zone_id: str
    source: str = Field(
        description="'batch' (exact, from Postgres) or 'speed' (approximate, Redis)"
    )
    exact: bool
    trips: int | None = None
    earnings: float | None = None
    active_vehicles: int | None = None
    idle_ratio: float | None = None
    utilization_pct: float | None = None
    avg_speed_kmh: float | None = None
    hours_reported: int | None = Field(
        None, description="Batch rows only: how many of the day's 24 hours reported data"
    )
    approximation_note: str | None = None


class MergedEnvelope(BaseModel):
    """The merged answer plus everything needed to judge it."""

    as_of_sim: datetime
    batch_complete_thru: date | None = Field(
        None, description="The watermark. null means the batch layer has never run."
    )
    consistency: str = Field(description="Plain-English statement of what this answer is")
    degraded: bool
    missing_stores: list[str] = Field(default_factory=list)
    uncovered_dates: list[date] = Field(
        default_factory=list,
        description="Requested dates neither view can answer for - the batch layer is behind",
    )
    row_count: int
    rows: list[MetricRowOut]


class ZoneLive(BaseModel):
    zone_id: str
    active_vehicles: int | None = None
    trips: int | None = None
    earnings: float | None = None
    completed_trips: int | None = None
    idle_ratio: float | None = None
    avg_speed_kmh: float | None = None
    window_start: str | None = None
    window_end: str | None = None
    updated_at_sim: str | None = None
    reporting: bool = True


class FleetLive(BaseModel):
    as_of_sim: datetime
    total_active: int | None = None
    total_trips: int | None = None
    fleet_idle_ratio: float | None = None
    zones_reporting: int | None = None
    updated_at_sim: str | None = None
    approximation_note: str


class VehiclePnlRow(BaseModel):
    sim_date: date
    trips: int | None = None
    revenue: float | None = None
    telemetry_distance_km: float | None = None
    paid_distance_km: float | None = None
    deadhead_distance_km: float | None = None
    on_trip_hours: float | None = None
    idle_hours: float | None = None
    utilization_pct: float | None = None
    fuel_cost: float | None = None
    maintenance_cost: float | None = None
    expense_status: str | None = None
    distance_variance_pct: float | None = None
    net_profit: float | None = None
    profit_per_km: float | None = None
    margin_pct: float | None = None
    rolling_7d_avg_profit: float | None = None
    profit_trend_slope: float | None = None
    classification: str | None = None
    computed_at: datetime | None = None
    restated_at: datetime | None = None
    restatement_count: int | None = None


class VehicleProfitability(BaseModel):
    vehicle_id: str
    source: str = "batch"
    exact: bool = True
    batch_complete_thru: date | None = None
    row_count: int
    rows: list[VehiclePnlRow]


class UnprofitableVehicle(BaseModel):
    vehicle_id: str
    sim_date: date
    classification: str | None = None
    net_profit: float | None = None
    rolling_7d_avg_profit: float | None = None
    profit_trend_slope: float | None = None
    profit_per_km: float | None = None
    margin_pct: float | None = None
    revenue: float | None = None
    fuel_cost: float | None = None
    maintenance_cost: float | None = None
    trips: int | None = None
    utilization_pct: float | None = None


class IdleAlert(BaseModel):
    alert_id: str | None = None
    vehicle_id: str | None = None
    severity: str | None = None
    idle_minutes: float | None = None
    zone_id: str | None = None
    raised_at_sim: str | None = None


class StoreHealth(BaseModel):
    status: str = Field(description="'up' or 'down'")
    latency_ms: float | None = None
    detail: str | None = None


class DeepHealth(BaseModel):
    status: str = Field(description="'ok' when every dependency answered, else 'degraded'")
    as_of_sim: datetime
    stores: dict[str, StoreHealth]


class PipelineStatus(BaseModel):
    as_of_sim: datetime
    sim_day_index: int
    batch_complete_thru: date | None = None
    watermark_age_sim_days: int | None = None
    last_job_run_id: str | None = None
    watermark_updated_at: datetime | None = None
    latest_fact_sim_date: date | None = None
    pnl_rows: int | None = None
    zone_hourly_rows: int | None = None
    restated_rows: int | None = None
    speed_view: dict[str, Any] = Field(default_factory=dict)
    degraded: bool = False
    missing_stores: list[str] = Field(default_factory=list)
