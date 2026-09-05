"""Typed configuration, loaded from the environment.

Everything tunable lives here. There are no magic numbers anywhere else in the
codebase — a threshold in a `.py` file is a bug.

Settings are split into one class per architectural layer, mirroring the repository
layout (see plan/11 §1). The split is not cosmetic: it means the streaming job can
be handed only what the speed layer needs, and a missing batch-layer variable cannot
break the producer.

The validators at the bottom of `ProcessingSettings` are the important part. They
encode the project's subtlest coupling — between the simulated-time speed-up and the
stream watermark — as a **start-up failure** rather than as a comment nobody reads.
See plan/03 §3.3.
"""

from __future__ import annotations

from datetime import UTC, datetime
from functools import lru_cache

from pydantic import Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from fleet.common.simclock import SECONDS_PER_DAY

_BASE = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class SimulationSettings(BaseSettings):
    """The simulated clock and the shape of the fake fleet."""

    model_config = SettingsConfigDict(**_BASE, env_prefix="SIM_")

    day_seconds: int = Field(300, gt=0, description="Real seconds per simulated day")
    epoch_sim: datetime = Field(
        datetime(2026, 3, 1, tzinfo=UTC),
        description="Simulated datetime the run starts at. Fixed so screenshots are stable.",
    )
    state_path: str = "/state/sim_epoch.json"

    @computed_field
    @property
    def speedup(self) -> float:
        return SECONDS_PER_DAY / self.day_seconds

    @field_validator("epoch_sim")
    @classmethod
    def _must_be_aware(cls, v: datetime) -> datetime:
        return v if v.tzinfo else v.replace(tzinfo=UTC)


class FleetSettings(BaseSettings):
    """The simulated fleet itself."""

    model_config = SettingsConfigDict(**_BASE, env_prefix="FLEET_")

    size: int = Field(150, gt=0, description="Number of vehicles")
    target_eps: int = Field(240, gt=0, description="Aggregate events/second, real time")
    ping_interval_sim_minutes: int = Field(3, gt=0, description="Telemetry interval, sim time")
    defect_rate: float = Field(0.01, ge=0.0, le=1.0, description="Fraction of malformed events")
    random_seed: int = 42


class KafkaSettings(BaseSettings):
    """Broker connection and topic names. Topic *design* is in plan/04 §1."""

    model_config = SettingsConfigDict(**_BASE, env_prefix="KAFKA_")

    bootstrap: str = "kafka:29092"
    schema_registry_url: str = "http://schema-registry:8081"

    telemetry_topic: str = "fleet.telemetry.v1"
    registry_topic: str = "fleet.vehicle.registry"
    alerts_topic: str = "fleet.alerts.v1"
    dlq_topic: str = "fleet.telemetry.dlq"

    telemetry_partitions: int = Field(6, gt=0)
    registry_partitions: int = Field(3, gt=0)
    alerts_partitions: int = Field(3, gt=0)
    replication_factor: int = Field(1, gt=0)

    # 1 real hour. The *design* value is 7 days; the demo runs a reduced retention
    # because 7 days of real-time production would be ~36 GB. Declared in the
    # report rather than hidden — see plan/04 §1.4.
    telemetry_retention_ms: int = 3_600_000
    telemetry_retention_bytes: int = 2_147_483_648


class ProcessingSettings(BaseSettings):
    """Windowing and watermarking, all expressed in **simulated** time."""

    model_config = SettingsConfigDict(**_BASE, env_prefix="PROC_")

    watermark_sim_minutes: int = Field(30, ge=1)
    window_sim_minutes: int = Field(15, ge=1)
    window_slide_sim_minutes: int = Field(5, ge=1)
    idle_alert_sim_minutes: int = Field(45, ge=1)
    idle_critical_sim_minutes: int = Field(90, ge=1)
    trigger_seconds: int = Field(10, ge=1, description="Micro-batch trigger, real seconds")

    # Minimum real-world lateness tolerance we are willing to ship. Below this, a
    # Kafka rebalance or a GC pause silently drops events.
    MIN_WATERMARK_REAL_SECONDS: float = 5.0

    @model_validator(mode="after")
    def _watermark_must_survive_real_jitter(self) -> ProcessingSettings:
        """Reject a watermark that is too tight once the speed-up is applied.

        At 288x, a 30-simulated-minute watermark is only 6.25 real seconds. Halve
        `SIM_DAY_SECONDS` without revisiting this and the watermark drops to 3.1 real
        seconds, at which point normal jitter starts discarding events — with no
        error, just empty windows. This validator turns that into a start-up crash.
        """
        sim = SimulationSettings()
        real = (self.watermark_sim_minutes * 60) / sim.speedup
        if real < self.MIN_WATERMARK_REAL_SECONDS:
            raise ValueError(
                f"PROC_WATERMARK_SIM_MINUTES={self.watermark_sim_minutes} gives only "
                f"{real:.2f} real seconds of lateness tolerance at a {sim.speedup:.0f}x "
                f"speed-up (SIM_DAY_SECONDS={sim.day_seconds}). Minimum is "
                f"{self.MIN_WATERMARK_REAL_SECONDS}s. Raise the watermark or slow the clock. "
                f"See plan/03 §3.3."
            )
        return self

    @model_validator(mode="after")
    def _window_slide_must_divide_window(self) -> ProcessingSettings:
        """A sliding window whose slide does not divide its width produces uneven
        coverage — some instants belong to more windows than others, which makes the
        dashboard flicker. Spark permits it; we do not."""
        if self.window_sim_minutes % self.window_slide_sim_minutes != 0:
            raise ValueError(
                f"PROC_WINDOW_SIM_MINUTES ({self.window_sim_minutes}) must be a multiple "
                f"of PROC_WINDOW_SLIDE_SIM_MINUTES ({self.window_slide_sim_minutes})."
            )
        return self

    @model_validator(mode="after")
    def _idle_thresholds_must_escalate(self) -> ProcessingSettings:
        if self.idle_critical_sim_minutes <= self.idle_alert_sim_minutes:
            raise ValueError("Critical idle threshold must exceed the warning threshold.")
        return self


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE, env_prefix="STORE_")

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "fleetadmin"
    minio_secret_key: str = "fleetadmin"
    lake_bucket: str = "fleet-lake"

    postgres_dsn: str = "postgresql://fleet:fleet@postgres-mart:5432/fleet_mart"
    redis_url: str = "redis://redis:6379/0"

    speed_view_ttl_sim_hours: int = Field(2, gt=0)
    vehicle_state_ttl_sim_minutes: int = Field(30, gt=0)


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE, env_prefix="OBS_")

    log_level: str = "INFO"
    log_json: bool = True
    service_name: str = "fleet"
    metrics_port: int = 8001
    pushgateway_url: str = "http://pushgateway:9091"
    otel_endpoint: str = "http://otel-collector:4317"
    otel_enabled: bool = False  # switched on with the `full` compose profile


class ServingSettings(BaseSettings):
    """The FastAPI serving layer (plan/07 §5).

    `max_page_size` is a cap, not a default. Without one, `?limit=100000` on the
    unprofitable-vehicles endpoint turns a 10-row dashboard query into a full table
    scan serialised through the driver, and the first person to discover that is
    whoever is running the live demo.
    """

    model_config = SettingsConfigDict(**_BASE, env_prefix="API_")

    host: str = "0.0.0.0"
    port: int = Field(8000, gt=0, lt=65536)
    max_page_size: int = Field(200, gt=0)
    default_history_days: int = Field(7, gt=0)
    degraded_header: str = "X-Data-Degraded"
    cors_origins: str = "*"

    # How long a store may take before the serving layer calls it unavailable and
    # degrades. Short on purpose: a dashboard that hangs for 30 seconds is worse
    # than one that says "batch only" in 2, and the degraded path is the behaviour
    # the assignment asks us to demonstrate.
    store_timeout_seconds: float = Field(2.0, gt=0)

    @field_validator("cors_origins")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("API_CORS_ORIGINS must not be empty; use '*' to allow all")
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def sim() -> SimulationSettings:
    return SimulationSettings()


@lru_cache(maxsize=1)
def fleet() -> FleetSettings:
    return FleetSettings()


@lru_cache(maxsize=1)
def kafka() -> KafkaSettings:
    return KafkaSettings()


@lru_cache(maxsize=1)
def processing() -> ProcessingSettings:
    return ProcessingSettings()


@lru_cache(maxsize=1)
def storage() -> StorageSettings:
    return StorageSettings()


@lru_cache(maxsize=1)
def observability() -> ObservabilitySettings:
    return ObservabilitySettings()


@lru_cache(maxsize=1)
def serving() -> ServingSettings:
    return ServingSettings()
