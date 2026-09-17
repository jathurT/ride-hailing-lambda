-- Batch-layer data mart. Star schema, per the module's OLAP material.
--
-- The module defines a data mart as "a refined subset of a data warehouse ... an
-- additional transformation stage beyond initial ETL/ELT pipelines, improving
-- performance for complex queries by pre-joining and aggregating data". That is a
-- precise description of fact_vehicle_daily_pnl: telemetry pre-joined with
-- expenses and pre-aggregated to one row per vehicle per simulated day.
--
-- Design notes are in plan/07 section 2.

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------- dimensions

-- SCD Type 2. If a vehicle is reassigned to a different home zone mid-history,
-- past profitability rows still join to the attributes that were true at the time.
CREATE TABLE IF NOT EXISTS mart.dim_vehicle (
    vehicle_key   SERIAL PRIMARY KEY,
    vehicle_id    TEXT        NOT NULL,
    model         TEXT        NOT NULL,
    fuel_type     TEXT        NOT NULL CHECK (fuel_type IN ('petrol','hybrid','electric')),
    capacity      SMALLINT    NOT NULL,
    home_zone     TEXT        NOT NULL,
    driver_id     TEXT        NOT NULL,
    valid_from    DATE        NOT NULL,
    valid_to      DATE,
    is_current    BOOLEAN     NOT NULL DEFAULT TRUE,
    UNIQUE (vehicle_id, valid_from)
);
CREATE INDEX IF NOT EXISTS ix_dim_vehicle_current
    ON mart.dim_vehicle (vehicle_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS mart.dim_zone (
    zone_key      SERIAL PRIMARY KEY,
    zone_id       TEXT UNIQUE NOT NULL,
    zone_name     TEXT        NOT NULL,
    zone_class    TEXT        NOT NULL,
    demand_weight NUMERIC(4,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key      SERIAL PRIMARY KEY,
    sim_date      DATE UNIQUE NOT NULL,
    sim_dow       SMALLINT    NOT NULL,
    is_weekend    BOOLEAN     NOT NULL,
    sim_day_index INT         NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.dim_driver (
    driver_key SERIAL PRIMARY KEY,
    driver_id  TEXT UNIQUE NOT NULL,
    joined_on  DATE NOT NULL
);

-- --------------------------------------------------------------------- facts

CREATE TABLE IF NOT EXISTS mart.fact_vehicle_daily_pnl (
    vehicle_id            TEXT NOT NULL,
    sim_date              DATE NOT NULL,
    vehicle_key           INT REFERENCES mart.dim_vehicle(vehicle_key),
    date_key              INT REFERENCES mart.dim_date(date_key),

    -- recomputed exactly from the immutable master dataset
    trips                 INT           NOT NULL DEFAULT 0,
    revenue               NUMERIC(12,2) NOT NULL DEFAULT 0,
    telemetry_distance_km NUMERIC(10,2) NOT NULL DEFAULT 0,
    paid_distance_km      NUMERIC(10,2) NOT NULL DEFAULT 0,
    deadhead_distance_km  NUMERIC(10,2) NOT NULL DEFAULT 0,
    on_trip_hours         NUMERIC(6,2)  NOT NULL DEFAULT 0,
    idle_hours            NUMERIC(6,2)  NOT NULL DEFAULT 0,
    utilization_pct       NUMERIC(5,2),

    -- from the daily expense file
    fuel_cost             NUMERIC(12,2),
    maintenance_cost      NUMERIC(12,2),
    partner_distance_km   NUMERIC(10,2),
    service_flag          BOOLEAN,
    partner_id            TEXT,

    -- reconciliation between the two sources
    expense_status        TEXT NOT NULL DEFAULT 'MATCHED'
        CHECK (expense_status IN ('MATCHED','MISSING_EXPENSE','MISSING_TELEMETRY')),
    distance_variance_pct NUMERIC(6,2),

    -- derived
    net_profit            NUMERIC(12,2),
    profit_per_km         NUMERIC(10,4),
    margin_pct            NUMERIC(6,2),
    rolling_7d_avg_profit NUMERIC(12,2),
    profit_trend_slope    NUMERIC(12,4),
    classification        TEXT NOT NULL DEFAULT 'INSUFFICIENT_DATA'
        CHECK (classification IN
            ('HEALTHY','WATCH','UNPROFITABLE','CRITICAL','INSUFFICIENT_DATA')),

    -- provenance, so any figure is traceable back to the job that produced it
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    restated_at           TIMESTAMPTZ,
    restatement_count     INT NOT NULL DEFAULT 0,
    job_run_id            TEXT NOT NULL,

    PRIMARY KEY (vehicle_id, sim_date)     -- the upsert conflict target
);
CREATE INDEX IF NOT EXISTS ix_pnl_sim_date ON mart.fact_vehicle_daily_pnl (sim_date);
CREATE INDEX IF NOT EXISTS ix_pnl_classification
    ON mart.fact_vehicle_daily_pnl (classification, sim_date);

-- The exact counterpart of what the speed layer approximates. Having both is what
-- makes the divergence chart in plan/07 section 5.4 possible.
CREATE TABLE IF NOT EXISTS mart.fact_zone_hourly (
    zone_id         TEXT     NOT NULL,
    sim_date        DATE     NOT NULL,
    sim_hour        SMALLINT NOT NULL CHECK (sim_hour BETWEEN 0 AND 23),
    zone_key        INT REFERENCES mart.dim_zone(zone_key),
    date_key        INT REFERENCES mart.dim_date(date_key),
    trips           INT           NOT NULL DEFAULT 0,
    earnings        NUMERIC(12,2) NOT NULL DEFAULT 0,
    active_vehicles INT           NOT NULL DEFAULT 0,
    idle_ratio      NUMERIC(5,4),
    avg_speed_kmh   NUMERIC(6,2),
    job_run_id      TEXT NOT NULL,
    PRIMARY KEY (zone_id, sim_date, sim_hour)
);

-- ------------------------------------------------- the serving-layer boundary

-- Single-row table. The CHECK on a BOOLEAN primary key makes a second row
-- impossible, so there can never be two conflicting watermarks.
--
-- Everything about the serving-layer contract depends on this advancing ONLY
-- after the fact upsert has committed. See plan/07 section 5 and ADR-005.
CREATE TABLE IF NOT EXISTS mart.batch_high_water_mark (
    id                  BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    batch_complete_thru DATE        NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_job_run_id     TEXT        NOT NULL
);

-- ------------------------------------------------------------ observability

CREATE TABLE IF NOT EXISTS mart.dq_run_log (
    id         BIGSERIAL PRIMARY KEY,
    job_run_id TEXT NOT NULL,
    sim_date   DATE NOT NULL,
    check_name TEXT NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('PASS','WARN','FAIL')),
    observed   NUMERIC,
    threshold  NUMERIC,
    detail     TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_sim_date ON mart.dq_run_log (sim_date, status);

-- Measures the divergence between the speed view and the batch view for the same
-- simulated date, turning the module's abstract "reconciling data between systems"
-- problem into a number we monitor.
CREATE TABLE IF NOT EXISTS mart.reconciliation_delta (
    id          BIGSERIAL PRIMARY KEY,
    sim_date    DATE NOT NULL,
    zone_id     TEXT NOT NULL,
    metric      TEXT NOT NULL,
    speed_value NUMERIC,
    batch_value NUMERIC,
    delta_pct   NUMERIC(8,3),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mart.restatement_log (
    id            BIGSERIAL PRIMARY KEY,
    sim_date      DATE NOT NULL,
    marker_key    TEXT UNIQUE NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    triggered_run TEXT
);
