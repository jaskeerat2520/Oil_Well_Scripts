-- Migration: Add water monitoring tables for oil well risk scoring
-- Requires: PostGIS extension

-- Enable PostGIS if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================
-- 1. Water monitoring stations (USGS Ohio)
-- ============================================================
CREATE TABLE IF NOT EXISTS water_stations (
    id              SERIAL PRIMARY KEY,
    usgs_site_id    VARCHAR(20) UNIQUE NOT NULL,
    station_name    VARCHAR(255) NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    geog            GEOGRAPHY(Point, 4326) NOT NULL,
    site_type       VARCHAR(50),
    state_code      VARCHAR(10) DEFAULT 'US:39',
    huc_code        VARCHAR(20),
    has_discharge   SMALLINT DEFAULT 0,
    has_gage_height SMALLINT DEFAULT 0,
    has_water_depth SMALLINT DEFAULT 0,
    has_temperature SMALLINT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_water_stations_geog
    ON water_stations USING GIST (geog);

CREATE INDEX IF NOT EXISTS idx_water_stations_site_id
    ON water_stations (usgs_site_id);


-- ============================================================
-- 2. Continuous readings cache
-- ============================================================
CREATE TABLE IF NOT EXISTS water_readings (
    id              SERIAL PRIMARY KEY,
    station_id      INTEGER REFERENCES water_stations(id),
    usgs_site_id    VARCHAR(20) NOT NULL,
    parameter_code  VARCHAR(10) NOT NULL,
    parameter_name  VARCHAR(100),
    value           DOUBLE PRECISION NOT NULL,
    unit            VARCHAR(30),
    recorded_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_readings_station_param
    ON water_readings (station_id, parameter_code);

CREATE INDEX IF NOT EXISTS idx_readings_recorded
    ON water_readings (recorded_at);

CREATE INDEX IF NOT EXISTS idx_readings_site_id
    ON water_readings (usgs_site_id);


-- ============================================================
-- 3. Well ↔ water station proximity scores
-- ============================================================
CREATE TABLE IF NOT EXISTS well_water_proximity (
    id                    SERIAL PRIMARY KEY,
    well_id               INTEGER NOT NULL,
    station_id            INTEGER REFERENCES water_stations(id) NOT NULL,
    distance_meters       DOUBLE PRECISION NOT NULL,
    distance_miles        DOUBLE PRECISION NOT NULL,
    latest_discharge      DOUBLE PRECISION,
    latest_gage_height    DOUBLE PRECISION,
    latest_depth_to_water DOUBLE PRECISION,
    water_risk_score      DOUBLE PRECISION DEFAULT 0.0,
    scored_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (well_id, station_id)
);

CREATE INDEX IF NOT EXISTS idx_proximity_well_score
    ON well_water_proximity (well_id, water_risk_score);

-- Add a FK to your wells table if it exists:
-- ALTER TABLE well_water_proximity
--     ADD CONSTRAINT fk_proximity_well
--     FOREIGN KEY (well_id) REFERENCES wells(id);


-- ============================================================
-- 4. Ensure your wells table has a geography column
--    (skip if you already have one)
-- ============================================================
-- ALTER TABLE wells ADD COLUMN IF NOT EXISTS geog GEOGRAPHY(Point, 4326);
-- UPDATE wells SET geog = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography;
-- CREATE INDEX IF NOT EXISTS idx_wells_geog ON wells USING GIST (geog);
