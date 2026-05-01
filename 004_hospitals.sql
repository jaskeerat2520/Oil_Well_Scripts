-- Migration: Hospitals table + nearest-hospital columns on well_risk_scores
-- Source: Ohio Dept of Health Hospital Registration Information CSV (2023 reporting year).
-- Tier 1: informational — does NOT touch composite scoring.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ── Hospitals table ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hospitals (
    id                            SERIAL PRIMARY KEY,
    hospital_number               TEXT UNIQUE NOT NULL,    -- ODH's hospital_number
    name                          TEXT NOT NULL,           -- hospital_dba_name
    address                       TEXT,
    city                          TEXT,
    zip                           TEXT,
    county                        TEXT,
    corporate_phone               TEXT,
    medicare_classification       TEXT,
    service_category              TEXT,                    -- category_best_describing_hospital_services
    trauma_level_adult            TEXT,
    trauma_level_pediatric        TEXT,
    emergency_services_type       TEXT,
    registered_beds               INTEGER,                 -- summed across all beds_category rows
    raw_attrs                     JSONB,
    geometry                      GEOMETRY(Point, 4326),   -- nullable: a hospital we couldn't geocode is still recorded
    geocode_source                TEXT,                    -- 'census' | 'manual' | NULL
    ingested_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS hospitals_geom_gist  ON hospitals USING GIST (geometry);
CREATE INDEX IF NOT EXISTS hospitals_county_idx ON hospitals (county);

-- ── Flat view for the API route (mirrors schools_flat) ──────────────────────
CREATE OR REPLACE VIEW hospitals_flat AS
SELECT
  id, hospital_number, name, address, city, zip, county,
  medicare_classification, service_category,
  trauma_level_adult, trauma_level_pediatric,
  registered_beds,
  ST_X(geometry) AS lng,
  ST_Y(geometry) AS lat
FROM hospitals
WHERE geometry IS NOT NULL;

GRANT SELECT ON hospitals      TO anon, authenticated;
GRANT SELECT ON hospitals_flat TO anon, authenticated;

-- ── Nearest-hospital columns on well_risk_scores ────────────────────────────
ALTER TABLE well_risk_scores
  ADD COLUMN IF NOT EXISTS nearest_hospital_id          INTEGER REFERENCES hospitals(id),
  ADD COLUMN IF NOT EXISTS nearest_hospital_distance_m  DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS nearest_hospital_name        TEXT;
