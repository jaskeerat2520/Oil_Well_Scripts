-- Migration: Schools table + nearest-school columns on well_risk_scores
-- Source: Ohio Dept of Education map layers (ODE_Layers/MapServer)
-- Tier 1: informational — distance-only, no composite scoring impact yet.
-- Apply via psycopg2 from ingest_schools.py setup, or via Supabase MCP.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ── Schools table ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schools (
    id           SERIAL PRIMARY KEY,
    external_id  TEXT,                              -- ODE building ID / IRN if present
    name         TEXT NOT NULL,
    district     TEXT,
    school_type  TEXT,                              -- Elementary, Middle, High, Career-Tech, etc.
    ownership    TEXT,                              -- Public, Private, Charter, etc.
    raw_attrs    JSONB,                             -- preserve all original fields for forward-compat
    geometry     GEOMETRY(Point, 4326) NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (external_id)
);

CREATE INDEX IF NOT EXISTS schools_geom_gist ON schools USING GIST (geometry);
CREATE INDEX IF NOT EXISTS schools_district_idx ON schools (district);

-- ── Nearest-school columns on well_risk_scores ────────────────────────────────
ALTER TABLE well_risk_scores
  ADD COLUMN IF NOT EXISTS nearest_school_id          INTEGER REFERENCES schools(id),
  ADD COLUMN IF NOT EXISTS nearest_school_distance_m  DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS nearest_school_name        TEXT;
