-- Migration: ODNR hazard overlays (mine subsidence, abandoned-mine reclamation,
-- state floodplain, DOGRM regulatory urban areas) + nearest abandoned-mine opening.
--
-- Tier 1: informational. None of these touch composite_risk_score.
-- Source endpoints documented in memory `reference_odnr_rest_endpoints.md`.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ── Unified polygon table for ODNR hazard layers ─────────────────────────────
-- One row per polygon feature, layer_type column distinguishes the source.
CREATE TABLE IF NOT EXISTS odnr_hazard_layers (
    id           BIGSERIAL PRIMARY KEY,
    layer_type   TEXT NOT NULL,    -- 'aum_mine' | 'aml_project' | 'amlis_area' | 'state_floodplain' | 'dogrm_urban_area'
    external_id  TEXT,             -- ArcGIS OBJECTID (string) per source service
    name         TEXT,             -- MINE_NAME / PROJ_NAME / etc
    raw_attrs    JSONB,            -- preserve all fields from source for forward-compat
    geometry     GEOMETRY(MultiPolygon, 4326) NOT NULL,
    area_km2     DOUBLE PRECISION,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (layer_type, external_id)
);
CREATE INDEX IF NOT EXISTS odnr_hazard_layers_geom_gist ON odnr_hazard_layers USING GIST (geometry);
CREATE INDEX IF NOT EXISTS odnr_hazard_layers_type_idx  ON odnr_hazard_layers (layer_type);

-- ── Point table for AUM mine openings (separate geom type) ───────────────────
CREATE TABLE IF NOT EXISTS aum_openings (
    id           SERIAL PRIMARY KEY,
    external_id  TEXT,
    opening_type TEXT,
    raw_attrs    JSONB,
    geometry     GEOMETRY(Point, 4326) NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (external_id)
);
CREATE INDEX IF NOT EXISTS aum_openings_geom_gist ON aum_openings USING GIST (geometry);

-- ── New columns on well_risk_scores ──────────────────────────────────────────
ALTER TABLE well_risk_scores
  ADD COLUMN IF NOT EXISTS in_aum_subsidence_zone   BOOLEAN,
  ADD COLUMN IF NOT EXISTS in_aml_project           BOOLEAN,    -- state OR federal AMLIS
  ADD COLUMN IF NOT EXISTS in_state_floodplain      BOOLEAN,
  ADD COLUMN IF NOT EXISTS in_dogrm_urban_area      BOOLEAN,
  ADD COLUMN IF NOT EXISTS nearest_aum_opening_id   INTEGER REFERENCES aum_openings(id),
  ADD COLUMN IF NOT EXISTS nearest_aum_opening_m    DOUBLE PRECISION;

-- ── Update well_table_view to expose the new columns ─────────────────────────
-- Re-creates the view from the existing definition + the six new fields.
CREATE OR REPLACE VIEW well_table_view AS
 SELECT w.api_no,
    w.well_name,
    w.county,
    w.township,
    w.status,
    w.operator,
    w.operator_address,
    w.operator_phone,
    w.well_type,
    w.lease_name,
    w.well_number,
    w.orphan_status,
    w.in_orphan_program,
    w.total_depth,
    w.deepest_formation,
    w.ip_oil,
    w.ip_gas,
    w.permit_issued,
    w.completion_date,
    w.plug_date,
    w.last_nonzero_production_year,
    w.last_production_quarter,
    w.lat,
    w.lng,
    COALESCE(w.last_nonzero_production_year::integer,
        CASE
            WHEN EXTRACT(year FROM w.completion_date) > 1900::numeric THEN EXTRACT(year FROM w.completion_date)::integer
            ELSE NULL::integer
        END) AS last_active_year,
    CASE
        WHEN w.last_nonzero_production_year IS NOT NULL THEN 'prod'::text
        WHEN w.completion_date IS NOT NULL AND EXTRACT(year FROM w.completion_date) > 1900::numeric THEN 'compl'::text
        ELSE NULL::text
    END AS last_active_source,
    r.priority,
    r.risk_score,
    r.water_risk_score,
    r.population_risk_score,
    r.inactivity_score,
    r.nearest_water_distance_m,
    r.within_protection_zone,
    r.operator_status,
    r.population_within_1km,
    r.population_within_5km,
    r.years_inactive,
    was.admin_status,
    w.state_code,
    -- ── new ODNR hazard columns ──
    r.in_aum_subsidence_zone,
    r.in_aml_project,
    r.in_state_floodplain,
    r.in_dogrm_urban_area,
    r.nearest_aum_opening_m
   FROM wells w
     LEFT JOIN well_risk_scores r ON r.api_no::text = w.api_no::text
     LEFT JOIN well_admin_status was ON was.api_no::text = w.api_no::text;

GRANT SELECT ON odnr_hazard_layers TO anon, authenticated;
GRANT SELECT ON aum_openings       TO anon, authenticated;
GRANT SELECT ON well_table_view    TO anon, authenticated;
