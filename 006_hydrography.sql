-- ─────────────────────────────────────────────────────────────────────────────
-- 006_hydrography.sql
--
-- Surface hydrography (actual rivers + lakes) sourced from USGS NHD small-scale.
-- This is a *visualization* layer — distinct from `water_sources` (Ohio EPA SWAP),
-- which remains the regulatory protection-zone dataset used for risk scoring.
--
-- Why a new table:
--   `water_sources.surface_water_*` rows are catchment polygons (200–16,000 km²),
--   not waterways themselves. Some Ohio River rows have a sentinel area of 1e9
--   km² that the `< 50` filter strips, so the river never renders. Rather than
--   patching SWAP, we store NHD features alongside it.
--
-- Geometry column is generic `geometry(Geometry, 4326)` because the table holds
-- both LineString/MultiLineString (flowlines) and Polygon/MultiPolygon
-- (waterbodies). Mapbox layers filter on `feature_type` to render each style.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS hydrography (
    id              BIGSERIAL PRIMARY KEY,
    feature_type    TEXT      NOT NULL CHECK (feature_type IN ('flowline', 'waterbody')),
    gnis_name       TEXT,
    fcode           INTEGER,
    ftype           TEXT,
    stream_order    SMALLINT,                       -- flowlines only
    area_km2        DOUBLE PRECISION,               -- waterbodies only (from AREASQKM)
    geometry        geometry(Geometry, 4326) NOT NULL,
    source          TEXT      NOT NULL DEFAULT 'usgs_nhd_small',
    state_code      CHAR(2)   NOT NULL DEFAULT 'OH',
    raw_attrs       JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS hydrography_geom_gist
    ON hydrography USING GIST (geometry);

CREATE INDEX IF NOT EXISTS hydrography_feature_type_idx
    ON hydrography (feature_type);

CREATE INDEX IF NOT EXISTS hydrography_state_code_idx
    ON hydrography (state_code);
