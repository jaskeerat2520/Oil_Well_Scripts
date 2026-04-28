-- Migration: well_pad_detection table for sub-meter pad-detection scoring.
--
-- Built by score_pad_detection.py + pad_detection_worker.py (Cloud Run).
-- Combines NAIP NDVI (absolute + delta) with OSIP RGB Sobel edge ratio in a
-- three-signal scoring model validated across forest (Hocking), shale
-- (Carroll), and cropland (Hancock) terrain.
--
-- Apply via Supabase MCP:
--   mcp__claude_ai_Supabase__apply_migration with this file's contents.

CREATE TABLE IF NOT EXISTS well_pad_detection (
    api_no            TEXT PRIMARY KEY REFERENCES wells(api_no) ON DELETE CASCADE,
    county            TEXT,

    -- Raw NAIP measurements (4-band aerial, ~1 m resolution)
    naip_ndvi_pad     NUMERIC(5,3),    -- mean NDVI inside 15m disk around well
    naip_ndvi_bg      NUMERIC(5,3),    -- mean NDVI in 15-50m annulus
    naip_delta        NUMERIC(5,3),    -- pad - bg; negative = pad less vegetated

    -- Raw OSIP measurements (3-band aerial, ~0.3 m resolution)
    edge_ratio        NUMERIC(6,3),    -- Sobel edge mean: pad / bg

    -- Per-signal contributions (0 if signal silent, see score_pad_detection.py)
    abs_signal        SMALLINT NOT NULL DEFAULT 0,    -- max 30
    delta_signal      SMALLINT NOT NULL DEFAULT 0,    -- max 30
    edge_signal       SMALLINT NOT NULL DEFAULT 0,    -- max 20

    -- Combined: min(80, abs + delta + edge). Threshold "likely pad" at 30.
    pad_score         SMALLINT NOT NULL DEFAULT 0,

    pad_processed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_well_pad_detection_county
    ON well_pad_detection (county);

-- Partial index for the priority subset only — keeps the hot index small
-- while still serving "show me likely-pad wells" queries fast.
CREATE INDEX IF NOT EXISTS idx_well_pad_detection_score_priority
    ON well_pad_detection (pad_score DESC)
    WHERE pad_score >= 30;

COMMENT ON TABLE well_pad_detection IS
    'Sub-meter pad-detection scores from NAIP NDVI + OSIP RGB Sobel. Three independent signal pathways combined. Validated 2026-04-27 on Hocking/Carroll/Hancock — see project_osip_evaluation_20260427 memory note.';

COMMENT ON COLUMN well_pad_detection.abs_signal IS
    'NAIP pad NDVI absolute, tiered: <0.10 → 30, <0.20 → 15, <0.30 → 5';

COMMENT ON COLUMN well_pad_detection.delta_signal IS
    'NAIP pad-vs-bg NDVI delta: <-0.10 → 30, <-0.05 → 15';

COMMENT ON COLUMN well_pad_detection.edge_signal IS
    'OSIP Sobel edge ratio: >1.50 → 20, >1.20 → 10';
