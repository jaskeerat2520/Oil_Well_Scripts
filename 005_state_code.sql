-- Migration: Add state_code to wells, counties, and well_risk_scores
-- Context: Wells-only PoC for multi-state expansion (PA + WV alongside existing OH).
-- counties.fips_code and wells.county_fips are already 5-digit (state+county) FIPS,
-- so cross-state collisions only happen in code that joins on county *name*. state_code
-- adds a fast 2-char filter for state-scoped queries and is what the frontend route
-- segments use (/states/oh, /states/pa, /states/wv).
--
-- No DEFAULT after backfill: forces every new ingest path to specify state_code
-- explicitly so a forgotten assignment fails loudly instead of silently labelling
-- PA wells as Ohio. Apply via Supabase MCP apply_migration.

ALTER TABLE wells            ADD COLUMN IF NOT EXISTS state_code CHAR(2);
ALTER TABLE counties         ADD COLUMN IF NOT EXISTS state_code CHAR(2);
ALTER TABLE well_risk_scores ADD COLUMN IF NOT EXISTS state_code CHAR(2);

UPDATE wells            SET state_code = 'OH' WHERE state_code IS NULL;
UPDATE counties         SET state_code = 'OH' WHERE state_code IS NULL;
UPDATE well_risk_scores SET state_code = 'OH' WHERE state_code IS NULL;

ALTER TABLE wells            ALTER COLUMN state_code SET NOT NULL;
ALTER TABLE counties         ALTER COLUMN state_code SET NOT NULL;
ALTER TABLE well_risk_scores ALTER COLUMN state_code SET NOT NULL;

CREATE INDEX IF NOT EXISTS wells_state_code_idx            ON wells            (state_code);
CREATE INDEX IF NOT EXISTS wells_state_county_idx          ON wells            (state_code, county);
CREATE INDEX IF NOT EXISTS counties_state_code_idx         ON counties         (state_code);
CREATE INDEX IF NOT EXISTS well_risk_scores_state_code_idx ON well_risk_scores (state_code);
