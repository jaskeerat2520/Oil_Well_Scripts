# Ohio Well Plugging Prioritization

## Project Overview
Identifies and prioritizes Ohio oil/gas wells that need plugging based on environmental risk.
Combines water source proximity, population exposure, vegetation stress, terrain anomalies,
and emission signals (methane + thermal) into a composite risk score.

## Tech Stack
- **Database**: Supabase PostgreSQL with PostGIS (project: `fdehtiqlmijdnfxzjufi`)
- **Language**: Python 3 with psycopg2 (direct connections, not Supabase client)
- **Spatial**: PostGIS for all geometry operations (ST_DWithin, ST_Intersects, ST_Distance, KNN `<->`)
- **Remote sensing**: Google Earth Engine (earthengine-api) — Sentinel-2, Sentinel-5P, Landsat 9, USGS 3DEP
- **Data sources**: Ohio RBDMS CSV (wells), Ohio EPA SWAP FeatureServer (water protection zones), US Census Bureau API + pygris (population tracts), GEE (satellite imagery + DEM)

## Database Tables
| Table | Rows | Purpose |
|---|---|---|
| `wells` | 242,005 | Clean typed wells with PostGIS geometry |
| `counties` | 88 | Ohio county boundaries |
| `water_sources` | 8,307 | EPA drinking water protection zone polygons |
| `water_source_centroids` | 8,307 | Helper table for fast KNN distance queries |
| `well_risk_scores` | 130,953 | Risk scores for active/unplugged wells only |
| `population_tracts` | 3,168 | Census 2020 tract-level population + geometry |
| `well_surface_anomalies` | up to 130K | Sentinel-2 NDVI/NDMI/NDWI/NBR baseline vs recent + yearly NDVI trend slope |
| `well_remote_sensing` | up to 130K | 3DEP terrain (slope ratio, artificial flatness) + Sentinel-5P CH4 + Landsat 9 thermal |

Note: `oil_gas_wells` (raw CSV staging) was dropped after ETL completed. Re-create with `import_wells.py` if needed.

Views: `orphan_candidates`, `county_summary`

## Pipeline Scripts (run in order)
1. `import_wells.py` — CSV -> `oil_gas_wells` (raw staging)
2. `import_county_geometry.py` — Census county boundaries -> `counties`
3. SQL migration via Supabase MCP — `oil_gas_wells` -> `wells` (typed, with geometry)
4. `ingest_water_sources.py` — Ohio EPA SWAP polygons -> `water_sources`
5. `score_wells.py` — Water proximity scoring -> `well_risk_scores`
6. `backfill_zones.py` — ST_Intersects fix for 3 large counties (MEDINA, STARK, WASHINGTON)
7. `ingest_population.py` — Census 2020 tracts -> `population_tracts`
8. `score_population.py` — Population exposure scoring -> `well_risk_scores`
9. `detect_surface_anomalies.py` — Sentinel-2 NDVI/NDMI/NDWI/NBR + yearly NDVI trend (2017-2024) -> `well_surface_anomalies`
10. `score_terrain.py` — USGS 3DEP slope-ratio artificial-pad detection -> `well_remote_sensing`
11. `score_emissions.py` — Sentinel-5P CH4 + Landsat 9 thermal anomalies -> `well_remote_sensing`
12. `compute_composite.py` — Weighted merge of all five dimensions + priority assignment -> `well_risk_scores`

`satellite_service.py` is a standalone FastAPI microservice (port 8001) for on-demand per-well analysis + thumbnail URLs — used by the frontend, not part of the batch pipeline.
`view_anomaly.py` is an ad-hoc tool that opens a local HTML page with Sentinel-2 before/after thumbnails for flagged wells.

## Key Patterns
- **County-by-county batching**: All scoring scripts process one county at a time to avoid Supabase CPU/timeout limits
- **Resume-safe**: Scripts check which counties are already scored and skip them
- **Two-pass scoring for large counties**: Distance scoring first (fast via centroids), then ST_Intersects separately (expensive)
- **All scripts use `.env`** for DB credentials via `python-dotenv`

## Environment Variables (`.env`)
- `SUPABASE_DB_HOST`, `SUPABASE_DB_PASSWORD` (required)
- `SUPABASE_DB_NAME` (default: postgres), `SUPABASE_DB_USER` (default: postgres), `SUPABASE_DB_PORT` (default: 5432)
- `CSV_FILE` (path to RBDMS well data CSV)
- `GEE_PROJECT` (Google Earth Engine cloud project ID — required for remote-sensing scripts; free tier is fine)

## GEE One-Time Setup
All remote-sensing scripts use `ee.Initialize(project=GEE_PROJECT)`. First run needs:
    pip install earthengine-api
    earthengine authenticate          # opens browser, stores creds in ~/.config/earthengine

## Status Filter (scoring exclusions)
Scoring scripts exclude wells with these statuses — they are already resolved or actively operated:
`'Plugged and Abandoned', 'Final Restoration', 'Storage Well', 'Active Injection', 'Well Permitted', 'Drilling'`
Wells with status `'Producing'` are included but capped at priority = `medium`.

Note: `'Final Restoration'` was added after discovery that 35,607 already-plugged wells were being scored (including 95 fake critical wells). "Final Restoration" = plugged + surface restored, more complete than P&A.

## Known Issues
- `001_water_stations.sql` is unused (was for USGS approach, replaced by Ohio EPA SWAP)
- `etl_wells.py` is unused (SQL approach was faster, kept for reference)
- Large counties can timeout on spatial queries — use longer `statement_timeout` or batch further

## Scoring System
- **Water risk** (0-100, weight 30%): Distance to nearest water source + whether inside protection zone
- **Population risk** (0-100, weight 20%): Population within 1km and 5km of well
- **Vegetation risk** (0-100, weight 20%): Combines NDVI baseline-vs-recent anomaly + multi-year NDVI trend slope (2017-2024) + NDMI brine/salt stress delta. Cropland and built-up masked out via ESA WorldCover 2021.
- **Terrain risk** (0-100, weight 10%): Ratio of mean slope inside 100m well buffer vs 400m surroundings, derived from USGS 3DEP 10m DEM. `is_artificially_flat = true` when bg slope > 1° AND ratio < 0.4.
- **Emissions risk** (0-100, weight 20%): Sentinel-5P CH4 column anomaly vs 10km background (neighborhood scale, ~7km pixels) + Landsat 9 summer thermal anomaly over the 100m well vs 1km background.
- **Composite** (0-100): Weighted average of the five dimensions, renormalized by which components have data for each well (missing dims don't penalize). Populated via `compute_composite.py`.
- **Priority**: critical ≥75, high ≥50, medium ≥25, low <25. `Producing` wells are capped at medium per existing convention.

**Inactivity score is still TODO** — plug_date / completion_date / last_nonzero_production_year columns exist on `wells`, but a score isn't yet computed. When added, reserve weight by shaving 5% off water and population.
