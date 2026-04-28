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
| `well_risk_scores` | 103,824 | Risk scores for active/unplugged wells only (post-2026-04-23 ghost-permit cleanup) |
| `population_tracts` | 3,168 | Census 2020 tract-level population + geometry |
| `well_surface_anomalies` | up to 130K | Sentinel-2 NDVI/NDMI/NDWI/NBR baseline vs recent + yearly NDVI trend slope |
| `well_remote_sensing` | up to 130K | 3DEP terrain (slope ratio, artificial flatness) + Sentinel-5P CH4 + Landsat 9 thermal |
| `parcels` | 5,836,675 (statewide) | Surface-owner parcel polygons from county auditor ArcGIS Online tenants. Includes `owner_mailing_address`/`city`/`state`/`zip` columns (populated for most counties; coverage varies). GIST index `parcels_geom_gist` on `geom`; map UI fetches via `parcels_in_bbox(w,s,e,n,lim)` RPC. |

Note: `oil_gas_wells` (raw CSV staging) was dropped after ETL completed. Re-create with `import_wells.py` if needed.

Views: `orphan_candidates`, `county_summary`, `well_admin_status` (operational classification — extends `well_risk_scores.operator_status` with zombie_producer, paperwork_producer, permit_expired, drilled_never_produced, well_extinct, status_unknown)

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

## Scoring exclusions (applied in `score_wells.py` and `backfill_zones.py`)

Four exclusion rules, all ANDed in the scoring WHERE clause:

1. **Status exclusion** (original): `status NOT IN ('Plugged and Abandoned', 'Final Restoration', 'Storage Well', 'Active Injection', 'Well Permitted', 'Drilling')`
2. **plug_date authoritative** (added 2026-04-23): `plug_date IS NULL` — catches wells that were physically plugged but have an unusual status (`Orphan Well - Ready/Pending`, `Field Inspected, Well Not Found`). 475 wells fixed.
3. **Non-production well_types** (added 2026-04-23): exclude `well_type IN ('Injection', 'Gas storage', 'Water supply', 'Solution mining', 'Observation', 'Stratigraphy test', 'Lost hole', 'Brine for dust control', 'Plugged injection', 'Plugged water supply')` — infrastructure wells, not orphan candidates. ~130 wells.
4. **Ghost permits** (added 2026-04-23): exclude `status IN ('Cancelled', 'Permit Expired') AND (completion_date IS NULL OR completion_date = '1900-01-02') AND last_nonzero_production_year IS NULL` — permits where drilling never occurred. ~26,600 wells. Their lat/lng is a proposed drilling location, not a physical wellbore.

**Do NOT exclude:**
- `status = 'Unknown status'` (26,823 wells) — 99.8% HISTORIC OWNER, real legacy wells concentrated in Lima-Indiana (Hancock/Allen/Auglaize) and SE Ohio. Validated 2026-04-23.
- `status = 'Well Drilled'` with null completion/production (1,791 wells) — same pattern, 98.4% HISTORIC OWNER, real legacy wells.
- `well_type LIKE 'Plugged %'` — the "Plugged" is stale historical metadata, not current plug state. 189 such wells have status = 'Producing' with recent production. Trust `plug_date`, not `well_type` for current plug status.

**Producing-cap revision** (2026-04-23): `status = 'Producing'` wells are capped at `medium` **only when** `last_nonzero_production_year >= 2020` (verified-active producers where enforcement runs through the operator). Zombie producers (Producing status + stale/null last_prod) are NOT capped — they're hidden orphans that should surface at their true composite rank. ~8,600 wells re-tiered upward.

Historical note: `'Final Restoration'` was added earlier after discovery that 35,607 already-plugged wells were being scored (including 95 fake critical wells). "Final Restoration" = plugged + surface restored, more complete than P&A.

## Known Issues
- `001_water_stations.sql` is unused (was for USGS approach, replaced by Ohio EPA SWAP)
- `etl_wells.py` is unused (SQL approach was faster, kept for reference)
- Large counties can timeout on spatial queries — use longer `statement_timeout` or batch further

## Scoring System
- **Water risk** (0-100, weight 25%): Distance bucket to nearest water source (KNN against `water_source_centroids`) plus a tiered protection-zone bonus. Distance buckets: <500m → 90, <1000m → 70, <3000m → 50, <5000m → 30, <10000m → 15, else 5. Zone bonus by `protection_zone` type: `inner_management_zone` (~0.1 km² avg) +25, `source_water_protection_area` (~0.7 km² avg) +10, `surface_water` 0. **Polygons larger than 50 km² are excluded entirely** — Ohio River + huge inland watershed polygons covered 72% of Ohio's land area, so the original binary `+20 if intersects` flag fired for 89% of wells (no signal). After the size filter and the type tiers, only ~1–5% of wells are flagged "within zone." `water_sources.area_km2` is precomputed (geographic ST_Area / 1e6) and indexed; the SQL filters on the column rather than computing area inline. See `score_wells.py` for the SQL.
- **Population risk** (0-100, weight 15%): Population within 1km and 5km of well (Census 2020 tracts).
- **Vegetation risk** (0-100, weight 20%): Combines NDVI baseline-vs-recent anomaly + multi-year NDVI trend slope (2017-2024) + NDMI brine/salt stress delta. Cropland and built-up masked out via ESA WorldCover 2021.
- **Terrain risk** (0-100, weight 10%): Ratio of mean slope inside 100m well buffer vs 400m surroundings, derived from USGS 3DEP 10m DEM. `is_artificially_flat = true` when bg slope > 1° AND ratio < 0.4.
- **Emissions risk** (0-100, weight 20%): Sentinel-5P CH4 column anomaly vs 10km background (neighborhood scale, ~7km pixels) + Landsat 9 summer thermal anomaly over the 100m well vs 1km background.
- **Inactivity risk** (0-100, weight 10%): Years since last non-zero production / plug-or-completion fallback. Set in `well_risk_scores.inactivity_score`; populated by `backfill_production_years.py`. 0 by design for `Producing` wells with recent production.
- **Composite** (0-100): Weighted average of the six dimensions, renormalized by which components have data for each well (missing dims don't penalize). Populated via `compute_composite.py`.
- **Priority**: critical ≥45, high ≥35, medium ≥25, low <25. Thresholds calibrated to the realised composite distribution (RS signals contribute 0s rather than NULLs for wells without anomalies, capping achievable composite well below 100). `Producing` wells with `last_nonzero_production_year >= 2020` are capped at medium; zombie/paperwork producers are not capped — see "Scoring exclusions" section for details.
