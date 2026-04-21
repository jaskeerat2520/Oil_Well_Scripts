"""
detect_surface_anomalies.py

Vegetation-based surface anomaly detection for Ohio wells using Sentinel-2.

For each well in well_risk_scores, computes:

  1. Baseline (2017-2019) vs recent (2023-2024) composites of four indices:
       NDVI  (B8 - B4)/(B8 + B4)   vegetation greenness
       NDMI  (B8 - B11)/(B8 + B11) leaf moisture / brine-salt stress (NEW)
       NDWI  (B3 - B8)/(B3 + B8)   surface water / brine pools
       NBR   (B8 - B12)/(B8 + B12) bare soil / burn / SWIR disturbance

  2. Per-year NDVI medians for 2017-2024 (growing season only), fit to a
     linear trend to detect slow, multi-year decline that binary before/after
     comparisons miss.

Writes one row per well to well_surface_anomalies with:
  - baseline_*, recent_*, *_change for all four indices
  - ndvi_trend_slope / ndvi_trend_r2 / ndvi_years_sampled / ndvi_yearly_values
  - anomaly_score (0-100) + anomaly_type label

Agricultural and built-up land is masked via ESA WorldCover 2021 to avoid
crop-rotation false positives.

Usage:
    python detect_surface_anomalies.py                       # all 131K wells
    python detect_surface_anomalies.py --county ATHENS       # one county
    python detect_surface_anomalies.py --reprocess           # wipe + redo
"""

import os
import json
import time
import argparse
import psycopg2
import psycopg2.extras
import ee
from dotenv import load_dotenv

load_dotenv()

# ── DB config ──────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

# ── Analysis config ────────────────────────────────────────────────────────────
# Growing-season composites only — winter collapses NDVI state-wide
GROWING_MONTHS = ("04-01", "10-31")

BASELINE_YEARS = (2017, 2018, 2019)
RECENT_YEARS   = (2023, 2024)
TREND_YEARS    = tuple(range(2017, 2025))   # 2017..2024 inclusive

BUFFER_M   = 150
CLOUD_PCT  = 25
BATCH_SIZE = 200                             # increased from 50 — amortizes 16-band stack cost
SLEEP_S    = 0.0                             # removed 1,300+ seconds of unnecessary sleep

# ESA WorldCover 2021 classes to mask (crop-rotation false positives, built-up)
LANDCOVER_EXCLUDE = [40, 50]                 # 40=cropland, 50=built-up


# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def get_counties(cur, county_filter: str | None) -> list[str]:
    """Counties that have scored wells not yet processed for anomalies."""
    sql = """
        SELECT DISTINCT w.county
        FROM wells w
        JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
        WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL
          AND w.api_no NOT IN (SELECT api_no FROM well_surface_anomalies)
    """
    params: list = []
    if county_filter:
        sql += " AND UPPER(w.county) = %s"
        params.append(county_filter.upper())
    sql += " ORDER BY w.county"
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


def get_wells_for_county(cur, county: str) -> list[dict]:
    cur.execute("""
        SELECT w.api_no, w.lat, w.lng
        FROM wells w
        JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
        WHERE w.county = %s
          AND w.lat IS NOT NULL AND w.lng IS NOT NULL
          AND w.api_no NOT IN (SELECT api_no FROM well_surface_anomalies)
        ORDER BY wrs.risk_score DESC NULLS LAST
    """, (county,))
    return [{"api_no": r[0], "lat": r[1], "lng": r[2]} for r in cur.fetchall()]


# ── Earth Engine helpers ───────────────────────────────────────────────────────
def mask_s2_clouds(img: ee.Image) -> ee.Image:
    """Mask cloud shadow (SCL=3) and cloud/snow (SCL 8-11)."""
    scl = img.select("SCL")
    return img.updateMask(scl.neq(3).And(scl.lt(8).Or(scl.gt(10))))


def s2_col(region: ee.Geometry, start: str, end: str) -> ee.ImageCollection:
    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_PCT))
        .map(mask_s2_clouds)
    )


def indices_composite(region: ee.Geometry, years: tuple[int, ...], prefix: str) -> ee.Image:
    """
    Median composite over growing seasons of the given years, with all four
    indices as named bands: {prefix}_ndvi, {prefix}_ndmi, {prefix}_ndwi, {prefix}_nbr.
    """
    start = f"{years[0]}-{GROWING_MONTHS[0]}"
    end   = f"{years[-1]}-{GROWING_MONTHS[1]}"
    col = s2_col(region, start, end)

    def add_idx(img):
        ndvi = img.normalizedDifference(["B8", "B4"]).rename("ndvi")
        ndmi = img.normalizedDifference(["B8", "B11"]).rename("ndmi")  # SWIR1 → brine/salt
        ndwi = img.normalizedDifference(["B3", "B8"]).rename("ndwi")
        nbr  = img.normalizedDifference(["B8", "B12"]).rename("nbr")   # SWIR2 → bare soil
        return img.addBands([ndvi, ndmi, ndwi, nbr])

    composite = col.map(add_idx).select(["ndvi", "ndmi", "ndwi", "nbr"]).median()
    return composite.rename([f"{prefix}_ndvi", f"{prefix}_ndmi",
                             f"{prefix}_ndwi", f"{prefix}_nbr"])


def yearly_ndvi_band(region: ee.Geometry, year: int) -> ee.Image:
    """Single-year growing-season NDVI median, renamed ndvi_{year}."""
    col = s2_col(region, f"{year}-{GROWING_MONTHS[0]}", f"{year}-{GROWING_MONTHS[1]}")
    return col.median().normalizedDifference(["B8", "B4"]).rename(f"ndvi_{year}")


def build_stack(region: ee.Geometry) -> ee.Image:
    """
    16-band stack: 4 baseline indices + 4 recent indices + 8 yearly NDVI,
    all masked to exclude cropland and built-up.
    """
    baseline = indices_composite(region, BASELINE_YEARS, "baseline")
    recent   = indices_composite(region, RECENT_YEARS,   "recent")

    yearly = ee.Image.cat(*[yearly_ndvi_band(region, y) for y in TREND_YEARS])

    stack = baseline.addBands(recent).addBands(yearly)

    # Mask cropland / built-up (ESA WorldCover 2021)
    lc = ee.Image("ESA/WorldCover/v200/2021").select("Map")
    mask = lc.neq(LANDCOVER_EXCLUDE[0])
    for cls in LANDCOVER_EXCLUDE[1:]:
        mask = mask.And(lc.neq(cls))

    return stack.updateMask(mask)


# ── Python-side helpers ────────────────────────────────────────────────────────
def fit_trend(year_vals: dict[int, float | None]) -> tuple[float | None, float | None, int]:
    """
    Ordinary-least-squares linear fit: x = year, y = NDVI.
    Returns (slope_per_year, r_squared, n_points_used).
    """
    pts = [(y, v) for y, v in year_vals.items() if v is not None]
    n = len(pts)
    if n < 3:
        return None, None, n

    xs, ys = zip(*pts)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    num = sum((x - mean_x) * (y - mean_y) for x, y in pts)
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return None, None, n

    slope     = num / den
    intercept = mean_y - slope * mean_x
    ss_res    = sum((y - (slope * x + intercept)) ** 2 for x, y in pts)
    ss_tot    = sum((y - mean_y) ** 2 for y in ys)
    r2        = (1 - ss_res / ss_tot) if ss_tot > 0 else None

    return slope, r2, n


def ndvi_change_to_score(change: float | None, baseline: float | None
                         ) -> tuple[int, str, float | None]:
    """
    Relative-change → anomaly bucket. Baselines below 0.25 are skipped
    (bare soil or impervious — NDVI is noise there).
    """
    if baseline is None or baseline < 0.25:
        return 0, "low_baseline_skip", None
    if change is None:
        return 0, "no_data", None

    rel = change / baseline
    if rel >= -0.06: return 0,   "stable",          rel
    if rel >= -0.12: return 15,  "minor_change",    rel
    if rel >= -0.20: return 30,  "moderate_change", rel
    if rel >= -0.35: return 60,  "vegetation_loss", rel
    if rel >= -0.55: return 80,  "severe_loss",     rel
    return 100, "near_total_loss", rel


# ── Core processing ────────────────────────────────────────────────────────────
def process_batch(wells: list[dict], county: str, cur) -> int:
    features = [
        ee.Feature(
            ee.Geometry.Point([w["lng"], w["lat"]]).buffer(BUFFER_M),
            {"api_no": w["api_no"]}
        )
        for w in wells
    ]
    fc   = ee.FeatureCollection(features)
    bbox = fc.geometry()  # use buffer union instead of bounding rectangle

    try:
        stack = build_stack(bbox)
        reduced = stack.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.mean(),
            scale=10,
        )
        results = reduced.getInfo()["features"]
    except Exception as e:
        print(f"  [EE error] {e}")
        return 0

    rows = []
    for feat in results:
        p = feat.get("properties", {})
        api_no = p.get("api_no")

        b_ndvi = p.get("baseline_ndvi"); r_ndvi = p.get("recent_ndvi")
        b_ndmi = p.get("baseline_ndmi"); r_ndmi = p.get("recent_ndmi")
        b_ndwi = p.get("baseline_ndwi"); r_ndwi = p.get("recent_ndwi")
        b_nbr  = p.get("baseline_nbr");  r_nbr  = p.get("recent_nbr")

        ndvi_change = (r_ndvi - b_ndvi) if (b_ndvi is not None and r_ndvi is not None) else None
        ndmi_change = (r_ndmi - b_ndmi) if (b_ndmi is not None and r_ndmi is not None) else None
        ndwi_change = (r_ndwi - b_ndwi) if (b_ndwi is not None and r_ndwi is not None) else None
        nbr_change  = (r_nbr  - b_nbr)  if (b_nbr  is not None and r_nbr  is not None) else None

        year_vals = {y: p.get(f"ndvi_{y}") for y in TREND_YEARS}
        slope, r2, n_years = fit_trend(year_vals)

        score, atype, relative = ndvi_change_to_score(ndvi_change, b_ndvi)

        rows.append((
            api_no, county,
            b_ndvi, r_ndvi, ndvi_change, relative,
            b_ndmi, r_ndmi, ndmi_change,
            ndwi_change, nbr_change,
            slope, r2, n_years, json.dumps({str(k): v for k, v in year_vals.items()}),
            score, score > 0, atype,
            f"{BASELINE_YEARS[0]}-{BASELINE_YEARS[-1]}",
            f"{RECENT_YEARS[0]}-{RECENT_YEARS[-1]}",
        ))

    if rows:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO well_surface_anomalies
              (api_no, county,
               baseline_ndvi, recent_ndvi, ndvi_change, ndvi_relative,
               baseline_ndmi, recent_ndmi, ndmi_change,
               ndwi_change, nbr_change,
               ndvi_trend_slope, ndvi_trend_r2, ndvi_years_sampled, ndvi_yearly_values,
               anomaly_score, anomaly_detected, anomaly_type,
               baseline_period, recent_period)
            VALUES %s
            ON CONFLICT (api_no) DO UPDATE SET
              baseline_ndvi       = EXCLUDED.baseline_ndvi,
              recent_ndvi         = EXCLUDED.recent_ndvi,
              ndvi_change         = EXCLUDED.ndvi_change,
              ndvi_relative       = EXCLUDED.ndvi_relative,
              baseline_ndmi       = EXCLUDED.baseline_ndmi,
              recent_ndmi         = EXCLUDED.recent_ndmi,
              ndmi_change         = EXCLUDED.ndmi_change,
              ndwi_change         = EXCLUDED.ndwi_change,
              nbr_change          = EXCLUDED.nbr_change,
              ndvi_trend_slope    = EXCLUDED.ndvi_trend_slope,
              ndvi_trend_r2       = EXCLUDED.ndvi_trend_r2,
              ndvi_years_sampled  = EXCLUDED.ndvi_years_sampled,
              ndvi_yearly_values  = EXCLUDED.ndvi_yearly_values,
              anomaly_score       = EXCLUDED.anomaly_score,
              anomaly_detected    = EXCLUDED.anomaly_detected,
              anomaly_type        = EXCLUDED.anomaly_type,
              processed_at        = NOW()
        """, rows)

    return len(rows)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county",    help="Process only this county (e.g. ATHENS)")
    parser.add_argument("--reprocess", action="store_true",
                        help="Wipe existing rows for the target scope and re-run")
    args = parser.parse_args()

    print("Initializing Google Earth Engine…")
    ee.Initialize(project=os.getenv("GEE_PROJECT", "earthengine-legacy"))
    print("GEE connected.\n")

    conn = get_conn()
    total_saved = 0

    try:
        with conn.cursor() as cur:
            if args.reprocess:
                if args.county:
                    cur.execute("DELETE FROM well_surface_anomalies WHERE UPPER(county) = %s",
                                (args.county.upper(),))
                else:
                    cur.execute("DELETE FROM well_surface_anomalies")
                conn.commit()
                print(f"Cleared existing rows (scope: {args.county or 'ALL'})")

            counties = get_counties(cur, args.county)
            print(f"Counties to process: {len(counties)}")

            for county in counties:
                wells = get_wells_for_county(cur, county)
                if not wells:
                    continue

                print(f"\n── {county} ({len(wells):,} wells) ──")

                for i in range(0, len(wells), BATCH_SIZE):
                    batch = wells[i : i + BATCH_SIZE]
                    label = f"  Batch {i // BATCH_SIZE + 1:>3} ({len(batch)} wells)"
                    print(f"{label}…", end=" ", flush=True)

                    saved = process_batch(batch, county, cur)
                    conn.commit()
                    total_saved += saved
                    print(f"{saved} saved")

                    time.sleep(SLEEP_S)
    finally:
        conn.close()

    print(f"\n✓ Done. Total wells recorded: {total_saved:,}")
    print("\nQuick-look query:")
    print("  SELECT api_no, county, ndvi_relative, ndvi_trend_slope, ndmi_change, anomaly_type")
    print("  FROM well_surface_anomalies")
    print("  WHERE anomaly_detected = true")
    print("  ORDER BY ndvi_trend_slope ASC NULLS LAST LIMIT 20;")


if __name__ == "__main__":
    main()
