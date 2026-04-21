"""
score_terrain.py

Detects artificial well-pad shelves carved into natural Ohio terrain using
USGS 3DEP 10m DEM via Google Earth Engine.

Logic
-----
A drilling pad is graded flat (~0-1° slope) and set into terrain that is
usually rougher. So for each well we compute:

  INNER  (100m radius around wellhead)
      mean_slope_well       — average slope of the pad area
      elevation_stddev_well — terrain roughness on the pad

  OUTER  (400m radius — context)
      mean_slope_bg         — average slope of surrounding terrain
      elevation_stddev_bg   — terrain roughness of surroundings

  slope_ratio = mean_slope_well / mean_slope_bg
  is_artificially_flat = bg > 1.0°  AND  ratio < 0.4

Using a ratio (not absolute slope) auto-normalizes for regional terrain:
a 2° pad in flat Paulding County is natural, but a 2° pad in hilly Athens
County is suspicious.

Writes one row per well to well_remote_sensing (terrain columns only —
emissions columns are filled by score_emissions.py).

Usage:
    python score_terrain.py                  # all 131K wells in risk_scores
    python score_terrain.py --county ATHENS
    python score_terrain.py --reprocess
"""

import os
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
INNER_BUFFER_M  = 100      # ≈ well pad + immediate edge
OUTER_BUFFER_M  = 400      # ≈ surrounding terrain context

# Flatness classification thresholds
FLAT_RATIO_THRESH = 0.4    # pad slope must be < 40% of surrounding
MIN_BG_SLOPE_DEG  = 1.0    # only meaningful where surroundings aren't already flat

BATCH_SIZE = 120           # DEM reductions are cheap — larger batches OK
SLEEP_S    = 0.3


# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def get_counties(cur, county_filter: str | None) -> list[str]:
    sql = """
        SELECT DISTINCT w.county
        FROM wells w
        JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
        LEFT JOIN well_remote_sensing wrs2 ON w.api_no = wrs2.api_no
        WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL
          AND (wrs2.terrain_processed_at IS NULL)
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
        LEFT JOIN well_remote_sensing wrs2 ON w.api_no = wrs2.api_no
        WHERE w.county = %s
          AND w.lat IS NOT NULL AND w.lng IS NOT NULL
          AND wrs2.terrain_processed_at IS NULL
        ORDER BY wrs.risk_score DESC NULLS LAST
    """, (county,))
    return [{"api_no": r[0], "lat": r[1], "lng": r[2]} for r in cur.fetchall()]


# ── Earth Engine helpers ───────────────────────────────────────────────────────
def get_terrain_stack() -> ee.Image:
    """
    Returns a 2-band image: ['slope' (degrees), 'elevation' (meters)]
    backed by USGS 3DEP 10m DEM.

    Note: the old single-image asset `USGS/3DEP/10m` was superseded by the
    tiled collection `USGS/3DEP/10m_collection`. `.mosaic()` flattens the
    per-tile ImageCollection back into a single virtual image so we can
    apply `ee.Terrain.slope()` and `addBands()` as before.
    """
    dem = ee.ImageCollection("USGS/3DEP/10m_collection").mosaic().select("elevation")
    slope = ee.Terrain.slope(dem).rename("slope")
    return slope.addBands(dem)


def reduce_batch(fc: ee.FeatureCollection, terrain: ee.Image) -> list[dict]:
    """
    Mean + stdDev of (slope, elevation) per feature, returned as a list of
    properties dicts, one per feature.
    """
    reducer = ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True)
    reduced = terrain.reduceRegions(
        collection=fc,
        reducer=reducer,
        scale=10,
    )
    return [f["properties"] for f in reduced.getInfo()["features"]]


# ── Scoring ────────────────────────────────────────────────────────────────────
def score_terrain(slope_well: float | None, slope_bg: float | None,
                  rough_well: float | None, rough_bg: float | None
                  ) -> tuple[float | None, bool, int]:
    """
    Returns (slope_ratio, is_artificially_flat, terrain_risk_score).

    Scoring (0-100):
      100  — clearly artificial pad: bg > 1° AND ratio < 0.25
       70  — strongly anomalous:                  ratio < 0.4
       40  — moderately flat:                     ratio < 0.6
       15  — mildly flatter than surroundings:    ratio < 0.8
        0  — no signal (or terrain already flat everywhere)
    """
    if slope_well is None or slope_bg is None or slope_bg <= 0:
        return None, False, 0

    ratio = slope_well / slope_bg
    flat = (slope_bg > MIN_BG_SLOPE_DEG) and (ratio < FLAT_RATIO_THRESH)

    if slope_bg <= MIN_BG_SLOPE_DEG:
        # Surrounding terrain already flat — no useful signal
        return round(ratio, 3), False, 0

    if ratio < 0.25:   score = 100
    elif ratio < 0.40: score = 70
    elif ratio < 0.60: score = 40
    elif ratio < 0.80: score = 15
    else:              score = 0

    return round(ratio, 3), flat, score


# ── Core processing ────────────────────────────────────────────────────────────
def process_batch(wells: list[dict], county: str, cur, terrain: ee.Image) -> int:
    points = [ee.Geometry.Point([w["lng"], w["lat"]]) for w in wells]

    inner_fc = ee.FeatureCollection([
        ee.Feature(p.buffer(INNER_BUFFER_M), {"api_no": w["api_no"]})
        for p, w in zip(points, wells)
    ])
    outer_fc = ee.FeatureCollection([
        ee.Feature(p.buffer(OUTER_BUFFER_M), {"api_no": w["api_no"]})
        for p, w in zip(points, wells)
    ])

    try:
        inner_results = reduce_batch(inner_fc, terrain)
        outer_results = reduce_batch(outer_fc, terrain)
    except Exception as e:
        print(f"  [EE error] {e}")
        return 0

    # Index outer by api_no for the join
    outer_by_api = {r["api_no"]: r for r in outer_results}

    rows = []
    for inner in inner_results:
        api_no = inner["api_no"]
        outer  = outer_by_api.get(api_no, {})

        slope_well = inner.get("slope_mean")
        slope_bg   = outer.get("slope_mean")
        rough_well = inner.get("elevation_stdDev")
        rough_bg   = outer.get("elevation_stdDev")

        ratio, flat, score = score_terrain(slope_well, slope_bg, rough_well, rough_bg)

        rows.append((
            api_no, county,
            round(slope_well, 3) if slope_well is not None else None,
            round(slope_bg,   3) if slope_bg   is not None else None,
            ratio,
            round(rough_well, 3) if rough_well is not None else None,
            round(rough_bg,   3) if rough_bg   is not None else None,
            flat, score,
        ))

    if rows:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO well_remote_sensing
              (api_no, county,
               mean_slope_well, mean_slope_bg, slope_ratio,
               elevation_stddev_well, elevation_stddev_bg,
               is_artificially_flat, terrain_risk_score, terrain_processed_at)
            VALUES %s
            ON CONFLICT (api_no) DO UPDATE SET
              county                = EXCLUDED.county,
              mean_slope_well       = EXCLUDED.mean_slope_well,
              mean_slope_bg         = EXCLUDED.mean_slope_bg,
              slope_ratio           = EXCLUDED.slope_ratio,
              elevation_stddev_well = EXCLUDED.elevation_stddev_well,
              elevation_stddev_bg   = EXCLUDED.elevation_stddev_bg,
              is_artificially_flat  = EXCLUDED.is_artificially_flat,
              terrain_risk_score    = EXCLUDED.terrain_risk_score,
              terrain_processed_at  = NOW()
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
        )

    return len(rows)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county",    help="Process only this county")
    parser.add_argument("--reprocess", action="store_true",
                        help="Null out terrain_processed_at for the target scope and re-run")
    args = parser.parse_args()

    print("Initializing Google Earth Engine…")
    ee.Initialize(project=os.getenv("GEE_PROJECT", "earthengine-legacy"))
    print("GEE connected.\n")

    terrain = get_terrain_stack()
    conn = get_conn()
    total = 0

    try:
        with conn.cursor() as cur:
            if args.reprocess:
                if args.county:
                    cur.execute("""
                        UPDATE well_remote_sensing SET terrain_processed_at = NULL
                        WHERE UPPER(county) = %s
                    """, (args.county.upper(),))
                else:
                    cur.execute("UPDATE well_remote_sensing SET terrain_processed_at = NULL")
                conn.commit()
                print(f"Reset terrain_processed_at (scope: {args.county or 'ALL'})")

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

                    saved = process_batch(batch, county, cur, terrain)
                    conn.commit()
                    total += saved
                    print(f"{saved} saved")

                    time.sleep(SLEEP_S)
    finally:
        conn.close()

    print(f"\n✓ Done. Wells with terrain scores: {total:,}")
    print("\nArtificially flat candidates:")
    print("  SELECT api_no, county, mean_slope_well, mean_slope_bg, slope_ratio")
    print("  FROM well_remote_sensing")
    print("  WHERE is_artificially_flat = TRUE")
    print("  ORDER BY slope_ratio ASC LIMIT 20;")


if __name__ == "__main__":
    main()
