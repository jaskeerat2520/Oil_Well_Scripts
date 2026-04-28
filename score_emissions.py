"""
score_emissions.py

Detects emission signals at Ohio well sites using two per-well sources:

  1. Methane plume proximity (table: methane_plumes)
       Combined CarbonMapper (Tanager-1 + aircraft) and MethaneAIR (aircraft)
       point detections. Truly per-leak — wells close to a known plume get a
       high score; wells far from any detection get zero plume contribution.

  2. Landsat 9 thermal (LANDSAT/LC09/C02/T1_L2, band ST_B10)
       100 m resolution. Summer-only (Jun-Aug 2022-2024) mean land-surface
       temperature over a 100 m well buffer vs a 1 km background ring.
       Active venting warms the pad by a few °C.

The previous Sentinel-5P CH4 component was removed: its ~7 km pixels smeared
the same score across every well in a cell, producing pixel-shaped red blobs
on the map rather than per-well signals.

Writes to well_remote_sensing:
    nearest_plume_m, nearest_plume_source, max_plume_flux_kgph_5km,
    thermal_well_c, thermal_background_c, thermal_anomaly_c,
    emissions_risk_score, emissions_processed_at
and NULLs the legacy CH4 columns (ch4_well_ppb, ch4_background_ppb,
ch4_anomaly_ratio, ch4_is_anomaly).

Usage:
    python score_emissions.py                 # all unprocessed wells
    python score_emissions.py --county STARK
    python score_emissions.py --reprocess
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
# Landsat 9 is cleanly calibrated from its 2021-10 launch; use 2022 onwards.
THERMAL_START = "2022-06-01"
THERMAL_END   = "2024-09-01"
THERMAL_WELL_BUFFER_M = 100
THERMAL_BG_BUFFER_M   = 1000
THERMAL_CLOUD_MAX     = 20

# Plume proximity search radius — wells beyond this get no plume contribution.
PLUME_SEARCH_M        = 10000
PLUME_FLUX_BONUS_KGPH = 1000   # large-leak bonus threshold

BATCH_SIZE = 80
SLEEP_S    = 0.4


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
          AND (wrs2.emissions_processed_at IS NULL)
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
          AND wrs2.emissions_processed_at IS NULL
        ORDER BY wrs.risk_score DESC NULLS LAST
    """, (county,))
    return [{"api_no": r[0], "lat": r[1], "lng": r[2]} for r in cur.fetchall()]


# ── Plume proximity (per batch, one SQL call) ─────────────────────────────────
def fetch_plume_proximity(cur, api_nos: list[str]) -> dict[str, tuple]:
    """
    For each well in api_nos, return (nearest_m, nearest_source, max_flux_5km).
    Wells with no plume within PLUME_SEARCH_M are omitted from the result
    (callers default missing wells to (None, None, None)).
    """
    cur.execute("""
        WITH candidates AS (
            SELECT w.api_no,
                   p.source,
                   p.emission_kgph,
                   ST_Distance(
                       w.geometry::geography,
                       p.geometry::geography
                   ) AS dist_m
            FROM wells w
            JOIN methane_plumes p
              ON ST_DWithin(
                     w.geometry::geography,
                     p.geometry::geography,
                     %s
                 )
            WHERE w.api_no = ANY(%s)
        )
        SELECT
          api_no,
          MIN(dist_m)                                            AS nearest_m,
          (ARRAY_AGG(source ORDER BY dist_m ASC))[1]             AS nearest_source,
          MAX(emission_kgph) FILTER (WHERE dist_m <= 5000)       AS max_flux_5km
        FROM candidates
        GROUP BY api_no
    """, (PLUME_SEARCH_M, api_nos))
    return {
        row[0]: (row[1], row[2], row[3])
        for row in cur.fetchall()
    }


# ── Earth Engine thermal source ───────────────────────────────────────────────
def get_thermal_mean(region: ee.Geometry) -> ee.Image:
    """
    Landsat 9 Collection-2 L2 thermal: summer mean LST in °C.
    Applies cloud/shadow masking from QA_PIXEL and unit conversion.
    """
    def to_celsius(img):
        qa = img.select("QA_PIXEL")
        cloud  = qa.bitwiseAnd(1 << 3).neq(0)
        shadow = qa.bitwiseAnd(1 << 4).neq(0)
        clear_mask = cloud.Or(shadow).Not()
        lst_c = (
            img.select("ST_B10")
               .multiply(0.00341802).add(149.0)    # DN → Kelvin
               .subtract(273.15)                   # → Celsius
               .rename("lst")
        )
        return lst_c.updateMask(clear_mask)

    return (
        ee.ImageCollection("LANDSAT/LC09/C02/T1_L2")
        .filterBounds(region)
        .filterDate(THERMAL_START, THERMAL_END)
        .filter(ee.Filter.calendarRange(6, 8, "month"))
        .filter(ee.Filter.lt("CLOUD_COVER", THERMAL_CLOUD_MAX))
        .map(to_celsius)
        .mean()
    )


# ── Scoring ────────────────────────────────────────────────────────────────────
def score_emissions(
    nearest_plume_m: float | None,
    max_flux_5km: float | None,
    thermal_delta: float | None,
) -> int:
    """
    Combined 0-100 score from plume proximity + thermal per-well anomaly.

    plume_pts:
        ≤   500 m → +50
        ≤ 1,000 m → +35
        ≤ 2,500 m → +20
        ≤ 5,000 m → +10
        further   →   0
        + flux bonus: +20 if any plume within 5 km has emission_kgph ≥ 1000

    thermal_pts:
        Δ ≥ 8°C → +60
        Δ ≥ 5°C → +40
        Δ ≥ 2°C → +20
        else    →   0
    """
    score = 0

    if nearest_plume_m is not None:
        if   nearest_plume_m <=   500: score += 50
        elif nearest_plume_m <=  1000: score += 35
        elif nearest_plume_m <=  2500: score += 20
        elif nearest_plume_m <=  5000: score += 10

        if max_flux_5km is not None and max_flux_5km >= PLUME_FLUX_BONUS_KGPH:
            score += 20

    if thermal_delta is not None:
        if   thermal_delta >= 8.0: score += 60
        elif thermal_delta >= 5.0: score += 40
        elif thermal_delta >= 2.0: score += 20

    return min(100, score)


# ── Core processing ────────────────────────────────────────────────────────────
def process_batch(wells: list[dict], county: str, cur) -> int:
    api_nos = [w["api_no"] for w in wells]
    points  = [ee.Geometry.Point([w["lng"], w["lat"]]) for w in wells]

    # Thermal image is filterBounds-dependent — rebuild per batch bbox
    bbox_geom = ee.FeatureCollection([
        ee.Feature(p.buffer(THERMAL_BG_BUFFER_M), {}) for p in points
    ]).geometry().bounds()
    thermal_img = get_thermal_mean(bbox_geom)

    def fc(bufs):
        return ee.FeatureCollection([
            ee.Feature(p.buffer(bufs), {"api_no": api})
            for p, api in zip(points, api_nos)
        ])

    fc_thrm_well = fc(THERMAL_WELL_BUFFER_M)
    fc_thrm_bg   = fc(THERMAL_BG_BUFFER_M)

    def reduce_to_map(img, fc_, band, scale):
        result = img.reduceRegions(
            collection=fc_,
            reducer=ee.Reducer.mean(),
            scale=scale,
        ).getInfo()["features"]
        return {f["properties"]["api_no"]: f["properties"].get(band) for f in result}

    try:
        thrm_well = reduce_to_map(thermal_img, fc_thrm_well, "mean", 100)
        thrm_bg   = reduce_to_map(thermal_img, fc_thrm_bg,   "mean", 100)
    except Exception as e:
        print(f"  [EE error] {e}")
        return 0

    # Plume proximity — one SQL call per batch.
    plume_map = fetch_plume_proximity(cur, api_nos)

    rows = []
    for api in api_nos:
        thrm_w = thrm_well.get(api)
        thrm_b = thrm_bg.get(api)
        thrm_delta = (thrm_w - thrm_b) if (thrm_w is not None and thrm_b is not None) else None

        nearest_m, nearest_src, max_flux_5km = plume_map.get(api, (None, None, None))

        score = score_emissions(nearest_m, max_flux_5km, thrm_delta)

        rows.append((
            api, county,
            # legacy CH4 columns NULLed out
            None, None, None, None,
            # thermal
            round(thrm_w, 2)     if thrm_w     is not None else None,
            round(thrm_b, 2)     if thrm_b     is not None else None,
            round(thrm_delta, 2) if thrm_delta is not None else None,
            # plume proximity
            round(nearest_m, 1)   if nearest_m   is not None else None,
            nearest_src,
            round(max_flux_5km, 2) if max_flux_5km is not None else None,
            score,
        ))

    if rows:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO well_remote_sensing
              (api_no, county,
               ch4_well_ppb, ch4_background_ppb, ch4_anomaly_ratio, ch4_is_anomaly,
               thermal_well_c, thermal_background_c, thermal_anomaly_c,
               nearest_plume_m, nearest_plume_source, max_plume_flux_kgph_5km,
               emissions_risk_score, emissions_processed_at)
            VALUES %s
            ON CONFLICT (api_no) DO UPDATE SET
              county                    = EXCLUDED.county,
              ch4_well_ppb              = EXCLUDED.ch4_well_ppb,
              ch4_background_ppb        = EXCLUDED.ch4_background_ppb,
              ch4_anomaly_ratio         = EXCLUDED.ch4_anomaly_ratio,
              ch4_is_anomaly            = EXCLUDED.ch4_is_anomaly,
              thermal_well_c            = EXCLUDED.thermal_well_c,
              thermal_background_c      = EXCLUDED.thermal_background_c,
              thermal_anomaly_c         = EXCLUDED.thermal_anomaly_c,
              nearest_plume_m           = EXCLUDED.nearest_plume_m,
              nearest_plume_source      = EXCLUDED.nearest_plume_source,
              max_plume_flux_kgph_5km   = EXCLUDED.max_plume_flux_kgph_5km,
              emissions_risk_score      = EXCLUDED.emissions_risk_score,
              emissions_processed_at    = NOW()
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
        )

    return len(rows)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county",    help="Process only this county")
    parser.add_argument("--reprocess", action="store_true",
                        help="Null out emissions_processed_at for the target scope and re-run")
    args = parser.parse_args()

    print("Initializing Google Earth Engine…")
    ee.Initialize(project=os.getenv("GEE_PROJECT", "earthengine-legacy"))
    print("GEE connected.\n")

    conn = get_conn()
    total = 0

    try:
        with conn.cursor() as cur:
            if args.reprocess:
                if args.county:
                    cur.execute("""
                        UPDATE well_remote_sensing SET emissions_processed_at = NULL
                        WHERE UPPER(county) = %s
                    """, (args.county.upper(),))
                else:
                    cur.execute("UPDATE well_remote_sensing SET emissions_processed_at = NULL")
                conn.commit()
                print(f"Reset emissions_processed_at (scope: {args.county or 'ALL'})")

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
                    total += saved
                    print(f"{saved} saved")

                    time.sleep(SLEEP_S)
    finally:
        conn.close()

    print(f"\n✓ Done. Wells with emissions scores: {total:,}")
    print("\nTop emission candidates:")
    print("  SELECT api_no, county, nearest_plume_m, nearest_plume_source,")
    print("         max_plume_flux_kgph_5km, thermal_anomaly_c, emissions_risk_score")
    print("  FROM well_remote_sensing")
    print("  WHERE emissions_risk_score >= 50")
    print("  ORDER BY emissions_risk_score DESC LIMIT 20;")


if __name__ == "__main__":
    main()
