"""
score_emissions.py

Detects active leak / emission signals at Ohio well sites using two sensors:

  1. Sentinel-5P CH4  (COPERNICUS/S5P/OFFL/L3_CH4)
       ~7 km resolution. Multi-year mean (2021-2024) for the 5.5 km disk
       containing the well vs a 10 km background disk.
       Neighborhood-scale: wells inside the same cell share the signal.

  2. Landsat 9 thermal (LANDSAT/LC09/C02/T1_L2, band ST_B10)
       100 m resolution. Summer-only (Jun-Aug) mean land-surface temperature
       over 100 m well buffer vs 1 km background ring.
       Per-well scale: active venting warms the pad by a few °C.

Writes emissions columns of well_remote_sensing and an emissions_risk_score
(0-100) combining both signals.

Usage:
    python score_emissions.py                 # all 131K wells
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
CH4_START = "2021-01-01"
CH4_END   = "2024-12-31"
CH4_WELL_BUFFER_M = 5500
CH4_BG_BUFFER_M   = 10000

# Landsat-9 is cleanly calibrated from its 2021-10 launch; use 2022 onwards
THERMAL_START = "2022-06-01"
THERMAL_END   = "2024-09-01"
THERMAL_WELL_BUFFER_M = 100
THERMAL_BG_BUFFER_M   = 1000
THERMAL_CLOUD_MAX     = 20

# Anomaly thresholds
CH4_ANOMALY_RATIO      = 1.05    # well column > 5% above background
THERMAL_ANOMALY_DELTA  = 2.0     # well warmer by >2°C than surroundings

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


# ── Earth Engine source images ─────────────────────────────────────────────────
def get_ch4_mean() -> ee.Image:
    return (
        ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_CH4")
        .filterDate(CH4_START, CH4_END)
        .select("CH4_column_volume_mixing_ratio_dry_air")
        .mean()
        .rename("ch4")
    )


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
def score_emissions(ch4_ratio: float | None, thermal_delta: float | None) -> int:
    """
    Combined 0-100 score from CH4 neighborhood anomaly + thermal per-well anomaly.

    CH4:     1.05 → 30, 1.10 → 60, 1.15+ → 80
    Thermal: 2°C → 20, 5°C → 40, 8°C+ → 60
    Sum capped at 100.
    """
    score = 0

    if ch4_ratio is not None:
        if   ch4_ratio >= 1.15: score += 80
        elif ch4_ratio >= 1.10: score += 60
        elif ch4_ratio >= 1.05: score += 30

    if thermal_delta is not None:
        if   thermal_delta >= 8.0: score += 60
        elif thermal_delta >= 5.0: score += 40
        elif thermal_delta >= 2.0: score += 20

    return min(100, score)


# ── Core processing ────────────────────────────────────────────────────────────
def process_batch(wells: list[dict], county: str, cur,
                  ch4_img: ee.Image) -> int:
    points = [ee.Geometry.Point([w["lng"], w["lat"]]) for w in wells]
    api_nos = [w["api_no"] for w in wells]

    # Thermal image is filterBounds-dependent — rebuild per batch bbox
    bbox_geom = ee.FeatureCollection([
        ee.Feature(p.buffer(THERMAL_BG_BUFFER_M), {}) for p in points
    ]).geometry().bounds()
    thermal_img = get_thermal_mean(bbox_geom)

    # Build four feature collections: CH4 well/bg, thermal well/bg
    def fc(bufs):
        return ee.FeatureCollection([
            ee.Feature(p.buffer(bufs), {"api_no": api})
            for p, api in zip(points, api_nos)
        ])

    fc_ch4_well  = fc(CH4_WELL_BUFFER_M)
    fc_ch4_bg    = fc(CH4_BG_BUFFER_M)
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
        ch4_well  = reduce_to_map(ch4_img,     fc_ch4_well,  "mean", 5500)
        ch4_bg    = reduce_to_map(ch4_img,     fc_ch4_bg,    "mean", 5500)
        thrm_well = reduce_to_map(thermal_img, fc_thrm_well, "mean", 100)
        thrm_bg   = reduce_to_map(thermal_img, fc_thrm_bg,   "mean", 100)
    except Exception as e:
        print(f"  [EE error] {e}")
        return 0

    rows = []
    for api in api_nos:
        ch4_w  = ch4_well.get(api)
        ch4_b  = ch4_bg.get(api)
        thrm_w = thrm_well.get(api)
        thrm_b = thrm_bg.get(api)

        ch4_ratio = (ch4_w / ch4_b) if (ch4_w and ch4_b and ch4_b > 0) else None
        ch4_flag  = ch4_ratio is not None and ch4_ratio >= CH4_ANOMALY_RATIO

        thrm_delta = (thrm_w - thrm_b) if (thrm_w is not None and thrm_b is not None) else None

        score = score_emissions(ch4_ratio, thrm_delta)

        rows.append((
            api, county,
            round(ch4_w, 2)    if ch4_w    is not None else None,
            round(ch4_b, 2)    if ch4_b    is not None else None,
            round(ch4_ratio, 4) if ch4_ratio is not None else None,
            ch4_flag,
            round(thrm_w, 2)   if thrm_w   is not None else None,
            round(thrm_b, 2)   if thrm_b   is not None else None,
            round(thrm_delta, 2) if thrm_delta is not None else None,
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
               emissions_risk_score, emissions_processed_at)
            VALUES %s
            ON CONFLICT (api_no) DO UPDATE SET
              county                 = EXCLUDED.county,
              ch4_well_ppb           = EXCLUDED.ch4_well_ppb,
              ch4_background_ppb     = EXCLUDED.ch4_background_ppb,
              ch4_anomaly_ratio      = EXCLUDED.ch4_anomaly_ratio,
              ch4_is_anomaly         = EXCLUDED.ch4_is_anomaly,
              thermal_well_c         = EXCLUDED.thermal_well_c,
              thermal_background_c   = EXCLUDED.thermal_background_c,
              thermal_anomaly_c      = EXCLUDED.thermal_anomaly_c,
              emissions_risk_score   = EXCLUDED.emissions_risk_score,
              emissions_processed_at = NOW()
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
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

    ch4_img = get_ch4_mean()
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

                    saved = process_batch(batch, county, cur, ch4_img)
                    conn.commit()
                    total += saved
                    print(f"{saved} saved")

                    time.sleep(SLEEP_S)
    finally:
        conn.close()

    print(f"\n✓ Done. Wells with emissions scores: {total:,}")
    print("\nTop emission candidates:")
    print("  SELECT api_no, county, ch4_anomaly_ratio, thermal_anomaly_c, emissions_risk_score")
    print("  FROM well_remote_sensing")
    print("  WHERE emissions_risk_score >= 50")
    print("  ORDER BY emissions_risk_score DESC LIMIT 20;")


if __name__ == "__main__":
    main()
