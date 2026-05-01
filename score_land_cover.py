"""
score_land_cover.py

Classifies the dominant ESA WorldCover 2021 land-cover class at each Ohio
well location and writes it to wells.land_cover (smallint, raw WC code).

WorldCover 2021 classes:
   10 Tree cover
   20 Shrubland
   30 Grassland
   40 Cropland
   50 Built-up
   60 Bare / sparse vegetation
   70 Snow and ice
   80 Permanent water bodies
   90 Herbaceous wetland
   95 Mangroves
  100 Moss and lichen

Storing the raw WorldCover code (not a derived label) keeps the DB stable
if we later evolve the label mapping in the frontend.

This is not a risk score — it's geographic context. Unlike the scoring
scripts, we process every row in `wells` that has a lat/lng, not just rows
in well_risk_scores, so downstream views and filters can use land-cover on
every well (including plugged ones).

Usage:
    python score_land_cover.py                    # all unscored wells
    python score_land_cover.py --county ATHENS    # one county
    python score_land_cover.py --reprocess        # wipe + redo (scope-aware)
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
# WorldCover is 10 m. Point-sample is effectively one pixel — plenty for a
# classification label; a buffer-mode would add cost without changing the
# class for >99% of wells. We keep batches small-ish because getInfo()
# payload grows linearly with features.
BATCH_SIZE = 400
SLEEP_S    = 0.2

WORLDCOVER_ASSET = "ESA/WorldCover/v200/2021"


# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def get_counties(cur, county_filter: str | None) -> list[str]:
    sql = """
        SELECT DISTINCT county
        FROM wells
        WHERE lat IS NOT NULL AND lng IS NOT NULL
          AND land_cover_processed_at IS NULL
    """
    params: list = []
    if county_filter:
        sql += " AND UPPER(county) = %s"
        params.append(county_filter.upper())
    sql += " ORDER BY county"
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


def get_wells_for_county(cur, county: str) -> list[dict]:
    cur.execute("""
        SELECT api_no, lat, lng
        FROM wells
        WHERE county = %s
          AND lat IS NOT NULL AND lng IS NOT NULL
          AND land_cover_processed_at IS NULL
        ORDER BY api_no
    """, (county,))
    return [{"api_no": r[0], "lat": r[1], "lng": r[2]} for r in cur.fetchall()]


# ── Earth Engine ───────────────────────────────────────────────────────────────
def get_worldcover() -> ee.Image:
    """Single 10m WorldCover 2021 band, renamed for clarity."""
    return ee.Image(WORLDCOVER_ASSET).select("Map").rename("land_cover")


def classify_batch(wells: list[dict], wc: ee.Image) -> list[tuple[str, int]]:
    """
    Sample WorldCover at each well point. Returns [(api_no, class_code), ...].
    Wells that fall outside Ohio's WorldCover coverage (shouldn't happen, but
    possible near the lake) are silently skipped — the caller keeps them in
    the to-do queue until they're fixed upstream.
    """
    fc = ee.FeatureCollection([
        ee.Feature(ee.Geometry.Point([w["lng"], w["lat"]]), {"api_no": w["api_no"]})
        for w in wells
    ])

    try:
        sampled = wc.sampleRegions(
            collection=fc,
            scale=10,
            geometries=False,
        ).getInfo()["features"]
    except Exception as e:
        print(f"  [EE error] {e}")
        return []

    out: list[tuple[str, int]] = []
    for f in sampled:
        props = f["properties"]
        code = props.get("land_cover")
        if code is None:
            continue
        out.append((props["api_no"], int(code)))
    return out


# ── Core processing ────────────────────────────────────────────────────────────
def process_batch(wells: list[dict], cur, wc: ee.Image) -> int:
    rows = classify_batch(wells, wc)
    if not rows:
        return 0

    psycopg2.extras.execute_values(
        cur,
        """
        UPDATE wells AS w
           SET land_cover              = v.lc,
               land_cover_processed_at = NOW()
          FROM (VALUES %s) AS v(api_no, lc)
         WHERE w.api_no = v.api_no
        """,
        rows,
        template="(%s, %s)",
    )
    return len(rows)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county",    help="Process only this county")
    parser.add_argument("--reprocess", action="store_true",
                        help="Null out land_cover_processed_at for the target scope and re-run")
    args = parser.parse_args()

    print("Initializing Google Earth Engine…")
    ee.Initialize(project=os.getenv("GEE_PROJECT", "earthengine-legacy"))
    print("GEE connected.\n")

    wc = get_worldcover()
    conn = get_conn()
    total = 0

    try:
        with conn.cursor() as cur:
            if args.reprocess:
                if args.county:
                    cur.execute("""
                        UPDATE wells
                           SET land_cover = NULL, land_cover_processed_at = NULL
                         WHERE UPPER(county) = %s
                    """, (args.county.upper(),))
                else:
                    cur.execute("UPDATE wells SET land_cover = NULL, land_cover_processed_at = NULL")
                conn.commit()
                print(f"Reset land_cover (scope: {args.county or 'ALL'})")

            counties = get_counties(cur, args.county)
            print(f"Counties to process: {len(counties)}")

            for county in counties:
                wells = get_wells_for_county(cur, county)
                if not wells:
                    continue

                print(f"\n-- {county} ({len(wells):,} wells) --")

                for i in range(0, len(wells), BATCH_SIZE):
                    batch = wells[i : i + BATCH_SIZE]
                    label = f"  Batch {i // BATCH_SIZE + 1:>3} ({len(batch)} wells)"
                    print(f"{label}…", end=" ", flush=True)

                    saved = process_batch(batch, cur, wc)
                    conn.commit()
                    total += saved
                    print(f"{saved} saved")

                    time.sleep(SLEEP_S)
    finally:
        conn.close()

    print(f"\nDone. Wells classified: {total:,}")
    print("\nClass distribution:")
    print("  SELECT land_cover, COUNT(*)")
    print("  FROM wells WHERE land_cover IS NOT NULL")
    print("  GROUP BY land_cover ORDER BY COUNT(*) DESC;")


if __name__ == "__main__":
    main()
