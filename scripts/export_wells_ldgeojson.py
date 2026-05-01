"""
Export the `wells` table (OH + PA + WV, 242K rows) to line-delimited GeoJSON
(NDGeoJSON) for upload as a Mapbox Tiling Service (MTS) source.

Why a separate exporter (and not bake_parcel_tiles.py):
  - bake_parcel_tiles.py runs tippecanoe locally and uploads PMTiles to
    Supabase Storage. The MTS pipeline does its own tile-baking on Mapbox's
    side, so we just need NDGeoJSON.
  - The wells join layers in well_risk_scores (composite_risk_score,
    priority, nearest_school/hospital_distance_m), which only exists for
    Ohio — PA/WV rows fall through with NULL via LEFT JOIN. That's
    expected; the frontend will handle NULL priorities downstream.
  - psycopg2 server-side cursor (named) keeps memory bounded at
    itersize = 10_000 rows per round-trip.

Usage:
    python scripts/export_wells_ldgeojson.py tile-cache/wells.ndjson
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))


# Column allowlist mirrored in tilesets/recipes/wells_all_states.json
# (`features.attributes.allowed_output`). Keep in sync if either changes.
EXPORT_SQL = """
SELECT
    w.api_no,
    w.state_code,
    w.status,
    w.well_type,
    w.county,
    w.last_nonzero_production_year,
    w.plug_date,
    r.composite_risk_score,
    r.priority,
    r.nearest_school_distance_m,
    r.nearest_hospital_distance_m,
    ST_AsGeoJSON(w.geometry) AS geom_json
FROM wells w
LEFT JOIN well_risk_scores r ON r.api_no = w.api_no
WHERE w.geometry IS NOT NULL
"""


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} ...")
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


def export(conn, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    t0 = time.time()
    with conn.cursor(name="wells_export") as cur:
        cur.itersize = 10_000
        cur.execute(EXPORT_SQL)
        with out_path.open("w", encoding="utf-8") as f:
            for row in cur:
                (api_no, state_code, status, well_type, county,
                 last_prod_year, plug_date,
                 composite_risk_score, priority,
                 nearest_school_m, nearest_hospital_m, geom_json) = row
                if not geom_json:
                    continue
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(geom_json),
                    "properties": {
                        "api_no":                       api_no,
                        "state_code":                   state_code,
                        "status":                       status,
                        "well_type":                    well_type,
                        "county":                       county,
                        "last_nonzero_production_year": last_prod_year,
                        "plug_date":                    plug_date.isoformat() if plug_date else None,
                        "composite_risk_score":         float(composite_risk_score) if composite_risk_score is not None else None,
                        "priority":                     priority,
                        "nearest_school_distance_m":    float(nearest_school_m) if nearest_school_m is not None else None,
                        "nearest_hospital_distance_m":  float(nearest_hospital_m) if nearest_hospital_m is not None else None,
                    },
                }
                f.write(json.dumps(feature, separators=(",", ":")))
                f.write("\n")
                n += 1
                if n % 50_000 == 0:
                    rate = n / (time.time() - t0)
                    print(f"[INFO]  {n:>7,} features written ({rate:,.0f}/s)")
    elapsed = time.time() - t0
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"[OK]    {n:,} features -> {out_path} ({size_mb:.1f} MB, {elapsed:.0f}s)")
    return n


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("output", help="Path to write NDGeoJSON")
    args = ap.parse_args()

    if not (DB_HOST and DB_PASSWORD):
        print("[ERROR] SUPABASE_DB_HOST and SUPABASE_DB_PASSWORD must be set in .env", file=sys.stderr)
        sys.exit(1)

    conn = connect()
    try:
        export(conn, Path(args.output))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
