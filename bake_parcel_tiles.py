"""
Bake the parcels table into a PMTiles vector-tile archive that the well-viewer
loads directly via the pmtiles:// protocol.

Why a static bake instead of live ST_AsMVT tiles?
    Parcel ownership and land use change on yearly timescales, not minutes.
    Pre-baking once per quarter gives sub-50ms tile fetches off the CDN with
    zero DB load — and the join with state_owned_parcels (the SORP overlay)
    only has to run once.

Pipeline:
    1. Stream parcels JOIN state_owned_parcels via psycopg2 server-side cursor
       (memory-bounded — 5.9M rows would not fit in RAM otherwise).
    2. Derive land_use_class from Ohio CAUV three-digit codes inside the SQL.
    3. Emit NDGeoJSON to disk, one feature per line.
    4. Run tippecanoe to produce parcels.pmtiles (z6–z14).
    5. Optionally upload to Supabase Storage.

Tippecanoe is not natively packaged for Windows; run this script under WSL
or in a Linux container. The script aborts with a clear error on win32.

Usage:
    python bake_parcel_tiles.py                          # statewide, no upload
    python bake_parcel_tiles.py --county HOCKING         # pilot county only
    python bake_parcel_tiles.py --upload                 # also push to Storage
    python bake_parcel_tiles.py --skip-tippecanoe        # just emit NDGeoJSON

Env vars required:
    SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD            (always)
    NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (only with --upload)

Storage setup (one-time, via Supabase dashboard):
    Create a public bucket named `parcels-tiles`. Files inside it are served
    at <SUPABASE_URL>/storage/v1/object/public/parcels-tiles/<path>.
"""

import argparse
import json
import os
import shutil
import subprocess
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

OUT_DIR = Path("tile-cache")
NDJSON_PATH = OUT_DIR / "parcels.ndjson"
PMTILES_PATH = OUT_DIR / "parcels.pmtiles"

# Ohio property classes follow a three-digit hundreds-bucket scheme set by
# the Ohio Department of Taxation. The first digit is the broad use category;
# the next two are sub-types we don't need for visual coloring.
#   1xx — Agricultural (CAUV)
#   2xx — Mineral / vacant residential
#   3xx — Industrial
#   4xx — Commercial
#   5xx — Single-family residential
#   6xx — Multi-family residential
#   7xx — Tax-exempt (government, religious, schools)
#   8xx — Public utility, railroad
#   9xx — Conservation, forest, public lands, vacant
# The SORP overlay (is_state_owned) wins visually over land_use_class, so
# the bucketing only has to be defensible — not pixel-perfect.
EXPORT_SQL = """
SELECT
    p.parcel_id,
    p.county,
    p.acreage,
    p.owner_name,
    p.land_use_code,
    CASE
        WHEN p.land_use_code IS NULL                   THEN 'unknown'
        WHEN LEFT(p.land_use_code, 1) = '1'            THEN 'agriculture'
        WHEN LEFT(p.land_use_code, 1) = '2'            THEN 'vacant'
        WHEN LEFT(p.land_use_code, 1) = '3'            THEN 'industrial'
        WHEN LEFT(p.land_use_code, 1) = '4'            THEN 'commercial'
        WHEN LEFT(p.land_use_code, 1) IN ('5', '6')    THEN 'residential'
        WHEN LEFT(p.land_use_code, 1) = '7'            THEN 'public'
        WHEN LEFT(p.land_use_code, 1) = '8'            THEN 'industrial'
        WHEN LEFT(p.land_use_code, 1) = '9'            THEN 'forest'
        ELSE 'other'
    END AS land_use_class,
    s.state_agency IS NOT NULL AS is_state_owned,
    s.state_agency,
    ST_AsGeoJSON(p.geom) AS geom_json
FROM parcels p
LEFT JOIN LATERAL (
    SELECT state_agency
    FROM state_owned_parcels
    WHERE ST_Intersects(geometry, p.geom)
    LIMIT 1
) s ON TRUE
{where_clause}
"""


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


def export_ndjson(conn, county: str | None, out_path: Path) -> int:
    where = "WHERE p.county = %s" if county else ""
    sql = EXPORT_SQL.format(where_clause=where)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Server-side named cursor streams in batches; itersize controls how many
    # rows the driver pulls per round-trip. 10k is a reasonable balance
    # between network overhead and memory.
    with conn.cursor(name="parcel_export") as cur:
        cur.itersize = 10_000
        params = (county,) if county else ()
        cur.execute(sql, params)

        n = 0
        t0 = time.time()
        with out_path.open("w", encoding="utf-8") as f:
            for row in cur:
                (parcel_id, cnty, acreage, owner_name, land_use_code,
                 land_use_class, is_state_owned, state_agency, geom_json) = row
                if not geom_json:
                    continue
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(geom_json),
                    "properties": {
                        "parcel_id":      parcel_id,
                        "county":         cnty,
                        "acreage":        float(acreage) if acreage is not None else None,
                        "owner_name":     owner_name,
                        "land_use_code":  land_use_code,
                        "land_use_class": land_use_class,
                        "is_state_owned": bool(is_state_owned),
                        "state_agency":   state_agency,
                    },
                }
                f.write(json.dumps(feature, separators=(",", ":")))
                f.write("\n")
                n += 1
                if n % 50_000 == 0:
                    rate = n / (time.time() - t0)
                    print(f"[INFO]  {n:>8,} features written ({rate:,.0f}/s)…")

    elapsed = time.time() - t0
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"[OK]    {n:,} features → {out_path} ({size_mb:.1f} MB, {elapsed:.0f}s)")
    return n


def run_tippecanoe(ndjson: Path, pmtiles: Path, county: str | None):
    if sys.platform == "win32":
        print("[ERROR] tippecanoe is not packaged for native Windows.")
        print("        Run this script under WSL or in a Linux container,")
        print("        or pass --skip-tippecanoe and run tippecanoe manually:")
        print(f"        tippecanoe -o {pmtiles} -Z 6 -z 14 \\")
        print("            --drop-densest-as-needed --coalesce-densest-as-needed \\")
        print(f"            --extend-zooms-if-still-dropping -l parcels {ndjson}")
        sys.exit(1)

    if shutil.which("tippecanoe") is None:
        print("[ERROR] tippecanoe not found on PATH. Install via:")
        print("        macOS:  brew install tippecanoe")
        print("        Linux:  apt install tippecanoe   (or build from source)")
        sys.exit(1)

    # Pilot bakes (single county) get a tighter min-zoom so they render
    # immediately when the viewer flies in; statewide needs z6 for context.
    min_zoom = "9" if county else "6"

    cmd = [
        "tippecanoe",
        "-o", str(pmtiles),
        "-Z", min_zoom, "-z", "14",
        "--drop-densest-as-needed",
        "--coalesce-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "-l", "parcels",
        "--force",  # overwrite existing output
        str(ndjson),
    ]
    print("[INFO]  " + " ".join(cmd))
    t0 = time.time()
    subprocess.run(cmd, check=True)
    size_mb = pmtiles.stat().st_size / 1_048_576
    print(f"[OK]    {pmtiles} ({size_mb:.1f} MB, {time.time() - t0:.0f}s)")


def upload_to_supabase(pmtiles: Path):
    import requests  # imported lazily so the no-upload path has no extra dep

    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not (url and key):
        print("[ERROR] --upload needs NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env")
        sys.exit(1)

    bucket = "parcels-tiles"
    object_path = pmtiles.name
    endpoint = f"{url.rstrip('/')}/storage/v1/object/{bucket}/{object_path}"

    print(f"[INFO]  Uploading {pmtiles.name} → {bucket}/{object_path} …")
    with pmtiles.open("rb") as f:
        # x-upsert: true so a re-bake overwrites the previous file in place,
        # which is what we want — the public URL stays stable.
        resp = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/octet-stream",
                "x-upsert":      "true",
            },
            data=f,
        )
    if not resp.ok:
        print(f"[ERROR] Upload failed: HTTP {resp.status_code} {resp.text[:200]}")
        sys.exit(1)
    public = f"{url.rstrip('/')}/storage/v1/object/public/{bucket}/{object_path}"
    print(f"[OK]    Public URL: {public}")
    print(f"[INFO]  Set NEXT_PUBLIC_PARCELS_PMTILES_URL={public} in well-viewer/.env.local")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--county", help="Restrict to a single county (uppercase, e.g. HOCKING).")
    ap.add_argument("--upload", action="store_true", help="Push the resulting PMTiles to Supabase Storage.")
    ap.add_argument("--skip-tippecanoe", action="store_true", help="Stop after writing NDGeoJSON.")
    args = ap.parse_args()

    conn = connect()
    try:
        ndjson = OUT_DIR / (f"parcels-{args.county.lower()}.ndjson" if args.county else "parcels.ndjson")
        pmtiles = OUT_DIR / (f"parcels-{args.county.lower()}.pmtiles" if args.county else "parcels.pmtiles")
        export_ndjson(conn, args.county, ndjson)
    finally:
        conn.close()

    if args.skip_tippecanoe:
        print("[INFO]  --skip-tippecanoe set; stopping after NDGeoJSON.")
        return

    run_tippecanoe(ndjson, pmtiles, args.county)
    if args.upload:
        upload_to_supabase(pmtiles)


if __name__ == "__main__":
    main()
