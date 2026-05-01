"""
Ingest Ohio public/private schools into the schools table.

Source: Ohio Dept of Education map service.
    https://maps.ohio.gov/arcgis/rest/services/Education/ODE_Layers/MapServer

The MapServer hosts multiple layers; default is 0. Use `--list-layers` to
see what's available, or `--layer-id N` to override.

Field naming in ArcGIS layers varies. The script introspects the chosen
layer's metadata at startup, prints the field list, and uses a tolerant
mapping (tries several common patterns) to populate `name`, `district`,
`school_type`, `ownership`, and `external_id`. Whatever doesn't map cleanly
still lands in `raw_attrs` (jsonb), so nothing is lost.

Usage:
    python ingest_schools.py                  # statewide, layer 0
    python ingest_schools.py --list-layers    # discover layers, print, exit
    python ingest_schools.py --layer-id 2     # if schools live elsewhere
"""

import argparse
import json
import os
import sys
import time
from typing import Iterable

import psycopg2
import requests
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

SERVICE_URL = "https://maps.ohio.gov/arcgis/rest/services/Education/ODE_Layers/MapServer"
PAGE_SIZE   = 1000
BATCH_SIZE  = 200

# Tolerant field-name mapping. Order matters — first hit wins. Lowercased
# comparison so case differences don't matter.
NAME_FIELDS = [
    "org_name", "bldg_name", "building_name", "school_name", "name",
    "fac_name", "facility_name", "schoolname", "bldgname",
]
DISTRICT_FIELDS = [
    "lea_name", "districtname", "district_name", "district",
    "leaname", "schooldistrict", "school_district",
]
TYPE_FIELDS = [
    "org_type_descr", "school_type", "schooltype", "type",
    "grade_range", "graderange", "category", "level",
]
OWNERSHIP_FIELDS = [
    "ownership", "owner_type", "entity_type", "public_private",
    "publicprivate", "sponsor_type", "operator_type",
]
EXTERNAL_ID_FIELDS = [
    "org_irn", "irn", "building_irn", "bldg_irn", "bldgirn",
    "ncessch", "nces_id", "nces", "school_id", "facilityid", "fac_id",
]


def connect():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


def discover_layers() -> list[dict]:
    """List every layer exposed by the MapServer."""
    resp = requests.get(SERVICE_URL, params={"f": "json"}, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    return body.get("layers", []) + body.get("tables", [])


def discover_fields(layer_id: int) -> list[dict]:
    """Get the field metadata for a specific layer."""
    resp = requests.get(f"{SERVICE_URL}/{layer_id}", params={"f": "json"}, timeout=30)
    resp.raise_for_status()
    return resp.json().get("fields", [])


def pick_field(props: dict, candidates: Iterable[str]) -> str | None:
    """Find the first candidate field name that exists in props (case-insensitive)
    and has a non-empty value."""
    lower = {k.lower(): k for k in props.keys()}
    for c in candidates:
        actual = lower.get(c.lower())
        if actual is None:
            continue
        v = props.get(actual)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def fetch_features(layer_id: int) -> list[dict]:
    """Paginated geojson fetch from a single layer with retry on transient
    server errors (the maps.ohio.gov host occasionally 504s under load)."""
    out: list[dict] = []
    offset = 0
    url = f"{SERVICE_URL}/{layer_id}/query"

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }

        body = None
        for attempt in range(4):
            try:
                resp = requests.get(url, params=params, timeout=120)
                if 500 <= resp.status_code < 600:
                    raise requests.HTTPError(f"{resp.status_code}", response=resp)
                resp.raise_for_status()
                body = resp.json()
                break
            except (requests.HTTPError, requests.Timeout, requests.ConnectionError, ValueError) as e:
                if isinstance(e, requests.HTTPError) and e.response is not None and 400 <= e.response.status_code < 500:
                    raise
                if attempt < 3:
                    sleep_s = 5 * (3 ** attempt)
                    print(f"  ! offset {offset}: {e} — retrying in {sleep_s}s "
                          f"(attempt {attempt + 1}/3)", flush=True)
                    time.sleep(sleep_s)
                else:
                    raise

        features = (body or {}).get("features", [])
        if not features:
            break
        out.extend(features)
        if len(features) < PAGE_SIZE and not body.get("exceededTransferLimit"):
            break
        offset += PAGE_SIZE

    return out


def insert_features(conn, features: list[dict]) -> tuple[int, int]:
    """Insert all features into schools. Returns (inserted, skipped)."""
    inserted = 0
    skipped = 0

    with conn.cursor() as cur:
        batch = []
        for f in features:
            props = f.get("properties") or {}
            geom = f.get("geometry")
            if geom is None or geom.get("type") != "Point":
                skipped += 1
                continue

            name = pick_field(props, NAME_FIELDS)
            if not name:
                skipped += 1
                continue

            row = (
                pick_field(props, EXTERNAL_ID_FIELDS),
                name,
                pick_field(props, DISTRICT_FIELDS),
                pick_field(props, TYPE_FIELDS),
                pick_field(props, OWNERSHIP_FIELDS),
                json.dumps(props),
                json.dumps(geom),
            )
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                _exec_batch(cur, batch)
                inserted += len(batch)
                batch = []

        if batch:
            _exec_batch(cur, batch)
            inserted += len(batch)

    conn.commit()
    return inserted, skipped


def _exec_batch(cur, batch: list[tuple]):
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s::jsonb, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))",
            row,
        ).decode()
        for row in batch
    )
    cur.execute(
        "INSERT INTO schools "
        "(external_id, name, district, school_type, ownership, raw_attrs, geometry) "
        f"VALUES {args} "
        # external_id may be NULL for some layers — only de-dupe when present.
        "ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO UPDATE SET "
        "  name        = EXCLUDED.name, "
        "  district    = EXCLUDED.district, "
        "  school_type = EXCLUDED.school_type, "
        "  ownership   = EXCLUDED.ownership, "
        "  raw_attrs   = EXCLUDED.raw_attrs, "
        "  geometry    = EXCLUDED.geometry, "
        "  ingested_at = NOW()"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer-id", type=int, default=0,
                        help="Which MapServer layer to ingest. Default: 0.")
    parser.add_argument("--list-layers", action="store_true",
                        help="Print available layers and exit (no DB writes).")
    parser.add_argument("--truncate", action="store_true",
                        help="DELETE FROM schools before ingest (full refresh).")
    args = parser.parse_args()

    print("=== Ohio Schools Ingest ===\n")

    if args.list_layers:
        layers = discover_layers()
        print(f"Layers exposed by {SERVICE_URL}:")
        for L in layers:
            print(f"  id={L.get('id')}  type={L.get('type'):<12}  name={L.get('name')}")
        return

    layer_id = args.layer_id
    print(f"[INFO] Discovering fields on layer {layer_id} …")
    fields = discover_fields(layer_id)
    print(f"[INFO] Layer has {len(fields)} fields:")
    for fld in fields:
        print(f"          {fld.get('name'):<30}  {fld.get('type')}")
    print()

    conn = connect()
    try:
        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM schools")
            conn.commit()
            print("[INFO] Truncated schools table.")

        t0 = time.time()
        print(f"[INFO] Fetching features (paginated) …")
        features = fetch_features(layer_id)
        print(f"[OK]   Fetched {len(features):,} features in {time.time()-t0:.1f}s.")

        if not features:
            print("[WARN] No features returned. Check --layer-id.")
            return

        t0 = time.time()
        inserted, skipped = insert_features(conn, features)
        print(f"[OK]   Inserted {inserted:,} schools "
              f"({skipped:,} skipped — missing geom/name) in {time.time()-t0:.1f}s.")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM schools")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT district) FROM schools WHERE district IS NOT NULL")
            n_districts = cur.fetchone()[0]
        print()
        print("─" * 55)
        print(f"[DONE] schools rows : {total:>8,}")
        print(f"       districts    : {n_districts:>8,}")
        print("─" * 55)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
