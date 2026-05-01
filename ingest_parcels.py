"""
Ingest county auditor parcel data into the parcels table.

Pulls polygon features (one parcel per row) from a county's ArcGIS REST
FeatureServer and inserts owner / address / acreage attributes alongside
geometry.

Currently scoped to Hocking County (pilot validator). Statewide expansion
is a follow-up — every Ohio county exposes its auditor data with a slightly
different field schema, so FIELD_CANDIDATES below holds the common variants
and the script picks whichever the source actually publishes.

Usage:
    python ingest_parcels.py                  # default: HOCKING
    python ingest_parcels.py --county HOCKING --force
"""

import argparse
import json
import os
import sys
import time

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

# Windows console default codepage (cp1252) can't encode the arrows / ✓ used
# in field-detection logging. Force UTF-8 so the script runs cleanly on Windows.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

# Per-county REST FeatureServer layer URLs.
# Discover these from the auditor's web map — open DevTools → Network →
# filter for "/query" and copy the URL up to and including the layer index.
# Example shape: https://gis.example.org/arcgis/rest/services/Parcels/FeatureServer/0
#
# HOCKING note: layer is `parcel_joinedHocOH` on services7.arcgis.com — parcel
# geometry joined with a CAMA slice. 23,744 parcels, public read (no token).
# Owner mailing address is NOT in this feed (only site address, which we don't
# store) — so surface_owner_mailing_state will be NULL for Hocking. Absentee-
# owner detection requires a different data source for this county.
COUNTY_LAYER_URLS: dict[str, str] = {
    "HOCKING": "https://services7.arcgis.com/clXmZ04BrbYyKlqh/arcgis/rest/services/parcel_joinedHocOH/FeatureServer/0",
}

PAGE_SIZE = 2000  # Hocking server caps at 2000; matches server-side maxRecordCount.
BATCH_SIZE = 100

# Auditor schemas vary across Ohio's 88 counties. For each logical field we
# need, we try a list of candidate property keys and use whichever the source
# actually returns. The script logs which keys it picked so users can confirm.
#
# Hocking uses a `PP` ("Parcel Property") prefix on every CAMA field; other
# counties will use their own. When adding a new county, run the layer's
# /query?where=1=1&outFields=*&resultRecordCount=1&f=json once and append any
# missing source-side keys to the candidate lists below.
FIELD_CANDIDATES: dict[str, list[str]] = {
    "parcel_id":             ["PARCEL_ID", "PARCELID", "PIN", "PIDN", "PARCEL_NO", "Parcel", "PARCEL", "Parcel2"],
    "owner_name":            ["OWNER_NAME", "OWNERNAME", "OWNER", "Owner", "OWNER1", "OwnerName", "PPOwner"],
    "owner_mailing_address": ["MAIL_ADDR", "MAILADDR", "MAIL_ADDRESS", "OWNER_ADDR"],
    "owner_mailing_city":    ["MAIL_CITY", "MAILCITY", "OWNER_CITY"],
    "owner_mailing_state":   ["MAIL_STATE", "MAILSTATE", "OWNER_STATE"],
    "owner_mailing_zip":     ["MAIL_ZIP", "MAILZIP", "OWNER_ZIP", "ZIPCODE"],
    "acreage":               ["ACRES", "ACREAGE", "DEED_ACRES", "GIS_ACRES", "CALC_ACRES", "PPAcres"],
    "land_use_code":         ["LAND_USE", "LANDUSE", "USE_CODE", "PROP_CLASS", "PPClassCode"],
    "tax_district":          ["TAX_DIST", "TAXDIST", "DISTRICT", "TAX_DISTRICT"],
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def validate_env():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


def already_loaded(conn, county) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM parcels WHERE county = %s", (county,))
        return cur.fetchone()[0]


def detect_field_map(sample_props: dict) -> dict[str, str | None]:
    """For each logical field, pick the first candidate key that's present
    in the sample feature's properties. Returns logical→actual key mapping."""
    keys = set(sample_props.keys())
    mapping: dict[str, str | None] = {}
    for logical, candidates in FIELD_CANDIDATES.items():
        mapping[logical] = next((c for c in candidates if c in keys), None)
    return mapping


def fetch_parcels(layer_url: str) -> list[dict]:
    """Paginated GeoJSON fetch; mirrors ingest_water_sources.fetch_layer."""
    all_features: list[dict] = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "outSR": 4326,
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }
        resp = requests.get(f"{layer_url}/query", params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += PAGE_SIZE

        print(f"        … fetched {len(all_features):,} so far")

        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break

    return all_features


def _merge_polygons(geoms: list[dict]) -> dict | None:
    """Combine N Polygon/MultiPolygon GeoJSON geometries into one MultiPolygon.
    Ohio auditors commonly publish a single legal parcel as multiple Polygon
    features when the parcel has disjoint pieces; we union them so the unique
    (county, parcel_id) constraint holds and the spatial join sees the full
    extent."""
    coords: list = []
    for g in geoms:
        if not g:
            continue
        if g.get("type") == "Polygon":
            coords.append(g["coordinates"])
        elif g.get("type") == "MultiPolygon":
            coords.extend(g["coordinates"])
    if not coords:
        return None
    return {"type": "MultiPolygon", "coordinates": coords}


def insert_features(conn, features: list[dict], county: str, field_map: dict[str, str | None]) -> int:
    if not features:
        return 0

    # Phase 1: group features by parcel_id, merging duplicate geometries.
    from collections import defaultdict
    geoms_by_pid: dict[str, list[dict]] = defaultdict(list)
    props_by_pid: dict[str, dict] = {}

    pid_key = field_map.get("parcel_id")
    if not pid_key:
        return 0

    for f in features:
        props = f.get("properties") or {}
        geom = f.get("geometry")
        raw_pid = props.get(pid_key)
        if raw_pid is None or geom is None:
            continue
        pid = str(raw_pid).strip() or None
        if not pid:
            continue
        geoms_by_pid[pid].append(geom)
        # Keep the first feature's attributes — duplicates share the same
        # CAMA record, so any of them works.
        if pid not in props_by_pid:
            props_by_pid[pid] = props

    duplicates = sum(1 for pid in geoms_by_pid if len(geoms_by_pid[pid]) > 1)
    if duplicates:
        print(f"[INFO]  {duplicates:,} parcels have multi-polygon geometry "
              f"(merged into MultiPolygon before insert).")

    # Phase 2: build rows + batch insert.
    def get_from(props, logical):
        key = field_map.get(logical)
        if not key:
            return None
        v = props.get(key)
        if v is None:
            return None
        v = str(v).strip()
        return v or None

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for pid, geoms in geoms_by_pid.items():
            merged = _merge_polygons(geoms)
            if merged is None:
                continue
            props = props_by_pid[pid]

            acreage = get_from(props, "acreage")
            try:
                acreage_num = float(acreage) if acreage else None
            except ValueError:
                acreage_num = None

            batch.append((
                county,
                pid,
                get_from(props, "owner_name"),
                get_from(props, "owner_mailing_address"),
                get_from(props, "owner_mailing_city"),
                get_from(props, "owner_mailing_state"),
                get_from(props, "owner_mailing_zip"),
                acreage_num,
                get_from(props, "land_use_code"),
                get_from(props, "tax_district"),
                json.dumps(merged),
            ))

            if len(batch) >= BATCH_SIZE:
                _execute_batch(cur, batch)
                inserted += len(batch)
                batch = []

        if batch:
            _execute_batch(cur, batch)
            inserted += len(batch)

    conn.commit()
    return inserted


def _execute_batch(cur, batch: list[tuple]):
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))",
            row,
        ).decode()
        for row in batch
    )
    cur.execute(
        "INSERT INTO parcels "
        "(county, parcel_id, owner_name, owner_mailing_address, owner_mailing_city, "
        " owner_mailing_state, owner_mailing_zip, acreage, land_use_code, tax_district, geom) "
        f"VALUES {args} "
        "ON CONFLICT (county, parcel_id) DO UPDATE SET "
        "  owner_name            = EXCLUDED.owner_name, "
        "  owner_mailing_address = EXCLUDED.owner_mailing_address, "
        "  owner_mailing_city    = EXCLUDED.owner_mailing_city, "
        "  owner_mailing_state   = EXCLUDED.owner_mailing_state, "
        "  owner_mailing_zip     = EXCLUDED.owner_mailing_zip, "
        "  acreage               = EXCLUDED.acreage, "
        "  land_use_code         = EXCLUDED.land_use_code, "
        "  tax_district          = EXCLUDED.tax_district, "
        "  geom                  = EXCLUDED.geom, "
        "  ingested_at           = now()"
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def run_one(county: str, force: bool):
    layer_url = COUNTY_LAYER_URLS.get(county)
    if not layer_url:
        print(f"[ERROR] No layer URL configured for county '{county}'.")
        print(f"        Edit COUNTY_LAYER_URLS in ingest_parcels.py.")
        sys.exit(1)

    conn = connect()
    start = time.time()
    try:
        existing = already_loaded(conn, county)
        if existing and not force:
            print(f"[SKIP]  {county} already has {existing:,} parcels. Pass --force to re-ingest.")
            return

        print(f"[INFO]  Fetching parcels from {layer_url} …")
        features = fetch_parcels(layer_url)
        print(f"[OK]    Fetched {len(features):,} features.")

        if not features:
            print(f"[WARN]  No features returned.")
            return

        field_map = detect_field_map(features[0].get("properties") or {})
        print(f"[INFO]  Detected field mapping (logical → source key):")
        for logical, actual in field_map.items():
            marker = "✓" if actual else "✗"
            print(f"          {marker} {logical:25s} → {actual or '(not found)'}")

        if not field_map.get("parcel_id"):
            print("[ERROR] No parcel_id field detected — review FIELD_CANDIDATES "
                  "against the source schema and add the missing key.")
            sys.exit(1)

        if not field_map.get("owner_name"):
            print("[WARN]  No owner_name field detected. Continuing (geometry will "
                  "still be loaded), but the data is much less useful — verify "
                  "the auditor isn't masking PII before proceeding to the join.")

        count = insert_features(conn, features, county, field_map)
        elapsed = time.time() - start
        print(f"[OK]    {county}: {count:,} parcels upserted in {elapsed:.1f}s")

    except requests.RequestException as e:
        print(f"[ERROR] HTTP request failed: {e}")
        raise
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[ERROR] Database error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Connection closed.")


if __name__ == "__main__":
    print()
    print("=== County Auditor Parcel Ingestion ===")
    print()

    parser = argparse.ArgumentParser()
    parser.add_argument("--county", default="HOCKING",
                        help="County (uppercase) to ingest. Default: HOCKING.")
    parser.add_argument("--force", action="store_true",
                        help="Re-ingest even if parcels already loaded.")
    args = parser.parse_args()

    validate_env()
    run_one(args.county.upper(), args.force)
