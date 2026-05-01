"""
Ingest State of Ohio SORP (State-Owned Real Property) parcels into
state_owned_parcels table.

Fetches polygon data from the State of Ohio's ArcGIS Online tenant (DAS GIS),
the SORP_Parcels_2025 layer. Used to flag wells that sit on state-owned land —
those are the action-ready candidates where the regulator (ODNR) is also the
landowner, so plugging requires no surface-owner negotiation.

Source: State of Ohio DAS GIS tenant (services2.arcgis.com/MlJ0G8iWUyC7jAmu).
Public, no auth required.

Usage:
    python ingest_state_parcels.py
"""

import os
import sys
import time
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

# Windows console default codepage (cp1252) can't encode the ─ separator.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

LAYER_URL = (
    "https://services2.arcgis.com/MlJ0G8iWUyC7jAmu/ArcGIS/rest/services/"
    "State_Owned_Real_Property_%28SORP%29_for_the_State_of_Ohio_view/"
    "FeatureServer/0/query"
)

PAGE_SIZE = 2000   # matches the layer's maxRecordCount

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS state_owned_parcels (
    id              SERIAL PRIMARY KEY,
    state_parcel_id TEXT,
    local_parcel_id TEXT,
    county          TEXT,
    state_agency    TEXT,
    acres           DOUBLE PRECISION,
    auditor_link    TEXT,
    prop_type       TEXT,
    own_type        TEXT,
    common_name     TEXT,
    global_id       TEXT,
    geometry        GEOMETRY(Geometry, 4326),
    imported_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sop_geometry ON state_owned_parcels USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_sop_county   ON state_owned_parcels (county);
CREATE INDEX IF NOT EXISTS idx_sop_agency   ON state_owned_parcels (state_agency);
"""

INSERT_SQL = """
INSERT INTO state_owned_parcels (
    state_parcel_id, local_parcel_id, county, state_agency, acres,
    auditor_link, prop_type, own_type, common_name, global_id, geometry
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
);
"""


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


def fetch_page(offset: int) -> dict:
    """One paginated GeoJSON page from the SORP FeatureServer."""
    params = {
        "where": "1=1",
        "outFields": "*",
        "outSR": 4326,
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
    }
    resp = requests.get(LAYER_URL, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def insert_features(cur, features) -> int:
    """Insert one batch. Returns rows inserted."""
    import json
    rows = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")
        if not geom:
            continue
        cur.execute(INSERT_SQL, (
            props.get("StateParcelID"),
            props.get("LocalParcelID"),
            props.get("County"),
            props.get("StateAgency"),
            props.get("Acres"),
            props.get("auditorlink"),
            props.get("PROP_TYPE"),
            props.get("OWN_TYPE"),
            props.get("COMMON_NAME"),
            props.get("GlobalID"),
            json.dumps(geom),
        ))
        rows += 1
    return rows


def main():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    print()
    print("=== State-Owned Real Property (SORP) Ingest ===")
    print()

    conn = connect()
    print("[OK]    Connected.")

    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute("TRUNCATE state_owned_parcels RESTART IDENTITY;")
        conn.commit()
        print("[OK]    Table prepared (truncated for fresh ingest).")

        total = 0
        offset = 0
        start = time.time()

        while True:
            page_start = time.time()
            print(f"[FETCH] offset={offset} …", end=" ", flush=True)
            page = fetch_page(offset)
            features = page.get("features") or []

            if not features:
                print("no more features.")
                break

            with conn.cursor() as cur:
                inserted = insert_features(cur, features)
            conn.commit()
            total += inserted

            elapsed = time.time() - page_start
            print(f"got {len(features)}, inserted {inserted} ({elapsed:.1f}s, total={total:,})")

            # If the server says we received fewer than PAGE_SIZE rows, we're done.
            # Otherwise, advance and ask for the next page.
            if len(features) < PAGE_SIZE and not page.get("properties", {}).get("exceededTransferLimit"):
                break
            offset += len(features)

        elapsed = time.time() - start
        print()
        print("─" * 50)
        print(f"[DONE]  Ingest complete in {elapsed:.1f}s — {total:,} parcels.")
        print("─" * 50)

    finally:
        conn.close()
        print("[INFO]  Connection closed.")


if __name__ == "__main__":
    main()
