"""
Ingest Ohio EPA Source Water Protection Areas into water_sources table.

Fetches polygon data from the Ohio EPA SWAP FeatureServer (6 layers)
covering groundwater and surface water protection zones across Ohio.

Usage:
    python ingest_water_sources.py
"""

import json
import os
import sys
import time
import psycopg2
from dotenv import load_dotenv
import requests

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

BASE_URL = "https://geo.epa.ohio.gov/arcgis/rest/services/DrinkingWater/SWAP/FeatureServer"
PAGE_SIZE = 1000

# Layer definitions: (layer_id, source_type, protection_zone)
LAYERS = [
    (0, "groundwater",           "inner_management_zone"),
    (1, "groundwater",           "source_water_protection_area"),
    (2, "surface_water_inland",  "surface_water"),
    (3, "surface_water_lake_erie", "surface_water"),
    (4, "surface_water_ohio_river", "surface_water"),
    (5, "surface_water_ohio_river", "surface_water"),
]

BATCH_SIZE = 100


# ── Helpers ───────────────────────────────────────────────────────────────────

def validate_env():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    print("[OK]    Environment variables loaded.")


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER,
            password=DB_PASSWORD, port=DB_PORT,
            connect_timeout=15,
            sslmode="require",
        )
        print("[OK]    Connected.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


def fetch_layer(layer_id: int) -> list[dict]:
    """
    Fetch all features from a single SWAP FeatureServer layer.
    Paginates through the ArcGIS REST API using resultOffset.
    Returns a list of GeoJSON features.
    """
    all_features = []
    offset = 0

    while True:
        url = f"{BASE_URL}/{layer_id}/query"
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }

        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += PAGE_SIZE

        # ArcGIS returns exceededTransferLimit when there's more data
        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break

    return all_features


def insert_features(conn, features: list[dict], source_type: str, protection_zone: str):
    """
    Insert a batch of GeoJSON features into the water_sources table.
    """
    if not features:
        return 0

    inserted = 0

    with conn.cursor() as cur:
        batch = []
        for f in features:
            props = f.get("properties", {})
            geom = f.get("geometry")

            if geom is None:
                continue

            name = (props.get("sys_name") or props.get("SYS_NAME") or "").strip() or None
            pwsid = (props.get("pwsid") or props.get("PWSID") or "").strip() or None
            geom_json = json.dumps(geom)

            batch.append((name, source_type, protection_zone, pwsid, geom_json))

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
    """Execute a batch INSERT into water_sources."""
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))",
            row,
        ).decode()
        for row in batch
    )
    cur.execute(
        f"INSERT INTO water_sources (name, source_type, protection_zone, public_water_system, geometry) "
        f"VALUES {args}"
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    conn = connect()
    total_inserted = 0
    start = time.time()

    try:
        for layer_id, source_type, protection_zone in LAYERS:
            print(f"\n[INFO]  Layer {layer_id}: {source_type} / {protection_zone}")
            print(f"[INFO]  Fetching from Ohio EPA SWAP FeatureServer …")

            features = fetch_layer(layer_id)
            print(f"[OK]    Fetched {len(features):,} features.")

            if not features:
                print(f"[WARN]  No features returned for layer {layer_id}.")
                continue

            count = insert_features(conn, features, source_type, protection_zone)
            total_inserted += count
            print(f"[OK]    Inserted {count:,} records into water_sources.")

        elapsed = time.time() - start

        # Final count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM water_sources")
            final = cur.fetchone()[0]

        print()
        print("─" * 55)
        print(f"[DONE]  Ingestion complete in {elapsed:.1f}s")
        print(f"        Total inserted : {total_inserted:>8,}")
        print(f"        water_sources  : {final:>8,} rows")
        print("─" * 55)

    except requests.RequestException as e:
        print(f"[ERROR] HTTP request failed: {e}")
        raise
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[ERROR] Database error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    print()
    print("=== Ohio EPA Water Source Protection Areas Ingestion ===")
    print()

    validate_env()
    run()
