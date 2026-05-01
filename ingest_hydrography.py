"""
Ingest USGS NHD (National Hydrography Dataset) small-scale flowlines and
waterbodies into the `hydrography` table.

Source:
    https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer
        layer 4  — Flowline, Small Scale (lines: streams + rivers)
        layer 10 — Waterbody, Small Scale (polygons: lakes + reservoirs)

Why small-scale:
    Large-scale (layers 6/12) is the high-res NHD HD product — every culvert
    and roadside ditch. Small-scale is pre-simplified by USGS for display and
    is what most state GIS portals serve. For an Ohio-wide overview map,
    small-scale is ~10–20× lighter and renders fast.

We pull features intersecting the Ohio bounding box from the ArcGIS REST
endpoint, then clip more tightly to the Ohio union polygon in PostGIS
during insert (a feature whose bbox touches Ohio may sit just over the
border).

Filter rules:
  Flowlines  — keep only FCODE 46006 (StreamRiver, perennial) plus 46003
               (intermittent) when StreamOrde >= 2. Throws away 46007
               (ephemeral) entirely. The intent is to look like a road map
               of rivers, not every drainage line.
  Waterbodies — keep all FTYPE values (LakePond, Reservoir, SwampMarsh,
                Estuary). AREASQKM populated from source.

Usage:
    python ingest_hydrography.py
    python ingest_hydrography.py --truncate         # wipe before insert
    python ingest_hydrography.py --feature flowline # only one type
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

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

BASE_URL  = "https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer"
PAGE_SIZE = 2000   # service maxRecordCount
BATCH     = 200    # rows per INSERT

# Ohio extent (from counties table)
OH_BBOX = (-84.820305, 38.403423, -80.518705, 42.327132)

# Service-layer config:  (layer_id, feature_type, out_fields, where_clause)
# Where clause filters at the source so we don't drag tens of thousands of
# ephemeral channels across the wire.
LAYERS = {
    "flowline": {
        "id": 4,
        "out_fields": "FCODE,FTYPE,StreamOrde,GNIS_NAME,GNIS_ID,REACHCODE",
        # 46006 = perennial, 46003 = intermittent (only larger orders)
        "where": "FCODE = 46006 OR (FCODE = 46003 AND StreamOrde >= 2)",
    },
    "waterbody": {
        "id": 10,
        "out_fields": "FCODE,FTYPE,GNIS_NAME,GNIS_ID,AREASQKM,REACHCODE",
        "where": "1=1",
    },
}


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


def fetch_layer(layer_id: int, where: str, out_fields: str) -> list[dict]:
    """Page through an ArcGIS REST layer and return all GeoJSON features
    intersecting the Ohio bounding box."""
    url = f"{BASE_URL}/{layer_id}/query"
    xmin, ymin, xmax, ymax = OH_BBOX

    all_features: list[dict] = []
    offset = 0
    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "geometry": f"{xmin},{ymin},{xmax},{ymax}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": 4326,
            "outSR": 4326,
            "spatialRel": "esriSpatialRelIntersects",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "returnGeometry": "true",
        }
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        feats = data.get("features", []) or []
        if not feats:
            break

        all_features.extend(feats)
        offset += PAGE_SIZE
        print(f"[INFO]    fetched {len(all_features):,} so far …")

        # ArcGIS signals more data via this flag; missing flag + short page = done.
        if not data.get("exceededTransferLimit", False) and len(feats) < PAGE_SIZE:
            break

        time.sleep(0.3)  # polite

    return all_features


def insert_batch(cur, rows: list[tuple]):
    """Bulk-insert a batch of rows. ST_Multi normalises to Multi* and
    ST_Intersection clips to the Ohio union polygon (computed once via CTE)."""
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s, %s, "
            "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))",
            row,
        ).decode()
        for row in rows
    )
    cur.execute(
        "INSERT INTO hydrography "
        "(feature_type, gnis_name, fcode, ftype, stream_order, area_km2, raw_attrs, geometry) "
        f"VALUES {args}"
    )


def insert_features(conn, features: list[dict], feature_type: str) -> int:
    if not features:
        return 0

    inserted = 0
    skipped_invalid = 0
    batch: list[tuple] = []

    with conn.cursor() as cur:
        for f in features:
            geom = f.get("geometry")
            if geom is None:
                skipped_invalid += 1
                continue

            props = f.get("properties") or {}
            gnis_name = (props.get("GNIS_NAME") or "").strip() or None
            fcode = props.get("FCODE")
            ftype = (props.get("FTYPE") or "").strip() or None
            stream_order = props.get("StreamOrde") if feature_type == "flowline" else None
            area_km2 = props.get("AREASQKM") if feature_type == "waterbody" else None

            batch.append((
                feature_type,
                gnis_name,
                int(fcode) if fcode is not None else None,
                ftype,
                int(stream_order) if stream_order is not None else None,
                float(area_km2) if area_km2 is not None else None,
                json.dumps(props),
                json.dumps(geom),
            ))

            if len(batch) >= BATCH:
                insert_batch(cur, batch)
                inserted += len(batch)
                batch.clear()

        if batch:
            insert_batch(cur, batch)
            inserted += len(batch)

    conn.commit()
    if skipped_invalid:
        print(f"[WARN]    skipped {skipped_invalid} features with NULL geometry")
    return inserted


def clip_to_ohio(conn) -> int:
    """Delete any rows that don't actually fall inside the Ohio counties
    union — bbox-prefilter at the source can include features just over
    the state line."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ohio AS (SELECT ST_Union(geometry) AS g FROM counties WHERE state_code = 'OH')
            DELETE FROM hydrography h
            USING ohio
            WHERE NOT ST_Intersects(h.geometry, ohio.g);
            """
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def run(args):
    targets = ["flowline", "waterbody"] if args.feature == "all" else [args.feature]
    conn = connect()
    try:
        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE hydrography RESTART IDENTITY")
            conn.commit()
            print("[OK]    Truncated hydrography.")

        total = 0
        t0 = time.time()
        for ft in targets:
            cfg = LAYERS[ft]
            print(f"\n[INFO]  Layer {cfg['id']} — {ft}  (where: {cfg['where']})")
            feats = fetch_layer(cfg["id"], cfg["where"], cfg["out_fields"])
            print(f"[OK]    Fetched {len(feats):,} {ft} features.")
            n = insert_features(conn, feats, ft)
            total += n
            print(f"[OK]    Inserted {n:,} {ft} rows.")

        print("\n[INFO]  Clipping rows that escape the Ohio polygon …")
        purged = clip_to_ohio(conn)
        print(f"[OK]    Removed {purged:,} cross-border features.")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT feature_type, COUNT(*) FROM hydrography GROUP BY 1 ORDER BY 1"
            )
            counts = cur.fetchall()

        elapsed = time.time() - t0
        print()
        print("─" * 55)
        print(f"[DONE]  Ingestion complete in {elapsed:.1f}s")
        print(f"        Inserted: {total:,}   Removed: {purged:,}")
        for ft, n in counts:
            print(f"        hydrography.{ft:<10} {n:>8,} rows")
        print("─" * 55)
    finally:
        conn.close()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--feature", choices=("all", "flowline", "waterbody"), default="all")
    p.add_argument("--truncate", action="store_true",
                   help="Wipe hydrography table before insert (re-run safely).")
    return p.parse_args()


if __name__ == "__main__":
    print()
    print("=== USGS NHD hydrography ingest ===")
    print()
    run(parse_args())
