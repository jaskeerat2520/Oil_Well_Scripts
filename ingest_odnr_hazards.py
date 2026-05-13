"""
Ingest ODNR hazard overlays into PostGIS.

Pulls five layers (four polygon, one point) from ODNR's public ArcGIS REST
services and lands them in `odnr_hazard_layers` and `aum_openings`.

Layers fetched (see memory `reference_odnr_rest_endpoints.md`):
    1. AUM mines (polygons)        -> odnr_hazard_layers.layer_type='aum_mine'
    2. AUM openings (points)       -> aum_openings
    3. AML projects (polygons)     -> odnr_hazard_layers.layer_type='aml_project'
    4. AMLIS federal (polygons)    -> odnr_hazard_layers.layer_type='amlis_area'
    5. State floodplain (polygons) -> odnr_hazard_layers.layer_type='state_floodplain'
    6. DOGRM urban areas (polygons) -> odnr_hazard_layers.layer_type='dogrm_urban_area'

Re-running is safe — uses UPSERT on (layer_type, external_id) for polygons
and on external_id for openings.

Usage:
    python ingest_odnr_hazards.py
    python ingest_odnr_hazards.py --layer aum_mine    # ingest a single layer
    python ingest_odnr_hazards.py --discover          # list layers per MapServer and exit
"""

import argparse
import json
import os
import sys
import time
import psycopg2
from dotenv import load_dotenv
import requests

load_dotenv()

# Force UTF-8 stdout for the ─ separator on Windows.
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

PAGE_SIZE  = 1000
BATCH_SIZE = 100
HTTP_TIMEOUT = 90

ODNR_ROOT = "https://gis.ohiodnr.gov/arcgis/rest/services"

# Each polygon layer maps to a single (service, layer_id) on the ODNR REST root.
# `name_fields` is an ordered list of property names to try for the polygon's
# display name — first hit wins. layer_id of None means "discover at runtime".
POLY_LAYERS = [
    {
        "layer_type": "aum_mine",
        "service":    "MRM_Services/MRMMapViewer_AMLINFO/MapServer",
        "layer_id":   5,
        "name_fields": ["MINE_NAME", "Mine_Name", "NAME"],
    },
    {
        "layer_type": "aml_project",
        "service":    "MRM_Services/MRMMapViewer_AMLINFO/MapServer",
        "layer_id":   1,
        "name_fields": ["PROJ_NAME", "Project_Name", "NAME"],
    },
    {
        "layer_type": "amlis_area",
        "service":    "MRM_Services/MRMMapViewer_AMLISINFO/MapServer",
        "layer_id":   None,             # discover by name match
        "match":      "polygon",        # case-insensitive substring
        "name_fields": ["NAME", "PROBLEM_NAME", "PROJ_NAME"],
    },
    {
        "layer_type": "state_floodplain",
        "service":    "OIT_Services/FloodPlain_supplemental/MapServer",
        "layer_id":   0,
        "name_fields": ["NAME", "FLD_ZONE"],
    },
    {
        "layer_type": "dogrm_urban_area",
        "service":    None,                                 # discover service
        "service_match": "NotificationAreas",
        "service_root_folder": "DOG_Services",
        "layer_id":   None,
        "match":      "urban",
        "name_fields": ["NAME", "Urban_Name"],
    },
]

# AUM openings — separate table because they're points, not polygons.
OPENINGS_LAYER = {
    "service":     "MRM_Services/MRMMapViewer_AMLINFO/MapServer",
    "layer_id":    2,
    "type_fields": ["OPENING_TYPE", "FEATURE_TYPE", "TYPE"],
}


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
            connect_timeout=15, sslmode="require",
        )
        print("[OK]    Connected.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


def list_service_layers(service_path: str) -> list[dict]:
    """List layers in a MapServer (returns each layer's id+name)."""
    url = f"{ODNR_ROOT}/{service_path}"
    resp = requests.get(url, params={"f": "json"}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("layers", [])


def list_folder_services(folder: str) -> list[str]:
    """List MapServer service paths under an ArcGIS REST folder."""
    url = f"{ODNR_ROOT}/{folder}"
    resp = requests.get(url, params={"f": "json"}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    out = []
    for svc in resp.json().get("services", []):
        if svc.get("type") in ("MapServer", "FeatureServer"):
            out.append(f"{svc['name']}/{svc['type']}")
    return out


def resolve_layer(cfg: dict) -> tuple[str, int] | None:
    """Resolve a layer config to a (service_path, layer_id) tuple, discovering as needed."""
    service = cfg.get("service")
    layer_id = cfg.get("layer_id")

    # Discover service path if needed (e.g. the DOGRM NotificationAreas service).
    if service is None:
        folder = cfg.get("service_root_folder")
        match  = (cfg.get("service_match") or "").lower()
        if not (folder and match):
            return None
        try:
            svcs = list_folder_services(folder)
        except requests.RequestException as e:
            print(f"[WARN]  Could not list services in {folder}: {e}")
            return None
        candidates = [s for s in svcs if match in s.lower()]
        if not candidates:
            print(f"[WARN]  No service matching '{match}' in folder {folder}; skipping.")
            return None
        service = candidates[0]
        print(f"[INFO]  Discovered service: {service}")

    # Discover layer ID if needed.
    if layer_id is None:
        match = (cfg.get("match") or "").lower()
        try:
            layers = list_service_layers(service)
        except requests.RequestException as e:
            print(f"[WARN]  Could not list layers in {service}: {e}")
            return None
        # Prefer polygons over points/lines if a substring match is given.
        candidates = [L for L in layers if match in L.get("name", "").lower()]
        if not candidates:
            print(f"[WARN]  No layer matching '{match}' in {service}; layers were:")
            for L in layers:
                print(f"          {L.get('id')}: {L.get('name')}")
            return None
        layer_id = candidates[0]["id"]
        print(f"[INFO]  Discovered layer: {service}/{layer_id} ({candidates[0].get('name')})")

    return service, layer_id


def fetch_layer(service_path: str, layer_id: int) -> list[dict]:
    """Pull all features from an ArcGIS REST layer with WGS84 geometry."""
    all_features = []
    offset = 0
    while True:
        url = f"{ODNR_ROOT}/{service_path}/{layer_id}/query"
        params = {
            "where": "1=1",
            "outFields": "*",
            "outSR": 4326,            # force WGS84; ODNR services often default to NAD83 SP Ohio
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features") or []
        if not features:
            break
        all_features.extend(features)
        offset += PAGE_SIZE
        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break
    return all_features


def first_present(props: dict, keys: list[str]) -> str | None:
    for k in keys:
        v = props.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


# ── Insert paths ─────────────────────────────────────────────────────────────

def upsert_polygons(conn, features: list[dict], layer_type: str, name_fields: list[str]) -> int:
    """Insert polygon features into odnr_hazard_layers."""
    if not features:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for f in features:
            geom = f.get("geometry")
            props = f.get("properties") or {}
            if geom is None:
                continue

            # ArcGIS sometimes returns OBJECTID at varying capitalizations.
            ext_id = (
                str(props.get("OBJECTID") or props.get("objectid") or props.get("FID") or "")
                or None
            )
            name = first_present(props, name_fields)
            geom_json = json.dumps(geom)
            raw = json.dumps(props)

            batch.append((layer_type, ext_id, name, raw, geom_json))

            if len(batch) >= BATCH_SIZE:
                _flush_polygons(cur, batch)
                inserted += len(batch)
                batch = []

        if batch:
            _flush_polygons(cur, batch)
            inserted += len(batch)

    conn.commit()
    return inserted


def _flush_polygons(cur, batch):
    args = ",".join(
        cur.mogrify(
            # ST_Multi guarantees MultiPolygon; ST_MakeValid heals self-intersections;
            # ST_Force2D drops Z/M dimensions a few ODNR layers carry.
            "(%s, %s, %s, %s::jsonb, "
            "ST_Multi(ST_MakeValid(ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))), "
            "ST_Area(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)::geography) / 1e6)",
            (layer_type, ext_id, name, raw, geom_json, geom_json),
        ).decode()
        for (layer_type, ext_id, name, raw, geom_json) in batch
    )
    cur.execute(
        "INSERT INTO odnr_hazard_layers "
        "  (layer_type, external_id, name, raw_attrs, geometry, area_km2) "
        f"VALUES {args} "
        "ON CONFLICT (layer_type, external_id) DO UPDATE SET "
        "  name = EXCLUDED.name, "
        "  raw_attrs = EXCLUDED.raw_attrs, "
        "  geometry = EXCLUDED.geometry, "
        "  area_km2 = EXCLUDED.area_km2, "
        "  ingested_at = NOW()"
    )


def upsert_openings(conn, features: list[dict], type_fields: list[str]) -> int:
    if not features:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for f in features:
            geom = f.get("geometry")
            props = f.get("properties") or {}
            if geom is None:
                continue

            ext_id = (
                str(props.get("OBJECTID") or props.get("objectid") or props.get("FID") or "")
                or None
            )
            opening_type = first_present(props, type_fields)
            raw = json.dumps(props)
            geom_json = json.dumps(geom)

            batch.append((ext_id, opening_type, raw, geom_json))

            if len(batch) >= BATCH_SIZE:
                _flush_openings(cur, batch)
                inserted += len(batch)
                batch = []

        if batch:
            _flush_openings(cur, batch)
            inserted += len(batch)

    conn.commit()
    return inserted


def _flush_openings(cur, batch):
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s::jsonb, ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))",
            row,
        ).decode()
        for row in batch
    )
    cur.execute(
        "INSERT INTO aum_openings (external_id, opening_type, raw_attrs, geometry) "
        f"VALUES {args} "
        "ON CONFLICT (external_id) DO UPDATE SET "
        "  opening_type = EXCLUDED.opening_type, "
        "  raw_attrs = EXCLUDED.raw_attrs, "
        "  geometry = EXCLUDED.geometry, "
        "  ingested_at = NOW()"
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def run(only_layer: str | None = None, discover_only: bool = False):
    if discover_only:
        for cfg in POLY_LAYERS + [{"service": OPENINGS_LAYER["service"]}]:
            svc = cfg.get("service")
            if not svc:
                print(f"[SKIP]  {cfg.get('layer_type')} has no static service path (would discover at runtime).")
                continue
            print(f"\n=== {svc} ===")
            try:
                for L in list_service_layers(svc):
                    print(f"  {L.get('id'):>3}: {L.get('name')}")
            except requests.RequestException as e:
                print(f"  [ERROR] {e}")
        return

    conn = connect()
    start = time.time()
    summary: list[tuple[str, int]] = []

    try:
        # Polygon layers
        for cfg in POLY_LAYERS:
            if only_layer and cfg["layer_type"] != only_layer:
                continue
            print(f"\n[INFO]  Layer: {cfg['layer_type']}")

            resolved = resolve_layer(cfg)
            if resolved is None:
                summary.append((cfg["layer_type"], 0))
                continue
            service, layer_id = resolved

            try:
                features = fetch_layer(service, layer_id)
            except requests.RequestException as e:
                print(f"[ERROR] Fetch failed for {cfg['layer_type']}: {e}")
                summary.append((cfg["layer_type"], 0))
                continue

            print(f"[OK]    Fetched {len(features):,} features.")
            count = upsert_polygons(conn, features, cfg["layer_type"], cfg["name_fields"])
            print(f"[OK]    Upserted {count:,} into odnr_hazard_layers.")
            summary.append((cfg["layer_type"], count))

        # Point layer (AUM openings)
        if not only_layer or only_layer == "aum_opening":
            print(f"\n[INFO]  Layer: aum_opening")
            try:
                features = fetch_layer(OPENINGS_LAYER["service"], OPENINGS_LAYER["layer_id"])
                print(f"[OK]    Fetched {len(features):,} features.")
                count = upsert_openings(conn, features, OPENINGS_LAYER["type_fields"])
                print(f"[OK]    Upserted {count:,} into aum_openings.")
                summary.append(("aum_opening", count))
            except requests.RequestException as e:
                print(f"[ERROR] Fetch failed for aum_opening: {e}")
                summary.append(("aum_opening", 0))

        elapsed = time.time() - start

        # Final stats
        with conn.cursor() as cur:
            cur.execute(
                "SELECT layer_type, COUNT(*) FROM odnr_hazard_layers GROUP BY layer_type ORDER BY layer_type"
            )
            poly_stats = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM aum_openings")
            opening_count = cur.fetchone()[0]

        print()
        print("─" * 55)
        print(f"[DONE]  Ingestion complete in {elapsed:.1f}s")
        print(f"        odnr_hazard_layers:")
        for lt, n in poly_stats:
            print(f"           {lt:<22} {n:>8,}")
        print(f"        aum_openings:           {opening_count:>8,}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest ODNR hazard overlays.")
    parser.add_argument("--layer", help="Run only one layer (aum_mine|aml_project|amlis_area|state_floodplain|dogrm_urban_area|aum_opening)")
    parser.add_argument("--discover", action="store_true",
                        help="Print available layers per MapServer and exit (no DB writes).")
    args = parser.parse_args()

    print()
    print("=== ODNR Hazard Overlays Ingestion ===")
    print()

    validate_env()
    run(only_layer=args.layer, discover_only=args.discover)
