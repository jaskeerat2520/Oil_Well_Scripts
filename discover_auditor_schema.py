"""
Probe a candidate county-auditor ArcGIS REST FeatureServer URL and report
everything needed to wire it into ingest_parcels.py.

Why this exists: extending owner-name discovery from Hocking-only to all 86
Ohio counties means adding one entry to COUNTY_LAYER_URLS per county, AND
making sure the auditor's parcel_id format aligns with the OGRIP-loaded
LocalParcelID format already in the parcels table. A format mismatch causes
the join to silently fail (surface_owner_name stays NULL with no error).
This script catches that before any bulk ingest commits.

Usage:
    python discover_auditor_schema.py --county ALLEN --url <REST_URL>

Where <REST_URL> is the FeatureServer layer URL captured from browser
DevTools, e.g.
    https://services7.arcgis.com/clXmZ04BrbYyKlqh/arcgis/rest/services/parcel_joinedHocOH/FeatureServer/0

Read-only — does not write to the DB.
"""

import argparse
import json
import os
import re
import sys

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

# Same logical-field → candidate-keys map as ingest_parcels.py. Kept in sync
# manually for now; if it grows to 30+ counties we should refactor both into
# a shared module.
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


def connect():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


# ── REST probes ───────────────────────────────────────────────────────────────

def fetch_json(url: str, params: dict | None = None) -> dict:
    p = {"f": "json"}
    if params:
        p.update(params)
    r = requests.get(url, params=p, timeout=60)
    r.raise_for_status()
    body = r.json()
    if isinstance(body, dict) and body.get("error"):
        raise RuntimeError(f"ArcGIS error: {body['error']}")
    return body


def probe_layer_metadata(layer_url: str) -> dict:
    """GET <url>?f=json — returns layer name, geometry type, fields, SRID, max page size."""
    return fetch_json(layer_url)


def probe_count(layer_url: str) -> int:
    body = fetch_json(f"{layer_url}/query", {"where": "1=1", "returnCountOnly": "true"})
    return body.get("count", 0)


def probe_sample_feature(layer_url: str) -> dict | None:
    body = fetch_json(f"{layer_url}/query", {
        "where": "1=1", "outFields": "*",
        "resultRecordCount": 1, "returnGeometry": "false",
    })
    feats = body.get("features", [])
    if not feats:
        return None
    return feats[0].get("attributes") or {}


def probe_sample_parcel_ids(layer_url: str, parcel_field: str, n: int = 5) -> list[str]:
    body = fetch_json(f"{layer_url}/query", {
        "where": f"{parcel_field} IS NOT NULL",
        "outFields": parcel_field,
        "resultRecordCount": n, "returnGeometry": "false",
    })
    out = []
    for f in body.get("features", []):
        v = (f.get("attributes") or {}).get(parcel_field)
        if v is not None:
            out.append(str(v).strip())
    return out


# ── Field detection ──────────────────────────────────────────────────────────

def detect_field_map(field_names: list[str]) -> dict[str, str | None]:
    """For each logical field, pick the first FIELD_CANDIDATES entry that's
    present in field_names."""
    out: dict[str, str | None] = {}
    for logical, candidates in FIELD_CANDIDATES.items():
        out[logical] = next((c for c in candidates if c in field_names), None)
    return out


# ── Format alignment ─────────────────────────────────────────────────────────

def shape_signature(s: str) -> str:
    """Reduce a string to its character-class shape: digits→9, letters→A,
    keep dashes/dots/underscores. Two parcel IDs with the same signature are
    safely joinable; different signatures need normalization."""
    return re.sub(r"[A-Za-z]", "A", re.sub(r"\d", "9", s))


def fetch_existing_ogrip_samples(conn, county: str, n: int = 5) -> list[str]:
    """Pull n sample parcel IDs from already-loaded OGRIP rows for this county."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT parcel_id FROM parcels WHERE county = %s ORDER BY id LIMIT %s",
            (county, n),
        )
        return [row[0] for row in cur.fetchall()]


def compare_format(auditor_ids: list[str], ogrip_ids: list[str]) -> dict:
    """Return a structured comparison: shapes match? sample-by-sample?"""
    a_sigs = sorted({shape_signature(s) for s in auditor_ids if s})
    o_sigs = sorted({shape_signature(s) for s in ogrip_ids if s})
    return {
        "auditor_signatures": a_sigs,
        "ogrip_signatures":   o_sigs,
        "shapes_match":       a_sigs == o_sigs and len(a_sigs) == 1,
        "auditor_samples":    auditor_ids,
        "ogrip_samples":      ogrip_ids,
    }


# ── Reporting ────────────────────────────────────────────────────────────────

def print_report(county: str, layer_url: str, meta: dict, count: int,
                 sample: dict | None, field_map: dict, fmt: dict | None):
    sep = "─" * 65
    print()
    print(sep)
    print(f"  Auditor schema probe — {county}")
    print(sep)

    print(f"  Layer URL  : {layer_url}")
    print(f"  Layer name : {meta.get('name', '?')}")
    print(f"  Geom type  : {meta.get('geometryType', '?')}")
    sr = meta.get("sourceSpatialReference") or meta.get("extent", {}).get("spatialReference") or {}
    print(f"  SRID       : wkid {sr.get('wkid', '?')} (latest {sr.get('latestWkid', '?')})")
    print(f"  Max page   : {meta.get('maxRecordCount', '?')}")
    print(f"  Features   : {count:,}")
    print()

    print("  Field mapping (logical → source key):")
    for logical, actual in field_map.items():
        marker = "✓" if actual else "✗"
        print(f"    {marker} {logical:25s} → {actual or '(not found)'}")

    if sample:
        print()
        print("  Sample feature attributes:")
        for k, v in sample.items():
            sv = str(v)[:60]
            print(f"    {k:30s} = {sv}")

    if fmt:
        print()
        print("  Parcel-ID format alignment vs OGRIP rows in DB:")
        print(f"    auditor samples : {fmt['auditor_samples']}")
        print(f"    OGRIP samples   : {fmt['ogrip_samples']}")
        print(f"    auditor shapes  : {fmt['auditor_signatures']}")
        print(f"    OGRIP shapes    : {fmt['ogrip_signatures']}")
        if fmt["shapes_match"]:
            print(f"    ✓ MATCH — safe to ingest with current join key.")
        else:
            print(f"    ✗ MISMATCH — adding this county will need a normalization step")
            print(f"                  before the (county, parcel_id) join works.")

    print()
    print(sep)
    print("  Recommended additions to ingest_parcels.py:")
    print(sep)
    print(f"  COUNTY_LAYER_URLS additions:")
    print(f'      "{county}": "{layer_url}",')

    # Surface any source-side field names not yet in FIELD_CANDIDATES that
    # look promising (matched by alias/common pattern).
    if sample:
        new_keys: dict[str, list[str]] = {}
        for logical, actual in field_map.items():
            if actual:
                continue
            for k in sample.keys():
                if _key_looks_like(k, logical):
                    new_keys.setdefault(logical, []).append(k)
        if new_keys:
            print()
            print("  FIELD_CANDIDATES additions to consider:")
            for logical, keys in new_keys.items():
                print(f'      "{logical}": [..., {", ".join(repr(k) for k in keys)}],')

    print()


def _key_looks_like(source_key: str, logical: str) -> bool:
    """Heuristic for surfacing unmapped source-side fields that probably
    correspond to one of our logical fields."""
    k = source_key.lower()
    if logical == "parcel_id":
        return any(t in k for t in ("parcel", "pin", "pid"))
    if logical == "owner_name":
        return "owner" in k and "address" not in k and "city" not in k and "state" not in k and "zip" not in k
    if logical == "owner_mailing_address":
        return ("mail" in k or "owner" in k) and ("addr" in k or "street" in k or "address" in k)
    if logical == "owner_mailing_city":
        return ("mail" in k or "owner" in k) and "city" in k
    if logical == "owner_mailing_state":
        return ("mail" in k or "owner" in k) and "state" in k
    if logical == "owner_mailing_zip":
        return ("mail" in k or "owner" in k) and "zip" in k
    if logical == "acreage":
        return "acre" in k
    if logical == "land_use_code":
        return "use" in k or "class" in k or "luc" in k
    if logical == "tax_district":
        return "district" in k or "tax_dist" in k
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run(county: str, url: str, skip_db: bool):
    print(f"[INFO]  Probing {url} …")
    meta = probe_layer_metadata(url)
    field_names = [f["name"] for f in meta.get("fields", [])]
    field_map   = detect_field_map(field_names)

    count = probe_count(url)
    sample = probe_sample_feature(url)

    fmt = None
    if not skip_db and field_map["parcel_id"]:
        try:
            ids_auditor = probe_sample_parcel_ids(url, field_map["parcel_id"])
            with connect() as conn:
                ids_ogrip = fetch_existing_ogrip_samples(conn, county)
            if ids_ogrip:
                fmt = compare_format(ids_auditor, ids_ogrip)
            else:
                print(f"[INFO]  No OGRIP rows in parcels for {county} — "
                      f"format-alignment check skipped.")
        except psycopg2.Error as e:
            print(f"[WARN]  DB probe failed: {e}")
    elif not field_map["parcel_id"]:
        print(f"[WARN]  No parcel_id field detected — extend FIELD_CANDIDATES "
              f"first, then re-run.")

    print_report(county, url, meta, count, sample, field_map, fmt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", required=True,
                        help="County name UPPERCASE (e.g. ALLEN). Must match wells.county "
                             "casing for the format-alignment check to find OGRIP rows.")
    parser.add_argument("--url", required=True,
                        help="Full ArcGIS REST FeatureServer layer URL, ending in /<layerNum>. "
                             "Get from browser DevTools network tab on the auditor's web map.")
    parser.add_argument("--skip-db", action="store_true",
                        help="Skip the parcel-ID format-alignment DB check (use if you "
                             "haven't loaded OGRIP rows for this county yet).")
    args = parser.parse_args()

    if not (DB_HOST and DB_PASSWORD) and not args.skip_db:
        print("[ERROR] DB env vars missing. Pass --skip-db to do schema-only probe.")
        sys.exit(1)

    run(args.county.upper(), args.url, args.skip_db)
