"""
Ingest the Ohio EPA "Spills as Reported" current-year feed into oepa_spills.

Source is a public ArcGIS REST FeatureServer published by OGRIP with no auth:
  https://geo.epa.ohio.gov/arcgis/rest/services/EmergResponse/Spills2_OpenData/MapServer/0

Each row is one OEPA Office-of-Emergency-Response report. case_number repeats
across rows when an incident has multiple recovery line items, so this script
keys upserts on the source-side `objectid` (stable across publishes).

Front-end-only feature — does NOT contribute to well_risk_scores. The map and
the well sidebar surface these as visible-context only.

Usage:
    python ingest_oepa_spills.py            # full refresh, idempotent
"""

import json
import os
import sys
import time

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

# Windows console default codepage chokes on the unicode glyphs we use in logs.
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

LAYER_URL = (
    "https://geo.epa.ohio.gov/arcgis/rest/services/"
    "EmergResponse/Spills2_OpenData/MapServer/0"
)
PAGE_SIZE  = 1000   # server maxRecordCount is 3600; 1000 keeps payloads small.
BATCH_SIZE = 200

# Oil/gas classification lives in SQL (see classify_oil_gas) so the taxonomy
# survives an OEPA-side string change without re-ingesting. Insert sets
# is_oil_gas=false; we recompute via UPDATE after all rows are loaded.
OIL_GAS_SQL_PREDICATE = (
    "  reported_product ILIKE '%CRUDE%'"
    "  OR reported_product ILIKE '%BRINE%OIL%GAS%'"
    "  OR reported_product ILIKE '%BRINE%GAS%OIL%'"
    "  OR reported_product ILIKE 'NATURAL GAS%'"
    "  OR reported_product ILIKE '%CONDENSATE%'"
)


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


def fetch_layer() -> list[dict]:
    """
    Paginate the OEPA spills FeatureServer. Returns plain attribute dicts +
    geometry coords (we don't need full GeoJSON since each record is a single
    point — building the geometry from latitude/longitude is simpler than
    parsing the geojson response and gives us the same SRID 4326 result).
    """
    all_rows: list[dict] = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",            # plain JSON; we read attributes directly
            "outSR": 4326,
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields": "objectid ASC",
        }
        resp = requests.get(f"{LAYER_URL}/query", params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        all_rows.extend(features)
        offset += PAGE_SIZE
        print(f"        … fetched {len(all_rows):,} so far")

        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break

    return all_rows


def clean_text(v):
    """Strip whitespace and treat OEPA's literal string sentinels ("NULL",
    "N/A", "") as actual nulls so the front-end doesn't have to."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.upper() in {"NULL", "N/A", "NONE"}:
        return None
    return s


def epoch_ms_to_date(v):
    """OEPA returns dates as epoch milliseconds; convert to ISO yyyy-mm-dd."""
    if v is None:
        return None
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def insert_rows(conn, features: list[dict]) -> int:
    if not features:
        return 0

    rows = []
    for f in features:
        attrs = f.get("attributes") or {}
        geom  = f.get("geometry") or {}

        oid = attrs.get("objectid")
        if oid is None:
            continue  # PK required

        lng = attrs.get("longitude") if attrs.get("longitude") is not None else geom.get("x")
        lat = attrs.get("latitude")  if attrs.get("latitude")  is not None else geom.get("y")

        rows.append((
            int(oid),
            clean_text(attrs.get("casenumber")),
            clean_text(attrs.get("reportedproduct")),
            attrs.get("reportedamount"),
            clean_text(attrs.get("reporteduom")),
            attrs.get("recovamount"),
            clean_text(attrs.get("recovunit")),
            clean_text(attrs.get("recovproducttype")),
            clean_text(attrs.get("county")),
            clean_text(attrs.get("city_twn")),
            clean_text(attrs.get("waterway")),
            clean_text(attrs.get("oepadist")),
            epoch_ms_to_date(attrs.get("reporteddate")),
            attrs.get("spillyear"),
            attrs.get("spillmonthnum"),
            lat,
            lng,
            False,  # is_oil_gas — recomputed in SQL after insert (see classify_oil_gas)
        ))

    if not rows:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            args = ",".join(
                cur.mogrify(
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                    " CASE WHEN %s IS NULL OR %s IS NULL THEN NULL "
                    "      ELSE ST_SetSRID(ST_MakePoint(%s, %s), 4326) END)",
                    (*row, row[16], row[15], row[16], row[15]),  # lng, lat for both null-check + ST_MakePoint
                ).decode()
                for row in batch
            )
            cur.execute(
                "INSERT INTO oepa_spills ("
                "  source_objectid, case_number, reported_product, reported_amount, reported_uom,"
                "  recovered_amount, recovered_unit, recovered_product_type,"
                "  county, city_township, waterway, oepa_district,"
                "  reported_date, spill_year, spill_month_num,"
                "  latitude, longitude, is_oil_gas, geom"
                f") VALUES {args} "
                "ON CONFLICT (source_objectid) DO UPDATE SET "
                "  case_number = EXCLUDED.case_number,"
                "  reported_product = EXCLUDED.reported_product,"
                "  reported_amount = EXCLUDED.reported_amount,"
                "  reported_uom = EXCLUDED.reported_uom,"
                "  recovered_amount = EXCLUDED.recovered_amount,"
                "  recovered_unit = EXCLUDED.recovered_unit,"
                "  recovered_product_type = EXCLUDED.recovered_product_type,"
                "  county = EXCLUDED.county,"
                "  city_township = EXCLUDED.city_township,"
                "  waterway = EXCLUDED.waterway,"
                "  oepa_district = EXCLUDED.oepa_district,"
                "  reported_date = EXCLUDED.reported_date,"
                "  spill_year = EXCLUDED.spill_year,"
                "  spill_month_num = EXCLUDED.spill_month_num,"
                "  latitude = EXCLUDED.latitude,"
                "  longitude = EXCLUDED.longitude,"
                "  is_oil_gas = EXCLUDED.is_oil_gas,"
                "  geom = EXCLUDED.geom,"
                "  ingested_at = now()"
            )
            inserted += len(batch)

    conn.commit()
    return inserted


def classify_oil_gas(conn):
    """Recompute is_oil_gas from reported_product. Centralised in SQL so the
    taxonomy can be tweaked without re-ingesting the source."""
    with conn.cursor() as cur:
        cur.execute(f"UPDATE oepa_spills SET is_oil_gas = ({OIL_GAS_SQL_PREDICATE});")
    conn.commit()


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    conn = connect()
    start = time.time()
    try:
        print(f"[INFO]  Fetching from {LAYER_URL}/query …")
        features = fetch_layer()
        print(f"[OK]    Fetched {len(features):,} features.")

        if not features:
            print("[WARN]  No features returned. OEPA endpoint may be down.")
            return

        count = insert_rows(conn, features)
        classify_oil_gas(conn)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM oepa_spills")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM oepa_spills WHERE is_oil_gas")
            og = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT case_number) FROM oepa_spills")
            distinct_cases = cur.fetchone()[0]

        elapsed = time.time() - start
        print()
        print("─" * 60)
        print(f"[DONE]  OEPA spills ingest complete in {elapsed:.1f}s")
        print(f"        Upserted              : {count:>6,}")
        print(f"        oepa_spills total     : {total:>6,} rows")
        print(f"        Distinct case numbers : {distinct_cases:>6,}")
        print(f"        Oil/gas-tagged rows   : {og:>6,}")
        print("─" * 60)

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
    print("=== Ohio EPA Spills (Current Year) Ingestion ===")
    print()

    validate_env()
    run()
