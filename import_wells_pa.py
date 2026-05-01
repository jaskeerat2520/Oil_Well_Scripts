"""
Ingest Pennsylvania DEP oil & gas well records into the `wells` table.

Source: PA DEP "Oil and Gas Wells All" layer
        https://gis.dep.pa.gov/depgisprd/rest/services/OilGas/OilGasAllStrayGasEGSP/MapServer/3
        (~223,880 wells, paginated 5000 per request)

PA-specific rules:
  - api_no is namespaced as "PA-<PERMIT_NUMBER>" so PA permit numbers
    (5-6 digits) cannot collide with Ohio's short legacy api_no values.
  - state_code='PA' is set on every row.
  - county_fips is resolved from the counties table at ingest time. If PA
    county geometry hasn't been loaded yet, county_fips stays NULL and can
    be backfilled later.
  - Status values are preserved verbatim (e.g. 'DEP Orphan List', 'Plugged
    OG Well'); the existing Ohio-specific exclusion lists in score_*.py do
    not apply to PA. Scoring for PA is out of scope for the PoC.

Usage:
    python import_wells_pa.py
"""

import datetime as dt
import os
import sys
import time
from typing import Optional

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

LAYER_URL  = "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas/OilGasAllStrayGasEGSP/MapServer/3/query"
PAGE_SIZE  = 5000
BATCH_SIZE = 500
STATE_CODE = "PA"

# ArcGIS REST returns dates as epoch milliseconds (UTC). Helper below converts.


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


def fetch_pa_county_fips(conn) -> dict[str, str]:
    """county name (uppercase, trimmed) -> 5-digit fips_code, scoped to PA."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, fips_code FROM counties WHERE state_code = %s",
            (STATE_CODE,),
        )
        return {name.upper().strip(): fips for name, fips in cur.fetchall()}


def epoch_ms_to_date(ms) -> Optional[dt.date]:
    if ms is None or ms == "" or ms == 0:
        return None
    try:
        return dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc).date()
    except (ValueError, OSError, OverflowError):
        return None


def feature_count(session: requests.Session) -> int:
    resp = session.get(
        LAYER_URL,
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("count", 0)


def fetch_page(session: requests.Session, offset: int) -> list[dict]:
    """Fetch one page of features (ArcGIS native JSON)."""
    resp = session.get(
        LAYER_URL,
        params={
            "where":             "1=1",
            "outFields":         "*",
            "outSR":             "4326",
            "returnGeometry":    "true",
            "resultOffset":      offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields":     "OBJECTID",
            "f":                 "json",
        },
        timeout=180,
    )
    resp.raise_for_status()
    return resp.json().get("features", [])


def map_feature(feature: dict, county_fips_lookup: dict[str, str]) -> Optional[tuple]:
    """
    Convert one PA DEP feature to a `wells` row tuple.
    Returns None for unusable rows (no permit number, no geometry).
    """
    attrs = feature.get("attributes") or {}
    geom  = feature.get("geometry") or {}

    permit = (attrs.get("PERMIT_NUMBER") or "").strip()
    if not permit:
        return None

    api_no = f"PA-{permit}"

    lat = attrs.get("LATITUDE")
    lng = attrs.get("LONGITUDE")
    # Fall back to geometry x/y if attribute lat/lng missing.
    if (lat is None or lng is None) and geom:
        lng = lng if lng is not None else geom.get("x")
        lat = lat if lat is not None else geom.get("y")

    if lat is None or lng is None:
        return None

    county_raw = (attrs.get("COUNTY") or "").upper().strip() or None
    county_fips = county_fips_lookup.get(county_raw) if county_raw else None

    return (
        api_no,                                     # api_no
        STATE_CODE,                                 # state_code
        (attrs.get("WELL_NAME") or "").strip() or None,
        (attrs.get("OPERATOR") or "").strip() or None,
        county_raw,                                 # county
        county_fips,                                # county_fips
        (attrs.get("MUNICIPALITY") or "").strip() or None,  # township
        (attrs.get("WELL_TYPE") or "").strip() or None,
        None,                                       # lease_name (PA has no equivalent)
        None,                                       # well_number (PA bundles into PERMIT_NUMBER)
        (attrs.get("WELL_STATUS") or "").strip() or None,
        float(lat),
        float(lng),
        epoch_ms_to_date(attrs.get("PERMIT_DATE")),     # permit_issued
        epoch_ms_to_date(attrs.get("SPUD_DATE")),       # completion_date proxy
        epoch_ms_to_date(attrs.get("DATE_PLUGGED")),    # plug_date
    )


INSERT_SQL = """
INSERT INTO wells (
    api_no, state_code, well_name, operator,
    county, county_fips, township,
    well_type, lease_name, well_number, status,
    lat, lng, geometry,
    permit_issued, completion_date, plug_date
) VALUES %s
ON CONFLICT (api_no) DO UPDATE SET
    well_name       = EXCLUDED.well_name,
    operator        = EXCLUDED.operator,
    county          = EXCLUDED.county,
    county_fips     = COALESCE(EXCLUDED.county_fips, wells.county_fips),
    township        = EXCLUDED.township,
    well_type       = EXCLUDED.well_type,
    status          = EXCLUDED.status,
    lat             = EXCLUDED.lat,
    lng             = EXCLUDED.lng,
    geometry        = EXCLUDED.geometry,
    permit_issued   = EXCLUDED.permit_issued,
    completion_date = EXCLUDED.completion_date,
    plug_date       = EXCLUDED.plug_date,
    updated_at      = NOW()
"""

# Each VALUES tuple is rendered with ST_SetSRID(ST_MakePoint(lng,lat), 4326) for
# the geometry column. execute_values + a `template` lets us reference the
# tuple positions for that expression while still using literal binding for the
# rest of the columns.
INSERT_TEMPLATE = (
    "(%s, %s, %s, %s, "
    " %s, %s, %s, "
    " %s, %s, %s, %s, "
    " %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), "
    " %s, %s, %s)"
)


def expand_for_geometry(row: tuple) -> tuple:
    """Repeat lng,lat into the geometry slot to match INSERT_TEMPLATE column count."""
    (
        api_no, state_code, well_name, operator,
        county, county_fips, township,
        well_type, lease_name, well_number, status,
        lat, lng,
        permit_issued, completion_date, plug_date,
    ) = row
    return (
        api_no, state_code, well_name, operator,
        county, county_fips, township,
        well_type, lease_name, well_number, status,
        lat, lng,             # plain columns
        lng, lat,             # geometry: ST_MakePoint takes lng, lat
        permit_issued, completion_date, plug_date,
    )


def upsert_batch(cur, batch: list[tuple]) -> int:
    """
    Upsert a batch, deduplicating on api_no (column 0) with last-write-wins.
    PA's MapServer layer 3 contains multiple records per PERMIT_NUMBER for some
    wells (multi-bore, ORND/PBNM consolidated rows, etc). Without intra-batch
    dedup, ON CONFLICT raises CardinalityViolation. Returns count of unique
    rows actually sent.
    """
    if not batch:
        return 0
    seen: dict[str, tuple] = {}
    for row in batch:
        seen[row[0]] = row  # row[0] = api_no
    deduped = list(seen.values())
    expanded = [expand_for_geometry(r) for r in deduped]
    execute_values(cur, INSERT_SQL, expanded, template=INSERT_TEMPLATE)
    return len(deduped)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    validate_env()
    conn = connect()

    session = requests.Session()
    session.headers["User-Agent"] = "ohio-wells-multistate-poc/0.1 (+pa ingest)"

    try:
        county_lookup = fetch_pa_county_fips(conn)
        if not county_lookup:
            print(
                "[WARN]  counties table has no rows for PA. county_fips will be NULL "
                "until you run: python import_county_geometry.py --state PA"
            )
        else:
            print(f"[OK]    Loaded {len(county_lookup)} PA county FIPS mappings.")

        total = feature_count(session)
        print(f"[INFO]  PA DEP layer reports {total:,} features.")

        offset = 0
        inserted = 0
        skipped  = 0
        start    = time.time()

        with conn.cursor() as cur:
            while True:
                features = fetch_page(session, offset)
                if not features:
                    break

                batch: list[tuple] = []
                for f in features:
                    row = map_feature(f, county_lookup)
                    if row is None:
                        skipped += 1
                        continue
                    batch.append(row)
                    if len(batch) >= BATCH_SIZE:
                        inserted += upsert_batch(cur, batch)
                        conn.commit()
                        batch = []

                if batch:
                    inserted += upsert_batch(cur, batch)
                    conn.commit()
                    batch = []

                offset += PAGE_SIZE
                elapsed = time.time() - start
                rate    = inserted / elapsed if elapsed > 0 else 0
                pct     = inserted / total * 100 if total else 0
                eta     = (total - inserted) / rate if rate > 0 else 0
                print(
                    f"[PROG]  {inserted:>9,} / {total:,} rows  "
                    f"({pct:5.1f}%)  {rate:,.0f} rows/s  ETA {eta:.0f}s"
                )

                if len(features) < PAGE_SIZE:
                    break

        elapsed = time.time() - start
        print()
        print("-" * 55)
        print(f"[DONE]  PA wells ingestion complete in {elapsed:.1f}s.")
        print(f"        Upserted : {inserted:>10,}")
        print(f"        Skipped  : {skipped:>10,}  (no permit number or no geometry)")
        print("-" * 55)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT state_code, COUNT(*) FROM wells GROUP BY state_code ORDER BY state_code"
            )
            for sc, cnt in cur.fetchall():
                print(f"        wells[{sc}] = {cnt:,}")

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
    print("=== PA DEP Oil & Gas Wells Importer ===")
    print()
    run()
