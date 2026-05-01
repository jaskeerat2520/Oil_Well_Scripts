"""
Ingest West Virginia DEP / TAGIS oil & gas well records into the `wells` table.

Source: WVDEP_enterprise/oil_gas Layer 7 "All DEP Wells"
        https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/7
        (~153,267 wells, paginated 3000 per request)

WV-specific rules:
  - api_no is namespaced as "WV-<api>" so WV API numbers can never collide
    with Ohio's short legacy api_no values (e.g. '8', '37') or PA's
    "PA-..." namespace.
  - state_code='WV' is set on every row.
  - county_fips is resolved from the counties table at ingest time.
  - plug_date is *inferred*: the WV layer has no explicit DATE_PLUGGED field,
    so plug_date = compdate when wellstatus = 'Plugged'. Otherwise NULL.
  - operator maps from `respparty` (Responsible Party); WV does not
    distinguish operator from responsible party in this dataset.
  - status values are preserved verbatim.

Usage:
    python import_wells_wv.py
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

LAYER_URL  = "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/7/query"
PAGE_SIZE  = 3000
BATCH_SIZE = 500
STATE_CODE = "WV"


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


def fetch_wv_county_fips(conn) -> dict[str, str]:
    """county name (uppercase, trimmed) -> 5-digit fips_code, scoped to WV."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, fips_code FROM counties WHERE state_code = %s",
            (STATE_CODE,),
        )
        return {name.upper().strip(): fips for name, fips in cur.fetchall()}


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
)


def parse_wv_date(value) -> Optional[dt.date]:
    """WV TAGIS returns dates as strings, but the format varies; try common ones."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc).date()
        except (ValueError, OSError, OverflowError):
            return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
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
    resp = session.get(
        LAYER_URL,
        params={
            "where":             "1=1",
            "outFields":         "*",
            "outSR":             "4326",
            "returnGeometry":    "true",
            "resultOffset":      offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields":     "objectid",
            "f":                 "json",
        },
        timeout=180,
    )
    resp.raise_for_status()
    return resp.json().get("features", [])


def map_feature(feature: dict, county_fips_lookup: dict[str, str]) -> Optional[tuple]:
    attrs = feature.get("attributes") or {}
    geom  = feature.get("geometry") or {}

    api_raw = (attrs.get("api") or "").strip()
    if not api_raw:
        return None

    api_no = f"WV-{api_raw}"

    lat = attrs.get("welly")
    lng = attrs.get("wellx")
    if (lat is None or lng is None) and geom:
        lng = lng if lng is not None else geom.get("x")
        lat = lat if lat is not None else geom.get("y")
    if lat is None or lng is None:
        return None

    county_raw = (attrs.get("county") or "").upper().strip() or None
    county_fips = county_fips_lookup.get(county_raw) if county_raw else None

    status        = (attrs.get("wellstatus") or "").strip() or None
    permit_issued   = parse_wv_date(attrs.get("issuedate"))
    completion_date = parse_wv_date(attrs.get("compdate"))
    # WV has no explicit plug date — infer from status.
    plug_date = completion_date if status == "Plugged" else None

    well_depth_raw = (attrs.get("welldepth") or "").strip()
    try:
        total_depth: Optional[int] = int(float(well_depth_raw)) if well_depth_raw else None
    except ValueError:
        total_depth = None

    return (
        api_no,                                                 # api_no
        STATE_CODE,                                             # state_code
        None,                                                   # well_name (WV bundles into farmname/wellnumber)
        (attrs.get("respparty") or "").strip() or None,         # operator
        county_raw,
        county_fips,
        None,                                                   # township
        (attrs.get("welltype") or "").strip() or None,
        (attrs.get("farmname") or "").strip() or None,          # lease_name
        (attrs.get("wellnumber") or "").strip() or None,        # well_number
        status,
        float(lat),
        float(lng),
        total_depth,
        permit_issued,
        completion_date,
        plug_date,
    )


INSERT_SQL = """
INSERT INTO wells (
    api_no, state_code, well_name, operator,
    county, county_fips, township,
    well_type, lease_name, well_number, status,
    lat, lng, geometry, total_depth,
    permit_issued, completion_date, plug_date
) VALUES %s
ON CONFLICT (api_no) DO UPDATE SET
    operator        = EXCLUDED.operator,
    county          = EXCLUDED.county,
    county_fips     = COALESCE(EXCLUDED.county_fips, wells.county_fips),
    well_type       = EXCLUDED.well_type,
    lease_name      = EXCLUDED.lease_name,
    well_number     = EXCLUDED.well_number,
    status          = EXCLUDED.status,
    lat             = EXCLUDED.lat,
    lng             = EXCLUDED.lng,
    geometry        = EXCLUDED.geometry,
    total_depth     = EXCLUDED.total_depth,
    permit_issued   = EXCLUDED.permit_issued,
    completion_date = EXCLUDED.completion_date,
    plug_date       = EXCLUDED.plug_date,
    updated_at      = NOW()
"""

INSERT_TEMPLATE = (
    "(%s, %s, %s, %s, "
    " %s, %s, %s, "
    " %s, %s, %s, %s, "
    " %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, "
    " %s, %s, %s)"
)


def expand_for_geometry(row: tuple) -> tuple:
    (
        api_no, state_code, well_name, operator,
        county, county_fips, township,
        well_type, lease_name, well_number, status,
        lat, lng, total_depth,
        permit_issued, completion_date, plug_date,
    ) = row
    return (
        api_no, state_code, well_name, operator,
        county, county_fips, township,
        well_type, lease_name, well_number, status,
        lat, lng,
        lng, lat,             # geometry positional args (ST_MakePoint(lng, lat))
        total_depth,
        permit_issued, completion_date, plug_date,
    )


def upsert_batch(cur, batch: list[tuple]) -> int:
    """
    Upsert a batch, deduplicating on api_no (column 0) with last-write-wins.
    TAGIS layer 7 can contain multiple rows per `api` (the same well registered
    under multiple programs, or layer-overlap artefacts). Without intra-batch
    dedup, ON CONFLICT raises CardinalityViolation. Returns the unique row
    count actually sent so progress reporting stays honest.
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
    session.headers["User-Agent"] = "ohio-wells-multistate-poc/0.1 (+wv ingest)"

    try:
        county_lookup = fetch_wv_county_fips(conn)
        if not county_lookup:
            print(
                "[WARN]  counties table has no rows for WV. county_fips will be NULL "
                "until you run: python import_county_geometry.py --state WV"
            )
        else:
            print(f"[OK]    Loaded {len(county_lookup)} WV county FIPS mappings.")

        total = feature_count(session)
        print(f"[INFO]  WV TAGIS layer reports {total:,} features.")

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
        print(f"[DONE]  WV wells ingestion complete in {elapsed:.1f}s.")
        print(f"        Upserted : {inserted:>10,}")
        print(f"        Skipped  : {skipped:>10,}  (no api or no geometry)")
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
    print("=== WV TAGIS Oil & Gas Wells Importer ===")
    print()
    run()
