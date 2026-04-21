"""
ETL: oil_gas_wells (raw text staging) -> wells (clean, typed)

Reads from the raw staging table in batches, transforms each row
(type casts, date parsing, geometry building, county FIPS lookup),
and upserts into the clean wells table.

Run after import_wells.py has loaded the CSV.
Usage:
    python etl_wells.py
"""

import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

BATCH_SIZE = 500

# Dates in the CSV include a time component: "11/16/2000 12:00:00 AM"
DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"

# Sentinel year used as a NULL stand-in in the source data
SENTINEL_YEAR = 1900


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


def load_county_fips(cur) -> dict:
    """
    Loads the counties table into a dict: UPPERCASE_NAME -> fips_code.
    Used to resolve each well's county name to the FK value.
    """
    cur.execute("SELECT name, fips_code FROM counties")
    mapping = {row[0].upper().strip(): row[1] for row in cur.fetchall()}
    print(f"[OK]    Loaded {len(mapping)} county FIPS codes.")
    return mapping


def safe_float(val) -> float | None:
    """Cast a text value to float, returning None for blank or unparseable input."""
    if not val or not str(val).strip():
        return None
    try:
        return float(str(val).strip())
    except ValueError:
        return None


def safe_int(val) -> int | None:
    """Cast a text value to int, returning None for blank or unparseable input."""
    if not val or not str(val).strip():
        return None
    try:
        # float() first to handle strings like "123.0"
        return int(float(str(val).strip()))
    except ValueError:
        return None


def parse_date(val) -> object | None:
    """
    Parse a date string in the format used by the ODNR CSV:
        "11/16/2000 12:00:00 AM"
    Returns a Python date object, or None for blank/sentinel/unparseable values.
    The source data uses 1/X/1900 as a NULL stand-in — those are also discarded.
    """
    if not val or not str(val).strip():
        return None
    try:
        dt = datetime.strptime(str(val).strip(), DATE_FORMAT)
        if dt.year == SENTINEL_YEAR:
            return None
        return dt.date()
    except ValueError:
        return None


def is_truthy(val) -> bool:
    """Returns True if val is a non-empty, non-whitespace string."""
    return bool(val and str(val).strip())


def is_valid_county(val: str) -> bool:
    """
    A small number of rows have date strings in the county column due to
    CSV column misalignment. Filter those out so they don't poison the lookup.
    """
    if not val or not val.strip():
        return False
    # Anything containing '/' is almost certainly a date, not a county name
    return "/" not in val


# Valid single-character slant codes used by ODNR
_VALID_SLANT = {"V", "H", "D", "O"}

# 6 rows in the source data have well_type = "1" — bad data from CSV misalignment
_INVALID_WELL_TYPES = {"1"}

def clean_well_type(val) -> str | None:
    """Discard known-bad well_type values that are clearly not well type strings."""
    if not val or not str(val).strip():
        return None
    v = str(val).strip()
    return None if v in _INVALID_WELL_TYPES else v

def clean_slant(val) -> str | None:
    """
    Real slant values are single chars: V (Vertical), H (Horizontal),
    D (Directional), O (Other). 9 rows contain an address string due to
    CSV column misalignment — those are discarded as NULL.
    """
    if not val or not str(val).strip():
        return None
    v = str(val).strip().upper()
    return v if v in _VALID_SLANT else None


def transform_row(row: dict, county_fips: dict) -> tuple | None:
    """
    Transform one raw oil_gas_wells row into a tuple ready for the
    wells INSERT. Returns None for rows that should be skipped entirely.

    Column order must match INSERT_COLUMNS and TEMPLATE below.
    """
    api_no = (row.get("permit_number_api") or "").strip()
    if not api_no:
        return None  # no API number — cannot be the PK, skip

    lat = safe_float(row.get("well_latitude"))
    lng = safe_float(row.get("well_longitude"))

    # Build WKT for PostGIS — None is safe, ST_GeomFromText(NULL) returns NULL
    geom_wkt = f"POINT({lng} {lat})" if lat is not None and lng is not None else None

    # County FIPS lookup — skip malformed county values (date strings, etc.)
    raw_county = (row.get("county") or "").strip()
    fips = None
    if is_valid_county(raw_county):
        fips = county_fips.get(raw_county.upper())

    orphan_raw = (row.get("orphan_well_program_status") or "").strip() or None

    # bottom_hole_latitude has 212k values; well_bh_lat only 5k — prefer the former
    bh_lat = safe_float(row.get("bottom_hole_latitude") or row.get("well_bh_lat"))
    bh_lng = safe_float(row.get("bottom_hole_longitude") or row.get("well_bh_long"))

    return (
        api_no,                                                       # api_no (PK)
        (row.get("well_name") or "").strip()          or None,        # well_name
        (row.get("well_operator") or "").strip()      or None,        # operator
        (row.get("well_op_address") or "").strip()    or None,        # operator_address
        (row.get("well_company_phone") or "").strip() or None,        # operator_phone
        raw_county                                    or None,        # county
        fips,                                                         # county_fips (FK)
        (row.get("township") or "").strip()           or None,        # township
        clean_well_type(row.get("well_type")),                        # well_type
        (row.get("well_lease_name") or "").strip()    or None,        # lease_name
        (row.get("well_number") or "").strip()        or None,        # well_number
        (row.get("well_status") or "").strip()        or None,        # status
        orphan_raw,                                                   # orphan_status
        orphan_raw is not None,                                       # in_orphan_program
        lat,                                                          # lat
        lng,                                                          # lng
        geom_wkt,                                                     # geometry (WKT)
        bh_lat,                                                       # bh_lat
        bh_lng,                                                       # bh_lng
        safe_int(row.get("well_total_depth")),                        # total_depth
        (row.get("deepest_formation") or "").strip()  or None,        # deepest_formation
        safe_float(row.get("du_acres")),                              # acreage
        safe_int(row.get("elevation")),                               # elevation
        clean_slant(row.get("slant")),                                # slant
        safe_float(row.get("well_ip_oil")),                           # ip_oil
        safe_float(row.get("well_ip_gas")),                           # ip_gas
        (row.get("producing_formation") or "").strip()  or None,      # prod_formation_1
        (row.get("second_prod_formation") or "").strip() or None,     # prod_formation_2
        parse_date(row.get("well_date_approved")),                    # permit_issued
        parse_date(row.get("well_date_complete")),                    # completion_date
        parse_date(row.get("well_date_plugged")),                     # plug_date
        is_truthy(row.get("geophys_logs")),                           # has_geophys_log
    )


# ── SQL ───────────────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO wells (
        api_no, well_name, operator, operator_address, operator_phone,
        county, county_fips, township, well_type, lease_name, well_number,
        status, orphan_status, in_orphan_program,
        lat, lng, geometry,
        bh_lat, bh_lng,
        total_depth, deepest_formation, acreage, elevation, slant,
        ip_oil, ip_gas, prod_formation_1, prod_formation_2,
        permit_issued, completion_date, plug_date,
        has_geophys_log
    )
    VALUES %s
    ON CONFLICT (api_no) DO UPDATE SET
        well_name         = EXCLUDED.well_name,
        operator          = EXCLUDED.operator,
        operator_address  = EXCLUDED.operator_address,
        operator_phone    = EXCLUDED.operator_phone,
        county            = EXCLUDED.county,
        county_fips       = EXCLUDED.county_fips,
        status            = EXCLUDED.status,
        orphan_status     = EXCLUDED.orphan_status,
        in_orphan_program = EXCLUDED.in_orphan_program,
        lat               = EXCLUDED.lat,
        lng               = EXCLUDED.lng,
        geometry          = EXCLUDED.geometry,
        bh_lat            = EXCLUDED.bh_lat,
        bh_lng            = EXCLUDED.bh_lng,
        total_depth       = EXCLUDED.total_depth,
        acreage           = EXCLUDED.acreage,
        elevation         = EXCLUDED.elevation,
        ip_oil            = EXCLUDED.ip_oil,
        ip_gas            = EXCLUDED.ip_gas,
        permit_issued     = EXCLUDED.permit_issued,
        completion_date   = EXCLUDED.completion_date,
        plug_date         = EXCLUDED.plug_date,
        has_geophys_log   = EXCLUDED.has_geophys_log,
        updated_at        = NOW()
"""

# Each %s corresponds to one value in the tuple returned by transform_row.
# The 17th placeholder (geometry) uses ST_GeomFromText so PostGIS parses the WKT.
TEMPLATE = (
    "(%s, %s, %s, %s, %s, "          # api_no … operator_phone
    "%s, %s, %s, %s, %s, %s, "       # county … well_number
    "%s, %s, %s, "                   # status, orphan_status, in_orphan_program
    "%s, %s, ST_GeomFromText(%s, 4326), "  # lat, lng, geometry
    "%s, %s, "                       # bh_lat, bh_lng
    "%s, %s, %s, %s, %s, "          # total_depth … slant
    "%s, %s, %s, %s, "              # ip_oil, ip_gas, prod_formation_1/2
    "%s, %s, %s, "                  # permit_issued, completion_date, plug_date
    "%s)"                            # has_geophys_log
)


# ── Main ─────────────────────────────────────────────────────────────────────

def run_etl(conn):
    county_fips = {}
    with conn.cursor() as cur:
        county_fips = load_county_fips(cur)

    # Count total source rows for progress reporting
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM oil_gas_wells")
        total = cur.fetchone()[0]
    print(f"[INFO]  {total:,} rows in oil_gas_wells to process.")

    inserted = 0
    updated  = 0
    skipped  = 0
    errors   = 0
    offset   = 0
    start    = time.time()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as read_cur, \
         conn.cursor() as write_cur:

        while True:
            read_cur.execute(
                "SELECT * FROM oil_gas_wells ORDER BY ctid LIMIT %s OFFSET %s",
                (BATCH_SIZE, offset),
            )
            rows = read_cur.fetchall()
            if not rows:
                break

            batch = []
            for row in rows:
                try:
                    t = transform_row(dict(row), county_fips)
                    if t is None:
                        skipped += 1
                    else:
                        batch.append(t)
                except Exception as e:
                    skipped += 1
                    print(f"[WARN]  Skipped row (offset {offset}): {e}")

            if batch:
                try:
                    execute_values(write_cur, INSERT_SQL, batch, template=TEMPLATE)
                    conn.commit()
                    inserted += len(batch)
                except psycopg2.Error as e:
                    conn.rollback()
                    errors += len(batch)
                    print(f"[ERROR] Batch at offset {offset} failed: {e}")

            offset += BATCH_SIZE

            # Progress every 10 batches
            if (offset // BATCH_SIZE) % 10 == 0:
                elapsed = time.time() - start
                rate    = offset / elapsed if elapsed > 0 else 0
                eta     = (total - offset) / rate if rate > 0 else 0
                print(
                    f"[PROG]  {offset:>9,} / {total:,} "
                    f"({offset / total * 100:5.1f}%)  "
                    f"{rate:,.0f} rows/s  ETA {eta:.0f}s"
                )

    elapsed = time.time() - start
    rate    = inserted / elapsed if elapsed > 0 else 0

    print()
    print("─" * 55)
    print(f"[DONE]  ETL complete in {elapsed:.1f}s  ({rate:,.0f} rows/s)")
    print(f"        Upserted : {inserted:>10,}")
    print(f"        Skipped  : {skipped:>10,}  (no API number or transform error)")
    print(f"        Errors   : {errors:>10,}  (batch insert failures)")
    print("─" * 55)

    # Validation query
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wells")
        final_count = cur.fetchone()[0]
    print(f"[INFO]  wells table now contains {final_count:,} rows.")

    if errors > 0:
        print("[WARN]  Some batches failed. Review errors above.")


if __name__ == "__main__":
    print()
    print("=== Oil & Gas Wells ETL: staging -> wells ===")
    print()

    validate_env()

    conn = connect()
    try:
        run_etl(conn)
    except KeyboardInterrupt:
        print()
        print("[ABORT] ETL interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Database connection closed.")
