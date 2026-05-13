"""
Ingest EPA Toxic Release Inventory (TRI) facility master data into PostGIS.

Source: EPA Envirofacts tri_facility endpoint, downloaded as CSV
(https://data.epa.gov/efservice/tri_facility/state_abbr/OH/CSV).
Default input path is ./CSV.csv at the repo root.

Tier 1 informational — facility proximity context for wells. Does NOT touch
composite_risk_score; matches the schools / hospitals pattern.

Behavior:
    - Prefers EPA's QA'd `pref_latitude`/`pref_longitude` over the original
      `fac_latitude`/`fac_longitude`. Either being non-empty / non-zero counts.
    - Inserts rows with no usable coords (geometry NULL) so the parent-company
      crosslinks survive even when geocoding is missing.
    - UPSERTs on tri_facility_id so re-running is safe.
    - Preserves the entire source row in raw_attrs JSONB for forward-compat.

Usage:
    python ingest_tri_facilities.py
    python ingest_tri_facilities.py --csv path/to/file.csv
"""

import argparse
import csv
import json
import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

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

DEFAULT_CSV = "CSV.csv"
BATCH_SIZE  = 500


# ── Helpers ───────────────────────────────────────────────────────────────────

def validate_env():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    print("[OK]    Environment variables loaded.")


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


# Ohio bounds. Used as a sanity gate after coordinate parsing — anything outside
# this box is wrong (most likely a sign-strip or DDMMSS misdecode), so reject.
OH_LAT_MIN, OH_LAT_MAX = 38.0, 42.5
OH_LNG_MIN, OH_LNG_MAX = -85.0, -80.0


def parse_coord(s: str) -> float | None:
    """Return a float or None for an EPA lat/lng cell. Treats 0.0 as missing."""
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        f = float(s)
        return f if f != 0.0 else None
    except ValueError:
        return None


def decode_dms_packed(value: float | None) -> float | None:
    """Decode EPA's legacy 6-digit DDDMMSS packed integer into decimal degrees.

    A value like 824914 is parsed as 82°49'14" → 82.8206°. Detection rule:
    if |value| > 180, it can't be plain decimal (lng is bounded by 180), so
    it must be packed. Returns the decimal value, sign preserved.
    """
    if value is None:
        return None
    sign = -1 if value < 0 else 1
    v = abs(value)
    if v <= 180:
        return value  # already decimal
    # Packed: ddmmss (lat) or dddmmss (lng).
    deg = int(v // 10000)
    mn  = int((v // 100) % 100)
    sec = v - (deg * 10000) - (mn * 100)
    return sign * (deg + mn / 60.0 + sec / 3600.0)


def normalize_oh(lat: float | None, lng: float | None) -> tuple[float | None, float | None]:
    """Apply EPA-CSV-quirk fixes: negate stripped longitudes, decode DDMMSS,
    then clamp-validate against Ohio bounds. Returns (None, None) on failure."""
    if lat is None or lng is None:
        return None, None
    # First pass: decode legacy DDMMSS if abs > 180.
    lat_d = decode_dms_packed(lat)
    lng_d = decode_dms_packed(lng)
    if lat_d is None or lng_d is None:
        return None, None
    # EPA's `pref_*` exporter strips the W-hemisphere negative sign on longitude.
    # Ohio is always Western; if lng came in positive in the OH range, negate.
    if lng_d > 0 and OH_LNG_MIN <= -lng_d <= OH_LNG_MAX:
        lng_d = -lng_d
    # Final sanity gate.
    if not (OH_LAT_MIN <= lat_d <= OH_LAT_MAX):
        return None, None
    if not (OH_LNG_MIN <= lng_d <= OH_LNG_MAX):
        return None, None
    return lat_d, lng_d


def best_coord(row: dict) -> tuple[float | None, float | None, str | None]:
    """Prefer EPA's QA'd pref_* over the original fac_*. Both formats are
    normalized through normalize_oh() before being accepted."""
    plat = parse_coord(row.get("pref_latitude"))
    plng = parse_coord(row.get("pref_longitude"))
    plat, plng = normalize_oh(plat, plng)
    if plat is not None and plng is not None:
        return plat, plng, "pref"
    flat = parse_coord(row.get("fac_latitude"))
    flng = parse_coord(row.get("fac_longitude"))
    flat, flng = normalize_oh(flat, flng)
    if flat is not None and flng is not None:
        return flat, flng, "fac"
    return None, None, None


def is_closed(row: dict) -> bool:
    v = (row.get("fac_closed_ind") or "").strip()
    return v in ("1", "Y", "y", "T", "TRUE", "true")


# ── Insert path ──────────────────────────────────────────────────────────────

def upsert_rows(conn, rows: list[dict]) -> tuple[int, int, int]:
    """
    Returns (inserted_or_updated, with_geom, closed).
    Deduplicates on tri_facility_id within the input — Envirofacts can
    occasionally double-list a facility under permit history changes.
    """
    seen: dict[str, dict] = {}
    for r in rows:
        fid = (r.get("tri_facility_id") or "").strip()
        if not fid:
            continue
        # Keep the row with valid geometry over one without; otherwise keep first.
        existing = seen.get(fid)
        new_has_geom = best_coord(r)[0] is not None
        old_has_geom = existing and best_coord(existing)[0] is not None
        if existing is None or (new_has_geom and not old_has_geom):
            seen[fid] = r

    inserted = 0
    with_geom = 0
    closed = 0

    with conn.cursor() as cur:
        batch = []
        for r in seen.values():
            fid           = r["tri_facility_id"].strip()
            facility_name = (r.get("facility_name") or "").strip() or None
            parent        = (r.get("standardized_parent_company") or r.get("parent_co_name") or "").strip() or None
            if parent and parent.upper() == "NA":
                parent = None
            foreign_parent = (r.get("standardized_foreign_parent_company")
                              or r.get("foreign_parent_co_name") or "").strip() or None
            if foreign_parent and foreign_parent.upper() == "NA":
                foreign_parent = None
            street  = (r.get("street_address") or "").strip() or None
            city    = (r.get("city_name") or "").strip() or None
            county  = (r.get("county_name") or "").strip().upper() or None
            zipc    = (r.get("zip_code") or "").strip() or None
            ic      = is_closed(r)
            if ic:
                closed += 1

            lat, lng, src = best_coord(r)
            if lat is not None:
                with_geom += 1

            raw = json.dumps(r, ensure_ascii=False)
            batch.append((fid, facility_name, parent, foreign_parent,
                          street, city, county, zipc, ic, raw, lng, lat, src))

            if len(batch) >= BATCH_SIZE:
                _flush(cur, batch)
                inserted += len(batch)
                batch = []

        if batch:
            _flush(cur, batch)
            inserted += len(batch)

    conn.commit()
    return inserted, with_geom, closed


def _flush(cur, batch):
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, "
            "CASE WHEN %s IS NULL OR %s IS NULL THEN NULL "
            "     ELSE ST_SetSRID(ST_MakePoint(%s::float, %s::float), 4326) END, %s)",
            (fid, facility_name, parent, foreign_parent,
             street, city, county, zipc, ic, raw,
             lng, lat, lng, lat, src),
        ).decode()
        for (fid, facility_name, parent, foreign_parent,
             street, city, county, zipc, ic, raw, lng, lat, src) in batch
    )
    cur.execute(
        "INSERT INTO tri_facilities "
        "(tri_facility_id, facility_name, parent_company, foreign_parent, "
        " street_address, city, county, zip, is_closed, raw_attrs, geometry, geocode_source) "
        f"VALUES {args} "
        "ON CONFLICT (tri_facility_id) DO UPDATE SET "
        "  facility_name   = EXCLUDED.facility_name, "
        "  parent_company  = EXCLUDED.parent_company, "
        "  foreign_parent  = EXCLUDED.foreign_parent, "
        "  street_address  = EXCLUDED.street_address, "
        "  city            = EXCLUDED.city, "
        "  county          = EXCLUDED.county, "
        "  zip             = EXCLUDED.zip, "
        "  is_closed       = EXCLUDED.is_closed, "
        "  raw_attrs       = EXCLUDED.raw_attrs, "
        "  geometry        = EXCLUDED.geometry, "
        "  geocode_source  = EXCLUDED.geocode_source, "
        "  ingested_at     = NOW()"
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def run(csv_path: str):
    if not os.path.exists(csv_path):
        print(f"[ERROR] CSV not found at {csv_path}")
        sys.exit(1)

    conn = connect()
    start = time.time()

    try:
        with open(csv_path, newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        print(f"[INFO]  Read {len(rows):,} rows from {csv_path}.")

        inserted, with_geom, closed = upsert_rows(conn, rows)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COUNT(*) AS total,
                  COUNT(*) FILTER (WHERE geometry IS NOT NULL) AS with_geom,
                  COUNT(*) FILTER (WHERE is_closed) AS closed,
                  COUNT(DISTINCT parent_company) FILTER (WHERE parent_company IS NOT NULL) AS parents
                FROM tri_facilities
            """)
            total, total_geom, total_closed, parents = cur.fetchone()

        elapsed = time.time() - start
        print()
        print("─" * 55)
        print(f"[DONE]  TRI ingestion complete in {elapsed:.1f}s")
        print(f"        Upserted this run     : {inserted:>8,}")
        print(f"        With geometry (run)   : {with_geom:>8,}")
        print(f"        Closed flagged (run)  : {closed:>8,}")
        print(f"        ── In tri_facilities ──")
        print(f"        Total rows            : {total:>8,}")
        print(f"        With geometry         : {total_geom:>8,}")
        print(f"        Flagged closed        : {total_closed:>8,}")
        print(f"        Unique parents        : {parents:>8,}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest EPA TRI facility master CSV.")
    parser.add_argument("--csv", default=DEFAULT_CSV,
                        help=f"Path to TRI facility CSV (default: {DEFAULT_CSV})")
    args = parser.parse_args()

    print()
    print("=== EPA TRI Facilities Ingestion ===")
    print()
    validate_env()
    run(args.csv)
