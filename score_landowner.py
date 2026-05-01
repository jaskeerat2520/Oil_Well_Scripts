"""
Resolve the surface landowner for each scored well by intersecting wells
against the parcels table.

This is METADATA-only — no composite-score impact. compute_composite.py
is unchanged. The columns written here exist purely to surface "who owns
the land this well sits on?" in the well-viewer per-well detail card.

Note on surface vs mineral rights: parcel data carries the SURFACE owner
of the property. The mineral leaseholder is already on wells.operator
and is usually a different party — that distinction matters in Ohio,
especially in Utica shale counties.

Prerequisites:
    - wells table populated
    - well_risk_scores table populated
    - parcels table populated for the target county (run ingest_parcels.py)

Usage:
    python score_landowner.py                      # all counties with parcels loaded
    python score_landowner.py --county HOCKING     # one county (Cloud Run worker mode)
    python score_landowner.py --force              # re-resolve all counties
"""

import argparse
import os
import sys
import time

import psycopg2
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


# Pick the single best parcel for each well. ST_Intersects covers both
# "point inside polygon" and "point on boundary"; the LATERAL ORDER BY
# ST_Distance(centroid) breaks ties deterministically when a well falls
# exactly on a shared edge between two parcels (rare but real).
SCORE_SQL = """
UPDATE well_risk_scores wrs
SET surface_parcel_id           = p.parcel_id,
    surface_owner_name          = p.owner_name,
    surface_owner_mailing_state = p.owner_mailing_state,
    surface_parcel_acreage      = p.acreage,
    landowner_resolved_at       = NOW()
FROM wells w
LEFT JOIN LATERAL (
    SELECT parcel_id, owner_name, owner_mailing_state, acreage
    FROM parcels
    WHERE county = %s
      AND ST_Intersects(geom, w.geometry)
    ORDER BY ST_Distance(ST_Centroid(geom), w.geometry)
    LIMIT 1
) p ON TRUE
WHERE wrs.api_no = w.api_no
  AND w.county   = %s;
"""


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


def resolve_county_name(conn, county_input):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT w.county
            FROM wells w
            JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
            WHERE UPPER(REPLACE(w.county, ' ', '')) = UPPER(REPLACE(%s, ' ', ''))
            LIMIT 1
        """, (county_input,))
        row = cur.fetchone()
        return row[0] if row else None


def parcels_loaded_for(conn, county) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM parcels WHERE county = %s", (county,))
        return cur.fetchone()[0]


def already_resolved(conn, county) -> bool:
    """True when every wrs row for this county has landowner_resolved_at set."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE wrs.landowner_resolved_at IS NULL) AS unresolved,
                COUNT(*) AS total
            FROM well_risk_scores wrs
            JOIN wells w ON w.api_no = wrs.api_no
            WHERE w.county = %s
        """, (county,))
        unresolved, total = cur.fetchone()
        return total > 0 and unresolved == 0


def score_county(conn, county):
    with conn.cursor() as cur:
        cur.execute(SCORE_SQL, (county, county))
        count = cur.rowcount
    conn.commit()
    return count


def report_match_quality(conn, county):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(surface_parcel_id) AS matched,
                COUNT(surface_owner_name) AS with_owner,
                COUNT(*) FILTER (WHERE surface_owner_mailing_state IS NOT NULL
                                 AND surface_owner_mailing_state <> 'OH') AS out_of_state
            FROM well_risk_scores wrs
            JOIN wells w ON w.api_no = wrs.api_no
            WHERE w.county = %s
        """, (county,))
        total, matched, with_owner, out_of_state = cur.fetchone()
    pct = (matched / total * 100) if total else 0
    print(f"        match coverage : {matched:>5}/{total:<5} ({pct:.1f}%)")
    print(f"        with owner name: {with_owner:>5}")
    print(f"        out-of-state   : {out_of_state:>5}")


def run_one(county_input, force=False):
    conn = connect()
    start = time.time()
    try:
        county = resolve_county_name(conn, county_input)
        if not county:
            print(f"[ERROR] No wells found for county '{county_input}'")
            sys.exit(1)

        n_parcels = parcels_loaded_for(conn, county)
        if n_parcels == 0:
            print(f"[ERROR] No parcels loaded for {county}. Run ingest_parcels.py "
                  f"--county {county} first.")
            sys.exit(1)

        if not force and already_resolved(conn, county):
            print(f"[SKIP]  {county} already resolved. Pass --force to re-resolve.")
            return

        count = score_county(conn, county)
        elapsed = time.time() - start
        print(f"[OK]    {county}: {count:,} wells touched in {elapsed:.1f}s "
              f"({n_parcels:,} parcels in scope)")
        report_match_quality(conn, county)

    finally:
        conn.close()
        print("[INFO]  Connection closed.")


def run_all(force=False):
    conn = connect()
    start = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT county FROM parcels ORDER BY county")
            counties = [row[0] for row in cur.fetchall()]
        print(f"[INFO]  {len(counties)} counties have parcels loaded.")

        scored_total = 0
        for i, county in enumerate(counties, 1):
            if not force and already_resolved(conn, county):
                print(f"[{i:>3}/{len(counties)}]  {county:<15} (already resolved — skipping)")
                continue
            try:
                count = score_county(conn, county)
                scored_total += count
                print(f"[{i:>3}/{len(counties)}]  {county:<15} {count:>5} wells")
            except psycopg2.Error as e:
                conn.rollback()
                print(f"[ERROR] {county}: {e}")

        elapsed = time.time() - start
        print()
        print("─" * 55)
        print(f"[DONE]  Landowner resolution complete in {elapsed:.1f}s")
        print(f"        Wells touched: {scored_total:>10,}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Connection closed.")


if __name__ == "__main__":
    print()
    print("=== Surface Landowner Resolution ===")
    print()

    parser = argparse.ArgumentParser()
    parser.add_argument("--county", help="Resolve a single county (Cloud Run worker mode).")
    parser.add_argument("--force", action="store_true",
                        help="Re-resolve even if already done.")
    args = parser.parse_args()

    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    if args.county:
        run_one(args.county, args.force)
    else:
        run_all(args.force)
