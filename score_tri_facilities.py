"""
For each well in well_risk_scores, compute the nearest active TRI facility
and store id / distance / name / parent on the well row.

Tier 1: informational only. Does NOT touch composite_risk_score or priority.

"Active" excludes facilities flagged closed (fac_closed_ind=1) and any rows
without geometry. The active subset is materialized as a CTE so PostGIS GiST
KNN works against the filtered set.

Usage:
    python score_tri_facilities.py            # only counties not yet scored
    python score_tri_facilities.py --rescore  # overwrite all
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


# Mirrors score_schools.py / score_hospitals.py — county-by-county UPDATE,
# CTE filters to active + has-geom subset, GiST KNN via <-> with geography
# distance for the meters value.
SCORE_SQL = """
WITH active_tri AS (
    SELECT id, facility_name, parent_company, geometry
      FROM tri_facilities
     WHERE is_closed = FALSE
       AND geometry IS NOT NULL
)
UPDATE well_risk_scores AS wrs
   SET nearest_tri_facility_id    = nearest.id,
       nearest_tri_distance_m     = nearest.distance_m,
       nearest_tri_facility_name  = nearest.facility_name,
       nearest_tri_parent_company = nearest.parent_company,
       computed_at                = NOW()
  FROM wells w
  CROSS JOIN LATERAL (
    SELECT t.id, t.facility_name, t.parent_company,
           ST_Distance(w.geometry::geography, t.geometry::geography) AS distance_m
      FROM active_tri t
     ORDER BY w.geometry <-> t.geometry
     LIMIT 1
  ) nearest
 WHERE wrs.api_no = w.api_no
   AND w.county = %(county)s
   AND ( %(rescore)s::boolean OR wrs.nearest_tri_distance_m IS NULL )
"""


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


def run(rescore: bool):
    conn = connect()
    start = time.time()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT w.county
                  FROM wells w
                  JOIN well_risk_scores wrs ON wrs.api_no = w.api_no
                 WHERE w.county IS NOT NULL AND w.geometry IS NOT NULL
                 ORDER BY w.county
            """)
            counties = [row[0] for row in cur.fetchall()]

        if rescore:
            remaining = counties
            print(f"[INFO]  --rescore: re-scoring all {len(counties)} counties.")
        else:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT w.county
                      FROM wells w
                      JOIN well_risk_scores wrs ON wrs.api_no = w.api_no
                     WHERE wrs.nearest_tri_distance_m IS NOT NULL
                       AND w.county IS NOT NULL
                """)
                already = {row[0] for row in cur.fetchall()}
            remaining = [c for c in counties if c not in already]
            print(f"[INFO]  {len(counties)} counties total, {len(already)} already scored, {len(remaining)} remaining.")

        scored_total = 0
        errors = 0

        for i, county in enumerate(remaining, 1):
            t0 = time.time()
            try:
                with conn.cursor() as cur:
                    cur.execute(SCORE_SQL, {"county": county, "rescore": rescore})
                    n = cur.rowcount
                conn.commit()
                scored_total += n
                print(f"[{i:>3}/{len(remaining)}]  {county:<15} {n:>5} wells  {time.time()-t0:.1f}s")
            except psycopg2.Error as e:
                conn.rollback()
                errors += 1
                print(f"[ERROR] {county}: {e}")

        elapsed = time.time() - start

        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COUNT(*) FILTER (WHERE nearest_tri_distance_m IS NOT NULL) AS scored,
                  COUNT(*) FILTER (WHERE nearest_tri_distance_m < 500)  AS within_500m,
                  COUNT(*) FILTER (WHERE nearest_tri_distance_m < 1000) AS within_1km,
                  COUNT(*) FILTER (WHERE nearest_tri_distance_m < 5000) AS within_5km,
                  ROUND(AVG(nearest_tri_distance_m)::numeric, 0)        AS avg_dist
                FROM well_risk_scores
            """)
            scored, w500, w1k, w5k, avg = cur.fetchone()

        print()
        print("─" * 55)
        print(f"[DONE]  TRI proximity scoring complete in {elapsed:.1f}s")
        print(f"        Updated this run    : {scored_total:>10,}")
        print(f"        Total scored        : {scored:>10,}")
        print(f"        ≤ 500m of TRI       : {w500:>10,}")
        print(f"        ≤ 1km  of TRI       : {w1k:>10,}")
        print(f"        ≤ 5km  of TRI       : {w5k:>10,}")
        print(f"        Avg distance        : {avg:>10,} m")
        print(f"        Errors              : {errors:>10}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score wells against nearest active TRI facility.")
    parser.add_argument("--rescore", action="store_true",
                        help="Re-score all counties (default: skip counties already done)")
    args = parser.parse_args()

    print()
    print("=== TRI Facility Proximity Scoring ===")
    print()
    validate_env()
    run(rescore=args.rescore)
