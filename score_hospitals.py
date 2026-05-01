"""
For each well in well_risk_scores, compute the nearest hospital and store
id / distance / name on the well row.

Tier 1: informational only. Does NOT touch composite_risk_score or priority.
Mirrors score_schools.py.

Usage:
    python score_hospitals.py
    python score_hospitals.py --rescore
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

SCORE_SQL = """
UPDATE well_risk_scores AS wrs
   SET nearest_hospital_id         = nearest.id,
       nearest_hospital_distance_m = nearest.distance_m,
       nearest_hospital_name       = nearest.name,
       computed_at                 = NOW()
  FROM wells w
  CROSS JOIN LATERAL (
    SELECT h.id, h.name,
           ST_Distance(w.geometry::geography, h.geometry::geography) AS distance_m
    FROM hospitals h
    WHERE h.geometry IS NOT NULL
    ORDER BY w.geometry <-> h.geometry
    LIMIT 1
  ) nearest
 WHERE wrs.api_no = w.api_no
   AND w.county = %(county)s
   AND ( %(rescore)s::boolean OR wrs.nearest_hospital_distance_m IS NULL )
"""


def connect():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


def run(rescore: bool):
    conn = connect()
    start = time.time()
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT w.county
                FROM wells w
                JOIN well_risk_scores wrs USING (api_no)
                WHERE w.county IS NOT NULL AND w.geometry IS NOT NULL
                ORDER BY w.county
            """)
            counties = [row[0] for row in cur.fetchall()]

        print(f"[INFO] Scoring nearest hospital for {len(counties)} counties (rescore={rescore}) …\n")

        for i, county in enumerate(counties, 1):
            t0 = time.time()
            with conn.cursor() as cur:
                cur.execute(SCORE_SQL, {"county": county, "rescore": rescore})
                count = cur.rowcount
            conn.commit()
            total += count
            print(f"[{i:>3}/{len(counties)}] {county:<14} {count:>5} wells  {time.time()-t0:.1f}s")

        elapsed = time.time() - start

        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COUNT(*) FILTER (WHERE nearest_hospital_distance_m IS NOT NULL) AS scored,
                  COUNT(*) FILTER (WHERE nearest_hospital_distance_m < 500)        AS within_500m,
                  COUNT(*) FILTER (WHERE nearest_hospital_distance_m < 1000)       AS within_1km,
                  ROUND(AVG(nearest_hospital_distance_m)::numeric, 0)              AS avg_m,
                  ROUND(MIN(nearest_hospital_distance_m)::numeric, 0)              AS min_m,
                  ROUND(MAX(nearest_hospital_distance_m)::numeric, 0)              AS max_m
                FROM well_risk_scores
            """)
            stats = cur.fetchone()

        print()
        print("─" * 55)
        print(f"[DONE] Scored in {elapsed:.1f}s. Updates this run: {total:,}")
        print(f"       Wells with score : {stats[0]:>8,}")
        print(f"       Within 500 m     : {stats[1]:>8,}")
        print(f"       Within 1 km      : {stats[2]:>8,}")
        print(f"       Avg distance     : {stats[3]:>8} m")
        print(f"       Min / max        : {stats[4]:>8} / {stats[5]:,} m")
        print("─" * 55)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rescore", action="store_true",
                        help="Overwrite existing nearest-hospital values.")
    args = parser.parse_args()
    run(rescore=args.rescore)
