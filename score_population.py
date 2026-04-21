"""
Score wells by population exposure using Census tract data.

For each well, uses areal interpolation to estimate population within 1km and 5km:
each tract's population is weighted by the fraction of its area that overlaps the
buffer radius, preventing double-counting when two wells share the same tract.
Also recalculates composite risk_score and priority after population scoring.
Runs county-by-county to avoid timeouts.

Prerequisites:
    - wells table populated
    - well_risk_scores table populated (run score_wells.py first)
    - population_tracts table populated (run ingest_population.py first)

Usage:
    python score_population.py           # skip already-scored counties
    python score_population.py --force   # re-score all counties
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

SCORE_SQL = """
UPDATE well_risk_scores wrs
SET population_within_1km = pop.pop_1km,
    population_within_5km = pop.pop_5km,
    population_risk_score = CASE
        WHEN pop.pop_1km > 5000 THEN 100
        WHEN pop.pop_1km > 1000 THEN 80
        WHEN pop.pop_1km > 500  THEN 60
        WHEN pop.pop_5km > 5000 THEN 40
        WHEN pop.pop_5km > 1000 THEN 20
        ELSE 5
    END,
    computed_at = NOW()
FROM wells w
CROSS JOIN LATERAL (
    SELECT
        COALESCE(SUM(
            pt.total_population *
            ST_Area(ST_Intersection(
                ST_Buffer(w.geometry::geography, 1000)::geometry,
                pt.geometry
            )::geography) /
            NULLIF(ST_Area(pt.geometry::geography), 0)
        ), 0)::integer AS pop_1km,
        COALESCE(SUM(
            pt.total_population *
            ST_Area(ST_Intersection(
                ST_Buffer(w.geometry::geography, 5000)::geometry,
                pt.geometry
            )::geography) /
            NULLIF(ST_Area(pt.geometry::geography), 0)
        ), 0)::integer AS pop_5km
    FROM population_tracts pt
    WHERE ST_DWithin(w.geometry::geography, pt.geometry::geography, 5000)
) pop
WHERE wrs.api_no = w.api_no
  AND w.county = %s
  AND w.status NOT IN ('Plugged and Abandoned','Final Restoration','Storage Well','Active Injection','Well Permitted','Drilling');
"""

RISK_SQL = """
UPDATE well_risk_scores r
SET
  risk_score = ROUND(
    (0.25 * water_risk_score
     + 0.35 * COALESCE(population_risk_score, 0)
     + 0.40 * COALESCE(inactivity_score, 0))::numeric
  , 1),
  priority = CASE
    WHEN w.status = 'Producing' AND (w.operator IS NULL OR w.operator != 'HISTORIC OWNER') THEN
      CASE
        WHEN (0.25 * water_risk_score + 0.35 * COALESCE(population_risk_score, 0) + 0.40 * COALESCE(inactivity_score, 0)) >= 35
        THEN 'medium' ELSE 'low'
      END
    WHEN (0.25 * water_risk_score + 0.35 * COALESCE(population_risk_score, 0) + 0.40 * COALESCE(inactivity_score, 0)) >= 75 THEN 'critical'
    WHEN (0.25 * water_risk_score + 0.35 * COALESCE(population_risk_score, 0) + 0.40 * COALESCE(inactivity_score, 0)) >= 55 THEN 'high'
    WHEN (0.25 * water_risk_score + 0.35 * COALESCE(population_risk_score, 0) + 0.40 * COALESCE(inactivity_score, 0)) >= 35 THEN 'medium'
    ELSE 'low'
  END
FROM wells w
WHERE r.api_no = w.api_no
  AND w.county = %s;
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


def run(force=False):
    conn = connect()
    start = time.time()

    try:
        # Get all counties
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT w.county
                FROM wells w
                JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
                WHERE w.county IS NOT NULL
                ORDER BY w.county
            """)
            counties = [row[0] for row in cur.fetchall()]

        if force:
            remaining = counties
            print(f"[INFO]  --force: re-scoring all {len(counties)} counties.")
        else:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT w.county
                    FROM well_risk_scores wrs
                    JOIN wells w ON w.api_no = wrs.api_no
                    WHERE wrs.population_risk_score > 0
                      AND w.county IS NOT NULL
                """)
                already_done = {row[0] for row in cur.fetchall()}
            remaining = [c for c in counties if c not in already_done]
            print(f"[INFO]  {len(counties)} counties total, {len(already_done)} already scored, {len(remaining)} remaining.")

        scored_total = 0
        errors = 0

        for i, county in enumerate(remaining, 1):
            county_start = time.time()
            try:
                with conn.cursor() as cur:
                    cur.execute(SCORE_SQL, (county,))
                    count = cur.rowcount
                with conn.cursor() as cur:
                    cur.execute(RISK_SQL, (county,))
                conn.commit()

                elapsed = time.time() - county_start
                scored_total += count
                print(f"[{i:>3}/{len(remaining)}]  {county:<15} {count:>5} wells  {elapsed:.1f}s")

            except psycopg2.Error as e:
                conn.rollback()
                errors += 1
                print(f"[ERROR] {county}: {e}")

        elapsed = time.time() - start

        # Final stats
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    round(AVG(population_within_1km)::numeric, 0) AS avg_pop_1km,
                    round(AVG(population_within_5km)::numeric, 0) AS avg_pop_5km,
                    COUNT(*) FILTER (WHERE population_risk_score >= 80) AS high_pop_risk
                FROM well_risk_scores
            """)
            stats = cur.fetchone()

        print()
        print("─" * 55)
        print(f"[DONE]  Population scoring complete in {elapsed:.1f}s")
        print(f"        Scored this run  : {scored_total:>10,}")
        print(f"        Avg pop within 1km: {stats[1]:>10,}")
        print(f"        Avg pop within 5km: {stats[2]:>10,}")
        print(f"        High pop risk     : {stats[3]:>10,}")
        print(f"        Errors            : {errors:>10}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Connection closed.")


if __name__ == "__main__":
    print()
    print("=== Well Population Risk Scoring ===")
    print()

    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    force = "--force" in sys.argv
    run(force=force)
