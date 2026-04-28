"""
Score wells by population exposure using Census tract data.

For each well, uses areal interpolation to estimate population within 1km and 5km:
each tract's population is weighted by the fraction of its area that overlaps the
buffer radius, preventing double-counting when two wells share the same tract.
Runs county-by-county to avoid timeouts.

The composite score and priority are NOT recomputed here — that's the job of
compute_composite.py, which folds all five risk dimensions together. Run
compute_composite.py after this script to refresh priority.

Prerequisites:
    - wells table populated
    - well_risk_scores table populated (run score_wells.py first)
    - population_tracts table populated (run ingest_population.py first)

Usage:
    python score_population.py                  # all unscored counties
    python score_population.py --county STARK   # one county only (Cloud Run worker mode)
    python score_population.py --force          # re-score all counties
"""

import os
import sys
import time
import argparse
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
    WHERE ST_DWithin(w.geometry, pt.geometry, 0.06)
      AND ST_DWithin(w.geometry::geography, pt.geometry::geography, 5000)
) pop
WHERE wrs.api_no = w.api_no
  AND w.county = %s
  AND w.status NOT IN ('Plugged and Abandoned','Final Restoration','Storage Well','Active Injection','Well Permitted','Drilling');
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
    """Map a user-supplied county string to the canonical case-form stored in
    `wells.county`. Returns None if no wells exist for the input.

    Space-normalised so `enqueue_counties.sh` (which uses a bash array and can't
    easily contain spaces) can send 'VANWERT' and still match the DB's
    'VAN WERT'. Same handling for any future multi-word county."""
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


def already_scored(conn, county):
    """Resume guard — true if any well in this county already has a nonzero
    population_risk_score."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM well_risk_scores wrs
            JOIN wells w ON w.api_no = wrs.api_no
            WHERE w.county = %s
              AND wrs.population_risk_score > 0
            LIMIT 1
        """, (county,))
        return cur.fetchone() is not None


def score_county(conn, county):
    with conn.cursor() as cur:
        cur.execute(SCORE_SQL, (county,))
        count = cur.rowcount
    conn.commit()
    return count


def run_one(county_input, force=False):
    """Cloud Run worker entry — score a single county and exit."""
    conn = connect()
    start = time.time()
    try:
        county = resolve_county_name(conn, county_input)
        if not county:
            print(f"[ERROR] No wells found for county '{county_input}'")
            sys.exit(1)

        if not force and already_scored(conn, county):
            print(f"[SKIP]  {county} already scored. Pass --force to re-score.")
            return

        count = score_county(conn, county)
        elapsed = time.time() - start
        print(f"[OK]    {county}: {count:,} wells scored in {elapsed:.1f}s")
    finally:
        conn.close()
        print("[INFO]  Connection closed.")


def run_all(force=False):
    """Local batch entry — score every county (or every unscored one)."""
    conn = connect()
    start = time.time()

    try:
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
                count = score_county(conn, county)
                elapsed = time.time() - county_start
                scored_total += count
                print(f"[{i:>3}/{len(remaining)}]  {county:<15} {count:>5} wells  {elapsed:.1f}s")
            except psycopg2.Error as e:
                conn.rollback()
                errors += 1
                print(f"[ERROR] {county}: {e}")

        elapsed = time.time() - start

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

    parser = argparse.ArgumentParser()
    parser.add_argument("--county", help="Score only this county (Cloud Run worker mode)")
    parser.add_argument("--force", action="store_true",
                        help="Re-score even if the county already has population scores")
    args = parser.parse_args()

    if args.county:
        run_one(args.county, force=args.force)
    else:
        run_all(force=args.force)
