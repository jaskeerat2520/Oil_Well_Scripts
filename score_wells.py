"""
Score wells against water source protection areas.

For each well, finds the nearest water source (by centroid distance)
and checks if the well is inside any protection zone polygon.
Runs county-by-county to avoid overloading the database.

Prerequisites:
    - wells table populated
    - water_sources table populated (run ingest_water_sources.py first)
    - water_source_centroids table exists

Usage:
    python score_wells.py
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

SCORE_SQL = """
INSERT INTO well_risk_scores (api_no, nearest_water_source_id, nearest_water_distance_m,
    nearest_water_type, within_protection_zone, water_risk_score,
    has_operator, operator_status)
SELECT
    w.api_no,
    nearest.ws_id,
    nearest.distance_m,
    nearest.source_type,
    zone.intersects,
    LEAST(100,
        CASE
            WHEN nearest.distance_m < 500   THEN 90
            WHEN nearest.distance_m < 1000  THEN 70
            WHEN nearest.distance_m < 3000  THEN 50
            WHEN nearest.distance_m < 5000  THEN 30
            WHEN nearest.distance_m < 10000 THEN 15
            ELSE 5
        END
        + CASE WHEN zone.intersects THEN 20 ELSE 0 END
    ),
    (w.operator IS NOT NULL AND w.operator != 'HISTORIC OWNER'),
    CASE
        WHEN w.in_orphan_program = true    THEN 'orphan_program'
        WHEN w.operator = 'HISTORIC OWNER' THEN 'historic_owner'
        WHEN w.operator IS NOT NULL        THEN 'named_operator'
        ELSE 'unknown'
    END
FROM wells w
CROSS JOIN LATERAL (
    SELECT wsc.id AS ws_id, wsc.source_type,
           ST_Distance(w.geometry::geography, wsc.centroid::geography) AS distance_m
    FROM water_source_centroids wsc
    ORDER BY w.geometry <-> wsc.centroid
    LIMIT 1
) nearest
CROSS JOIN LATERAL (
    SELECT EXISTS (
        SELECT 1 FROM water_sources ws
        WHERE ST_Intersects(w.geometry, ws.geometry)
    ) AS intersects
) zone
WHERE w.geometry IS NOT NULL
  AND w.county = %s
  AND w.status NOT IN ('Plugged and Abandoned','Final Restoration','Storage Well','Active Injection','Well Permitted','Drilling')
ON CONFLICT (api_no) DO UPDATE SET
    nearest_water_source_id = EXCLUDED.nearest_water_source_id,
    nearest_water_distance_m = EXCLUDED.nearest_water_distance_m,
    nearest_water_type = EXCLUDED.nearest_water_type,
    within_protection_zone = EXCLUDED.within_protection_zone,
    water_risk_score = EXCLUDED.water_risk_score,
    has_operator = EXCLUDED.has_operator,
    operator_status = EXCLUDED.operator_status,
    computed_at = NOW();
"""


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


def run():
    conn = connect()
    start = time.time()

    try:
        # Get all counties
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT county FROM wells
                WHERE county IS NOT NULL AND geometry IS NOT NULL
                ORDER BY county
            """)
            counties = [row[0] for row in cur.fetchall()]

        # Check which counties are already scored
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT w.county
                FROM well_risk_scores wrs
                JOIN wells w ON w.api_no = wrs.api_no
                WHERE w.county IS NOT NULL
            """)
            already_scored = {row[0] for row in cur.fetchall()}

        remaining = [c for c in counties if c not in already_scored]
        print(f"[INFO]  {len(counties)} counties total, {len(already_scored)} already scored, {len(remaining)} remaining.")

        scored_total = 0
        errors = 0

        for i, county in enumerate(remaining, 1):
            county_start = time.time()
            try:
                with conn.cursor() as cur:
                    cur.execute(SCORE_SQL, (county,))
                    count = cur.rowcount
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
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE within_protection_zone) AS inside,
                       round(AVG(nearest_water_distance_m)::numeric, 0) AS avg_dist
                FROM well_risk_scores
            """)
            stats = cur.fetchone()

        print()
        print("─" * 55)
        print(f"[DONE]  Scoring complete in {elapsed:.1f}s")
        print(f"        Scored this run : {scored_total:>10,}")
        print(f"        Total scored    : {stats[0]:>10,}")
        print(f"        Inside zone     : {stats[1]:>10,}")
        print(f"        Avg distance    : {stats[2]:>10,} m")
        print(f"        Errors          : {errors:>10}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    print()
    print("=== Well Water Risk Scoring ===")
    print()

    validate_env()
    run()
