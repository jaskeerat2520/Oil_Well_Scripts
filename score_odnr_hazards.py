"""
Score wells against ODNR hazard overlays.

For each well already in well_risk_scores, set:
    in_aum_subsidence_zone   — well sits inside an Abandoned Underground Mine polygon
    in_aml_project           — well sits inside a state-AML or federal-AMLIS project area
    in_state_floodplain      — well sits inside Ohio's supplemental floodplain layer
    in_dogrm_urban_area      — well sits inside DOGRM's regulatory urban-area polygon
    nearest_aum_opening_id   — KNN nearest mine opening (point hazard)
    nearest_aum_opening_m    — meters to that opening

Tier 1: informational. Does NOT touch composite_risk_score / priority.

Pattern is identical to score_schools.py — UPDATE existing well_risk_scores
rows only, county-by-county, with a --rescore flag.

Usage:
    python score_odnr_hazards.py            # only wells with NULL hazard fields
    python score_odnr_hazards.py --rescore  # overwrite all
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


# One UPDATE per county. The EXISTS subqueries are independent and the GiST
# index on odnr_hazard_layers.geometry handles ST_Intersects efficiently as
# long as we filter by layer_type first (partial-index-like behavior via the
# AND clause). Nearest-opening uses the standard KNN <-> + ::geography pattern
# (see memory `project_postgis_gist_pattern.md`).
SCORE_SQL = """
UPDATE well_risk_scores AS wrs
   SET in_aum_subsidence_zone = (
           SELECT EXISTS (
               SELECT 1 FROM odnr_hazard_layers L
               WHERE L.layer_type = 'aum_mine'
                 AND ST_Intersects(L.geometry, w.geometry)
           )
       ),
       in_aml_project = (
           SELECT EXISTS (
               SELECT 1 FROM odnr_hazard_layers L
               WHERE L.layer_type IN ('aml_project', 'amlis_area')
                 AND ST_Intersects(L.geometry, w.geometry)
           )
       ),
       in_state_floodplain = (
           SELECT EXISTS (
               SELECT 1 FROM odnr_hazard_layers L
               WHERE L.layer_type = 'state_floodplain'
                 AND ST_Intersects(L.geometry, w.geometry)
           )
       ),
       in_dogrm_urban_area = (
           SELECT EXISTS (
               SELECT 1 FROM odnr_hazard_layers L
               WHERE L.layer_type = 'dogrm_urban_area'
                 AND ST_Intersects(L.geometry, w.geometry)
           )
       ),
       nearest_aum_opening_id = nearest.id,
       nearest_aum_opening_m  = nearest.distance_m,
       computed_at            = NOW()
  FROM wells w
  LEFT JOIN LATERAL (
      SELECT a.id,
             ST_Distance(w.geometry::geography, a.geometry::geography) AS distance_m
        FROM aum_openings a
       ORDER BY w.geometry <-> a.geometry
       LIMIT 1
  ) nearest ON true
 WHERE wrs.api_no = w.api_no
   AND w.county = %(county)s
   AND ( %(rescore)s::boolean OR wrs.in_aum_subsidence_zone IS NULL )
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
            connect_timeout=15, sslmode="require",
        )
        print("[OK]    Connected.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


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
                     WHERE wrs.in_aum_subsidence_zone IS NOT NULL
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

        # Final stats
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COUNT(*) FILTER (WHERE in_aum_subsidence_zone IS NOT NULL) AS scored,
                  COUNT(*) FILTER (WHERE in_aum_subsidence_zone) AS in_aum,
                  COUNT(*) FILTER (WHERE in_aml_project)         AS in_aml,
                  COUNT(*) FILTER (WHERE in_state_floodplain)    AS in_flood,
                  COUNT(*) FILTER (WHERE in_dogrm_urban_area)    AS in_urban,
                  COUNT(*) FILTER (WHERE nearest_aum_opening_m < 500)  AS near_opening_500m,
                  COUNT(*) FILTER (WHERE nearest_aum_opening_m < 1000) AS near_opening_1km
                FROM well_risk_scores
            """)
            scored, in_aum, in_aml, in_flood, in_urban, near500, near1k = cur.fetchone()

        print()
        print("─" * 55)
        print(f"[DONE]  ODNR hazard scoring complete in {elapsed:.1f}s")
        print(f"        Updated this run     : {scored_total:>10,}")
        print(f"        Total scored         : {scored:>10,}")
        print(f"        In AUM subsidence    : {in_aum:>10,}")
        print(f"        In AML/AMLIS project : {in_aml:>10,}")
        print(f"        In state floodplain  : {in_flood:>10,}")
        print(f"        In DOGRM urban area  : {in_urban:>10,}")
        print(f"        ≤500m from opening   : {near500:>10,}")
        print(f"        ≤1km from opening    : {near1k:>10,}")
        print(f"        Errors               : {errors:>10}")
        print("─" * 55)

    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score wells against ODNR hazard overlays.")
    parser.add_argument("--rescore", action="store_true",
                        help="Re-score all counties (default: skip counties already done)")
    args = parser.parse_args()

    print()
    print("=== ODNR Hazard Scoring ===")
    print()

    validate_env()
    run(rescore=args.rescore)
