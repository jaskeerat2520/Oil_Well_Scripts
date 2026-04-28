"""
Backfill within_protection_zone for counties that were scored without it.
Runs ST_Intersects check directly — no MCP timeout constraint.

Usage:
    python backfill_zones.py
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

COUNTIES = ["MEDINA", "STARK", "WASHINGTON"]


def connect():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    return conn


def run():
    conn = connect()
    print("[OK]    Connected.")

    try:
        for county in COUNTIES:
            start = time.time()
            print(f"[INFO]  {county}: checking ST_Intersects …")

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE well_risk_scores wrs
                    SET within_protection_zone = true,
                        water_risk_score = 100,
                        computed_at = NOW()
                    FROM wells w
                    JOIN water_sources ws ON ST_Intersects(w.geometry, ws.geometry)
                    WHERE wrs.api_no = w.api_no
                      AND w.county = %s
                      AND wrs.within_protection_zone = false
                      AND w.status NOT IN ('Plugged and Abandoned','Final Restoration','Storage Well','Active Injection','Well Permitted','Drilling')
                      AND w.plug_date IS NULL
                      AND (w.well_type IS NULL OR w.well_type NOT IN (
                          'Injection','Gas storage','Water supply','Solution mining',
                          'Observation','Stratigraphy test','Lost hole','Brine for dust control',
                          'Plugged injection','Plugged water supply'
                      ))
                      AND NOT (
                          w.status IN ('Cancelled','Permit Expired')
                          AND (w.completion_date IS NULL OR w.completion_date = '1900-01-02')
                          AND w.last_nonzero_production_year IS NULL
                      )
                """, (county,))
                updated = cur.rowcount

            conn.commit()
            elapsed = time.time() - start
            print(f"[OK]    {county}: {updated} wells inside protection zones ({elapsed:.1f}s)")

        # Final stats
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE within_protection_zone) AS inside
                FROM well_risk_scores
            """)
            total, inside = cur.fetchone()

        print()
        print("─" * 45)
        print(f"[DONE]  Total scored:  {total:,}")
        print(f"        Inside zone:   {inside:,}")
        print("─" * 45)

    finally:
        conn.close()
        print("[INFO]  Connection closed.")


if __name__ == "__main__":
    print()
    print("=== Backfill Protection Zone Flags ===")
    print()
    run()
