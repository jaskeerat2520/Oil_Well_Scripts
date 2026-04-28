"""
Backfill last_nonzero_production_year and last_production_quarter into the wells
table from the original CSV, then recalculate years_inactive and inactivity_score
in well_risk_scores.

years_inactive is computed in two passes:
  1. Primary: NOW() - last_nonzero_production_year (when production data exists).
  2. Fallback: NOW() - completion_date_year (for wells where production data is
     NULL but a real completion year exists). Skips the '1900-01-02' sentinel.
     This catches the legacy 100+ year-old "Producing" wells from the early-1900s
     Ohio oil boom that predate RBDMS electronic reporting.

Composite risk_score and priority are NOT recalculated here — that's the job of
compute_composite.py, which folds inactivity_score together with the other five
risk dimensions. Run compute_composite.py after this script to refresh priority.

Usage:
    python backfill_production_years.py
"""

import csv
import os
import sys
import time
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))
CSV_FILE    = os.getenv("CSV_FILE", "Oil_And_Gas_Wells.csv")

BATCH_SIZE  = 1000


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


def parse_year(val: str):
    """Return an integer year or None."""
    v = val.strip()
    if not v or v == '0':
        return None
    try:
        y = int(v)
        return y if 1800 <= y <= 2100 else None
    except ValueError:
        return None


def backfill_from_csv(conn):
    if not os.path.isfile(CSV_FILE):
        print(f"[ERROR] CSV not found: {CSV_FILE}")
        sys.exit(1)

    print(f"[INFO]  Reading {CSV_FILE} …")
    start = time.time()
    batch = []
    updated = skipped = 0

    with open(CSV_FILE, encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            api_no  = (row.get("Permit number - API") or "").strip()
            yr_raw  = (row.get("Last_Nonzero_Production_Year") or "").strip()
            qt_raw  = (row.get("Last_Production_Quarter") or "").strip()

            if not api_no:
                skipped += 1
                continue

            yr = parse_year(yr_raw)
            qt = qt_raw if qt_raw and qt_raw != "0" else None

            # Only include rows that have at least one production field
            if yr is None and qt is None:
                skipped += 1
                continue

            batch.append((yr, qt, api_no))

            if len(batch) >= BATCH_SIZE:
                flush(conn, batch)
                updated += len(batch)
                batch = []
                print(f"[PROG]  {updated:>9,} wells updated …")

    if batch:
        flush(conn, batch)
        updated += len(batch)

    elapsed = time.time() - start
    print(f"[DONE]  CSV backfill: {updated:,} wells updated, {skipped:,} skipped in {elapsed:.1f}s")


def flush(conn, batch):
    with conn.cursor() as cur:
        execute_values(cur, """
            UPDATE wells AS w
            SET last_nonzero_production_year = v.yr,
                last_production_quarter      = v.qt
            FROM (VALUES %s) AS v(yr, qt, api_no)
            WHERE w.api_no = v.api_no
        """, batch, template="(%s::smallint, %s::varchar, %s)")
    conn.commit()


def recalculate_scores(conn):
    print("[INFO]  Recalculating years_inactive and inactivity_score …")
    with conn.cursor() as cur:
        # Step 1: Update years_inactive from the now-populated wells column
        cur.execute("""
            UPDATE well_risk_scores r
            SET years_inactive = EXTRACT(YEAR FROM NOW()) - w.last_nonzero_production_year
            FROM wells w
            WHERE r.api_no = w.api_no
              AND w.last_nonzero_production_year IS NOT NULL;
        """)
        print(f"[OK]    years_inactive updated for {cur.rowcount:,} wells.")

        # Step 1b: Fallback to completion_date for wells with no production data.
        # RBDMS electronic reporting started in the 1980s; legacy "Producing" and
        # "Well Drilled" wells from the early-1900s Ohio oil boom have NULL
        # last_nonzero_production_year but a real completion_date. For these,
        # completion year is the only timestamp available — using it gives the
        # correct ~100-year dormancy signal rather than the conservative score=50
        # "unknown" bucket. The whole year 1900 is excluded as a sentinel zone:
        # 148 wells share dates spread uniformly across Jan-Dec 1900, which is a
        # systematically-generated-placeholder pattern, not real drilling
        # (compare to ~6 wells/decade in adjacent years 1901-1919). Only fills
        # NULL years_inactive — won't overwrite Step 1's values.
        cur.execute("""
            UPDATE well_risk_scores r
            SET years_inactive = EXTRACT(YEAR FROM NOW())::int
                               - EXTRACT(YEAR FROM w.completion_date)::int
            FROM wells w
            WHERE r.api_no = w.api_no
              AND r.years_inactive IS NULL
              AND w.last_nonzero_production_year IS NULL
              AND w.completion_date IS NOT NULL
              AND EXTRACT(YEAR FROM w.completion_date) > 1900;
        """)
        print(f"[OK]    years_inactive completion_date fallback: {cur.rowcount:,} wells.")

        # Step 2: Recalculate inactivity_score from years_inactive.
        # Producing wells with recent production get 0 — the 1-2 year carveout
        # absorbs the RBDMS reporting lag (last_nonzero_production_year is
        # annual-resolution and RBDMS reports trail calendar time). The carveout
        # is gated on production recency, not on Producing status alone. Zombie
        # producers (Producing status with stale or null last_nonzero_production_year)
        # fall through to the years_inactive buckets so they surface at their
        # true dormancy. Distinct from the 5-year priority cap in
        # compute_composite.py — that gate is for regulated-operator enforcement;
        # this gate is for reporting-lag noise.
        cur.execute("""
            UPDATE well_risk_scores r
            SET inactivity_score = CASE
                WHEN w.status = 'Producing'
                     AND w.last_nonzero_production_year >= EXTRACT(YEAR FROM NOW())::int - 2
                    THEN 0
                WHEN r.years_inactive >= 50 THEN 100
                WHEN r.years_inactive >= 25 THEN 80
                WHEN r.years_inactive >= 15 THEN 60
                WHEN r.years_inactive >= 10 THEN 40
                WHEN r.years_inactive >= 5  THEN 20
                WHEN r.years_inactive IS NOT NULL THEN 5
                ELSE 50  -- unknown: conservative middle estimate
            END
            FROM wells w
            WHERE r.api_no = w.api_no;
        """)
        print(f"[OK]    inactivity_score recalculated for {cur.rowcount:,} wells.")

    conn.commit()
    print("[INFO]  Run compute_composite.py to refresh composite risk_score + priority.")


def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT priority, COUNT(*) AS count
            FROM well_risk_scores
            GROUP BY priority
            ORDER BY CASE priority
              WHEN 'critical' THEN 1 WHEN 'high' THEN 2
              WHEN 'medium'   THEN 3 WHEN 'low'  THEN 4 END;
        """)
        rows = cur.fetchall()

    print()
    print("─" * 35)
    print("  Updated priority distribution:")
    for priority, count in rows:
        print(f"  {priority:<10} {count:>8,}")
    print("─" * 35)


if __name__ == "__main__":
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    print()
    print("=== Backfill Production Years ===")
    print()

    conn = connect()
    try:
        backfill_from_csv(conn)
        recalculate_scores(conn)
        print_summary(conn)
    finally:
        conn.close()
        print("[INFO]  Connection closed.")
