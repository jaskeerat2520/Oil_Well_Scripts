import csv
import os
import sys
import time
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))
CSV_FILE    = os.getenv("CSV_FILE", "Oil_And_Gas_Wells.csv")
BATCH_SIZE  = 500

# ── Helpers ─────────────────────────────────────────────────────────────────
def validate_env():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing required environment variables: {', '.join(missing)}")
        print("        Copy env.example to .env and fill in your credentials.")
        sys.exit(1)
    print("[OK]    Environment variables loaded.")


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER} …")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER,
            password=DB_PASSWORD, port=DB_PORT,
            connect_timeout=15,
            sslmode="require",
        )
        print("[OK]    Database connection established.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"[ERROR] Could not connect to database: {e}")
        sys.exit(1)


def create_table(cur):
    print("[INFO]  Ensuring table 'oil_gas_wells' exists …")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS oil_gas_wells (
            obj_id                       TEXT,
            permit_number_api            TEXT,
            well_type                    TEXT,
            well_status                  TEXT,
            well_gis_status              TEXT,
            county                       TEXT,
            township                     TEXT,
            company                      TEXT,
            well_op_address              TEXT,
            well_company_phone           TEXT,
            elevation                    TEXT,
            du_acres                     TEXT,
            well_wh_lat                  TEXT,
            well_wh_long                 TEXT,
            well_latitude                TEXT,
            well_longitude               TEXT,
            well_bh_lat                  TEXT,
            well_bh_long                 TEXT,
            bottom_hole_latitude         TEXT,
            bottom_hole_longitude        TEXT,
            slant                        TEXT,
            well_proposed_formations     TEXT,
            well_name                    TEXT,
            well_number                  TEXT,
            well_ip_gas                  TEXT,
            well_ip_oil                  TEXT,
            well_total_depth             TEXT,
            well_date_approved           TEXT,
            well_date_complete           TEXT,
            well_date_plugged            TEXT,
            producing_formation          TEXT,
            second_prod_formation        TEXT,
            deepest_formation            TEXT,
            well_pdf_link                TEXT,
            geophys_logs                 TEXT,
            utica_shale                  TEXT,
            marcellus_shale              TEXT,
            map_symble                   TEXT,
            well_well_status             TEXT,
            well_operator                TEXT,
            well_lease_name              TEXT,
            well_location_id             TEXT,
            well_typ                     TEXT,
            well_company_addr1           TEXT,
            well_company_addr2           TEXT,
            well_company_city            TEXT,
            well_company_zip1            TEXT,
            toe_longitude                TEXT,
            toe_latitude                 TEXT,
            heel_longitude               TEXT,
            heel_latitude                TEXT,
            owps_description             TEXT,
            last_nonzero_production_year TEXT,
            last_production_quarter      TEXT,
            orphan_well_program_status   TEXT,
            x                            TEXT,
            y                            TEXT
        );
    """)
    print("[OK]    Table ready.")


def row_to_tuple(row):
    """Pad or trim row to exactly 57 values."""
    padded = (row + [""] * 57)[:57]
    return tuple(v if v.strip() != "" else None for v in padded)


def import_csv(conn, csv_path):
    if not os.path.isfile(csv_path):
        print(f"[ERROR] CSV file not found: {csv_path}")
        sys.exit(1)

    print(f"[INFO]  Counting rows in {csv_path} …")
    with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
        total_rows = sum(1 for _ in f) - 1  # subtract header
    print(f"[INFO]  {total_rows:,} data rows to import.")

    insert_sql = """
        INSERT INTO oil_gas_wells VALUES %s
        ON CONFLICT DO NOTHING
    """

    inserted = 0
    skipped  = 0
    errors   = 0
    batch    = []
    start    = time.time()

    print(f"[INFO]  Starting import (batch size: {BATCH_SIZE:,}) …")

    with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        with conn.cursor() as cur:
            for line_num, row in enumerate(reader, start=2):  # line 2 = first data row
                try:
                    batch.append(row_to_tuple(row))
                except Exception as e:
                    print(f"[WARN]  Line {line_num}: skipping malformed row — {e}")
                    skipped += 1
                    continue

                if len(batch) >= BATCH_SIZE:
                    try:
                        execute_values(cur, insert_sql, batch)
                        conn.commit()
                        inserted += len(batch)
                        batch = []

                        # Progress update every 10 batches
                        if (inserted // BATCH_SIZE) % 10 == 0:
                            pct     = inserted / total_rows * 100
                            elapsed = time.time() - start
                            rate    = inserted / elapsed if elapsed > 0 else 0
                            eta     = (total_rows - inserted) / rate if rate > 0 else 0
                            print(
                                f"[PROG]  {inserted:>9,} / {total_rows:,} rows "
                                f"({pct:5.1f}%)  "
                                f"{rate:,.0f} rows/s  "
                                f"ETA {eta:.0f}s"
                            )
                    except psycopg2.Error as e:
                        conn.rollback()
                        errors += len(batch)
                        print(f"[ERROR] Batch insert failed near line {line_num}: {e}")
                        batch = []

            # Flush remaining rows
            if batch:
                try:
                    execute_values(cur, insert_sql, batch)
                    conn.commit()
                    inserted += len(batch)
                    print(f"[INFO]  Flushed final batch of {len(batch):,} rows.")
                    batch = []
                except psycopg2.Error as e:
                    conn.rollback()
                    errors += len(batch)
                    print(f"[ERROR] Final batch insert failed: {e}")

    elapsed = time.time() - start
    rate    = inserted / elapsed if elapsed > 0 else 0

    print()
    print("─" * 55)
    print(f"[DONE]  Import complete in {elapsed:.1f}s  ({rate:,.0f} rows/s)")
    print(f"        Inserted : {inserted:>10,}")
    print(f"        Skipped  : {skipped:>10,}")
    print(f"        Errors   : {errors:>10,}")
    print("─" * 55)

    if errors > 0:
        print("[WARN]  Some rows failed to insert. Review errors above.")
    if inserted == 0:
        print("[WARN]  No rows were inserted. Check your CSV path and table schema.")


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print()
    print("=== Oil & Gas Wells CSV Importer ===")
    print()

    validate_env()

    conn = connect()
    try:
        with conn.cursor() as cur:
            create_table(cur)
        conn.commit()

        import_csv(conn, CSV_FILE)
    except KeyboardInterrupt:
        print()
        print("[ABORT] Import interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Database connection closed.")
