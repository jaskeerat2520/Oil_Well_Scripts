"""
Import county boundary polygons from the US Census Bureau into the `counties` table.

Defaults to Ohio for backwards compatibility, but accepts --state to load county
geometry for any US state. Use --state PA or --state WV to add Pennsylvania or
West Virginia counties for the multi-state expansion. Upserts on fips_code so
re-running for the same state refreshes geometry without creating duplicates.
"""

import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))


def check_dependencies():
    missing = []
    for pkg in ("geopandas", "shapely", "pygris"):
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"[ERROR] Missing packages: {', '.join(missing)}")
        print(f"        Run: pip install {' '.join(missing)}")
        sys.exit(1)
    print("[OK]    Dependencies available (geopandas, shapely, pygris).")


def connect():
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
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
        print(f"[ERROR] Could not connect: {e}")
        sys.exit(1)


def fetch_county_boundaries(state_code: str):
    import pygris
    print(f"[INFO]  Fetching {state_code} county boundaries from Census Bureau via pygris …")
    gdf = pygris.counties(state=state_code, year=2023)
    print(f"[OK]    Fetched {len(gdf)} county features.")
    print(f"[INFO]  CRS: {gdf.crs}")

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print("[INFO]  Reprojecting to WGS84 (EPSG:4326) …")
        gdf = gdf.to_crs(epsg=4326)
        print("[OK]    Reprojection done.")

    return gdf


def upsert_geometry(conn, gdf, state_code: str):
    inserted = 0
    updated  = 0
    errors   = 0

    print(f"[INFO]  Upserting {state_code} county geometries …")
    with conn.cursor() as cur:
        for _, row in gdf.iterrows():
            geoid       = str(row.get("GEOID", "")).strip()
            county_name = row.get("NAME", "").upper().strip()
            geom_wkt    = row.geometry.wkt

            if not geoid or not county_name:
                print(f"[WARN]  Missing GEOID or NAME for row — skipping.")
                errors += 1
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO counties (fips_code, name, geometry, state_code)
                    VALUES (%s, %s, ST_Multi(ST_GeomFromText(%s, 4326)), %s)
                    ON CONFLICT (fips_code) DO UPDATE SET
                        name       = EXCLUDED.name,
                        geometry   = EXCLUDED.geometry,
                        state_code = EXCLUDED.state_code
                    RETURNING xmax = 0 AS was_insert
                    """,
                    (geoid, county_name, geom_wkt, state_code),
                )
                result = cur.fetchone()
                if result and result[0]:
                    inserted += 1
                    print(f"[NEW]   {state_code} {geoid} {county_name}")
                else:
                    updated += 1
                    print(f"[UPD]   {state_code} {geoid} {county_name}")
            except psycopg2.Error as e:
                conn.rollback()
                print(f"[ERROR] Failed for {geoid} {county_name}: {e}")
                errors += 1
                continue

        conn.commit()

    print()
    print("-" * 50)
    print(f"[DONE]  {state_code} county geometry import complete.")
    print(f"        Inserted : {inserted:>5}")
    print(f"        Updated  : {updated:>5}")
    print(f"        Errors   : {errors:>5}")
    print("-" * 50)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state",
        default="OH",
        help="Two-letter state code (e.g. OH, PA, WV). Default: OH.",
    )
    args = parser.parse_args()
    state_code = args.state.upper().strip()
    if len(state_code) != 2:
        print(f"[ERROR] --state must be a 2-letter code, got '{args.state}'.")
        sys.exit(1)

    print()
    print(f"=== County Geometry Importer ({state_code}) ===")
    print()

    check_dependencies()

    conn = connect()
    try:
        gdf = fetch_county_boundaries(state_code)
        upsert_geometry(conn, gdf, state_code)
    except KeyboardInterrupt:
        print()
        print("[ABORT] Interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Database connection closed.")
