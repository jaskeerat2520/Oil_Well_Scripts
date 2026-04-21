"""
Ingest 2020 Census tract population and geometry into population_tracts table.

Data sources:
    - Population: Census Bureau Decennial PL API (P1_001N = total population)
    - Geometry:   Census TIGER/Line via pygris

Usage:
    python ingest_population.py
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv
import requests

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

CENSUS_URL = "https://api.census.gov/data/2020/dec/pl"
OHIO_FIPS  = "39"
BATCH_SIZE = 100


# ── Helpers ───────────────────────────────────────────────────────────────────

def validate_env():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    print("[OK]    Environment variables loaded.")


def check_dependencies():
    for pkg in ("geopandas", "pygris"):
        try:
            __import__(pkg)
        except ImportError:
            print(f"[ERROR] Missing package: {pkg}")
            print(f"        Run: pip install {pkg}")
            sys.exit(1)
    print("[OK]    Dependencies available (geopandas, pygris).")


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


def fetch_population() -> dict:
    """
    Fetch total population per tract from the 2020 Decennial Census API.
    Returns dict: geoid -> {population, county_code, tract_code, county_name}
    """
    print("[INFO]  Fetching population from Census API …")
    params = {
        "get": "P1_001N,NAME",
        "for": "tract:*",
        "in": f"state:{OHIO_FIPS}",
    }
    resp = requests.get(CENSUS_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # First row is headers: ['P1_001N', 'NAME', 'state', 'county', 'tract']
    headers = data[0]
    rows = data[1:]

    result = {}
    for row in rows:
        pop = int(row[0]) if row[0] else 0
        name = row[1]  # "Census Tract 7701, Adams County, Ohio"
        state = row[2]
        county = row[3]
        tract = row[4]

        geoid = f"{state}{county}{tract}"
        county_fips = f"{state}{county}"

        # Extract county name from the NAME field
        county_name = None
        if "," in name:
            parts = name.split(",")
            if len(parts) >= 2:
                county_name = parts[1].strip().replace(" County", "").upper()

        result[geoid] = {
            "population": pop,
            "state_fips": state,
            "county_fips": county_fips,
            "tract_code": tract,
            "county_name": county_name,
        }

    print(f"[OK]    Fetched population for {len(result):,} tracts.")
    return result


def fetch_geometries() -> dict:
    """
    Fetch tract boundary geometries from Census TIGER/Line via pygris.
    Returns dict: geoid -> WKT geometry string
    """
    import pygris

    print("[INFO]  Fetching tract geometries from Census Bureau via pygris …")
    gdf = pygris.tracts(state="OH", year=2020)
    print(f"[OK]    Fetched {len(gdf)} tract geometries.")
    print(f"[INFO]  CRS: {gdf.crs}")

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print("[INFO]  Reprojecting to WGS84 (EPSG:4326) …")
        gdf = gdf.to_crs(epsg=4326)

    result = {}
    for _, row in gdf.iterrows():
        geoid = row.get("GEOID", "")
        if geoid and row.geometry:
            result[geoid] = row.geometry.wkt

    print(f"[OK]    {len(result):,} geometries ready.")
    return result


def insert_tracts(conn, population: dict, geometries: dict):
    """Join population + geometry and insert into population_tracts."""
    # Match on GEOID
    matched = []
    unmatched_pop = 0
    unmatched_geo = 0

    for geoid, pop_data in population.items():
        wkt = geometries.get(geoid)
        if wkt is None:
            unmatched_pop += 1
            continue
        matched.append((geoid, pop_data, wkt))

    for geoid in geometries:
        if geoid not in population:
            unmatched_geo += 1

    print(f"[INFO]  {len(matched):,} tracts matched, {unmatched_pop} missing geometry, {unmatched_geo} missing population.")
    print(f"[INFO]  Inserting into population_tracts …")

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for geoid, pop_data, wkt in matched:
            batch.append((
                geoid,
                pop_data["state_fips"],
                pop_data["county_fips"],
                pop_data["tract_code"],
                pop_data["county_name"],
                pop_data["population"],
                wkt,
            ))

            if len(batch) >= BATCH_SIZE:
                _execute_batch(cur, batch)
                inserted += len(batch)
                batch = []

        if batch:
            _execute_batch(cur, batch)
            inserted += len(batch)

    conn.commit()
    print(f"[OK]    Inserted {inserted:,} tracts.")
    return inserted


def _execute_batch(cur, batch):
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s, ST_Multi(ST_SetSRID(ST_GeomFromText(%s), 4326)))",
            row,
        ).decode()
        for row in batch
    )
    cur.execute(
        f"INSERT INTO population_tracts (geoid, state_fips, county_fips, tract_code, county_name, total_population, geometry) "
        f"VALUES {args} "
        f"ON CONFLICT (geoid) DO UPDATE SET "
        f"  total_population = EXCLUDED.total_population, "
        f"  geometry = EXCLUDED.geometry, "
        f"  imported_at = NOW()"
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    conn = connect()
    start = time.time()

    try:
        population = fetch_population()
        geometries = fetch_geometries()
        count = insert_tracts(conn, population, geometries)

        elapsed = time.time() - start

        # Final count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), SUM(total_population) FROM population_tracts")
            row = cur.fetchone()

        print()
        print("─" * 55)
        print(f"[DONE]  Ingestion complete in {elapsed:.1f}s")
        print(f"        Tracts          : {row[0]:>10,}")
        print(f"        Total population : {row[1]:>10,}")
        print("─" * 55)

    except requests.RequestException as e:
        print(f"[ERROR] Census API failed: {e}")
        raise
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[ERROR] Database error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Database connection closed.")


if __name__ == "__main__":
    print()
    print("=== Ohio Census Tract Population Ingestion (2020) ===")
    print()

    validate_env()
    check_dependencies()
    run()
