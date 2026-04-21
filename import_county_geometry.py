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


def fetch_county_boundaries():
    import pygris
    print("[INFO]  Fetching Ohio county boundaries from Census Bureau via pygris …")
    gdf = pygris.counties(state="OH", year=2023)
    print(f"[OK]    Fetched {len(gdf)} county features.")
    print(f"[INFO]  CRS: {gdf.crs}")

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print("[INFO]  Reprojecting to WGS84 (EPSG:4326) …")
        gdf = gdf.to_crs(epsg=4326)
        print("[OK]    Reprojection done.")

    return gdf


def import_geometry(conn, gdf):
    updated = 0
    skipped = 0
    errors  = 0

    print("[INFO]  Updating county geometries in database …")
    with conn.cursor() as cur:
        for _, row in gdf.iterrows():
            county_name = row.get("NAME", "").upper().strip()
            geom_wkt    = row.geometry.wkt

            try:
                cur.execute(
                    """
                    UPDATE counties
                    SET geometry = ST_Multi(ST_GeomFromText(%s, 4326))
                    WHERE name = %s
                    """,
                    (geom_wkt, county_name)
                )
                if cur.rowcount == 1:
                    updated += 1
                    print(f"[OK]    {county_name}")
                else:
                    print(f"[WARN]  No match for '{county_name}' — skipping.")
                    skipped += 1
            except psycopg2.Error as e:
                conn.rollback()
                print(f"[ERROR] Failed to update {county_name}: {e}")
                errors += 1
                continue

        conn.commit()

    print()
    print("─" * 50)
    print(f"[DONE]  Geometry import complete.")
    print(f"        Updated : {updated:>5}")
    print(f"        Skipped : {skipped:>5}")
    print(f"        Errors  : {errors:>5}")
    print("─" * 50)

    if updated < 88:
        print(f"[WARN]  Only {updated}/88 counties updated. Check name mismatches above.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print()
    print("=== Ohio County Geometry Importer ===")
    print()

    check_dependencies()

    conn = connect()
    try:
        gdf = fetch_county_boundaries()
        import_geometry(conn, gdf)
    except KeyboardInterrupt:
        print()
        print("[ABORT] Interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        raise
    finally:
        conn.close()
        print("[INFO]  Database connection closed.")
