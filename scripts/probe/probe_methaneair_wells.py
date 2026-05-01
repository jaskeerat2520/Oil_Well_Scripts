"""
probe_methaneair_wells.py

Read-only. Answers: how many Ohio wells fall inside MethaneAIR's flight
footprints, and how close are they to detected L4 plumes?

Output of this probe decides whether MethaneAIR becomes the primary
methane signal (most scored wells are in-footprint and near plumes) or
an enhancement layer on top of the Sentinel-5P hotspot fallback.

No database writes. Safe to re-run.

Usage:
    python probe_methaneair_wells.py
"""

import os
import json
import ee
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

METHANEAIR_L3 = "EDF/MethaneSAT/MethaneAIR/L3concentration"
METHANEAIR_L4 = "EDF/MethaneSAT/MethaneAIR/L4point"

OHIO_BBOX = [-84.82, 38.40, -80.52, 41.98]


def pct(n: int, d: int) -> str:
    return f"{(100.0 * n / d):5.1f}%" if d else "   n/a"


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def fetch_ohio_footprints(ohio: ee.Geometry) -> list[str]:
    """Return a list of GeoJSON polygon strings, one per Ohio-touching flight."""
    l3 = ee.ImageCollection(METHANEAIR_L3).filterBounds(ohio)
    fc = l3.map(lambda img: ee.Feature(img.geometry(), {
        "flight_id": img.get("flight_id"),
        "basin":     img.get("basin"),
    })).getInfo()
    return [json.dumps(f["geometry"]) for f in fc["features"]]


def fetch_ohio_plumes(ohio: ee.Geometry) -> list[tuple[float, float, float]]:
    """Return (lng, lat, flux_kgph) per plume; flux=0 if missing."""
    l4 = ee.FeatureCollection(METHANEAIR_L4).filterBounds(ohio).getInfo()
    out = []
    for f in l4["features"]:
        p = f["properties"]
        lng, lat = f["geometry"]["coordinates"]
        out.append((lng, lat, float(p.get("flux") or 0.0)))
    return out


def main():
    project = os.getenv("GEE_PROJECT", "earthengine-legacy")
    print(f"Initializing GEE (project: {project})…")
    ee.Initialize(project=project)
    print("Connected.\n")

    ohio = ee.Geometry.Rectangle(OHIO_BBOX)

    print("Fetching MethaneAIR Ohio flight footprints from GEE…")
    footprint_geojsons = fetch_ohio_footprints(ohio)
    print(f"  → {len(footprint_geojsons)} flight footprints\n")

    print("Fetching MethaneAIR Ohio plume points from GEE…")
    plumes = fetch_ohio_plumes(ohio)
    print(f"  → {len(plumes)} plume points\n")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # ── Baseline ────────────────────────────────────────────────
            cur.execute("SELECT COUNT(*) FROM wells")
            total_wells = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM well_risk_scores")
            scored_wells = cur.fetchone()[0]

            print("── Baseline ──")
            print(f"  Total Ohio wells:  {total_wells:>9,}")
            print(f"  Scored wells:      {scored_wells:>9,}")

            # ── Wells inside union of flight footprints ────────────────
            print("\n── Wells inside MethaneAIR flight footprints ──")
            cur.execute("""
                WITH fp AS (
                    SELECT ST_SetSRID(ST_GeomFromGeoJSON(gj), 4326) AS geom
                    FROM unnest(%s::text[]) AS gj
                ),
                fp_union AS (SELECT ST_Union(geom) AS geom FROM fp)
                SELECT
                  (SELECT COUNT(*) FROM wells w, fp_union u
                   WHERE ST_Intersects(w.geometry, u.geom)),
                  (SELECT COUNT(*) FROM wells w
                   JOIN well_risk_scores wrs ON w.api_no = wrs.api_no,
                   fp_union u
                   WHERE ST_Intersects(w.geometry, u.geom))
            """, (footprint_geojsons,))
            all_in_fp, scored_in_fp = cur.fetchone()

            print(f"  All wells in footprint:     {all_in_fp:>9,}  ({pct(all_in_fp, total_wells)} of all)")
            print(f"  Scored wells in footprint:  {scored_in_fp:>9,}  ({pct(scored_in_fp, scored_wells)} of scored)")

            # ── Plume proximity ────────────────────────────────────────
            if plumes:
                cur.execute("""
                    CREATE TEMP TABLE tmp_plumes (
                        lng float8, lat float8, flux float8
                    ) ON COMMIT DROP
                """)
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO tmp_plumes (lng, lat, flux) VALUES %s",
                    plumes,
                )

                cur.execute("""
                    WITH plume_pts AS (
                        SELECT ST_SetSRID(ST_MakePoint(lng, lat), 4326) AS geom,
                               flux
                        FROM tmp_plumes
                    ),
                    nearest AS (
                        SELECT w.api_no,
                               wrs.risk_score,
                               MIN(ST_Distance(
                                    w.geometry::geography,
                                    p.geom::geography
                               )) AS min_dist_m,
                               MAX(p.flux) FILTER (
                                   WHERE ST_DWithin(
                                       w.geometry::geography,
                                       p.geom::geography,
                                       1000
                                   )
                               ) AS max_flux_1km
                        FROM wells w
                        JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
                        CROSS JOIN plume_pts p
                        WHERE ST_DWithin(
                            w.geometry::geography,
                            p.geom::geography,
                            10000
                        )
                        GROUP BY w.api_no, wrs.risk_score
                    )
                    SELECT
                      COUNT(*) FILTER (WHERE min_dist_m <=   500),
                      COUNT(*) FILTER (WHERE min_dist_m <=  1000),
                      COUNT(*) FILTER (WHERE min_dist_m <=  5000),
                      COUNT(*) FILTER (WHERE min_dist_m <= 10000)
                    FROM nearest
                """)
                w500, w1k, w5k, w10k = cur.fetchone()

                print("\n── Scored wells within distance of any plume ──")
                print(f"  within    500 m:  {w500:>9,}  ({pct(w500,  scored_wells)} of scored)")
                print(f"  within  1,000 m:  {w1k:>9,}  ({pct(w1k,   scored_wells)} of scored)")
                print(f"  within  5,000 m:  {w5k:>9,}  ({pct(w5k,   scored_wells)} of scored)")
                print(f"  within 10,000 m:  {w10k:>9,}  ({pct(w10k, scored_wells)} of scored)")

                # ── Top nearest-plume wells (qualitative check) ────────
                cur.execute("""
                    WITH plume_pts AS (
                        SELECT ST_SetSRID(ST_MakePoint(lng, lat), 4326) AS geom,
                               flux
                        FROM tmp_plumes
                    ),
                    ranked AS (
                        SELECT w.api_no, w.county, wrs.risk_score,
                               ST_Distance(w.geometry::geography, p.geom::geography) AS dist_m,
                               p.flux AS plume_flux_kgph
                        FROM wells w
                        JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
                        CROSS JOIN plume_pts p
                        WHERE ST_DWithin(
                            w.geometry::geography,
                            p.geom::geography,
                            1000
                        )
                    )
                    SELECT api_no, county, ROUND(risk_score::numeric, 1),
                           ROUND(dist_m::numeric, 0), ROUND(plume_flux_kgph::numeric, 0)
                    FROM ranked
                    ORDER BY plume_flux_kgph DESC NULLS LAST, dist_m ASC
                    LIMIT 10
                """)
                top = cur.fetchall()
                if top:
                    print("\n── Top scored-wells-near-plumes (within 1 km) ──")
                    print(f"  {'api_no':<14} {'county':<15} {'risk':>6} {'dist_m':>8} {'flux_kgph':>10}")
                    for api, county, rs, d, flux in top:
                        print(f"  {api:<14} {county:<15} {rs!s:>6} {d!s:>8} {flux!s:>10}")
    finally:
        conn.close()

    print("\n── Interpretation ──")
    if scored_in_fp and scored_in_fp / scored_wells > 0.25:
        print(f"  ✓ MethaneAIR covers a meaningful slice ({pct(scored_in_fp, scored_wells).strip()}).")
        print("    → Make MethaneAIR the primary methane signal for in-footprint")
        print("      wells; S5P hotspot remains the fallback for the rest.")
    elif scored_in_fp:
        print(f"  ~ MethaneAIR covers a narrow slice ({pct(scored_in_fp, scored_wells).strip()}).")
        print("    → Use it as an enhancement layer: wells inside its footprint")
        print("      get the sharper 10m signal; everyone else stays on S5P.")
    else:
        print("  ✗ Zero scored wells in MethaneAIR footprints.")
        print("    → Keep S5P hotspot, skip MethaneAIR integration.")


if __name__ == "__main__":
    main()
