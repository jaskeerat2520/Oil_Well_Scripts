"""
probe_carbonmapper_wells.py

Read-only. Queries CarbonMapper's REST API for CH4 plumes in Ohio and
reports how many scored wells fall within 500m / 1km / 5km / 10km of
them. Complements probe_methaneair_wells.py:

  MethaneAIR  = 2023 aircraft snapshot (10 m), free, but ended Oct 2023
  CarbonMapper = Tanager-1 + aircraft, live and ongoing, account-gated
                 Also carries the persistence metric (chronic vs one-off)

Setup:
  1. Register at https://data.carbonmapper.org
  2. Generate an API token from account settings
  3. Add to .env:
       CARBONMAPPER_API_TOKEN=<your-token>

Usage:
    python probe_carbonmapper_wells.py
"""

import os
import sys
import time
import json
import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

CM_TOKEN        = os.getenv("CARBONMAPPER_API_TOKEN")
PLUMES_ENDPOINT = "https://api.carbonmapper.org/api/v1/catalog/plumes/annotated"

OHIO_BBOX = [-84.82, 38.40, -80.52, 41.98]   # W, S, E, N
PAGE_SIZE = 500


def pct(n: int, d: int) -> str:
    return f"{(100.0 * n / d):5.1f}%" if d else "   n/a"


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def fetch_ohio_plumes() -> list[dict]:
    if not CM_TOKEN:
        print("✗ CARBONMAPPER_API_TOKEN not set in .env")
        print("  Register at https://data.carbonmapper.org, generate a token,")
        print("  then add CARBONMAPPER_API_TOKEN=<token> to .env.")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {CM_TOKEN}"}
    # Django Ninja expects bbox as a repeated query param (bbox=a&bbox=b&...),
    # so pass a list — requests auto-serializes lists that way.
    base_params = {
        "bbox": OHIO_BBOX,
        "limit": PAGE_SIZE,
    }
    plumes: list[dict] = []
    offset = 0
    while True:
        params = {**base_params, "offset": offset}
        r = requests.get(PLUMES_ENDPOINT, headers=headers, params=params, timeout=60)
        if r.status_code == 401:
            print("✗ Auth failed (401). Token in .env may be invalid/expired.")
            print(f"  Response: {r.text[:300]}")
            sys.exit(1)
        if not r.ok:
            print(f"✗ API error {r.status_code} at offset={offset}:")
            print(f"  {r.text[:500]}")
            sys.exit(1)

        body = r.json()
        batch = (
            body.get("items")
            or body.get("features")
            or body.get("results")
            or (body if isinstance(body, list) else [])
        )
        if not batch:
            if offset == 0:
                # Dump the body so the user can see what the API actually returned.
                print("✗ Zero records in first page. Raw response head:")
                print("  " + json.dumps(body, indent=2)[:800])
            break

        plumes.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)   # be polite
    return plumes


def normalize(p: dict) -> dict:
    """Map CarbonMapper annotated-plume record fields to our common shape."""
    geom = p.get("geometry_json") or {}
    coords = geom.get("coordinates") or [None, None]
    lng = coords[0] if len(coords) >= 1 else None
    lat = coords[1] if len(coords) >= 2 else None
    return {
        "lng":      lng,
        "lat":      lat,
        "emission": p.get("emission_auto"),
        "unc":      p.get("emission_uncertainty_auto"),
        "datetime": p.get("scene_timestamp") or p.get("published_at"),
        "platform": p.get("platform") or p.get("instrument"),
        "sector":   p.get("sector"),
        "gas":      p.get("gas"),
        "quality":  p.get("plume_quality"),
        "status":   p.get("status"),
        "plume_id": p.get("plume_id") or p.get("id"),
    }


def main():
    print("Fetching CarbonMapper plumes over Ohio…")
    raw = fetch_ohio_plumes()
    print(f"  Raw records returned: {len(raw)}")

    normalized = [normalize(p) for p in raw]
    normalized = [p for p in normalized if p["lng"] is not None and p["lat"] is not None]
    ch4 = [p for p in normalized if (p["gas"] or "").upper() in ("CH4", "METHANE", "")]
    print(f"  Records with usable coords: {len(normalized)}")
    print(f"  CH4 plumes:                 {len(ch4)}\n")

    if not ch4:
        print("No usable CH4 plumes. Dumping first raw record so we can see the shape:")
        if raw:
            print(json.dumps(raw[0], indent=2)[:2000])
        return

    # ── Breakdown: platform / sector / dates ────────────────────────────
    print("── Ohio plume breakdown ──")
    platforms: dict[str | None, int] = {}
    sectors:   dict[str | None, int] = {}
    dates = [p["datetime"] for p in ch4 if p["datetime"]]
    for p in ch4:
        platforms[p["platform"]] = platforms.get(p["platform"], 0) + 1
        sectors[p["sector"]]     = sectors.get(p["sector"], 0) + 1

    print("  By platform:")
    for pl, n in sorted(platforms.items(), key=lambda x: -x[1]):
        print(f"    {str(pl or '?'):<15} {n:>5}")
    print("  By IPCC sector:")
    for sec, n in sorted(sectors.items(), key=lambda x: -x[1]):
        print(f"    {str(sec or '?'):<15} {n:>5}")
    if dates:
        dates.sort()
        print(f"  Date range: {dates[0][:10]} → {dates[-1][:10]}")

    # ── Top plumes by emission ──────────────────────────────────────────
    with_em = [p for p in ch4 if p["emission"] is not None]
    with_em.sort(key=lambda p: p["emission"], reverse=True)
    print("\n── Top 10 Ohio plumes by emission rate ──")
    for p in with_em[:10]:
        unc = p["unc"] or 0
        print(
            f"  {str(p['datetime'] or '?')[:10]}  "
            f"{p['emission']:>8.1f} ± {unc:>5.1f} kg/hr  "
            f"[{str(p['platform'] or '?'):<10}]  "
            f"sector={p['sector'] or '?'}  "
            f"@ ({p['lng']:.4f}, {p['lat']:.4f})"
        )

    # ── Well-proximity ──────────────────────────────────────────────────
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM well_risk_scores")
            scored_wells = cur.fetchone()[0]
            print(f"\n── Baseline ──\n  Scored wells: {scored_wells:,}")

            cur.execute("""
                CREATE TEMP TABLE tmp_cm_plumes (
                    lng float8, lat float8, emission float8
                ) ON COMMIT DROP
            """)
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO tmp_cm_plumes (lng, lat, emission) VALUES %s",
                [(p["lng"], p["lat"], p["emission"] or 0.0) for p in ch4],
            )

            cur.execute("""
                WITH plume_pts AS (
                    SELECT ST_SetSRID(ST_MakePoint(lng, lat), 4326) AS geom,
                           emission
                    FROM tmp_cm_plumes
                ),
                nearest AS (
                    SELECT w.api_no,
                           MIN(ST_Distance(
                                w.geometry::geography,
                                p.geom::geography
                           )) AS min_dist_m
                    FROM wells w
                    JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
                    CROSS JOIN plume_pts p
                    WHERE ST_DWithin(
                        w.geometry::geography, p.geom::geography, 10000
                    )
                    GROUP BY w.api_no
                )
                SELECT
                  COUNT(*) FILTER (WHERE min_dist_m <=   500),
                  COUNT(*) FILTER (WHERE min_dist_m <=  1000),
                  COUNT(*) FILTER (WHERE min_dist_m <=  5000),
                  COUNT(*) FILTER (WHERE min_dist_m <= 10000)
                FROM nearest
            """)
            w500, w1k, w5k, w10k = cur.fetchone()

            print("\n── Scored wells within distance of any CarbonMapper plume ──")
            print(f"  within    500 m:  {w500:>9,}  ({pct(w500,  scored_wells)} of scored)")
            print(f"  within  1,000 m:  {w1k:>9,}  ({pct(w1k,   scored_wells)} of scored)")
            print(f"  within  5,000 m:  {w5k:>9,}  ({pct(w5k,   scored_wells)} of scored)")
            print(f"  within 10,000 m:  {w10k:>9,}  ({pct(w10k, scored_wells)} of scored)")

            cur.execute("""
                WITH plume_pts AS (
                    SELECT ST_SetSRID(ST_MakePoint(lng, lat), 4326) AS geom,
                           emission
                    FROM tmp_cm_plumes
                )
                SELECT w.api_no, w.county,
                       ROUND(wrs.risk_score::numeric, 1),
                       ROUND(ST_Distance(
                            w.geometry::geography, p.geom::geography
                       )::numeric, 0) AS dist_m,
                       ROUND(p.emission::numeric, 1) AS flux
                FROM wells w
                JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
                CROSS JOIN plume_pts p
                WHERE ST_DWithin(
                    w.geometry::geography, p.geom::geography, 1000
                )
                ORDER BY p.emission DESC NULLS LAST, dist_m ASC
                LIMIT 10
            """)
            top = cur.fetchall()
            if top:
                print("\n── Top scored wells within 1 km of a plume ──")
                print(f"  {'api_no':<14} {'county':<15} {'risk':>6} {'dist_m':>8} {'kgph':>8}")
                for r in top:
                    api, county, rs, d, flux = r
                    print(f"  {api:<14} {county:<15} {rs!s:>6} {d!s:>8} {flux!s:>8}")
    finally:
        conn.close()

    # ── Verdict ─────────────────────────────────────────────────────────
    print("\n── Interpretation ──")
    print("  Compare these counts against probe_methaneair_wells.py output:")
    print("  - If CarbonMapper covers MORE wells than MethaneAIR, make it primary.")
    print("  - If similar slice but DIFFERENT wells, union both for max coverage.")
    print("  - If similar slice + same wells, prefer CarbonMapper (live, persistent).")


if __name__ == "__main__":
    main()
