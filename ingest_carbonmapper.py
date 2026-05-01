"""
ingest_carbonmapper.py

Pulls CarbonMapper CH4 plumes over Ohio from the CarbonMapper REST API
and upserts them into the `methane_plumes` Postgres table. This table
is a unified home for plume detections from any methane sensor
(CarbonMapper + MethaneAIR L4), joined later by the emissions scorer.

Idempotent: re-runs update existing rows on (source, plume_id).

Setup:
    1. Register at https://data.carbonmapper.org
    2. Generate API token from account settings
    3. Add to .env: CARBONMAPPER_API_TOKEN=<token>

Usage:
    python ingest_carbonmapper.py
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

OHIO_BBOX = [-84.82, 38.40, -80.52, 41.98]
PAGE_SIZE = 500
SOURCE    = "carbonmapper"


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS methane_plumes (
            id             BIGSERIAL PRIMARY KEY,
            source         TEXT        NOT NULL,
            plume_id       TEXT        NOT NULL,
            platform       TEXT,
            sector         TEXT,
            gas            TEXT        NOT NULL DEFAULT 'CH4',
            emission_kgph  DOUBLE PRECISION,
            emission_sd    DOUBLE PRECISION,
            observed_at    TIMESTAMPTZ,
            geometry       geometry(Point, 4326) NOT NULL,
            ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source, plume_id)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS methane_plumes_geom_idx
        ON methane_plumes USING GIST (geometry);
    """)


def fetch_plumes() -> list[dict]:
    if not CM_TOKEN:
        print("✗ CARBONMAPPER_API_TOKEN not set in .env"); sys.exit(1)

    headers = {"Authorization": f"Bearer {CM_TOKEN}"}
    base_params = {"bbox": OHIO_BBOX, "limit": PAGE_SIZE}

    plumes, offset = [], 0
    while True:
        r = requests.get(
            PLUMES_ENDPOINT,
            headers=headers,
            params={**base_params, "offset": offset},
            timeout=60,
        )
        if not r.ok:
            print(f"✗ API error {r.status_code} at offset={offset}: {r.text[:300]}")
            sys.exit(1)
        body = r.json()
        batch = (
            body.get("items") or body.get("features") or body.get("results")
            or (body if isinstance(body, list) else [])
        )
        if not batch:
            break
        plumes.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)
    return plumes


def to_row(p: dict) -> tuple | None:
    """Return a row tuple matching the INSERT template, or None to skip."""
    geom = p.get("geometry_json") or {}
    coords = geom.get("coordinates") or [None, None]
    if len(coords) < 2 or coords[0] is None or coords[1] is None:
        return None
    lng, lat = coords[0], coords[1]

    plume_id = p.get("plume_id") or p.get("id")
    if not plume_id:
        return None

    gas = (p.get("gas") or "CH4").upper()
    if gas not in ("CH4", "METHANE"):
        return None
    gas = "CH4"

    return (
        SOURCE,
        plume_id,
        p.get("platform") or p.get("instrument"),
        p.get("sector"),
        gas,
        p.get("emission_auto"),
        p.get("emission_uncertainty_auto"),
        p.get("scene_timestamp") or p.get("published_at"),
        lng,
        lat,
    )


INSERT_SQL = """
    INSERT INTO methane_plumes
      (source, plume_id, platform, sector, gas,
       emission_kgph, emission_sd, observed_at, geometry)
    VALUES %s
    ON CONFLICT (source, plume_id) DO UPDATE SET
      platform      = EXCLUDED.platform,
      sector        = EXCLUDED.sector,
      gas           = EXCLUDED.gas,
      emission_kgph = EXCLUDED.emission_kgph,
      emission_sd   = EXCLUDED.emission_sd,
      observed_at   = EXCLUDED.observed_at,
      geometry      = EXCLUDED.geometry,
      ingested_at   = NOW()
"""

INSERT_TEMPLATE = (
    "(%s, %s, %s, %s, %s, %s, %s, %s, "
    "ST_SetSRID(ST_MakePoint(%s, %s), 4326))"
)


def main():
    print("Fetching CarbonMapper plumes over Ohio…")
    raw = fetch_plumes()
    print(f"  Raw records: {len(raw)}")

    rows = [r for r in (to_row(p) for p in raw) if r is not None]
    skipped = len(raw) - len(rows)
    print(f"  Usable CH4 rows: {len(rows)}  (skipped {skipped} non-CH4 or missing coords)")
    if not rows:
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            psycopg2.extras.execute_values(
                cur, INSERT_SQL, rows, template=INSERT_TEMPLATE
            )
            conn.commit()

            cur.execute("""
                SELECT
                  COUNT(*)                                AS n,
                  COUNT(*) FILTER (WHERE sector = '1B2')  AS n_oil_gas,
                  MIN(observed_at)                        AS earliest,
                  MAX(observed_at)                        AS latest
                FROM methane_plumes WHERE source = %s
            """, (SOURCE,))
            n, n_og, earliest, latest = cur.fetchone()
            print(f"\n✓ methane_plumes table now has {n:,} CarbonMapper rows")
            print(f"  sector=1B2 (oil/gas): {n_og:,}")
            print(f"  observed_at range:    {earliest}  →  {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
