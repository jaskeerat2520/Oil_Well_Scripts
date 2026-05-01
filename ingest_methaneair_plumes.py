"""
ingest_methaneair_plumes.py

Pulls MethaneAIR L4 point-source plumes over Ohio from the public GEE
FeatureCollection and upserts into the `methane_plumes` Postgres table
(shared with CarbonMapper via the `source` column).

Idempotent: re-runs update existing rows on (source, plume_id).

Usage:
    python ingest_methaneair_plumes.py
"""

import os
from datetime import datetime, timezone
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

METHANEAIR_L4_POINTS = "EDF/MethaneSAT/MethaneAIR/L4point"

OHIO_BBOX = [-84.82, 38.40, -80.52, 41.98]
SOURCE    = "methaneair"


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


def fetch_ohio_plumes():
    ohio = ee.Geometry.Rectangle(OHIO_BBOX)
    fc = ee.FeatureCollection(METHANEAIR_L4_POINTS).filterBounds(ohio).getInfo()
    return fc.get("features", [])


def parse_date(s: str | None) -> str | None:
    """Normalize MethaneAIR date strings to ISO 8601 (Postgres TIMESTAMPTZ-friendly)."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    # MethaneAIR dates have been seen as ISO 8601 already ("2023-07-31T..."),
    # or plain YYYY-MM-DD. Both parse cleanly as TIMESTAMPTZ when passed raw.
    return s or None


def to_row(f: dict) -> tuple | None:
    props  = f.get("properties", {}) or {}
    geom   = f.get("geometry", {}) or {}
    coords = geom.get("coordinates") or [None, None]
    if len(coords) < 2 or coords[0] is None or coords[1] is None:
        return None
    lng, lat = coords[0], coords[1]

    plume_id = props.get("plume_id") or f.get("id")
    if not plume_id:
        return None

    flux    = props.get("flux")
    flux_sd = props.get("flux_sd")
    if flux is not None:
        flux = float(flux)
    if flux_sd is not None:
        flux_sd = float(flux_sd)

    return (
        SOURCE,
        str(plume_id),
        "MethaneAIR",            # single aircraft platform identifier
        None,                    # MethaneAIR L4 has no IPCC sector field
        "CH4",
        flux,
        flux_sd,
        parse_date(props.get("date")),
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
    project = os.getenv("GEE_PROJECT", "earthengine-legacy")
    print(f"Initializing GEE (project: {project})…")
    ee.Initialize(project=project)
    print("Connected.\n")

    print("Fetching MethaneAIR L4 plumes over Ohio…")
    features = fetch_ohio_plumes()
    print(f"  Raw features: {len(features)}")

    raw_rows = [r for r in (to_row(f) for f in features) if r is not None]

    # MethaneAIR re-publishes the same plume_id across overlapping Zamboni
    # swaths when the same super-emitter is re-observed, which trips
    # ON CONFLICT DO UPDATE. Dedupe keeping the highest-flux observation
    # per plume_id — one row per unique physical source.
    by_id: dict[str, tuple] = {}
    EMISSION_IDX = 5  # index of emission_kgph in the row tuple
    for r in raw_rows:
        plume_id = r[1]
        prev = by_id.get(plume_id)
        if prev is None or (r[EMISSION_IDX] or 0) > (prev[EMISSION_IDX] or 0):
            by_id[plume_id] = r
    rows = list(by_id.values())

    skipped = len(features) - len(raw_rows)
    deduped_out = len(raw_rows) - len(rows)
    print(f"  Usable rows: {len(raw_rows)}  (skipped {skipped} missing id/coords)")
    print(f"  After dedupe on plume_id (keep max flux): {len(rows)}  "
          f"(collapsed {deduped_out} duplicate observations)")
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
                  COUNT(*),
                  MIN(observed_at),
                  MAX(observed_at),
                  MIN(emission_kgph),
                  MAX(emission_kgph)
                FROM methane_plumes WHERE source = %s
            """, (SOURCE,))
            n, earliest, latest, fmin, fmax = cur.fetchone()
            print(f"\n✓ methane_plumes table now has {n:,} MethaneAIR rows")
            print(f"  observed_at range: {earliest}  →  {latest}")
            print(f"  emission range:    {fmin}  →  {fmax} kg/hr")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
