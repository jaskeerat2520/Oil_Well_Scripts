"""
Statewide OGRIP parcel ingest. Pulls geometry + mailing-address fields from
the Ohio Statewide Parcels public ArcGIS layer, county-by-county.

Why this is separate from enrich_parcels_ogrip.py:
    enrich_parcels_ogrip.py only UPDATEs existing rows' mailing fields
    (used for Hocking, where auditor data was loaded first). This script
    INSERTS new rows for counties that have no parcels yet, AND uses a
    conflict resolution that PRESERVES owner_name when it exists. So you
    can run this on Hocking after the auditor pull and it won't clobber
    the Hocking owner names; running it on a fresh county loads geometry
    + mailing with NULL owner_name.

Source:
    https://services2.arcgis.com/MlJ0G8iWUyC7jAmu/arcgis/rest/services/
        OhioStatewidePacels_full_view/FeatureServer/0
    (URL has a typo: "Pacels" not "Parcels". Don't fix.)

Usage:
    python ingest_parcels_ogrip_statewide.py --counties COLUMBIANA,ALLEN
    python ingest_parcels_ogrip_statewide.py --top 5    # top-5 by hi-prio wells
    python ingest_parcels_ogrip_statewide.py --all       # every Ohio county
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

OGRIP_URL  = "https://services2.arcgis.com/MlJ0G8iWUyC7jAmu/arcgis/rest/services/OhioStatewidePacels_full_view/FeatureServer/0/query"
PAGE_SIZE  = 2000
BATCH_SIZE = 100

# ZIP3 → state for deriving owner_mailing_state from MailZip (OGRIP's
# MailState field is broken — see enrich_parcels_ogrip.py for context).
ZIP3_RANGES: list[tuple[tuple[int, int], str]] = [
    ((350, 369), 'AL'), ((995, 999), 'AK'), ((850, 865), 'AZ'),
    ((716, 729), 'AR'), ((900, 961), 'CA'), ((800, 816), 'CO'),
    ((60,  69),  'CT'), ((197, 199), 'DE'), ((320, 349), 'FL'),
    ((300, 319), 'GA'), ((398, 399), 'GA'), ((967, 968), 'HI'),
    ((832, 838), 'ID'), ((600, 629), 'IL'), ((460, 479), 'IN'),
    ((500, 528), 'IA'), ((660, 679), 'KS'), ((400, 427), 'KY'),
    ((700, 714), 'LA'), ((39,  49),  'ME'), ((206, 219), 'MD'),
    ((10,  27),  'MA'), ((480, 499), 'MI'), ((550, 567), 'MN'),
    ((386, 397), 'MS'), ((630, 658), 'MO'), ((590, 599), 'MT'),
    ((680, 693), 'NE'), ((889, 898), 'NV'), ((30,  38),  'NH'),
    ((70,  89),  'NJ'), ((870, 884), 'NM'), ((100, 149), 'NY'),
    ((270, 289), 'NC'), ((580, 588), 'ND'), ((430, 458), 'OH'),
    ((730, 749), 'OK'), ((970, 979), 'OR'), ((150, 196), 'PA'),
    ((6,    9),  'PR'), ((28,  29),  'RI'), ((290, 299), 'SC'),
    ((570, 577), 'SD'), ((370, 385), 'TN'), ((750, 799), 'TX'),
    ((885, 885), 'TX'), ((840, 847), 'UT'), ((50,  54),  'VT'),
    ((56,  59),  'VT'), ((220, 246), 'VA'), ((200, 205), 'DC'),
    ((980, 994), 'WA'), ((247, 268), 'WV'), ((530, 549), 'WI'),
    ((820, 831), 'WY'),
]


def zip3_to_state(zip_code: str | None) -> str | None:
    if not zip_code:
        return None
    digits = "".join(c for c in str(zip_code) if c.isdigit())
    if len(digits) < 3:
        return None
    z3 = int(digits[:3])
    for (lo, hi), state in ZIP3_RANGES:
        if lo <= z3 <= hi:
            return state
    return None


def connect():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


# ── County selection ──────────────────────────────────────────────────────────

def list_counties_by_priority(conn) -> list[tuple[str, int, int]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              w.county,
              COUNT(*) AS wells_in_pool,
              COUNT(*) FILTER (WHERE wrs.priority IN ('critical','high')) AS hi_prio
            FROM wells w
            JOIN well_risk_scores wrs USING (api_no)
            WHERE w.county IS NOT NULL
            GROUP BY w.county
            ORDER BY hi_prio DESC, wells_in_pool DESC
        """)
        return cur.fetchall()


def already_loaded(conn, county) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM parcels WHERE county = %s", (county,))
        return cur.fetchone()[0]


# ── OGRIP fetch ───────────────────────────────────────────────────────────────

def fetch_county_features(county_titlecase: str) -> list[dict]:
    """Paginated geojson fetch with retry on transient OGRIP errors. The big
    counties (Cuyahoga, Franklin, Hamilton at 400K+ parcels) sometimes 504 on
    the deeper offsets when the server's under load — those are recoverable
    with a wait. Permanent failures (4xx, malformed JSON) raise out."""
    out: list[dict] = []
    offset = 0
    while True:
        params = {
            "where": f"County='{county_titlecase}'",
            "outFields": "LocalParcelID,MailAddressAll,MailCity,MailZip,LandArea,StateLUC",
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }
        body = None
        last_err: Exception | None = None
        for attempt in range(4):  # 4 tries: initial + 3 retries
            try:
                resp = requests.get(OGRIP_URL, params=params, timeout=180)
                # 5xx = retry; 4xx = permanent (raise)
                if 500 <= resp.status_code < 600:
                    last_err = requests.HTTPError(f"{resp.status_code}", response=resp)
                    raise last_err
                resp.raise_for_status()
                body = resp.json()
                break
            except (requests.HTTPError, requests.Timeout, requests.ConnectionError, ValueError) as e:
                last_err = e
                # Don't retry on 4xx — those won't get better.
                if isinstance(e, requests.HTTPError) and e.response is not None \
                        and 400 <= e.response.status_code < 500:
                    raise
                if attempt < 3:
                    sleep_s = 5 * (3 ** attempt)  # 5, 15, 45
                    print(f"        ! offset {offset}: {e} — retrying in {sleep_s}s "
                          f"(attempt {attempt + 1}/3)", flush=True)
                    time.sleep(sleep_s)
                else:
                    raise
        if body is None:
            raise last_err or RuntimeError("Empty response after retries")

        features = body.get("features", [])
        if not features:
            break
        out.extend(features)
        if len(features) < PAGE_SIZE and not body.get("exceededTransferLimit"):
            break
        offset += PAGE_SIZE
    return out


# ── Geometry merge for multi-polygon parcels ──────────────────────────────────

def merge_polygons(geoms: list[dict]) -> dict | None:
    coords: list = []
    for g in geoms:
        if not g:
            continue
        if g.get("type") == "Polygon":
            coords.append(g["coordinates"])
        elif g.get("type") == "MultiPolygon":
            coords.extend(g["coordinates"])
    if not coords:
        return None
    return {"type": "MultiPolygon", "coordinates": coords}


# ── DB upsert ─────────────────────────────────────────────────────────────────

def upsert_features(conn, features: list[dict], county_uppercase: str) -> int:
    if not features:
        return 0

    geoms_by_pid: dict[str, list[dict]] = defaultdict(list)
    props_by_pid: dict[str, dict] = {}

    for f in features:
        props = f.get("properties") or {}
        geom  = f.get("geometry")
        pid = props.get("LocalParcelID")
        if pid is None or geom is None:
            continue
        pid = str(pid).strip()
        if not pid:
            continue
        geoms_by_pid[pid].append(geom)
        if pid not in props_by_pid:
            props_by_pid[pid] = props

    inserted = 0
    with conn.cursor() as cur:
        batch = []
        for pid, geoms in geoms_by_pid.items():
            merged = merge_polygons(geoms)
            if merged is None:
                continue
            p = props_by_pid[pid]

            addr  = (p.get("MailAddressAll") or "").strip() or None
            city  = (p.get("MailCity")       or "").strip() or None
            zipc  = (p.get("MailZip")        or "").strip() or None
            state = zip3_to_state(zipc)
            try:
                acreage = float(p.get("LandArea")) if p.get("LandArea") not in (None, "") else None
            except (TypeError, ValueError):
                acreage = None
            luc = (p.get("StateLUC") or "").strip() or None

            batch.append((
                county_uppercase, pid,
                addr, city, state, zipc,
                acreage, luc,
                json.dumps(merged),
            ))
            if len(batch) >= BATCH_SIZE:
                _exec_batch(cur, batch)
                inserted += len(batch)
                batch = []
        if batch:
            _exec_batch(cur, batch)
            inserted += len(batch)
    conn.commit()
    return inserted


def _exec_batch(cur, batch):
    args = ",".join(
        cur.mogrify(
            "(%s, %s, %s, %s, %s, %s, %s, %s, "
            "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))",
            row,
        ).decode()
        for row in batch
    )
    # Conflict policy:
    #   - Mailing fields, acreage, land_use_code, ingested_at: always refresh
    #     from EXCLUDED.
    #   - geom: keep existing on conflict (don't disturb the spatial join
    #     for Hocking which was loaded from the auditor).
    #   - owner_name and owner_type: NEVER update — those came from the
    #     auditor and OGRIP doesn't have them. NULLing them would be wrong.
    cur.execute(
        "INSERT INTO parcels "
        "(county, parcel_id, owner_mailing_address, owner_mailing_city, "
        " owner_mailing_state, owner_mailing_zip, acreage, land_use_code, geom) "
        f"VALUES {args} "
        "ON CONFLICT (county, parcel_id) DO UPDATE SET "
        "  owner_mailing_address = EXCLUDED.owner_mailing_address, "
        "  owner_mailing_city    = EXCLUDED.owner_mailing_city, "
        "  owner_mailing_state   = EXCLUDED.owner_mailing_state, "
        "  owner_mailing_zip     = EXCLUDED.owner_mailing_zip, "
        "  ingested_at           = now()"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_counties: list[str], skip_loaded: bool):
    conn = connect()
    grand_total = 0
    grand_start = time.time()
    try:
        for i, county in enumerate(target_counties, 1):
            existing = already_loaded(conn, county)
            if skip_loaded and existing > 0:
                print(f"[{i:>2}/{len(target_counties)}] {county:<14} {existing:,} already loaded — skipping")
                continue

            t0 = time.time()
            print(f"[{i:>2}/{len(target_counties)}] {county:<14} fetching…", end=" ", flush=True)
            try:
                features = fetch_county_features(county.title())
                count = upsert_features(conn, features, county)
                grand_total += count
                print(f"{count:,} parcels in {time.time()-t0:.1f}s")
            except Exception as e:
                # Don't let one county take down the whole run. Logged and
                # skip-loaded means the next invocation retries it.
                print(f"FAILED after {time.time()-t0:.1f}s — {type(e).__name__}: {e}")
                continue
        elapsed = time.time() - grand_start
        print()
        print("─" * 55)
        print(f"[DONE] {grand_total:,} parcels processed across "
              f"{len(target_counties)} counties in {elapsed:.0f}s.")
        print("─" * 55)
    finally:
        conn.close()


if __name__ == "__main__":
    print("\n=== OGRIP Multi-County Parcel Ingest ===\n")
    parser = argparse.ArgumentParser()
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--counties", help="Comma-separated list of UPPERCASE county names.")
    g.add_argument("--top",      type=int, help="Top-N counties by hi-priority well count.")
    g.add_argument("--all",      action="store_true", help="All 88 counties.")
    parser.add_argument("--no-skip-loaded", action="store_true",
                        help="By default, counties with parcels already loaded are skipped.")
    args = parser.parse_args()

    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    if args.counties:
        targets = [c.strip().upper() for c in args.counties.split(",") if c.strip()]
    else:
        with connect() as c:
            ranking = list_counties_by_priority(c)
        if args.top:
            targets = [row[0] for row in ranking[:args.top]]
        else:
            targets = [row[0] for row in ranking]
        print(f"[INFO]  Target counties (in priority order):")
        for c in targets:
            print(f"          {c}")

    run(targets, skip_loaded=not args.no_skip_loaded)
