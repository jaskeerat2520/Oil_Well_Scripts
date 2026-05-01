"""
Enrich parcels.owner_mailing_* columns from the OGRIP Ohio Statewide Parcels
public ArcGIS layer.

Why this is separate from ingest_parcels.py:
    The OGRIP layer has mailing-address fields but NO owner name. The auditor
    layers we already ingested have owner name but no mailing address. Rather
    than redo the auditor pull, this script does an UPDATE pass keyed by
    (county, parcel_id) — keeping owner names intact and only filling the
    NULL mailing_* columns. Smallest possible blast radius.

Source:
    https://services2.arcgis.com/MlJ0G8iWUyC7jAmu/arcgis/rest/services/
        OhioStatewidePacels_full_view/FeatureServer/0
    (Yes, the URL has a typo: "Pacels" not "Parcels". Don't fix.)

Coverage caveat:
    79% of Hocking parcels have populated MailState in the OGRIP feed —
    21% are vacant lots, government parcels, or counties without published
    CAMA. Aggregate-empty parcels are a long tail in any source.

Usage:
    python enrich_parcels_ogrip.py --county HOCKING
    python enrich_parcels_ogrip.py --county HOCKING --dry-run   # preview only
"""

import argparse
import os
import sys
import time

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
PAGE_SIZE  = 2000   # OGRIP server-side max
BATCH_SIZE = 200    # DB UPDATE batch size

# OGRIP's MailState field is broken — it's populated with the ZIP3 prefix
# (e.g. '431' for Logan OH, '377' for Loudon TN) rather than the state
# abbreviation. We derive the actual state from MailZip using USPS ZIP3
# ranges. Source: standard USPS ZIP3-to-state ranges, en.wikipedia.org/wiki/ZIP_Code.
# A ZIP3 may resolve to None (rare unassigned ranges) or to a region we don't
# care about; for the absentee-owner signal, "OH vs not-OH" is what matters.
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
    """Derive USPS state abbreviation from a ZIP code (5- or 9-digit). Returns
    None for codes outside the known ranges."""
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
    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )
    print("[OK]    Connected.")
    return conn


def fetch_county(county_titlecase: str) -> list[dict]:
    """Page through OGRIP filtered by County. We only need attributes — geometry
    isn't useful here since the parcels table already has it."""
    out: list[dict] = []
    offset = 0
    while True:
        params = {
            "where": f"County='{county_titlecase}'",
            "outFields": "LocalParcelID,MailAddressAll,MailCity,MailState,MailZip",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        }
        resp = requests.get(OGRIP_URL, params=params, timeout=120)
        resp.raise_for_status()
        body = resp.json()

        if "error" in body:
            print(f"[ERROR] OGRIP returned: {body['error']}")
            sys.exit(1)

        features = body.get("features", [])
        if not features:
            break

        out.extend(features)
        print(f"        … {len(out):,} fetched")

        if not body.get("exceededTransferLimit") and len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return out


def build_updates(features: list[dict]) -> list[tuple]:
    """One row per (parcel_id, mailadd, mailcity, mailstate, mailzip).
    Skip features with no parcel_id and skip those with no mailing data at
    all (no point in writing all-NULLs)."""
    rows = []
    skipped_empty = 0
    skipped_no_id = 0
    for f in features:
        a = f.get("attributes") or {}
        pid = a.get("LocalParcelID")
        if not pid:
            skipped_no_id += 1
            continue
        pid = str(pid).strip()
        if not pid:
            skipped_no_id += 1
            continue

        addr  = (a.get("MailAddressAll") or "").strip() or None
        city  = (a.get("MailCity")       or "").strip() or None
        zipc  = (a.get("MailZip")        or "").strip() or None
        # OGRIP's MailState is broken (returns ZIP3, not state abbrev) — derive.
        state = zip3_to_state(zipc)

        if addr is None and city is None and state is None and zipc is None:
            skipped_empty += 1
            continue

        rows.append((pid, addr, city, state, zipc))
    if skipped_no_id:
        print(f"[INFO]  Skipped {skipped_no_id:,} features with no LocalParcelID.")
    if skipped_empty:
        print(f"[INFO]  Skipped {skipped_empty:,} features with all-NULL mailing fields "
              f"(would not change DB).")
    return rows


def apply_updates(conn, county_uppercase: str, rows: list[tuple]) -> int:
    """Batch UPDATE parcels by (county, parcel_id). Uses VALUES list join
    pattern for one round-trip per batch."""
    if not rows:
        return 0

    updated = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            values_args = ",".join(
                cur.mogrify("(%s,%s,%s,%s,%s)", row).decode()
                for row in batch
            )
            sql = (
                "UPDATE parcels p "
                "SET owner_mailing_address = v.addr, "
                "    owner_mailing_city    = v.city, "
                "    owner_mailing_state   = v.state, "
                "    owner_mailing_zip     = v.zipc, "
                "    ingested_at           = now() "
                f"FROM (VALUES {values_args}) v(parcel_id, addr, city, state, zipc) "
                "WHERE p.county = %s AND p.parcel_id = v.parcel_id"
            )
            cur.execute(sql, (county_uppercase,))
            updated += cur.rowcount
    conn.commit()
    return updated


def report(conn, county_uppercase: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE owner_mailing_state IS NOT NULL)            AS with_state,
                COUNT(*) FILTER (WHERE owner_mailing_state IS NOT NULL
                                  AND owner_mailing_state <> 'OH')                 AS out_of_state,
                COUNT(DISTINCT owner_mailing_state)                                AS distinct_states
            FROM parcels
            WHERE county = %s
        """, (county_uppercase,))
        total, with_state, oos, distinct_states = cur.fetchone()
    print(f"        parcels in county   : {total:>6}")
    print(f"        with mailing state  : {with_state:>6}  ({100.0*with_state/total:.1f}%)" if total else "")
    print(f"        out-of-state owners : {oos:>6}  ({100.0*oos/total:.1f}%)" if total else "")
    print(f"        distinct states     : {distinct_states:>6}")


def run(county_uppercase: str, dry_run: bool):
    county_titlecase = county_uppercase.title()  # OGRIP uses 'Hocking' not 'HOCKING'
    conn = connect()
    start = time.time()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM parcels WHERE county = %s",
                        (county_uppercase,))
            existing = cur.fetchone()[0]
        if existing == 0:
            print(f"[ERROR] No existing parcels for county '{county_uppercase}'. "
                  f"Run ingest_parcels.py first to load the auditor data.")
            sys.exit(1)
        print(f"[INFO]  {existing:,} existing parcels in {county_uppercase}.")

        print(f"[INFO]  Fetching OGRIP parcels for County='{county_titlecase}' …")
        features = fetch_county(county_titlecase)
        print(f"[OK]    Fetched {len(features):,} features.")

        rows = build_updates(features)
        print(f"[INFO]  {len(rows):,} rows have non-null mailing data.")

        if dry_run:
            print("[DRY]   --dry-run set; not writing to DB.")
            print("[DRY]   Sample rows:")
            for r in rows[:5]:
                print(f"        {r}")
            return

        updated = apply_updates(conn, county_uppercase, rows)
        elapsed = time.time() - start
        print(f"[OK]    {county_uppercase}: {updated:,} parcels updated in {elapsed:.1f}s.")
        print()
        print("Post-update coverage:")
        report(conn, county_uppercase)
    finally:
        conn.close()
        print("[INFO]  Connection closed.")


if __name__ == "__main__":
    print()
    print("=== OGRIP Mailing-Address Enrichment ===")
    print()
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", required=True,
                        help="County name (uppercase, as stored in parcels.county / wells.county).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse but do not write to DB.")
    args = parser.parse_args()

    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    run(args.county.upper(), args.dry_run)
