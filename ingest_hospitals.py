"""
Ingest Ohio hospitals from the ODH Hospital Registration Information CSV.

Source CSV: Hospital_Registration_Information.csv (one row per
  hospital × beds_category × report_year). We collapse to one row per
  hospital_number, keeping only report_year=2023 and registration_status =
  'Approved by ODH'. Bed counts are summed across all beds_category rows
  for the same hospital so 'registered_beds' reflects total facility size.

Geocoding: the CSV has no lat/lng. We use the US Census Bureau geocoder
(`geocoding.geo.census.gov`) — free, no API key, very accurate for US
addresses. Failed addresses are recorded with NULL geometry so we can
hand-fix them later.

Usage:
    python ingest_hospitals.py
    python ingest_hospitals.py --truncate   # full refresh
    python ingest_hospitals.py --year 2022  # different reporting year
"""

import argparse
import csv
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

CSV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "Hospital_Registration_Information.csv",
)

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
GEOCODER_BENCHMARK = "Public_AR_Current"
GEOCODE_SLEEP_S = 0.4  # politeness; Census geocoder doesn't publish a hard rate but 2-3 req/s is comfortable


def connect():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )


def to_int(s) -> int | None:
    if s is None:
        return None
    s = str(s).strip()
    if s == "" or s.upper() == "NULL":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def clean(s) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    if s == "" or s.upper() == "NULL":
        return None
    return s


def collapse_rows(rows: list[dict]) -> dict:
    """Collapse N rows for the same hospital_number into one record. Identity
    fields come from the first row; bed counts sum across all rows."""
    out = dict(rows[0])
    bed_total = 0
    for r in rows:
        n = to_int(r.get("registered_beds"))
        if n is not None:
            bed_total += n
    out["registered_beds_summed"] = bed_total if bed_total > 0 else None
    return out


def geocode_address(address: str, city: str, state: str, zip_code: str) -> tuple[float | None, float | None]:
    """Single-address Census geocoder call. Returns (lng, lat) or (None, None)."""
    one_line = ", ".join(filter(None, [address, city, state, zip_code]))
    try:
        resp = requests.get(
            GEOCODER_URL,
            params={
                "address":   one_line,
                "benchmark": GEOCODER_BENCHMARK,
                "format":    "json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        body = resp.json()
        matches = body.get("result", {}).get("addressMatches", []) or []
        if not matches:
            return None, None
        c = matches[0].get("coordinates") or {}
        return c.get("x"), c.get("y")
    except (requests.RequestException, ValueError) as e:
        print(f"  ! geocode failed for {one_line!r}: {e}", flush=True)
        return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default="2023",
                        help="Reporting year to ingest (default 2023).")
    parser.add_argument("--truncate", action="store_true",
                        help="DELETE FROM hospitals before ingest.")
    args = parser.parse_args()

    print("=== Ohio Hospitals Ingest (ODH Hospital Registration) ===\n")
    print(f"[INFO] Reading {CSV_PATH}")
    with open(CSV_PATH, encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))
    print(f"[INFO] Total CSV rows: {len(all_rows):,}")

    eligible = [
        r for r in all_rows
        if r.get("report_year") == args.year
        and r.get("registration_status") == "Approved by ODH"
    ]
    print(f"[INFO] After filter (year={args.year}, Approved by ODH): {len(eligible):,} rows")

    by_hospital: dict[str, list[dict]] = defaultdict(list)
    for r in eligible:
        hn = clean(r.get("hospital_number"))
        if hn:
            by_hospital[hn].append(r)
    print(f"[INFO] Unique hospitals: {len(by_hospital):,}")

    conn = connect()
    try:
        if args.truncate:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM hospitals")
            conn.commit()
            print("[INFO] Truncated hospitals table.")

        inserted = 0
        geocoded = 0
        ungeocoded = 0
        t_start = time.time()

        for i, (hn, rows) in enumerate(sorted(by_hospital.items()), 1):
            rec = collapse_rows(rows)
            name = clean(rec.get("hospital_dba_name"))
            if not name:
                ungeocoded += 1
                continue

            address = clean(rec.get("address"))
            city    = clean(rec.get("city"))
            state   = clean(rec.get("state")) or "OH"
            zipc    = clean(rec.get("zip_code"))

            lng, lat = (None, None)
            if address and city:
                lng, lat = geocode_address(address, city, state, zipc or "")
                time.sleep(GEOCODE_SLEEP_S)
            geocode_source = "census" if lng is not None else None
            if lng is not None:
                geocoded += 1
            else:
                ungeocoded += 1

            beds_summed = rec.get("registered_beds_summed")
            beds_summed = beds_summed if isinstance(beds_summed, int) else None

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO hospitals (
                        hospital_number, name, address, city, zip, county,
                        corporate_phone, medicare_classification, service_category,
                        trauma_level_adult, trauma_level_pediatric,
                        emergency_services_type, registered_beds,
                        raw_attrs, geometry, geocode_source
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s::jsonb,
                        CASE WHEN %s::double precision IS NULL THEN NULL
                             ELSE ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                        END,
                        %s
                    )
                    ON CONFLICT (hospital_number) DO UPDATE SET
                        name                    = EXCLUDED.name,
                        address                 = EXCLUDED.address,
                        city                    = EXCLUDED.city,
                        zip                     = EXCLUDED.zip,
                        county                  = EXCLUDED.county,
                        corporate_phone         = EXCLUDED.corporate_phone,
                        medicare_classification = EXCLUDED.medicare_classification,
                        service_category        = EXCLUDED.service_category,
                        trauma_level_adult      = EXCLUDED.trauma_level_adult,
                        trauma_level_pediatric  = EXCLUDED.trauma_level_pediatric,
                        emergency_services_type = EXCLUDED.emergency_services_type,
                        registered_beds         = EXCLUDED.registered_beds,
                        raw_attrs               = EXCLUDED.raw_attrs,
                        geometry                = EXCLUDED.geometry,
                        geocode_source          = EXCLUDED.geocode_source,
                        ingested_at             = NOW()
                    """,
                    (
                        hn, name, address, city, zipc, clean(rec.get("county")),
                        clean(rec.get("corporate_phone")),
                        clean(rec.get("medicare_classification")),
                        clean(rec.get("category_best_describing_hospital_services")),
                        clean(rec.get("trauma_level_adult")),
                        clean(rec.get("trauma_level_pediatric")),
                        clean(rec.get("emergency_services_type")),
                        beds_summed,
                        json.dumps({k: v for k, v in rec.items() if v not in (None, "")}),
                        lng, lng, lat,
                        geocode_source,
                    ),
                )
            conn.commit()
            inserted += 1

            if i % 25 == 0:
                rate = i / (time.time() - t_start)
                eta = (len(by_hospital) - i) / rate if rate > 0 else 0
                print(f"  [{i:>3}/{len(by_hospital)}] geocoded={geocoded} ungeocoded={ungeocoded} "
                      f"({rate:.1f}/s, ETA {eta:.0f}s)", flush=True)

        elapsed = time.time() - t_start

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hospitals")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM hospitals WHERE geometry IS NOT NULL")
            with_geom = cur.fetchone()[0]

        print()
        print("─" * 55)
        print(f"[DONE] Ingest complete in {elapsed:.0f}s.")
        print(f"       Inserted   : {inserted:>5,}")
        print(f"       Geocoded   : {geocoded:>5,}")
        print(f"       Ungeocoded : {ungeocoded:>5,}")
        print(f"       hospitals  : {total:>5,} rows ({with_geom:,} with geometry)")
        print("─" * 55)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
