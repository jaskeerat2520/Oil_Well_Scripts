"""
Convert PA DEP `OilGasLocations_ConventionalUnconventional2026_04.geojson`
into line-delimited GeoJSON (NDGeoJSON) suitable for the Mapbox Tiling
Service (MTS).

Why this script exists:
  - The source file declares `crs.name = "EPSG:3857"` and stores geometry
    as Web Mercator metres. MTS / tippecanoe require WGS84 (EPSG:4326)
    lon/lat. Each feature also carries WGS84 `LATITUDE` / `LONGITUDE`
    properties, so we rebuild geometry from those — no pyproj dependency.
  - The DBF-truncated property names (`WELL_STATU`, `MUNICIPA_1`, etc.) are
    cleaned to readable lowercase keys.
  - Empty-string sentinels (" ") are normalised to null so MTS doesn't
    waste tile bytes encoding them.
  - The 216 MB file is streamed via `ijson.items(...)` so peak RAM stays
    bounded.

Usage:
    python scripts/prep_oilgas_ldgeojson.py \\
        OilGasLocations_ConventionalUnconventional2026_04.geojson \\
        tile-cache/pa_oilgas.ndjson
"""

import argparse
import json
import sys
import time
from pathlib import Path

import ijson

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass


# Source DBF-style key  ->  cleaned NDGeoJSON key.  Order matters only for
# human-readability of the sample output; MTS doesn't care.
PROPERTY_MAP = {
    "PERMIT_NUM":  "permit_num",
    "WELL_NAME":   "well_name",
    "OPERATOR":    "operator",
    "WELL_TYPE":   "well_type",
    "WELL_STATU":  "well_status",
    "COUNTY":      "county",
    "MUNICIPALI":  "municipality",
    "UNCONVENTI":  "unconventional",
    "COAL_IND":    "coal_ind",
    "WELL_CONFI":  "well_config",
    "PERMIT_DAT":  "permit_date_ms",
    "SPUD_DATE":   "spud_date_ms",
    "DATE_PLUGG":  "plug_date_ms",
    "SITE_ID":     "site_id",
}


def clean_value(v):
    """Normalise empty-string sentinels to null. Pass everything else through."""
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", help="Path to the source FeatureCollection .geojson")
    ap.add_argument("output", help="Path to write NDGeoJSON")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        print(f"[ERROR] {in_path} not found", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO]  Streaming {in_path} ({in_path.stat().st_size / 1_048_576:.0f} MB) -> {out_path}")
    t0 = time.time()
    n_written = 0
    n_skipped_no_coords = 0

    with in_path.open("rb") as fin, out_path.open("w", encoding="utf-8") as fout:
        for feature in ijson.items(fin, "features.item"):
            props = feature.get("properties", {}) or {}
            lat = props.get("LATITUDE")
            lon = props.get("LONGITUDE")
            # Reject features without WGS84 coordinates rather than fall back
            # to the EPSG:3857 geometry — that would silently re-introduce the
            # bug this script exists to prevent.
            if lat is None or lon is None:
                n_skipped_no_coords += 1
                continue

            cleaned = {
                clean_key: clean_value(props.get(raw_key))
                for raw_key, clean_key in PROPERTY_MAP.items()
            }

            out_feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                "properties": cleaned,
            }
            fout.write(json.dumps(out_feature, separators=(",", ":")))
            fout.write("\n")
            n_written += 1
            if n_written % 25_000 == 0:
                rate = n_written / (time.time() - t0)
                print(f"[INFO]  {n_written:>7,} features written ({rate:,.0f}/s)")

    elapsed = time.time() - t0
    size_mb = out_path.stat().st_size / 1_048_576
    print(
        f"[OK]    {n_written:,} features -> {out_path} "
        f"({size_mb:.1f} MB, {elapsed:.0f}s; "
        f"skipped {n_skipped_no_coords:,} without LAT/LONG)"
    )


if __name__ == "__main__":
    main()
