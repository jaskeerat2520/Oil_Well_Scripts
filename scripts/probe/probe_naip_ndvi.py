"""
probe_naip_ndvi.py

Computes NDVI from USDA NAIP 4-band aerial imagery (R, G, B, real NIR) for
Hocking County wells, comparing the well-pad NDVI against immediately
adjacent vegetation. NAIP gives us *real* NIR — unlike OSIP which only
publishes RGB through the public service — so this is a true NDVI signal
at sub-meter resolution, directly comparable to (but ~10x sharper than)
the Sentinel-2 NDVI in well_surface_anomalies.

Output: probe_naip_ndvi_hocking.csv (no DB writes)

Usage
-----
    python probe_naip_ndvi.py                          # all wells from sample file
    python probe_naip_ndvi.py --apis-file sample.txt
    python probe_naip_ndvi.py --apis 34073600000000,34073600010000
"""

import os
import io
import csv
import math
import time
import argparse

import numpy as np
import requests
import psycopg2
import tifffile
from pyproj import Transformer
from dotenv import load_dotenv

load_dotenv()

# ── DB ────────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))

# ── NAIP service ─────────────────────────────────────────────────────────────
NAIP_URL = "https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer/exportImage"
NAIP_SR  = 3857           # Web Mercator
PIXEL_M_REQUEST = 1.0     # request 1 Mercator unit per pixel; ground res ≈ 0.77m at 40°N

# ── Analysis radii (ground meters) ───────────────────────────────────────────
INNER_M = 15
OUTER_M = 50

# NAIP standard band order (USDA): 0=R, 1=G, 2=B, 3=NIR
RED_BAND = 0
NIR_BAND = 3


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


# ── NAIP fetch + NDVI math ───────────────────────────────────────────────────
def merc_per_meter(lat: float) -> float:
    """At latitude `lat`, 1 ground meter = (1/cos(lat)) Web Mercator units."""
    return 1.0 / math.cos(math.radians(lat))


def fetch_naip_tile(x_m: float, y_m: float, half_m: float, mpm: float) -> np.ndarray:
    """Fetch 4-band U8 NAIP patch around (x_m, y_m) in Web Mercator. half_m is
    the desired half-side in *ground meters*; the request is sized in Mercator
    units using the latitude-corrected scale `mpm`."""
    half_merc = half_m * mpm
    bbox = f"{x_m - half_merc},{y_m - half_merc},{x_m + half_merc},{y_m + half_merc}"
    size_px = int(round((2 * half_merc) / PIXEL_M_REQUEST))
    params = {
        "bbox":    bbox,
        "bboxSR":  NAIP_SR,
        "imageSR": NAIP_SR,
        "size":    f"{size_px},{size_px}",
        "format":  "tiff",
        "bandIds": "0,1,2,3",
        "f":       "image",
    }
    r = requests.get(NAIP_URL, params=params, timeout=60)
    r.raise_for_status()
    arr = tifffile.imread(io.BytesIO(r.content))
    if arr.ndim == 3 and arr.shape[0] in (3, 4) and arr.shape[0] < arr.shape[-1]:
        arr = np.transpose(arr, (1, 2, 0))
    return arr


def compute_ndvi(arr: np.ndarray) -> np.ndarray:
    r = arr[..., RED_BAND].astype(np.float32)
    n = arr[..., NIR_BAND].astype(np.float32)
    denom = n + r
    return np.where(denom > 0, (n - r) / denom, np.nan)


def annular_means(field: np.ndarray, inner_r_px: float, outer_r_px: float):
    h, w = field.shape
    yy, xx = np.ogrid[:h, :w]
    cy, cx = (h - 1) / 2.0, (w - 1) / 2.0
    r2 = (xx - cx) ** 2 + (yy - cy) ** 2

    inner_mask = r2 <= inner_r_px ** 2
    outer_mask = (r2 <= outer_r_px ** 2) & ~inner_mask

    iv = field[inner_mask]; iv = iv[~np.isnan(iv)]
    ov = field[outer_mask]; ov = ov[~np.isnan(ov)]
    in_mean = float(iv.mean()) if iv.size else None
    bg_mean = float(ov.mean()) if ov.size else None
    fi = iv.size / max(int(inner_mask.sum()), 1)
    fo = ov.size / max(int(outer_mask.sum()), 1)
    return in_mean, bg_mean, fi, fo


def score_ndvi_contrast(in_n, bg_n):
    if in_n is None or bg_n is None:
        return None, 0
    delta = in_n - bg_n
    if   delta < -0.30: score = 100
    elif delta < -0.20: score = 70
    elif delta < -0.10: score = 40
    elif delta < -0.05: score = 15
    else:               score = 0
    return round(delta, 3), score


# ── Sample loading ───────────────────────────────────────────────────────────
def load_apis(apis_file: str | None, apis_arg: str | None) -> list[tuple[str, str]]:
    """Returns list of (api_no, stratum). stratum is empty string if not given."""
    if apis_arg:
        return [(a.strip(), "") for a in apis_arg.split(",") if a.strip()]
    if apis_file:
        items: list[tuple[str, str]] = []
        with open(apis_file) as f:
            for line in f:
                line = line.strip()
                if not line: continue
                parts = line.split(",", 1)
                items.append((parts[0], parts[1] if len(parts) > 1 else ""))
        return items
    raise SystemExit("Provide --apis or --apis-file")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apis-file", default="hocking_sample_apis.txt")
    parser.add_argument("--apis",       default=None)
    parser.add_argument("--out",        default="probe_naip_ndvi_hocking.csv")
    args = parser.parse_args()

    api_strata = load_apis(args.apis_file, args.apis)
    print(f"Loading {len(api_strata)} wells from DB…")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT w.api_no, w.lat, w.lng,
               wsa.baseline_ndvi, wsa.recent_ndvi, wsa.ndvi_relative
          FROM wells w
          LEFT JOIN well_surface_anomalies wsa USING (api_no)
         WHERE w.api_no = ANY(%s)
    """, ([a for a, _ in api_strata],))
    db_rows = {r[0]: r for r in cur.fetchall()}
    conn.close()

    transformer = Transformer.from_crs(4326, NAIP_SR, always_xy=True)

    out_rows = []
    for api_no, stratum in api_strata:
        if api_no not in db_rows:
            print(f"  {api_no}: not in DB"); continue
        _, lat, lng, s2_baseline, s2_recent, s2_rel = db_rows[api_no]
        if lat is None or lng is None:
            print(f"  {api_no}: no lat/lng"); continue

        x_m, y_m = transformer.transform(lng, lat)
        mpm = merc_per_meter(lat)
        # request size for outer radius
        outer_merc = OUTER_M * mpm
        inner_r_px = (INNER_M * mpm) / PIXEL_M_REQUEST
        outer_r_px = outer_merc / PIXEL_M_REQUEST

        try:
            arr = fetch_naip_tile(x_m, y_m, OUTER_M, mpm)
        except Exception as e:
            print(f"  {api_no}: FETCH ERROR {e}"); continue

        # Sanity: if returned image isn't 4-band, NAIP rendering may have collapsed it
        if arr.ndim != 3 or arr.shape[-1] < 4:
            print(f"  {api_no}: unexpected NAIP shape {arr.shape}"); continue

        ndvi = compute_ndvi(arr)
        in_n, bg_n, fi, fo = annular_means(ndvi, inner_r_px, outer_r_px)
        delta, score = score_ndvi_contrast(in_n, bg_n)

        out_rows.append({
            "api_no":            api_no,
            "stratum":           stratum,
            "lat":               lat,
            "lng":               lng,
            "s2_baseline_ndvi":  float(s2_baseline) if s2_baseline is not None else None,
            "s2_recent_ndvi":    float(s2_recent)   if s2_recent   is not None else None,
            "s2_ndvi_relative":  float(s2_rel)      if s2_rel      is not None else None,
            "naip_ndvi_pad":     round(in_n, 3) if in_n is not None else None,
            "naip_ndvi_bg":      round(bg_n, 3) if bg_n is not None else None,
            "naip_ndvi_delta":   delta,
            "naip_score":        score,
            "fill_inner":        round(fi, 3),
            "fill_outer":        round(fo, 3),
        })

        s2 = "    -" if s2_rel is None else f"{float(s2_rel):>+5.2f}"
        ipd = "    -" if in_n is None else f"{in_n:>+5.2f}"
        ibg = "    -" if bg_n is None else f"{bg_n:>+5.2f}"
        d   = "    -" if delta is None else f"{delta:>+5.2f}"
        print(f"  {api_no} [{stratum:<14}] S2 rel {s2}  ->  NAIP pad {ipd} bg {ibg} (d {d}) score {score:>3}")
        time.sleep(0.2)

    if not out_rows:
        print("\nNo successful results."); return

    fieldnames = list(out_rows[0].keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    pad_lower = sum(1 for r in out_rows
                    if r["naip_ndvi_delta"] is not None and r["naip_ndvi_delta"] < 0)
    sig       = sum(1 for r in out_rows
                    if r["naip_ndvi_delta"] is not None and r["naip_ndvi_delta"] < -0.10)
    strong    = sum(1 for r in out_rows
                    if r["naip_ndvi_delta"] is not None and r["naip_ndvi_delta"] < -0.20)

    print(f"\n  Wrote {len(out_rows)} rows -> {args.out}")
    print(f"  Pad less green than surroundings: {pad_lower}/{len(out_rows)}")
    print(f"  Significant disturbance (delta < -0.10): {sig}")
    print(f"  Strong disturbance     (delta < -0.20): {strong}")


if __name__ == "__main__":
    main()
