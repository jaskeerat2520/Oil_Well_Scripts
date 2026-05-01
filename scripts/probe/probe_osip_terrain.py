"""
probe_osip_terrain.py

Re-scores Hocking County wells using Ohio's OSIP 2.5ft (~0.76m) LiDAR DEM
instead of the USGS 3DEP 10m DEM that score_terrain.py currently uses, and
writes a side-by-side comparison CSV against the existing 3DEP-based scores
already in well_remote_sensing.

This is a validation probe — no DB writes. Output: probe_osip_hocking.csv

Usage
-----
    python probe_osip_terrain.py                  # 5-well smoke test
    python probe_osip_terrain.py --limit 30
    python probe_osip_terrain.py --limit 30 --order score_asc   # low-score wells
    python probe_osip_terrain.py --limit 30 --order random
"""

import os
import io
import csv
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

# ── OSIP DEM service ─────────────────────────────────────────────────────────
DEM_URL  = "https://maps.ohio.gov/image/rest/services/OSIPI_DEM/ImageServer/exportImage"
DEM_SR   = 3754              # NAD83(2011) Ohio South State Plane (ftUS)
PIXEL_FT = 2.5               # native pixel size of the DEM, in US Survey feet

# 1 metre = 3937/1200 US Survey feet  ≈  3.2808333335
M_TO_FTUS = 3937.0 / 1200.0

# ── Analysis params (mirror score_terrain.py) ────────────────────────────────
INNER_M  = 100
OUTER_M  = 400
INNER_FT = INNER_M * M_TO_FTUS
OUTER_FT = OUTER_M * M_TO_FTUS

FLAT_RATIO_THRESH = 0.4
MIN_BG_SLOPE_DEG  = 1.0

# OSIPI_DEM publishes a min value of ~-520 ftUS — that's a no-data sentinel
# (Ohio's actual lowest elevation is ~455 ft). Treat any value below -100 ft
# as nodata so we don't pollute slope statistics.
NODATA_THRESH = -100.0


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


# ── Scoring (identical to score_terrain.py for direct comparability) ─────────
def score_terrain(slope_well, slope_bg):
    if slope_well is None or slope_bg is None or slope_bg <= 0:
        return None, False, 0
    ratio = slope_well / slope_bg
    flat = (slope_bg > MIN_BG_SLOPE_DEG) and (ratio < FLAT_RATIO_THRESH)
    if slope_bg <= MIN_BG_SLOPE_DEG:
        return round(ratio, 3), False, 0
    if   ratio < 0.25: score = 100
    elif ratio < 0.40: score = 70
    elif ratio < 0.60: score = 40
    elif ratio < 0.80: score = 15
    else:              score = 0
    return round(ratio, 3), flat, score


# ── OSIP fetch + slope math ──────────────────────────────────────────────────
def fetch_dem_tile(x_ft: float, y_ft: float, half_ft: float) -> np.ndarray:
    """Pull a square DEM patch centered on (x_ft, y_ft) with half-side `half_ft`."""
    bbox = f"{x_ft - half_ft},{y_ft - half_ft},{x_ft + half_ft},{y_ft + half_ft}"
    size_px = int(round((2 * half_ft) / PIXEL_FT))
    params = {
        "bbox":      bbox,
        "bboxSR":    DEM_SR,
        "imageSR":   DEM_SR,
        "size":      f"{size_px},{size_px}",
        "format":    "tiff",
        "pixelType": "F32",
        "f":         "image",
    }
    r = requests.get(DEM_URL, params=params, timeout=60)
    r.raise_for_status()
    arr = tifffile.imread(io.BytesIO(r.content)).astype(np.float32)
    arr[arr < NODATA_THRESH] = np.nan
    return arr


def slope_degrees(elev_ft: np.ndarray, pixel_ft: float = PIXEL_FT) -> np.ndarray:
    """Per-pixel slope in degrees from the central-difference elevation gradient.
    Both elevation and pixel size are in feet, so units cancel and arctan gives degrees."""
    dy, dx = np.gradient(elev_ft, pixel_ft)
    rise_run = np.sqrt(dx ** 2 + dy ** 2)
    return np.degrees(np.arctan(rise_run))


def annular_means(elev_ft: np.ndarray, inner_r_px: float, outer_r_px: float):
    """Mean slope (deg) inside the inner disk and the outer annulus, plus
    coverage fraction in each (1.0 = no NaN pixels)."""
    slope = slope_degrees(elev_ft)
    h, w = slope.shape
    yy, xx = np.ogrid[:h, :w]
    cy, cx = (h - 1) / 2.0, (w - 1) / 2.0
    r2 = (xx - cx) ** 2 + (yy - cy) ** 2

    inner_mask = r2 <= inner_r_px ** 2
    outer_mask = (r2 <= outer_r_px ** 2) & ~inner_mask

    iv = slope[inner_mask]; iv = iv[~np.isnan(iv)]
    ov = slope[outer_mask]; ov = ov[~np.isnan(ov)]

    s_in = float(iv.mean()) if iv.size else None
    s_bg = float(ov.mean()) if ov.size else None
    fi = iv.size / max(int(inner_mask.sum()), 1)
    fo = ov.size / max(int(outer_mask.sum()), 1)
    return s_in, s_bg, fi, fo


# ── Driver ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5,
                        help="Wells to probe (default 5 for smoke test)")
    parser.add_argument("--out",   default="probe_osip_hocking.csv")
    parser.add_argument("--order", choices=["score_desc", "score_asc", "random"],
                        default="score_desc")
    args = parser.parse_args()

    print(f"Loading {args.limit} Hocking wells…")
    conn = get_conn()
    cur = conn.cursor()
    order_clause = {
        "score_desc": "wrs.terrain_risk_score DESC NULLS LAST, w.api_no",
        "score_asc":  "wrs.terrain_risk_score ASC NULLS LAST, w.api_no",
        "random":     "random()",
    }[args.order]
    cur.execute(f"""
        SELECT w.api_no, w.lat, w.lng,
               wrs.mean_slope_well, wrs.mean_slope_bg, wrs.slope_ratio,
               wrs.is_artificially_flat, wrs.terrain_risk_score
          FROM wells w
          JOIN well_remote_sensing wrs USING (api_no)
         WHERE UPPER(w.county) = 'HOCKING'
         ORDER BY {order_clause}
         LIMIT %s
    """, (args.limit,))
    rows = cur.fetchall()
    conn.close()
    print(f"  Got {len(rows)} wells.\n")

    transformer = Transformer.from_crs(4326, DEM_SR, always_xy=True)
    inner_r_px = INNER_FT / PIXEL_FT
    outer_r_px = OUTER_FT / PIXEL_FT

    out_rows = []
    for (api_no, lat, lng,
         s_well_3dep, s_bg_3dep, ratio_3dep, flat_3dep, score_3dep) in rows:
        x_ft, y_ft = transformer.transform(lng, lat)
        try:
            arr = fetch_dem_tile(x_ft, y_ft, OUTER_FT)
            s_in, s_bg, fi, fo = annular_means(arr, inner_r_px, outer_r_px)
        except Exception as e:
            print(f"  {api_no}: ERROR {e}")
            continue

        ratio_osip, flat_osip, score_osip = score_terrain(s_in, s_bg)

        out_rows.append({
            "api_no":          api_no,
            "lat":             lat,
            "lng":             lng,
            "slope_well_3dep": float(s_well_3dep) if s_well_3dep is not None else None,
            "slope_bg_3dep":   float(s_bg_3dep)   if s_bg_3dep   is not None else None,
            "ratio_3dep":      float(ratio_3dep)  if ratio_3dep  is not None else None,
            "flat_3dep":       flat_3dep,
            "score_3dep":      score_3dep,
            "slope_well_osip": round(s_in, 3) if s_in is not None else None,
            "slope_bg_osip":   round(s_bg, 3) if s_bg is not None else None,
            "ratio_osip":      ratio_osip,
            "flat_osip":       flat_osip,
            "score_osip":      score_osip,
            "fill_inner":      round(fi, 3),
            "fill_outer":      round(fo, 3),
        })

        f3 = "*" if flat_3dep else " "
        fO = "*" if flat_osip else " "
        s3 = "  -" if score_3dep is None else f"{score_3dep:>3}"
        sO = "  -" if score_osip is None else f"{score_osip:>3}"
        r3 = "    -" if ratio_3dep is None else f"{ratio_3dep:>5.2f}"
        rO = "    -" if ratio_osip is None else f"{ratio_osip:>5.2f}"
        print(f"  {api_no}  3DEP{f3}{s3} (ratio {r3})  ->  OSIP{fO}{sO} (ratio {rO})")
        time.sleep(0.2)

    if not out_rows:
        print("\nNo successful results.")
        return

    fieldnames = list(out_rows[0].keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    moves_up = sum(1 for r in out_rows
                   if (r["score_osip"] or 0) > (r["score_3dep"] or 0))
    moves_dn = sum(1 for r in out_rows
                   if (r["score_osip"] or 0) < (r["score_3dep"] or 0))
    same     = sum(1 for r in out_rows
                   if (r["score_osip"] or 0) == (r["score_3dep"] or 0))
    flat3    = sum(1 for r in out_rows if r["flat_3dep"])
    flatO    = sum(1 for r in out_rows if r["flat_osip"])

    print(f"\n  Wrote {len(out_rows)} rows -> {args.out}")
    print(f"  OSIP score higher: {moves_up}")
    print(f"  OSIP score lower:  {moves_dn}")
    print(f"  Same:              {same}")
    print(f"  Artificially-flat (3DEP / OSIP): {flat3} / {flatO}")


if __name__ == "__main__":
    main()
