"""
probe_osip_ndvi.py

Computes pseudo-NDVI from Ohio's OSIP 4-band aerial imagery (RGB + NIR) for
Hocking County wells, comparing the well-pad NDVI against immediately
adjacent vegetation. The pad-vs-background delta is a candidate new dimension
that complements the existing Sentinel-2 vegetation score in
well_surface_anomalies — Sentinel-2 averages a ~100m area at 10m pixels;
this probe focuses on a ~30m well-pad footprint at sub-meter resolution.

Output: probe_osip_ndvi_hocking.csv (no DB writes)

Usage
-----
    python probe_osip_ndvi.py                  # 5-well smoke test
    python probe_osip_ndvi.py --limit 30
    python probe_osip_ndvi.py --limit 30 --order score_desc
    python probe_osip_ndvi.py --limit 30 --order random
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

# ── OSIP imagery service ─────────────────────────────────────────────────────
IMG_URL = "https://maps.ohio.gov/image/rest/services/osip_most_current/ImageServer/exportImage"
IMG_SR  = 3753              # NAD83(2011) Ohio North State Plane (ftUS) — service native SR
PIXEL_FT_REQUEST = 1.0      # request resolution; native is ~0.08 ft, 1 ft is plenty

# 1 metre = 3937/1200 US Survey feet  ≈  3.2808333335
M_TO_FTUS = 3937.0 / 1200.0

# ── Analysis radii ────────────────────────────────────────────────────────────
INNER_M  = 15               # well-pad center; typical Ohio drilling pad is ~30m wide
OUTER_M  = 50               # immediate vegetation context
INNER_FT = INNER_M * M_TO_FTUS
OUTER_FT = OUTER_M * M_TO_FTUS

# OSIP imagery is U8 4-band; bands ordered R, G, B, NIR (verified against the
# service's band names downstream — if NDVI looks inverted, swap RED/NIR indices)
RED_BAND = 0
NIR_BAND = 3


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


# ── OSIP fetch + NDVI math ───────────────────────────────────────────────────
def fetch_aerial_tile(x_ft: float, y_ft: float, half_ft: float) -> np.ndarray:
    """Fetch a 4-band U8 patch centered at (x_ft, y_ft). Returns array (h, w, 4)."""
    bbox = f"{x_ft - half_ft},{y_ft - half_ft},{x_ft + half_ft},{y_ft + half_ft}"
    size_px = int(round((2 * half_ft) / PIXEL_FT_REQUEST))
    params = {
        "bbox":    bbox,
        "bboxSR":  IMG_SR,
        "imageSR": IMG_SR,
        "size":    f"{size_px},{size_px}",
        "format":  "tiff",
        "f":       "image",
    }
    r = requests.get(IMG_URL, params=params, timeout=60)
    r.raise_for_status()
    arr = tifffile.imread(io.BytesIO(r.content))
    # tifffile may return (bands, h, w) (planar) or (h, w, bands) (chunky).
    # Normalise to (h, w, bands).
    if arr.ndim == 3 and arr.shape[0] in (3, 4) and arr.shape[0] < arr.shape[-1]:
        arr = np.transpose(arr, (1, 2, 0))
    return arr


def compute_ndvi(arr: np.ndarray) -> np.ndarray:
    """Pseudo-NDVI from U8 4-band aerial. (NIR - R) / (NIR + R)."""
    r = arr[..., RED_BAND].astype(np.float32)
    n = arr[..., NIR_BAND].astype(np.float32)
    denom = n + r
    return np.where(denom > 0, (n - r) / denom, np.nan)


def annular_means(ndvi: np.ndarray, inner_r_px: float, outer_r_px: float):
    """Returns (mean_inner, mean_outer_annulus, fill_inner, fill_outer)."""
    h, w = ndvi.shape
    yy, xx = np.ogrid[:h, :w]
    cy, cx = (h - 1) / 2.0, (w - 1) / 2.0
    r2 = (xx - cx) ** 2 + (yy - cy) ** 2

    inner_mask = r2 <= inner_r_px ** 2
    outer_mask = (r2 <= outer_r_px ** 2) & ~inner_mask

    iv = ndvi[inner_mask]; iv = iv[~np.isnan(iv)]
    ov = ndvi[outer_mask]; ov = ov[~np.isnan(ov)]
    in_mean = float(iv.mean()) if iv.size else None
    bg_mean = float(ov.mean()) if ov.size else None
    fi = iv.size / max(int(inner_mask.sum()), 1)
    fo = ov.size / max(int(outer_mask.sum()), 1)
    return in_mean, bg_mean, fi, fo


def score_ndvi_contrast(in_ndvi, bg_ndvi):
    """Returns (delta, score). Score 0-100 reflects how much greener the
    surroundings are vs the pad. Negative delta = pad less vegetated than
    surroundings = surface disturbance signal.

    Bins are heuristic; will need calibration once we see the distribution
    across the full Hocking sample.
    """
    if in_ndvi is None or bg_ndvi is None:
        return None, 0
    delta = in_ndvi - bg_ndvi
    if   delta < -0.30: score = 100
    elif delta < -0.20: score = 70
    elif delta < -0.10: score = 40
    elif delta < -0.05: score = 15
    else:               score = 0
    return round(delta, 3), score


# ── Driver ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--out",   default="probe_osip_ndvi_hocking.csv")
    parser.add_argument("--order", choices=["score_desc", "score_asc", "random"],
                        default="score_desc")
    args = parser.parse_args()

    print(f"Loading {args.limit} Hocking wells...")
    conn = get_conn()
    cur = conn.cursor()
    order_clause = {
        "score_desc": "wrs.vegetation_risk_score DESC NULLS LAST, w.api_no",
        "score_asc":  "wrs.vegetation_risk_score ASC NULLS LAST, w.api_no",
        "random":     "random()",
    }[args.order]
    cur.execute(f"""
        SELECT w.api_no, w.lat, w.lng,
               wsa.baseline_ndvi, wsa.recent_ndvi,
               wsa.ndvi_change,   wsa.ndvi_relative,
               wsa.ndvi_trend_slope,
               wrs.vegetation_risk_score
          FROM wells w
          LEFT JOIN well_surface_anomalies wsa USING (api_no)
          LEFT JOIN well_risk_scores      wrs USING (api_no)
         WHERE UPPER(w.county) = 'HOCKING'
           AND w.lat IS NOT NULL AND w.lng IS NOT NULL
         ORDER BY {order_clause}
         LIMIT %s
    """, (args.limit,))
    rows = cur.fetchall()
    conn.close()
    print(f"  Got {len(rows)} wells.\n")

    transformer = Transformer.from_crs(4326, IMG_SR, always_xy=True)
    inner_r_px = INNER_FT / PIXEL_FT_REQUEST
    outer_r_px = OUTER_FT / PIXEL_FT_REQUEST

    out_rows = []
    for (api_no, lat, lng,
         s2_baseline, s2_recent, s2_change, s2_rel, s2_trend, s2_score) in rows:
        x_ft, y_ft = transformer.transform(lng, lat)
        try:
            arr = fetch_aerial_tile(x_ft, y_ft, OUTER_FT)
            ndvi = compute_ndvi(arr)
            in_n, bg_n, fi, fo = annular_means(ndvi, inner_r_px, outer_r_px)
        except Exception as e:
            print(f"  {api_no}: ERROR {e}")
            continue

        delta, score_osip = score_ndvi_contrast(in_n, bg_n)
        s2_score_int = int(s2_score) if s2_score is not None else None
        out_rows.append({
            "api_no":           api_no,
            "lat":              lat,
            "lng":              lng,
            "s2_baseline_ndvi": float(s2_baseline) if s2_baseline is not None else None,
            "s2_recent_ndvi":   float(s2_recent)   if s2_recent   is not None else None,
            "s2_ndvi_change":   float(s2_change)   if s2_change   is not None else None,
            "s2_ndvi_relative": float(s2_rel)      if s2_rel      is not None else None,
            "s2_ndvi_trend":    float(s2_trend)    if s2_trend    is not None else None,
            "s2_veg_score":     s2_score_int,
            "osip_ndvi_pad":    round(in_n, 3) if in_n is not None else None,
            "osip_ndvi_bg":     round(bg_n, 3) if bg_n is not None else None,
            "osip_ndvi_delta":  delta,
            "osip_score":       score_osip,
            "fill_inner":       round(fi, 3),
            "fill_outer":       round(fo, 3),
        })

        s2 = "  -" if s2_score_int is None else f"{s2_score_int:>3}"
        oo = f"{score_osip:>3}"
        d  = "    -" if delta is None else f"{delta:>+5.2f}"
        ipd = "    -" if in_n is None else f"{in_n:>+5.2f}"
        ibg = "    -" if bg_n is None else f"{bg_n:>+5.2f}"
        print(f"  {api_no}  S2 veg {s2}  ->  OSIP pad {ipd} bg {ibg} (d {d}) score {oo}")
        time.sleep(0.2)

    if not out_rows:
        print("\nNo successful results.")
        return

    fieldnames = list(out_rows[0].keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    pad_lower  = sum(1 for r in out_rows
                     if r["osip_ndvi_delta"] is not None and r["osip_ndvi_delta"] <  0)
    pad_higher = sum(1 for r in out_rows
                     if r["osip_ndvi_delta"] is not None and r["osip_ndvi_delta"] >  0)
    sig_dist   = sum(1 for r in out_rows
                     if r["osip_ndvi_delta"] is not None and r["osip_ndvi_delta"] < -0.10)

    print(f"\n  Wrote {len(out_rows)} rows -> {args.out}")
    print(f"  Pad less green than surroundings: {pad_lower}")
    print(f"  Pad more green than surroundings: {pad_higher}")
    print(f"  Significant disturbance (delta < -0.10): {sig_dist}")


if __name__ == "__main__":
    main()
