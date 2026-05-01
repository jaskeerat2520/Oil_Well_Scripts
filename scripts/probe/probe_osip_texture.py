"""
probe_osip_texture.py

Pad-detection probe using OSIP RGB aerial imagery (no NIR — that band isn't
exposed by the public service). Two signals:

  1. VARI = (G - R) / (G + R - B)  — visible-band vegetation index. Bounded
     to roughly [-1, +1]; higher = more vegetation. Negative pad-vs-bg delta
     suggests a cleared/disturbed pad.

  2. Sobel edge magnitude — captures geometric texture. Drilling pads tend to
     have sharp linear boundaries (gravel meets forest); the annular Sobel
     mean compares pad interior smoothness vs the surroundings.

Output: probe_osip_texture_hocking.csv (no DB writes)
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

# ── OSIP imagery ──────────────────────────────────────────────────────────────
IMG_URL = "https://maps.ohio.gov/image/rest/services/osip_most_current/ImageServer/exportImage"
IMG_SR  = 3753
PIXEL_FT_REQUEST = 1.0
M_TO_FTUS = 3937.0 / 1200.0

INNER_M  = 15
OUTER_M  = 50
INNER_FT = INNER_M * M_TO_FTUS
OUTER_FT = OUTER_M * M_TO_FTUS


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def fetch_aerial(x_ft: float, y_ft: float, half_ft: float) -> np.ndarray:
    bbox = f"{x_ft - half_ft},{y_ft - half_ft},{x_ft + half_ft},{y_ft + half_ft}"
    size_px = int(round((2 * half_ft) / PIXEL_FT_REQUEST))
    params = {
        "bbox":    bbox,
        "bboxSR":  IMG_SR,
        "imageSR": IMG_SR,
        "size":    f"{size_px},{size_px}",
        "format":  "tiff",
        "bandIds": "0,1,2",     # request only the real RGB; band 3 is a duplicate
        "f":       "image",
    }
    r = requests.get(IMG_URL, params=params, timeout=60)
    r.raise_for_status()
    arr = tifffile.imread(io.BytesIO(r.content))
    if arr.ndim == 3 and arr.shape[0] in (3, 4) and arr.shape[0] < arr.shape[-1]:
        arr = np.transpose(arr, (1, 2, 0))
    return arr


def compute_vari(arr: np.ndarray) -> np.ndarray:
    """VARI = (G - R) / (G + R - B), clipped to [-1.5, 1.5] to suppress outliers."""
    r = arr[..., 0].astype(np.float32)
    g = arr[..., 1].astype(np.float32)
    b = arr[..., 2].astype(np.float32)
    denom = g + r - b
    out = np.where(np.abs(denom) > 1e-3, (g - r) / denom, np.nan)
    return np.clip(out, -1.5, 1.5)


def sobel_magnitude(gray: np.ndarray) -> np.ndarray:
    """Per-pixel Sobel gradient magnitude. Returns same-shape float array;
    boundary pixels are zero. Uses pure numpy slicing — no scipy dependency."""
    g = gray.astype(np.float32)
    sx = ((g[:-2, 2:] + 2 * g[1:-1, 2:] + g[2:, 2:])
        - (g[:-2, :-2] + 2 * g[1:-1, :-2] + g[2:, :-2]))
    sy = ((g[2:, :-2] + 2 * g[2:, 1:-1] + g[2:, 2:])
        - (g[:-2, :-2] + 2 * g[:-2, 1:-1] + g[:-2, 2:]))
    mag = np.sqrt(sx * sx + sy * sy)
    out = np.zeros_like(g, dtype=np.float32)
    out[1:-1, 1:-1] = mag
    return out


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
    return in_mean, bg_mean


def score_combined(vari_delta, edge_inner, edge_bg):
    """Heuristic combined pad score 0-100. Two contributions:
      - VARI delta: pad less green = signal
      - Edge contrast: pad interior much smoother OR much rougher than bg
    Equal weighting, capped at 100. Will need calibration after we see the
    distribution; for now this gives us a single number per well to rank by."""
    score = 0.0
    if vari_delta is not None:
        if   vari_delta < -0.30: score += 60
        elif vari_delta < -0.20: score += 40
        elif vari_delta < -0.10: score += 25
        elif vari_delta < -0.05: score += 10
    if edge_inner is not None and edge_bg is not None and edge_bg > 0:
        ratio = edge_inner / edge_bg
        # Pads are typically smoother (ratio < 0.5) OR have prominent edges (ratio > 1.5)
        if   ratio < 0.4 or ratio > 2.0: score += 40
        elif ratio < 0.6 or ratio > 1.5: score += 25
        elif ratio < 0.8 or ratio > 1.2: score += 10
    return min(100, int(score))


def load_apis(apis_file: str | None, apis_arg: str | None) -> list[tuple[str, str]]:
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
    parser.add_argument("--out",        default="probe_osip_texture_hocking.csv")
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

    transformer = Transformer.from_crs(4326, IMG_SR, always_xy=True)
    inner_r_px = INNER_FT / PIXEL_FT_REQUEST
    outer_r_px = OUTER_FT / PIXEL_FT_REQUEST

    out_rows = []
    for api_no, stratum in api_strata:
        if api_no not in db_rows:
            print(f"  {api_no}: not in DB"); continue
        _, lat, lng, s2_baseline, s2_recent, s2_rel = db_rows[api_no]
        if lat is None or lng is None:
            print(f"  {api_no}: no lat/lng"); continue

        x_ft, y_ft = transformer.transform(lng, lat)
        try:
            arr = fetch_aerial(x_ft, y_ft, OUTER_FT)
        except Exception as e:
            print(f"  {api_no}: FETCH ERROR {e}"); continue

        vari = compute_vari(arr)
        gray = arr.mean(axis=-1)
        edges = sobel_magnitude(gray)

        vari_in, vari_bg = annular_means(vari,  inner_r_px, outer_r_px)
        edge_in, edge_bg = annular_means(edges, inner_r_px, outer_r_px)

        vari_delta = (vari_in - vari_bg) if (vari_in is not None and vari_bg is not None) else None
        edge_ratio = (edge_in / edge_bg) if (edge_in is not None and edge_bg is not None and edge_bg > 0) else None
        score = score_combined(vari_delta, edge_in, edge_bg)

        out_rows.append({
            "api_no":            api_no,
            "stratum":           stratum,
            "lat":               lat,
            "lng":               lng,
            "s2_recent_ndvi":    float(s2_recent) if s2_recent is not None else None,
            "s2_ndvi_relative":  float(s2_rel)    if s2_rel    is not None else None,
            "vari_pad":          round(vari_in, 3) if vari_in is not None else None,
            "vari_bg":           round(vari_bg, 3) if vari_bg is not None else None,
            "vari_delta":        round(vari_delta, 3) if vari_delta is not None else None,
            "edge_pad":          round(edge_in, 2) if edge_in is not None else None,
            "edge_bg":           round(edge_bg, 2) if edge_bg is not None else None,
            "edge_ratio":        round(edge_ratio, 3) if edge_ratio is not None else None,
            "texture_score":     score,
        })

        vd = "    -" if vari_delta is None else f"{vari_delta:>+5.2f}"
        er = "    -" if edge_ratio is None else f"{edge_ratio:>5.2f}"
        s2 = "    -" if s2_rel is None else f"{float(s2_rel):>+5.2f}"
        print(f"  {api_no} [{stratum:<14}] S2 rel {s2}  ->  VARI d {vd}  edge ratio {er}  score {score:>3}")
        time.sleep(0.2)

    if not out_rows:
        print("\nNo successful results."); return

    fieldnames = list(out_rows[0].keys())
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    sig_vari   = sum(1 for r in out_rows
                     if r["vari_delta"] is not None and r["vari_delta"] < -0.10)
    sig_edge   = sum(1 for r in out_rows
                     if r["edge_ratio"] is not None and (r["edge_ratio"] < 0.6 or r["edge_ratio"] > 1.5))
    flagged    = sum(1 for r in out_rows if r["texture_score"] >= 40)

    print(f"\n  Wrote {len(out_rows)} rows -> {args.out}")
    print(f"  Significant VARI delta (< -0.10):   {sig_vari}")
    print(f"  Significant edge contrast (ratio):  {sig_edge}")
    print(f"  Texture score >= 40 (likely pad):   {flagged}")


if __name__ == "__main__":
    main()
