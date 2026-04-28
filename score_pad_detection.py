"""
score_pad_detection.py

Sub-meter pad-detection scoring using NAIP 4-band aerial (NDVI absolute +
delta) and OSIP RGB aerial (Sobel edge ratio). Combines three independent
signal pathways so wells in any of {pad-in-vegetation, pad-in-clearing,
sharp-edged pad} get surfaced.

Scoring (max 80, threshold "likely pad" at 30)
----------------------------------------------
  abs_signal   ← NAIP pad NDVI absolute, tiered:  <0.10 → 30,  <0.20 → 15,  <0.30 → 5
  delta_signal ← NAIP pad-vs-bg NDVI delta:       <-0.10 → 30, <-0.05 → 15
  edge_signal  ← OSIP Sobel edge ratio (pad/bg):  >1.50 → 20,  >1.20 → 10
  pad_score    = min(80, abs + delta + edge)

Validation history (2026-04-27)
-------------------------------
  Hocking  (forest):          combined >=30 caught 7/30 sample (4 hidden by old delta-only)
  Carroll  (shale):           combined >=30 caught 23/30, including 9/10 multi-bore Utica pads
  Hancock  (NW Ohio cropland): see merge_probes.py output for that county

Different counties light up different pathways — absolute NDVI is the
universal workhorse, edge fires in shale country, delta fires when a small
pad sits inside dense vegetation.

Usage
-----
    python score_pad_detection.py                    # all unscored wells
    python score_pad_detection.py --county HANCOCK
    python score_pad_detection.py --county HANCOCK --reprocess
    python score_pad_detection.py --county HANCOCK --limit 100
"""

import os
import io
import math
import time
import argparse
import psycopg2
import psycopg2.extras
import numpy as np
import requests
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

# ── NAIP service (Web Mercator, 4-band U8 with real NIR) ─────────────────────
NAIP_URL = "https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer/exportImage"
NAIP_SR  = 3857
NAIP_PIXEL_REQUEST = 1.0   # 1 Mercator unit per pixel ≈ 0.77m at 40°N

# ── OSIP service (Ohio South State Plane ftUS, 3-band RGB) ───────────────────
OSIP_URL = "https://maps.ohio.gov/image/rest/services/osip_most_current/ImageServer/exportImage"
OSIP_SR  = 3753
OSIP_PIXEL_FT = 1.0
M_TO_FTUS = 3937.0 / 1200.0

# ── Analysis radii (ground meters) ───────────────────────────────────────────
INNER_M = 15
OUTER_M = 50

# ── Network politeness ────────────────────────────────────────────────────────
SLEEP_S = 0.25
HTTP_TIMEOUT = 60


# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=30,
    )


def get_counties(cur, county_filter: str | None) -> list[str]:
    sql = """
        SELECT DISTINCT w.county
        FROM wells w
        JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
        LEFT JOIN well_pad_detection wpd ON w.api_no = wpd.api_no
        WHERE w.lat IS NOT NULL AND w.lng IS NOT NULL
          AND wpd.pad_processed_at IS NULL
    """
    params: list = []
    if county_filter:
        sql += " AND UPPER(w.county) = %s"
        params.append(county_filter.upper())
    sql += " ORDER BY w.county"
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


def get_wells_for_county(cur, county: str, limit: int | None) -> list[dict]:
    sql = """
        SELECT w.api_no, w.lat, w.lng
          FROM wells w
          JOIN well_risk_scores wrs ON w.api_no = wrs.api_no
          LEFT JOIN well_pad_detection wpd ON w.api_no = wpd.api_no
         WHERE w.county = %s
           AND w.lat IS NOT NULL AND w.lng IS NOT NULL
           AND wpd.pad_processed_at IS NULL
         ORDER BY wrs.risk_score DESC NULLS LAST
    """
    if limit:
        sql += " LIMIT %s"
        cur.execute(sql, (county, limit))
    else:
        cur.execute(sql, (county,))
    return [{"api_no": r[0], "lat": r[1], "lng": r[2]} for r in cur.fetchall()]


# ── NAIP fetch + NDVI ─────────────────────────────────────────────────────────
def merc_per_meter(lat: float) -> float:
    return 1.0 / math.cos(math.radians(lat))


def fetch_naip(x_m: float, y_m: float, half_m: float, mpm: float) -> np.ndarray:
    half_merc = half_m * mpm
    bbox = f"{x_m - half_merc},{y_m - half_merc},{x_m + half_merc},{y_m + half_merc}"
    size_px = int(round((2 * half_merc) / NAIP_PIXEL_REQUEST))
    params = {
        "bbox": bbox, "bboxSR": NAIP_SR, "imageSR": NAIP_SR,
        "size": f"{size_px},{size_px}", "format": "tiff",
        "bandIds": "0,1,2,3", "f": "image",
    }
    r = requests.get(NAIP_URL, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    arr = tifffile.imread(io.BytesIO(r.content))
    if arr.ndim == 3 and arr.shape[0] in (3, 4) and arr.shape[0] < arr.shape[-1]:
        arr = np.transpose(arr, (1, 2, 0))
    return arr


def naip_ndvi(arr: np.ndarray) -> np.ndarray:
    r = arr[..., 0].astype(np.float32)
    n = arr[..., 3].astype(np.float32)
    denom = n + r
    return np.where(denom > 0, (n - r) / denom, np.nan)


# ── OSIP fetch + Sobel ────────────────────────────────────────────────────────
def fetch_osip_rgb(x_ft: float, y_ft: float, half_ft: float) -> np.ndarray:
    bbox = f"{x_ft - half_ft},{y_ft - half_ft},{x_ft + half_ft},{y_ft + half_ft}"
    size_px = int(round((2 * half_ft) / OSIP_PIXEL_FT))
    params = {
        "bbox": bbox, "bboxSR": OSIP_SR, "imageSR": OSIP_SR,
        "size": f"{size_px},{size_px}", "format": "tiff",
        "bandIds": "0,1,2", "f": "image",
    }
    r = requests.get(OSIP_URL, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    arr = tifffile.imread(io.BytesIO(r.content))
    if arr.ndim == 3 and arr.shape[0] in (3, 4) and arr.shape[0] < arr.shape[-1]:
        arr = np.transpose(arr, (1, 2, 0))
    return arr


def sobel_magnitude(gray: np.ndarray) -> np.ndarray:
    g = gray.astype(np.float32)
    sx = ((g[:-2, 2:] + 2 * g[1:-1, 2:] + g[2:, 2:])
        - (g[:-2, :-2] + 2 * g[1:-1, :-2] + g[2:, :-2]))
    sy = ((g[2:, :-2] + 2 * g[2:, 1:-1] + g[2:, 2:])
        - (g[:-2, :-2] + 2 * g[:-2, 1:-1] + g[:-2, 2:]))
    out = np.zeros_like(g, dtype=np.float32)
    out[1:-1, 1:-1] = np.sqrt(sx * sx + sy * sy)
    return out


def annular_means(field: np.ndarray, inner_r_px: float, outer_r_px: float):
    h, w = field.shape
    yy, xx = np.ogrid[:h, :w]
    cy, cx = (h - 1) / 2.0, (w - 1) / 2.0
    r2 = (xx - cx) ** 2 + (yy - cy) ** 2
    inner = r2 <= inner_r_px ** 2
    outer = (r2 <= outer_r_px ** 2) & ~inner
    iv = field[inner]; iv = iv[~np.isnan(iv)]
    ov = field[outer]; ov = ov[~np.isnan(ov)]
    in_mean = float(iv.mean()) if iv.size else None
    bg_mean = float(ov.mean()) if ov.size else None
    return in_mean, bg_mean


# ── Combined scoring ──────────────────────────────────────────────────────────
def score_pad(pad_ndvi, delta, edge_ratio):
    """Three-signal combined score, max 80. See module docstring for thresholds."""
    abs_signal = (30 if pad_ndvi is not None and pad_ndvi < 0.10 else
                  15 if pad_ndvi is not None and pad_ndvi < 0.20 else
                   5 if pad_ndvi is not None and pad_ndvi < 0.30 else 0)
    delta_signal = (30 if delta is not None and delta < -0.10 else
                    15 if delta is not None and delta < -0.05 else 0)
    edge_signal  = (20 if edge_ratio is not None and edge_ratio > 1.50 else
                    10 if edge_ratio is not None and edge_ratio > 1.20 else 0)
    return abs_signal, delta_signal, edge_signal, min(80, abs_signal + delta_signal + edge_signal)


# ── Per-well processing ───────────────────────────────────────────────────────
def process_well(well: dict, naip_xform: Transformer, osip_xform: Transformer,
                 inner_naip_px: float, outer_naip_px: float,
                 inner_osip_px: float, outer_osip_px: float):
    """Returns a tuple suitable for INSERT, or None on total failure."""
    api_no, lat, lng = well["api_no"], well["lat"], well["lng"]

    pad_ndvi = bg_ndvi = edge_pad = edge_bg = None

    # NAIP — vegetation channels
    try:
        x_m, y_m = naip_xform.transform(lng, lat)
        mpm = merc_per_meter(lat)
        arr = fetch_naip(x_m, y_m, OUTER_M, mpm)
        if arr.ndim == 3 and arr.shape[-1] >= 4:
            ndvi = naip_ndvi(arr)
            inner_naip_px_w = (INNER_M * mpm) / NAIP_PIXEL_REQUEST
            outer_naip_px_w = (OUTER_M * mpm) / NAIP_PIXEL_REQUEST
            pad_ndvi, bg_ndvi = annular_means(ndvi, inner_naip_px_w, outer_naip_px_w)
    except Exception as e:
        print(f"    [{api_no}] NAIP error: {e}")

    # OSIP — texture/edge
    try:
        x_ft, y_ft = osip_xform.transform(lng, lat)
        arr = fetch_osip_rgb(x_ft, y_ft, OUTER_M * M_TO_FTUS)
        gray = arr.mean(axis=-1)
        edges = sobel_magnitude(gray)
        edge_pad, edge_bg = annular_means(edges, inner_osip_px, outer_osip_px)
    except Exception as e:
        print(f"    [{api_no}] OSIP error: {e}")

    delta = (pad_ndvi - bg_ndvi) if (pad_ndvi is not None and bg_ndvi is not None) else None
    edge_ratio = (edge_pad / edge_bg) if (edge_pad is not None and edge_bg is not None and edge_bg > 0) else None
    abs_s, delta_s, edge_s, score = score_pad(pad_ndvi, delta, edge_ratio)

    if pad_ndvi is None and edge_ratio is None:
        return None  # both fetches failed; don't mark processed

    return (
        api_no,
        well.get("county"),
        round(pad_ndvi, 3) if pad_ndvi is not None else None,
        round(bg_ndvi,  3) if bg_ndvi  is not None else None,
        round(delta,    3) if delta    is not None else None,
        round(edge_ratio, 3) if edge_ratio is not None else None,
        abs_s, delta_s, edge_s, score,
    )


# ── Driver ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county",    help="Process only this county")
    parser.add_argument("--reprocess", action="store_true",
                        help="Null pad_processed_at for the target scope and re-run")
    parser.add_argument("--limit",     type=int, default=None,
                        help="Cap wells per county (useful for pilots)")
    parser.add_argument("--apis-file", default=None,
                        help="Process exactly these api_nos (one per line, optional ',stratum'); skips county scan")
    args = parser.parse_args()

    naip_xform = Transformer.from_crs(4326, NAIP_SR, always_xy=True)
    osip_xform = Transformer.from_crs(4326, OSIP_SR, always_xy=True)
    inner_osip_px = (INNER_M * M_TO_FTUS) / OSIP_PIXEL_FT
    outer_osip_px = (OUTER_M * M_TO_FTUS) / OSIP_PIXEL_FT
    # NAIP px varies by latitude (Mercator scaling) — computed per well above

    conn = get_conn()
    total = 0
    try:
        with conn.cursor() as cur:
            if args.reprocess:
                if args.county:
                    cur.execute("UPDATE well_pad_detection SET pad_processed_at = NULL WHERE UPPER(county) = %s",
                                (args.county.upper(),))
                else:
                    cur.execute("UPDATE well_pad_detection SET pad_processed_at = NULL")
                conn.commit()
                print(f"Reset pad_processed_at (scope: {args.county or 'ALL'})")

            if args.apis_file:
                # Targeted-list mode: load exact api_nos from the file (typically
                # used to validate the production scorer against a probe sample).
                api_nos: list[str] = []
                with open(args.apis_file) as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        api_nos.append(line.split(",", 1)[0])
                cur.execute("""
                    SELECT api_no, lat, lng, county
                      FROM wells
                     WHERE api_no = ANY(%s)
                       AND lat IS NOT NULL AND lng IS NOT NULL
                """, (api_nos,))
                rows_db = cur.fetchall()
                wells_by_county: dict[str, list[dict]] = {}
                for api_no, lat, lng, county in rows_db:
                    wells_by_county.setdefault(county, []).append(
                        {"api_no": api_no, "lat": lat, "lng": lng, "county": county})
                counties = sorted(wells_by_county.keys())
                print(f"--apis-file mode: {len(rows_db)} wells across {len(counties)} counties")
            else:
                counties = get_counties(cur, args.county)
                wells_by_county = None
                print(f"Counties to process: {len(counties)}")

            for county in counties:
                if wells_by_county is not None:
                    wells = wells_by_county[county]
                else:
                    wells = get_wells_for_county(cur, county, args.limit)
                    if not wells:
                        continue
                    for w in wells:
                        w["county"] = county

                print(f"\n-- {county} ({len(wells):,} wells) --")
                rows = []
                for i, well in enumerate(wells, start=1):
                    row = process_well(well, naip_xform, osip_xform,
                                       0, 0, inner_osip_px, outer_osip_px)
                    if row is not None:
                        rows.append(row)
                    if i % 25 == 0 or i == len(wells):
                        print(f"  {i}/{len(wells)} processed, {len(rows)} OK")
                    time.sleep(SLEEP_S)

                if rows:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO well_pad_detection
                          (api_no, county,
                           naip_ndvi_pad, naip_ndvi_bg, naip_delta, edge_ratio,
                           abs_signal, delta_signal, edge_signal, pad_score, pad_processed_at)
                        VALUES %s
                        ON CONFLICT (api_no) DO UPDATE SET
                          county          = EXCLUDED.county,
                          naip_ndvi_pad   = EXCLUDED.naip_ndvi_pad,
                          naip_ndvi_bg    = EXCLUDED.naip_ndvi_bg,
                          naip_delta      = EXCLUDED.naip_delta,
                          edge_ratio      = EXCLUDED.edge_ratio,
                          abs_signal      = EXCLUDED.abs_signal,
                          delta_signal    = EXCLUDED.delta_signal,
                          edge_signal     = EXCLUDED.edge_signal,
                          pad_score       = EXCLUDED.pad_score,
                          pad_processed_at = NOW()
                        """,
                        rows,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
                    )
                    conn.commit()
                    total += len(rows)
    finally:
        conn.close()

    print(f"\nDone. Wells with pad scores: {total:,}")


if __name__ == "__main__":
    main()
