"""
view_anomaly.py

Generates side-by-side before/after Sentinel-2 NDVI comparison images
for a flagged well and opens them in your browser.

Usage:
    python view_anomaly.py --api_no 34009XXXXXXXX
    python view_anomaly.py --county ATHENS          # shows all flagged wells
"""

import os
import argparse
import webbrowser
import psycopg2
import ee
from dotenv import load_dotenv

load_dotenv()

BUFFER_M       = 300   # wider view for visual context
BASELINE_START = "2019-04-01"
BASELINE_END   = "2019-10-31"
RECENT_START   = "2023-04-01"
RECENT_END     = "2023-10-31"
CLOUD_PCT      = 25


def get_conn():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST"),
        dbname=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER", "postgres"),
        password=os.getenv("SUPABASE_DB_PASSWORD"),
        port=int(os.getenv("SUPABASE_DB_PORT", 5432)),
    )


def get_flagged_wells(cur, api_no=None, county=None):
    if api_no:
        cur.execute("""
            SELECT a.api_no, w.lat, w.lng, a.ndvi_change, a.anomaly_type
            FROM well_surface_anomalies a
            JOIN wells w ON a.api_no = w.api_no
            WHERE a.api_no = %s
        """, (api_no,))
    else:
        cur.execute("""
            SELECT a.api_no, w.lat, w.lng, a.ndvi_change, a.anomaly_type
            FROM well_surface_anomalies a
            JOIN wells w ON a.api_no = w.api_no
            WHERE a.county = %s AND a.anomaly_detected = true
            ORDER BY a.ndvi_change ASC
        """, (county.upper(),))
    return cur.fetchall()


def ndvi_image(region, start, end):
    def mask_clouds(img):
        scl = img.select("SCL")
        mask = scl.neq(3).And(scl.lt(8).Or(scl.gt(10)))
        return img.updateMask(mask)

    def add_ndvi(img):
        return img.addBands(
            img.normalizedDifference(["B8", "B4"]).rename("NDVI")
        )

    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_PCT))
        .map(mask_clouds)
        .map(add_ndvi)
        .select("NDVI")
        .median()
    )


def rgb_image(region, start, end):
    """True-color Sentinel-2 for visual context."""
    def mask_clouds(img):
        scl = img.select("SCL")
        mask = scl.neq(3).And(scl.lt(8).Or(scl.gt(10)))
        return img.updateMask(mask)

    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_PCT))
        .map(mask_clouds)
        .select(["B4", "B3", "B2"])   # RGB bands
        .median()
    )


def make_thumbnail_url(image, region, vis_params):
    return image.getThumbURL({
        **vis_params,
        "region":     region,
        "dimensions": 512,
        "format":     "png",
    })


def build_html(wells):
    """Generate an HTML page showing before/after for each well."""
    cards = []
    for api_no, lat, lng, ndvi_change, anomaly_type in wells:
        print(f"  Generating imagery for {api_no} ({lat:.4f}, {lng:.4f})…")

        point  = ee.Geometry.Point([lng, lat])
        region = point.buffer(BUFFER_M).bounds()

        ndvi_vis = {
            "min": -0.2, "max": 0.8,
            "palette": ["#7f1d1d", "#b45309", "#fde68a", "#4ade80", "#166534"],
        }
        rgb_vis = {"min": 0, "max": 3000, "gamma": 1.4}

        try:
            b_ndvi = make_thumbnail_url(ndvi_image(region, BASELINE_START, BASELINE_END), region, ndvi_vis)
            r_ndvi = make_thumbnail_url(ndvi_image(region, RECENT_START, RECENT_END),   region, ndvi_vis)
            b_rgb  = make_thumbnail_url(rgb_image(region, BASELINE_START, BASELINE_END), region, rgb_vis)
            r_rgb  = make_thumbnail_url(rgb_image(region, RECENT_START, RECENT_END),    region, rgb_vis)
        except Exception as e:
            print(f"  [skip] {e}")
            continue

        change_pct = round(abs(ndvi_change) * 100) if ndvi_change else 0
        color = "#ef4444" if ndvi_change and ndvi_change < -0.15 else "#f59e0b"

        gmaps = f"https://www.google.com/maps?cbll={lat},{lng}&layer=c"

        cards.append(f"""
        <div class="card">
          <div class="header">
            <span class="api">{api_no}</span>
            <span class="badge" style="background:{color}">{anomaly_type.replace('_',' ')} &mdash; NDVI {ndvi_change:.3f} ({change_pct}% drop)</span>
            <a href="{gmaps}" target="_blank" class="sv">Street View ↗</a>
          </div>
          <div class="grid">
            <div class="col">
              <p class="label">2019 baseline &mdash; True color</p>
              <img src="{b_rgb}" />
            </div>
            <div class="col">
              <p class="label">2023 recent &mdash; True color</p>
              <img src="{r_rgb}" />
            </div>
            <div class="col">
              <p class="label">2019 &mdash; NDVI (green = healthy)</p>
              <img src="{b_ndvi}" />
            </div>
            <div class="col">
              <p class="label">2023 &mdash; NDVI (red = stressed)</p>
              <img src="{r_ndvi}" />
            </div>
          </div>
        </div>
        """)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Well Surface Anomalies</title>
<style>
  body {{ font-family: system-ui; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
  h1   {{ font-size: 18px; margin-bottom: 20px; color: #f8fafc; }}
  .card {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 20px; margin-bottom: 28px; }}
  .header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }}
  .api  {{ font-family: monospace; font-size: 13px; color: #94a3b8; }}
  .badge {{ padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: 600; color: #000; }}
  .sv   {{ font-size: 12px; color: #60a5fa; text-decoration: none; margin-left: auto; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px; }}
  .col  {{ display: flex; flex-direction: column; gap: 6px; }}
  .label {{ font-size: 11px; color: #64748b; margin: 0; }}
  img  {{ width: 100%; border-radius: 6px; border: 1px solid #334155; }}
</style>
</head>
<body>
<h1>Surface Anomaly Viewer — Sentinel-2 Before / After</h1>
{''.join(cards) if cards else '<p style="color:#64748b">No flagged wells found.</p>'}
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--api_no", help="Specific well API number")
    group.add_argument("--county", help="All flagged wells in a county")
    args = parser.parse_args()

    project = os.getenv("GEE_PROJECT", "earthengine-legacy")
    ee.Initialize(project=project)

    conn = get_conn()
    with conn.cursor() as cur:
        wells = get_flagged_wells(cur, api_no=args.api_no, county=args.county)
    conn.close()

    if not wells:
        print("No flagged wells found.")
        return

    print(f"Generating imagery for {len(wells)} well(s)…")
    html = build_html(wells)

    out = "anomaly_viewer.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nOpening {out} in browser…")
    webbrowser.open(out)


if __name__ == "__main__":
    main()
