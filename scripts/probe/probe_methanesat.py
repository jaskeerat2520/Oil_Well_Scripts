"""
probe_methanesat.py

Read-only probe of EDF's methane products on Google Earth Engine. Checks
both the aircraft mission (MethaneAIR, public, no auth needed) and the
satellite mission (MethaneSAT, gated behind the EDF data request form).

Reports for each dataset:
  1. Which basins / regions EDF targeted
  2. How many observations intersect Ohio
  3. Date range of Ohio coverage (if any)

Ends with a verdict telling you whether MethaneAIR / MethaneSAT is usable
for the Ohio well-plugging pipeline, or whether to stick with Sentinel-5P.

No DB connection, no writes. Safe to re-run any time.

Usage:
    python probe_methanesat.py
"""

import os
import ee
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# Public catalog (no auth gate)
METHANEAIR_L3 = "EDF/MethaneSAT/MethaneAIR/L3concentration"
METHANEAIR_L4_POINTS = "EDF/MethaneSAT/MethaneAIR/L4point"

# MethaneSAT public-preview assets (gated behind request form)
MSAT_L4_POINTS = "projects/edf-methanesat-ee/assets/public-preview/L4point"
MSAT_L3_CONC   = "projects/edf-methanesat-ee/assets/public-preview/L3concentration"

# Ohio rough bounding box (WGS84): west, south, east, north
OHIO_BBOX = [-84.82, 38.40, -80.52, 41.98]


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def list_distinct(fc: ee.FeatureCollection, prop: str) -> list:
    return fc.aggregate_array(prop).distinct().sort().getInfo()


def probe_methaneair(ohio: ee.Geometry) -> int:
    """
    Probe the public MethaneAIR (aircraft) datasets. No access form required.
    Returns number of Ohio-intersecting L3 images found (for the verdict).
    """
    print("=" * 70)
    print("  MethaneAIR (aircraft, public)")
    print("=" * 70)

    # L3 Concentration — ~10m XCH4 raster per flight
    print("\n── MethaneAIR L3 Concentration ──")
    l3 = ee.ImageCollection(METHANEAIR_L3)
    total = l3.size().getInfo()
    print(f"  Total flights (images): {total:,}")

    basins = list_distinct(l3, "basin")
    print(f"  Distinct basins flown: {len(basins)}")
    for b in basins:
        print(f"    - {b}")

    ohio_l3 = l3.filterBounds(ohio)
    n_ohio = ohio_l3.size().getInfo()
    print(f"\n  L3 flights intersecting Ohio: {n_ohio:,}")
    if n_ohio > 0:
        times = ohio_l3.aggregate_array("system:time_start").getInfo()
        dates = sorted(ms_to_iso(t) for t in times)
        ohio_basins = list_distinct(ohio_l3, "basin")
        print(f"  Ohio flight dates: {', '.join(dates)}")
        print(f"  Basins touching Ohio: {ohio_basins}")

    # L4 Point Sources (aircraft-detected plumes)
    print("\n── MethaneAIR L4 Point Sources ──")
    try:
        l4 = ee.FeatureCollection(METHANEAIR_L4_POINTS)
        n_total = l4.size().getInfo()
        print(f"  Total global aircraft plumes: {n_total:,}")
        ohio_l4 = l4.filterBounds(ohio)
        n_ohio_l4 = ohio_l4.size().getInfo()
        print(f"  Aircraft plumes in Ohio: {n_ohio_l4:,}")
        if n_ohio_l4 > 0:
            top = (ohio_l4.filter(ee.Filter.notNull(["flux"]))
                   .sort("flux", False).limit(10).getInfo()["features"])
            print(f"  Top {len(top)} Ohio plumes by flux:")
            for f in top:
                p = f["properties"]
                print(f"    flux={p.get('flux')}±{p.get('flux_sd')} kg/hr  "
                      f"@ {f['geometry']['coordinates']}  "
                      f"date={str(p.get('date', '?'))[:10]}")
    except ee.ee_exception.EEException as e:
        print(f"  Error reading L4 points: {e}")

    return n_ohio


def probe_methanesat(ohio: ee.Geometry, project: str) -> int | None:
    """
    Probe the gated MethaneSAT (satellite) datasets. Returns Ohio plume count,
    or None if access isn't granted yet.
    """
    print("\n" + "=" * 70)
    print("  MethaneSAT (satellite, gated)")
    print("=" * 70)

    print("\n── Checking L4 Point Sources access ──")
    try:
        pts = ee.FeatureCollection(MSAT_L4_POINTS)
        total = pts.size().getInfo()
        print(f"  ✓ Access OK. Total global plumes: {total:,}")
    except ee.ee_exception.EEException as e:
        msg = str(e)
        if "not found" in msg.lower() or "permission" in msg.lower():
            print(f"  ✗ No access yet. EDF has not ACL'd project '{project}'.")
            print("    Form may still be under review, or the GEE project you")
            print("    submitted doesn't match GEE_PROJECT in .env.")
            return None
        raise

    regions = list_distinct(pts, "region")
    print(f"\n  Distinct regions: {len(regions)}")
    for r in regions:
        print(f"    - {r}")

    ohio_pts = pts.filterBounds(ohio)
    n_ohio_pts = ohio_pts.size().getInfo()
    print(f"\n  Plumes inside Ohio bbox: {n_ohio_pts:,}")

    if n_ohio_pts > 0:
        top = (ohio_pts.filter(ee.Filter.notNull(["flux"]))
               .sort("flux", False).limit(10).getInfo()["features"])
        print(f"  Top {len(top)} Ohio plumes by flux:")
        for f in top:
            p = f["properties"]
            print(f"    {str(p.get('date','?'))[:10]}  "
                  f"flux={p.get('flux')}±{p.get('flux_sd')} kg/hr  "
                  f"@ {f['geometry']['coordinates']}  region={p.get('region')}")

    print("\n── MethaneSAT L3 Concentration scenes over Ohio ──")
    try:
        l3 = ee.ImageCollection(MSAT_L3_CONC).filterBounds(ohio)
        n_l3 = l3.size().getInfo()
        print(f"  L3 scenes covering Ohio: {n_l3:,}")
        if n_l3 > 0:
            times = l3.aggregate_array("system:time_start").getInfo()
            dates = sorted(ms_to_iso(t) for t in times)
            print(f"  First: {dates[0]}   Last: {dates[-1]}")
    except ee.ee_exception.EEException as e:
        print(f"  L3 access error: {e}")

    return n_ohio_pts


def main():
    project = os.getenv("GEE_PROJECT", "earthengine-legacy")
    print(f"Initializing GEE (project: {project})…")
    ee.Initialize(project=project)
    print("Connected.\n")

    ohio = ee.Geometry.Rectangle(OHIO_BBOX)

    air_ohio = probe_methaneair(ohio)
    sat_ohio = probe_methanesat(ohio, project)

    # ── Verdict ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  Verdict")
    print("=" * 70)
    if air_ohio > 0:
        print(f"  ✓ MethaneAIR flew Ohio ({air_ohio} flights). No access gate.")
        print("    → Best primary signal. 10m resolution, 25 ppb precision,")
        print("      public dataset, no form needed.")
    elif sat_ohio and sat_ohio > 0:
        print(f"  ✓ MethaneSAT imaged Ohio ({sat_ohio} plumes). Gated access granted.")
        print("    → Viable primary signal. 45m resolution, kg/hr flux per plume.")
    elif sat_ohio is None:
        print("  ? MethaneSAT access not yet granted; re-run this probe after")
        print("    EDF emails you. MethaneAIR did not fly Ohio.")
        print("    → Keep the Sentinel-5P hotspot approach for now.")
    else:
        print("  ✗ Neither mission covered Ohio. Keep Sentinel-5P hotspot approach.")


if __name__ == "__main__":
    main()
