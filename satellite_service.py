"""
satellite_service.py

FastAPI microservice wrapping Google Earth Engine for on-demand well analysis.

Four environmental signals per well:

  1. NDVI  — Vegetation "ghost"
             (NIR-Red)/(NIR+Red) from Sentinel-2 10 m.
             An old well pad shows a productivity gap even under regrown forest
             due to soil compaction and residual hydrocarbons.

  2. NDMI  — Salt burn / moisture stress
             (NIR-SWIR1)/(NIR+SWIR1) from Sentinel-2.
             Produced-water (brine) spills alter leaf moisture for decades.
             A dry patch on the NDMI map = past spill site.

  3. SWIR  — Methane/gas hypoxia signal
             False-colour composite B12/B8/B4 (SWIR2-NIR-Red).
             Gas replacing soil oxygen creates bare spots; SWIR highlights
             exposed soil, stressed vegetation, and thermal anomalies.
             Also uses Sentinel-5P CH₄ column for direct methane detection.

  4. Terrain — Artificial flatness (well-pad shelf)
             USGS NED 10 m DEM → slope within buffer.
             A well pad is an unnaturally flat square carved into natural terrain.
             Low mean slope + low roughness = likely artificial grading.

Usage:
    pip install fastapi uvicorn earthengine-api python-dotenv
    earthengine authenticate
    python satellite_service.py     # http://localhost:8001
"""

import os
from datetime import date

import uvicorn
import ee
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Well Satellite Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

GEE_PROJECT = os.getenv("GEE_PROJECT", "earthengine-legacy")

# ── Date windows ───────────────────────────────────────────────────────────────
# 2017-2018 stitched (2 growing seasons). 2016 SR_HARMONIZED coverage is too
# thin in Ohio — many tiles had <8 cloud-free acquisitions, so s2_median
# returned None and the UI rendered "No clear pixels". Two seasons of medians
# gives reliable coverage everywhere; well-pad disturbance is decadal so a
# 1-year shift in "pre" doesn't change what's detected.
S2_BASELINE_START = "2017-05-01"
S2_BASELINE_END   = "2018-10-31"
S2_RECENT_START   = "2023-05-01"
S2_RECENT_END     = "2024-10-31"

METHANE_START = "2021-01-01"
METHANE_END   = "2024-01-01"

# Landsat dates kept for NDVI /analyze scoring (longer baseline)
LS_BASELINE_START = "2000-04-01"
LS_BASELINE_END   = "2003-10-31"
LS_RECENT_START   = "2023-04-01"
LS_RECENT_END     = "2024-10-31"

BUFFER_M       = 500
# Sentinel-2 native bands are 10m (B2/B3/B4/B8) or 20m (B11/B12), so the 1400m
# thumb region (THUMB_REGION_M*2) only carries ~140 real pixels of detail. Output
# at 1280 + bicubic resampling so the upsample looks smooth rather than blocky.
THUMB_SIZE     = 1280
THUMB_REGION_M = 700
METHANE_BG_KM  = 10

_ee_ready = False

def init_ee():
    global _ee_ready
    if not _ee_ready:
        ee.Initialize(project=GEE_PROJECT)
        _ee_ready = True


# ── Sentinel-2 helpers ─────────────────────────────────────────────────────────
def s2_collection(region: ee.Geometry, start: str, end: str) -> ee.ImageCollection:
    def mask(img):
        scl = img.select("SCL")
        return img.updateMask(scl.neq(3).And(scl.lt(8).Or(scl.gt(10))))
    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30))
        .map(mask)
    )


def s2_median(region: ee.Geometry, start: str, end: str) -> ee.Image | None:
    col = s2_collection(region, start, end)
    if col.size().getInfo() == 0:
        return None
    # bicubic so the 10m native pixels don't show up as blocks when getThumbURL
    # upsamples to THUMB_SIZE. resample() applies during reprojection only, so
    # mean_index() reduceRegion calls (scale=20) are barely affected.
    return col.median().resample('bicubic')


def thumb(img: ee.Image, region: ee.Geometry, bands: list[str],
          min_val, max_val, palette: list[str] | None = None,
          gamma: float = 1.0) -> str | None:
    """Generate a signed GEE thumbnail URL for a single- or multi-band image."""
    selected = img.select(bands)
    params: dict = {
        "region":     region,
        "dimensions": THUMB_SIZE,
        "format":     "png",
        "gamma":      gamma,
    }
    if palette and len(bands) == 1:
        params["min"]     = min_val
        params["max"]     = max_val
        params["palette"] = palette
    else:
        params["min"] = min_val
        params["max"] = max_val
    try:
        return selected.getThumbURL(params)
    except Exception:
        return None


def mean_index(img: ee.Image, band: str, region: ee.Geometry) -> float | None:
    result = img.select(band).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=20,
        maxPixels=50_000,
    ).get(band).getInfo()
    return round(result, 4) if result is not None else None


# ── Landsat helpers (NDVI scoring, longer baseline) ────────────────────────────
def mask_landsat_c2(img: ee.Image) -> ee.Image:
    qa = img.select("QA_PIXEL")
    return img.updateMask(
        qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))
    )


def landsat_ndvi(region: ee.Geometry, start: str, end: str,
                 nir: str, red: str, collections: list[str]) -> ee.Image | None:
    merged = ee.ImageCollection([])
    for c in collections:
        merged = merged.merge(
            ee.ImageCollection(c).filterBounds(region).filterDate(start, end).map(mask_landsat_c2)
        )
    if merged.size().getInfo() == 0:
        return None
    return merged.median().normalizedDifference([nir, red]).rename("NDVI")


# ── Methane ────────────────────────────────────────────────────────────────────
def methane_anomaly(point: ee.Geometry, bg_region: ee.Geometry) -> dict:
    col = (
        ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_CH4")
        .filterDate(METHANE_START, METHANE_END)
        .select("CH4_column_volume_mixing_ratio_dry_air")
    )
    if col.size().getInfo() == 0:
        return {"well_ppb": None, "background_ppb": None, "anomaly_ratio": None, "is_anomaly": False}

    composite = col.mean()
    well_val = composite.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=point.buffer(5500), scale=5500, maxPixels=10
    ).get("CH4_column_volume_mixing_ratio_dry_air").getInfo()
    bg_val = composite.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=bg_region, scale=5500, maxPixels=100
    ).get("CH4_column_volume_mixing_ratio_dry_air").getInfo()

    if well_val is None or bg_val is None or bg_val == 0:
        return {"well_ppb": None, "background_ppb": None, "anomaly_ratio": None, "is_anomaly": False}

    ratio = well_val / bg_val
    return {
        "well_ppb":       round(well_val, 1),
        "background_ppb": round(bg_val, 1),
        "anomaly_ratio":  round(ratio, 4),
        "is_anomaly":     ratio > 1.05,
    }


# ── Terrain (USGS 3DEP 10 m) ───────────────────────────────────────────────────
def terrain_analysis(point: ee.Geometry, buffer_m: int = 300) -> dict:
    """
    Returns mean slope and terrain roughness (std dev of elevation) within buffer.
    Low mean slope + low roughness vs. surrounding area = likely artificial grading.
    Also generates a hillshade thumbnail.
    """
    # USGS/NED was deprecated in favor of the tiled USGS/3DEP/10m_collection;
    # .mosaic() flattens the per-tile ImageCollection into a single virtual image.
    dem = ee.ImageCollection("USGS/3DEP/10m_collection").mosaic().select("elevation")
    region   = point.buffer(buffer_m)
    surround = point.buffer(buffer_m * 4)  # wider area for context

    slope = ee.Terrain.slope(dem)

    def reduce(geom, scale=10):
        return slope.reduceRegion(
            reducer=ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True),
            geometry=geom, scale=scale, maxPixels=50_000,
        ).getInfo()

    well_stats = reduce(region)
    bg_stats   = reduce(surround)

    mean_slope_well = well_stats.get("slope_mean")
    mean_slope_bg   = bg_stats.get("slope_mean")

    # Artificially flat if well slope is less than 40% of surrounding terrain slope
    is_flat = (
        mean_slope_well is not None and
        mean_slope_bg   is not None and
        mean_slope_bg > 1.0 and
        mean_slope_well < mean_slope_bg * 0.4
    )

    # Hillshade thumbnail — shows terrain relief clearly
    hillshade = ee.Terrain.hillshade(dem)
    thumb_region = point.buffer(THUMB_REGION_M)
    hillshade_url = thumb(hillshade, thumb_region, ["hillshade"], 0, 255)

    return {
        "mean_slope_well": round(mean_slope_well, 2) if mean_slope_well else None,
        "mean_slope_bg":   round(mean_slope_bg,   2) if mean_slope_bg   else None,
        "is_flat":         is_flat,
        "hillshade_url":   hillshade_url,
    }


# ── Thumbnail endpoint (all four analyses) ─────────────────────────────────────
@app.get("/thumbnails")
def thumbnails(
    lat: float = Query(..., ge=24.0, le=50.0),
    lng: float = Query(..., ge=-125.0, le=-66.0),
    # Baseline is the 2017-2018 stitched median; floor recent_year at 2019 so the
    # gap is at least 1 year and the recent window doesn't overlap the baseline.
    # Cap at last fully-completed growing season (current year - 1).
    recent_year: int = Query(
        date.today().year - 1,
        ge=2019,
        le=date.today().year - 1,
    ),
):
    """
    Returns thumbnail URLs and scores for all four environmental analyses.
    getThumbURL calls are fast (signed URL only) — pixel computation is deferred
    until the browser loads the image.
    """
    try:
        init_ee()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"GEE init failed: {e}")

    point        = ee.Geometry.Point([lng, lat])
    region       = point.buffer(BUFFER_M)
    thumb_region = point.buffer(THUMB_REGION_M)

    recent_start = f"{recent_year}-05-01"
    recent_end   = f"{recent_year}-10-31"

    # Build baseline and recent Sentinel-2 composites once — reuse for all indices
    baseline = s2_median(thumb_region, S2_BASELINE_START, S2_BASELINE_END)
    recent   = s2_median(thumb_region, recent_start,      recent_end)

    result: dict = {
        "baseline_year": "2017–18",
        "recent_year":   str(recent_year),
        "gap_years":     recent_year - 2017,
    }

    # ── 1. True colour ─────────────────────────────────────────────────────────
    result["imagery"] = {
        "baseline_url": thumb(baseline, thumb_region, ["B4","B3","B2"], 200, 2800, gamma=1.4) if baseline else None,
        "recent_url":   thumb(recent,   thumb_region, ["B4","B3","B2"], 200, 2800, gamma=1.4) if recent   else None,
    }

    # ── 2. NDVI — numeric only ─────────────────────────────────────────────────
    # Frontend renders only the true-color photo above; NDVI false-color tiles
    # were dropped because two near-saturated greens carry less actionable
    # signal than the actual RGB scene. We still compute the means here so the
    # header text can show "NDVI 0.78 → 0.74" and label the anomaly type.
    ndvi_b = baseline.normalizedDifference(["B8","B4"]).rename("NDVI") if baseline else None
    ndvi_r = recent.normalizedDifference(["B8","B4"]).rename("NDVI")   if recent   else None

    ndvi_base_mean   = mean_index(ndvi_b, "NDVI", region) if ndvi_b else None
    ndvi_recent_mean = mean_index(ndvi_r, "NDVI", region) if ndvi_r else None
    ndvi_change      = round(ndvi_recent_mean - ndvi_base_mean, 4) if (ndvi_base_mean and ndvi_recent_mean) else None
    ndvi_relative    = round(ndvi_change / ndvi_base_mean, 4) if (ndvi_change and ndvi_base_mean and ndvi_base_mean > 0.1) else None

    result["ndvi"] = {
        "baseline_mean": ndvi_base_mean,
        "recent_mean":   ndvi_recent_mean,
        "change":        ndvi_change,
        "relative":      ndvi_relative,
        "anomaly_type":  _ndvi_label(ndvi_change, ndvi_base_mean),
    }

    # ── 3. NDMI — salt burn / moisture stress ──────────────────────────────────
    # B8=NIR (10m), B11=SWIR1 (20m) — both available in S2_SR_HARMONIZED
    # High NDMI = moist healthy leaves; low NDMI = salt or drought stress
    ndmi_palette = ["b71c1c","ef5350","ffee58","29b6f6","0277bd","01579b"]
    ndmi_b = baseline.normalizedDifference(["B8","B11"]).rename("NDMI") if baseline else None
    ndmi_r = recent.normalizedDifference(["B8","B11"]).rename("NDMI")   if recent   else None

    ndmi_base_mean   = mean_index(ndmi_b, "NDMI", region) if ndmi_b else None
    ndmi_recent_mean = mean_index(ndmi_r, "NDMI", region) if ndmi_r else None
    ndmi_change      = round(ndmi_recent_mean - ndmi_base_mean, 4) if (ndmi_base_mean and ndmi_recent_mean) else None

    result["ndmi"] = {
        "baseline_url":  thumb(ndmi_b, thumb_region, ["NDMI"], -0.3, 0.6, palette=ndmi_palette) if ndmi_b else None,
        "recent_url":    thumb(ndmi_r, thumb_region, ["NDMI"], -0.3, 0.6, palette=ndmi_palette) if ndmi_r else None,
        "baseline_mean": ndmi_base_mean,
        "recent_mean":   ndmi_recent_mean,
        "change":        ndmi_change,
        "is_dry_anomaly": ndmi_change is not None and ndmi_change < -0.05,
    }

    # ── 4. SWIR false-colour — gas hypoxia / bare soil ─────────────────────────
    # B12=SWIR2, B8=NIR, B4=Red — exposed soil/stressed veg appears vivid red/magenta
    result["swir"] = {
        "baseline_url": thumb(baseline, thumb_region, ["B12","B8","B4"], 200, 3500, gamma=1.3) if baseline else None,
        "recent_url":   thumb(recent,   thumb_region, ["B12","B8","B4"], 200, 3500, gamma=1.3) if recent   else None,
    }

    # ── 5. Terrain ─────────────────────────────────────────────────────────────
    try:
        result["terrain"] = terrain_analysis(point, buffer_m=300)
    except Exception as e:
        result["terrain"] = {"error": str(e)}

    return result


# ── Full analysis endpoint (NDVI + methane scores, no thumbnails) ──────────────
@app.get("/analyze")
def analyze(
    lat: float = Query(..., ge=24.0, le=50.0),
    lng: float = Query(..., ge=-125.0, le=-66.0),
):
    try:
        init_ee()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"GEE init failed: {e}")

    point       = ee.Geometry.Point([lng, lat])
    well_buffer = point.buffer(BUFFER_M)
    bg_buffer   = point.buffer(METHANE_BG_KM * 1000)

    # NDVI (Landsat — longer 20-year baseline for scoring)
    try:
        baseline_img = landsat_ndvi(well_buffer, LS_BASELINE_START, LS_BASELINE_END,
                                    "SR_B4", "SR_B3", ["LANDSAT/LE07/C02/T1_L2"])
        recent_img   = landsat_ndvi(well_buffer, LS_RECENT_START, LS_RECENT_END,
                                    "SR_B5", "SR_B4", ["LANDSAT/LC08/C02/T1_L2", "LANDSAT/LC09/C02/T1_L2"])
        ndvi_result: dict = {"baseline_years": "2000-2003", "recent_years": "2023-2024"}

        if baseline_img and recent_img:
            def mean_ndvi(img):
                return img.reduceRegion(
                    reducer=ee.Reducer.mean(), geometry=well_buffer,
                    scale=30, maxPixels=10_000,
                ).get("NDVI").getInfo()

            bv = mean_ndvi(baseline_img)
            rv = mean_ndvi(recent_img)
            if bv is not None and rv is not None:
                change   = rv - bv
                relative = change / bv if bv > 0.1 else None
                score, atype = _ndvi_score(change, bv)
                ndvi_result.update({
                    "baseline": round(bv, 4), "recent": round(rv, 4),
                    "change": round(change, 4),
                    "relative_change": round(relative, 4) if relative else None,
                    "score": score, "anomaly_type": atype,
                })
            else:
                ndvi_result["error"] = "Insufficient clear pixels"
        else:
            ndvi_result["error"] = "No Landsat imagery available"
    except Exception as e:
        ndvi_result = {"error": str(e), "baseline_years": "2000-2003", "recent_years": "2023-2024"}

    # Methane
    try:
        methane_result = methane_anomaly(point, bg_buffer)
    except Exception as e:
        methane_result = {"error": str(e), "is_anomaly": False}

    return {"lat": lat, "lng": lng, "ndvi": ndvi_result, "methane": methane_result}


def _ndvi_score(change: float, baseline: float) -> tuple[int, str]:
    if baseline < 0.25: return 0, "low_baseline_skip"
    r = change / baseline
    if r >= -0.06:  return 0,   "stable"
    if r >= -0.12:  return 15,  "minor_change"
    if r >= -0.20:  return 30,  "moderate_change"
    if r >= -0.35:  return 60,  "vegetation_loss"
    if r >= -0.55:  return 80,  "severe_loss"
    return 100, "near_total_loss"


def _ndvi_label(change: float | None, baseline: float | None) -> str:
    if change is None or baseline is None: return "no_data"
    if baseline < 0.25: return "low_baseline_skip"
    r = change / baseline
    if r >= -0.06:  return "stable"
    if r >= -0.12:  return "minor_change"
    if r >= -0.20:  return "moderate_change"
    if r >= -0.35:  return "vegetation_loss"
    if r >= -0.55:  return "severe_loss"
    return "near_total_loss"


if __name__ == "__main__":
    uvicorn.run("satellite_service:app", host="0.0.0.0", port=8001, reload=True)
