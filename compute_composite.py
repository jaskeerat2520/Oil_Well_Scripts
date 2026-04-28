"""
compute_composite.py

Merges all six risk dimensions into a single composite score + priority
for every well in well_risk_scores.

Dimensions and default weights:

  water_risk_score        25%   (distance to protection zones)
  population_risk_score   15%   (human exposure within 1km / 5km buffers)
  vegetation_risk_score   20%   (derived from NDVI trend + NDMI change + anomaly)
  terrain_risk_score      10%   (from well_remote_sensing — artificial pad flatness)
  emissions_risk_score    20%   (from well_remote_sensing — plumes + thermal)
  inactivity_score        10%   (from well_risk_scores — years_inactive bucketing
                                 set by backfill_production_years.py; 0 for
                                 Producing wells by design)

Missing components are handled by renormalizing: the weights of the *available*
dimensions sum to 100% for each well, so partial-data wells are scored fairly
against themselves rather than penalized.

Priority buckets (calibrated to the realised composite distribution — RS
signals contribute 0s rather than NULLs for wells without anomalies, which
caps the achievable composite well below 100):

    composite ≥ 45  → critical   (~top 0.06% — map-noticeable elite tier)
    composite ≥ 35  → high       (~top 1.3%)
    composite ≥ 25  → medium     (~top 17.6%)
    else            → low

Producing wells are capped at priority='medium' regardless of composite
(per CLAUDE.md convention — active producers are not plugging candidates).

Usage:
    python compute_composite.py
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Windows console default codepage (cp1252) can't encode the ─ separator used in
# the stats summary. Force UTF-8 so the script exits 0 on Windows too.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

DB_HOST     = os.getenv("SUPABASE_DB_HOST")
DB_NAME     = os.getenv("SUPABASE_DB_NAME", "postgres")
DB_USER     = os.getenv("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_PORT     = int(os.getenv("SUPABASE_DB_PORT", 5432))


COMPOSITE_SQL = """
WITH dims AS (
  SELECT
    wrs.api_no,
    w.status,
    w.last_nonzero_production_year,
    wrs.water_risk_score,
    wrs.population_risk_score,
    wrs.inactivity_score,

    -- Vegetation: combine the binary NDVI anomaly score with the slope of
    -- the multi-year trend and any NDMI decline.
    --   slope < -0.03  →  ~80   (steep multi-year decline, strongest signal)
    --   slope < -0.015 →  ~50
    --   slope < -0.005 →  ~25
    -- NDMI drop adds up to +20 on top.  Capped at 100.
    LEAST(100,
      GREATEST(
        COALESCE(wsa.anomaly_score, 0),
        CASE
          WHEN wsa.ndvi_trend_slope < -0.03  THEN 80
          WHEN wsa.ndvi_trend_slope < -0.015 THEN 50
          WHEN wsa.ndvi_trend_slope < -0.005 THEN 25
          ELSE 0
        END
      )
      + CASE
          WHEN wsa.ndmi_change < -0.10 THEN 20
          WHEN wsa.ndmi_change < -0.05 THEN 10
          ELSE 0
        END
    )::double precision AS vegetation_score,

    wrs2.terrain_risk_score::double precision   AS terrain_score,
    wrs2.emissions_risk_score::double precision AS emissions_score
  FROM well_risk_scores wrs
  JOIN wells w                         ON wrs.api_no = w.api_no
  LEFT JOIN well_surface_anomalies wsa ON wrs.api_no = wsa.api_no
  LEFT JOIN well_remote_sensing   wrs2 ON wrs.api_no = wrs2.api_no
),
scored AS (
  SELECT
    api_no, status, last_nonzero_production_year,
    water_risk_score, population_risk_score, inactivity_score,
    vegetation_score, terrain_score, emissions_score,
    -- Weighted sum (zeros for NULL, numerator only)
    (COALESCE(water_risk_score,      0) * 0.25
   + COALESCE(population_risk_score, 0) * 0.15
   + COALESCE(vegetation_score,      0) * 0.20
   + COALESCE(terrain_score,         0) * 0.10
   + COALESCE(emissions_score,       0) * 0.20
   + COALESCE(inactivity_score,      0) * 0.10) AS weighted_sum,
    -- Sum of weights for *present* dimensions only — normalizer
    ( (CASE WHEN water_risk_score      IS NOT NULL THEN 0.25 ELSE 0 END)
    + (CASE WHEN population_risk_score IS NOT NULL THEN 0.15 ELSE 0 END)
    + (CASE WHEN vegetation_score      IS NOT NULL THEN 0.20 ELSE 0 END)
    + (CASE WHEN terrain_score         IS NOT NULL THEN 0.10 ELSE 0 END)
    + (CASE WHEN emissions_score       IS NOT NULL THEN 0.20 ELSE 0 END)
    + (CASE WHEN inactivity_score      IS NOT NULL THEN 0.10 ELSE 0 END) ) AS weights_present
  FROM dims
),
final AS (
  SELECT
    api_no, status, last_nonzero_production_year,
    vegetation_score, terrain_score, emissions_score,
    CASE
      WHEN weights_present > 0 THEN weighted_sum / weights_present
      ELSE NULL
    END AS composite
  FROM scored
)
UPDATE well_risk_scores wrs
SET
  vegetation_risk_score = f.vegetation_score,
  terrain_risk_score    = f.terrain_score,
  emissions_risk_score  = f.emissions_score,
  composite_risk_score  = f.composite,
  priority = CASE
    -- Verified-active producers (recent production) stay capped at medium —
    -- these are regulated operations where enforcement runs through the
    -- operator, not the state plugging fund.
    WHEN f.status = 'Producing' AND f.last_nonzero_production_year >= 2020 THEN
      CASE
        WHEN f.composite >= 25 THEN 'medium'
        ELSE 'low'
      END
    -- Zombie / paperwork producers (Producing status with stale or null
    -- production) are hidden orphans and should NOT be capped.
    WHEN f.composite >= 45 THEN 'critical'
    WHEN f.composite >= 35 THEN 'high'
    WHEN f.composite >= 25 THEN 'medium'
    ELSE 'low'
  END,
  computed_at = NOW()
FROM final f
WHERE wrs.api_no = f.api_no;
"""


STATS_SQL = """
SELECT
  priority,
  COUNT(*)                                        AS n,
  ROUND(AVG(composite_risk_score)::numeric, 1)    AS avg_composite,
  ROUND(AVG(vegetation_risk_score)::numeric, 1)   AS avg_veg,
  ROUND(AVG(terrain_risk_score)::numeric, 1)      AS avg_terrain,
  ROUND(AVG(emissions_risk_score)::numeric, 1)    AS avg_emissions
FROM well_risk_scores
GROUP BY priority
ORDER BY
  CASE priority
    WHEN 'critical' THEN 1
    WHEN 'high'     THEN 2
    WHEN 'medium'   THEN 3
    WHEN 'low'      THEN 4
    ELSE 5
  END;
"""


def main():
    missing = [k for k in ("SUPABASE_DB_HOST", "SUPABASE_DB_PASSWORD") if not os.getenv(k)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    print(f"[INFO]  Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} …")
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT,
        connect_timeout=15, sslmode="require",
    )

    try:
        start = time.time()
        with conn.cursor() as cur:
            print("[INFO]  Computing composite scores (single UPDATE)…")
            cur.execute(COMPOSITE_SQL)
            updated = cur.rowcount
        conn.commit()
        elapsed = time.time() - start
        print(f"[OK]    Updated {updated:,} wells in {elapsed:.1f}s\n")

        with conn.cursor() as cur:
            cur.execute(STATS_SQL)
            rows = cur.fetchall()

        hdr = ("priority", "n", "avg_comp", "avg_veg", "avg_terr", "avg_emis")
        widths = (10, 8, 9, 9, 10, 10)
        print("─" * sum(widths))
        print("".join(f"{h:<{w}}" for h, w in zip(hdr, widths)))
        print("─" * sum(widths))
        for r in rows:
            print("".join(f"{str(v or '-'):<{w}}" for v, w in zip(r, widths)))
        print("─" * sum(widths))

    finally:
        conn.close()
        print("\n[INFO]  Done.")


if __name__ == "__main__":
    main()
