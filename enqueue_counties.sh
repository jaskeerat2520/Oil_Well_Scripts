#!/usr/bin/env bash
#
# enqueue_counties.sh — fire one HTTP POST per Ohio county to a Cloud Run worker,
# in parallel batches so the 88 counties don't take forever end-to-end.
#
# The scoring scripts are resume-safe (they skip counties already processed),
# so re-running this is cheap and idempotent.
#
# Usage:
#   ./enqueue_counties.sh emissions         # fires to emissions-worker URL
#   ./enqueue_counties.sh population        # fires to population-worker URL
#   ./enqueue_counties.sh surface_anomalies # fires to surface-anomalies-worker
#   ./enqueue_counties.sh both              # emissions first, then surface_anomalies
#
# Tune parallelism with PAR=N env var (default 4 — Cloud Run max-instances is 10,
# leaving headroom for retries).

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gen-lang-client-0658746801}"
REGION="${REGION:-us-central1}"
PAR="${PAR:-4}"

TARGET="${1:-}"

if [[ -z "$TARGET" || ( "$TARGET" != "emissions" && "$TARGET" != "population" && "$TARGET" != "surface_anomalies" && "$TARGET" != "pad_detection" && "$TARGET" != "both" ) ]]; then
  echo "Usage: $0 [emissions | population | surface_anomalies | pad_detection | both]"
  exit 1
fi

# ── 88 Ohio counties (RBDMS uses uppercase) ─────────────────────────────────
COUNTIES=(
  ADAMS ALLEN ASHLAND ASHTABULA ATHENS AUGLAIZE BELMONT BROWN BUTLER CARROLL
  CHAMPAIGN CLARK CLERMONT CLINTON COLUMBIANA COSHOCTON CRAWFORD CUYAHOGA
  DARKE DEFIANCE DELAWARE ERIE FAIRFIELD FAYETTE FRANKLIN FULTON GALLIA
  GEAUGA GREENE GUERNSEY HAMILTON HANCOCK HARDIN HARRISON HENRY HIGHLAND
  HOCKING HOLMES HURON JACKSON JEFFERSON KNOX LAKE LAWRENCE LICKING LOGAN
  LORAIN LUCAS MADISON MAHONING MARION MEDINA MEIGS MERCER MIAMI MONROE
  MONTGOMERY MORGAN MORROW MUSKINGUM NOBLE OTTAWA PAULDING PERRY PICKAWAY
  PIKE PORTAGE PREBLE PUTNAM RICHLAND ROSS SANDUSKY SCIOTO SENECA SHELBY
  STARK SUMMIT TRUMBULL TUSCARAWAS UNION VANWERT VINTON WARREN WASHINGTON
  WAYNE WILLIAMS WOOD WYANDOT
)

# ── Look up the Cloud Run service URL ───────────────────────────────────────
get_url() {
  local svc="$1"
  gcloud run services describe "$svc" \
    --project="$PROJECT_ID" --region="$REGION" \
    --format='value(status.url)'
}

# ── Fire one request per county with bounded parallelism (xargs -P) ─────────
enqueue() {
  local svc="$1"
  local url
  url=$(get_url "$svc")
  echo ""
  echo "🚀 Dispatching ${#COUNTIES[@]} counties to $svc ($url)"
  echo "   Parallelism: $PAR · each county timeout 1h · resume-safe"
  echo ""

  # printf + xargs is the most portable bounded-parallelism pattern in bash.
  # Each county prints its own success/failure line so you can grep the log.
  printf "%s\n" "${COUNTIES[@]}" | \
    xargs -P "$PAR" -I{} \
    bash -c '
      county="$1"; url="$2"
      start=$(date +%s)
      status=$(curl -s -o /tmp/resp_"$county".json -w "%{http_code}" \
        -X POST -H "Content-Type: application/json" \
        --max-time 3600 \
        "$url/process-county" \
        -d "{\"county\":\"$county\"}")
      elapsed=$(( $(date +%s) - start ))
      if [[ "$status" == "200" ]]; then
        echo "✓ $county ($elapsed s)"
      else
        echo "✗ $county — HTTP $status ($elapsed s): $(head -c 300 /tmp/resp_"$county".json)"
      fi
      rm -f /tmp/resp_"$county".json
    ' _ {} "$url"
}

case "$TARGET" in
  emissions)         enqueue emissions-worker ;;
  population)        enqueue population-worker ;;
  surface_anomalies) enqueue surface-anomalies-worker ;;
  pad_detection)     enqueue pad-detection-worker ;;
  both)
    enqueue emissions-worker
    enqueue surface-anomalies-worker ;;
esac

echo ""
echo "🎯 Done. Check progress in Supabase:"
echo "   SELECT COUNT(*) FROM well_remote_sensing WHERE emissions_processed_at IS NOT NULL;"
echo "   SELECT COUNT(*) FROM well_surface_anomalies;"
echo "   SELECT COUNT(*), AVG(pad_score) FROM well_pad_detection WHERE pad_score >= 30;"
