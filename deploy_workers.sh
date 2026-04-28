#!/usr/bin/env bash
#
# deploy_workers.sh — build one image, deploy all 3 Cloud Run services.
#
# Reads the DB password from Secret Manager instead of hard-coding it. This
# replaces the old `deploy.sh` pattern where the password was --set-env-vars'd
# in plain text (and accidentally committed to git).
#
# Prerequisites (one-time setup):
#   1. gcloud auth login
#   2. Rotate your Supabase DB password, then run:
#        gcloud secrets create supabase-db-password --project=$PROJECT_ID \
#          --replication-policy=automatic
#        printf '%s' 'NEW_PASSWORD' | gcloud secrets versions add supabase-db-password \
#          --project=$PROJECT_ID --data-file=-
#   3. Grant the default Cloud Run service account read access:
#        gcloud secrets add-iam-policy-binding supabase-db-password \
#          --project=$PROJECT_ID \
#          --member="serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com" \
#          --role=roles/secretmanager.secretAccessor
#
# Usage:
#   deploy_workers.sh [terrain | emissions | population | surface_anomalies | pad_detection | all]
#
# Default: all

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gen-lang-client-0658746801}"
REGION="${REGION:-us-central1}"
IMAGE_NAME="gcr.io/${PROJECT_ID}/well-scoring-worker"

TARGET="${1:-all}"

# ── Build + push one image for all workers ──────────────────────────────────
echo "🔧 Building & pushing image $IMAGE_NAME …"
gcloud config set project "$PROJECT_ID" >/dev/null
docker build -t "$IMAGE_NAME" .
docker push "$IMAGE_NAME"

# ── Helper to deploy one Cloud Run service ──────────────────────────────────
deploy_service() {
  local worker_name="$1"      # e.g. emissions-worker
  local worker_script="$2"    # e.g. emissions_worker.py

  echo ""
  echo "☁️  Deploying $worker_name (script: $worker_script) to $REGION …"
  gcloud run deploy "$worker_name" \
    --image "$IMAGE_NAME" \
    --platform managed \
    --region "$REGION" \
    --memory 2Gi \
    --cpu 1 \
    --timeout 3600 \
    --max-instances 10 \
    --concurrency 1 \
    --allow-unauthenticated \
    --set-env-vars "WORKER_SCRIPT=$worker_script" \
    --set-env-vars "SUPABASE_DB_HOST=db.fdehtiqlmijdnfxzjufi.supabase.co,SUPABASE_DB_NAME=postgres,SUPABASE_DB_USER=postgres,GEE_PROJECT=$PROJECT_ID" \
    --set-secrets "SUPABASE_DB_PASSWORD=supabase-db-password:latest"

  echo "✅ $worker_name deployed."
  echo -n "URL: "
  gcloud run services describe "$worker_name" \
    --region "$REGION" --format='value(status.url)'
}

case "$TARGET" in
  terrain)           deploy_service terrain-worker           terrain_worker.py ;;
  emissions)         deploy_service emissions-worker         emissions_worker.py ;;
  population)        deploy_service population-worker        population_worker.py ;;
  surface_anomalies) deploy_service surface-anomalies-worker surface_anomalies_worker.py ;;
  pad_detection)     deploy_service pad-detection-worker     pad_detection_worker.py ;;
  all)
    deploy_service terrain-worker           terrain_worker.py
    deploy_service emissions-worker         emissions_worker.py
    deploy_service population-worker        population_worker.py
    deploy_service surface-anomalies-worker surface_anomalies_worker.py
    deploy_service pad-detection-worker     pad_detection_worker.py ;;
  *)
    echo "Usage: $0 [terrain | emissions | population | surface_anomalies | pad_detection | all]"
    exit 1 ;;
esac

echo ""
echo "🎯 Done. To enqueue all 88 counties, run: ./enqueue_counties.sh"
