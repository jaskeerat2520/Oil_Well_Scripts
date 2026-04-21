#!/bin/bash

# deploy.sh
# Deploy terrain scoring worker to Cloud Run + set up Cloud Tasks queue

set -e

PROJECT_ID="gen-lang-client-0658746801"
SERVICE_NAME="terrain-worker"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "🔧 Deploying terrain worker to Cloud Run…"

# 1. Set GCP project
gcloud config set project ${PROJECT_ID}

# 2. Build & push Docker image to Container Registry
echo "📦 Building Docker image…"
docker build -t ${IMAGE_NAME} .

echo "🚀 Pushing to Container Registry…"
docker push ${IMAGE_NAME}

# 3. Deploy to Cloud Run
echo "☁️  Deploying to Cloud Run…"
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --memory 2Gi \
  --timeout 3600 \
  --set-env-vars "SUPABASE_DB_HOST=db.fdehtiqlmijdnfxzjufi.supabase.co,SUPABASE_DB_NAME=postgres,SUPABASE_DB_USER=postgres,GEE_PROJECT=gen-lang-client-0658746801" \
  --set-env-vars "SUPABASE_DB_PASSWORD=1k9xvYdmPDfeTTfc8g" \
  --allow-unauthenticated \
  --max-instances 10

echo "✅ Deployment complete!"
echo ""
echo "Service URL:"
gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)'
