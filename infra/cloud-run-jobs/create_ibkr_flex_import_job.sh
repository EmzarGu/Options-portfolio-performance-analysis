#!/usr/bin/env bash
set -euo pipefail

# Creates or updates the IBKR import Cloud Run Job after the backend image
# containing portfolio_backend.ibkr.import_job has been built and pushed.

PROJECT_ID="${PROJECT_ID:-options-performance-dashboard}"
REGION="${REGION:-europe-west6}"
JOB_NAME="${JOB_NAME:-ibkr-flex-import}"
SERVICE_NAME="${SERVICE_NAME:-options-roi-mobile-api}"
RUNTIME_SA="${RUNTIME_SA:-595990983720-compute@developer.gserviceaccount.com}"
RAW_BUCKET="${IBKR_RAW_BUCKET:-options-portfolio-ibkr-raw-595990983720}"
: "${IBKR_IMPORT_INCEPTION_DATE:?Set IBKR_IMPORT_INCEPTION_DATE=YYYY-MM-DD to the agreed IBKR import inception date.}"
IMPORT_INCEPTION_DATE="${IBKR_IMPORT_INCEPTION_DATE}"
IMPORT_RECENT_OVERLAP_DAYS="${IBKR_IMPORT_RECENT_OVERLAP_DAYS:-14}"
IMPORT_TO_OFFSET_DAYS="${IBKR_IMPORT_TO_OFFSET_DAYS:-1}"

IMAGE_URI="${IMAGE_URI:-$(gcloud run services describe "${SERVICE_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format='value(spec.template.spec.containers[0].image)')}"

COMMON_FLAGS=(
  --project="${PROJECT_ID}"
  --region="${REGION}"
  --image="${IMAGE_URI}"
  --service-account="${RUNTIME_SA}"
  --command=python
  --args=-m,portfolio_backend.ibkr.import_job
  --set-env-vars="IBKR_RAW_BUCKET=${RAW_BUCKET},IBKR_IMPORT_INCEPTION_DATE=${IMPORT_INCEPTION_DATE},IBKR_IMPORT_RECENT_OVERLAP_DAYS=${IMPORT_RECENT_OVERLAP_DAYS},IBKR_IMPORT_TO_OFFSET_DAYS=${IMPORT_TO_OFFSET_DAYS}"
  --set-secrets="IBKR_FLEX_TOKEN=ibkr-flex-token:latest,IBKR_FLEX_QUERY_ID=ibkr-flex-query-id:latest"
  --tasks=1
  --max-retries=1
  --task-timeout=7200s
)

if gcloud run jobs describe "${JOB_NAME}" --project="${PROJECT_ID}" --region="${REGION}" >/dev/null 2>&1; then
  gcloud run jobs update "${JOB_NAME}" "${COMMON_FLAGS[@]}"
else
  gcloud run jobs create "${JOB_NAME}" "${COMMON_FLAGS[@]}"
fi
