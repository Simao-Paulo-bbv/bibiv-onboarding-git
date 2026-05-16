#!/usr/bin/env bash
# Source this file to use the BIBIV Onboarding GCP context without changing
# your global gcloud defaults:
#
#   source scripts/use-gcp-bibiv-onboarding.sh
#   bibiv_logs_errors
#
# The service account is keyless. Access is granted through IAM impersonation.

export BIBIV_GCP_PROJECT_ID="bibiv-onboarding-app"
export BIBIV_GCP_PROJECT_NUMBER="205037587961"
export BIBIV_GCP_LOGS_SERVICE_ACCOUNT="codex-logs-reader@bibiv-onboarding-app.iam.gserviceaccount.com"
export BIBIV_GCP_IMPERSONATE_ARGS="--project=${BIBIV_GCP_PROJECT_ID} --impersonate-service-account=${BIBIV_GCP_LOGS_SERVICE_ACCOUNT}"

bibiv_gcloud() {
  gcloud "$@" \
    --project="${BIBIV_GCP_PROJECT_ID}" \
    --impersonate-service-account="${BIBIV_GCP_LOGS_SERVICE_ACCOUNT}"
}

bibiv_logs_errors() {
  bibiv_gcloud logging read \
    'resource.type="app_script_function" AND severity>=ERROR' \
    --limit="${1:-100}" \
    --format=json
}

bibiv_logs_docgen() {
  bibiv_gcloud logging read \
    'resource.type="app_script_function" AND resource.labels.function_name="processNextQueuedDocGenerationJob"' \
    --limit="${1:-100}" \
    --format=json
}

bibiv_logs_function() {
  local function_name="$1"
  local limit="${2:-100}"
  if [[ -z "${function_name}" ]]; then
    echo "Usage: bibiv_logs_function <function_name> [limit]" >&2
    return 2
  fi

  bibiv_gcloud logging read \
    "resource.type=\"app_script_function\" AND resource.labels.function_name=\"${function_name}\"" \
    --limit="${limit}" \
    --format=json
}

bibiv_logs_search() {
  local text="$1"
  local limit="${2:-100}"
  if [[ -z "${text}" ]]; then
    echo "Usage: bibiv_logs_search <text> [limit]" >&2
    return 2
  fi

  bibiv_gcloud logging read \
    "resource.type=\"app_script_function\" AND jsonPayload.message:\"${text}\"" \
    --limit="${limit}" \
    --format=json
}

echo "BIBIV GCP context loaded: ${BIBIV_GCP_PROJECT_ID}"
echo "Using keyless impersonation: ${BIBIV_GCP_LOGS_SERVICE_ACCOUNT}"
