#!/usr/bin/env bash

# Reproducible pipeline script for HF-EOLUS interpolation experiments.
# All case-specific values are configured through environment variables so the
# toolkit remains independent from the bundled case study inside case_study/.

set -euo pipefail
export AWS_PAGER=""

die() {
  echo "Error: $*" >&2
  exit 1
}

require_var() {
  local key="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    die "Set ${key} before running this script."
  fi
}

split_s3_uri() {
  if [[ "$1" != s3://* ]]; then
    die "Expected an s3:// URI, got '$1'"
  fi
  local uri="${1#s3://}"
  local bucket="${uri%%/*}"
  local key=""
  if [[ "$uri" != "$bucket" ]]; then
    key="${uri#*/}"
  fi
  # Trim trailing slashes from the key to avoid duplicate delimiters.
  key="${key%%/}"
  printf '%s %s' "$bucket" "$key"
}

PROFILE=${PIPELINE_PROFILE:-hf_eolus}
AWS_REGION=${PIPELINE_AWS_REGION:-$(aws configure get region --profile "$PROFILE" 2>/dev/null || echo "eu-west-3")}
BUCKET_NAME=${PIPELINE_BUCKET_NAME:-}
require_var PIPELINE_BUCKET_NAME "$BUCKET_NAME"

LAMBDA_FUNCTION=${PIPELINE_LAMBDA_FUNCTION:-MeteoGaliciaProcessor}
STATE_MACHINE_NAME=${PIPELINE_STATE_MACHINE_NAME:-MeteoGaliciaStateMachine}
ECR_REPO=${PIPELINE_ECR_REPO:-meteogalicia-processor}
LAMBDA_DEST_PREFIX=${PIPELINE_LAMBDA_DEST_PREFIX:-meteogalicia/data}

INGEST_START=${PIPELINE_INGEST_START:-}
INGEST_END=${PIPELINE_INGEST_END:-}
require_var PIPELINE_INGEST_START "$INGEST_START"
require_var PIPELINE_INGEST_END "$INGEST_END"
MODEL_CHOICE=${PIPELINE_MODEL:-wrf4km}
BOUNDARY_FILE=${PIPELINE_BOUNDARY_FILE:-}
REGION_NAME=${PIPELINE_REGION_NAME:-}
TEST_POINTS_FILE=${PIPELINE_TEST_POINTS_FILE:-}

INTERP_START=${PIPELINE_INTERP_START:-$INGEST_START}
INTERP_END=${PIPELINE_INTERP_END:-$INGEST_END}
require_var PIPELINE_INTERP_START "$INTERP_START"
require_var PIPELINE_INTERP_END "$INTERP_END"

INPUT_PREFIX=${PIPELINE_INPUT_PREFIX:-"s3://${BUCKET_NAME}/meteogalicia/data/"}
OUTPUT_PREFIX=${PIPELINE_OUTPUT_PREFIX:-"s3://${BUCKET_NAME}/meteogalicia/interpolation"}
PLOTS_PREFIX=${PIPELINE_PLOTS_PREFIX:-"s3://${BUCKET_NAME}/meteogalicia/interp_plots"}
METADATA_PREFIX=${PIPELINE_METADATA_PREFIX:-"s3://${BUCKET_NAME}/meteogalicia/metadata/interpolation"}

RES_FACTOR=${PIPELINE_RES_FACTOR:-8}
CUTOFF_KM=${PIPELINE_CUTOFF_KM:-20}
WIDTH_KM=${PIPELINE_WIDTH_KM:-1}
SUBSAMPLE_PCT=${PIPELINE_SUBSAMPLE_PCT:-50}
NFOLD=${PIPELINE_NFOLD:-5}
NMAX_MODEL=${PIPELINE_NMAX_MODEL:-32}

STAC_OUTPUT_DIR=${PIPELINE_STAC_OUTPUT_DIR:-case_study/catalogs/meteogalicia_interpolation}
COLLECTION_ID=${PIPELINE_COLLECTION_ID:-meteogalicia-interpolation}
COLLECTION_TITLE=${PIPELINE_COLLECTION_TITLE:-"HF-EOLUS MeteoGalicia Interpolation"}
TEMPORAL_START=${PIPELINE_TEMPORAL_START:-"${INTERP_START}T00:00:00Z"}
TEMPORAL_END=${PIPELINE_TEMPORAL_END:-"${INTERP_END}T23:00:00Z"}
ITEM_OVERRIDES=${PIPELINE_STAC_ITEM_OVERRIDES:-case_study/stac_overrides/item_override.json}
COLLECTION_OVERRIDES=${PIPELINE_STAC_COLLECTION_OVERRIDES:-case_study/stac_overrides/collection_override.json}

BUOY_CONFIG=${PIPELINE_BUOY_CONFIG:-}
BUOY_REPORTS_DIR=${PIPELINE_BUOY_REPORTS_DIR:-reports/buoys}
BUOY_PREDICTION_PATH=${PIPELINE_BUOY_PREDICTION_PATH:-local_sync}

echo "[pipeline] Using profile=${PROFILE}, bucket=${BUCKET_NAME}, region=${AWS_REGION}"

echo "[pipeline] Running unit tests..."
./scripts/00-verify_tests.sh

echo "[pipeline] Creating IAM roles..."
./scripts/01-create_roles.sh -p "$PROFILE" -b "$BUCKET_NAME"

echo "[pipeline] Deploying Lambda ingestion function..."
./scripts/02-deploy_lambda.sh \
  -p "$PROFILE" \
  -b "$BUCKET_NAME" \
  -r "$ECR_REPO" \
  -f "$LAMBDA_FUNCTION" \
  -x "$LAMBDA_DEST_PREFIX" \
  -k

echo "[pipeline] Waiting for Lambda to become active..."
aws lambda wait function-active --function-name "$LAMBDA_FUNCTION" --profile "$PROFILE" --region "$AWS_REGION"

echo "[pipeline] Creating Step Functions state machine..."
./scripts/03-create_state_machine.sh -p "$PROFILE" -n "$STATE_MACHINE_NAME" -f "$LAMBDA_FUNCTION"

echo "[pipeline] Running ingestion via Step Functions..."
INGEST_ARGS=(-p "$PROFILE" -n "$STATE_MACHINE_NAME" -s "$INGEST_START" -e "$INGEST_END" -m "$MODEL_CHOICE")
if [[ -n "$BOUNDARY_FILE" ]]; then
  INGEST_ARGS+=(-g "$BOUNDARY_FILE")
  if [[ -n "$REGION_NAME" ]]; then
    INGEST_ARGS+=(-r "$REGION_NAME")
  fi
fi
if [[ -n "$TEST_POINTS_FILE" ]]; then
  INGEST_ARGS+=(-t "$TEST_POINTS_FILE")
fi
./scripts/04-run_pipeline.sh "${INGEST_ARGS[@]}"

echo "[pipeline] Running AWS Batch setup..."
./scripts/07-setup_aws_resources.sh -p "$PROFILE" -r "$AWS_REGION"

echo "[pipeline] Launching interpolation batch..."
INTERP_ARGS=(
  -p "$PROFILE"
  -r "$RES_FACTOR"
  -c "$CUTOFF_KM"
  -w "$WIDTH_KM"
  -n "$SUBSAMPLE_PCT"
  -F "$NFOLD"
  -m "$NMAX_MODEL"
  --start "$INTERP_START"
  --end "$INTERP_END"
  --input-path "$INPUT_PREFIX"
  --output-path "$OUTPUT_PREFIX"
  --plots-root "$PLOTS_PREFIX"
  -v
)
if [[ -n "$REGION_NAME" ]]; then
  INTERP_ARGS+=(--region-name "$REGION_NAME")
fi
./scripts/08-run_interpolation.sh "${INTERP_ARGS[@]}"

read -r OUTPUT_BUCKET OUTPUT_KEY <<<"$(split_s3_uri "$OUTPUT_PREFIX")"

echo "[pipeline] Generating STAC catalog for interpolation outputs..."
STAC_ARGS=(
  --aws-profile "$PROFILE"
  --input-root "$OUTPUT_PREFIX"
  --output-dir "$STAC_OUTPUT_DIR"
  --collection-id "$COLLECTION_ID"
  --collection-title "$COLLECTION_TITLE"
  --temporal-start "$TEMPORAL_START"
  --temporal-end "$TEMPORAL_END"
  --metadata-prefix "$METADATA_PREFIX"
  --plots-prefix "$PLOTS_PREFIX"
)
if [[ -n "$ITEM_OVERRIDES" ]]; then
  if [[ ! -f "$ITEM_OVERRIDES" ]]; then
    die "Item overrides file not found at $ITEM_OVERRIDES"
  fi
  STAC_ARGS+=(--item-overrides "$ITEM_OVERRIDES")
fi
if [[ -n "$COLLECTION_OVERRIDES" ]]; then
  if [[ ! -f "$COLLECTION_OVERRIDES" ]]; then
    die "Collection overrides file not found at $COLLECTION_OVERRIDES"
  fi
  STAC_ARGS+=(--collection-overrides "$COLLECTION_OVERRIDES")
fi
./scripts/run_build_stac_catalog.sh "${STAC_ARGS[@]}"

if [[ -n "$BUOY_CONFIG" ]]; then
  echo "[pipeline] Running buoy comparisons defined in $BUOY_CONFIG"
  if [[ ! -f "$BUOY_CONFIG" ]]; then
    die "Buoy comparison config not found at $BUOY_CONFIG"
  fi
  if [[ ! -d "$BUOY_PREDICTION_PATH" && ! -f "$BUOY_PREDICTION_PATH" ]]; then
    die "Buoy comparison prediction path not found at $BUOY_PREDICTION_PATH"
  fi
  Rscript --vanilla scripts/compare_pde_buoy.R \
    --buoy-config "$BUOY_CONFIG" \
    --prediction-path "$BUOY_PREDICTION_PATH" \
    --output-dir "$BUOY_REPORTS_DIR"
fi

echo "[pipeline] Pipeline completed successfully."
