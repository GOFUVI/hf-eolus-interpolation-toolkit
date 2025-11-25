#!/usr/bin/env bash
set -euo pipefail

# Wrapper to execute 11-build_stac_catalog.py inside a Docker container that
# ships Python dependencies (pyarrow, pystac, shapely). Handles optional
# synchronization of S3 prefixes for data, metadata sidecars and diagnostic
# plots, mirroring the structure expected by the STAC builder.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required to run this wrapper." >&2
  exit 1
fi

SYNC_ENABLED=1
SYNC_SOURCE=""
SYNC_TARGET=""
INPUT_ROOT=""
INPUT_ROOT_INDEX=-1
INPUT_ROOT_FORMAT=""
AWS_PROFILE_OPT=""
AWS_REGION_OPT=""
HOST_INPUT_ROOT=""
CONTAINER_INPUT_ROOT=""
OUTPUT_DIR=""
OUTPUT_DIR_INDEX=-1
OUTPUT_DIR_FORMAT=""
SYNC_TARGET_SET=0
declare -a ARG_BUFFER=()

METADATA_PREFIX_ORIGINAL=""
METADATA_ARG_INDEX=-1
METADATA_ARG_FORMAT=""
METADATA_IS_S3=0
METADATA_ARG_NAME="--metadata-prefix"

PLOTS_PREFIX_ORIGINAL=""
PLOTS_ARG_INDEX=-1
PLOTS_ARG_FORMAT=""
PLOTS_IS_S3=0
PLOTS_ARG_NAME="--plots-prefix"

ASSET_HREF_PREFIX=""
ASSET_HREF_INDEX=-1
ASSET_HREF_FORMAT=""

BY_YEAR=0
YEAR_FILTER=""

map_to_container_path() {
  local path="$1"
  if [[ "$path" == s3://* || "$path" == http://* || "$path" == https://* ]]; then
    echo "$path"
    return
  fi
  if [[ "$path" == /* ]]; then
    if [[ "$path" == "$REPO_ROOT"* ]]; then
      local rel="${path#$REPO_ROOT/}"
      if [[ -n "$rel" ]]; then
        echo "/workspace/${rel}"
      else
        echo "/workspace"
      fi
    else
      echo "$path"
    fi
  else
    echo "$path"
  fi
}

compute_host_container_paths() {
  local target="$1"
  local host_var="$2"
  local container_var="$3"
  local host_path=""
  local container_path=""
  if [[ "$target" == /* ]]; then
    host_path="$target"
    container_path="$target"
  else
    host_path="${REPO_ROOT}/${target}"
    container_path="/workspace/${target}"
  fi
  printf -v "$host_var" '%s' "$host_path"
  printf -v "$container_var" '%s' "$container_path"
}

assign_option_value() {
  local option="$1"
  local index="$2"
  local format="$3"
  local value="$4"
  if [ "$index" -lt 0 ]; then
    return
  fi
  if [ "$format" = "space" ]; then
    ARG_BUFFER[$((index + 1))]="$value"
  else
    ARG_BUFFER[$index]="${option}=${value}"
  fi
}

set_option_value_or_append() {
  local option="$1"
  local index="$2"
  local format="$3"
  local value="$4"
  if [ "$index" -ge 0 ]; then
    assign_option_value "$option" "$index" "$format" "$value"
  else
    if [ "$format" = "equals" ]; then
      ARG_BUFFER+=("${option}=${value}")
    else
      ARG_BUFFER+=("$option" "$value")
    fi
  fi
}

resolve_host_path() {
  local path="$1"
  if [[ "$path" == /* ]]; then
    echo "$path"
  else
    echo "${REPO_ROOT}/${path}"
  fi
}

count_items_for_year() {
  local year="$1"
  local items_root="$2"
  if [[ ! -d "$items_root" ]]; then
    echo 0
    return
  fi
  python3 - "$year" "$items_root" <<'PY'
import sys, os
year = sys.argv[1]
root = sys.argv[2]
count = 0
for current, _, files in os.walk(root):
    for name in files:
        if not name.endswith(".json"):
            continue
        parent = os.path.basename(os.path.dirname(os.path.join(current, name)))
        if parent.startswith(year):
            count += 1
print(count)
PY
}

count_source_parquet_for_year() {
  local year="$1"
  local input_prefix="$2"
  if [[ "$input_prefix" == s3://* ]]; then
    if [ ${#AWS_CMD_BASE[@]} -eq 0 ]; then
      echo 0
      return
    fi
    "${AWS_CMD_BASE[@]}" s3 ls "${input_prefix%/}/year=${year}/" --recursive | awk '/data\.parquet$/ {c++} END {print c+0}'
  else
    local host_src
    host_src=$(resolve_host_path "$input_prefix")
    if [[ ! -d "$host_src" ]]; then
      echo 0
      return
    fi
    find "${host_src}/year=${year}" -name 'data.parquet' -type f 2>/dev/null | wc -l | awk '{print $1+0}'
  fi
}

IMAGE_NAME=${STAC_IMAGE_NAME:-wind-interpolation-stac-dkr:latest}

ensure_image() {
  local image="$1"
  local needs_build=0
  if ! docker image inspect "${image}" >/dev/null 2>&1; then
    needs_build=1
  else
    if ! docker run --rm "${image}" sh -c "command -v docker >/dev/null 2>&1"; then
      needs_build=1
    fi
  fi
  if [ "$needs_build" -eq 1 ]; then
    docker build -t "${image}" - <<'EOF'
FROM python:3.11-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir \
    "pyarrow>=14.0.2" \
    "pystac>=1.8.5" \
    "shapely>=2.0.0" \
    "duckdb>=0.10.2"
WORKDIR /workspace
EOF
  fi
}

ORIG_ARGS=("$@")
IDX=0
while [ $IDX -lt ${#ORIG_ARGS[@]} ]; do
  arg=${ORIG_ARGS[$IDX]}
  case "$arg" in
    --skip-sync)
      SYNC_ENABLED=0
      IDX=$((IDX + 1))
      ;;
    --sync-prefix)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --sync-prefix requires a value." >&2
        exit 1
      fi
      SYNC_SOURCE=${ORIG_ARGS[$((IDX + 1))]}
      IDX=$((IDX + 2))
      ;;
    --sync-prefix=*)
      SYNC_SOURCE=${arg#*=}
      IDX=$((IDX + 1))
      ;;
    --sync-target)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --sync-target requires a value." >&2
        exit 1
      fi
      SYNC_TARGET=${ORIG_ARGS[$((IDX + 1))]}
      SYNC_TARGET_SET=1
      IDX=$((IDX + 2))
      ;;
    --sync-target=*)
      SYNC_TARGET=${arg#*=}
      SYNC_TARGET_SET=1
      IDX=$((IDX + 1))
      ;;
    --input-root)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --input-root requires a value." >&2
        exit 1
      fi
      INPUT_ROOT=${ORIG_ARGS[$((IDX + 1))]}
      INPUT_ROOT_INDEX=${#ARG_BUFFER[@]}
      INPUT_ROOT_FORMAT="space"
      ARG_BUFFER+=("--input-root" "__INPUT_ROOT_PLACEHOLDER__")
      IDX=$((IDX + 2))
      ;;
    --input-root=*)
      INPUT_ROOT=${arg#*=}
      INPUT_ROOT_INDEX=${#ARG_BUFFER[@]}
      INPUT_ROOT_FORMAT="equals"
      ARG_BUFFER+=("__INPUT_ROOT_PLACEHOLDER__")
      IDX=$((IDX + 1))
      ;;
    --metadata-prefix)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --metadata-prefix requires a value." >&2
        exit 1
      fi
      value=${ORIG_ARGS[$((IDX + 1))]}
      METADATA_PREFIX_ORIGINAL="$value"
      METADATA_ARG_INDEX=${#ARG_BUFFER[@]}
      METADATA_ARG_FORMAT="space"
      if [[ "$value" == s3://* ]]; then
        METADATA_IS_S3=1
        ARG_BUFFER+=("--metadata-prefix" "__METADATA_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--metadata-prefix" "$container_value")
      fi
      IDX=$((IDX + 2))
      ;;
    --metadata-prefix=*)
      value=${arg#*=}
      METADATA_PREFIX_ORIGINAL="$value"
      METADATA_ARG_INDEX=${#ARG_BUFFER[@]}
      METADATA_ARG_FORMAT="equals"
      if [[ "$value" == s3://* ]]; then
        METADATA_IS_S3=1
        ARG_BUFFER+=("__METADATA_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--metadata-prefix=$container_value")
      fi
      IDX=$((IDX + 1))
      ;;
    --plots-prefix)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --plots-prefix requires a value." >&2
        exit 1
      fi
      value=${ORIG_ARGS[$((IDX + 1))]}
      PLOTS_PREFIX_ORIGINAL="$value"
      PLOTS_ARG_INDEX=${#ARG_BUFFER[@]}
      PLOTS_ARG_FORMAT="space"
      if [[ "$value" == s3://* ]]; then
        PLOTS_IS_S3=1
        ARG_BUFFER+=("--plots-prefix" "__PLOTS_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--plots-prefix" "$container_value")
      fi
      IDX=$((IDX + 2))
      ;;
    --plots-prefix=*)
      value=${arg#*=}
      PLOTS_PREFIX_ORIGINAL="$value"
      PLOTS_ARG_INDEX=${#ARG_BUFFER[@]}
      PLOTS_ARG_FORMAT="equals"
      if [[ "$value" == s3://* ]]; then
        PLOTS_IS_S3=1
        ARG_BUFFER+=("__PLOTS_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--plots-prefix=$container_value")
      fi
      IDX=$((IDX + 1))
      ;;
    --asset-href-prefix)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --asset-href-prefix requires a value." >&2
        exit 1
      fi
      value=${ORIG_ARGS[$((IDX + 1))]}
      ASSET_HREF_PREFIX="$value"
      ASSET_HREF_INDEX=${#ARG_BUFFER[@]}
      ASSET_HREF_FORMAT="space"
      container_value=$(map_to_container_path "$value")
      ARG_BUFFER+=("--asset-href-prefix" "$container_value")
      IDX=$((IDX + 2))
      ;;
    --asset-href-prefix=*)
      value=${arg#*=}
      ASSET_HREF_PREFIX="$value"
      ASSET_HREF_INDEX=${#ARG_BUFFER[@]}
      ASSET_HREF_FORMAT="equals"
      container_value=$(map_to_container_path "$value")
      ARG_BUFFER+=("--asset-href-prefix=$container_value")
      IDX=$((IDX + 1))
      ;;
    --by-year)
      BY_YEAR=1
      IDX=$((IDX + 1))
      ;;
    --years)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --years requires a comma-separated list." >&2
        exit 1
      fi
      BY_YEAR=1
      YEAR_FILTER=${ORIG_ARGS[$((IDX + 1))]}
      IDX=$((IDX + 2))
      ;;
    --years=*)
      BY_YEAR=1
      YEAR_FILTER=${arg#*=}
      IDX=$((IDX + 1))
      ;;
    --aws-profile)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --aws-profile requires a value." >&2
        exit 1
      fi
      AWS_PROFILE_OPT=${ORIG_ARGS[$((IDX + 1))]}
      IDX=$((IDX + 2))
      ;;
    --aws-profile=*)
      AWS_PROFILE_OPT=${arg#*=}
      IDX=$((IDX + 1))
      ;;
    --aws-region)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --aws-region requires a value." >&2
        exit 1
      fi
      AWS_REGION_OPT=${ORIG_ARGS[$((IDX + 1))]}
      IDX=$((IDX + 2))
      ;;
    --aws-region=*)
      AWS_REGION_OPT=${arg#*=}
      IDX=$((IDX + 1))
      ;;
    --output-dir)
      if [ $((IDX + 1)) -ge ${#ORIG_ARGS[@]} ]; then
        echo "Error: --output-dir requires a value." >&2
        exit 1
      fi
      OUTPUT_DIR=${ORIG_ARGS[$((IDX + 1))]}
      OUTPUT_DIR_INDEX=${#ARG_BUFFER[@]}
      OUTPUT_DIR_FORMAT="space"
      ARG_BUFFER+=("--output-dir" "$OUTPUT_DIR")
      IDX=$((IDX + 2))
      ;;
    --output-dir=*)
      OUTPUT_DIR=${arg#*=}
      OUTPUT_DIR_INDEX=${#ARG_BUFFER[@]}
      OUTPUT_DIR_FORMAT="equals"
      ARG_BUFFER+=("--output-dir=$OUTPUT_DIR")
      IDX=$((IDX + 1))
      ;;
    *)
      ARG_BUFFER+=("$arg")
      IDX=$((IDX + 1))
      ;;
  esac
done

if [ -z "${INPUT_ROOT}" ]; then
  echo "Error: --input-root must be provided to determine catalogue location." >&2
  exit 1
fi

HOST_INPUT_ROOT="${INPUT_ROOT}"
CONTAINER_INPUT_ROOT="${INPUT_ROOT}"

if [ "${SYNC_ENABLED}" -eq 1 ] && [ -z "${SYNC_SOURCE}" ] && [[ "${INPUT_ROOT}" == s3://* ]]; then
  SYNC_SOURCE="${INPUT_ROOT}"
fi

ASSETS_BASE_DEFAULT=""
if [ -n "${OUTPUT_DIR}" ]; then
  ASSETS_BASE_DEFAULT="${OUTPUT_DIR%/}/assets"
fi

DATA_SYNC_DEFAULT="${ASSETS_BASE_DEFAULT:+${ASSETS_BASE_DEFAULT}/parquet}"
if [ -z "${DATA_SYNC_DEFAULT}" ]; then
  DATA_SYNC_DEFAULT="local_sync"
fi

METADATA_SYNC_DEFAULT="${ASSETS_BASE_DEFAULT:+${ASSETS_BASE_DEFAULT}/metadata}"
if [ -z "${METADATA_SYNC_DEFAULT}" ]; then
  METADATA_SYNC_DEFAULT="local_sync_metadata"
fi

PLOTS_SYNC_DEFAULT="${ASSETS_BASE_DEFAULT:+${ASSETS_BASE_DEFAULT}/plots}"
if [ -z "${PLOTS_SYNC_DEFAULT}" ]; then
  PLOTS_SYNC_DEFAULT="local_sync_plots"
fi

NEED_S3_SYNC=0
if [ "${SYNC_ENABLED}" -eq 1 ]; then
  if [ -n "${SYNC_SOURCE}" ]; then
    NEED_S3_SYNC=1
  fi
  if [ "${METADATA_IS_S3}" -eq 1 ]; then
    NEED_S3_SYNC=1
  fi
  if [ "${PLOTS_IS_S3}" -eq 1 ]; then
    NEED_S3_SYNC=1
  fi
fi

if [ "${NEED_S3_SYNC}" -eq 1 ]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "Error: aws CLI is required to sync requested S3 prefixes." >&2
    exit 1
  fi
  AWS_CMD_BASE=(aws)
  if [ -n "${AWS_PROFILE_OPT}" ]; then
    AWS_CMD_BASE+=("--profile" "${AWS_PROFILE_OPT}")
  elif [ -n "${AWS_PROFILE}" ]; then
    AWS_PROFILE_OPT="${AWS_PROFILE}"
    AWS_CMD_BASE+=("--profile" "${AWS_PROFILE}")
  fi
  EFFECTIVE_REGION="${AWS_REGION_OPT}"
  if [ -z "${EFFECTIVE_REGION}" ]; then
    PROFILE_FOR_REGION="${AWS_PROFILE_OPT}"
    if [ -z "${PROFILE_FOR_REGION}" ] && [ -n "${AWS_PROFILE}" ]; then
      PROFILE_FOR_REGION="${AWS_PROFILE}"
    fi
    if [ -n "${PROFILE_FOR_REGION}" ]; then
      EFFECTIVE_REGION=$(aws configure get region --profile "${PROFILE_FOR_REGION}" 2>/dev/null || true)
    fi
    if [ -z "${EFFECTIVE_REGION}" ] && [ -n "${AWS_REGION}" ]; then
      EFFECTIVE_REGION="${AWS_REGION}"
    fi
  fi
  if [ -n "${EFFECTIVE_REGION}" ]; then
    AWS_CMD_BASE+=("--region" "${EFFECTIVE_REGION}")
  fi
fi

if [ "${BY_YEAR}" -eq 1 ]; then
  WORK_BASE="${OUTPUT_DIR:-tmp}"
  WORK_BASE="${WORK_BASE%/}"
  if [[ "${WORK_BASE}" != /* ]]; then
    WORK_BASE="${REPO_ROOT}/${WORK_BASE}"
  fi
  YEAR_STAGE_BASE="${WORK_BASE}/.stac_year_build"
  mkdir -p "${YEAR_STAGE_BASE}"

  if [ "${SYNC_ENABLED}" -eq 0 ] && [[ "${INPUT_ROOT}" == s3://* ]]; then
    echo "Error: --by-year with an s3:// input-root requires sync; omit --skip-sync." >&2
    exit 1
  fi
  if [ "${SYNC_ENABLED}" -eq 0 ] && { [ "${METADATA_IS_S3}" -eq 1 ] || [ "${PLOTS_IS_S3}" -eq 1 ]; }; then
    echo "Error: --by-year with S3 metadata/plots requires sync; omit --skip-sync." >&2
    exit 1
  fi

  if [ -z "${ASSET_HREF_PREFIX}" ]; then
    ASSET_HREF_PREFIX="${INPUT_ROOT}"
    ASSET_HREF_FORMAT="space"
  fi
  href_value=$(map_to_container_path "${ASSET_HREF_PREFIX}")
  set_option_value_or_append "--asset-href-prefix" "${ASSET_HREF_INDEX}" "${ASSET_HREF_FORMAT:-space}" "${href_value}"

  INCREMENTAL_PRESENT=0
  for arg in "${ARG_BUFFER[@]}"; do
    if [ "$arg" = "--incremental" ]; then
      INCREMENTAL_PRESENT=1
      break
    fi
  done
  if [ $INCREMENTAL_PRESENT -eq 0 ]; then
    ARG_BUFFER+=("--incremental")
  fi

  YEARS=()
  if [ -n "${YEAR_FILTER}" ]; then
    IFS=',' read -r -a YEARS <<< "${YEAR_FILTER}"
  else
    if [[ "${INPUT_ROOT}" == s3://* ]]; then
      if [ ${#AWS_CMD_BASE[@]} -eq 0 ]; then
        echo "Error: aws CLI is required to list years from S3 input-root." >&2
        exit 1
      fi
      while IFS= read -r line; do
        candidate=$(echo "$line" | awk '/PRE *year=/{print $2}')
        if [[ -n "$candidate" ]]; then
          year_val=${candidate#year=}
          year_val=${year_val%/}
          YEARS+=("$year_val")
        fi
      done < <("${AWS_CMD_BASE[@]}" s3 ls "${INPUT_ROOT%/}/")
    else
      HOST_BASE=$(resolve_host_path "${INPUT_ROOT}")
      if [ -d "$HOST_BASE" ]; then
        while IFS= read -r dir; do
          base=$(basename "$dir")
          year_val=${base#year=}
          YEARS+=("$year_val")
        done < <(find "$HOST_BASE" -maxdepth 1 -type d -name 'year=*')
      fi
    fi
  fi

  if [ ${#YEARS[@]} -eq 0 ]; then
    echo "Error: could not determine years to process; specify --years or ensure year=YYYY partitions exist." >&2
    exit 1
  fi

  echo "Year-by-year mode enabled. Years: ${YEARS[*]}"

  ensure_image "${IMAGE_NAME}"

  HOST_OUTPUT_DIR=$(resolve_host_path "${OUTPUT_DIR:-tmp}")
  PARQUET_ITEMS_DIR="${HOST_OUTPUT_DIR%/}/items/parquet"

  for YEAR in "${YEARS[@]}"; do
    EXISTING_COUNT=$(count_items_for_year "$YEAR" "$PARQUET_ITEMS_DIR")
    SOURCE_COUNT=$(count_source_parquet_for_year "$YEAR" "$INPUT_ROOT")
    if [[ $EXISTING_COUNT -gt 0 && $SOURCE_COUNT -gt 0 && $EXISTING_COUNT -eq $SOURCE_COUNT ]]; then
      echo "[${YEAR}] Items already present (${EXISTING_COUNT}/${SOURCE_COUNT}); skipping sync/build for this year."
      continue
    fi
    echo "[${YEAR}] Existing items: ${EXISTING_COUNT}, source parquet: ${SOURCE_COUNT}. Proceeding with build."

    STAGE_ROOT="${YEAR_STAGE_BASE}/year_${YEAR}"
    PARQUET_STAGE="${STAGE_ROOT}/parquet"
    METADATA_STAGE="${STAGE_ROOT}/metadata"
    PLOTS_STAGE="${STAGE_ROOT}/plots"
    mkdir -p "${PARQUET_STAGE}"

    if [[ "${INPUT_ROOT}" == s3://* ]]; then
      SRC="${INPUT_ROOT%/}/year=${YEAR}"
      TGT="${PARQUET_STAGE}/year=${YEAR}"
      echo "[${YEAR}] Syncing assets: ${SRC} -> ${TGT}"
      "${AWS_CMD_BASE[@]}" s3 sync "${SRC}" "${TGT}"
    else
      SRC=$(resolve_host_path "${INPUT_ROOT}")
      if [ ! -d "${SRC}/year=${YEAR}" ]; then
        echo "Warning: assets for year=${YEAR} not found under ${SRC}; skipping this year."
        continue
      fi
      if command -v rsync >/dev/null 2>&1; then
        rsync -a "${SRC}/year=${YEAR}" "${PARQUET_STAGE}/"
      else
        cp -a "${SRC}/year=${YEAR}" "${PARQUET_STAGE}/"
      fi
    fi

    if [ -n "${METADATA_PREFIX_ORIGINAL}" ]; then
      mkdir -p "${METADATA_STAGE}"
      if [[ "${METADATA_PREFIX_ORIGINAL}" == s3://* ]]; then
        SRC="${METADATA_PREFIX_ORIGINAL%/}/year=${YEAR}"
        TGT="${METADATA_STAGE}/year=${YEAR}"
        echo "[${YEAR}] Syncing metadata: ${SRC} -> ${TGT}"
        "${AWS_CMD_BASE[@]}" s3 sync "${SRC}" "${TGT}"
      else
        SRC=$(resolve_host_path "${METADATA_PREFIX_ORIGINAL}")
        if [ -d "${SRC}/year=${YEAR}" ]; then
          if command -v rsync >/dev/null 2>&1; then
            rsync -a "${SRC}/year=${YEAR}" "${METADATA_STAGE}/"
          else
            cp -a "${SRC}/year=${YEAR}" "${METADATA_STAGE}/"
          fi
        fi
      fi
    fi

    if [ -n "${PLOTS_PREFIX_ORIGINAL}" ]; then
      mkdir -p "${PLOTS_STAGE}"
      if [[ "${PLOTS_PREFIX_ORIGINAL}" == s3://* ]]; then
        SRC="${PLOTS_PREFIX_ORIGINAL%/}/year=${YEAR}"
        TGT="${PLOTS_STAGE}/year=${YEAR}"
        echo "[${YEAR}] Syncing plots: ${SRC} -> ${TGT}"
        "${AWS_CMD_BASE[@]}" s3 sync "${SRC}" "${TGT}"
      else
        SRC=$(resolve_host_path "${PLOTS_PREFIX_ORIGINAL}")
        if [ -d "${SRC}/year=${YEAR}" ]; then
          if command -v rsync >/dev/null 2>&1; then
            rsync -a "${SRC}/year=${YEAR}" "${PLOTS_STAGE}/"
          else
            cp -a "${SRC}/year=${YEAR}" "${PLOTS_STAGE}/"
          fi
        fi
      fi
    fi

    CONTAINER_INPUT=$(map_to_container_path "${PARQUET_STAGE}")
    set_option_value_or_append "--input-root" "${INPUT_ROOT_INDEX}" "${INPUT_ROOT_FORMAT}" "${CONTAINER_INPUT}"

    if [ -n "${METADATA_PREFIX_ORIGINAL}" ]; then
      CONTAINER_METADATA=$(map_to_container_path "${METADATA_STAGE}")
      set_option_value_or_append "${METADATA_ARG_NAME}" "${METADATA_ARG_INDEX}" "${METADATA_ARG_FORMAT:-space}" "${CONTAINER_METADATA}"
    fi

    if [ -n "${PLOTS_PREFIX_ORIGINAL}" ]; then
      CONTAINER_PLOTS=$(map_to_container_path "${PLOTS_STAGE}")
      set_option_value_or_append "${PLOTS_ARG_NAME}" "${PLOTS_ARG_INDEX}" "${PLOTS_ARG_FORMAT:-space}" "${CONTAINER_PLOTS}"
    fi

    docker run --rm \
      --ulimit nofile=${DOCKER_NOFILE:-65536}:${DOCKER_NOFILE:-65536} \
      -v "${REPO_ROOT}":/workspace \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -w /workspace \
      "${IMAGE_NAME}" \
      python scripts/11-build_stac_catalog.py "${ARG_BUFFER[@]}"
  done
  exit 0
fi

if [ "${SYNC_ENABLED}" -eq 1 ] && [ -n "${SYNC_SOURCE}" ]; then
  if [ -z "${SYNC_TARGET}" ]; then
    SYNC_TARGET="${DATA_SYNC_DEFAULT}"
  fi
  compute_host_container_paths "${SYNC_TARGET}" HOST_TARGET CONTAINER_TARGET
  mkdir -p "${HOST_TARGET}"
  echo "Syncing ${SYNC_SOURCE} -> ${HOST_TARGET} ..."
  AWS_SYNC_CMD=("${AWS_CMD_BASE[@]}" s3 sync "${SYNC_SOURCE}" "${HOST_TARGET}")
  "${AWS_SYNC_CMD[@]}"
  HOST_INPUT_ROOT="${HOST_TARGET}"
  CONTAINER_INPUT_ROOT="${CONTAINER_TARGET}"
else
  if [[ "${INPUT_ROOT}" == s3://* ]]; then
    echo "Warning: --skip-sync specified but --input-root points to S3; ensure data is available locally." >&2
  else
    HOST_INPUT_ROOT="${INPUT_ROOT}"
    CONTAINER_INPUT_ROOT=$(map_to_container_path "${HOST_INPUT_ROOT}")
  fi
fi

if [[ "${INPUT_ROOT}" != s3://* ]]; then
  if [ -z "${CONTAINER_INPUT_ROOT}" ]; then
    CONTAINER_INPUT_ROOT=$(map_to_container_path "${HOST_INPUT_ROOT}")
  fi
fi
assign_option_value "--input-root" "${INPUT_ROOT_INDEX}" "${INPUT_ROOT_FORMAT}" "${CONTAINER_INPUT_ROOT}"

if [ "${METADATA_IS_S3}" -eq 1 ]; then
  metadata_value="${METADATA_PREFIX_ORIGINAL}"
  if [ "${SYNC_ENABLED}" -eq 1 ] && [ "${NEED_S3_SYNC}" -eq 1 ]; then
    if [ "${SYNC_TARGET_SET}" -eq 1 ]; then
      metadata_target="${SYNC_TARGET%/}_metadata"
    else
      metadata_target="${METADATA_SYNC_DEFAULT}"
    fi
    compute_host_container_paths "${metadata_target}" HOST_METADATA_TARGET CONTAINER_METADATA_TARGET
    mkdir -p "${HOST_METADATA_TARGET}"
    echo "Syncing ${METADATA_PREFIX_ORIGINAL} -> ${HOST_METADATA_TARGET} ..."
    AWS_SYNC_CMD=("${AWS_CMD_BASE[@]}" s3 sync "${METADATA_PREFIX_ORIGINAL}" "${HOST_METADATA_TARGET}")
    "${AWS_SYNC_CMD[@]}"
    metadata_value="${CONTAINER_METADATA_TARGET}"
  fi
  assign_option_value "${METADATA_ARG_NAME}" "${METADATA_ARG_INDEX}" "${METADATA_ARG_FORMAT}" "${metadata_value}"
fi

if [ "${PLOTS_IS_S3}" -eq 1 ]; then
  plots_value="${PLOTS_PREFIX_ORIGINAL}"
  if [ "${SYNC_ENABLED}" -eq 1 ] && [ "${NEED_S3_SYNC}" -eq 1 ]; then
    if [ "${SYNC_TARGET_SET}" -eq 1 ]; then
      plots_target="${SYNC_TARGET%/}_plots"
    else
      plots_target="${PLOTS_SYNC_DEFAULT}"
    fi
    compute_host_container_paths "${plots_target}" HOST_PLOTS_TARGET CONTAINER_PLOTS_TARGET
    mkdir -p "${HOST_PLOTS_TARGET}"
    echo "Syncing ${PLOTS_PREFIX_ORIGINAL} -> ${HOST_PLOTS_TARGET} ..."
    AWS_SYNC_CMD=("${AWS_CMD_BASE[@]}" s3 sync "${PLOTS_PREFIX_ORIGINAL}" "${HOST_PLOTS_TARGET}")
    "${AWS_SYNC_CMD[@]}"
    plots_value="${CONTAINER_PLOTS_TARGET}"
  fi
  assign_option_value "${PLOTS_ARG_NAME}" "${PLOTS_ARG_INDEX}" "${PLOTS_ARG_FORMAT}" "${plots_value}"
fi

ensure_image "${IMAGE_NAME}"

docker run --rm \
  --ulimit nofile=${DOCKER_NOFILE:-65536}:${DOCKER_NOFILE:-65536} \
  -v "${REPO_ROOT}":/workspace \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -w /workspace \
  "${IMAGE_NAME}" \
  python scripts/11-build_stac_catalog.py "${ARG_BUFFER[@]}"
