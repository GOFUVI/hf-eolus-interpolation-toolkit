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

map_to_container_path() {
  local path="$1"
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
      IDX=$((IDX + 2))
      ;;
    --sync-target=*)
      SYNC_TARGET=${arg#*=}
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
      if [[ "$value" == s3://* ]]; then
        METADATA_PREFIX_ORIGINAL="$value"
        METADATA_IS_S3=1
        METADATA_ARG_INDEX=${#ARG_BUFFER[@]}
        METADATA_ARG_FORMAT="space"
        ARG_BUFFER+=("--metadata-prefix" "__METADATA_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--metadata-prefix" "$container_value")
      fi
      IDX=$((IDX + 2))
      ;;
    --metadata-prefix=*)
      value=${arg#*=}
      if [[ "$value" == s3://* ]]; then
        METADATA_PREFIX_ORIGINAL="$value"
        METADATA_IS_S3=1
        METADATA_ARG_INDEX=${#ARG_BUFFER[@]}
        METADATA_ARG_FORMAT="equals"
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
      if [[ "$value" == s3://* ]]; then
        PLOTS_PREFIX_ORIGINAL="$value"
        PLOTS_IS_S3=1
        PLOTS_ARG_INDEX=${#ARG_BUFFER[@]}
        PLOTS_ARG_FORMAT="space"
        ARG_BUFFER+=("--plots-prefix" "__PLOTS_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--plots-prefix" "$container_value")
      fi
      IDX=$((IDX + 2))
      ;;
    --plots-prefix=*)
      value=${arg#*=}
      if [[ "$value" == s3://* ]]; then
        PLOTS_PREFIX_ORIGINAL="$value"
        PLOTS_IS_S3=1
        PLOTS_ARG_INDEX=${#ARG_BUFFER[@]}
        PLOTS_ARG_FORMAT="equals"
        ARG_BUFFER+=("__PLOTS_PLACEHOLDER__")
      else
        container_value=$(map_to_container_path "$value")
        ARG_BUFFER+=("--plots-prefix=$container_value")
      fi
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

if [ "${SYNC_ENABLED}" -eq 1 ] && [ -n "${SYNC_SOURCE}" ]; then
  if [ -z "${SYNC_TARGET}" ]; then
    SYNC_TARGET="local_sync"
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
    if [ -n "${SYNC_TARGET}" ]; then
      metadata_target="${SYNC_TARGET%/}_metadata"
    else
      metadata_target="local_sync_metadata"
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
    if [ -n "${SYNC_TARGET}" ]; then
      plots_target="${SYNC_TARGET%/}_plots"
    else
      plots_target="local_sync_plots"
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

IMAGE_NAME=${STAC_IMAGE_NAME:-wind-interpolation-stac:latest}

if ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
  docker build -t "${IMAGE_NAME}" - <<'EOF'
FROM python:3.11-slim
RUN pip install --no-cache-dir \
    "pyarrow>=14.0.2" \
    "pystac>=1.8.5" \
    "shapely>=2.0.0"
WORKDIR /workspace
EOF
fi

docker run --rm \
  -v "${REPO_ROOT}":/workspace \
  -w /workspace \
  "${IMAGE_NAME}" \
  python scripts/11-build_stac_catalog.py "${ARG_BUFFER[@]}"
