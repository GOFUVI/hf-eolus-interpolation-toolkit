#!/usr/bin/env bash
set -euo pipefail

# Wrapper to execute 12-subset_stac_nodes.py inside a Docker container that
# already bundles the Python dependencies (pyarrow, pystac). Reuses the same
# image built for run_build_stac_catalog.sh when available.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required to run this wrapper." >&2
  exit 1
fi

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

ensure_image "${IMAGE_NAME}"

docker run --rm \
  -v "${REPO_ROOT}":/workspace \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -w /workspace \
  "${IMAGE_NAME}" \
  python scripts/12-subset_stac_nodes.py "$@"
