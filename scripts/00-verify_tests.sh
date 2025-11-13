#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_PATH="${REPO_ROOT}/logs/coverage_summary.json"
PIPELINE_REPORT_DIR="${REPO_ROOT}/reports/pipeline"
BADGE_PATH="${PIPELINE_REPORT_DIR}/coverage_badge.json"

echo "[tests] Running R unit tests via scripts/run_tests.R..."
(cd "${REPO_ROOT}" && Rscript --vanilla "${SCRIPT_DIR}/run_tests.R")

if [[ ! -f "${LOG_PATH}" ]]; then
  echo "[tests] coverage summary not found at ${LOG_PATH}" >&2
  exit 1
fi

mkdir -p "${PIPELINE_REPORT_DIR}"
TARGET_JSON="${PIPELINE_REPORT_DIR}/coverage_summary.json"
cp "${LOG_PATH}" "${TARGET_JSON}"

python3 - "${LOG_PATH}" "${PIPELINE_REPORT_DIR}/coverage_summary.md" "${BADGE_PATH}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

log_path = Path(sys.argv[1])
md_path = Path(sys.argv[2])
badge_path = Path(sys.argv[3])
data = json.loads(log_path.read_text())
overall = data.get("overall", {})
files = data.get("files", [])

lines = [
    "# Test Coverage Summary",
    "",
    f"- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}",
    f"- Functions covered: {overall.get('covered_functions', 0)}/{overall.get('total_functions', 0)} ({overall.get('percent', 0)}%)",
    "",
]

if files:
    lines.extend([
        "| File | Functions | Covered | Coverage (%) |",
        "| --- | --- | --- | --- |",
    ])
    for entry in files:
        lines.append(
            f"| {entry.get('file', 'unknown')} "
            f"| {entry.get('functions', 0)} "
            f"| {entry.get('covered', 0)} "
            f"| {entry.get('percent', 0)} |"
        )
else:
    lines.append("_No instrumented modules were found in this run._")

md_path.write_text("\n".join(lines), encoding="utf-8")

try:
    percent_value = float(overall.get("percent", 0))
except (TypeError, ValueError):
    percent_value = 0.0

def pick_color(pct: float) -> str:
    if pct >= 90:
        return "brightgreen"
    if pct >= 80:
        return "green"
    if pct >= 70:
        return "yellow"
    if pct >= 60:
        return "orange"
    return "red"

badge_payload = {
    "schemaVersion": 1,
    "label": "coverage",
    "message": f"{percent_value:.1f}%",
    "color": pick_color(percent_value)
}

badge_path.write_text(json.dumps(badge_payload, indent=2), encoding="utf-8")
PY

echo "[tests] Coverage summary copied to ${TARGET_JSON}"
echo "[tests] Markdown summary available at ${PIPELINE_REPORT_DIR}/coverage_summary.md"
echo "[tests] Shields badge payload refreshed at ${BADGE_PATH}"
