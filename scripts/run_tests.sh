#!/usr/bin/env bash
set -euo pipefail

# Local test runner for data-platform.
# Usage:
#   ./scripts/run_tests.sh                    # unit + api
#   TEST_MARKERS="integration or quality" ./scripts/run_tests.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export DATABASE_URL="${DATABASE_URL:-postgresql://cv:cv@localhost:5432/cv_test}"
export PIPELINE_API_TOKEN="${PIPELINE_API_TOKEN:-local-test-token}"

TEST_MARKERS="${TEST_MARKERS:-unit or api}"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Missing .venv/bin/python. Create a virtualenv and install dependencies first."
  exit 1
fi

echo "Running pytest with markers: ${TEST_MARKERS}"
.venv/bin/python -m pytest -m "${TEST_MARKERS}" "$@"
