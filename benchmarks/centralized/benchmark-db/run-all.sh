#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="${ROOT_DIR}/../db-market-api"
API_ADDR="${BENCH_DB_API_ADDR:-127.0.0.1:18080}"
API_BASE_URL="${BENCH_DB_API_BASE_URL:-http://${API_ADDR}}"
LOG_DIR="${ROOT_DIR}/.logs"

mkdir -p "${LOG_DIR}"

if ! command -v go >/dev/null 2>&1; then
  echo "Error: go is not available in PATH"
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "Error: curl is required for API readiness checks"
  exit 1
fi

cleanup() {
  if [[ -n "${API_PID:-}" ]]; then
    pkill -P "${API_PID}" 2>/dev/null || true
    kill "${API_PID}" 2>/dev/null || true
    wait "${API_PID}" 2>/dev/null || true
    unset API_PID
  fi
}
trap cleanup EXIT INT TERM

wait_api() {
  local retries=40
  for _ in $(seq 1 "$retries"); do
    if curl -fsS "${API_BASE_URL}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

echo "Preparing modules..."
(
  cd "${ROOT_DIR}"
  go mod tidy
)
(
  cd "${API_DIR}"
  go mod tidy
)

for dataset in "${ROOT_DIR}"/*/dataset.csv; do
  [[ -f "$dataset" ]] || continue

  dataset_dir="$(dirname "$dataset")"
  dataset_name="$(basename "$dataset_dir")"
  results_path="${dataset_dir}/benchmark_results.csv"
  db_path="${dataset_dir}/benchmark.sqlite"
  api_log="${LOG_DIR}/${dataset_name}.api.log"

  rm -f "$db_path"

  echo ""
  echo "== Running dataset: ${dataset_name} =="

  (
    cd "${API_DIR}"
    APP_LISTEN_ADDR="${API_ADDR}" APP_DB_PATH="${db_path}" go run main.go
  ) >"${api_log}" 2>&1 &
  API_PID=$!

  if ! wait_api; then
    echo "API failed to become ready for ${dataset_name}. Check ${api_log}"
    exit 1
  fi

  (
    cd "${ROOT_DIR}"
    BENCH_DB_API_BASE_URL="${API_BASE_URL}" go run benchmark.go --dataset "$dataset" --results "$results_path"
  )

  cleanup

  echo "Results written to ${results_path}"
done

echo ""
echo "All datasets completed."
