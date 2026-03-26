#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CENTRALIZED_DIR="${ROOT_DIR}/benchmarks/centralized"
LOG_DIR="${ROOT_DIR}/.logs-db"
mkdir -p "${LOG_DIR}"

API_URL="${APP_API_BASE_URL:-http://127.0.0.1:8080}"
API_ADDR="${APP_LISTEN_ADDR:-:8080}"
DB_PATH="${APP_DB_PATH:-${CENTRALIZED_DIR}/db-market-api/market.db}"

ensure_go_module_ready() {
  local dir="$1"
  (
    cd "$dir"
    go mod tidy
  )
}

cleanup() {
  echo "Stopping DB benchmark stack..."
  [[ -n "${API_PID:-}" ]] && kill "${API_PID}" 2>/dev/null || true
  [[ -n "${AUCTIONEER_PID:-}" ]] && kill "${AUCTIONEER_PID}" 2>/dev/null || true
  [[ -n "${ORACLE_PID:-}" ]] && kill "${ORACLE_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if ! command -v go >/dev/null 2>&1; then
  echo "Error: 'go' is not available in PATH."
  echo "If you use Nix, run: nix develop"
  exit 1
fi

echo "Preparing Go modules (go mod tidy)..."
ensure_go_module_ready "${CENTRALIZED_DIR}/db-market-api"
ensure_go_module_ready "${CENTRALIZED_DIR}/gateways-http/auctioneer"
ensure_go_module_ready "${CENTRALIZED_DIR}/gateways-http/client"
ensure_go_module_ready "${CENTRALIZED_DIR}/gateways-http/oracle"

echo "Starting DB API on ${API_ADDR} (DB: ${DB_PATH})"
(
  cd "${CENTRALIZED_DIR}/db-market-api"
  APP_LISTEN_ADDR="${API_ADDR}" APP_DB_PATH="${DB_PATH}" go run main.go
) >"${LOG_DIR}/api.log" 2>&1 &
API_PID=$!

echo "Starting HTTP auctioneer"
(
  cd "${CENTRALIZED_DIR}/gateways-http/auctioneer"
  APP_API_BASE_URL="${API_URL}" go run main.go
) >"${LOG_DIR}/auctioneer.log" 2>&1 &
AUCTIONEER_PID=$!

echo "Starting HTTP oracle"
(
  cd "${CENTRALIZED_DIR}/gateways-http/oracle"
  APP_API_BASE_URL="${API_URL}" go run main.go
) >"${LOG_DIR}/oracle.log" 2>&1 &
ORACLE_PID=$!

echo "DB stack running. Logs at ${LOG_DIR}"
echo "Press Ctrl+C to stop all services."

wait
