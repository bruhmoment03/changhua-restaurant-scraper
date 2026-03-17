#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
DASHBOARD_HOST="${DASHBOARD_HOST:-127.0.0.1}"
DASHBOARD_PORT="${DASHBOARD_PORT:-3000}"

# Load project env (e.g. GOOGLE_PLACES_API_KEY) when present.
if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ROOT_DIR}/.env"
  set +a
fi

# Load Google Maps cookie env for cookie-auth scraping when present.
# This file is intentionally optional; dashboard/API can still run without it.
if [[ -f "${ROOT_DIR}/.env.google_maps.cookies" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ROOT_DIR}/.env.google_maps.cookies"
  set +a
fi

export NEXT_PUBLIC_API_BASE_URL="${NEXT_PUBLIC_API_BASE_URL:-http://${API_HOST}:${API_PORT}}"

API_PID=""
DASHBOARD_PID=""
API_MANAGED=0
DASHBOARD_MANAGED=0
CLEANUP_DONE=0

find_listener_pid() {
  local port="$1"
  # First matching listener PID for tcp port, empty if none.
  lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
}

pid_command() {
  local pid="$1"
  ps -p "${pid}" -o command= 2>/dev/null | sed 's/^[[:space:]]*//' || true
}

http_ready() {
  local url="$1"
  curl -fsS --max-time 2 "${url}" >/dev/null 2>&1
}

http_contains() {
  local url="$1"
  local marker="$2"
  curl -fsS --max-time 2 "${url}" 2>/dev/null | grep -Fq "${marker}"
}

find_free_port() {
  local start_port="$1"
  local port="${start_port}"
  local limit=$((start_port + 20))
  while [[ "${port}" -le "${limit}" ]]; do
    if [[ -z "$(find_listener_pid "${port}")" ]]; then
      echo "${port}"
      return 0
    fi
    port=$((port + 1))
  done
  return 1
}

start_ignoring_sigint() {
  local __pid_var="$1"
  shift

  /bin/bash -c 'trap "" INT; exec "$0" "$@"' "$@" &
  local __pid="$!"
  printf -v "${__pid_var}" '%s' "${__pid}"
}

stop_managed_pid() {
  local pid="$1"
  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
    return
  fi

  kill "${pid}" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done
  if kill -0 "${pid}" 2>/dev/null; then
    kill -9 "${pid}" 2>/dev/null || true
  fi
  wait "${pid}" 2>/dev/null || true
}

cleanup() {
  if [[ "${CLEANUP_DONE}" == "1" ]]; then
    return
  fi
  CLEANUP_DONE=1

  if [[ "${DASHBOARD_MANAGED}" == "1" ]]; then
    stop_managed_pid "${DASHBOARD_PID}"
  fi
  if [[ "${API_MANAGED}" == "1" ]]; then
    stop_managed_pid "${API_PID}"
  fi
}

on_signal() {
  echo
  echo "Stopping dev services..."
  cleanup
  exit 0
}

trap cleanup EXIT
trap on_signal INT TERM

cd "${ROOT_DIR}"
API_EXISTING_PID="$(find_listener_pid "${API_PORT}")"
if [[ -n "${API_EXISTING_PID}" ]]; then
  if http_ready "http://${API_HOST}:${API_PORT}/"; then
    echo "API server already listening on http://${API_HOST}:${API_PORT} (pid: ${API_EXISTING_PID}), reusing."
  else
    echo "API listener on http://${API_HOST}:${API_PORT} is unresponsive (pid: ${API_EXISTING_PID}), restarting."
    stop_managed_pid "${API_EXISTING_PID}"
    API_EXISTING_PID=""
  fi
fi

if [[ -z "${API_EXISTING_PID}" ]]; then
  start_ignoring_sigint API_PID python3 -m uvicorn api_server:app --host "${API_HOST}" --port "${API_PORT}" --reload
  API_MANAGED=1
  echo "API server started: http://${API_HOST}:${API_PORT}"
fi

if [[ -z "${GOOGLE_MAPS_COOKIE_1PSID:-}" || -z "${GOOGLE_MAPS_COOKIE_1PSIDTS:-}" ]]; then
  echo "Warning: cookie auth vars missing (GOOGLE_MAPS_COOKIE_1PSID / GOOGLE_MAPS_COOKIE_1PSIDTS)."
  echo "Scrape actions in cookie mode may fail."
fi

cd "${ROOT_DIR}/dashboard"
DASHBOARD_EXISTING_PID="$(find_listener_pid "${DASHBOARD_PORT}")"
if [[ -n "${DASHBOARD_EXISTING_PID}" ]]; then
  DASHBOARD_URL="http://${DASHBOARD_HOST}:${DASHBOARD_PORT}/"
  DASHBOARD_CMD="$(pid_command "${DASHBOARD_EXISTING_PID}")"
  if http_contains "${DASHBOARD_URL}" "Reviews Ops"; then
    echo "Dashboard already listening on http://${DASHBOARD_HOST}:${DASHBOARD_PORT} (pid: ${DASHBOARD_EXISTING_PID}), reusing."
  elif [[ "${DASHBOARD_CMD}" == *"${ROOT_DIR}/dashboard"* ]] || [[ "${DASHBOARD_CMD}" == *"next dev"* ]]; then
    echo "Dashboard listener on http://${DASHBOARD_HOST}:${DASHBOARD_PORT} is stale or not ready (pid: ${DASHBOARD_EXISTING_PID}), restarting."
    stop_managed_pid "${DASHBOARD_EXISTING_PID}"
    DASHBOARD_EXISTING_PID=""
  else
    ALT_DASHBOARD_PORT="$(find_free_port "$((DASHBOARD_PORT + 1))")" || {
      echo "Port ${DASHBOARD_PORT} is occupied by an unrelated process and no free fallback dashboard port was found."
      echo "Conflicting process: ${DASHBOARD_CMD:-unknown} (pid: ${DASHBOARD_EXISTING_PID})"
      exit 1
    }
    echo "Port ${DASHBOARD_PORT} is occupied by an unrelated process, not this dashboard."
    echo "Conflicting process: ${DASHBOARD_CMD:-unknown} (pid: ${DASHBOARD_EXISTING_PID})"
    DASHBOARD_PORT="${ALT_DASHBOARD_PORT}"
    DASHBOARD_EXISTING_PID=""
  fi
fi

if [[ -z "${DASHBOARD_EXISTING_PID}" ]]; then
  start_ignoring_sigint DASHBOARD_PID npm run dev -- --hostname "${DASHBOARD_HOST}" --port "${DASHBOARD_PORT}"
  DASHBOARD_MANAGED=1
  echo "Dashboard started: http://${DASHBOARD_HOST}:${DASHBOARD_PORT}"
fi

echo "Using API base URL: ${NEXT_PUBLIC_API_BASE_URL}"

if [[ "${API_MANAGED}" == "0" && "${DASHBOARD_MANAGED}" == "0" ]]; then
  echo "Both services are already running. Nothing new was started."
  exit 0
fi

# macOS ships Bash 3.2, which does not support `wait -n`.
# Keep the script alive until one managed child exits, then let trap cleanup the other.
while true; do
  if [[ "${API_MANAGED}" == "1" ]] && ! kill -0 "${API_PID}" 2>/dev/null; then
    wait "${API_PID}" 2>/dev/null || true
    break
  fi
  if [[ "${DASHBOARD_MANAGED}" == "1" ]] && ! kill -0 "${DASHBOARD_PID}" 2>/dev/null; then
    wait "${DASHBOARD_PID}" 2>/dev/null || true
    break
  fi
  sleep 1
done
