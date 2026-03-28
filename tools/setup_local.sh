#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKIP_PYTHON=0
SKIP_DASHBOARD=0
COPY_EXAMPLES=1

usage() {
  cat <<'EOF'
Usage: ./setup [options]

Bootstrap this repo on a new machine.

Options:
  --skip-python       Skip Python virtualenv creation and pip install
  --skip-dashboard    Skip dashboard npm install
  --no-copy-examples  Do not create .env/config example files
  -h, --help          Show this help text
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Missing required command: ${cmd}" >&2
    exit 1
  fi
}

copy_if_missing() {
  local src="$1"
  local dst="$2"
  if [[ -f "${dst}" ]]; then
    return 0
  fi
  cp "${src}" "${dst}"
  echo "Created ${dst#${ROOT_DIR}/} from ${src#${ROOT_DIR}/}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-python)
      SKIP_PYTHON=1
      ;;
    --skip-dashboard)
      SKIP_DASHBOARD=1
      ;;
    --no-copy-examples)
      COPY_EXAMPLES=0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [[ "${SKIP_PYTHON}" == "0" ]]; then
  require_cmd python3
  if [[ ! -d "${ROOT_DIR}/.venv" ]]; then
    python3 -m venv "${ROOT_DIR}/.venv"
    echo "Created Python virtualenv at .venv/"
  fi
  "${ROOT_DIR}/.venv/bin/python" -m pip install -r "${ROOT_DIR}/requirements.txt"
fi

if [[ "${SKIP_DASHBOARD}" == "0" ]]; then
  require_cmd npm
  (
    cd "${ROOT_DIR}/dashboard"
    npm install
  )
fi

if [[ "${COPY_EXAMPLES}" == "1" ]]; then
  copy_if_missing "${ROOT_DIR}/config.sample.yaml" "${ROOT_DIR}/config.yaml"
  copy_if_missing "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
  copy_if_missing "${ROOT_DIR}/.env.google_maps.cookies.example" "${ROOT_DIR}/.env.google_maps.cookies"
fi

cat <<'EOF'

Setup complete.

Next steps:
1. Edit .env and add GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY.
2. If using cookie-mode scraping, edit .env.google_maps.cookies with valid Google Maps cookies.
3. Review config.yaml or batch/config.top50.yaml.
4. Start the API + dashboard with ./dev

Useful checks:
- ./.venv/bin/python -m pytest -q
- ./.venv/bin/python start.py progress --config batch/config.top50.yaml
EOF
