#!/usr/bin/env bash
set -euo pipefail

# Allow the working directory (and therefore the checkout) to be set in the unit file.
APP_DIR=${EDGIS_APP_DIR:-$(pwd)}
cd "$APP_DIR"

# Load environment variables when available (the unit controls permissions for secrets).
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

VENV_PATH=${EDGIS_VENV_PATH:-.direnv}
if [[ -d "$VENV_PATH/bin" ]]; then
  # shellcheck disable=SC1091
  source "$VENV_PATH/bin/activate"
fi

HOST=${UVICORN_HOST:-0.0.0.0}
PORT=${UVICORN_PORT:-8384}
APP_IMPORT=${UVICORN_APP:-api.systems_staging:app}

exec uvicorn "$APP_IMPORT" --host "$HOST" --port "$PORT"
