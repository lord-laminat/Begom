#!/bin/bash
# A separate fresh profile; existing volumes and identities are never removed.
set -euo pipefail
cd "$(dirname "$0")/.."
export BEGOM_SOURCE="${BEGOM_SOURCE:-kinescope}"
case "$BEGOM_SOURCE" in kinescope|mock) ;; *) echo 'BEGOM_SOURCE: kinescope or mock' >&2; exit 1;; esac
export COMPOSE_PROJECT_NAME="${BEGOM_V2_PROJECT:-begom-v2-$BEGOM_SOURCE}"
export BEGOM_STATE_DIR="${BEGOM_V2_STATE_DIR:-./.local-v2-$BEGOM_SOURCE}"
if [ "$BEGOM_SOURCE" = mock ]; then
  export BEGOM_WEB_PORT="${BEGOM_WEB_PORT:-8446}"
  export BEGOM_BAO_PORT="${BEGOM_BAO_PORT:-8203}"
  export BEGOM_LOG_PORT="${BEGOM_LOG_PORT:-24226}"
  export BEGOM_MOCK_PORT="${BEGOM_MOCK_PORT:-8093}"
  export BEGOM_CONTROL_SUBNET="${BEGOM_CONTROL_SUBNET:-172.30.89.0/24}"
else
  export BEGOM_WEB_PORT="${BEGOM_WEB_PORT:-8445}"
  export BEGOM_BAO_PORT="${BEGOM_BAO_PORT:-8202}"
  export BEGOM_LOG_PORT="${BEGOM_LOG_PORT:-24225}"
  export BEGOM_CONTROL_SUBNET="${BEGOM_CONTROL_SUBNET:-172.30.79.0/24}"
fi
export BEGOM_PUBLIC_ORIGIN="${BEGOM_PUBLIC_ORIGIN:-https://localhost:$BEGOM_WEB_PORT}"
unset COMPOSE_PROFILES
export COMPOSE_FILE=docker-compose.yml
if [ "$BEGOM_SOURCE" = kinescope ]; then export COMPOSE_FILE="$COMPOSE_FILE:deploy/compose.kinescope.yml"; fi
export COMPOSE_FILE="$COMPOSE_FILE:deploy/compose.v2.yml"
case "${1:-up}" in
  init|up)
    make build
    if [ ! -f "$BEGOM_STATE_DIR/client.json" ]; then
      bin/begomctl init --dir "$BEGOM_STATE_DIR" --source "$BEGOM_SOURCE"
    fi
    if [ "${1:-up}" = up ]; then
      docker compose up -d --build --wait --wait-timeout 240
      echo "Begom: $BEGOM_PUBLIC_ORIGIN; login: admin; password file: $BEGOM_STATE_DIR/client-password"
    fi
    ;;
  down) docker compose down ;;
  status) docker compose ps -a ;;
  logs) docker compose logs --tail 100 ;;
  acceptance)
    make build
    if [ "$BEGOM_SOURCE" = kinescope ]; then
      python3 tests/kinescope/run.py --config "$BEGOM_STATE_DIR/client.json" --id "${BEGOM_ACCEPTANCE_ID:-203613411}" --output artifacts/v2/kinescope
    else
      bin/begomctl acceptance --config "$BEGOM_STATE_DIR/client.json" --output artifacts/v2/acceptance
    fi
    ;;
  *) echo 'Usage: bash deploy/local-v2.sh [init|up|down|status|logs|acceptance]' >&2; exit 1 ;;
esac
