#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINISTACK_IMAGE="${FLUXEL_MINISTACK_IMAGE:-nahuelnucera/ministack}"
MINISTACK_HOST="${FLUXEL_MINISTACK_HOST:-127.0.0.1}"
MINISTACK_PORT="${FLUXEL_MINISTACK_PORT:-4566}"
DEFAULT_ENDPOINT="http://${MINISTACK_HOST}:${MINISTACK_PORT}"
MANAGE_CONTAINER="${FLUXEL_MINISTACK_MANAGE_CONTAINER:-auto}"

export FLUXEL_MINISTACK_ENDPOINT="${FLUXEL_MINISTACK_ENDPOINT:-$DEFAULT_ENDPOINT}"
export FLUXEL_MINISTACK_ACCESS_KEY="${FLUXEL_MINISTACK_ACCESS_KEY:-test}"
export FLUXEL_MINISTACK_SECRET_KEY="${FLUXEL_MINISTACK_SECRET_KEY:-test}"
export FLUXEL_MINISTACK_REGION="${FLUXEL_MINISTACK_REGION:-us-east-1}"

HEALTH_URL="${FLUXEL_MINISTACK_ENDPOINT%/}/_ministack/health"
RESET_URL="${FLUXEL_MINISTACK_ENDPOINT%/}/_ministack/reset"

container_started=0
container_name="fluxel-ministack-${USER:-user}-$$"

cleanup() {
    if [[ "$container_started" -eq 1 ]]; then
        docker rm -f "$container_name" >/dev/null 2>&1 || true
    fi
}

print_docker_daemon_help() {
    case "$(uname -s)" in
        Darwin)
            echo "docker daemon is not running. Start Docker Desktop, Colima, or another local Docker runtime, then rerun this script." >&2
            ;;
        Linux)
            echo "docker daemon is not running. Start Docker Engine or point docker at a running daemon, then rerun this script." >&2
            ;;
        *)
            echo "docker daemon is not running. Start your local Docker runtime, then rerun this script." >&2
            ;;
    esac
}

wait_for_ministack() {
    local attempt

    for attempt in $(seq 1 30); do
        if curl -fsS "$HEALTH_URL" >/dev/null; then
            return 0
        fi
        sleep 1
    done

    echo "MiniStack did not become healthy at $HEALTH_URL" >&2
    return 1
}

start_ministack_if_needed() {
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
        return 0
    fi

    if [[ "$MANAGE_CONTAINER" == "0" ]]; then
        return 0
    fi

    if [[ "$MANAGE_CONTAINER" == "auto" && "$FLUXEL_MINISTACK_ENDPOINT" != "$DEFAULT_ENDPOINT" ]]; then
        return 0
    fi

    if ! command -v docker >/dev/null 2>&1; then
        echo "docker is required to start MiniStack locally" >&2
        return 1
    fi

    if ! docker info >/dev/null 2>&1; then
        print_docker_daemon_help
        return 1
    fi

    docker run -d --rm -p "${MINISTACK_PORT}:4566" --name "$container_name" "$MINISTACK_IMAGE" >/dev/null
    container_started=1
}

trap cleanup EXIT

start_ministack_if_needed
wait_for_ministack
curl -fsS -X POST "$RESET_URL" >/dev/null 2>&1 || true

cd "$ROOT_DIR"
uv run pytest tests/test_s3_integration.py -m integration "$@"