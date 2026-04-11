#!/bin/bash
#
# The Glass - Publish league data to Google Sheets
#
# Usage:
#   scripts/run_publish.sh nba                         # Sync all NBA teams
#   scripts/run_publish.sh ncaa                        # Sync all NCAA teams
#   scripts/run_publish.sh nba --tab BOS               # Sync Boston first
#   scripts/run_publish.sh nba --rate per_game          # Per-game stat rate
#

set -e

if [ -z "$1" ]; then
    echo "Usage: scripts/run_publish.sh <nba|ncaa> [--tab NAME] [--rate ...]"
    exit 1
fi

LEAGUE="$1"
shift
LEAGUE=$(echo "$LEAGUE" | tr '[:upper:]' '[:lower:]')

case "$LEAGUE" in
    nba)  LABEL="NBA"  ;;
    ncaa) LABEL="NCAA" ;;
    *)    echo "Unknown league: $LEAGUE (use nba or ncaa)"; exit 1 ;;
esac

# Resolve to repo root and activate virtual environment
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    source "$REPO_ROOT/.env"
    set +a
fi

if [ -d "$REPO_ROOT/venv" ]; then
    source "$REPO_ROOT/venv/bin/activate"
fi

echo "Publishing ${LABEL} data to Google Sheets..."

export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"
python3 -m src.publish.runner --league "$LEAGUE" "$@" 2>&1
