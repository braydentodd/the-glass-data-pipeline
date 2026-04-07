#!/bin/bash
#
# The Glass - Sync team rosters and stats to Google Sheets
#
# Usage:
#   scripts/sync_sheets.sh nba                         # Sync all NBA teams
#   scripts/sync_sheets.sh ncaa                        # Sync all NCAA teams
#   scripts/sync_sheets.sh nba BOS                     # Sync Boston only
#   scripts/sync_sheets.sh ncaa DUKE --mode per_48     # Sync Duke, per-48 mode
#

set -e

if [ -z "$1" ]; then
    echo "Usage: scripts/sync_sheets.sh <nba|ncaa> [TEAM_ABBR] [--mode ...]"
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
    export $(grep -v '^#' "$REPO_ROOT/.env" | xargs)
fi

if [ -d "$REPO_ROOT/venv" ]; then
    source "$REPO_ROOT/venv/bin/activate"
fi

# Build arguments
ARGS=""
if [ -n "$1" ]; then
    if [[ "$1" != --* ]]; then
        ARGS="--team $1"
        shift
    fi
fi
ARGS="$ARGS $@"

if [ -n "$(echo $ARGS | tr -d ' ')" ]; then
    echo "Syncing ${LABEL} sheets with args: $ARGS"
else
    echo "Syncing all ${LABEL} team sheets..."
fi

export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"
python3 -m src.publish.runner --league "$LEAGUE" $ARGS 2>&1
