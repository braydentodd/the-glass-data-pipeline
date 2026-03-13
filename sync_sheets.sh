#!/bin/bash
#
# Sync NBA team rosters and stats to Google Sheets
# Usage: ./sync_sheets.sh [TEAM_ABBR] [--mode per_game|per_36|per_100|totals]
# Example: ./sync_sheets.sh BOS             (sync Boston, per_game mode)
#          ./sync_sheets.sh BOS --mode per_36
#          ./sync_sheets.sh                  (sync all teams)
#

set -e  # Exit on error

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Activate virtual environment if it exists
if [ -d venv ]; then
    source venv/bin/activate
fi

# Build arguments
ARGS=""
if [ -n "$1" ]; then
    # First arg might be a team abbreviation or a flag
    if [[ "$1" != --* ]]; then
        ARGS="--team $1"
        shift
    fi
fi
# Pass remaining args (e.g. --mode per_36) straight through
ARGS="$ARGS $@"

if [ -n "$(echo $ARGS | tr -d ' ')" ]; then
    echo "Syncing with args: $ARGS"
else
    echo "Syncing all team sheets..."
fi

python3 -m src.sheets $ARGS 2>&1
