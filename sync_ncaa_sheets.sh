#!/bin/bash
#
# Sync NCAA team rosters and stats to Google Sheets
# Usage: ./sync_ncaa_sheets.sh [TEAM_ABBR] [--mode per_game|per_48|per_100]
# Example: ./sync_ncaa_sheets.sh DUKE             (sync Duke, default mode)
#          ./sync_ncaa_sheets.sh DUKE --mode per_48
#          ./sync_ncaa_sheets.sh                   (sync all teams)
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
# Pass remaining args (e.g. --mode per_48) straight through
ARGS="$ARGS $@"

if [ -n "$(echo $ARGS | tr -d ' ')" ]; then
    echo "Syncing NCAA sheets with args: $ARGS"
else
    echo "Syncing all NCAA team sheets..."
fi

python3 -m runners.ncaa_sheets $ARGS 2>&1
