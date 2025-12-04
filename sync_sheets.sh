#!/bin/bash
#
# Sync NBA team rosters and stats to Google Sheets
# Usage: ./sync_sheets.sh [TEAM_ABBR]
# Example: ./sync_sheets.sh BOS  (syncs only Boston Celtics)
#          ./sync_sheets.sh       (syncs all teams)
#

set -e  # Exit on error

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Set default stats mode if not already set
export STATS_MODE="${STATS_MODE:-per_100_poss}"

# Activate virtual environment if it exists
if [ -d venv ]; then
    source venv/bin/activate
fi

# Run the sync with optional team argument
if [ -n "$1" ]; then
    echo "Syncing $1 team sheet..."
    PYTHONPATH=. python src/sheets_sync.py "$1" 2>&1
else
    echo "Syncing all team sheets..."
    PYTHONPATH=. python src/sheets_sync.py 2>&1
fi
