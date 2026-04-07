#!/bin/bash

# ============================================================================
# The Glass - Daily ETL Auto-Restart Wrapper
# ============================================================================
# Unified script for NBA and NCAA ETL.  Automatically restarts on exit code 42
# (triggered when API session exhaustion is detected).
#
# The ETL will automatically resume where it left off using endpoint_tracker.
#
# Usage:
#   scripts/run_etl.sh nba                    # Run NBA daily ETL
#   scripts/run_etl.sh ncaa                   # Run NCAA daily ETL
#   scripts/run_etl.sh nba --max-restarts 10  # Limit to 10 restarts
#
# GitHub Actions Compatible: Yes
# ============================================================================

set -e

if [ -z "$1" ]; then
    echo "Usage: scripts/run_etl.sh <nba|ncaa> [--max-restarts N]"
    exit 1
fi

LEAGUE="$1"
shift
LEAGUE=$(echo "$LEAGUE" | tr '[:upper:]' '[:lower:]')

case "$LEAGUE" in
    nba)  RUNNER="src.etl.runner";  LABEL="NBA"  ;;
    ncaa) RUNNER="src.etl.runner"; LABEL="NCAA" ;;
    *)    echo "Unknown league: $LEAGUE (use nba or ncaa)"; exit 1 ;;
esac

# Configuration
MAX_RESTARTS=${1:-999999}  # Default: essentially unlimited
RESTART_COUNT=0
EXIT_CODE=42

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "========================================================================"
echo -e "${BLUE}THE GLASS - ${LABEL} Daily ETL with Auto-Restart${NC}"
echo "========================================================================"
echo "Max restarts: ${MAX_RESTARTS}"
echo "Started: $(date)"
echo ""

# Resolve to repo root and activate virtual environment
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

if [ -d "$REPO_ROOT/venv" ]; then
    echo "Activating virtual environment..."
    source "$REPO_ROOT/venv/bin/activate"
else
    echo -e "${RED}ERROR: Virtual environment not found at $REPO_ROOT/venv${NC}"
    echo "Please create it with: python3 -m venv venv"
    exit 1
fi

# Main restart loop
export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"

while [ $RESTART_COUNT -lt $MAX_RESTARTS ]; do
    echo -e "${YELLOW}[$(date +%H:%M:%S)]${NC} Starting ${LABEL} ETL (Attempt $(($RESTART_COUNT + 1)))..."
    echo ""

    python3 -m "$RUNNER"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo ""
        echo "========================================================================"
        echo -e "${GREEN}✓ ${LABEL} ETL COMPLETED SUCCESSFULLY${NC}"
        echo "========================================================================"
        echo "Total restarts: ${RESTART_COUNT}"
        echo "Finished: $(date)"
        exit 0

    elif [ $EXIT_CODE -eq 42 ]; then
        RESTART_COUNT=$((RESTART_COUNT + 1))
        echo ""
        echo "========================================================================"
        echo -e "${YELLOW}↻ AUTO-RESTART ${RESTART_COUNT}/${MAX_RESTARTS}${NC}"
        echo "========================================================================"
        echo "Reason: API session exhaustion detected (exit code 42)"
        echo "Action: Restarting ETL to get fresh API session..."
        echo "Note: ETL will resume where it left off using endpoint_tracker"
        echo ""
        sleep 5

    else
        echo ""
        echo "========================================================================"
        echo -e "${RED}✗ ${LABEL} ETL FAILED${NC}"
        echo "========================================================================"
        echo "Exit code: ${EXIT_CODE}"
        echo "Restarts: ${RESTART_COUNT}"
        echo "This is an unexpected error - not restarting automatically"
        echo "Check logs above for details"
        exit $EXIT_CODE
    fi
done

# Max restarts reached
echo "========================================================================"
echo -e "${RED}✗ MAX RESTARTS REACHED${NC}"
echo "========================================================================"
echo "Attempted ${RESTART_COUNT} restarts"
echo "The ETL may still be incomplete - check endpoint_tracker for status"
echo "You can restart this script to continue"
exit 1
