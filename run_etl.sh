#!/bin/bash

# ============================================================================
# The Glass - Daily ETL Auto-Restart Wrapper
# ============================================================================
# This script runs the daily ETL and automatically restarts on exit code 42
# (triggered when API session exhaustion is detected).
#
# The ETL will automatically resume where it left off using endpoint_tracker.
#
# Usage:
#   ./run_etl.sh                    # Run daily ETL with auto-restart
#   ./run_etl.sh --max-restarts 10  # Limit to 10 restarts (default: unlimited)
#
# GitHub Actions Compatible: Yes
# ============================================================================

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
echo -e "${BLUE}THE GLASS - Daily ETL with Auto-Restart${NC}"
echo "========================================================================"
echo "Max restarts: ${MAX_RESTARTS}"
echo "Started: $(date)"
echo ""

# Main restart loop
while [ $RESTART_COUNT -lt $MAX_RESTARTS ]; do
    echo -e "${YELLOW}[$(date +%H:%M:%S)]${NC} Starting ETL (Attempt $(($RESTART_COUNT + 1)))..."
    echo ""
    
    # Run the ETL
    python3 -m src.etl
    EXIT_CODE=$?
    
    # Check exit code
    if [ $EXIT_CODE -eq 0 ]; then
        # Success - ETL completed normally
        echo ""
        echo "========================================================================"
        echo -e "${GREEN}✓ ETL COMPLETED SUCCESSFULLY${NC}"
        echo "========================================================================"
        echo "Total restarts: ${RESTART_COUNT}"
        echo "Finished: $(date)"
        exit 0
        
    elif [ $EXIT_CODE -eq 42 ]; then
        # Restart requested (API session exhaustion)
        RESTART_COUNT=$((RESTART_COUNT + 1))
        echo ""
        echo "========================================================================"
        echo -e "${YELLOW}↻ AUTO-RESTART ${RESTART_COUNT}/${MAX_RESTARTS}${NC}"
        echo "========================================================================"
        echo "Reason: API session exhaustion detected (exit code 42)"
        echo "Action: Restarting ETL to get fresh API session..."
        echo "Note: ETL will resume where it left off using endpoint_tracker"
        echo ""
        
        # Brief pause before restart
        sleep 5
        
    else
        # Unexpected error - stop
        echo ""
        echo "========================================================================"
        echo -e "${RED}✗ ETL FAILED${NC}"
        echo "========================================================================"
        echo "Exit code: ${EXIT_CODE}"
        echo "Restarts: ${RESTART_COUNT}"
        echo "This is an unexpected error - not restarting automatically"
        echo "Check logs above for details"
        exit $EXIT_CODE
    fi
done

# Max restarts reached
echo ""
echo "========================================================================"
echo -e "${RED}✗ MAX RESTARTS REACHED${NC}"
echo "========================================================================"
echo "Attempted ${RESTART_COUNT} restarts"
echo "The ETL may still be incomplete - check endpoint_tracker for status"
echo "You can restart this script to continue"
exit 1
