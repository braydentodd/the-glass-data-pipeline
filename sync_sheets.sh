#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate

# Required
export DB_PASSWORD='blCH12..CHEESE'

# Optional: Historical stats configuration
# Uncomment and modify these to override the Google Sheets settings
# export HISTORICAL_MODE='years'        # Options: 'years', 'seasons', 'career'
# export HISTORICAL_YEARS='3'           # Number of years (1-25)
# export HISTORICAL_SEASONS='2023-24,2022-23'  # Specific seasons
# export INCLUDE_CURRENT_YEAR='false'   # Include current season: 'true' or 'false'

python -m src.sync_all_teams
deactivate