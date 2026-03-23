"""
THE GLASS - NBA Google Sheets Sync

Thin league-specific wrapper over the shared orchestrator.
Creates an NBA LeagueSyncContext and delegates all sync logic.

Entry point:
    python -m runners.nba_sheets [--team BOS] [--mode per_game|per_48|per_100]
"""

import argparse
import logging
import os

from dotenv import load_dotenv

import lib.nba_sheets as nba_lib
import lib.nba_etl as nba_etl
from config.nba_etl import NBA_CONFIG
from config.nba_sheets import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING

from lib.sheets_orchestrator import LeagueSyncContext, sync_all_teams

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# NBA LEAGUE CONTEXT
# ============================================================================

NBA_CONTEXT = LeagueSyncContext(
    sheets_lib=nba_lib,
    etl_lib=nba_etl,
    league_config=NBA_CONFIG,
    google_sheets_config=GOOGLE_SHEETS_CONFIG,
    sheet_formatting=SHEET_FORMATTING,
    season_year_key='current_season_year',
    team_abbr_field='team_abbr',
    avg_fields=['years_experience', 'age', 'height_inches', 'weight_lbs', 'wingspan_inches'],
    include_hist_post_players=True,
    wrap_opp_pct=lambda vals: sorted(vals),
    load_desired_teams=None,
)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Sync NBA data to Google Sheets')
    parser.add_argument('--team', metavar='ABBR',
                        help='Sync this team first (e.g. BOS)')
    parser.add_argument('--mode',
                        choices=['per_game', 'per_36', 'per_100', 'totals'],
                        default=None,
                        help='Stats display mode (default: per_100)')
    parser.add_argument('--percentiles', action='store_true',
                        help='Show percentile rank columns')
    parser.add_argument('--hist-years', type=int, default=None,
                        help='Past seasons for historical stats (default: 3)')
    parser.add_argument('--post-years', type=int, default=None,
                        help='Past seasons for postseason stats (default: 3)')
    parser.add_argument('--data-only', action='store_true',
                        help='Fast sync: skip structural formatting, only update data + colors')
    args = parser.parse_args()

    # Environment variables (set by API subprocess) override CLI defaults.
    # Priority: CLI arg > env var > hardcoded default
    mode = args.mode or os.environ.get('STATS_MODE', 'per_100')
    show_percentiles = args.percentiles or os.environ.get('SHOW_PERCENTILES') == 'true'
    show_advanced = os.environ.get('SHOW_ADVANCED') == 'true'
    priority_team = args.team or os.environ.get('PRIORITY_TEAM_ABBR')
    data_only = args.data_only or os.environ.get('DATA_ONLY_SYNC') == 'true'

    # Historical timeframe
    hist_mode = os.environ.get('HISTORICAL_MODE', 'years')
    include_current = os.environ.get('INCLUDE_CURRENT_YEAR', 'false') == 'true'

    if hist_mode == 'career':
        historical_config = {'mode': 'career'}
    elif hist_mode == 'seasons':
        season_str = os.environ.get('HISTORICAL_SEASONS', '')
        seasons = [s.strip() for s in season_str.split(',') if s.strip()]
        historical_config = {'mode': 'seasons', 'value': seasons, 'include_current': include_current}
    else:
        hist_years = args.hist_years or int(os.environ.get('HISTORICAL_YEARS', '3'))
        historical_config = {'mode': 'years', 'value': hist_years, 'include_current': include_current}

    # Postseason timeframe - same structure as historical
    if hist_mode == 'career':
        postseason_config = {'mode': 'career'}
    elif hist_mode == 'seasons':
        season_str = os.environ.get('HISTORICAL_SEASONS', '')
        seasons = [s.strip() for s in season_str.split(',') if s.strip()]
        postseason_config = {'mode': 'seasons', 'value': seasons, 'include_current': include_current}
    else:
        post_years = args.post_years or int(os.environ.get('HISTORICAL_YEARS', '3'))
        postseason_config = {'mode': 'years', 'value': post_years, 'include_current': include_current}

    sync_all_teams(
        NBA_CONTEXT,
        mode=mode,
        show_percentiles=show_percentiles,
        show_advanced=show_advanced,
        historical_config=historical_config,
        postseason_config=postseason_config,
        priority_team=priority_team,
        data_only=data_only,
    )


if __name__ == '__main__':
    main()
