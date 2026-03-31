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

import sheets.nba.lib as nba_lib
import etl.nba.lib as lib
from etl.nba.config import NBA_CONFIG
from sheets.nba_sheets import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING

from lib.sheets_orchestrator import LeagueSyncContext, sync_all_teams, build_timeframe_configs

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
    etl_lib=lib,
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

    # Historical & postseason timeframes
    historical_config, postseason_config = build_timeframe_configs(
        hist_years_arg=args.hist_years,
        post_years_arg=args.post_years,
        default_mode='years',
    )

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
