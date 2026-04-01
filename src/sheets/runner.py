"""
THE GLASS - Universal Google Sheets Sync

Unified runner for synchronizing league data to Google Sheets.

Entry point:
    python -m sheets.runner --league ncaa [--team DUKE] [--mode per_game|per_36|per_100]
"""

import argparse
import os

from dotenv import load_dotenv

from src.sheets.config import STAT_MODES, DEFAULT_STAT_MODE
from src.sheets.lib.publisher import sync_all_teams, build_timeframe_configs

load_dotenv()

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Sync league data to Google Sheets')
    parser.add_argument('--league', choices=['nba', 'ncaa'], required=True,
                        help='The league to sync')
    parser.add_argument('--team', metavar='ABBR',
                        help='Sync this team first (e.g. BOS or DUKE)')
    parser.add_argument('--mode',
                        choices=STAT_MODES,
                        default=None,
                        help=f'Stats display mode (default: {DEFAULT_STAT_MODE})')
    parser.add_argument('--percentiles', action='store_true',
                        help='Show percentile rank columns')
    parser.add_argument('--hist-seasons', type=int, default=None,
                        help='Number of historical seasons to include')
    parser.add_argument('--include-current', action='store_true',
                        help='Include current season within the historical seasons count')
    parser.add_argument('--data-only', action='store_true',
                        help='Fast sync: skip structural formatting, only update data + colors')
    args = parser.parse_args()

    # Priority: CLI arg > env var > hardcoded default
    league = args.league.lower()
    mode = args.mode or os.environ.get('STATS_MODE', DEFAULT_STAT_MODE)
    show_percentiles = args.percentiles or os.environ.get('SHOW_PERCENTILES') == 'true'
    show_advanced = os.environ.get('SHOW_ADVANCED') == 'true'
    
    priority_team = args.team or os.environ.get('PRIORITY_TEAM_ABBR')
    data_only = args.data_only or os.environ.get('DATA_ONLY_SYNC') == 'true'

    historical_config, postseason_config = build_timeframe_configs(
        hist_seasons_arg=args.hist_seasons,
        post_years_arg=args.hist_seasons,
        default_mode='years',
    )

    sync_all_teams(
        league=league,
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
