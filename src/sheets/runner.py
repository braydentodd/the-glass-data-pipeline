"""
THE GLASS - Universal Google Sheets Sync

Unified runner for synchronizing league data to Google Sheets.

Entry point:
    python -m sheets.runner --league ncaa [--team DUKE] [--mode per_game|per_36|per_100|totals]
"""

import argparse
import logging
import os

from dotenv import load_dotenv

from sheets.config.settings import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING, STAT_MODES, DEFAULT_STAT_MODE
from sheets.lib.sheets_orchestrator import LeagueSyncContext, sync_all_teams, build_timeframe_configs

# Import NBA dependencies
import sheets.nba.lib as nba_lib
import etl.nba.lib as nba_etl_lib
from etl.nba.config import NBA_CONFIG

# Import NCAA dependencies
import sheets.ncaa.lib as ncaa_lib
import etl.ncaa.lib as ncaa_etl_lib
from etl.ncaa.config import NCAA_CONFIG

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# NCAA DESIRED TEAMS FILTER
# ============================================================================

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DESIRED_TEAMS_FILE = os.path.join(_REPO_ROOT, 'teamsThatIwant.py')

def load_ncaa_desired_teams() -> set:
    """Load desired team institution names from teamsThatIwant.py.
    Returns set of institution names, or empty set if file not found."""
    if not os.path.exists(DESIRED_TEAMS_FILE):
        logger.warning('teamsThatIwant.py not found at %s - syncing ALL teams', DESIRED_TEAMS_FILE)
        return set()
    with open(DESIRED_TEAMS_FILE) as f:
        names = {line.strip() for line in f if line.strip()}
    logger.info(f'Loaded {len(names)} desired teams from teamsThatIwant.py')
    return names

# ============================================================================
# LEAGUE CONTEXTS
# ============================================================================

LEAGUES = {
    'nba': LeagueSyncContext(
        sheets_lib=nba_lib,
        etl_lib=nba_etl_lib,
        league_config=NBA_CONFIG,
        google_sheets_config=GOOGLE_SHEETS_CONFIG,
        sheet_formatting=SHEET_FORMATTING,
        season_year_key='current_season_year',
        team_abbr_field='team_abbr',
        avg_fields=['years_experience', 'age', 'height_inches', 'weight_lbs', 'wingspan_inches'],
        include_hist_post_players=True,
        wrap_opp_pct=lambda vals: sorted(vals),
        load_desired_teams=None,
    ),
    'ncaa': LeagueSyncContext(
        sheets_lib=ncaa_lib,
        etl_lib=ncaa_etl_lib,
        league_config=NCAA_CONFIG,
        google_sheets_config=GOOGLE_SHEETS_CONFIG,
        sheet_formatting=SHEET_FORMATTING,
        season_year_key='current_season_int',
        team_abbr_field='abbr',
        avg_fields=['years_experience', 'height_inches', 'weight_lbs', 'wingspan_inches'],
        # User requested NCAA have everything exactly like NBA to build from there
        include_hist_post_players=True, 
        wrap_opp_pct=lambda vals: sorted((v, 1.0) for v in vals), 
        load_desired_teams=load_ncaa_desired_teams,
    )
}

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
    parser.add_argument('--hist-years', type=int, default=None,
                        help='Past seasons for historical stats (default: 3)')
    parser.add_argument('--post-years', type=int, default=None,
                        help='Past seasons for postseason stats (default: 3)')
    parser.add_argument('--data-only', action='store_true',
                        help='Fast sync: skip structural formatting, only update data + colors')
    args = parser.parse_args()

    # Priority: CLI arg > env var > hardcoded default
    league = args.league.lower()
    mode = args.mode or os.environ.get('STATS_MODE', DEFAULT_STAT_MODE)
    show_percentiles = args.percentiles or os.environ.get('SHOW_PERCENTILES') == 'true'
    
    # Enable advanced stats globally, handled gracefully if sections are missing
    show_advanced = os.environ.get('SHOW_ADVANCED') == 'true'
    
    priority_team = args.team or os.environ.get('PRIORITY_TEAM_ABBR')
    data_only = args.data_only or os.environ.get('DATA_ONLY_SYNC') == 'true'

    # Retrieve context
    context = LEAGUES[league]

    # Historical & postseason timeframes
    # Treat NCAA identical to NBA ("years" by default)
    historical_config, postseason_config = build_timeframe_configs(
        hist_years_arg=args.hist_years,
        post_years_arg=args.post_years,
        default_mode='years',
    )

    sync_all_teams(
        context,
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
