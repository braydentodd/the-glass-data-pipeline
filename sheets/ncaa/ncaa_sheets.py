"""
THE GLASS - NCAA Google Sheets Sync

Thin league-specific wrapper over the shared orchestrator.
Creates an NCAA LeagueSyncContext and delegates all sync logic.

Entry point:
    python -m runners.ncaa_sheets [--team DUKE] [--mode per_game|per_48|per_100]
"""

import argparse
import logging
import os

from dotenv import load_dotenv

import sheets.ncaa.lib as ncaa_lib
import etl.ncaa.lib as lib
from etl.ncaa.config import NCAA_CONFIG
from sheets.core.ncaa_sheets import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING

from lib.sheets_orchestrator import LeagueSyncContext, sync_all_teams, build_timeframe_configs

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# DESIRED TEAMS FILTER
# ============================================================================

# Resolve to repo root (1 level up from runners/ncaa_sheets.py)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DESIRED_TEAMS_FILE = os.path.join(_REPO_ROOT, 'teamsThatIwant.py')


def load_desired_teams() -> set:
    """Load desired team institution names from teamsThatIwant.py.
    Returns set of institution names, or empty set if file not found."""
    if not os.path.exists(DESIRED_TEAMS_FILE):
        logger.warning('teamsThatIwant.py not found at %s - syncing ALL teams',
                        DESIRED_TEAMS_FILE)
        return set()
    with open(DESIRED_TEAMS_FILE) as f:
        names = {line.strip() for line in f if line.strip()}
    logger.info(f'Loaded {len(names)} desired teams from teamsThatIwant.py')
    return names


# ============================================================================
# NCAA LEAGUE CONTEXT
# ============================================================================

NCAA_CONTEXT = LeagueSyncContext(
    sheets_lib=ncaa_lib,
    etl_lib=lib,
    league_config=NCAA_CONFIG,
    google_sheets_config=GOOGLE_SHEETS_CONFIG,
    sheet_formatting=SHEET_FORMATTING,
    season_year_key='current_season_int',
    team_abbr_field='abbr',
    avg_fields=['years_experience', 'height_inches', 'weight_lbs', 'wingspan_inches'],
    include_hist_post_players=False,
    wrap_opp_pct=lambda vals: sorted((v, 1.0) for v in vals),
    load_desired_teams=load_desired_teams,
)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Sync NCAA data to Google Sheets')
    parser.add_argument('--team', metavar='ABBR',
                        help='Sync this team first (e.g. DUKE)')
    parser.add_argument('--mode',
                        choices=['per_game', 'per_40', 'per_100', 'totals'],
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
    show_advanced = False  # NCAA has no advanced stats
    priority_team = args.team or os.environ.get('PRIORITY_TEAM_ABBR')
    data_only = args.data_only or os.environ.get('DATA_ONLY_SYNC') == 'true'

    # Historical & postseason timeframes (NCAA defaults to career)
    historical_config, postseason_config = build_timeframe_configs(
        hist_years_arg=args.hist_years,
        post_years_arg=args.post_years,
        default_mode='career',
    )

    sync_all_teams(
        NCAA_CONTEXT,
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
