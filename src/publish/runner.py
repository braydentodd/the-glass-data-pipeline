"""
THE GLASS - Universal Google Sheets Sync

Unified runner for synchronizing league data to Google Sheets.

Entry point:
    python -m publish.runner --league nba [--tab BOS] [--rate per_possession|per_minute|per_game]
"""

import argparse
import os
import logging
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from src.publish.definitions.config import (
    STAT_RATES, DEFAULT_STAT_RATE
)
from src.publish.core.executor import sync_league

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    from src.publish.core.config_validation import validate_config
    # validate_config()

    parser = argparse.ArgumentParser(description='Sync league data to Google Sheets')
    parser.add_argument('--league', choices=['nba', 'ncaa'], required=True,
                        help='The league to sync')
    parser.add_argument('--tab', metavar='NAME',
                        help='Sync this tab first (team abbr like BOS, or "all_players"/"all_teams")')
    parser.add_argument('--rate',
                        choices=STAT_RATES,
                        default=None,
                        help=f'Stats rate (default: {DEFAULT_STAT_RATE})')
    parser.add_argument('--historical-timeframe', type=int, default=None,
                        help='Historical timeframe: number of previous seasons to include')
    parser.add_argument('--partial-update', action='store_true',
                        help='Fast sync: skip structural formatting, only update data + colors')
    parser.add_argument('--export-config', action='store_true',
                        help='Export Apps Script config JS file and exit (no sheet sync)')
    parser.add_argument('--sync', action='store_true', default=True,
                        help='Sync data to Google Sheets (default behavior)')
    args = parser.parse_args()

    # Priority: CLI arg > env var > hardcoded default
    league = args.league.lower()
    rate = args.rate or os.environ.get('STATS_RATE', DEFAULT_STAT_RATE)
    show_advanced = os.environ.get('SHOW_ADVANCED') == 'true'
    priority_tab = args.tab or os.environ.get('PRIORITY_TAB')
    partial_update = args.partial_update or os.environ.get('PARTIAL_UPDATE') == 'true'
    sync_section = os.environ.get('SYNC_SECTION')

    # Export Apps Script config if requested (standalone action)
    if args.export_config:
        from src.publish.core.export_config import export_config
        path = export_config(league)
        logger.info('Config exported to %s', path)
        return

    # Build historical timeframe config (never includes current season)
    num_seasons = args.historical_timeframe or int(os.environ.get('HISTORICAL_TIMEFRAME', '3'))
    historical_config = {'mode': 'seasons', 'value': num_seasons}

    sync_league(league, rate, show_advanced, historical_config, partial_update, sync_section, priority_tab)

    if args.sync:
        import subprocess
        from pathlib import Path
        from src.publish.core.export_config import export_config

        logger.info('Exporting updated sheet configuration via export_config...')
        path = export_config(league)
        
        apps_script_dir = Path(__file__).resolve().parents[2] / 'apps_script'
        logger.info('Running clasp push from %s...', apps_script_dir)
        try:
            subprocess.run(['clasp', 'push', '-f'], cwd=apps_script_dir, check=True)
            logger.info('Successfully pushed updated configuration to Google Apps Script.')
        except subprocess.CalledProcessError as e:
            logger.error('Failed to execute clasp push: %s', e)
        except FileNotFoundError:
            logger.error('The clasp CLI is not installed or not in PATH.')

if __name__ == '__main__':
    main()