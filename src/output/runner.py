"""
THE GLASS - Universal Google Sheets Sync

Unified runner for synchronizing league data to Google Sheets.

Entry point:
    python -m output.runner --league nba [--tab BOS] [--rate per_possession|per_minute|per_game]
"""

import argparse
import os
import logging
import time

from dotenv import load_dotenv

from src.db import get_db_connection
from src.output.core.db import fetch_all_players, fetch_all_teams, get_teams_from_db
from src.output.core.calculations import calculate_all_percentiles
from src.output.destinations.sheets.client import get_sheets_client
from src.output.core.tabs import sync_teams_sheet, sync_team_sheet, sync_players_sheet
from src.output.config import SHEETS_COLUMNS, STAT_RATES, DEFAULT_STAT_RATE, SECTION_CONFIG
from src.input.core.db import get_table_name

load_dotenv()

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Sync league data to Google Sheets')
    parser.add_argument('--league', choices=['nba', 'ncaa'], required=True,
                        help='The league to sync')
    parser.add_argument('--tab', metavar='NAME',
                        help='Sync this tab first (team abbr like BOS, or "players"/"teams")')
    parser.add_argument('--rate',
                        choices=STAT_RATES,
                        default=None,
                        help=f'Stats rate (default: {DEFAULT_STAT_RATE})')
    parser.add_argument('--hist-seasons', type=int, default=None,
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
        from src.output.core.export import export_config
        path = export_config(league)
        logger.info('Config exported to %s', path)
        return

    # Build historical timeframe config (never includes current season)
    num_seasons = args.hist_seasons or int(os.environ.get('HISTORICAL_TIMEFRAME', '3'))
    historical_config = {'mode': 'seasons', 'value': num_seasons}

    # ---- Build context ----
    class Context:
        pass

    ctx = Context()
    ctx.league = league
    from src.output.config import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING
    from src.output.config import SHEETS_COLUMNS
    ctx.google_sheets_config = GOOGLE_SHEETS_CONFIG
    ctx.sheet_formatting = SHEET_FORMATTING
    ctx.season_key = 'current_season'
    ctx.team_abbr_field = 'abbr'
    ctx.include_hist_post_players = True
    ctx.wrap_opp_pct = lambda vals: sorted(vals)

    if league == 'nba':
        from src.input.sources.nba_api.config import DB_SCHEMA, SEASON_CONFIG as league_config
    else:
        DB_SCHEMA = league
        from src.db import get_current_season, get_current_season_year
        league_config = {
            'current_season': get_current_season(),
            'current_season_year': get_current_season_year(),
        }

    ctx.get_teams_from_db = lambda: get_teams_from_db(DB_SCHEMA)
    ctx.league_config = league_config

    ctx.player_entity_table = get_table_name('player', 'entity', DB_SCHEMA)
    ctx.team_entity_table = get_table_name('team', 'entity', DB_SCHEMA)
    ctx.player_stats_table = get_table_name('player', 'stats', DB_SCHEMA)
    ctx.team_stats_table = get_table_name('team', 'stats', DB_SCHEMA)

    ctx.player_entity_fields = {
        'player_id', 'name', 'team_id', 'height_inches', 'weight_lbs',
        'wingspan_inches', 'seasons_exp', 'age', 'jersey_num',
        'hand', 'notes', 'birthdate', 'updated_at',
    }
    ctx.team_entity_fields = {
        'team_id', 'abbr', 'team_name', 'notes', 'updated_at',
    }

    all_cols = {k for k, v in SHEETS_COLUMNS.items()
                if league in v.get('leagues', [])
                and any(SECTION_CONFIG.get(s, {}).get('is_stats_section') for s in v.get('sections', []))}
    ctx.stat_fields = {c for c in (all_cols - ctx.player_entity_fields - ctx.team_entity_fields) if not c[0].isupper()}
    ctx.team_stat_fields = {c for c in (all_cols - ctx.team_entity_fields) if not c[0].isupper()}
    ctx.team_abbr_col = 'abbr'
    ctx.primary_minutes_col = 'minutes_x10' if 'minutes_x10' in ctx.stat_fields else 'minutes'
    ctx.season_format_fn = str

    logger.info('Starting %s sync...', 'partial update' if partial_update else 'full')
    delay = 0.5 if partial_update else ctx.sheet_formatting.get('sync_delay_seconds', 3)

    client = get_sheets_client(ctx.google_sheets_config)
    spreadsheet = client.open_by_key(ctx.google_sheets_config['spreadsheet_id'])

    sync_kwargs = dict(mode=rate,
                       show_advanced=show_advanced,
                       historical_config=historical_config,
                       partial_update=partial_update,
                       sync_section=sync_section)

    # ---- Pre-compute league-wide percentile populations ONCE (all rates) ----
    logger.info('  Pre-computing league-wide percentile populations...')
    conn = get_db_connection()
    try:
        needs_current = sync_section is None or sync_section == 'current_stats'
        needs_historical = sync_section is None or sync_section == 'historical_stats'
        needs_postseason = sync_section is None or sync_section == 'postseason_stats'

        all_players_curr = fetch_all_players(conn, 'current_stats') if needs_current else []
        all_players_hist = fetch_all_players(
            conn, 'historical_stats', historical_config) if needs_historical else []
        all_players_post = fetch_all_players(
            conn, 'postseason_stats', historical_config) if needs_postseason else []
        _empty_teams = {'teams': [], 'opponents': []}
        all_teams_curr = fetch_all_teams(conn, 'current_stats') if needs_current else _empty_teams
        all_teams_hist = fetch_all_teams(
            conn, 'historical_stats', historical_config) if needs_historical else _empty_teams
        all_teams_post = fetch_all_teams(
            conn, 'postseason_stats', historical_config) if needs_postseason else _empty_teams

        def _build_pct_by_rate(section_data, entity_type):
            """Compute percentile populations for all stat rates."""
            result = {}
            for r in STAT_RATES:
                result[r] = {}
                for section, data_list in section_data.items():
                    if data_list:
                        result[r][section] = calculate_all_percentiles(
                            data_list, entity_type, r)
                    else:
                        result[r][section] = {}
            return result

        precomputed = {
            'player': _build_pct_by_rate({
                'current_stats': all_players_curr,
                'historical_stats': all_players_hist,
                'postseason_stats': all_players_post,
            }, 'player'),
            'team': _build_pct_by_rate({
                'current_stats': all_teams_curr['teams'],
                'historical_stats': all_teams_hist['teams'],
                'postseason_stats': all_teams_post['teams'],
            }, 'team'),
            'opponents': _build_pct_by_rate({
                'current_stats': all_teams_curr['opponents'],
                'historical_stats': all_teams_hist['opponents'],
                'postseason_stats': all_teams_post['opponents'],
            }, 'opponents'),
        }
        logger.info('  Percentile populations ready (%d rates)', len(STAT_RATES))
    finally:
        conn.close()

    # ---- Build team list ----
    teams_db = ctx.get_teams_from_db()
    team_names = {abbr: name for _, (abbr, name) in teams_db.items()}
    abbrs = sorted(team_names.keys())

    if priority_tab:
        pt = priority_tab.upper()
        if pt in abbrs:
            abbrs = [pt] + [a for a in abbrs if a != pt]

    # ---- Sync individual team sheets ----
    for abbr in abbrs:
        try:
            sync_team_sheet(
                ctx, client, spreadsheet, abbr,
                team_name=team_names.get(abbr, abbr),
                precomputed=precomputed,
                **sync_kwargs,
            )
        except Exception as exc:
            logger.error(f'  {abbr} failed: {exc}', exc_info=True)

        logger.info(f'  Rate limit pause ({delay}s)...')
        time.sleep(delay)

    # ---- Sync aggregate sheets (Players then Teams) ----
    # If priority_tab is an aggregate sheet name, sync it first
    aggregate_order = ['players', 'teams']
    if priority_tab and priority_tab.lower() in aggregate_order:
        first = priority_tab.lower()
        aggregate_order = [first] + [s for s in aggregate_order if s != first]

    for sheet_name in aggregate_order:
        try:
            if sheet_name == 'players':
                sync_players_sheet(ctx, client, spreadsheet, **sync_kwargs)
            else:
                sync_teams_sheet(ctx, client, spreadsheet, **sync_kwargs)
        except Exception as exc:
            logger.error(f'  {sheet_name.title()} sheet failed: {exc}', exc_info=True)

        logger.info(f'  Rate limit pause ({delay}s)...')
        time.sleep(delay)

    logger.info('Sync complete.')


if __name__ == '__main__':
    main()
