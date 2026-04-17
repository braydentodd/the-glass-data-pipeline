"""
THE GLASS - Universal Google Sheets Sync

Unified runner for synchronizing league data to Google Sheets.

Entry point:
    python -m publish.runner --league nba [--tab BOS] [--rate per_possession|per_minute|per_game]
"""

import argparse
import os
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Set

from dotenv import load_dotenv
load_dotenv()

from src.core.db import get_db_connection, get_table_name
from src.publish.core.queries import fetch_all_players, fetch_all_teams, get_teams_from_db
from src.publish.destinations.sheets.client import get_sheets_client
from src.publish.core.executor import sync_teams_tab, sync_team_tab, sync_players_tab, _compute_pct_by_rate
from src.publish.definitions.config import (
    STAT_RATES, DEFAULT_STAT_RATE, SECTIONS_CONFIG,
)
from src.publish.core.calculations import derive_db_fields

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


@dataclass
class SyncContext:
    """Bundles everything the sheets sync needs for a league run."""

    league: str
    google_sheets_config: dict
    sheet_formatting: dict
    league_config: dict
    db_schema: str

    # Table names (schema-qualified)
    player_entity_table: str
    team_entity_table: str
    player_stats_table: str
    team_stats_table: str

    # DB column sets for query construction
    player_entity_fields: Set[str] = field(default_factory=set)
    team_entity_fields: Set[str] = field(default_factory=set)
    stat_fields: Set[str] = field(default_factory=set)
    team_stat_fields: Set[str] = field(default_factory=set)

    # League-specific settings
    team_abbr_col: str = 'abbr'
    team_abbr_field: str = 'team_abbr'
    primary_minutes_col: str = 'minutes_x10'
    season_format_fn: Callable = str
    season_key: str = 'current_season'
    include_hist_post_players: bool = True


# ============================================================================
# PERCENTILE PRE-COMPUTATION
# ============================================================================

def _precompute_percentiles(
    ctx,
    sync_section: Optional[str],
    historical_config: dict,
) -> dict:
    """Pre-compute league-wide percentile populations for all stat rates.

    Called once per run so every team and the aggregate tabs share the
    same population baselines.
    """
    current_season = ctx.league_config[ctx.season_key]
    current_season_year = ctx.league_config['current_season_year']
    season_type_val = ctx.league_config.get('season_type', 'rs')

    query_kw = dict(
        historical_config=historical_config,
        ctx=ctx,
        current_season=current_season,
        current_season_year=current_season_year,
        season_type_val=season_type_val,
    )
    from src.publish.definitions.config import HISTORICAL_TIMEFRAMES
    conn = get_db_connection()
    try:
        needs_current = sync_section is None or sync_section == 'current_stats'
        needs_historical = sync_section is None or sync_section == 'historical_stats'
        needs_postseason = sync_section is None or sync_section == 'postseason_stats'

        supported_years = list(HISTORICAL_TIMEFRAMES.keys())

        all_players_curr = fetch_all_players(conn, 'current_stats', **query_kw) if needs_current else []
        
        _empty_teams = {'teams': [], 'opponents': []}
        all_teams_curr = fetch_all_teams(conn, 'current_stats', **query_kw) if needs_current else _empty_teams
        
        all_players_hist = {}
        all_players_post = {}
        all_teams_hist = {}
        all_teams_post = {}
        
        for y in supported_years:
            hist_kw = query_kw.copy()
            hist_kw['historical_config'] = {'mode': 'seasons', 'value': y}
            all_players_hist[y] = fetch_all_players(conn, 'historical_stats', **hist_kw) if needs_historical else []
            all_players_post[y] = fetch_all_players(conn, 'postseason_stats', **hist_kw) if needs_postseason else []
            all_teams_hist[y] = fetch_all_teams(conn, 'historical_stats', **hist_kw) if needs_historical else _empty_teams
            all_teams_post[y] = fetch_all_teams(conn, 'postseason_stats', **hist_kw) if needs_postseason else _empty_teams

        # Build player groups for team_average context in team percentiles
        from collections import defaultdict
        player_groups = defaultdict(list)
        for p in all_players_curr:
            ta = p.get(ctx.team_abbr_field)
            if ta:
                player_groups[ta].append(p)

        def _team_context_fn(entity):
            abbr = entity.get(ctx.team_abbr_field)
            return {'team_players': player_groups.get(abbr, [])}

        player_dict = {'current_stats': all_players_curr}
        team_dict = {'current_stats': all_teams_curr['teams']}
        opp_dict = {'current_stats': all_teams_curr['opponents']}
        
        for y in supported_years:
            player_dict[f'historical_stats_{y}yr'] = all_players_hist[y]
            player_dict[f'postseason_stats_{y}yr'] = all_players_post[y]
            team_dict[f'historical_stats_{y}yr'] = all_teams_hist[y]['teams']
            team_dict[f'postseason_stats_{y}yr'] = all_teams_post[y]['teams']
            opp_dict[f'historical_stats_{y}yr'] = all_teams_hist[y]['opponents']
            opp_dict[f'postseason_stats_{y}yr'] = all_teams_post[y]['opponents']

        precomputed = {
            'player': _compute_pct_by_rate(player_dict, 'player'),
            'team': _compute_pct_by_rate(team_dict, 'team', context_fn=_team_context_fn),
            'opponents': _compute_pct_by_rate(opp_dict, 'opponents'),
        }
        logger.info('  Percentile populations ready (%d rates)', len(STAT_RATES))
        return precomputed
    finally:
        conn.close()


# ============================================================================
# SYNC ORCHESTRATOR
# ============================================================================

def sync_league(
    league: str,
    rate: str,
    show_advanced: bool,
    historical_config: dict,
    partial_update: bool,
    sync_section: Optional[str],
    priority_tab: Optional[str],
) -> None:
    """Execute the full Google Sheets sync for a league.

    Called by ``main()`` after all CLI args and env vars are resolved.
    """
    # ---- Build context ----
    from src.publish.definitions.config import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING
    from src.etl.definitions import get_source_for_league
    import importlib

    db_schema = league
    source_key = get_source_for_league(league)
    source_config = importlib.import_module(f'src.etl.sources.{source_key}.config')
    league_config = source_config.SEASON_CONFIG

    stats_sections = frozenset(
        name for name, cfg in SECTIONS_CONFIG.items() if cfg.get('stats_timeframe')
    )
    computed_fields = set()
    db_fields = derive_db_fields(league, stats_sections, computed_fields)

    ctx = SyncContext(
        league=league,
        google_sheets_config=GOOGLE_SHEETS_CONFIG[league],
        sheet_formatting=SHEET_FORMATTING,
        league_config=league_config,
        db_schema=db_schema,
        player_entity_table=get_table_name('player', 'entity', db_schema),
        team_entity_table=get_table_name('team', 'entity', db_schema),
        player_stats_table=get_table_name('player', 'stats', db_schema),
        team_stats_table=get_table_name('team', 'stats', db_schema),
        player_entity_fields=db_fields['player_entity_fields'],
        team_entity_fields=db_fields['team_entity_fields'],
        stat_fields=db_fields['stat_fields'],
        team_stat_fields=db_fields['team_stat_fields'],
        primary_minutes_col='minutes_x10' if 'minutes_x10' in db_fields['stat_fields'] else 'minutes',
        season_format_fn=getattr(source_config, 'format_season', str),
    )

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
    precomputed = _precompute_percentiles(ctx, sync_section, historical_config)

    # ---- Build team list ----
    teams_db = get_teams_from_db(ctx.db_schema)
    team_names = {abbr: name for _, (abbr, name) in teams_db.items()}
    abbrs = sorted(team_names.keys())

    if priority_tab:
        pt = priority_tab.upper()
        if pt in abbrs:
            abbrs = [pt] + [a for a in abbrs if a != pt]

    # ---- Sync individual team tabs ----
    failed_tabs = []
    for abbr in abbrs:
        try:
            sync_team_tab(
                ctx, client, spreadsheet, abbr,
                team_name=team_names.get(abbr, abbr),
                precomputed=precomputed,
                **sync_kwargs,
            )
        except Exception as exc:
            logger.error(f'  {abbr} failed: {exc}', exc_info=True)
            failed_tabs.append(abbr)

        logger.info(f'  Rate limit pause ({delay}s)...')
        time.sleep(delay)

    # ---- Sync aggregate tabs (Players then Teams) ----
    # If priority_tab is an aggregate tab name, sync it first
    aggregate_order = ['all_players', 'all_teams']
    if priority_tab and priority_tab.lower() in aggregate_order:
        first = priority_tab.lower()
        aggregate_order = [first] + [s for s in aggregate_order if s != first]

    for tab_name in aggregate_order:
        try:
            if tab_name == 'all_players':
                sync_players_tab(ctx, client, spreadsheet, precomputed=precomputed, **sync_kwargs)
            else:
                sync_teams_tab(ctx, client, spreadsheet, precomputed=precomputed, **sync_kwargs)
        except Exception as exc:
            logger.error(f'  {tab_name.title()} tab failed: {exc}', exc_info=True)
            failed_tabs.append(tab_name)

        logger.info(f'  Rate limit pause ({delay}s)...')
        time.sleep(delay)

    if failed_tabs:
        failed_list = ', '.join(failed_tabs)
        logger.error('Sync finished with failures: %s', failed_list)
        raise RuntimeError(f'Sync failed for tab(s): {failed_list}')

    logger.info('Sync complete.')


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


if __name__ == '__main__':
    main()