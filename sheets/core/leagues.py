"""
The Glass - League Sync Profiles

Centralizes the league-specific data-fetching configuration for the orchestrator,
so runners/sheets.py can build generic instances.
"""
import importlib

from sheets.config.settings import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING, LEAGUE_CONSTANTS
from lib.sheets_data import get_db_table_columns
from sheets.config.columns import resolve_columns_for_league

def get_league_profile(league: str) -> dict:
    # 1. Dynamically load league-specific configs
    if league == 'nba':
        from etl.nba_api.config import NBA_CONFIG as LEAGUE_CONFIG, DB_CONFIG
        from etl.nba_api.lib import get_table_name
    elif league == 'ncaa':
        from etl.ncaa.config import NCAA_CONFIG as LEAGUE_CONFIG, DB_CONFIG
        from etl.ncaa.lib import get_table_name
    else:
        raise ValueError(f"Unknown league: {league}")

    etl_module = importlib.import_module(f"lib.{league}_etl")
    
    # 2. Fetch all dynamic DB columns
    player_fields = get_db_table_columns(DB_CONFIG, league, 'players', set())
    team_fields = get_db_table_columns(DB_CONFIG, league, 'teams', set())
    
    all_stat_sys = {'player_id', 'updated_at', 'team_id', 'season', 'season_type'}
    stat_fields = get_db_table_columns(DB_CONFIG, league, 'player_season_stats', all_stat_sys)
    team_stat_fields = get_db_table_columns(DB_CONFIG, league, 'team_season_stats', all_stat_sys)

    # 3. Identical config definition (100% DRY)
    return {
        'etl_lib': etl_module,
        'db_config': DB_CONFIG,
        'league_config': LEAGUE_CONFIG,
        'google_sheets_config': GOOGLE_SHEETS_CONFIG[league],
        'sheet_formatting': SHEET_FORMATTING,
        'season_year_key': 'current_season_year' if league == 'nba' else 'current_season_int',
        'team_abbr_field': 'abbr',
        'include_hist_post_players': True,
        
        'data_fetcher_kwargs': dict(
            current_season=LEAGUE_CONFIG['current_season'],
            current_year=LEAGUE_CONFIG['current_season_year' if league == 'nba' else 'current_season_int'],
            season_type=LEAGUE_CONFIG.get('season_type', 1),
            season_col='season',
            team_abbr_col='abbr',
            player_entity_fields=player_fields,
            team_entity_fields=team_fields,
            stat_table_fields={f for f in stat_fields - player_fields - team_fields if not f[0].isupper()},
            team_stat_table_fields={f for f in team_stat_fields - team_fields if not f[0].isupper()},
            get_table_name=get_table_name,
            player_join_type='LEFT JOIN',
            player_group_extras=['p.birthdate'] if league == 'nba' else None, # Handled gracefully if no birthdate
            age_expr='EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))::int AS "age"' if league == 'nba' else None,
            career_excludes_current=False,
        ),
        
        'engine_kwargs': dict(
            sheets_columns=resolve_columns_for_league(league),
            per_minute_mode=LEAGUE_CONSTANTS[league]['per_minute_mode'],
            league_key=league,
        )
    }
