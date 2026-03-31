"""
The Glass - League Sync Profiles

Centralizes the league-specific data-fetching configuration for the orchestrator,
so runners/sheets.py can build generic instances.
"""
import importlib

from config.sheets import GOOGLE_SHEETS_CONFIG, SHEET_FORMATTING
from lib.sheets_data import get_db_table_columns
from config.columns import resolve_columns_for_league

def get_league_profile(league: str) -> dict:
    if league == 'nba':
        from config.nba_etl import NBA_CONFIG as LEAGUE_CONFIG, DB_CONFIG
        from lib.nba_etl import get_table_name
        player_fields = get_db_table_columns(DB_CONFIG, 'nba', 'players', set())
        team_fields = get_db_table_columns(DB_CONFIG, 'nba', 'teams', set())
        season_year_key = 'current_season_year'
        team_abbr_field = 'abbr'
        include_hist_post_players = True
        season_type = LEAGUE_CONFIG['season_type']
        season_col = 'season'
        player_join_type = 'LEFT JOIN'
        player_group_extras = ['p.birthdate']
        age_expr = 'EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))::int AS "age"'
        career_excludes_current = False
        per_minute_mode = 'per_36'
    elif league == 'ncaa':
        from config.ncaa_etl import NCAA_CONFIG as LEAGUE_CONFIG, DB_CONFIG
        from lib.ncaa_etl import get_table_name
        player_fields = get_db_table_columns(DB_CONFIG, 'ncaa', 'players', set())
        team_fields = get_db_table_columns(DB_CONFIG, 'ncaa', 'teams', set())
        season_year_key = 'current_season_int'
        team_abbr_field = 'abbr'
        include_hist_post_players = False
        season_type = 1
        season_col = 'season'
        player_join_type = 'INNER JOIN'
        player_group_extras = None
        age_expr = None
        career_excludes_current = True
        per_minute_mode = 'per_36'
    else:
        raise ValueError(f"Unknown league: {league}")

    etl_module = importlib.import_module(f"lib.{league}_etl")
    
    all_stat_sys = {'player_id', 'updated_at', 'team_id'}
    all_stat_sys.add(season_col)
    all_stat_sys.add('season_type')
    
    stat_fields = get_db_table_columns(DB_CONFIG, league, 'player_season_stats', all_stat_sys)
    team_stat_fields = get_db_table_columns(DB_CONFIG, league, 'team_season_stats', all_stat_sys)

    return {
        'etl_lib': etl_module,
        'db_config': DB_CONFIG,
        'league_config': LEAGUE_CONFIG,
        'google_sheets_config': GOOGLE_SHEETS_CONFIG[league],
        'sheet_formatting': SHEET_FORMATTING,
        'season_year_key': season_year_key,
        'team_abbr_field': team_abbr_field,
        'include_hist_post_players': include_hist_post_players,
        
        'data_fetcher_kwargs': dict(
            current_season=LEAGUE_CONFIG['current_season'],
            current_year=LEAGUE_CONFIG[season_year_key],
            season_type=season_type,
            season_col=season_col,
            team_abbr_col=team_abbr_field,
            player_entity_fields=player_fields,
            team_entity_fields=team_fields,
            stat_table_fields={f for f in stat_fields - player_fields - team_fields if not f[0].isupper()},
            team_stat_table_fields={f for f in team_stat_fields - team_fields if not f[0].isupper()},
            get_table_name=get_table_name,
            player_join_type=player_join_type,
            player_group_extras=player_group_extras,
            age_expr=age_expr,
            career_excludes_current=career_excludes_current,
        ),
        
        'engine_kwargs': dict(
            sheets_columns=resolve_columns_for_league(league),
            per_minute_mode=per_minute_mode,
            league_key=league,
        )
    }
