"""
The Glass - Unified Column Registry

Single source of truth for all database column definitions across leagues.
Column names match the actual PostgreSQL schema exactly.

Sources (API field mappings) are defined in league-specific configs:
  - src/etl/nba/sources.py  -> NBA_SOURCE_MAP
  - src/etl/ncaa/sources.py -> NCAA_SOURCE_MAP

Attributes per column:
  tables           - Which table types contain this column: 'entity', 'stats'
  type             - PostgreSQL data type
  nullable         - Whether NULL is allowed
  update_frequency - How often data changes: 'daily', 'annual', or None (manual/computed)
  default          - SQL DEFAULT value (omitted when none)
  entity_types     - (entity only) Which entity tables have this column: 'player', 'team'
  has_opponent     - (stats only) Whether opp_<col> exists in team_season_stats
  rate_group       - (stats only) Games/minutes baseline: 'basic', 'tracking', 'hustle', 'onoff'
"""

from typing import Any, Dict


# ============================================================================
# COLUMN REGISTRY
# ============================================================================

DB_COLUMNS: Dict[str, Dict[str, Any]] = {

    'id': {
        'scope': ['entity', 'stats'],
        'type': 'SERIAL',
        'primary_key': True,
        'nullable': False,
        'default': None,
        'update_frequency': 'initial_only',
        'entity_types': ['player', 'team']
    },
    'nba_api_id': {
        'scope': ['entity', 'stats'],
        'type': 'VARCHAR(10)',
        'primary_key': False,
        'nullable': False,
        'default': None,
        'update_frequency': 'initial_only',
        'entity_types': ['player', 'team']
    },
    'team_id': {
        'scope': ['entity'],
        'type': 'INTEGER',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player']
    },
    'name': {
        'scope': ['entity'],
        'type': 'VARCHAR(100)',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['player', 'team']
    },
    'height_ins': {
        'scope': ['entity'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['player']
    },
    'weight_lbs': {
        'scope': ['entity'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['player']
    },
    'wingspan_ins': {
        'scope': ['entity'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['player']
    },
    'jersey_num': {
        'scope': ['entity'],
        'type': 'VARCHAR(3)',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player']
    },
    'birthdate': {
        'scope': ['entity'],
        'type': 'DATE',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['player']
    },
    'hand': {
        'scope': ['entity'],
        'type': 'CHAR',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': None,
        'entity_types': ['player']
    },
    'seasons_exp': {
        'scope': ['entity'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player']
    },
    'rookie_season': {
        'scope': ['entity'],
        'type': 'VARCHAR(10)',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['player']
    },
    'abbr': {
        'scope': ['entity'],
        'type': 'VARCHAR(5)',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['team']
    },
    'conf': {
        'scope': ['entity'],
        'type': 'VARCHAR(50)',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'annual',
        'entity_types': ['team']
    },
    'backfilled': {
        'scope': ['entity'],
        'type': 'BOOLEAN',
        'primary_key': False,
        'nullable': False,
        'default': 'FALSE',
        'update_frequency': 'initial_only',
        'entity_types': ['player', 'team']
    },
    'notes': {
        'scope': ['entity'],
        'type': 'TEXT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': None,
        'entity_types': ['player', 'team']
    },
    'created_at': {
        'scope': ['entity'],
        'type': 'TIMESTAMP',
        'primary_key': False,
        'nullable': True,
        'default': 'CURRENT_TIMESTAMP',
        'update_frequency': 'initial_only',
        'entity_types': ['player', 'team']
    },
    'updated_at': {
        'scope': ['entity', 'stats'],
        'type': 'TIMESTAMP',
        'primary_key': False,
        'nullable': True,
        'default': 'CURRENT_TIMESTAMP',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'season': {
        'scope': ['stats'],
        'type': 'VARCHAR(7)',
        'primary_key': True,
        'nullable': False,
        'default': None,
        'update_frequency': 'annual_initial_only',
        'entity_types': ['player', 'team'],
    },
    'season_type': {
        'scope': ['stats'],
        'type': 'VARCHAR(3)',
        'primary_key': True,
        'nullable': False,
        'default': None,
        'update_frequency': 'annual_initial_only',
        'entity_types': ['player', 'team']
    },
    'games': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'minutes_x10': {
        'scope': ['stats'],
        'type': 'INTEGER',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'tr_games': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'tr_minutes_x10': {
        'scope': ['stats'],
        'type': 'INTEGER',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'h_games': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'h_minutes_x10': {
        'scope': ['stats'],
        'type': 'INTEGER',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'off_games': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player']
    },
    'off_minutes_x10': {
        'scope': ['stats'],
        'type': 'INTEGER',
        'primary_key': False,
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'possessions': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'fg2m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'fg2a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'fg3m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'fg3a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'ftm': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'fta': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'cont_fg2m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_fg2a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'open_fg2m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'open_fg2a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_fg3m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_fg3a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'open_fg3m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,    
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'open_fg3a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_rim_fgm': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_rim_fga': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'open_rim_fgm': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'open_rim_fga': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'dunks': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'putbacks': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'unassisted_rim_fgm': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'unassisted_fg2m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'unassisted_fg3m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'o_rebs': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'd_rebs': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'o_reb_pct_x1000': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_reb_pct_x1000': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_o_rebs': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'cont_d_rebs': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'assists': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'pot_assists': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'turnovers': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'touches': {
        'scope': ['stats'],
        'type': 'INTEGER',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'time_on_ball': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'passes': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'sec_assists': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'o_dist_x10': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_dist_x10': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'steals': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'blocks': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'fouls': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent']
    },
    'deflections': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'charges_drawn': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'contests': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_rim_fgm': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_rim_fga': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_fg2m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_fg2a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_fg3m': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_fg3a': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'o_rtg_x10': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'd_rtg_x10': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    },
    'off_o_rtg_x10': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player']
    },
    'off_d_rtg_x10': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player']
    },
    'wins': {
        'scope': ['stats'],
        'type': 'SMALLINT',
        'primary_key': False,
        'nullable': True,
        'default': None,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team']
    }
}


# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_entity_columns(entity_type: str) -> list:
    """Get column names for an entity table (player or team)."""
    return [
        col for col, meta in DB_COLUMNS.items()
        if 'entity' in meta['scope']
        and entity_type in meta.get('entity_types', [])
    ]


def get_stats_columns() -> list:
    """Get all column names that belong to stats tables."""
    return [col for col, meta in DB_COLUMNS.items() if 'stats' in meta['scope']]


def get_opponent_columns() -> list:
    """Get column names that have opponent versions in team_season_stats."""
    return [col for col, meta in DB_COLUMNS.items() if meta.get('has_opponent')]


def get_columns_by_rate_group(rate_group: str) -> list:
    """Get stat columns associated with a specific rate group."""
    return [
        col for col, meta in DB_COLUMNS.items()
        if meta.get('rate_group') == rate_group
    ]


def get_columns_by_update_frequency(frequency: str) -> list:
    """Get columns that update at a given frequency ('daily', 'annual')."""
    return [
        col for col, meta in DB_COLUMNS.items()
        if meta.get('update_frequency') == frequency
    ]
