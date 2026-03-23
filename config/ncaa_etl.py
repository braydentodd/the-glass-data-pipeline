"""
The Glass NCAA ETL - Configuration Module

Pure configuration data: database settings, CBBD API constants, column schemas.
Mirrors config/nba_etl.py structure for NCAA D1 basketball via CollegeBasketballData.com.

Design Principles:
- Config is DATA, not code (no functions except season helpers)
- Every DB column maps to a CBBD API field via source definitions
- Nested API fields use dot-notation (e.g., 'teamStats.points.total')
- Shared DB_CONFIG with NBA (from config/nba_etl.py)
- Same exact basic stats columns as NBA, same types/nullable/formats
- per_100 possessions as default stat mode
- No tracking data, no hustle stats, no shot-type breakdowns
"""
import os
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# REUSE NBA DB CONFIG (same database, separate tables)
# ============================================================================

from config.db import DB_CONFIG

# ============================================================================
# CBBD API CONFIGURATION
# ============================================================================

CBBD_API_CONFIG = {
    'base_url': 'https://api.collegebasketballdata.com',
    'api_key': os.getenv('CBBD_API_KEY', ''),
    'rate_limit_delay': 1.5,
    'timeout': 30,
    'max_retries': 3,
    'backoff_base': 10,
}

# ============================================================================
# NCAA TABLE CONFIGURATION
# ============================================================================

DB_SCHEMA = 'ncaa'

TABLES_CONFIG = {
    'players': {
        'entity': 'player',
        'contents': 'entity',
    },
    'teams': {
        'entity': 'team',
        'contents': 'entity',
    },
    'player_season_stats': {
        'entity': 'player',
        'contents': 'stats',
    },
    'team_season_stats': {
        'entity': 'team',
        'contents': 'stats',
    },
}

TABLES = list(TABLES_CONFIG.keys())

def get_table_name(entity: str, contents: str) -> str:
    """Get schema-qualified table name by entity type and contents type.
    Returns e.g. 'ncaa.players', 'ncaa.team_season_stats'.
    """
    for table_name, meta in TABLES_CONFIG.items():
        if meta['entity'] == entity and meta['contents'] == contents:
            return f"{DB_SCHEMA}.{table_name}"
    raise ValueError(f"No table for entity={entity}, contents={contents}")


# ============================================================================
# NCAA SEASON CONFIGURATION
# ============================================================================
# NCAA uses the same VARCHAR(10) format as NBA: '2024-25'
# CBBD API accepts integer season (2025 = 2024-25 season).
# We convert to/from display format for DB storage.

SEASON_TYPE_CONFIG = {
    'Regular Season': {
        'season_code': 1,
        'cbbd_param': 'regular',
        'minimum_season': None,
    },
    'Postseason': {
        'season_code': 2,
        'cbbd_param': 'postseason',
        'minimum_season': None,
    },
}


def _get_current_ncaa_season_int() -> int:
    """
    Get current NCAA season as integer (for CBBD API calls).
    NCAA season year = calendar year of the spring semester.
    Season starts in November, so after August we're in next season.
    """
    now = datetime.now()
    return now.year + 1 if now.month > 8 else now.year


def _get_current_ncaa_season() -> str:
    """Get current NCAA season as display string (matching NBA format)."""
    year = _get_current_ncaa_season_int()
    return f"{year - 1}-{str(year)[-2:]}"


def season_int_to_display(season_int: int) -> str:
    """Convert CBBD integer season to display string. 2025 -> '2024-25'."""
    return f"{season_int - 1}-{str(season_int)[-2:]}"


def display_to_season_int(display: str) -> int:
    """Convert display string to CBBD integer season. '2024-25' -> 2025."""
    return int(display.split('-')[0]) + 1


# Legacy aliases
season_to_display = season_int_to_display
display_to_season = display_to_season_int


NCAA_CONFIG = {
    'current_season': _get_current_ncaa_season(),
    'current_season_int': _get_current_ncaa_season_int(),
    'backfill_start_season': '2018-19',
    'backfill_end_season': '2025-26',
    'game_length_minutes': 40.0,
}

# ============================================================================
# CBBD API ENDPOINTS
# ============================================================================

CBBD_ENDPOINTS = {
    'player_season_stats': {
        'path': '/stats/player/season',
        'id_field': 'athleteId',
        'entity_type': 'player',
        'requires_conference': False,
        'schedule': 'daily',
    },
    'team_season_stats': {
        'path': '/stats/team/season',
        'id_field': 'teamId',
        'entity_type': 'team',
        'requires_conference': False,
        'schedule': 'daily',
    },
    'roster': {
        'path': '/teams/roster',
        'id_field': 'players.id',
        'entity_type': 'player',
        'requires_conference': False,
        'schedule': 'manual',
    },
    'teams': {
        'path': '/teams',
        'id_field': 'id',
        'entity_type': 'team',
        'requires_conference': False,
        'schedule': 'monthly',
    },
}

# ============================================================================
# DATABASE SCHEMA: COLUMNS AND METADATA
# ============================================================================
#
# Matches NBA config/nba_etl.py DB_COLUMNS structure exactly:
#   table:    'entity' | 'stats' | 'both'
#   type:     PostgreSQL column type (same as NBA)
#   nullable: same as NBA where applicable
#
# Columns not available from CBBD (wingspan, hand, etc.) included for manual entry.

DB_COLUMNS = {

    # ==== ENTITY IDENTIFICATION ====

    'player_id': {
        'table': 'both',
        'type': 'INTEGER',
        'nullable': False,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'athleteId', 'transform': 'int'},
        'team_source': None,
        'opp_source': None,
    },

    'team_id': {
        'table': 'both',
        'type': 'INTEGER',
        'nullable': False,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'teamId', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamId', 'transform': 'int'},
        'opp_source': None,
    },

    # ==== PLAYER ENTITY COLUMNS (same types as NBA) ====

    'name': {
        'table': 'entity',
        'type': 'VARCHAR(200)',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'name', 'transform': 'str'},
        'team_source': None,
        'opp_source': None,
    },

    'height_inches': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'roster', 'field': 'height', 'transform': 'int'},
        'team_source': None,
        'opp_source': None,
    },

    'weight_lbs': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'roster', 'field': 'weight', 'transform': 'int'},
        'team_source': None,
        'opp_source': None,
    },

    'wingspan_inches': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': None,   # manual entry
        'team_source': None,
        'opp_source': None,
    },

    'jersey_number': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'roster', 'field': 'jersey', 'transform': 'int'},
        'team_source': None,
        'opp_source': None,
    },

    'birthdate': {
        'table': 'entity',
        'type': 'DATE',
        'nullable': True,
        'player_source': {'endpoint': 'roster', 'field': 'dateOfBirth', 'transform': 'date'},
        'team_source': None,
        'opp_source': None,
    },

    'hand': {
        'table': 'entity',
        'type': 'VARCHAR(10)',
        'nullable': True,
        'player_source': None,   # manual entry
        'team_source': None,
        'opp_source': None,
    },

    'years_experience': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': None,   # manual entry
        'team_source': None,
        'opp_source': None,
    },

    'notes': {
        'table': 'entity',
        'type': 'TEXT',
        'nullable': True,
        'player_source': None,
        'team_source': None,
        'opp_source': None,
    },

    'backfilled': {
        'table': 'entity',
        'type': 'BOOLEAN',
        'nullable': True,
        'default': 'FALSE',
        'player_source': None,
        'team_source': None,
        'opp_source': None,
    },

    # ==== TEAM ENTITY COLUMNS ====

    'institution': {
        'table': 'entity',
        'type': 'VARCHAR(200)',
        'nullable': True,
        'player_source': None,
        'team_source': {'endpoint': 'teams', 'field': 'school', 'transform': 'str'},
        'opp_source': None,
    },

    'abbr': {
        'table': 'entity',
        'type': 'VARCHAR(20)',
        'nullable': True,
        'player_source': None,
        'team_source': {'endpoint': 'teams', 'field': 'abbreviation', 'transform': 'str'},
        'opp_source': None,
    },

    'mascot': {
        'table': 'entity',
        'type': 'VARCHAR(100)',
        'nullable': True,
        'player_source': None,
        'team_source': {'endpoint': 'teams', 'field': 'mascot', 'transform': 'str'},
        'opp_source': None,
    },

    'conference': {
        'table': 'entity',
        'type': 'VARCHAR(100)',
        'nullable': True,
        'player_source': None,
        'team_source': {'endpoint': 'team_season_stats', 'field': 'conference', 'transform': 'str'},
        'opp_source': None,
    },

    # ==== TIMESTAMP COLUMNS (all tables) ====

    'created_at': {
        'table': 'both',
        'type': 'TIMESTAMP WITH TIME ZONE',
        'nullable': True,
        'default': 'NOW()',
        'player_source': None,
        'team_source': None,
        'opp_source': None,
    },

    'updated_at': {
        'table': 'both',
        'type': 'TIMESTAMP WITH TIME ZONE',
        'nullable': True,
        'default': 'NOW()',
        'player_source': None,
        'team_source': None,
        'opp_source': None,
    },

    # ==== STATS: SEASON IDENTIFIERS ====

    'season': {
        'table': 'stats',
        'type': 'VARCHAR(10)',
        'nullable': False,
        'player_source': None,
        'team_source': None,
        'opp_source': None,
    },

    'season_type': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': False,
        'player_source': None,
        'team_source': None,
        'opp_source': None,
    },

    # ==== STATS: RATES ====

    'games': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': False,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'games', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'games', 'transform': 'int'},
        'opp_source': None,
    },

    'wins': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': None,
        'team_source': {'endpoint': 'team_season_stats', 'field': 'wins', 'transform': 'int'},
        'opp_source': None,
    },

    'losses': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': None,
        'team_source': {'endpoint': 'team_season_stats', 'field': 'losses', 'transform': 'int'},
        'opp_source': None,
    },

    'minutes_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'minutes', 'transform': 'int_x10'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'totalMinutes', 'transform': 'int_x10'},
        'opp_source': None,
    },

    # ==== STATS: SCORING ====

    '2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'twoPointFieldGoals.made', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.twoPointFieldGoals.made', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.twoPointFieldGoals.made', 'transform': 'int'},
    },

    '2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'twoPointFieldGoals.attempted', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.twoPointFieldGoals.attempted', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.twoPointFieldGoals.attempted', 'transform': 'int'},
    },

    '3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'threePointFieldGoals.made', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.threePointFieldGoals.made', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.threePointFieldGoals.made', 'transform': 'int'},
    },

    '3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'threePointFieldGoals.attempted', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.threePointFieldGoals.attempted', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.threePointFieldGoals.attempted', 'transform': 'int'},
    },

    'ftm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'freeThrows.made', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.freeThrows.made', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.freeThrows.made', 'transform': 'int'},
    },

    'fta': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'freeThrows.attempted', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.freeThrows.attempted', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.freeThrows.attempted', 'transform': 'int'},
    },

    # ==== STATS: REBOUNDING ====

    'o_rebounds': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'rebounds.offensive', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.rebounds.offensive', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.rebounds.offensive', 'transform': 'int'},
    },

    'd_rebounds': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'rebounds.defensive', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.rebounds.defensive', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.rebounds.defensive', 'transform': 'int'},
    },

    'o_rebound_pct_x1000': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'offensiveReboundPct', 'transform': 'int_x1000'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.fourFactors.offensiveReboundPct', 'transform': 'int_x1000'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.fourFactors.offensiveReboundPct', 'transform': 'int_x1000'},
    },

    'd_rebound_pct_x1000': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        # Computed: d_rebounds / (d_rebounds + opp_o_rebounds) * 1000
        'player_source': None,
        'team_source': None,
        'opp_source': None,
        'computed': True,
    },

    # ==== STATS: BALL MANAGEMENT ====

    'assists': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'assists', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.assists', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.assists', 'transform': 'int'},
    },

    'turnovers': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'turnovers', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.turnovers.total', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.turnovers.total', 'transform': 'int'},
    },

    # ==== STATS: DEFENSE ====

    'steals': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'steals', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.steals', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.steals', 'transform': 'int'},
    },

    'blocks': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'blocks', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.blocks', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.blocks', 'transform': 'int'},
    },

    'fouls': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'fouls', 'transform': 'int'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.fouls.total', 'transform': 'int'},
        'opp_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.fouls.total', 'transform': 'int'},
    },

    # ==== STATS: RATINGS ====

    'o_rating_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'offensiveRating', 'transform': 'int_x10'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.rating', 'transform': 'int_x10'},
        'opp_source': None,
    },

    'd_rating_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'player_source': {'endpoint': 'player_season_stats', 'field': 'defensiveRating', 'transform': 'int_x10'},
        'team_source': {'endpoint': 'team_season_stats', 'field': 'opponentStats.rating', 'transform': 'int_x10'},
        'opp_source': None,
    },

    # ==== STATS: POSSESSIONS ====

    'possessions': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        # Player: computed as team_poss * (player_min / team_min)
        # Team: direct from API
        'player_source': None,
        'team_source': {'endpoint': 'team_season_stats', 'field': 'teamStats.possessions', 'transform': 'int'},
        'opp_source': None,
        'computed_for_player': True,
    },
}

# ============================================================================
# HELPERS
# ============================================================================

def get_columns_for_endpoint(endpoint_name: str, entity_type: str) -> Dict[str, dict]:
    """Return {col_name: source_config} for columns sourced from a given endpoint."""
    source_key = f'{entity_type}_source'
    result = {}
    for col_name, col_meta in DB_COLUMNS.items():
        src = col_meta.get(source_key)
        if src and src.get('endpoint') == endpoint_name:
            result[col_name] = src
        if entity_type == 'team':
            opp = col_meta.get('opp_source')
            if opp and opp.get('endpoint') == endpoint_name:
                result[f'opp_{col_name}'] = opp
    return result


def get_stats_columns() -> list:
    """Get all column names that belong to the stats table."""
    return [col for col, meta in DB_COLUMNS.items() if meta['table'] == 'stats']


def get_entity_columns(entity_type: str) -> list:
    """Get all column names for the entity table of a given type."""
    source_key = f'{entity_type}_source'
    result = []
    for col, meta in DB_COLUMNS.items():
        if meta['table'] not in ('entity', 'both'):
            continue
        src = meta.get(source_key)
        if src is not None:
            result.append(col)
        elif col in ('notes', 'backfilled', 'created_at', 'updated_at'):
            result.append(col)
        elif entity_type == 'player' and col in (
            'wingspan_inches', 'birthdate', 'hand', 'years_experience',
        ):
            result.append(col)
    return result


def get_all_columns_for_table(entity_type: str, contents: str) -> list:
    """Get ordered column list for a specific table."""
    if contents == 'entity':
        return get_entity_columns(entity_type)
    elif contents == 'stats':
        id_col = 'player_id' if entity_type == 'player' else 'team_id'
        stats = get_stats_columns()
        if id_col in stats:
            stats.remove(id_col)
        return [id_col] + stats
    raise ValueError(f"Unknown contents type: {contents}")
