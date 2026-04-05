"""
The Glass - NBA API Configuration

NBA-specific endpoint definitions, source mappings, and operational settings.
This is the NBA provider layer — the bridge between the NBA API and the
canonical column registry (src/etl/config.py).

Mirrors the Sheets pattern: league-specific provider config wires up
external data sources to canonical column names.
"""

import os
from typing import Any, Dict, List, Optional

from src.db import get_current_season, get_current_season_year


# ============================================================================
# SCHEMA & TABLES
# ============================================================================

DB_SCHEMA = 'nba'

TABLES_CONFIG = {
    'players':              {'entity': 'player', 'contents': 'entity'},
    'teams':                {'entity': 'team',   'contents': 'entity'},
    'player_season_stats':  {'entity': 'player', 'contents': 'stats'},
    'team_season_stats':    {'entity': 'team',   'contents': 'stats'},
}


def get_table_name(entity: str, table_type: str) -> str:
    """Resolve schema-qualified table name: get_table_name('player', 'stats') -> 'nba.player_season_stats'."""
    for name, meta in TABLES_CONFIG.items():
        if meta['entity'] == entity and meta['contents'] == table_type:
            return f"{DB_SCHEMA}.{name}"
    raise ValueError(f"No table for entity={entity}, type={table_type}")


# ============================================================================
# SEASON CONFIGURATION
# ============================================================================

SEASON_CONFIG = {
    'current_season': get_current_season(),
    'current_season_year': get_current_season_year(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),
    'backfill_start': '2003-04',
    'tracking_start': '2013-14',
    'hustle_start': '2015-16',
    'onoff_start': '2007-08',
    'combine_start_year': 2003,
}

SEASON_TYPES = {
    1: {'name': 'Regular Season', 'param': 'Regular Season', 'min_season': None},
    2: {'name': 'Playoffs',       'param': 'Playoffs',       'min_season': None},
    3: {'name': 'PlayIn',         'param': 'PlayIn',         'min_season': '2020-21'},
}


# ============================================================================
# TEAM IDS (lazy-loaded from database)
# ============================================================================

_team_ids_cache: Optional[Dict[str, int]] = None


def _load_team_ids() -> Dict[str, int]:
    """Lazy-load team IDs from database. Cached after first call."""
    global _team_ids_cache
    if _team_ids_cache is None:
        from src.db import db_connection
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT nba_api_id, abbr FROM {DB_SCHEMA}.teams ORDER BY nba_api_id")
                _team_ids_cache = {row[1]: int(row[0]) for row in cur.fetchall()}
    return _team_ids_cache


class _TeamIDsProxy:
    """Lazy proxy for TEAM_IDS that loads from DB on first access."""
    def __getitem__(self, key):   return _load_team_ids()[key]
    def __contains__(self, key):  return key in _load_team_ids()
    def keys(self):               return _load_team_ids().keys()
    def values(self):             return _load_team_ids().values()
    def items(self):              return _load_team_ids().items()
    def get(self, key, default=None): return _load_team_ids().get(key, default)


TEAM_IDS = _TeamIDsProxy()


# ============================================================================
# API OPERATIONAL CONFIGURATION
# ============================================================================

API_CONFIG = {
    'rate_limit_delay': 1.2,
    'per_player_rate_limit': 2.5,
    'timeout_default': 30,
    'timeout_bulk': 120,
    'backoff_divisor': 5,
    'cooldown_after_batch_seconds': 30,
    'max_consecutive_failures': 5,

    'roster_batch_size': 175,
    'roster_batch_cooldown': 120,

    'api_failure_threshold': 1,
    'api_restart_enabled': True,

    # Standard NBA API query parameters
    'league_id': '00',
    'per_mode_simple': 'Totals',
    'per_mode_time': 'Totals',
    'per_mode_detailed': 'Totals',
    'last_n_games': '0',
    'month': '0',
    'opponent_team_id': '0',
    'period': '0',
    'player_or_team_player': 'Player',
    'player_or_team_team': 'Team',
}

RETRY_CONFIG = {
    'max_retries': 3,
    'backoff_base': 30,
}

PARALLEL_CONFIG = {
    'league': {'max_workers': 10, 'timeout': 30},
    'team':   {'max_workers': 10, 'timeout': 30},
    'player': {'max_workers': 1},
}

DB_OPERATIONS = {
    'bulk_insert_batch_size': 1000,
    'statement_timeout_ms': 120000,
}


# ============================================================================
# ENDPOINT PARAMETER DEFINITIONS
# ============================================================================

ENDPOINT_PARAMS = {
    'pt_measure_type': {
        'api_param': 'pt_measure_type',
        'values': [
            'Passing', 'Possessions', 'Defense', 'SpeedDistance',
            'CatchShoot', 'PullUpShot', 'Drives', 'ElbowTouch',
            'PostTouch', 'PaintTouch', 'Efficiency',
        ],
    },
    'measure_type_detailed_defense': {
        'api_param': 'measure_type_detailed_defense',
        'values': ['Advanced', 'Base', 'Opponent'],
    },
    'defense_category': {
        'api_param': 'defense_category',
        'values': [
            'Overall', '3 Pointers', '2 Pointers',
            'Less Than 6Ft', 'Less Than 10Ft', 'Greater Than 15Ft',
        ],
    },
}

PARAM_KEYS = list(ENDPOINT_PARAMS.keys())


# ============================================================================
# ENDPOINTS
# ============================================================================

ENDPOINTS: Dict[str, Dict[str, Any]] = {
    # --- Basic stats (since 2003-04) ---
    'leaguedashplayerstats': {
        'min_season': '2003-04',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPlayerStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
        'tracking': False,
    },
    'leaguedashteamstats': {
        'min_season': '2003-04',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashTeamStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
        'tracking': False,
    },

    # --- Player tracking (since 2013-14) ---
    'leaguedashptstats': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['pt_measure_type'],
        'entity_types': ['player', 'team'],
        'tracking': True,
    },

    # --- Hustle stats (since 2015-16) ---
    'leaguehustlestatsplayer': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsPlayer',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['player'],
        'tracking': True,
        'games_column': 'h_games',
    },
    'leaguehustlestatsteam': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsTeam',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['team'],
        'tracking': True,
        'games_column': 'h_games',
    },

    # --- Defensive matchup (since 2013-14) ---
    'leaguedashptdefend': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtDefend',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['defense_category'],
        'entity_types': ['player'],
        'tracking': True,
    },
    'leaguedashptteamdefend': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtTeamDefend',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['defense_category'],
        'entity_types': ['team'],
        'tracking': True,
    },

    # --- Shot tracking league-wide (since 2013-14) ---
    'leaguedashplayerptshot': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPTShots',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
        'tracking': True,
    },
    'leaguedashteamptshot': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPTShots',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
        'tracking': True,
    },

    # --- Per-entity shot tracking (legacy, kept for reference) ---
    'playerdashptshots': {
        'min_season': '2013-14',
        'execution_tier': 'player',
        'default_result_set': 'ClosestDefenderShooting',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
        'tracking': True,
    },
    'teamdashptshots': {
        'min_season': '2013-14',
        'execution_tier': 'team',
        'default_result_set': 'ClosestDefenderShooting',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
        'tracking': True,
    },

    # --- Rebound tracking (since 2013-14) ---
    'playerdashptreb': {
        'min_season': '2013-14',
        'execution_tier': 'player',
        'default_result_set': 'OverallRebounding',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
        'tracking': True,
    },
    'teamdashptreb': {
        'min_season': '2013-14',
        'execution_tier': 'team',
        'default_result_set': 'OverallRebounding',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
        'tracking': True,
    },

    # --- Shooting splits (since 2012-13) ---
    'playerdashboardbyshootingsplits': {
        'min_season': '2012-13',
        'execution_tier': 'player',
        'default_result_set': 'ShotTypePlayerDashboard',
        'season_type_param': 'season_type_playoffs',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
        'accepts_team_id': False,
        'tracking': False,
    },
    'teamdashboardbyshootingsplits': {
        'min_season': '2012-13',
        'execution_tier': 'league',
        'default_result_set': 'ShotTypeTeamDashboard',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
        'tracking': False,
    },

    # --- Player info (all time) ---
    'commonplayerinfo': {
        'min_season': None,
        'execution_tier': 'player',
        'default_result_set': 'CommonPlayerInfo',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
        'tracking': False,
        'endpoint_tracker': False,
    },

    # --- Draft combine (since 2000-01) ---
    'draftcombineplayeranthro': {
        'min_season': '2000-01',
        'execution_tier': 'league',
        'default_result_set': 'DraftCombinePlayerAnthro',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
        'tracking': False,
        'endpoint_tracker': False,
    },

    # --- On/Off court (since 2007-08) ---
    'teamplayeronoffsummary': {
        'min_season': '2007-08',
        'execution_tier': 'league',
        'default_result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
        'tracking': False,
        'games_column': 'off_games',
    },
}


# ============================================================================
# API ENTITY ID / NAME FIELDS
# ============================================================================

API_FIELD_NAMES = {
    'entity_id': {'player': 'PLAYER_ID', 'team': 'TEAM_ID'},
    'entity_name': {'player': 'PLAYER_NAME', 'team': 'TEAM_NAME'},
    'special_ids': {'person': 'PERSON_ID'},
}


# ============================================================================
# SHOT TRACKING HELPERS (DRY source builders)
# ============================================================================

_CONTESTED_RIM_CALLS = [
    {'close_def_dist_range_nullable': '0-2 Feet - Very Tight', 'general_range_nullable': 'Less Than 10 ft'},
    {'close_def_dist_range_nullable': '2-4 Feet - Tight',      'general_range_nullable': 'Less Than 10 ft'},
]
_OPEN_RIM_CALLS = [
    {'close_def_dist_range_nullable': '4-6 Feet - Open',       'general_range_nullable': 'Less Than 10 ft'},
    {'close_def_dist_range_nullable': '6+ Feet - Wide Open',   'general_range_nullable': 'Less Than 10 ft'},
]
_CONTESTED_ALL_CALLS = [
    {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
    {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
]
_OPEN_ALL_CALLS = [
    {'close_def_dist_range_nullable': '4-6 Feet - Open'},
    {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
]


def _shot_source(entity: str, field: str, calls: list) -> dict:
    """Build multi-call aggregate source for shot tracking columns.

    Makes the 12 shot-tracking column definitions DRY — each is just
    ``_shot_source('player', 'FG2M', _CONTESTED_RIM_CALLS)``.
    """
    ep = 'leaguedashplayerptshot' if entity == 'player' else 'leaguedashteamptshot'
    return {
        'endpoint': ep,
        'execution_tier': 'league',
        'transformation': {
            'type': 'pipeline',
            'endpoint': ep,
            'execution_tier': 'league',
            'operations': [{
                'type': 'multi_league_extract',
                'field': field,
                'result_set': 'LeagueDashPTShots',
                'calls': calls,
            }],
        },
    }


# ============================================================================
# PUTBACK & DUNK FILTER VALUES
# ============================================================================

_PUTBACK_SHOT_TYPES = [
    'Putback Dunk Shot', 'Putback Layup Shot',
    'Tip Dunk Shot', 'Tip Layup Shot',
]

_DUNK_SHOT_TYPES = [
    'Alley Oop Dunk Shot', 'Cutting Dunk Shot', 'Driving Dunk Shot',
    'Driving Reverse Dunk Shot', 'Dunk Shot', 'Putback Dunk Shot',
    'Reverse Dunk Shot', 'Running Alley Oop Dunk Shot',
    'Running Dunk Shot', 'Tip Dunk Shot',
]


# ============================================================================
# SOURCES — NBA API field -> canonical DB column mappings
# ============================================================================
# Keyed by canonical DB column name from src/etl/config.py.
# Each maps entity type ('player', 'team', 'opponent') to its source config.
#
# Source config patterns:
#   Simple:      {'endpoint': '...', 'field': '...', 'transform': '...', 'scale': N}
#   With params: {'endpoint': '...', 'params': {...}, 'field': '...', ...}
#   Pipeline:    {'endpoint': '...', 'transformation': {'type': 'pipeline', ...}}
#   Team-call:   {'endpoint': '...', 'execution_tier': 'team_call', 'result_set': '...', ...}
#
# Columns without an API source (id, created_at, updated_at, backfilled,
# notes, season, season_type) are omitted — they're managed by the ETL engine.

SOURCES: Dict[str, Dict[str, Any]] = {

    # ---- Entity identification ----

    'nba_api_id': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'PLAYER_ID', 'transform': 'safe_str'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'TEAM_ID',   'transform': 'safe_str'},
    },
    'team_id': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'TEAM_ID', 'transform': 'safe_int'},
    },
    'name': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'PLAYER_NAME', 'transform': 'safe_str'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'TEAM_NAME',   'transform': 'safe_str'},
    },
    'height_ins': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'HEIGHT', 'transform': 'parse_height'},
    },
    'weight_lbs': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'WEIGHT', 'transform': 'safe_int'},
    },
    'wingspan_ins': {
        'player': {'endpoint': 'draftcombineplayeranthro', 'field': 'WINGSPAN', 'transform': 'parse_height'},
    },
    'jersey_num': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'JERSEY', 'transform': 'safe_str'},
    },
    'birthdate': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'BIRTHDATE', 'transform': 'parse_birthdate'},
    },
    'seasons_exp': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'SEASON_EXP', 'transform': 'safe_int'},
    },
    'rookie_season': {
        'player': {'endpoint': 'commonplayerinfo', 'field': 'FROM_YEAR', 'transform': 'format_season'},
    },
    'abbr': {
        'team': {'endpoint': 'leaguedashteamstats', 'field': 'TEAM_ABBREVIATION', 'transform': 'safe_str'},
    },
    'conf': {
        'team': {'endpoint': 'leaguedashteamstats', 'field': 'CONFERENCE', 'transform': 'safe_str'},
    },

    # ---- Games & minutes ----

    'games': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'GP', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'GP', 'transform': 'safe_int'},
    },
    'minutes_x10': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'MIN', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'MIN', 'transform': 'safe_int', 'scale': 10},
    },
    'wins': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'W', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'W', 'transform': 'safe_int'},
    },
    'tr_games': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'}, 'field': 'GP', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},   'field': 'GP', 'transform': 'safe_int'},
    },
    'tr_minutes_x10': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'}, 'field': 'MIN', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},   'field': 'MIN', 'transform': 'safe_int', 'scale': 10},
    },
    'h_games': {
        'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'GP', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'GP', 'transform': 'safe_int'},
    },
    'h_minutes_x10': {
        'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'MIN', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'MIN', 'transform': 'safe_int', 'scale': 10},
    },

    # ---- On/Off court ----

    'off_games': {
        'player': {
            'endpoint': 'teamplayeronoffsummary',
            'execution_tier': 'team_call',
            'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
            'player_id_field': 'VS_PLAYER_ID',
            'field': 'GP', 'transform': 'safe_int',
            'aggregation': 'sum',
        },
    },
    'off_minutes_x10': {
        'player': {
            'endpoint': 'teamplayeronoffsummary',
            'execution_tier': 'team_call',
            'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
            'player_id_field': 'VS_PLAYER_ID',
            'field': 'MIN', 'transform': 'safe_int', 'scale': 10,
            'aggregation': 'sum',
        },
    },

    # ---- Scoring: 2-point ----

    'fg2m': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FGM', 'transform': 'safe_int',
                   'derived': {'subtract_field': 'FG3M'}},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'FGM', 'transform': 'safe_int',
                   'derived': {'subtract_field': 'FG3M'}},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_FGM', 'transform': 'safe_int',
                     'derived': {'subtract_field': 'OPP_FG3M'}},
    },
    'fg2a': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FGA', 'transform': 'safe_int',
                   'derived': {'subtract_field': 'FG3A'}},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'FGA', 'transform': 'safe_int',
                   'derived': {'subtract_field': 'FG3A'}},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_FGA', 'transform': 'safe_int',
                     'derived': {'subtract_field': 'OPP_FG3A'}},
    },

    # ---- Scoring: 3-point ----

    'fg3m': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FG3M', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'FG3M', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_FG3M', 'transform': 'safe_int'},
    },
    'fg3a': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FG3A', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'FG3A', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_FG3A', 'transform': 'safe_int'},
    },

    # ---- Scoring: free throws ----

    'ftm': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FTM', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'FTM', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_FTM', 'transform': 'safe_int'},
    },
    'fta': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FTA', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'FTA', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_FTA', 'transform': 'safe_int'},
    },

    # ---- Shot tracking: contested/open × rim/all ----

    'cont_rim_fgm': {
        'player': _shot_source('player', 'FGM', _CONTESTED_RIM_CALLS),
        'team':   _shot_source('team',   'FGM', _CONTESTED_RIM_CALLS),
    },
    'cont_rim_fga': {
        'player': _shot_source('player', 'FGA', _CONTESTED_RIM_CALLS),
        'team':   _shot_source('team',   'FGA', _CONTESTED_RIM_CALLS),
    },
    'open_rim_fgm': {
        'player': _shot_source('player', 'FGM', _OPEN_RIM_CALLS),
        'team':   _shot_source('team',   'FGM', _OPEN_RIM_CALLS),
    },
    'open_rim_fga': {
        'player': _shot_source('player', 'FGA', _OPEN_RIM_CALLS),
        'team':   _shot_source('team',   'FGA', _OPEN_RIM_CALLS),
    },
    'cont_fg2m': {
        'player': _shot_source('player', 'FG2M', _CONTESTED_ALL_CALLS),
        'team':   _shot_source('team',   'FG2M', _CONTESTED_ALL_CALLS),
    },
    'cont_fg2a': {
        'player': _shot_source('player', 'FG2A', _CONTESTED_ALL_CALLS),
        'team':   _shot_source('team',   'FG2A', _CONTESTED_ALL_CALLS),
    },
    'open_fg2m': {
        'player': _shot_source('player', 'FG2M', _OPEN_ALL_CALLS),
        'team':   _shot_source('team',   'FG2M', _OPEN_ALL_CALLS),
    },
    'open_fg2a': {
        'player': _shot_source('player', 'FG2A', _OPEN_ALL_CALLS),
        'team':   _shot_source('team',   'FG2A', _OPEN_ALL_CALLS),
    },
    'cont_fg3m': {
        'player': _shot_source('player', 'FG3M', _CONTESTED_ALL_CALLS),
        'team':   _shot_source('team',   'FG3M', _CONTESTED_ALL_CALLS),
    },
    'cont_fg3a': {
        'player': _shot_source('player', 'FG3A', _CONTESTED_ALL_CALLS),
        'team':   _shot_source('team',   'FG3A', _CONTESTED_ALL_CALLS),
    },
    'open_fg3m': {
        'player': _shot_source('player', 'FG3M', _OPEN_ALL_CALLS),
        'team':   _shot_source('team',   'FG3M', _OPEN_ALL_CALLS),
    },
    'open_fg3a': {
        'player': _shot_source('player', 'FG3A', _OPEN_ALL_CALLS),
        'team':   _shot_source('team',   'FG3A', _OPEN_ALL_CALLS),
    },

    # ---- Putbacks & dunks (pipeline: filter shooting splits → aggregate) ----

    'putbacks': {
        'player': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'ShotTypePlayerDashboard', 'field': 'FGM',
                     'filter_field': 'GROUP_VALUE', 'filter_values': _PUTBACK_SHOT_TYPES},
                    {'type': 'aggregate', 'method': 'sum'},
                ],
            },
        },
        'team': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashboardbyshootingsplits',
                'execution_tier': 'team',
                'endpoint_params': {'measure_type_detailed_defense': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'ShotTypeTeamDashboard', 'field': 'FGM',
                     'filter_field': 'GROUP_VALUE', 'filter_values': _PUTBACK_SHOT_TYPES},
                    {'type': 'aggregate', 'method': 'sum'},
                ],
            },
        },
    },
    'dunks': {
        'player': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'ShotTypePlayerDashboard', 'field': 'FGM',
                     'filter_field': 'GROUP_VALUE', 'filter_values': _DUNK_SHOT_TYPES},
                    {'type': 'aggregate', 'method': 'sum'},
                ],
            },
        },
        'team': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashboardbyshootingsplits',
                'execution_tier': 'team',
                'endpoint_params': {'measure_type_detailed_defense': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'ShotTypeTeamDashboard', 'field': 'FGM',
                     'filter_field': 'GROUP_VALUE', 'filter_values': _DUNK_SHOT_TYPES},
                    {'type': 'aggregate', 'method': 'sum'},
                ],
            },
        },
    },

    # ---- Unassisted field goals (from shooting splits) ----

    'unassisted_rim_fgm': {
        'player': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'AssistTracking', 'field': 'FGM',
                     'filter_field': 'SHOT_TYPE', 'filter_values': ['AtRim']},
                ],
            },
        },
    },
    'unassisted_fg2m': {
        'player': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'AssistTracking', 'field': 'FGM',
                     'filter_field': 'SHOT_TYPE', 'filter_values': ['2PT']},
                ],
            },
        },
    },
    'unassisted_fg3m': {
        'player': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                'operations': [
                    {'type': 'extract', 'result_set': 'AssistTracking', 'field': 'FGM',
                     'filter_field': 'SHOT_TYPE', 'filter_values': ['3PT']},
                ],
            },
        },
    },

    # ---- Rebounds ----

    'o_rebs': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'OREB', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'OREB', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_OREB', 'transform': 'safe_int'},
    },
    'd_rebs': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'DREB', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'DREB', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_DREB', 'transform': 'safe_int'},
    },
    'o_reb_pct_x1000': {
        'player': {'endpoint': 'leaguedashplayerstats', 'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'OREB_PCT', 'transform': 'safe_int', 'scale': 1000},
        'team':   {'endpoint': 'leaguedashteamstats',   'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'OREB_PCT', 'transform': 'safe_int', 'scale': 1000},
    },
    'd_reb_pct_x1000': {
        'player': {'endpoint': 'leaguedashplayerstats', 'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'DREB_PCT', 'transform': 'safe_int', 'scale': 1000},
        'team':   {'endpoint': 'leaguedashteamstats',   'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'DREB_PCT', 'transform': 'safe_int', 'scale': 1000},
    },
    'cont_o_rebs': {
        'player': {
            'endpoint': 'playerdashptreb', 'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline', 'endpoint': 'playerdashptreb', 'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [{'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_OREB'}],
            },
        },
        'team': {
            'endpoint': 'teamdashptreb',
            'transformation': {
                'type': 'pipeline', 'endpoint': 'teamdashptreb', 'execution_tier': 'team',
                'operations': [{'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_OREB'}],
            },
        },
    },
    'cont_d_rebs': {
        'player': {
            'endpoint': 'playerdashptreb', 'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline', 'endpoint': 'playerdashptreb', 'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [{'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_DREB'}],
            },
        },
        'team': {
            'endpoint': 'teamdashptreb',
            'transformation': {
                'type': 'pipeline', 'endpoint': 'teamdashptreb', 'execution_tier': 'team',
                'operations': [{'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_DREB'}],
            },
        },
    },

    # ---- Playmaking ----

    'assists': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'AST', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'AST', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_AST', 'transform': 'safe_int'},
    },
    'pot_assists': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'},
                   'field': 'POTENTIAL_AST', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
                   'field': 'POTENTIAL_AST', 'transform': 'safe_int'},
    },
    'passes': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'},
                   'field': 'PASSES_MADE', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
                   'field': 'PASSES_MADE', 'transform': 'safe_int'},
    },
    'sec_assists': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'},
                   'field': 'SECONDARY_AST', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
                   'field': 'SECONDARY_AST', 'transform': 'safe_int'},
    },

    # ---- Ball handling ----

    'touches': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Player'},
                   'field': 'TOUCHES', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
                   'field': 'TOUCHES', 'transform': 'safe_int'},
    },
    'time_on_ball': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Player'},
                   'field': 'TIME_OF_POSS', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
                   'field': 'TIME_OF_POSS', 'transform': 'safe_int'},
    },
    'possessions': {
        'player': {'endpoint': 'leaguedashplayerstats', 'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'POSS', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'POSS', 'transform': 'safe_int'},
    },

    # ---- Turnovers ----

    'turnovers': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'TOV', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'TOV', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_TOV', 'transform': 'safe_int'},
    },

    # ---- Distance ----

    'o_dist_x10': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'},
                   'field': 'DIST_MILES_OFF', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
                   'field': 'DIST_MILES_OFF', 'transform': 'safe_int', 'scale': 10},
    },
    'd_dist_x10': {
        'player': {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'},
                   'field': 'DIST_MILES_DEF', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguedashptstats', 'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
                   'field': 'DIST_MILES_DEF', 'transform': 'safe_int', 'scale': 10},
    },

    # ---- Defense: steals / blocks / fouls ----

    'steals': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'STL', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'STL', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_STL', 'transform': 'safe_int'},
    },
    'blocks': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'BLK', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'BLK', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_BLK', 'transform': 'safe_int'},
    },
    'fouls': {
        'player': {'endpoint': 'leaguedashplayerstats', 'field': 'PF', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashteamstats',   'field': 'PF', 'transform': 'safe_int'},
        'opponent': {'endpoint': 'leaguedashteamstats', 'params': {'measure_type_detailed_defense': 'Opponent'},
                     'field': 'OPP_PF', 'transform': 'safe_int'},
    },

    # ---- Hustle stats ----

    'deflections': {
        'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'DEFLECTIONS', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'DEFLECTIONS', 'transform': 'safe_int'},
    },
    'charges_drawn': {
        'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'CHARGES_DRAWN', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'CHARGES_DRAWN', 'transform': 'safe_int'},
    },
    'contests': {
        'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'CONTESTED_SHOTS', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'CONTESTED_SHOTS', 'transform': 'safe_int'},
    },

    # ---- Defensive shot tracking (leaguedashptdefend / leaguedashptteamdefend) ----

    'd_rim_fgm': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': 'Less Than 10Ft'}, 'field': 'FGM', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': 'Less Than 10Ft'}, 'field': 'FGM', 'transform': 'safe_int'},
    },
    'd_rim_fga': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': 'Less Than 10Ft'}, 'field': 'FGA_LT_10', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': 'Less Than 10Ft'}, 'field': 'FGA_LT_10', 'transform': 'safe_int'},
    },
    'd_fg2m': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': '2 Pointers'}, 'field': 'FG2M', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': '2 Pointers'}, 'field': 'FG2M', 'transform': 'safe_int'},
    },
    'd_fg2a': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': '2 Pointers'}, 'field': 'FG2A', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': '2 Pointers'}, 'field': 'FG2A', 'transform': 'safe_int'},
    },
    'd_fg3m': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': '3 Pointers'}, 'field': 'FG3M', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': '3 Pointers'}, 'field': 'FG3M', 'transform': 'safe_int'},
    },
    'd_fg3a': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': '3 Pointers'}, 'field': 'FG3A', 'transform': 'safe_int'},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': '3 Pointers'}, 'field': 'FG3A', 'transform': 'safe_int'},
    },

    # ---- Ratings ----

    'o_rtg_x10': {
        'player': {'endpoint': 'leaguedashplayerstats', 'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'OFF_RATING', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguedashteamstats',   'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'OFF_RATING', 'transform': 'safe_int', 'scale': 10},
    },
    'd_rtg_x10': {
        'player': {'endpoint': 'leaguedashplayerstats', 'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'DEF_RATING', 'transform': 'safe_int', 'scale': 10},
        'team':   {'endpoint': 'leaguedashteamstats',   'params': {'measure_type_detailed_defense': 'Advanced'},
                   'field': 'DEF_RATING', 'transform': 'safe_int', 'scale': 10},
    },
    'off_o_rtg_x10': {
        'player': {
            'endpoint': 'teamplayeronoffsummary',
            'execution_tier': 'team_call',
            'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
            'player_id_field': 'VS_PLAYER_ID',
            'field': 'OFF_RATING', 'transform': 'safe_int', 'scale': 10,
            'aggregation': 'minute_weighted',
        },
    },
    'off_d_rtg_x10': {
        'player': {
            'endpoint': 'teamplayeronoffsummary',
            'execution_tier': 'team_call',
            'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
            'player_id_field': 'VS_PLAYER_ID',
            'field': 'DEF_RATING', 'transform': 'safe_int', 'scale': 10,
            'aggregation': 'minute_weighted',
        },
    },

    # ---- Defensive real FG% differentials (leaguedashptdefend) ----

    'real_d_fg_pct_x1000': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': 'Overall'},
                   'field': 'PCT_PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': 'Overall'},
                   'field': 'PCT_PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
    },
    'real_d_rim_fg_pct_x1000': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': 'Less Than 10Ft'},
                   'field': 'PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': 'Less Than 10Ft'},
                   'field': 'PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
    },
    'real_d_fg2_pct_x1000': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': '2 Pointers'},
                   'field': 'PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': '2 Pointers'},
                   'field': 'PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
    },
    'real_d_fg3_pct_x1000': {
        'player': {'endpoint': 'leaguedashptdefend',     'params': {'defense_category': '3 Pointers'},
                   'field': 'PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
        'team':   {'endpoint': 'leaguedashptteamdefend', 'params': {'defense_category': '3 Pointers'},
                   'field': 'PLUSMINUS', 'transform': 'safe_int', 'scale': 1000},
    },
}


# ============================================================================
# DATA INTEGRITY RULES
# ============================================================================

DATA_INTEGRITY_RULES = {
    'dependencies': {
        'o_rebs':      ['o_reb_pct_x1000', 'cont_o_rebs'],
        'd_rebs':      ['d_reb_pct_x1000', 'cont_d_rebs'],
        'minutes_x10': ['o_rtg_x10'],
        'assists':     ['pot_assists', 'passes', 'touches'],
        'fg2m':        ['putbacks', 'dunks'],
    },
    'minimum_thresholds': {
        'o_rebs': 10, 'd_rebs': 10, 'minutes_x10': 100,
        'assists': 10, 'fg2m': 10, 'fg2a': 10,
        'fg3m': 10, 'fg3a': 10,
    },
    'sum_validations': {
        'fg2m': {
            'components': ['open_fg2m', 'cont_fg2m'],
            'special_case_components': ['open_rim_fgm', 'cont_rim_fgm'],
        },
        'fg2a': {
            'components': ['open_fg2a', 'cont_fg2a'],
            'special_case_components': ['open_rim_fga', 'cont_rim_fga'],
        },
        'fg3m': {'components': ['open_fg3m', 'cont_fg3m']},
        'fg3a': {'components': ['open_fg3a', 'cont_fg3a']},
    },
}


# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_columns_for_endpoint(
    endpoint_name: str,
    entity: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Get all SOURCES entries whose source for *entity* uses *endpoint_name*.

    Optionally filters by matching source params (e.g. pt_measure_type).
    Returns ``{canonical_col_name: source_config_dict}``.
    """
    matched = {}
    for col_name, entity_map in SOURCES.items():
        source = entity_map.get(entity)
        if not source:
            continue
        if source.get('endpoint') != endpoint_name:
            continue
        if params:
            source_params = source.get('params', {})
            if not all(source_params.get(k) == v for k, v in params.items()):
                continue
        matched[col_name] = source
    return matched


def is_endpoint_available(endpoint_name: str, season: str) -> bool:
    """Check whether *endpoint_name* has data for *season*."""
    ep = ENDPOINTS.get(endpoint_name)
    if not ep:
        return False
    min_season = ep.get('min_season')
    if min_season is None:
        return True
    return season >= min_season
