"""
The Glass - NBA Provider Configuration

Pure data definitions for the NBA API provider: endpoint metadata,
rate limits, season boundaries, and field name mappings.

All query logic (column lookups, call grouping, team ID resolution)
lives in nba_api/resolver.py.  Column schema and NBA source mappings
live in the unified config (src/input/config.py).
"""

import os
from typing import Any, Dict

from src.core.db import get_current_season, get_current_season_year
from src.etl.core.transform import format_season_end_year


# ============================================================================
# SCHEMA
# ============================================================================

DB_SCHEMA = 'nba'


# ============================================================================
# SEASON CONFIGURATION
# ============================================================================

SEASON_CONFIG = {
    'current_season': get_current_season(),
    'current_season_year': get_current_season_year(),
    'season_type': os.getenv('SEASON_TYPE', 'rs'),
    'backfill_start': '2003-04',
    'tracking_start': '2013-14',
    'hustle_start': '2015-16',
    'onoff_start': '2007-08',
    'combine_start_year': 2003,
}


format_season = format_season_end_year


SEASON_TYPES = {
    'rs': {'name': 'Regular Season', 'param': 'Regular Season', 'min_season': None},
    'po': {'name': 'Playoffs',       'param': 'Playoffs',       'min_season': None},
    'pi': {'name': 'PlayIn',         'param': 'PlayIn',         'min_season': '2020-21'},
}


# ============================================================================
# API OPERATIONAL SETTINGS
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

    'league_id': '00',
    'per_mode_simple': 'Totals',
    'per_mode_time': 'Totals',
    'per_mode_detailed': 'Totals',
    'last_n_games': '0',
    'month': '0',
    'opponent_team_id': '0',
    'period': '0',
}

RETRY_CONFIG = {
    'max_retries': 3,
    'backoff_base': 30,
}


# ============================================================================
# ENDPOINT DEFINITIONS
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
    },
    'leaguedashteamstats': {
        'min_season': '2003-04',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashTeamStats',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
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
    },

    # --- Hustle stats (since 2015-16) ---

    'leaguehustlestatsplayer': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsPlayer',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['player'],
    },
    'leaguehustlestatsteam': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsTeam',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['team'],
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
    },
    'leaguedashptteamdefend': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPtTeamDefend',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'requires_params': ['defense_category'],
        'entity_types': ['team'],
    },

    # --- Shot tracking league-wide (since 2013-14) ---

    'leaguedashplayerptshot': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPTShots',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
    },
    'leaguedashteamptshot': {
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'LeagueDashPTShots',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
    },

    # --- Rebound tracking (since 2013-14) ---

    'playerdashptreb': {
        'min_season': '2013-14',
        'execution_tier': 'player',
        'default_result_set': 'OverallRebounding',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['player'],
    },
    'teamdashptreb': {
        'min_season': '2013-14',
        'execution_tier': 'team',
        'default_result_set': 'OverallRebounding',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_simple',
        'entity_types': ['team'],
    },

    # --- Shooting splits (since 2012-13) ---

    'playerdashboardbyshootingsplits': {
        'min_season': '2012-13',
        'execution_tier': 'player',
        'default_result_set': 'ShotTypePlayerDashboard',
        'season_type_param': 'season_type_playoffs',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
    },
    'teamdashboardbyshootingsplits': {
        'min_season': '2012-13',
        'execution_tier': 'league',
        'default_result_set': 'ShotTypeTeamDashboard',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
    },

    # --- Player info (all time) ---

    'commonallplayers': {
        'min_season': None,
        'execution_tier': 'league',
        'default_result_set': 'CommonAllPlayers',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
    },

    'commonplayerinfo': {
        'min_season': None,
        'execution_tier': 'player',
        'default_result_set': 'CommonPlayerInfo',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
    },

    # --- Draft combine (since 2000-01) ---

    'draftcombineplayeranthro': {
        'min_season': '2000-01',
        'execution_tier': 'league',
        'default_result_set': 'DraftCombinePlayerAnthro',
        'season_param': 'season_year',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
    },

    # --- On/Off court (since 2007-08) ---

    'teamplayeronoffsummary': {
        'min_season': '2007-08',
        'execution_tier': 'league',
        'default_result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['player'],
    },

    # --- Virtual: team metadata (abbreviation + conference) ---
    # Combines nba_api static teams data with LeagueStandings.
    # No real NBA API class — handled by fetch_team_metadata() in client.py.

    'team_metadata': {
        'min_season': None,
        'execution_tier': 'league',
        'default_result_set': 'TeamMetadata',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['team'],
        'virtual': True,
    },
}


# ============================================================================
# API FIELD NAME MAPPINGS
# ============================================================================

API_FIELD_NAMES = {
    'entity_id':   {'player': 'PLAYER_ID', 'team': 'TEAM_ID'},
    'entity_name': {'player': 'PLAYER_NAME', 'team': 'TEAM_NAME'},
    'special_ids': {'person': 'PERSON_ID'},
    'id_aliases':  {'PLAYER_ID': ['PERSON_ID']},
}


# ============================================================================
# VALIDATION SCHEMAS  (co-located with the config they describe)
# ============================================================================

VALID_EXECUTION_TIERS = {'league', 'player', 'team', 'team_call'}

ENDPOINTS_SCHEMA = {
    'min_season': {'required': True, 'types': (str, type(None))},
    'execution_tier': {'required': True, 'types': (str,), 'allowed_values': VALID_EXECUTION_TIERS},
    'default_result_set': {'required': True, 'types': (str,)},
    'season_type_param': {'required': True, 'types': (str, type(None))},
    'per_mode_param': {'required': True, 'types': (str, type(None))},
    'entity_types': {'required': True, 'types': (list,), 'list_item_values': {'player', 'team'}},
    'requires_params': {'required': False, 'types': (list,)},
    'virtual': {'required': False, 'types': (bool,)},
}

SEASON_TYPES_SCHEMA = {
    'name': {'required': True, 'types': (str,)},
    'param': {'required': True, 'types': (str,)},
    'min_season': {'required': True, 'types': (str, type(None))},
}
