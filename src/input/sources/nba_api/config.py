"""
The Glass - NBA Provider Configuration

Operational settings for the NBA API: endpoint definitions, rate limits,
season boundaries, team ID resolution, and query helpers.

Column definitions and source mappings live in the unified config
(src/etl/config.py).  This module contains everything needed to *execute*
NBA API calls but nothing about what columns they populate.
"""

import os
from typing import Any, Dict, List, Optional

from src.db import get_current_season, get_current_season_year
from src.etl.config import DB_COLUMNS, TYPE_TRANSFORMS


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
# TEAM IDS  (lazy-loaded from database, cached)
# ============================================================================

_team_ids_cache: Optional[Dict[str, int]] = None


def get_team_ids() -> Dict[str, int]:
    """Load team nba_api_id->abbr mapping from the database.

    Cached after first call so only one query per process lifetime.
    """
    global _team_ids_cache
    if _team_ids_cache is None:
        from src.db import db_connection
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT nba_api_id, abbr FROM {DB_SCHEMA}.teams "
                    f"ORDER BY nba_api_id"
                )
                _team_ids_cache = {row[1]: int(row[0]) for row in cur.fetchall()}
    return _team_ids_cache


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
}


# ============================================================================
# API FIELD NAME MAPPINGS
# ============================================================================

API_FIELD_NAMES = {
    'entity_id':   {'player': 'PLAYER_ID', 'team': 'TEAM_ID'},
    'entity_name': {'player': 'PLAYER_NAME', 'team': 'TEAM_NAME'},
    'special_ids': {'person': 'PERSON_ID'},
}


def get_entity_id_field(entity: str) -> str:
    """Return the NBA API header name for a given entity's ID column."""
    return API_FIELD_NAMES['entity_id'][entity]


# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_columns_for_endpoint(
    endpoint_name: str,
    entity: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Find all columns whose NBA source maps to the given endpoint.

    Walks DB_COLUMNS, looks under the ``'nba'`` provider key, and returns
    ``{col_name: enriched_source_dict}`` with default transforms injected.
    Columns with ``multi_call`` or ``pipeline`` sources are included so the
    runner can classify them.

    Args:
        endpoint_name: NBA API endpoint (e.g. ``'leaguedashplayerstats'``).
        entity:        ``'player'`` or ``'team'``.
        params:        Optional param filter — only include columns whose
                       source params are a superset of these.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = col_meta.get('nba', {}).get(entity)
        if not source:
            continue

        # Determine the endpoint from the source (may be nested in pipeline)
        ep = source.get('endpoint')
        if not ep:
            pipeline = source.get('pipeline', {})
            ep = pipeline.get('endpoint')
        if ep != endpoint_name:
            continue

        # Optional param matching (for sources with extra params)
        if params:
            source_params = source.get('params', {})
            if not all(source_params.get(k) == v for k, v in params.items()):
                continue

        # Enrich with default transform based on column type
        enriched = {**source}
        if 'transform' not in enriched and 'pipeline' not in enriched and 'multi_call' not in enriched:
            base_type = col_meta['type'].split('(')[0]
            enriched['transform'] = TYPE_TRANSFORMS.get(base_type, 'safe_int')

        matched[col_name] = enriched

    return matched


def get_all_sources_for_entity(
    entity: str,
    season: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return every column with an NBA source for the entity.

    If *season* is provided, excludes endpoints that aren't available
    for that season.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = col_meta.get('nba', {}).get(entity)
        if not source:
            continue

        if season:
            ep = source.get('endpoint') or source.get('pipeline', {}).get('endpoint', '')
            if not is_endpoint_available(ep, season):
                continue

        enriched = {**source}
        if 'transform' not in enriched and 'pipeline' not in enriched and 'multi_call' not in enriched:
            base_type = col_meta['type'].split('(')[0]
            enriched['transform'] = TYPE_TRANSFORMS.get(base_type, 'safe_int')

        matched[col_name] = enriched

    return matched


def is_endpoint_available(endpoint_name: str, season: str) -> bool:
    """Check whether an endpoint has data for the given season."""
    ep = ENDPOINTS.get(endpoint_name)
    if not ep:
        return False
    min_season = ep.get('min_season')
    if min_season is None:
        return True
    return season >= min_season
