"""
The Glass - Column Registry & Source Mappings

Single source of truth for database schema, column definitions, and
provider-specific API source mappings across all leagues.

Column names match the actual PostgreSQL schema exactly. Each column
includes schema metadata and provider source mappings (under 'nba' key).

Source mapping patterns:
  Simple:      {'endpoint': '...', 'field': '...'}
  With params: {'endpoint': '...', 'field': '...', 'params': {...}}
  With scale:  {'endpoint': '...', 'field': '...', 'scale': N}
  Derived:     {'endpoint': '...', 'field': '...', 'derived': {'subtract': '...'}}
  Pipeline:    {'pipeline': {'endpoint': '...', 'tier': '...', 'operations': [...]}}
  Multi-call:  {'endpoint': '...', 'field': '...', 'multi_call': [...param_sets...]}
  Team-call:   {'endpoint': '...', 'tier': 'team_call', 'result_set': '...', ...}

Transforms are inferred from column type unless overridden:
  SMALLINT / INTEGER -> safe_int
  VARCHAR / TEXT / CHAR -> safe_str
  Override with 'transform' key (e.g., parse_height, parse_birthdate, format_season)
"""

from typing import Any, Dict


# ============================================================================
# DEFAULT TRANSFORMS BY COLUMN TYPE
# ============================================================================

TYPE_TRANSFORMS = {
    'SMALLINT': 'safe_int',
    'INTEGER': 'safe_int',
    'VARCHAR': 'safe_str',
    'TEXT': 'safe_str',
    'CHAR': 'safe_str',
}


# ============================================================================
# TABLE DEFINITIONS
# ============================================================================

TABLES = {
    'players': {
        'entity': 'player',
        'scope': 'entity',
        'unique_key': ['nba_api_id'],
    },
    'teams': {
        'entity': 'team',
        'scope': 'entity',
        'unique_key': ['nba_api_id'],
    },
    'player_season_stats': {
        'entity': 'player',
        'scope': 'stats',
        'unique_key': ['nba_api_id', 'season', 'season_type'],
    },
    'team_season_stats': {
        'entity': 'team',
        'scope': 'stats',
        'unique_key': ['nba_api_id', 'season', 'season_type'],
        'has_opponent_columns': True,
    },
}


# ============================================================================
# SHOT TYPE CONSTANTS  (referenced by pipeline source mappings)
# ============================================================================

PUTBACK_SHOT_TYPES = [
    'Putback Dunk Shot', 'Putback Layup Shot',
    'Tip Dunk Shot', 'Tip Layup Shot',
]

DUNK_SHOT_TYPES = [
    'Alley Oop Dunk Shot', 'Cutting Dunk Shot', 'Driving Dunk Shot',
    'Driving Reverse Dunk Shot', 'Dunk Shot', 'Putback Dunk Shot',
    'Reverse Dunk Shot', 'Running Alley Oop Dunk Shot',
    'Running Dunk Shot', 'Tip Dunk Shot',
]


# ============================================================================
# SHOT TRACKING PARAMETER SETS  (multi-call sources aggregate across these)
# ============================================================================

CONTESTED_RIM_PARAMS = [
    {'close_def_dist_range_nullable': '0-2 Feet - Very Tight', 'general_range_nullable': 'Less Than 10 ft'},
    {'close_def_dist_range_nullable': '2-4 Feet - Tight',      'general_range_nullable': 'Less Than 10 ft'},
]

OPEN_RIM_PARAMS = [
    {'close_def_dist_range_nullable': '4-6 Feet - Open',       'general_range_nullable': 'Less Than 10 ft'},
    {'close_def_dist_range_nullable': '6+ Feet - Wide Open',   'general_range_nullable': 'Less Than 10 ft'},
]

CONTESTED_ALL_PARAMS = [
    {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
    {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
]

OPEN_ALL_PARAMS = [
    {'close_def_dist_range_nullable': '4-6 Feet - Open'},
    {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
]


# ============================================================================
# COLUMN REGISTRY
# ============================================================================

DB_COLUMNS: Dict[str, Dict[str, Any]] = {

    # ------------------------------------------------------------------
    # SYSTEM COLUMNS  (managed by DB / ETL engine, no provider sources)
    # ------------------------------------------------------------------

    'id': {
        'type': 'SERIAL',
        'scope': ['entity', 'stats'],
        'nullable': False,
        'primary_key': True,
        'entity_types': ['player', 'team'],
    },
    'nba_api_id': {
        'type': 'VARCHAR(10)',
        'scope': ['entity', 'stats'],
        'nullable': False,
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'PLAYER_ID', 'transform': 'safe_str'},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'TEAM_ID',   'transform': 'safe_str'},
        },
    },
    'updated_at': {
        'type': 'TIMESTAMP',
        'scope': ['entity', 'stats'],
        'nullable': True,
        'default': 'CURRENT_TIMESTAMP',
        'entity_types': ['player', 'team'],
    },
    'created_at': {
        'type': 'TIMESTAMP',
        'scope': ['entity'],
        'nullable': True,
        'default': 'CURRENT_TIMESTAMP',
        'entity_types': ['player', 'team'],
    },
    'season': {
        'type': 'VARCHAR(7)',
        'scope': ['stats'],
        'nullable': False,
        'entity_types': ['player', 'team'],
    },
    'season_type': {
        'type': 'VARCHAR(3)',
        'scope': ['stats'],
        'nullable': False,
        'entity_types': ['player', 'team'],
    },
    'backfilled': {
        'type': 'BOOLEAN',
        'scope': ['entity'],
        'nullable': False,
        'default': 'FALSE',
        'entity_types': ['player', 'team'],
    },
    'notes': {
        'type': 'TEXT',
        'scope': ['entity'],
        'nullable': True,
        'entity_types': ['player', 'team'],
    },

    # ------------------------------------------------------------------
    # ENTITY INFORMATION  (player / team profile data)
    # ------------------------------------------------------------------

    'team_id': {
        'type': 'INTEGER',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'TEAM_ID'},
        },
    },
    'name': {
        'type': 'VARCHAR(100)',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'PLAYER_NAME', 'transform': 'safe_str'},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'TEAM_NAME',   'transform': 'safe_str'},
        },
    },
    'height_ins': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'HEIGHT', 'transform': 'parse_height'},
        },
    },
    'weight_lbs': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'WEIGHT'},
        },
    },
    'wingspan_ins': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'draftcombineplayeranthro', 'field': 'WINGSPAN', 'transform': 'parse_height'},
        },
    },
    'jersey_num': {
        'type': 'VARCHAR(3)',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'JERSEY'},
        },
    },
    'birthdate': {
        'type': 'DATE',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'BIRTHDATE', 'transform': 'parse_birthdate'},
        },
    },
    'hand': {
        'type': 'CHAR',
        'scope': ['entity'],
        'nullable': True,
        'entity_types': ['player'],
    },
    'seasons_exp': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'SEASON_EXP'},
        },
    },
    'rookie_season': {
        'type': 'VARCHAR(10)',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['player'],
        'nba': {
            'player': {'endpoint': 'commonplayerinfo', 'field': 'FROM_YEAR', 'transform': 'format_season'},
        },
    },
    'abbr': {
        'type': 'VARCHAR(5)',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['team'],
        'nba': {
            'team': {'endpoint': 'leaguedashteamstats', 'field': 'TEAM_ABBREVIATION'},
        },
    },
    'conf': {
        'type': 'VARCHAR(50)',
        'scope': ['entity'],
        'nullable': True,
        'update_frequency': 'annual',
        'entity_types': ['team'],
        'nba': {
            'team': {'endpoint': 'leaguedashteamstats', 'field': 'CONFERENCE'},
        },
    },

    # ------------------------------------------------------------------
    # GAMES & MINUTES
    # ------------------------------------------------------------------

    'games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'GP'},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'GP'},
        },
    },
    'minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'MIN', 'scale': 10},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'MIN', 'scale': 10},
        },
    },
    'wins': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'W'},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'W'},
        },
    },
    'tr_games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'GP',
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'GP',
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'}},
        },
    },
    'tr_minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'MIN', 'scale': 10,
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'MIN', 'scale': 10,
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'}},
        },
    },
    'h_games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'hustle',
        'nba': {
            'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'GP'},
            'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'GP'},
        },
    },
    'h_minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'hustle',
        'nba': {
            'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'MIN', 'scale': 10},
            'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'MIN', 'scale': 10},
        },
    },
    'off_games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'rate_group': 'onoff',
        'nba': {
            'player': {
                'endpoint': 'teamplayeronoffsummary',
                'tier': 'team_call',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                'player_id_field': 'VS_PLAYER_ID',
                'field': 'GP',
                'aggregation': 'sum',
            },
        },
    },
    'off_minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'onoff',
        'nba': {
            'player': {
                'endpoint': 'teamplayeronoffsummary',
                'tier': 'team_call',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                'player_id_field': 'VS_PLAYER_ID',
                'field': 'MIN', 'scale': 10,
                'aggregation': 'sum',
            },
        },
    },

    # ------------------------------------------------------------------
    # SCORING: 2-POINT
    # ------------------------------------------------------------------

    'fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'FGM',
                         'derived': {'subtract': 'FG3M'}},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'FGM',
                         'derived': {'subtract': 'FG3M'}},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_FGM',
                         'params': {'measure_type_detailed_defense': 'Opponent'},
                         'derived': {'subtract': 'OPP_FG3M'}},
        },
    },
    'fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'FGA',
                         'derived': {'subtract': 'FG3A'}},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'FGA',
                         'derived': {'subtract': 'FG3A'}},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_FGA',
                         'params': {'measure_type_detailed_defense': 'Opponent'},
                         'derived': {'subtract': 'OPP_FG3A'}},
        },
    },

    # ------------------------------------------------------------------
    # SCORING: 3-POINT
    # ------------------------------------------------------------------

    'fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'FG3M'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'FG3M'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_FG3M',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'FG3A'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'FG3A'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_FG3A',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },

    # ------------------------------------------------------------------
    # SCORING: FREE THROWS
    # ------------------------------------------------------------------

    'ftm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'FTM'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'FTM'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_FTM',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'fta': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'FTA'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'FTA'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_FTA',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },

    # ------------------------------------------------------------------
    # SHOT TRACKING: CONTESTED / OPEN  x  RIM / ALL
    # ------------------------------------------------------------------

    'cont_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FGM',
                       'multi_call': CONTESTED_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FGM',
                       'multi_call': CONTESTED_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'cont_rim_fga': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FGA',
                       'multi_call': CONTESTED_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FGA',
                       'multi_call': CONTESTED_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'open_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FGM',
                       'multi_call': OPEN_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FGM',
                       'multi_call': OPEN_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'open_rim_fga': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FGA',
                       'multi_call': OPEN_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FGA',
                       'multi_call': OPEN_RIM_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'cont_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG2M',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG2M',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'cont_fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG2A',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG2A',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'open_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG2M',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG2M',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'open_fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG2A',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG2A',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'cont_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG3M',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG3M',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'cont_fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG3A',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG3A',
                       'multi_call': CONTESTED_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'open_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG3M',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG3M',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },
    'open_fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashplayerptshot', 'field': 'FG3A',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
            'team':   {'endpoint': 'leaguedashteamptshot',   'field': 'FG3A',
                       'multi_call': OPEN_ALL_PARAMS, 'result_set': 'LeagueDashPTShots'},
        },
    },

    # ------------------------------------------------------------------
    # PUTBACKS & DUNKS  (pipeline: filter shooting splits -> aggregate)
    # ------------------------------------------------------------------

    'putbacks': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashboardbyshootingsplits',
                    'tier': 'player',
                    'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'ShotTypePlayerDashboard', 'field': 'FGM',
                         'filter_field': 'GROUP_VALUE', 'filter_values': PUTBACK_SHOT_TYPES},
                        {'type': 'aggregate', 'method': 'sum'},
                    ],
                },
            },
            'team': {
                'pipeline': {
                    'endpoint': 'teamdashboardbyshootingsplits',
                    'tier': 'team',
                    'params': {'measure_type_detailed_defense': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'ShotTypeTeamDashboard', 'field': 'FGM',
                         'filter_field': 'GROUP_VALUE', 'filter_values': PUTBACK_SHOT_TYPES},
                        {'type': 'aggregate', 'method': 'sum'},
                    ],
                },
            },
        },
    },
    'dunks': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashboardbyshootingsplits',
                    'tier': 'player',
                    'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'ShotTypePlayerDashboard', 'field': 'FGM',
                         'filter_field': 'GROUP_VALUE', 'filter_values': DUNK_SHOT_TYPES},
                        {'type': 'aggregate', 'method': 'sum'},
                    ],
                },
            },
            'team': {
                'pipeline': {
                    'endpoint': 'teamdashboardbyshootingsplits',
                    'tier': 'team',
                    'params': {'measure_type_detailed_defense': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'ShotTypeTeamDashboard', 'field': 'FGM',
                         'filter_field': 'GROUP_VALUE', 'filter_values': DUNK_SHOT_TYPES},
                        {'type': 'aggregate', 'method': 'sum'},
                    ],
                },
            },
        },
    },

    # ------------------------------------------------------------------
    # UNASSISTED FIELD GOALS  (per-player shooting splits)
    # ------------------------------------------------------------------

    'unassisted_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashboardbyshootingsplits',
                    'tier': 'player',
                    'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'AssistTracking', 'field': 'FGM',
                         'filter_field': 'SHOT_TYPE', 'filter_values': ['AtRim']},
                    ],
                },
            },
        },
    },
    'unassisted_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashboardbyshootingsplits',
                    'tier': 'player',
                    'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'AssistTracking', 'field': 'FGM',
                         'filter_field': 'SHOT_TYPE', 'filter_values': ['2PT']},
                    ],
                },
            },
        },
    },
    'unassisted_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashboardbyshootingsplits',
                    'tier': 'player',
                    'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                    'operations': [
                        {'type': 'extract', 'result_set': 'AssistTracking', 'field': 'FGM',
                         'filter_field': 'SHOT_TYPE', 'filter_values': ['3PT']},
                    ],
                },
            },
        },
    },

    # ------------------------------------------------------------------
    # REBOUNDS
    # ------------------------------------------------------------------

    'o_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'OREB'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'OREB'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_OREB',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'd_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'DREB'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'DREB'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_DREB',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'o_reb_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'OREB_PCT', 'scale': 1000,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'OREB_PCT', 'scale': 1000,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
        },
    },
    'd_reb_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'DREB_PCT', 'scale': 1000,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'DREB_PCT', 'scale': 1000,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
        },
    },
    'cont_o_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashptreb',
                    'tier': 'player',
                    'params': {'team_id': 0},
                    'operations': [
                        {'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_OREB'},
                    ],
                },
            },
            'team': {
                'pipeline': {
                    'endpoint': 'teamdashptreb',
                    'tier': 'team',
                    'operations': [
                        {'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_OREB'},
                    ],
                },
            },
        },
    },
    'cont_d_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {
                'pipeline': {
                    'endpoint': 'playerdashptreb',
                    'tier': 'player',
                    'params': {'team_id': 0},
                    'operations': [
                        {'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_DREB'},
                    ],
                },
            },
            'team': {
                'pipeline': {
                    'endpoint': 'teamdashptreb',
                    'tier': 'team',
                    'operations': [
                        {'type': 'extract', 'result_set': 'OverallRebounding', 'field': 'C_DREB'},
                    ],
                },
            },
        },
    },

    # ------------------------------------------------------------------
    # PLAYMAKING
    # ------------------------------------------------------------------

    'assists': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'AST'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'AST'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_AST',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'pot_assists': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'POTENTIAL_AST',
                       'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'POTENTIAL_AST',
                       'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'}},
        },
    },
    'passes': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'PASSES_MADE',
                       'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'PASSES_MADE',
                       'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'}},
        },
    },
    'sec_assists': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'SECONDARY_AST',
                       'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'SECONDARY_AST',
                       'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'}},
        },
    },

    # ------------------------------------------------------------------
    # BALL HANDLING
    # ------------------------------------------------------------------

    'touches': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'TOUCHES',
                       'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'TOUCHES',
                       'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'}},
        },
    },
    'time_on_ball': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'TIME_OF_POSS',
                       'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'TIME_OF_POSS',
                       'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'}},
        },
    },
    'possessions': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'POSS',
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'POSS',
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
        },
    },

    # ------------------------------------------------------------------
    # TURNOVERS
    # ------------------------------------------------------------------

    'turnovers': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'TOV'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'TOV'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_TOV',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },

    # ------------------------------------------------------------------
    # DISTANCE
    # ------------------------------------------------------------------

    'o_dist_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'DIST_MILES_OFF', 'scale': 10,
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'DIST_MILES_OFF', 'scale': 10,
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'}},
        },
    },
    'd_dist_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptstats', 'field': 'DIST_MILES_DEF', 'scale': 10,
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'}},
            'team':   {'endpoint': 'leaguedashptstats', 'field': 'DIST_MILES_DEF', 'scale': 10,
                       'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'}},
        },
    },

    # ------------------------------------------------------------------
    # DEFENSE: STEALS / BLOCKS / FOULS
    # ------------------------------------------------------------------

    'steals': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'STL'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'STL'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_STL',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'blocks': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'BLK'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'BLK'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_BLK',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },
    'fouls': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team', 'opponent'],
        'nba': {
            'player':   {'endpoint': 'leaguedashplayerstats', 'field': 'PF'},
            'team':     {'endpoint': 'leaguedashteamstats',   'field': 'PF'},
            'opponent': {'endpoint': 'leaguedashteamstats',   'field': 'OPP_PF',
                         'params': {'measure_type_detailed_defense': 'Opponent'}},
        },
    },

    # ------------------------------------------------------------------
    # HUSTLE STATS
    # ------------------------------------------------------------------

    'deflections': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'hustle',
        'nba': {
            'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'DEFLECTIONS'},
            'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'DEFLECTIONS'},
        },
    },
    'charges_drawn': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'hustle',
        'nba': {
            'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'CHARGES_DRAWN'},
            'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'CHARGES_DRAWN'},
        },
    },
    'contests': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'hustle',
        'nba': {
            'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'CONTESTED_SHOTS'},
            'team':   {'endpoint': 'leaguehustlestatsteam',   'field': 'CONTESTED_SHOTS'},
        },
    },

    # ------------------------------------------------------------------
    # DEFENSIVE SHOT TRACKING  (leaguedashptdefend / leaguedashptteamdefend)
    # ------------------------------------------------------------------

    'd_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'FGM',
                       'params': {'defense_category': 'Less Than 10Ft'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'FGM',
                       'params': {'defense_category': 'Less Than 10Ft'}},
        },
    },
    'd_rim_fga': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'FGA_LT_10',
                       'params': {'defense_category': 'Less Than 10Ft'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'FGA_LT_10',
                       'params': {'defense_category': 'Less Than 10Ft'}},
        },
    },
    'd_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'FG2M',
                       'params': {'defense_category': '2 Pointers'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'FG2M',
                       'params': {'defense_category': '2 Pointers'}},
        },
    },
    'd_fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'FG2A',
                       'params': {'defense_category': '2 Pointers'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'FG2A',
                       'params': {'defense_category': '2 Pointers'}},
        },
    },
    'd_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'FG3M',
                       'params': {'defense_category': '3 Pointers'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'FG3M',
                       'params': {'defense_category': '3 Pointers'}},
        },
    },
    'd_fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'FG3A',
                       'params': {'defense_category': '3 Pointers'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'FG3A',
                       'params': {'defense_category': '3 Pointers'}},
        },
    },

    # ------------------------------------------------------------------
    # RATINGS
    # ------------------------------------------------------------------

    'o_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'OFF_RATING', 'scale': 10,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'OFF_RATING', 'scale': 10,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
        },
    },
    'd_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'nba': {
            'player': {'endpoint': 'leaguedashplayerstats', 'field': 'DEF_RATING', 'scale': 10,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
            'team':   {'endpoint': 'leaguedashteamstats',   'field': 'DEF_RATING', 'scale': 10,
                       'params': {'measure_type_detailed_defense': 'Advanced'}},
        },
    },
    'off_o_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'rate_group': 'onoff',
        'nba': {
            'player': {
                'endpoint': 'teamplayeronoffsummary',
                'tier': 'team_call',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                'player_id_field': 'VS_PLAYER_ID',
                'field': 'OFF_RATING', 'scale': 10,
                'aggregation': 'minute_weighted',
            },
        },
    },
    'off_d_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player'],
        'rate_group': 'onoff',
        'nba': {
            'player': {
                'endpoint': 'teamplayeronoffsummary',
                'tier': 'team_call',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                'player_id_field': 'VS_PLAYER_ID',
                'field': 'DEF_RATING', 'scale': 10,
                'aggregation': 'minute_weighted',
            },
        },
    },

    # ------------------------------------------------------------------
    # DEFENSIVE REAL FG% DIFFERENTIALS
    # ------------------------------------------------------------------

    'real_d_fg_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'PCT_PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': 'Overall'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'PCT_PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': 'Overall'}},
        },
    },
    'real_d_rim_fg_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': 'Less Than 10Ft'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': 'Less Than 10Ft'}},
        },
    },
    'real_d_fg2_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': '2 Pointers'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': '2 Pointers'}},
        },
    },
    'real_d_fg3_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'update_frequency': 'daily',
        'entity_types': ['player', 'team'],
        'rate_group': 'tracking',
        'nba': {
            'player': {'endpoint': 'leaguedashptdefend',     'field': 'PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': '3 Pointers'}},
            'team':   {'endpoint': 'leaguedashptteamdefend', 'field': 'PLUSMINUS', 'scale': 1000,
                       'params': {'defense_category': '3 Pointers'}},
        },
    },
}
