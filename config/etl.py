"""
The Glass ETL - Configuration Module

Pure configuration data: database settings, NBA API constants, column schemas.
All reusable functions moved to lib.etl for separation of data vs code.
"""
import os
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', ''),
    'port': int(os.getenv('DB_PORT', '')),
    'database': os.getenv('DB_NAME', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', '')
}

TABLES_CONFIG = {
    'players': {
        'entity': 'player',
        'contents': 'entity'
    },
    'teams': {
        'entity': 'team',
        'contents': 'entity'
    },
    'player_season_stats': {
        'entity': 'player',
        'contents': 'stats'
    },
    'team_season_stats': {
        'entity': 'team',
        'contents': 'stats'
    },
    'endpoint_tracker': {
        'entity': 'system',
        'contents': 'tracker'
    }
}

TABLES = list(TABLES_CONFIG.keys())

# ============================================================================
# NBA SEASON CONFIGURATION
# ============================================================================

SEASON_TYPE_CONFIG = {
    'Regular Season': {
        'season_code': 1,
        'minimum_season': None  # Available for all seasons
    },
    'Playoffs': {
        'season_code': 2,
        'minimum_season': None  # Available for all seasons
    },
    'PlayIn': {
        'season_code': 3,
        'minimum_season': '2020-21'  # PlayIn tournament started in 2020-21
    },
}

def _get_current_season_year() -> int:
    """Helper to calculate current season year (private - used only during module init)."""
    now = datetime.now()
    return now.year + 1 if now.month > 8 else now.year

def _get_current_season() -> str:
    """Helper to calculate current season string (private - used only during module init)."""
    year = _get_current_season_year()
    return f"{year - 1}-{str(year)[-2:]}"

NBA_CONFIG = {
    'current_season': _get_current_season(),
    'current_season_year': _get_current_season_year(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),
    'backfill_start_season': '2003-04',
    'combine_start_year': 2003,
}

# ============================================================================
# TEAM IDS (lazy-loaded from database)
# ============================================================================

_team_ids_cache: Optional[Dict[str, int]] = None

def _get_team_ids() -> Dict[str, int]:
    """Lazy-load team IDs from database. Cached after first call."""
    global _team_ids_cache
    
    if _team_ids_cache is None:
        from lib.etl import get_teams_from_db
        teams = get_teams_from_db(DB_CONFIG)
        _team_ids_cache = {abbr: tid for tid, (abbr, name) in teams.items()}
    
    return _team_ids_cache

# Property-like accessor for TEAM_IDS
class _TeamIDsProxy:
    """Lazy proxy for TEAM_IDS that loads from DB on first access."""
    def __getitem__(self, key: str) -> int:
        return _get_team_ids()[key]
    
    def __contains__(self, key: str) -> bool:
        return key in _get_team_ids()
    
    def keys(self):
        return _get_team_ids().keys()
    
    def values(self):
        return _get_team_ids().values()
    
    def items(self):
        return _get_team_ids().items()
    
    def get(self, key: str, default=None):
        return _get_team_ids().get(key, default)

TEAM_IDS = _TeamIDsProxy()

# ============================================================================
# DATABASE SCHEMA: COLUMNS AND METADATA
# ============================================================================

DB_COLUMNS = {
    'player_id': {
        'table': 'both',
        'type': 'INTEGER',
        'nullable': False,
        'update_frequency': None,
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'PLAYER_ID',
            'transform': 'safe_int'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'name': {
        'table': 'entity',
        'type': 'VARCHAR(50)',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'PLAYER_NAME',
            'transform': 'safe_str'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'year': {
        'table': 'stats',
        'type': 'VARCHAR(10)',
        'nullable': False,
        'update_frequency': 'daily',
        'api': False,
        'player_source': None,  # Computed field, not from API
        'team_source': None,  # Computed field, not from API
        'opponent_source': None
    },
    
    'season_type': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': False,
        'player_source': None,  # Computed field, not from API
        'team_source': None,  # Computed field, not from API
        'opponent_source': None
    },

    'height_inches': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'HEIGHT',
            'transform': 'parse_height'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'weight_lbs': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'WEIGHT',
            'transform': 'safe_int'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'wingspan_inches': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'draftcombineplayeranthro',
            'field': 'WINGSPAN',
            'transform': 'safe_float'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'birthdate': {
        'table': 'entity',
        'type': 'DATE',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'BIRTHDATE',
            'transform': 'parse_birthdate'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'rookie_year': {
        'table': 'entity',
        'type': 'VARCHAR(10)',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'FROM_YEAR',
            'transform': 'format_season',
            'notes': 'First season in NBA in ####-## format (e.g., 2012-13). Derived from FROM_YEAR API field.'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'jersey_number': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'JERSEY',
            'transform': 'safe_str'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'years_experience': {
        'table': 'entity',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'SEASON_EXP',
            'transform': 'safe_int'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'pre_nba_team': {
        'table': 'entity',
        'type': 'VARCHAR(100)',
        'nullable': True,
        'update_frequency': 'annual',
        'api': True,
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'SCHOOL',
            'transform': 'safe_str'
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'notes': {
        'table': 'entity',
        'type': 'TEXT',
        'nullable': True,
        'update_frequency': None,
        'api': False,
        'player_source': None,
        'team_source': None,
        'opponent_source': None
    },

    'games': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'GP',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'GP',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'minutes_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'MIN',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'MIN',
            'transform': 'safe_int',
            'scale': 10
        },
        'opponent_source': None
    },

    'tr_games': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'GP',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'GP',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },

    'tr_minutes_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'MIN',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'MIN',
            'transform': 'safe_int',
            'scale': 10
        },
        'opponent_source': None
    },

    '2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'FGM - FG3M',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'FGM - FG3M',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_FGM - OPP_FG3M',
            'transform': 'safe_int'
        }
    },
    
    '2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'FGA - FG3A',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'FGA - FG3A',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_FGA - OPP_FG3A',
            'transform': 'safe_int'
        }
    },
    
    '3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'FG3M',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'FG3M',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_FG3M',
            'transform': 'safe_int'
        }
    },
    
    '3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'FG3A',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'FG3A',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_FG3A',
            'transform': 'safe_int'
        }
    },
    
    'ftm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'FTM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'FTM',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_FTM',
            'transform': 'safe_int'
        }
    },
    
    'fta': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'FTA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'FTA',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_FTA',
            'transform': 'safe_int'
        }
    },
    
    'cont_close_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'team',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'cont_close_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'team',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'open_close_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'team',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'open_close_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'team',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'subtract',
                        'sources': [
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'}
                        ],
                        'formula': '(a + b) - (c + d)'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'cont_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'cont_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'open_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'open_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG2A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'cont_3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG3M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefender10ftPlusShooting',
                        'field': 'FG3M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'cont_3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG3A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefender10ftPlusShooting',
                        'field': 'FG3A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'open_3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG3M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefender10ftPlusShooting',
                        'field': 'FG3M',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'open_3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptshots',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptshots',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefenderShooting',
                        'field': 'FG3A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashptshots',
                'execution_tier': 'league',
                'endpoint_params': {},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ClosestDefender10ftPlusShooting',
                        'field': 'FG3A',
                        'filter_field': 'CLOSE_DEF_DIST_RANGE',
                        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'o_rebounds': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'OREB',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OREB',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_OREB',
            'transform': 'safe_int'
        }
    },
    
    'd_rebounds': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'DREB',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'DREB',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_DREB',
            'transform': 'safe_int'
        }
    },
    
    'o_rebound_pct_x1000': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'OREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'OREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        },
        'opponent_source': None
    },
    
    'd_rebound_pct_x1000': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'DREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'DREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        },
        'opponent_source': None
    },
    
    'cont_o_rebs': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptreb',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptreb',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'OverallRebounding',
                        'field': 'C_OREB'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptreb',
            'execution_tier': 'team',
            'result_set': 'OverallRebounding',
            'field': 'C_OREB',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'cont_d_rebs': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'playerdashptreb',
            'execution_tier': 'player',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashptreb',
                'execution_tier': 'player',
                'endpoint_params': {'team_id': 0},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'OverallRebounding',
                        'field': 'C_DREB'
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'teamdashptreb',
            'execution_tier': 'team',
            'result_set': 'OverallRebounding',
            'field': 'C_DREB',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'putbacks': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {
                    'measure_type_detailed': 'Base',
                    'per_mode_detailed': 'Totals'
                },
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ShotTypePlayerDashboard',
                        'field': 'FGM',
                        'filter_field': 'GROUP_VALUE',
                        'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashboardbyshootingsplits',
                'execution_tier': 'team',
                'endpoint_params': {
                    'measure_type_detailed_defense': 'Base',
                    'per_mode_detailed': 'Totals'
                },
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ShotTypeTeamDashboard',
                        'field': 'FGM',
                        'filter_field': 'GROUP_VALUE',
                        'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot']
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'dunks': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'endpoint_params': {
                    'measure_type_detailed': 'Base',
                    'per_mode_detailed': 'Totals'
                },
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ShotTypePlayerDashboard',
                        'field': 'FGM',
                        'filter_field': 'GROUP_VALUE',
                        'filter_values': [
                            'Alley Oop Dunk Shot',
                            'Cutting Dunk Shot',
                            'Driving Dunk Shot',
                            'Driving Reverse Dunk Shot',
                            'Dunk Shot',
                            'Putback Dunk Shot',
                            'Reverse Dunk Shot',
                            'Running Alley Oop Dunk Shot',
                            'Running Dunk Shot',
                            'Tip Dunk Shot'
                        ]
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'team_source': {
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'teamdashboardbyshootingsplits',
                'execution_tier': 'team',
                'endpoint_params': {
                    'measure_type_detailed_defense': 'Base',
                    'per_mode_detailed': 'Totals'
                },
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'ShotTypeTeamDashboard',
                        'field': 'FGM',
                        'filter_field': 'GROUP_VALUE',
                        'filter_values': [
                            'Alley Oop Dunk Shot',
                            'Cutting Dunk Shot',
                            'Driving Dunk Shot',
                            'Driving Reverse Dunk Shot',
                            'Dunk Shot',
                            'Putback Dunk Shot',
                            'Reverse Dunk Shot',
                            'Running Alley Oop Dunk Shot',
                            'Running Dunk Shot',
                            'Tip Dunk Shot'
                        ]
                    },
                    {
                        'type': 'aggregate',
                        'method': 'sum'
                    }
                ]
            }
        },
        'opponent_source': None
    },

    'possessions': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'POSS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'POSS',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'touches': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Possessions'},
            'field': 'TOUCHES',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
            'field': 'TOUCHES',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'time_on_ball': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Possessions'},
            'field': 'TIME_OF_POSS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
            'field': 'TIME_OF_POSS',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'dribbles': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'leaguedashptstats',
                'execution_tier': 'league',
                'endpoint_params': {'pt_measure_type': 'Possessions'},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'LeagueDashPtStats',
                        'fields': {
                            'touches': 'TOUCHES',
                            'avg_drib_per_touch': 'AVG_DRIB_PER_TOUCH'
                        }
                    },
                    {
                        'type': 'multiply',
                        'fields': ['touches', 'avg_drib_per_touch'],
                        'round': True
                    }
                ]
            }
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'execution_tier': 'league',
            'transformation': {
                'type': 'pipeline',
                'endpoint': 'leaguedashptstats',
                'execution_tier': 'league',
                'endpoint_params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
                'operations': [
                    {
                        'type': 'extract',
                        'result_set': 'LeagueDashPtStats',
                        'fields': {
                            'touches': 'TOUCHES',
                            'avg_drib_per_touch': 'AVG_DRIB_PER_TOUCH'
                        }
                    },
                    {
                        'type': 'multiply',
                        'fields': ['touches', 'avg_drib_per_touch'],
                        'round': True
                    }
                ]
            }
        },
        'opponent_source': None
    },
    
    'passes': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Passing'},
            'field': 'PASSES_MADE',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
            'field': 'PASSES_MADE',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'sec_assists': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Passing'},
            'field': 'SECONDARY_AST',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
            'field': 'SECONDARY_AST',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'o_dist_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'SpeedDistance'},
            'field': 'DIST_MILES_OFF',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
            'field': 'DIST_MILES_OFF',
            'transform': 'safe_int',
            'scale': 10
        },
        'opponent_source': None
    },
    
    'd_dist_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'SpeedDistance'},
            'field': 'DIST_MILES_DEF',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
            'field': 'DIST_MILES_DEF',
            'transform': 'safe_int',
            'scale': 10
        },
        'opponent_source': None
    },
    
    'assists': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'AST',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'AST',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_AST',
            'transform': 'safe_int'
        }
    },
    
    'pot_assists': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Passing'},
            'field': 'POTENTIAL_AST',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
            'field': 'POTENTIAL_AST',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'turnovers': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'TOV',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'TOV',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_TOV',
            'transform': 'safe_int'
        }
    },

    'steals': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'STL',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'STL',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_STL',
            'transform': 'safe_int'
        }
    },
    
    'blocks': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'BLK',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'BLK',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_BLK',
            'transform': 'safe_int'
        }
    },
    
    'fouls': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'PF',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'PF',
            'transform': 'safe_int'
        },
        'opponent_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'OPP_PF',
            'transform': 'safe_int'
        }
    },
    
    'deflections': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguehustlestatsplayer',
            'field': 'DEFLECTIONS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguehustlestatsteam',
            'field': 'DEFLECTIONS',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'charges_drawn': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguehustlestatsplayer',
            'field': 'CHARGES_DRAWN',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguehustlestatsteam',
            'field': 'CHARGES_DRAWN',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'contests': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguehustlestatsplayer',
            'field': 'CONTESTED_SHOTS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguehustlestatsteam',
            'field': 'CONTESTED_SHOTS',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'd_close_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': 'Less Than 10Ft'},
            'field': 'FGM_LT_10',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': 'Less Than 10Ft'},
            'field': 'FGM_LT_10',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'd_close_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': 'Less Than 10Ft'},
            'field': 'FGA_LT_10',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': 'Less Than 10Ft'},
            'field': 'FGA_LT_10',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'd_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': '2 Pointers'},
            'field': 'FG2M',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': '2 Pointers'},
            'field': 'FG2M',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'd_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': '2 Pointers'},
            'field': 'FG2A',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': '2 Pointers'},
            'field': 'FG2A',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'd_3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': '3 Pointers'},
            'field': 'FG3M',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': '3 Pointers'},
            'field': 'FG3M',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'd_3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': '3 Pointers'},
            'field': 'FG3A',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': '3 Pointers'},
            'field': 'FG3A',
            'transform': 'safe_int'
        },
        'opponent_source': None
    },
    
    'real_d_fg_pct_x1000': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'params': {'defense_category': 'Overall'},
            'field': 'PCT_PLUSMINUS',
            'transform': 'safe_int',
            'scale': 1000
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'params': {'defense_category': 'Overall'},
            'field': 'PCT_PLUSMINUS',
            'transform': 'safe_int',
            'scale': 1000
        },
        'opponent_source': None
    },
    
    'o_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'OFF_RATING',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'OFF_RATING',
            'transform': 'safe_int',
            'scale': 10
        },
        'opponent_source': None
    },
    
    'd_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'DEF_RATING',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'params': {'measure_type_detailed_defense': 'Advanced'},
            'field': 'DEF_RATING',
            'transform': 'safe_int',
            'scale': 10
        },
        'opponent_source': None
    },
}

# ============================================================================
# EXECUTION CONFIGURATION
# ============================================================================

PARALLEL_EXECUTION = {
    'league': {
        'max_workers': 10,
        'timeout': 30,
        'description': 'Single API call returns ALL entities'
    },
    'team': {
        'max_workers': 10,
        'timeout': 30,
        'description': 'One API call per team (30 total)'
    },
    'player': {
        'max_workers': 1,
        'description': 'One API call per player (536 total)'
    }
}

API_CONFIG = {
    'rate_limit_delay': 0.6,             # Light delay between API calls (session exhaustion occurs regardless)
    'per_player_rate_limit': 1.0,        # Increased delay for per-player endpoints to prevent throttling
    'season_delay': 0.0,                 # No delay needed for single-player sequential backfill
    'timeout_default': 20,
    'backoff_divisor': 5,               # Divisor for exponential backoff calculation
    'timeout_bulk': 120,
    'cooldown_after_batch_seconds': 30,  # Wait time after batch failures or before retries
    'max_consecutive_failures': 5,       # Max failures before taking a break
    
    # Batch processing configuration for roster updates
    'roster_batch_size': 175,            # Process 175 players at a time before cooldown
    'roster_batch_cooldown': 120,        # Wait 120 seconds between batches
    
    # Automatic restart configuration (handles session exhaustion at ~175 requests)
    'api_failure_threshold': 1,         # Auto-restart subprocess after first failure to preserve data
    'api_restart_enabled': True,        # Enable automatic restart via exit code 42
    
    # Standard NBA API parameters (single source of truth)
    'league_id': '00',  # NBA league
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

# ============================================================================
# ENDPOINTS CONFIGURATION
# ============================================================================
# Centralized configuration for all NBA API endpoints
# Defines availability, execution strategy, and endpoint-specific metadata

ENDPOINTS_CONFIG = {
    # Basic Stats Endpoints (available since 2003-04)
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
    
    # Player Tracking Stats (available since 2013-14)
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
    
    # Hustle Stats (available since 2015-16, but limited data until 2016-17)
    'leaguehustlestatsplayer': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsPlayer',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['player'],
        'tracking': True,
    },
    'leaguehustlestatsteam': {
        'min_season': '2015-16',
        'execution_tier': 'league',
        'default_result_set': 'HustleStatsTeam',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_time',
        'entity_types': ['team'],
        'tracking': True,
    },
    
    # Defensive Matchup Data (available since 2013-14)
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
    
    # Shot Tracking Data (available since 2013-14)
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
    
    # Rebounding Tracking (available since 2013-14)
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
        'min_season': '2013-14',
        'execution_tier': 'league',
        'default_result_set': 'ShotTypeTeamDashboard',
        'season_type_param': 'season_type_all_star',
        'per_mode_param': 'per_mode_detailed',
        'entity_types': ['team'],
        'tracking': False,
    },
    
    # Player Info (available all time, not season-specific)
    'commonplayerinfo': {
        'min_season': None,  # Available for all players regardless of season
        'execution_tier': 'player',
        'default_result_set': 'CommonPlayerInfo',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
        'tracking': False,
    },
    
    # Draft Combine Data (available since 2000-01, collected at draft time)
    'draftcombineplayeranthro': {
        'min_season': '2000-01',
        'execution_tier': 'league',
        'default_result_set': 'DraftCombinePlayerAnthro',
        'season_type_param': None,
        'per_mode_param': None,
        'entity_types': ['player'],
        'tracking': False,
    },
}

# ============================================================================
# DATA INTEGRITY RULES (Config-Driven Validation)
# ============================================================================
# Rules derived from subcategories.txt - defines validation logic for backfill
# Ensures data completeness and consistency across all stats

DATA_INTEGRITY_RULES = {
    # Dependency Rules: If value_x has a value, then value_y should have a non-zero and non-null value
    # Format: 'value_x': ['value_y1', 'value_y2', ...]
    # Minimum thresholds prevent false positives on small sample sizes
    'dependencies': {
        'o_rebounds': ['o_rebound_pct_x1000', 'cont_o_rebs'],
        'd_rebounds': ['d_rebound_pct_x1000', 'cont_d_rebs'],
        'minutes_x10': ['o_rating_x10'],
        'assists': ['pot_assists', 'passes', 'touches'],
        '2fgm': ['putbacks', 'dunks'],
    },
    
    # Minimum thresholds for dependency validation (skip validation if parent value below threshold)
    'minimum_thresholds': {
        'o_rebounds': 10,      # Need at least 10 offensive rebounds
        'd_rebounds': 10,      # Need at least 10 defensive rebounds
        'minutes_x10': 100,    # Need at least 100 (10 actual minutes) for ratings
        'assists': 10,         # Need at least 10 assists
        '2fgm': 10,           # Need at least 10 2-point FGM
        '2fga': 10,           # Need at least 10 2-point FGA
        '3fgm': 10,           # Need at least 10 3-point FGM
        '3fga': 10,           # Need at least 10 3-point FGA
    },
    
    # Sum Validation: If value_z has a value, then values_a should add up to a non-zero and non-null value
    # Format: 'value_z': {'components': ['comp1', 'comp2'], 'special_case_components': ['special1', 'special2']}
    'sum_validations': {
        '2fgm': {
            'components': ['open_2fgm', 'cont_2fgm'],
            'special_case_components': ['open_close_2fgm', 'cont_close_2fgm']
        },
        '2fga': {
            'components': ['open_2fga', 'cont_2fga'],
            'special_case_components': ['open_close_2fga', 'cont_close_2fga']
        },
        '3fgm': {
            'components': ['open_3fgm', 'cont_3fgm']
        },
        '3fga': {
            'components': ['open_3fga', 'cont_3fga']
        },
    }
}

# ============================================================================
# NBA API FIELD NAMES
# ============================================================================

API_FIELD_NAMES = {
    # Entity ID fields (primary identifiers)
    'entity_id': {
        'player': 'PLAYER_ID',
        'team': 'TEAM_ID'
    },
    # Entity name fields
    'entity_name': {
        'player': 'PLAYER_NAME',
        'team': 'TEAM_NAME'
    },
    # Special ID fields used in specific endpoints
    'special_ids': {
        'person': 'PERSON_ID'          # Used in commonplayerinfo
    }
}

# ============================================================================
# RETRY & DATABASE OPERATIONS CONFIGURATION
# ============================================================================

RETRY_CONFIG = {
    'max_retries': 3,
    'backoff_base': 20,
}

DB_OPERATIONS = {
    'bulk_insert_batch_size': 1000,
    'statement_timeout_ms': 120000,  # 2 minutes
}
