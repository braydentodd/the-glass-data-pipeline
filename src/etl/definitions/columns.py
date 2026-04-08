"""
The Glass - Column Registry

Single source of truth for database column definitions and provider source
mappings.  Column names match the actual PostgreSQL schema exactly.

Each column entry carries a 'sources' attribute that maps provider keys
(e.g., 'nba') to per-entity fetch definitions.  Columns with no external
source (system columns) have sources: None.
"""

from typing import Any, Dict



DB_COLUMNS: Dict[str, Dict[str, Any]] = {

    # ------------------------------------------------------------------
    # SYSTEM COLUMNS  (managed by DB / ETL engine, no provider sources)
    # ------------------------------------------------------------------
    'id': {
        'type': 'SERIAL',
        'scope': ['entity', 'stats'],
        'nullable': False,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'primary_key': True,
        'sources': None,
    },
    'nba_api_id': {
        'type': 'VARCHAR(10)',
        'scope': ['entity'],
        'nullable': False,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'PLAYER_ID',
                    'transform': 'safe_str',
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'TEAM_ID',
                    'transform': 'safe_str',
                },
            },
        },
    },
    'entity_id': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': 'FK to entity table serial id',
        'sources': None,
    },
    'updated_at': {
        'type': 'TIMESTAMP',
        'scope': ['entity', 'stats'],
        'nullable': True,
        'default': 'CURRENT_TIMESTAMP',
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    'created_at': {
        'type': 'TIMESTAMP',
        'scope': ['entity'],
        'nullable': True,
        'default': 'CURRENT_TIMESTAMP',
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    'season': {
        'type': 'VARCHAR(7)',
        'scope': ['stats'],
        'nullable': False,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    'season_type': {
        'type': 'VARCHAR(3)',
        'scope': ['stats'],
        'nullable': False,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    'backfilled': {
        'type': 'BOOLEAN',
        'scope': ['entity'],
        'nullable': False,
        'default': 'FALSE',
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    'notes': {
        'type': 'TEXT',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    # ------------------------------------------------------------------
    # ENTITY INFORMATION  (player / team profile data)
    # ------------------------------------------------------------------
    'team_id': {
        'type': 'INTEGER',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'commonplayerinfo', 'field': 'TEAM_ID'},
            },
        },
    },
    'name': {
        'type': 'VARCHAR(100)',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'PLAYER_NAME',
                    'transform': 'safe_str',
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'TEAM_NAME',
                    'transform': 'safe_str',
                },
            },
        },
    },
    'height_ins': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'commonplayerinfo',
                    'field': 'HEIGHT',
                    'transform': 'parse_height',
                },
            },
        },
    },
    'weight_lbs': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'commonplayerinfo', 'field': 'WEIGHT'},
            },
        },
    },
    'wingspan_ins': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'draftcombineplayeranthro',
                    'field': 'WINGSPAN',
                    'transform': 'parse_height',
                },
            },
        },
    },
    'jersey_num': {
        'type': 'VARCHAR(3)',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'commonplayerinfo', 'field': 'JERSEY'},
            },
        },
    },
    'birthdate': {
        'type': 'DATE',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'commonplayerinfo',
                    'field': 'BIRTHDATE',
                    'transform': 'parse_birthdate',
                },
            },
        },
    },
    'hand': {
        'type': 'CHAR',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': None,
        'rate_group': None,
        'comment': None,
        'sources': None,
    },
    'seasons_exp': {
        'type': 'SMALLINT',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'commonplayerinfo', 'field': 'SEASON_EXP'},
            },
        },
    },
    'rookie_season': {
        'type': 'VARCHAR(10)',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'commonplayerinfo',
                    'field': 'FROM_YEAR',
                    'transform': 'format_season',
                },
            },
        },
    },
    'abbr': {
        'type': 'VARCHAR(5)',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['team'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'TEAM_ABBREVIATION'},
            },
        },
    },
    'conf': {
        'type': 'VARCHAR(50)',
        'scope': ['entity'],
        'nullable': True,
        'default': None,
        'entity_types': ['team'],
        'update_frequency': 'annual',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'CONFERENCE'},
            },
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
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'GP'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'GP'},
            },
        },
    },
    'minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'MIN', 'scale': 10},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'MIN', 'scale': 10},
            },
        },
    },
    'wins': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'W'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'W'},
            },
        },
    },
    'tr_games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'GP',
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'GP',
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'tr_minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'MIN',
                    'scale': 10,
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'MIN',
                    'scale': 10,
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'h_games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'hustle',
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'GP'},
                'team': {'endpoint': 'leaguehustlestatsteam', 'field': 'GP'},
            },
        },
    },
    'h_minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'hustle',
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'MIN', 'scale': 10},
                'team': {'endpoint': 'leaguehustlestatsteam', 'field': 'MIN', 'scale': 10},
            },
        },
    },
    'off_games': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': 'onoff',
        'comment': None,
        'sources': {
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
    },
    'off_minutes_x10': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': False,
        'default': '0',
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'onoff',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'teamplayeronoffsummary',
                    'tier': 'team_call',
                    'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                    'player_id_field': 'VS_PLAYER_ID',
                    'field': 'MIN',
                    'scale': 10,
                    'aggregation': 'sum',
                },
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
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'FGM',
                    'derived': {'subtract': 'FG3M'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'FGM',
                    'derived': {'subtract': 'FG3M'},
                },
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_FGM',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                    'derived': {'subtract': 'OPP_FG3M'},
                },
            },
        },
    },
    'fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'FGA',
                    'derived': {'subtract': 'FG3A'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'FGA',
                    'derived': {'subtract': 'FG3A'},
                },
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_FGA',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                    'derived': {'subtract': 'OPP_FG3A'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # SCORING: 3-POINT
    # ------------------------------------------------------------------
    'fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FG3M'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'FG3M'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_FG3M',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FG3A'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'FG3A'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_FG3A',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # SCORING: FREE THROWS
    # ------------------------------------------------------------------
    'ftm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FTM'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'FTM'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_FTM',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'fta': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'FTA'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'FTA'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_FTA',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # SHOT TRACKING: CONTESTED / OPEN  x  RIM / ALL
    # ------------------------------------------------------------------
    'cont_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FGM',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '0-2 Feet - Very Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '2-4 Feet - Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FGM',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '0-2 Feet - Very Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '2-4 Feet - Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'cont_rim_fga': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FGA',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '0-2 Feet - Very Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '2-4 Feet - Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FGA',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '0-2 Feet - Very Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '2-4 Feet - Tight',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'open_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FGM',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '4-6 Feet - Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '6+ Feet - Wide Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FGM',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '4-6 Feet - Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '6+ Feet - Wide Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'open_rim_fga': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FGA',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '4-6 Feet - Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '6+ Feet - Wide Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FGA',
                    'multi_call': [
                        {
                            'close_def_dist_range_nullable': '4-6 Feet - Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                        {
                            'close_def_dist_range_nullable': '6+ Feet - Wide Open',
                            'general_range_nullable': 'Less Than 10 ft',
                        },
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'cont_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG2M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG2M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'cont_fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG2A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG2A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'open_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG2M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG2M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'open_fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG2A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG2A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'cont_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG3M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG3M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'cont_fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG3A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG3A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '0-2 Feet - Very Tight'},
                        {'close_def_dist_range_nullable': '2-4 Feet - Tight'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'open_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG3M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG3M',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    'open_fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerptshot',
                    'field': 'FG3A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
                'team': {
                    'endpoint': 'leaguedashteamptshot',
                    'field': 'FG3A',
                    'multi_call': [
                        {'close_def_dist_range_nullable': '4-6 Feet - Open'},
                        {'close_def_dist_range_nullable': '6+ Feet - Wide Open'},
                    ],
                    'result_set': 'LeagueDashPTShots',
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # PUTBACKS & DUNKS  (pipeline: filter shooting splits -> aggregate)
    # ------------------------------------------------------------------
    'putbacks': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashboardbyshootingsplits',
                        'tier': 'player',
                        'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'ShotTypePlayerDashboard',
                                'field': 'FGM',
                                'filter_field': 'GROUP_VALUE',
                                'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot'],
                            },
                            {'type': 'aggregate', 'method': 'sum'},
                        ],
                    },
                },
                'team': {
                    'pipeline': {
                        'endpoint': 'teamdashboardbyshootingsplits',
                        'tier': 'team',
                        'params': {
                            'measure_type_detailed_defense': 'Base',
                            'per_mode_detailed': 'Totals',
                        },
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'ShotTypeTeamDashboard',
                                'field': 'FGM',
                                'filter_field': 'GROUP_VALUE',
                                'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot'],
                            },
                            {'type': 'aggregate', 'method': 'sum'},
                        ],
                    },
                },
            },
        },
    },
    'dunks': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashboardbyshootingsplits',
                        'tier': 'player',
                        'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
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
                                    'Tip Dunk Shot',
                                ],
                            },
                            {'type': 'aggregate', 'method': 'sum'},
                        ],
                    },
                },
                'team': {
                    'pipeline': {
                        'endpoint': 'teamdashboardbyshootingsplits',
                        'tier': 'team',
                        'params': {
                            'measure_type_detailed_defense': 'Base',
                            'per_mode_detailed': 'Totals',
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
                                    'Tip Dunk Shot',
                                ],
                            },
                            {'type': 'aggregate', 'method': 'sum'},
                        ],
                    },
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
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashboardbyshootingsplits',
                        'tier': 'player',
                        'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'AssistTracking',
                                'field': 'FGM',
                                'filter_field': 'SHOT_TYPE',
                                'filter_values': ['AtRim'],
                            },
                        ],
                    },
                },
            },
        },
    },
    'unassisted_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashboardbyshootingsplits',
                        'tier': 'player',
                        'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'AssistTracking',
                                'field': 'FGM',
                                'filter_field': 'SHOT_TYPE',
                                'filter_values': ['2PT'],
                            },
                        ],
                    },
                },
            },
        },
    },
    'unassisted_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashboardbyshootingsplits',
                        'tier': 'player',
                        'params': {'measure_type_detailed': 'Base', 'per_mode_detailed': 'Totals'},
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'AssistTracking',
                                'field': 'FGM',
                                'filter_field': 'SHOT_TYPE',
                                'filter_values': ['3PT'],
                            },
                        ],
                    },
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
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'OREB'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'OREB'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_OREB',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'd_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'DREB'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'DREB'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_DREB',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'o_reb_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'OREB_PCT',
                    'scale': 1000,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OREB_PCT',
                    'scale': 1000,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
            },
        },
    },
    'd_reb_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'DREB_PCT',
                    'scale': 1000,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'DREB_PCT',
                    'scale': 1000,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
            },
        },
    },
    'cont_o_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashptreb',
                        'tier': 'player',
                        'params': {'team_id': 0},
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'OverallRebounding',
                                'field': 'C_OREB',
                            },
                        ],
                    },
                },
                'team': {
                    'pipeline': {
                        'endpoint': 'teamdashptreb',
                        'tier': 'team',
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'OverallRebounding',
                                'field': 'C_OREB',
                            },
                        ],
                    },
                },
            },
        },
    },
    'cont_d_rebs': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'pipeline': {
                        'endpoint': 'playerdashptreb',
                        'tier': 'player',
                        'params': {'team_id': 0},
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'OverallRebounding',
                                'field': 'C_DREB',
                            },
                        ],
                    },
                },
                'team': {
                    'pipeline': {
                        'endpoint': 'teamdashptreb',
                        'tier': 'team',
                        'operations': [
                            {
                                'type': 'extract',
                                'result_set': 'OverallRebounding',
                                'field': 'C_DREB',
                            },
                        ],
                    },
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
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'AST'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'AST'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_AST',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'pot_assists': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'POTENTIAL_AST',
                    'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'POTENTIAL_AST',
                    'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'passes': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'PASSES_MADE',
                    'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'PASSES_MADE',
                    'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'sec_assists': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'SECONDARY_AST',
                    'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'SECONDARY_AST',
                    'params': {'pt_measure_type': 'Passing', 'player_or_team': 'Team'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # BALL HANDLING
    # ------------------------------------------------------------------
    'touches': {
        'type': 'INTEGER',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'TOUCHES',
                    'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'TOUCHES',
                    'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'time_on_ball': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'TIME_OF_POSS',
                    'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'TIME_OF_POSS',
                    'params': {'pt_measure_type': 'Possessions', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'possessions': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'POSS',
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'POSS',
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # TURNOVERS
    # ------------------------------------------------------------------
    'turnovers': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'TOV'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'TOV'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_TOV',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # DISTANCE
    # ------------------------------------------------------------------
    'o_dist_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'DIST_MILES_OFF',
                    'scale': 10,
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'DIST_MILES_OFF',
                    'scale': 10,
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
                },
            },
        },
    },
    'd_dist_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'DIST_MILES_DEF',
                    'scale': 10,
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Player'},
                },
                'team': {
                    'endpoint': 'leaguedashptstats',
                    'field': 'DIST_MILES_DEF',
                    'scale': 10,
                    'params': {'pt_measure_type': 'SpeedDistance', 'player_or_team': 'Team'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # DEFENSE: STEALS / BLOCKS / FOULS
    # ------------------------------------------------------------------
    'steals': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'STL'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'STL'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_STL',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'blocks': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'BLK'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'BLK'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_BLK',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    'fouls': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team', 'opponent'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguedashplayerstats', 'field': 'PF'},
                'team': {'endpoint': 'leaguedashteamstats', 'field': 'PF'},
                'opponent': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OPP_PF',
                    'params': {'measure_type_detailed_defense': 'Opponent'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # HUSTLE STATS
    # ------------------------------------------------------------------
    'deflections': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'hustle',
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'DEFLECTIONS'},
                'team': {'endpoint': 'leaguehustlestatsteam', 'field': 'DEFLECTIONS'},
            },
        },
    },
    'charges_drawn': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'hustle',
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'CHARGES_DRAWN'},
                'team': {'endpoint': 'leaguehustlestatsteam', 'field': 'CHARGES_DRAWN'},
            },
        },
    },
    'contests': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'hustle',
        'comment': None,
        'sources': {
            'nba': {
                'player': {'endpoint': 'leaguehustlestatsplayer', 'field': 'CONTESTED_SHOTS'},
                'team': {'endpoint': 'leaguehustlestatsteam', 'field': 'CONTESTED_SHOTS'},
            },
        },
    },
    # ------------------------------------------------------------------
    # DEFENSIVE SHOT TRACKING  (leaguedashptdefend / leaguedashptteamdefend)
    # ------------------------------------------------------------------
    'd_rim_fgm': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'FGM',
                    'params': {'defense_category': 'Less Than 10Ft'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'FGM',
                    'params': {'defense_category': 'Less Than 10Ft'},
                },
            },
        },
    },
    'd_rim_fga': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'FGA_LT_10',
                    'params': {'defense_category': 'Less Than 10Ft'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'FGA_LT_10',
                    'params': {'defense_category': 'Less Than 10Ft'},
                },
            },
        },
    },
    'd_fg2m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'FG2M',
                    'params': {'defense_category': '2 Pointers'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'FG2M',
                    'params': {'defense_category': '2 Pointers'},
                },
            },
        },
    },
    'd_fg2a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'FG2A',
                    'params': {'defense_category': '2 Pointers'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'FG2A',
                    'params': {'defense_category': '2 Pointers'},
                },
            },
        },
    },
    'd_fg3m': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'FG3M',
                    'params': {'defense_category': '3 Pointers'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'FG3M',
                    'params': {'defense_category': '3 Pointers'},
                },
            },
        },
    },
    'd_fg3a': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'FG3A',
                    'params': {'defense_category': '3 Pointers'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'FG3A',
                    'params': {'defense_category': '3 Pointers'},
                },
            },
        },
    },
    # ------------------------------------------------------------------
    # RATINGS
    # ------------------------------------------------------------------
    'o_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'OFF_RATING',
                    'scale': 10,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'OFF_RATING',
                    'scale': 10,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
            },
        },
    },
    'd_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': None,
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashplayerstats',
                    'field': 'DEF_RATING',
                    'scale': 10,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
                'team': {
                    'endpoint': 'leaguedashteamstats',
                    'field': 'DEF_RATING',
                    'scale': 10,
                    'params': {'measure_type_detailed_defense': 'Advanced'},
                },
            },
        },
    },
    'off_o_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': 'onoff',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'teamplayeronoffsummary',
                    'tier': 'team_call',
                    'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                    'player_id_field': 'VS_PLAYER_ID',
                    'field': 'OFF_RATING',
                    'scale': 10,
                    'aggregation': 'minute_weighted',
                },
            },
        },
    },
    'off_d_rtg_x10': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player'],
        'update_frequency': 'daily',
        'rate_group': 'onoff',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'teamplayeronoffsummary',
                    'tier': 'team_call',
                    'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                    'player_id_field': 'VS_PLAYER_ID',
                    'field': 'DEF_RATING',
                    'scale': 10,
                    'aggregation': 'minute_weighted',
                },
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
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'PCT_PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': 'Overall'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'PCT_PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': 'Overall'},
                },
            },
        },
    },
    'real_d_rim_fg_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': 'Less Than 10Ft'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': 'Less Than 10Ft'},
                },
            },
        },
    },
    'real_d_fg2_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': '2 Pointers'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': '2 Pointers'},
                },
            },
        },
    },
    'real_d_fg3_pct_x1000': {
        'type': 'SMALLINT',
        'scope': ['stats'],
        'nullable': True,
        'default': None,
        'entity_types': ['player', 'team'],
        'update_frequency': 'daily',
        'rate_group': 'tracking',
        'comment': None,
        'sources': {
            'nba': {
                'player': {
                    'endpoint': 'leaguedashptdefend',
                    'field': 'PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': '3 Pointers'},
                },
                'team': {
                    'endpoint': 'leaguedashptteamdefend',
                    'field': 'PLUSMINUS',
                    'scale': 1000,
                    'params': {'defense_category': '3 Pointers'},
                },
            },
        },
    },
}

