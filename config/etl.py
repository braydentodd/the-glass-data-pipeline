import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', ''),
    'port': int(os.getenv('DB_PORT', '')),
    'database': os.getenv('DB_NAME', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', '')
}

TABLES = ['teams', 'players', 'player_season_stats', 'team_season_stats']

def infer_execution_tier_from_endpoint(endpoint_name):
    endpoint_lower = endpoint_name.lower()
    
    # Check player before team to avoid misclassifying teamplayer* endpoints
    if endpoint_lower.startswith('playerdash') or endpoint_lower.startswith('commonplayer'):
        return 'player'
    
    if endpoint_lower.startswith('team'):
        return 'team'
    
    if endpoint_lower.startswith('league'):
        return 'league'
    
    return 'league'

def get_current_season_year():
    now = datetime.now()
    return now.year + 1 if now.month > 8 else now.year

def get_current_season():
    year = get_current_season_year()
    return f"{year - 1}-{str(year)[-2:]}"

SEASON_TYPE_MAP = {
    'Regular Season': 1,
    'Playoffs': 2,
    'PlayIn': 3,
}

NBA_CONFIG = {
    'current_season': get_current_season(),
    'current_season_year': get_current_season_year(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),
    'backfill_start_season': '2003-04',
    'combine_start_year': 2003,
}

TEST_MODE_CONFIG = {
    'player_id': 1631170,
    'player_name': 'Jaime Jaquez Jr.',
    'team_id': 1610612748,
    'team_name': 'Miami Heat',
    'season': '2024-25',
}

def get_teams_from_db():
    import psycopg2
    
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        port=DB_CONFIG['port']
    )
    cursor = conn.cursor()
    cursor.execute("SELECT team_id, team_abbr, team_name FROM teams ORDER BY team_id")
    teams = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return teams

_teams_cache = None

def get_team_ids():
    global _teams_cache
    if _teams_cache is None:
        _teams_cache = get_teams_from_db()
    return {abbr: tid for tid, (abbr, name) in _teams_cache.items()}

TEAM_IDS = get_team_ids()

def get_editable_fields():
    editable = []
    
    for col_name, col_meta in DB_COLUMNS.items():
        if col_meta.get('table') not in ['entity', 'both']:
            continue
            
        if not col_meta.get('nullable', False):
            continue
        
        is_non_api = not col_meta.get('api', False)
        is_annual = col_meta.get('update_frequency') == 'annual'
        
        if is_non_api or is_annual:
            editable.append(col_name)
    
    return editable


def generate_schema_ddl():
    """
    Generate complete database schema DDL from DB_COLUMNS.
    Builds all CREATE TABLE statements with proper columns, constraints, and indexes.
    """
    # Define table metadata (primary keys, foreign keys, indexes)
    table_metadata = {
        'teams': {
            'primary_key': 'team_id',
            'additional_columns': [
                'team_id INTEGER PRIMARY KEY',
                'abbreviation VARCHAR(3) UNIQUE NOT NULL',
                'full_name VARCHAR(100) NOT NULL',
                'created_at TIMESTAMP DEFAULT NOW()',
                'updated_at TIMESTAMP DEFAULT NOW()'
            ]
        },
        'players': {
            'primary_key': 'player_id',
            'foreign_keys': [
                "FOREIGN KEY (team_id) REFERENCES teams(team_id)"
            ],
            'additional_columns': [
                'player_id INTEGER PRIMARY KEY',
                'team_id INTEGER',  # Current team assignment (nullable - free agents)
                'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            ]
        },
        'player_season_stats': {
            'primary_key': 'id',
            'foreign_keys': [
                "FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE"
            ],
            'unique_constraints': [
                'UNIQUE(player_id, year, season_type)'
            ],
            'additional_columns': [
                'id SERIAL PRIMARY KEY',
                'player_id INTEGER NOT NULL',
                'year INTEGER NOT NULL',
                'season_type INTEGER NOT NULL DEFAULT 1',
                'created_at TIMESTAMP DEFAULT NOW()',
                'updated_at TIMESTAMP DEFAULT NOW()'
            ],
            'indexes': [
                "CREATE INDEX IF NOT EXISTS idx_player_stats_year ON player_season_stats(year)",
                "CREATE INDEX IF NOT EXISTS idx_player_stats_player ON player_season_stats(player_id)"
            ]
        },
        'team_season_stats': {
            'primary_key': 'id',
            'foreign_keys': [
                "FOREIGN KEY (team_id) REFERENCES teams(team_id)"
            ],
            'unique_constraints': [
                'UNIQUE(team_id, year, season_type)'
            ],
            'additional_columns': [
                'id SERIAL PRIMARY KEY',
                'team_id INTEGER NOT NULL',
                'year INTEGER NOT NULL',
                'season_type INTEGER NOT NULL DEFAULT 1',
                'created_at TIMESTAMP DEFAULT NOW()',
                'updated_at TIMESTAMP DEFAULT NOW()'
            ],
            'indexes': [
                "CREATE INDEX IF NOT EXISTS idx_team_stats_year ON team_season_stats(year)",
                "CREATE INDEX IF NOT EXISTS idx_team_stats_team ON team_season_stats(team_id)"
            ]
        }
    }
    
    # Group columns by table based on current DB_COLUMNS structure
    # table: 'both' -> both entity tables | 'entity' -> entity tables | 'stats' -> stats tables
    tables = {}
    
    for col_name, col_config in DB_COLUMNS.items():
        table_category = col_config.get('table')
        
        # Determine which tables this column belongs to based on its sources
        table_names = []
        
        if table_category == 'both':
            # Column appears in both entity tables (e.g., player_id in both players and player_season_stats)
            if col_config.get('player_source') is not None:
                table_names.extend(['players', 'player_season_stats'])
            if col_config.get('team_source') is not None:
                table_names.extend(['teams', 'team_season_stats'])
                
        elif table_category == 'entity':
            # Column only in entity tables (e.g., name, height, wingspan)
            if col_config.get('player_source') is not None:
                table_names.append('players')
            if col_config.get('team_source') is not None:
                table_names.append('teams')
                
        elif table_category == 'stats':
            # Column only in stats tables (e.g., year, season_type, stats fields)
            if col_config.get('player_source') is not None:
                table_names.append('player_season_stats')
            if col_config.get('team_source') is not None:
                table_names.append('team_season_stats')
        
        # Add column to each applicable table
        for table_name in table_names:
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append((col_name, col_config))
    
    # Generate CREATE TABLE statements
    ddl_statements = []
    
    # Use TABLES list for proper order
    for table_name in TABLES:
        if table_name not in tables:
            continue
            
        metadata = table_metadata.get(table_name, {})
        columns = tables[table_name]
        
        # Start table DDL
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        col_defs = []
        
        # Add structural columns (id, player_id, etc.) from metadata
        col_defs.extend(metadata.get('additional_columns', []))
        
        # Add data columns from DB_COLUMNS
        for col_name, col_config in sorted(columns, key=lambda x: x[0]):
            # Skip columns that are in additional_columns
            if any(col_name in col for col in metadata.get('additional_columns', [])):
                continue
                
            col_type = col_config['type']
            nullable = '' if col_config.get('nullable', True) else ' NOT NULL'
            default = ''
            if 'default' in col_config:
                default_val = col_config['default']
                if isinstance(default_val, str):
                    default = f" DEFAULT '{default_val}'"
                else:
                    default = f" DEFAULT {default_val}"
            col_defs.append(f"    {col_name} {col_type}{nullable}{default}")
        
        # Add constraints
        for fk in metadata.get('foreign_keys', []):
            col_defs.append(f"    {fk}")
        for uc in metadata.get('unique_constraints', []):
            col_defs.append(f"    {uc}")
        
        ddl += ',\n'.join(col_defs)
        ddl += "\n);"
        ddl_statements.append(ddl)
    
    # Add indexes
    for table_name, metadata in table_metadata.items():
        for index in metadata.get('indexes', []):
            ddl_statements.append(index + ';')
    
    return '\n\n'.join(ddl_statements)

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
        'player_source': 'NBA_CONFIG["current_season"]',
        'team_source': 'NBA_CONFIG["current_season"]',
        'opponent_source': None
    },
    
    'season_type': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': False,
        'player_source': 'NBA_CONFIG["season_type"]',
        'team_source': 'NBA_CONFIG["season_type"]',
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

    'games_played': {
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
                'type': 'arithmetic_subtract',
                'group': 'playerdashptshots_player',
                'endpoint_params': {'team_id': 0},
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'}
                ],
                'formula': '(a + b) - (c + d)'
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'arithmetic_subtract',
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'}
                ],
                'formula': '(a + b) - (c + d)',
                'endpoint_params': {}
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
                'type': 'arithmetic_subtract',
                'group': 'playerdashptshots_player',
                'endpoint_params': {'team_id': 0},
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'}
                ],
                'formula': '(a + b) - (c + d)'
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'arithmetic_subtract',
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'}
                ],
                'formula': '(a + b) - (c + d)',
                'endpoint_params': {}
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
                'type': 'arithmetic_subtract',
                'group': 'playerdashptshots_player',
                'endpoint_params': {'team_id': 0},
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'}
                ],
                'formula': '(a + b) - (c + d)'
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'arithmetic_subtract',
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'}
                ],
                'formula': '(a + b) - (c + d)',
                'endpoint_params': {}
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
                'type': 'arithmetic_subtract',
                'group': 'playerdashptshots_player',
                'endpoint_params': {'team_id': 0},
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'}
                ],
                'formula': '(a + b) - (c + d)'
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'transformation': {
                'type': 'arithmetic_subtract',
                'subtract': [
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
                    {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'}
                ],
                'formula': '(a + b) - (c + d)',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG2M',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG2M',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG2A',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG2A',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG2M',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG2M',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG2A',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG2A',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG3M',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefender10ftPlusShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG3M',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG3A',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefender10ftPlusShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
                'aggregate': 'sum',
                'field': 'FG3A',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG3M',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefender10ftPlusShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG3M',
                'endpoint_params': {}
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
                'type': 'filter_aggregate',
                'group': 'playerdashptshots_player',
                'result_set': 'ClosestDefenderShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG3A',
                'endpoint_params': {'team_id': 0}
            }
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'league',
            'transformation': {
                'type': 'filter_aggregate',
                'result_set': 'ClosestDefender10ftPlusShooting',
                'filter_field': 'CLOSE_DEF_DIST_RANGE',
                'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
                'aggregate': 'sum',
                'field': 'FG3A',
                'endpoint_params': {}
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
                'type': 'simple_extract',
                'group': 'playerdashptreb_player',
                'result_set': 'OverallRebounding',
                'field': 'C_OREB',
                'endpoint_params': {'team_id': 0}
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
                'type': 'simple_extract',
                'group': 'playerdashptreb_player',
                'result_set': 'OverallRebounding',
                'field': 'C_DREB',
                'endpoint_params': {'team_id': 0}
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
                'type': 'filter_aggregate',
                'endpoint': 'playerdashboardbyshootingsplits',
                'execution_tier': 'player',
                'group': 'playerdashboardbyshootingsplits_player',
                'season_type_param': 'season_type_playoffs',
                'result_set': 'ShotTypePlayerDashboard',
                'filter_field': 'GROUP_VALUE',
                'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot'],
                'aggregate': 'sum',
                'field': 'FGM',
                'endpoint_params': {
                    'measure_type_detailed': 'Base',
                    'per_mode_detailed': 'Totals'
                }
            }
        },
        'team_source': {
            'transformation': {
                'type': 'filter_aggregate',
                'endpoint': 'teamdashboardbyshootingsplits',
                'execution_tier': 'league',
                'season_type_param': 'season_type_all_star',
                'result_set': 'ShotTypeTeamDashboard',
                'filter_field': 'GROUP_VALUE',
                'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot'],
                'aggregate': 'sum',
                'field': 'FGM',
                'endpoint_params': {
                    'measure_type_detailed_defense': 'Base',
                    'per_mode_detailed': 'Totals'
                }
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

    'tm_off_o_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'teamplayeronoffsummary',
            'execution_tier': 'team',
            'entity': 'player',
            'transformation': {
                'type': 'simple_extract',
                'group': 'teamplayeronoffsummary_team',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                'field': 'OFF_RATING',
                'player_id_field': 'VS_PLAYER_ID',
                'transform': 'safe_float',
                'scale': 10,
                'endpoint_params': {}
            }
        },
        'team_source': None,
        'opponent_source': None
    },
    
    'tm_off_d_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'api': True,
        'player_source': {
            'endpoint': 'teamplayeronoffsummary',
            'execution_tier': 'team',
            'entity': 'player',
            'transformation': {
                'type': 'simple_extract',
                'group': 'teamplayeronoffsummary_team',
                'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
                'field': 'DEF_RATING',
                'player_id_field': 'VS_PLAYER_ID',
                'transform': 'safe_float',
                'scale': 10,
                'endpoint_params': {}
            }
        },
        'team_source': None,
        'opponent_source': None
    }
}

# ============================================================================
# DATABASE SCHEMA DDL GENERATION
# ============================================================================

# Generate schema once DB_COLUMNS is fully populated
_GENERATED_SCHEMA = generate_schema_ddl()
_EDITABLE_FIELDS = get_editable_fields()

DB_SCHEMA = {
    'editable_fields': _EDITABLE_FIELDS,  # Dynamically generated from DB_COLUMNS
    'create_schema_sql': _GENERATED_SCHEMA
}

def generate_create_table_ddl():
    tables = {}
    for col_name, col_config in DB_COLUMNS.items():
        table_name = col_config.get('table')
        if table_name:
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append((col_name, col_config))
    
    ddl_statements = []
    
    for table_name, columns in tables.items():
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        col_defs = []
        
        for col_name, col_config in columns:
            col_type = col_config['type']
            nullable = '' if col_config.get('nullable', True) else ' NOT NULL'
            default = f" DEFAULT {col_config['default']}" if 'default' in col_config else ''
            col_defs.append(f"    {col_name} {col_type}{nullable}{default}")
        
        ddl += ',\n'.join(col_defs)
        ddl += "\n);"
        ddl_statements.append(ddl)
    
    return '\n\n'.join(ddl_statements)

def get_columns_by_endpoint(endpoint_name, entity='player', table=None, pt_measure_type=None, measure_type_detailed_defense=None, defense_category=None):

    result = {}
    source_key = f'{entity}_source'
    
    for col_name, col_config in DB_COLUMNS.items():
        # Skip if not API
        if not col_config.get('api', False):
            continue
            
        # Skip opponent columns (auto-generated)
        if col_name.startswith('opp_'):
            continue
            
        # Filter by table if specified
        # Accept both specific table name (player_season_stats, team_season_stats) and generic 'stats'
        if table:
            col_table = col_config.get('table')
            if col_table not in [table, 'stats']:
                continue
                
        # Check if column has source for this entity
        source = col_config.get(source_key)
        if source is None or not isinstance(source, dict):
            continue
            
        # Check endpoint match
        if source.get('endpoint') == endpoint_name:
            # Get params from source (new consistent structure)
            source_params = source.get('params', {})
            
            # If pt_measure_type is specified, filter by it
            if pt_measure_type is not None:
                if source_params.get('pt_measure_type') == pt_measure_type:
                    result[col_name] = col_config
            # If measure_type_detailed_defense is specified, filter by it
            elif measure_type_detailed_defense is not None:
                if source_params.get('measure_type_detailed_defense') == measure_type_detailed_defense:
                    result[col_name] = col_config
            # If defense_category is specified, filter by it
            elif defense_category is not None:
                if source_params.get('defense_category') == defense_category:
                    result[col_name] = col_config
            else:
                # If no parameter filter, only include columns without special parameters
                if not source_params:
                    result[col_name] = col_config
    
    return result


def get_columns_by_entity(entity):
    """
    Get all columns for a specific entity.
    
    Args:
        entity: 'player', 'team', or 'opponent'
    
    Returns:
        Dict of {column_name: column_config}
    """
    source_key = f'{entity}_source'
    result = {}
    for col_name, col_config in DB_COLUMNS.items():
        # Skip opponent columns if querying opponent (they're auto-generated)
        if entity == 'opponent' and col_name.startswith('opp_'):
            continue
            
        if col_config.get(source_key) is not None:
            result[col_name] = col_config
    
    return result


def get_columns_by_update_frequency(frequency):
    """
    Get all columns that should be updated at a specific frequency.
    
    Args:
        frequency: 'daily' or 'annual'
    
    Returns:
        Dict of {column_name: column_config}
    """
    result = {}
    for col_name, col_config in DB_COLUMNS.items():
        if col_name.startswith('opp_'):
            continue
            
        if col_config.get('update_frequency') == frequency:
            result[col_name] = col_config
    
    return result

def get_opponent_columns():
    """
    Get the 13 opponent columns that mirror basic stats.
    
    Returns:
        Dict of {column_name: column_config} for opponent columns
    """
    # Get base columns that have opponent_source
    base_columns = {}
    for col_name, col_config in DB_COLUMNS.items():
        if col_config.get('opponent_source') is not None:
            base_columns[col_name] = col_config
    
    # Generate opponent versions
    opponent_columns = {}
    for col_name, col_config in base_columns.items():
        opp_col_name = f'opp_{col_name}'
        if opp_col_name in DB_COLUMNS:
            opponent_columns[opp_col_name] = DB_COLUMNS[opp_col_name]
    
    return opponent_columns


def get_column_list_for_insert(entity='player', include_opponent=False):
    columns = []
    
    # Identity columns first
    identity_cols = ['player_id', 'team_id', 'year', 'season_type']
    for col in identity_cols:
        if col in DB_COLUMNS and entity in DB_COLUMNS[col].get('entities', []):
            columns.append(col)
    
    # Stat columns (alphabetically for consistency)
    for col_name in sorted(DB_COLUMNS.keys()):
        if col_name in identity_cols:
            continue
        if col_name.startswith('opp_') and not include_opponent:
            continue
        
        col_config = DB_COLUMNS[col_name]
        if entity in col_config.get('entities', []):
            columns.append(col_name)
    
    return columns

def execute_transform(value, transform_name, scale=1):
    # Import transform functions (must be available in calling context)
    # This is a helper - the actual functions are in etl.py
    transform_functions = {
        'safe_int': lambda v, s: safe_int(v, scale=s),
        'safe_float': lambda v, s: safe_float(v, scale=s),
        'safe_str': lambda v, s: safe_str(v),
        'parse_height': lambda v, s: parse_height(v),
        'parse_birthdate': lambda v, s: parse_birthdate(v)
    }
    
    if transform_name not in transform_functions:
        raise ValueError(f"Unknown transform: {transform_name}")
    
    return transform_functions[transform_name](value, scale)


# Safe transform functions (referenced by execute_transform)
# These are duplicated from etl.py to make db_config self-contained
def safe_int(value, scale=1):
    """Convert value to scaled integer, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return 0
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return 0


def safe_float(value, scale=1):
    """Convert value to scaled float (as integer), handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return 0
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return 0


def safe_str(value):
    """Safely convert to string"""
    if value is None or value == '' or (hasattr(value, '__len__') and len(value) == 0):
        return None
    return str(value)


def parse_height(height_str):
    """Parse height from NBA API format to inches"""
    if not height_str or height_str == '' or height_str == 'None':
        return 0
    try:
        if '-' in str(height_str):
            feet, inches = str(height_str).split('-')
            return int(feet) * 12 + int(inches)
        else:
            return int(float(height_str))
    except (ValueError, AttributeError):
        return 0


def parse_birthdate(date_str):
    """Parse birthdate string to date"""
    from datetime import datetime
    if not date_str or date_str == '' or str(date_str).lower() == 'nan':
        return None
    try:
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                return datetime.strptime(str(date_str).split('.')[0], fmt).date()
            except Exception:
                continue
        return None
    except Exception:
        return None

PRIMARY_KEYS = {
    'player': 'player_id',
    'team': 'team_id'
}

COMPOSITE_KEY_FIELDS = ['year', 'season_type']

def get_primary_key(entity):
    return PRIMARY_KEYS.get(entity, 'id')

def get_composite_keys():
    return COMPOSITE_KEY_FIELDS

def get_all_key_fields(entity):
    return [get_primary_key(entity)] + get_composite_keys()

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

SUBPROCESS_CONFIG = {
    'players_per_subprocess': 1000,
    'delay_between_calls': 1.5,
    'timeout': 20,
    'max_retries': 3,
    'subprocess_timeout': 1500,
    'queue_timeout': 30,
    'thread_join_timeout': 2,
    'failure_log_limit': 3,
}

API_CONFIG = {
    'rate_limit_delay': float(os.getenv('API_RATE_LIMIT_DELAY', '0.6')),
    'timeout_default': 20,
    'rate_limiter_window_size': 60,     # Seconds for rate limiter sliding window
    'backoff_divisor': 5,               # Divisor for exponential backoff calculation
    'timeout_bulk': 120,
    'max_retries': 3,
    
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
# NBA API FIELD NAMES
# ============================================================================
# Standard field names used across NBA API endpoints
# Centralizes all API field name references to eliminate hardcoding

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
        'vs_player': 'VS_PLAYER_ID',  # Used in teamplayeronoffsummary
        'person': 'PERSON_ID'          # Used in commonplayerinfo
    }
}

def get_entity_id_field(entity):
    return API_FIELD_NAMES['entity_id'].get(entity, 'ID')

def get_entity_name_field(entity):
    return API_FIELD_NAMES['entity_name'].get(entity, 'NAME')

RETRY_CONFIG = {
    'max_retries': 3,
    'backoff_base': 10,
}

DB_OPERATIONS = {
    'bulk_insert_batch_size': 1000,
    'statement_timeout_ms': 120000,  # 2 minutes
}
