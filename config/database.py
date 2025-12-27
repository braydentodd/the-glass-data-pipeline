"""
THE GLASS - Database Configuration
Single source of truth for all database columns, schema, and API mappings.

This module defines DB_COLUMNS - the complete database schema with:
- Column names and data types
- API source mappings (which endpoint provides which data)
- Entity applicability (player, team, opponent)

NO HARDCODING. Everything is driven by this config.
"""

import os
from datetime import datetime

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '150.136.255.23'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'the_glass_db'),
    'user': os.getenv('DB_USER', 'the_glass_user'),
    'password': os.getenv('DB_PASSWORD', '')
}

# ============================================================================
# TABLE NAMES - Single source of truth (no hardcoding!)
# ============================================================================

TABLES = ['teams', 'players', 'player_season_stats', 'team_season_stats']

# ============================================================================
# ENDPOINT EXECUTION TIER INFERENCE
# ============================================================================
# These functions infer execution tiers from endpoint names dynamically
# This eliminates hardcoding and makes DB_COLUMNS the single source of truth

def infer_execution_tier_from_endpoint(endpoint_name):
    """
    Infer execution tier from endpoint name pattern.
    
    Tier Classification:
    - TIER 1 (league): Single API call returns all entities
      Pattern: league* endpoints (leaguedashplayerstats, leaguehustlestatsplayer, etc.)
      Strategy: 10 workers, no batching
      
    - TIER 2 (team): One API call per team (30 total)
      Pattern: team* endpoints (teamdash*, teamplayeron*, etc.)
      Strategy: 10 workers, no batching needed
      
    - TIER 3 (player): One API call per player (536 total)
      Pattern: playerdash*, commonplayer* (per-player endpoints)
      Strategy: 3 workers, batched execution with cooldowns
      
    Args:
        endpoint_name (str): Name of the NBA API endpoint
        
    Returns:
        str: 'league', 'team', or 'player'
    """
    endpoint_lower = endpoint_name.lower()
    
    # Player-specific endpoints (per-player API calls)
    # MUST check these BEFORE team* to avoid misclassifying teamplayer* endpoints
    if endpoint_lower.startswith('playerdash') or endpoint_lower.startswith('commonplayer'):
        return 'player'
    
    # Team-specific endpoints (per-team API calls)
    # Covers: teamdash*, teamplayeron*, etc.
    if endpoint_lower.startswith('team'):
        return 'team'
    
    # League-wide endpoints (single API call)
    if endpoint_lower.startswith('league'):
        return 'league'
    
    # Default to league tier for unknown patterns (safest)
    return 'league'

# ============================================================================
# NBA CONFIGURATION
# ============================================================================

def get_current_season_year():
    """Get current NBA season year (e.g., 2026 for 2025-26 season)
    
    NOTE: Database uses ENDING year of season (2026 for 2025-26 season)
    """
    now = datetime.now()
    # Return ending year: if December 2025, return 2026 (for 2025-26 season)
    return now.year + 1 if now.month > 8 else now.year

def get_current_season():
    """Get current NBA season string (e.g., '2025-26')"""
    year = get_current_season_year()
    return f"{year - 1}-{str(year)[-2:]}"

# Season type mapping (used throughout ETL to avoid magic numbers)
SEASON_TYPE_MAP = {
    'Regular Season': 1,
    'Playoffs': 2,
    'PlayIn': 3,
}

NBA_CONFIG = {
    'current_season_year': get_current_season_year(),
    'current_season': get_current_season(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),  # 1=regular, 2=playoffs, 3=play-in
}

# ============================================================================
# NBA TEAMS - Database-driven (no hardcoding!)
# ============================================================================

def get_teams_from_db():
    """
    Fetch teams from database instead of hardcoding.
    Returns: dict of {team_id: (abbreviation, full_name)}
    """
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute("SELECT team_id, abbreviation, full_name FROM teams ORDER BY team_id")
        teams = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        return teams
    except Exception:
        # Fallback: Use NBA's standard team ID range (1610612737-1610612766 = 30 teams)
        # This allows config to load even before database is populated
        return {1610612737 + i: (f'TEAM{i}', f'Team {i}') for i in range(30)}

# Lazy-loaded team data (fetched from DB on first access)
_teams_cache = None

def get_team_ids():
    """Get dict of team IDs from database"""
    global _teams_cache
    if _teams_cache is None:
        _teams_cache = get_teams_from_db()
    return {abbr: tid for tid, (abbr, name) in _teams_cache.items()}

def get_teams_by_id():
    """Get dict of {team_id: full_name}"""
    global _teams_cache
    if _teams_cache is None:
        _teams_cache = get_teams_from_db()
    return {tid: name for tid, (abbr, name) in _teams_cache.items()}

def get_teams_by_abbr():
    """Get dict of {abbreviation: full_name}"""
    global _teams_cache
    if _teams_cache is None:
        _teams_cache = get_teams_from_db()
    return {abbr: name for tid, (abbr, name) in _teams_cache.items()}

# Team data - lazy-loaded from database
TEAM_IDS = get_team_ids()  # {abbreviation: team_id}

# ============================================================================
# DATABASE SCHEMA - Dynamically generated from DB_COLUMNS
# ============================================================================

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
    
    # Group columns by table (resolving category to actual table names)
    tables = {}
    
    def resolve_table_names(table_category, entities):
        """Convert table category + entities to actual table names."""
        if not table_category or not entities:
            return []
        
        result = []
        for entity in entities:
            if entity == 'opponent':
                continue  # Opponent columns auto-generated
            
            if table_category == 'entity':
                if entity == 'player':
                    result.append('players')
                elif entity == 'team':
                    result.append('teams')
            elif table_category == 'stats':
                if entity == 'player':
                    result.append('player_season_stats')
                elif entity == 'team':
                    result.append('team_season_stats')
            else:
                # Legacy: direct table name specification
                result.append(table_category)
        
        return result
    
    for col_name, col_config in DB_COLUMNS.items():
        table_category = col_config.get('table')
        entities = col_config.get('applies_to_entities', [])
        
        table_names = resolve_table_names(table_category, entities)
        
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


# ============================================================================
# DB_COLUMNS - Complete Database Schema
# ============================================================================
# Format:
# 'db_column_name': {
#     'table': 'player_season_stats' or 'team_season_stats',
#     'type': 'INTEGER', 'SMALLINT', 'VARCHAR(50)', etc.,
#     'nullable': True/False,
#     'default': default value (optional),
#     'update_frequency': 'daily' or 'annual' (tells ETL when to update this field),
#     'source': {
#         'endpoint': 'leaguedashplayerstats',
#         'field': 'GP',
#         'transform': 'safe_int' (function name to call),
#         'scale': 10 or 1000 (optional - multiplier for transform function),
#         'shot_zone': 'RestrictedArea' (optional filter),
#         'defender_distance': '0-4 Feet - Tight' (optional filter),
#         'defense_category': '2 Pointers' (optional filter)
#     } or None for ETL-derived fields,
#     'applies_to_entities': ['player', 'team', 'opponent']
# }

DB_COLUMNS = {
    # ========================================================================
    # IDENTITY COLUMNS
    # ========================================================================
    'player_id': {
        'table': 'entity',
        'type': 'INTEGER',
        'nullable': False,
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'PLAYER_ID',
            'transform': 'safe_int'
        }
    },
    
    'name': {
        'table': 'entity',
        'type': 'VARCHAR(50)',
        'nullable': True,
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'PLAYER_NAME',
            'transform': 'safe_str'
        }
    },
    
    'year': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': False,
        'applies_to_entities': ['player', 'team'],
        'source': None
    },
    
    'season_type': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': False,
        'applies_to_entities': ['player', 'team'],
        'source': None
    },
    
    # ========================================================================
    # PHYSICAL ATTRIBUTES (Players table)
    # ========================================================================
    'height_inches': {
        'table': 'entity',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'annual',
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'HEIGHT',
            'transform': 'parse_height'
        }
    },
    
    'weight_lbs': {
        'table': 'entity',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'annual',
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'WEIGHT',
            'transform': 'safe_int'
        }
    },
    
    'wingspan_inches': {
        'table': 'entity',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'annual',
        'applies_to_entities': ['player'],
        'source': None
    },
    
    'birthdate': {
        'table': 'entity',
        'type': 'DATE',
        'nullable': True,
        'update_frequency': 'annual',
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'BIRTHDATE',
            'transform': 'parse_birthdate'
        }
    },
    
    'jersey_number': {
        'table': 'entity',
        'type': 'VARCHAR(3)',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'JERSEY',
            'transform': 'safe_str'
        }
    },
    
    'pre_nba_team': {
        'table': 'entity',
        'type': 'VARCHAR(100)',
        'nullable': True,
        'applies_to_entities': ['player'],
        'player_source': {
            'endpoint': 'commonplayerinfo',
            'field': 'SCHOOL',
            'transform': 'safe_str'
        }
    },
    
    'notes': {
        'table': 'entity',
        'type': 'TEXT',
        'nullable': True,
        'applies_to_entities': ['player', 'team'],
        'source': None
    },
    
    # ========================================================================
    # RATE STATS (Games, Minutes)
    # ========================================================================
    'games_played': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': False,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'field': 'GP',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'field': 'GP',
            'transform': 'safe_int'
        }
    },
    
    'minutes_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
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
        }
    },
    
    # ========================================================================
    # SHOOTING STATS (2PT, 3PT, FT)
    # ========================================================================
    '2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
    
    # ========================================================================
    # ADVANCED SHOOTING STATS (Tracking data)
    # ========================================================================
    'cont_close_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': 'RestrictedArea',
            'defender_distance': '0-4 Feet - Tight',
            'field': 'FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefenderShooting',
            'defender_distance_category': 'contested',  # Aggregates: 0-2ft Very Tight + 2-4ft Tight
            'field': 'FG2M',
            'transform': 'safe_int'
        }
    },
    
    'cont_close_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': 'RestrictedArea',
            'defender_distance': '0-4 Feet - Tight',
            'field': 'FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefenderShooting',
            'defender_distance_category': 'contested',  # Aggregates: 0-2ft Very Tight + 2-4ft Tight
            'field': 'FG2A',
            'transform': 'safe_int'
        }
    },
    
    'open_close_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': 'RestrictedArea',
            'defender_distance': '4+ Feet - Open',
            'field': 'FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefenderShooting',
            'defender_distance_category': 'open',  # Aggregates: 4-6ft Open + 6+ft Wide Open
            'field': 'FG2M',
            'transform': 'safe_int'
        }
    },
    
    'open_close_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': 'RestrictedArea',
            'defender_distance': '4+ Feet - Open',
            'field': 'FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefenderShooting',
            'defender_distance_category': 'open',  # Aggregates: 4-6ft Open + 6+ft Wide Open
            'field': 'FG2A',
            'transform': 'safe_int'
        }
    },
    
    'cont_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_type': '2PT',
            'defender_distance': '0-4 Feet - Tight',
            'field': 'FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'contested',  # Aggregates: 0-2ft Very Tight + 2-4ft Tight
            'field': 'FG2M',  # Fixed: was FG3M, should be FG2M
            'transform': 'safe_int'
        }
    },
    
    'cont_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_type': '2PT',
            'defender_distance': '0-4 Feet - Tight',
            'field': 'FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'contested',  # Aggregates: 0-2ft Very Tight + 2-4ft Tight
            'field': 'FG2A',  # Fixed: was FG3A, should be FG2A
            'transform': 'safe_int'
        }
    },
    
    'open_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_type': '2PT',
            'defender_distance': '4+ Feet - Open',
            'field': 'FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'open',  # Aggregates: 4-6ft Open + 6+ft Wide Open
            'field': 'FG2M',  # Fixed: was FG3M, should be FG2M
            'transform': 'safe_int'
        }
    },
    
    'open_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_type': '2PT',
            'defender_distance': '4+ Feet - Open',
            'field': 'FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'open',  # Aggregates: 4-6ft Open + 6+ft Wide Open
            'field': 'FG2A',  # Fixed: was FG3A, should be FG2A
            'transform': 'safe_int'
        }
    },
    
    'cont_3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': '3PT',
            'defender_distance': '0-4 Feet - Tight',
            'field': 'FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'contested',  # Aggregates: 0-2ft Very Tight + 2-4ft Tight
            'field': 'FG3M',
            'transform': 'safe_int'
        }
    },
    
    'cont_3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': '3PT',
            'defender_distance': '0-4 Feet - Tight',
            'field': 'FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'contested',  # Aggregates: 0-2ft Very Tight + 2-4ft Tight
            'field': 'FG3A',
            'transform': 'safe_int'
        }
    },
    
    'open_3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': '3PT',
            'defender_distance': '4+ Feet - Open',
            'field': 'FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'open',  # Aggregates: 4-6ft Open + 6+ft Wide Open
            'field': 'FG3M',
            'transform': 'safe_int'
        }
    },
    
    'open_3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'playerdashptshots',
            'shot_zone': '3PT',
            'defender_distance': '4+ Feet - Open',
            'field': 'FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'teamdashptshots',
            'execution_tier': 'team',
            'result_set': 'ClosestDefender10ftPlusShooting',
            'defender_distance_category': 'open',  # Aggregates: 4-6ft Open + 6+ft Wide Open
            'field': 'FG3A',
            'transform': 'safe_int'
        }
    },
    
    # ========================================================================
    # REBOUNDING STATS
    # ========================================================================
    'o_rebounds': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
    
    'o_reb_pct_x1000': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'result_set': 'Advanced',
            'field': 'OREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'measure_type_detailed_defense': 'Advanced',
            'field': 'OREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        }
    },
    
    'd_reb_pct_x1000': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'result_set': 'Advanced',
            'field': 'DREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'measure_type_detailed_defense': 'Advanced',
            'field': 'DREB_PCT',
            'transform': 'safe_int',
            'scale': 1000
        }
    },
    
    'cont_o_rebs': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'description': 'Contested offensive rebounds',
        'team_source': {
            'endpoint': 'teamdashptreb',
            'execution_tier': 'team',
            'result_set': 'OverallRebounding',
            'field': 'C_OREB',
            'transform': 'safe_int'
        }
    },
    
    'cont_d_rebs': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'description': 'Contested defensive rebounds',
        'team_source': {
            'endpoint': 'teamdashptreb',
            'execution_tier': 'team',
            'result_set': 'OverallRebounding',
            'field': 'C_DREB',
            'transform': 'safe_int'
        }
    },
    
    'putbacks': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'description': 'Putbacks and tip shots (handled by transformation)'
    },
    
    # ========================================================================
    # DISTRIBUTION STATS (Assists, Turnovers, Touches)
    # ========================================================================
    'touches': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'TOUCHES',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'TOUCHES',
            'transform': 'safe_int'
        }
    },
    
    'poss_time': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'TIME_OF_POSS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Possessions',
            'field': 'TIME_OF_POSS',
            'transform': 'safe_int'
        }
    },
    
    'passes': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Passing',
            'field': 'PASSES_MADE',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Passing',
            'field': 'PASSES_MADE',
            'transform': 'safe_int'
        }
    },
    
    'sec_assists': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Passing',
            'field': 'SECONDARY_AST',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Passing',
            'field': 'SECONDARY_AST',
            'transform': 'safe_int'
        }
    },
    
    'o_dist_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'SpeedDistance',
            'field': 'DIST_MILES_OFF',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'SpeedDistance',
            'field': 'DIST_MILES_OFF',
            'transform': 'safe_int',
            'scale': 10
        }
    },
    
    'd_dist_x10': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'SpeedDistance',
            'field': 'DIST_MILES_DEF',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'SpeedDistance',
            'field': 'DIST_MILES_DEF',
            'transform': 'safe_int',
            'scale': 10
        }
    },
    
    'assists': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Passing',
            'field': 'POTENTIAL_AST',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptstats',
            'pt_measure_type': 'Passing',
            'field': 'POTENTIAL_AST',
            'transform': 'safe_int'
        }
    },
    
    'turnovers': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
    
    # ========================================================================
    # DEFENSE STATS
    # ========================================================================
    'steals': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team', 'opponent'],
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
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguehustlestatsplayer',
            'field': 'DEFLECTIONS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguehustlestatsteam',
            'field': 'DEFLECTIONS',
            'transform': 'safe_int'
        }
    },
    
    'charges_drawn': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguehustlestatsplayer',
            'field': 'CHARGES_DRAWN',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguehustlestatsteam',
            'field': 'CHARGES_DRAWN',
            'transform': 'safe_int'
        }
    },
    
    'contests': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguehustlestatsplayer',
            'field': 'CONTESTED_SHOTS',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguehustlestatsteam',
            'field': 'CONTESTED_SHOTS',
            'transform': 'safe_int'
        }
    },
    
    'd_close_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': 'Less Than 10Ft',
            'field': 'D_FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': 'Less Than 10Ft',
            'field': 'D_FGM',
            'transform': 'safe_int'
        }
    },
    
    'd_close_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': 'Less Than 10Ft',
            'field': 'D_FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': 'Less Than 10Ft',
            'field': 'D_FGA',
            'transform': 'safe_int'
        }
    },
    
    'd_2fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': '2 Pointers',
            'field': 'D_FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': '2 Pointers',
            'field': 'D_FGM',
            'transform': 'safe_int'
        }
    },
    
    'd_2fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': '2 Pointers',
            'field': 'D_FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': '2 Pointers',
            'field': 'D_FGA',
            'transform': 'safe_int'
        }
    },
    
    'd_3fgm': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': '3 Pointers',
            'field': 'D_FGM',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': '3 Pointers',
            'field': 'D_FGM',
            'transform': 'safe_int'
        }
    },
    
    'd_3fga': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'default': 0,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': '3 Pointers',
            'field': 'D_FGA',
            'transform': 'safe_int'
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': '3 Pointers',
            'field': 'D_FGA',
            'transform': 'safe_int'
        }
    },
    
    'real_d_fg_pct_x1000': {
        'table': 'stats',
        'type': 'INTEGER',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashptdefend',
            'defense_category': 'Overall',
            'field': 'PCT_PLUSMINUS',
            'transform': 'safe_int',
            'scale': 1000
        },
        'team_source': {
            'endpoint': 'leaguedashptteamdefend',
            'defense_category': 'Overall',
            'field': 'PCT_PLUSMINUS',
            'transform': 'safe_int',
            'scale': 1000
        }
    },
    
    # ========================================================================
    # ADVANCED METRICS (Ratings)
    # ========================================================================
    'o_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'result_set': 'Advanced',
            'field': 'OFF_RATING',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'measure_type_detailed_defense': 'Advanced',
            'field': 'OFF_RATING',
            'transform': 'safe_int',
            'scale': 10
        }
    },
    
    'd_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player', 'team'],
        'player_source': {
            'endpoint': 'leaguedashplayerstats',
            'result_set': 'Advanced',
            'field': 'DEF_RATING',
            'transform': 'safe_int',
            'scale': 10
        },
        'team_source': {
            'endpoint': 'leaguedashteamstats',
            'measure_type_detailed_defense': 'Advanced',
            'field': 'DEF_RATING',
            'transform': 'safe_int',
            'scale': 10
        }
    },
    
    # ========================================================================
    # ON/OFF STATS (Player only)
    # ========================================================================
    # ON/OFF STATS (Player only)
    # ========================================================================
    'tm_off_o_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player'],
        'description': 'Team offensive rating when player off court (handled by transformation)'
    },
    
    'tm_off_d_rating_x10': {
        'table': 'stats',
        'type': 'SMALLINT',
        'nullable': True,
        'update_frequency': 'daily',
        'applies_to_entities': ['player'],
        'description': 'Team defensive rating when player off court (handled by transformation)'
    }
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_db_columns_by_entity(entity_type='player'):
    """Get all database columns applicable to an entity type."""
    return {
        col_name: col_config
        for col_name, col_config in DB_COLUMNS.items()
        if entity_type in col_config.get('applies_to_entities', [])
    }


def get_db_columns_by_table(table_name):
    """Get all database columns for a specific table."""
    return {
        col_name: col_config
        for col_name, col_config in DB_COLUMNS.items()
        if col_config.get('table') == table_name
    }


def get_columns_by_api_endpoint(endpoint):
    """Get all columns populated by a specific API endpoint."""
    return {
        col_name: col_config
        for col_name, col_config in DB_COLUMNS.items()
        if (isinstance(col_config.get('source'), dict) and 
            col_config['source'].get('endpoint') == endpoint)
    }


# ============================================================================
# OPPONENT COLUMN GENERATION
# ============================================================================

def generate_opponent_columns():
    """Auto-generate opponent versions of base columns."""
    opponent_columns = {}
    
    for col_name, col_config in DB_COLUMNS.items():
        if 'opponent' in col_config.get('applies_to_entities', []):
            opp_col_name = f'opp_{col_name}'
            
            # Handle both string and list table values
            original_table = col_config['table']
            if isinstance(original_table, list):
                # If it's a list, convert player_season_stats to team_season_stats
                new_table = ['team_season_stats' if t == 'player_season_stats' else t for t in original_table]
            else:
                # If it's a string, use replace
                new_table = original_table.replace('player_', 'team_')
            
            opponent_columns[opp_col_name] = {
                **col_config,
                'table': new_table,
                'applies_to_entities': ['team'],
                'is_opponent_stat': True
            }
    
    return opponent_columns


# Auto-generate and merge opponent columns into DB_COLUMNS
OPPONENT_COLUMNS = generate_opponent_columns()
DB_COLUMNS.update(OPPONENT_COLUMNS)


# ============================================================================
# DATABASE SCHEMA DDL GENERATION
# ============================================================================

# Generate schema once DB_COLUMNS is fully populated
_GENERATED_SCHEMA = generate_schema_ddl()

DB_SCHEMA = {
    'editable_fields': ['wingspan_inches', 'notes'],
    'create_schema_sql': _GENERATED_SCHEMA
}


# ============================================================================
# ETL HELPER FUNCTIONS - Config-driven queries
# ============================================================================

def generate_create_table_ddl():
    """Generate CREATE TABLE statements from DB_COLUMNS."""
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


# ============================================================================
# ETL HELPER FUNCTIONS - Config-driven queries
# ============================================================================

def get_columns_by_endpoint(endpoint_name, entity='player', table=None, pt_measure_type=None, measure_type_detailed_defense=None):
    """
    Get all columns that use a specific API endpoint for a given entity.
    
    Args:
        endpoint_name: NBA API endpoint (e.g., 'leaguedashplayerstats')
        entity: 'player', 'team', or 'opponent'
        table: Optional table filter (e.g., 'player_season_stats', 'team_season_stats')
        pt_measure_type: Optional filter for endpoints that use pt_measure_type (e.g., 'Passing', 'Possessions')
        measure_type_detailed_defense: Optional filter for endpoints that use measure_type_detailed_defense (e.g., 'Advanced')
    
    Returns:
        Dict of {column_name: column_config} for matching columns
    """
    result = {}
    
    # Helper to resolve table category to actual table names
    def resolve_table_names(table_category, entities):
        """Convert table category + entities to actual table names."""
        if not table_category or not entities:
            return []
        
        result = []
        for ent in entities:
            if ent == 'opponent':
                continue
            
            if table_category == 'entity':
                if ent == 'player':
                    result.append('players')
                elif ent == 'team':
                    result.append('teams')
            elif table_category == 'stats':
                if ent == 'player':
                    result.append('player_season_stats')
                elif ent == 'team':
                    result.append('team_season_stats')
            else:
                # Legacy: direct table name
                result.append(table_category)
        
        return result
    
    # Determine which source key to check based on entity
    source_key = f'{entity}_source' if entity in ['player', 'team', 'opponent'] else 'source'
    
    for col_name, col_config in DB_COLUMNS.items():
        # Skip opponent columns (auto-generated)
        if col_name.startswith('opp_'):
            continue
            
        # Filter by table if specified
        if table:
            table_category = col_config.get('table')
            entities = col_config.get('applies_to_entities', [])
            col_tables = resolve_table_names(table_category, entities)
            
            # Check if requested table is in the column's resolved table list
            if table not in col_tables:
                continue
            
        # Check if column applies to this entity
        if entity not in col_config.get('applies_to_entities', []):
            continue
            
        # Check if column uses this endpoint (check entity-specific source first, then fall back to generic 'source')
        source = col_config.get(source_key) or col_config.get('source')
        if source and source.get('endpoint') == endpoint_name:
            # If pt_measure_type is specified, filter by it
            if pt_measure_type is not None:
                if source.get('pt_measure_type') == pt_measure_type:
                    result[col_name] = col_config
            # If measure_type_detailed_defense is specified, filter by it OR result_set
            elif measure_type_detailed_defense is not None:
                # Check both measure_type_detailed_defense (team) and result_set (player) fields
                if (source.get('measure_type_detailed_defense') == measure_type_detailed_defense or 
                    source.get('result_set') == measure_type_detailed_defense):
                    result[col_name] = col_config
            else:
                # If no parameter filter, only include columns without special parameters
                if ('pt_measure_type' not in source and 
                    'measure_type_detailed_defense' not in source and 
                    'result_set' not in source):
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
    result = {}
    for col_name, col_config in DB_COLUMNS.items():
        # Skip opponent columns if querying opponent (they're auto-generated)
        if entity == 'opponent' and col_name.startswith('opp_'):
            continue
            
        if entity in col_config.get('applies_to_entities', []):
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
    # Get base columns that apply to opponent
    base_columns = {}
    for col_name, col_config in DB_COLUMNS.items():
        if 'opponent' in col_config.get('applies_to_entities', []):
            base_columns[col_name] = col_config
    
    # Generate opponent versions
    opponent_columns = {}
    for col_name, col_config in base_columns.items():
        opp_col_name = f'opp_{col_name}'
        if opp_col_name in DB_COLUMNS:
            opponent_columns[opp_col_name] = DB_COLUMNS[opp_col_name]
    
    return opponent_columns


def get_calculated_columns():
    """
    Get all columns that require calculation (no direct API field).
    
    Returns:
        Dict of {column_name: column_config} for calculated columns
    """
    result = {}
    for col_name, col_config in DB_COLUMNS.items():
        if col_name.startswith('opp_'):
            continue
            
        source = col_config.get('source')
        # Column is calculated if source exists but field is None
        if source and source.get('field') is None:
            result[col_name] = col_config
    
    return result


def get_column_list_for_insert(entity='player', include_opponent=False):
    """
    Get ordered list of column names for SQL INSERT statements.
    
    Args:
        entity: 'player' or 'team'
        include_opponent: If True, include opponent columns (for team stats)
    
    Returns:
        List of column names in logical order
    """
    columns = []
    
    # Identity columns first
    identity_cols = ['player_id', 'team_id', 'year', 'season_type']
    for col in identity_cols:
        if col in DB_COLUMNS and entity in DB_COLUMNS[col].get('applies_to_entities', []):
            columns.append(col)
    
    # Stat columns (alphabetically for consistency)
    for col_name in sorted(DB_COLUMNS.keys()):
        if col_name in identity_cols:
            continue
        if col_name.startswith('opp_') and not include_opponent:
            continue
        
        col_config = DB_COLUMNS[col_name]
        if entity in col_config.get('applies_to_entities', []):
            columns.append(col_name)
    
    return columns


def execute_transform(value, transform_name, scale=1):
    """
    Execute a transform function dynamically from config.
    
    Args:
        value: Raw value from API
        transform_name: Function name (e.g., 'safe_int', 'safe_float')
        scale: Optional multiplier
    
    Returns:
        Transformed value
    """
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
