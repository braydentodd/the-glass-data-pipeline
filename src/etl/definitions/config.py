"""
The Glass - ETL Settings

Table definitions, operational settings, validation schemas, and
default transform mappings.

Transforms are inferred from column type unless overridden:
  SMALLINT / INTEGER -> safe_int
  VARCHAR / TEXT / CHAR -> safe_str
"""

from typing import Any, Dict


# ============================================================================
# VALIDATION SCHEMAS  (co-located with the config they describe)
# ============================================================================

VALID_PG_TYPES = {
    'SERIAL', 'SMALLINT', 'INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'CHAR',
    'BOOLEAN', 'TIMESTAMP', 'DATE', 'NUMERIC', 'REAL', 'DOUBLE PRECISION',
}
VALID_ENTITY_TYPES = {'player', 'team', 'opponent'}
VALID_SCOPES = {'entity', 'stats'}
VALID_UPDATE_FREQUENCIES = {'daily', 'annual', None}

DB_COLUMNS_SCHEMA = {
    'type': {'required': True, 'types': (str,)},
    'scope': {'required': True, 'types': (list,), 'list_item_values': VALID_SCOPES},
    'nullable': {'required': True, 'types': (bool,)},
    'default': {'required': True, 'types': (str, type(None))},
    'entity_types': {'required': True, 'types': (list,), 'list_item_values': VALID_ENTITY_TYPES},
    'update_frequency': {'required': True, 'types': (str, type(None)), 'allowed_values': VALID_UPDATE_FREQUENCIES},
    'rate_group': {'required': True, 'types': (str, type(None))},
    'comment': {'required': True, 'types': (str, type(None))},
    'sources': {'required': True, 'types': (dict, type(None))},
}

TABLES_SCHEMA = {
    'entity': {'required': True, 'types': (str,), 'allowed_values': {'player', 'team'}},
    'scope': {'required': True, 'types': (str,), 'allowed_values': VALID_SCOPES},
    'unique_key': {'required': False, 'types': (list,)},
    'has_opponent_columns': {'required': False, 'types': (bool,)},
}

ETL_CONFIG_SCHEMA = {
    'retention_seasons': {'required': True, 'types': (int,)},
    'calendar_flip_month': {'required': True, 'types': (int,)},
    'calendar_flip_day': {'required': True, 'types': (int,)},
    'max_retry_attempts': {'required': True, 'types': (int,)},
    'retry_delay_seconds': {'required': True, 'types': (int,)},
    'auto_resume': {'required': True, 'types': (bool,)},
}


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
    },
    'teams': {
        'entity': 'team',
        'scope': 'entity',
    },
    'player_season_stats': {
        'entity': 'player',
        'scope': 'stats',
        'unique_key': ['entity_id', 'season', 'season_type'],
    },
    'team_season_stats': {
        'entity': 'team',
        'scope': 'stats',
        'unique_key': ['entity_id', 'season', 'season_type'],
        'has_opponent_columns': True,
    },
}


# ============================================================================
# ETL OPERATIONAL SETTINGS
# ============================================================================

ETL_CONFIG = {
    'retention_seasons': 7,
    'calendar_flip_month': 7,
    'calendar_flip_day': 1,
    'max_retry_attempts': 3,
    'retry_delay_seconds': 60,
    'auto_resume': True,
}


# ============================================================================
# ETL OPERATIONAL TABLES  (inline columns, not in DB_COLUMNS)
# ============================================================================

ETL_TABLES = {
    'etl_runs': {
        'columns': {
            'id': {'type': 'SERIAL', 'primary_key': True, 'nullable': False},
            'run_type': {'type': 'VARCHAR(20)', 'nullable': False},
            'status': {'type': 'VARCHAR(20)', 'nullable': False, 'default': "'running'"},
            'started_at': {'type': 'TIMESTAMP', 'nullable': False, 'default': 'NOW()'},
            'completed_at': {'type': 'TIMESTAMP', 'nullable': True},
            'season': {'type': 'VARCHAR(7)', 'nullable': True},
            'season_type': {'type': 'VARCHAR(3)', 'nullable': True},
            'entity_type': {'type': 'VARCHAR(10)', 'nullable': True},
            'total_groups': {'type': 'INTEGER', 'nullable': True, 'default': '0'},
            'completed_groups': {'type': 'INTEGER', 'nullable': True, 'default': '0'},
            'total_rows': {'type': 'INTEGER', 'nullable': True, 'default': '0'},
            'error_message': {'type': 'TEXT', 'nullable': True},
        },
    },
    'etl_progress': {
        'columns': {
            'id': {'type': 'SERIAL', 'primary_key': True, 'nullable': False},
            'run_id': {'type': 'INTEGER', 'nullable': False},
            'entity_type': {'type': 'VARCHAR(10)', 'nullable': False},
            'endpoint': {'type': 'VARCHAR(100)', 'nullable': False},
            'tier': {'type': 'VARCHAR(20)', 'nullable': False},
            'column_name': {'type': 'VARCHAR(100)', 'nullable': True},
            'status': {'type': 'VARCHAR(20)', 'nullable': False, 'default': "'pending'"},
            'started_at': {'type': 'TIMESTAMP', 'nullable': True},
            'completed_at': {'type': 'TIMESTAMP', 'nullable': True},
            'rows_written': {'type': 'INTEGER', 'nullable': True, 'default': '0'},
            'error_message': {'type': 'TEXT', 'nullable': True},
            'retry_count': {'type': 'INTEGER', 'nullable': True, 'default': '0'},
        },
        'unique_key': ['run_id', 'entity_type', 'endpoint', 'column_name'],
    },
}


# ============================================================================
