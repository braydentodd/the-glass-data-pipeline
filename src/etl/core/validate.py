"""
The Glass - Schema-Driven Config Validation

Validates all configuration dictionaries at startup using declarative
schemas.  Each schema defines required/optional attributes, types,
allowed values, and cross-references — the validation engine is generic.

Add a new config?  Define a schema dict, add it to ``ALL_VALIDATIONS``.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


# ============================================================================
# VALIDATION SCHEMAS
#
# Each schema is a dict of {attribute_name: constraint_dict}.
# Constraint keys:
#   required        : bool — must be present (default: True)
#   types           : tuple of types — acceptable Python types (use None for NoneType)
#   allowed_values  : set — restrict to these values
#   list_item_values: set — if value is a list, restrict each item
#   dict_required   : dict — nested schema for dict values
# ============================================================================

VALID_PG_TYPES = {
    'SERIAL', 'SMALLINT', 'INTEGER', 'BIGINT', 'VARCHAR', 'TEXT', 'CHAR',
    'BOOLEAN', 'TIMESTAMP', 'DATE', 'NUMERIC', 'REAL', 'DOUBLE PRECISION',
}

VALID_ENTITY_TYPES = {'player', 'team', 'opponent'}
VALID_SCOPES = {'entity', 'stats'}
VALID_TRANSFORMS = {
    'safe_int', 'safe_float', 'safe_str',
    'parse_height', 'parse_birthdate', 'format_season',
}
VALID_EXECUTION_TIERS = {'league', 'player', 'team', 'team_call'}
VALID_UPDATE_FREQUENCIES = {'daily', 'annual', None}

# -- DB_COLUMNS entry schema --
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

# -- TABLES entry schema --
TABLES_SCHEMA = {
    'entity': {'required': True, 'types': (str,), 'allowed_values': {'player', 'team'}},
    'scope': {'required': True, 'types': (str,), 'allowed_values': VALID_SCOPES},
    'unique_key': {'required': True, 'types': (list,)},
    'has_opponent_columns': {'required': False, 'types': (bool,)},
}

# -- ENDPOINTS entry schema --
ENDPOINTS_SCHEMA = {
    'min_season': {'required': True, 'types': (str, type(None))},
    'execution_tier': {'required': True, 'types': (str,), 'allowed_values': VALID_EXECUTION_TIERS},
    'default_result_set': {'required': True, 'types': (str,)},
    'season_type_param': {'required': True, 'types': (str, type(None))},
    'per_mode_param': {'required': True, 'types': (str, type(None))},
    'entity_types': {'required': True, 'types': (list,), 'list_item_values': {'player', 'team'}},
    'requires_params': {'required': False, 'types': (list,)},
}

# -- ETL_CONFIG schema (flat, keys validated directly) --
ETL_CONFIG_SCHEMA = {
    'retention_seasons': {'required': True, 'types': (int,)},
    'calendar_flip_month': {'required': True, 'types': (int,)},
    'calendar_flip_day': {'required': True, 'types': (int,)},
    'max_retry_attempts': {'required': True, 'types': (int,)},
    'retry_delay_seconds': {'required': True, 'types': (int,)},
    'auto_resume': {'required': True, 'types': (bool,)},
}

# -- SEASON_TYPES entry schema --
SEASON_TYPES_SCHEMA = {
    'name': {'required': True, 'types': (str,)},
    'param': {'required': True, 'types': (str,)},
    'min_season': {'required': True, 'types': (str, type(None))},
}


# ============================================================================
# GENERIC VALIDATION ENGINE
# ============================================================================

def _validate_entry(
    entry: Dict[str, Any],
    schema: Dict[str, Dict[str, Any]],
    prefix: str,
) -> List[str]:
    """Validate a single config entry against a schema.

    Returns a list of error strings (empty = valid).
    """
    errors: List[str] = []

    for attr_name, constraint in schema.items():
        required = constraint.get('required', True)

        if attr_name not in entry:
            if required:
                errors.append(f"{prefix}: missing required attribute '{attr_name}'")
            continue

        value = entry[attr_name]
        allowed_types = constraint.get('types')

        if allowed_types and not isinstance(value, allowed_types):
            type_names = ', '.join(t.__name__ for t in allowed_types)
            errors.append(
                f"{prefix}: '{attr_name}' expected ({type_names}), "
                f"got {type(value).__name__}"
            )
            continue

        allowed_values = constraint.get('allowed_values')
        if allowed_values is not None and value not in allowed_values:
            errors.append(
                f"{prefix}: '{attr_name}' value {value!r} not in {allowed_values}"
            )

        list_item_values = constraint.get('list_item_values')
        if list_item_values and isinstance(value, list):
            for item in value:
                if item not in list_item_values:
                    errors.append(
                        f"{prefix}: '{attr_name}' contains invalid "
                        f"item {item!r}, expected one of {list_item_values}"
                    )

    return errors


def _validate_dict_config(
    data: Dict[str, Any],
    schema: Dict[str, Dict[str, Any]],
    config_name: str,
) -> List[str]:
    """Validate every entry in a dict-of-dicts config."""
    errors: List[str] = []
    for key, entry in data.items():
        prefix = f"{config_name}['{key}']"
        if not isinstance(entry, dict):
            errors.append(f"{prefix}: expected dict, got {type(entry).__name__}")
            continue
        errors.extend(_validate_entry(entry, schema, prefix))
    return errors


def _validate_flat_config(
    data: Dict[str, Any],
    schema: Dict[str, Dict[str, Any]],
    config_name: str,
) -> List[str]:
    """Validate a flat config dict (keys are the attributes)."""
    return _validate_entry(data, schema, config_name)


# ============================================================================
# CROSS-REFERENCE VALIDATORS
# ============================================================================

def _validate_pg_types(db_columns: Dict[str, Dict]) -> List[str]:
    """Validate that all DB_COLUMNS types are valid PostgreSQL types."""
    errors = []
    for col_name, meta in db_columns.items():
        col_type = meta.get('type', '')
        base = col_type.split('(')[0].upper()
        if base not in VALID_PG_TYPES:
            errors.append(f"DB_COLUMNS['{col_name}']: unknown type '{col_type}'")
    return errors


def _validate_source_structure(db_columns: Dict[str, Dict]) -> List[str]:
    """Validate the nested sources structure in DB_COLUMNS."""
    errors = []
    for col_name, meta in db_columns.items():
        sources = meta.get('sources')
        if sources is None:
            continue

        prefix = f"DB_COLUMNS['{col_name}']"
        if not isinstance(sources, dict):
            errors.append(f"{prefix}: 'sources' must be dict or None")
            continue

        for provider, entities in sources.items():
            if not isinstance(entities, dict):
                errors.append(f"{prefix}: sources['{provider}'] must be dict")
                continue
            for entity_name, source_def in entities.items():
                if entity_name not in VALID_ENTITY_TYPES:
                    errors.append(
                        f"{prefix}: sources.{provider} has "
                        f"invalid entity '{entity_name}'"
                    )
                if not isinstance(source_def, dict):
                    errors.append(
                        f"{prefix}: sources.{provider}.{entity_name} must be dict"
                    )
    return errors


def _validate_endpoint_refs(
    db_columns: Dict[str, Dict],
    endpoints: Dict[str, Dict],
) -> List[str]:
    """Validate that source endpoint references exist in ENDPOINTS."""
    errors = []
    for col_name, meta in db_columns.items():
        sources = meta.get('sources')
        if not sources or not isinstance(sources, dict):
            continue

        prefix = f"DB_COLUMNS['{col_name}']"
        for provider, entities in sources.items():
            if not isinstance(entities, dict):
                continue
            for entity_name, source_def in entities.items():
                if not isinstance(source_def, dict):
                    continue
                ep = (
                    source_def.get('endpoint')
                    or source_def.get('pipeline', {}).get('endpoint')
                )
                if ep and ep not in endpoints:
                    errors.append(
                        f"{prefix}: references unknown endpoint '{ep}'"
                    )
    return errors


def _validate_table_unique_keys(
    tables: Dict[str, Dict],
    db_columns: Dict[str, Dict],
) -> List[str]:
    """Validate that TABLES unique_key columns exist in DB_COLUMNS."""
    errors = []
    for table_name, meta in tables.items():
        for uk_col in meta.get('unique_key', []):
            if uk_col not in db_columns:
                errors.append(
                    f"TABLES['{table_name}']: unique_key references "
                    f"unknown column '{uk_col}'"
                )
    return errors


# ============================================================================
# PUBLIC API
# ============================================================================

def validate_config(
    endpoints: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Validate all ETL configuration at startup.

    Args:
        endpoints: Optional ENDPOINTS dict from the provider config.
                   If supplied, source endpoint references are cross-checked.

    Returns:
        Empty list if all valid.

    Raises:
        RuntimeError: If any validation errors are found.
    """
    from src.etl.config import DB_COLUMNS, TABLES, ETL_CONFIG, ETL_TABLES

    errors: List[str] = []

    # Schema validations
    errors.extend(_validate_dict_config(DB_COLUMNS, DB_COLUMNS_SCHEMA, 'DB_COLUMNS'))
    errors.extend(_validate_dict_config(TABLES, TABLES_SCHEMA, 'TABLES'))
    errors.extend(_validate_flat_config(ETL_CONFIG, ETL_CONFIG_SCHEMA, 'ETL_CONFIG'))

    if endpoints:
        errors.extend(_validate_dict_config(endpoints, ENDPOINTS_SCHEMA, 'ENDPOINTS'))

    # Type and structural validations
    errors.extend(_validate_pg_types(DB_COLUMNS))
    errors.extend(_validate_source_structure(DB_COLUMNS))
    errors.extend(_validate_table_unique_keys(TABLES, DB_COLUMNS))

    # Cross-reference validations
    if endpoints:
        errors.extend(_validate_endpoint_refs(DB_COLUMNS, endpoints))

    if errors:
        for err in errors:
            logger.error('Config validation: %s', err)
        raise RuntimeError(
            f"Config validation failed with {len(errors)} error(s)"
        )

    logger.info(
        'Config validation passed (%d columns, %d tables)',
        len(DB_COLUMNS), len(TABLES),
    )
    return errors
