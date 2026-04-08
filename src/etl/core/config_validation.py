"""
The Glass - ETL Config Validation

ETL-specific validation: cross-reference checks, PostgreSQL type validation,
and source structure checks.  Uses the generic validation engine from
``src.core.validate``.

Schemas are co-located with their config files:

  - DB_COLUMNS_SCHEMA, TABLES_SCHEMA, ETL_CONFIG_SCHEMA -> src/etl/config.py
  - ENDPOINTS_SCHEMA, SEASON_TYPES_SCHEMA -> src/etl/sources/nba_api/config.py

Add a new config?  Define a schema dict next to the data, then register
it in ``validate_config()``.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.config_validation import validate_dict_config, validate_flat_config

logger = logging.getLogger(__name__)


VALID_TRANSFORMS = {
    'safe_int', 'safe_float', 'safe_str',
    'parse_height', 'parse_birthdate', 'format_season',
}


# ============================================================================
# CROSS-REFERENCE VALIDATORS
# ============================================================================

def _validate_pg_types(db_columns: Dict[str, Dict]) -> List[str]:
    """Validate that all DB_COLUMNS types are valid PostgreSQL types."""
    from src.etl.definitions import VALID_PG_TYPES

    errors = []
    for col_name, meta in db_columns.items():
        col_type = meta.get('type', '')
        base = col_type.split('(')[0].upper()
        if base not in VALID_PG_TYPES:
            errors.append(f"DB_COLUMNS['{col_name}']: unknown type '{col_type}'")
    return errors


def _validate_source_structure(db_columns: Dict[str, Dict]) -> List[str]:
    """Validate the nested sources structure in DB_COLUMNS."""
    from src.etl.definitions import VALID_ENTITY_TYPES
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
    endpoints_schema: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Validate all ETL configuration at startup.

    Args:
        endpoints:        Optional ENDPOINTS dict from the provider config.
                          If supplied, source endpoint references are cross-checked.
        endpoints_schema: Optional schema dict for validating the endpoints config.
                          Required when *endpoints* is provided.

    Returns:
        Empty list if all valid.

    Raises:
        RuntimeError: If any validation errors are found.
    """
    from src.etl.definitions import (
        DB_COLUMNS, TABLES, ETL_CONFIG,
        DB_COLUMNS_SCHEMA, TABLES_SCHEMA, ETL_CONFIG_SCHEMA,
    )

    errors: List[str] = []

    # Schema validations
    errors.extend(validate_dict_config(DB_COLUMNS, DB_COLUMNS_SCHEMA, 'DB_COLUMNS'))
    errors.extend(validate_dict_config(TABLES, TABLES_SCHEMA, 'TABLES'))
    errors.extend(validate_flat_config(ETL_CONFIG, ETL_CONFIG_SCHEMA, 'ETL_CONFIG'))

    if endpoints and endpoints_schema:
        errors.extend(validate_dict_config(endpoints, endpoints_schema, 'ENDPOINTS'))

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
