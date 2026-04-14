"""
The Glass - Schema-Driven Config Validation Engine

Generic validation functions that check configuration dictionaries against
declarative schemas.  Shared across ETL, publish, and any future modules.

Schemas are co-located with their config files (e.g. DB_COLUMNS_SCHEMA
lives in etl/config.py).  This module provides only the engine — it has
no knowledge of any specific config structure.
"""

from typing import Any, Dict, List


def validate_entry(
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


def validate_dict_config(
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
        errors.extend(validate_entry(entry, schema, prefix))
    return errors


def validate_flat_config(
    data: Dict[str, Any],
    schema: Dict[str, Dict[str, Any]],
    config_name: str,
) -> List[str]:
    """Validate a flat config dict (keys are the attributes)."""
    return validate_entry(data, schema, config_name)

def validate_core_constants() -> List[str]:
    """Validates the core constants exported in src/core/config.py against their schema."""
    from src.core.config import SEASON_TYPE_GROUPS, CORE_CONFIG_SCHEMA
    
    errors: List[str] = []
    if 'SEASON_TYPE_GROUPS' in CORE_CONFIG_SCHEMA:
        errors.extend(validate_entry(SEASON_TYPE_GROUPS, CORE_CONFIG_SCHEMA['SEASON_TYPE_GROUPS'], "SEASON_TYPE_GROUPS"))
        
    return errors
