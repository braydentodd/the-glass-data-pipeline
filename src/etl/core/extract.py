"""
The Glass - ETL Extraction Engine

Source-agnostic field extraction from API responses using config-driven
source mappings.  Reads a provider's SOURCES dict and an API result dict,
and produces DB-ready {column: value} dicts per entity.

This module never calls the API directly — it only interprets the raw
JSON response that the provider client returns.
"""

import logging
from typing import Any, Dict, List, Literal, Optional, Tuple

from src.etl.core.transform import apply_transform

logger = logging.getLogger(__name__)


# ============================================================================
# FIELD EXTRACTION
# ============================================================================

def extract_field(
    row: List[Any],
    headers: List[str],
    source: Dict[str, Any],
) -> Any:
    """Extract and transform a single field from an API result row.

    Args:
        row: A single row from a resultSet's rowSet.
        headers: The column headers for the result set.
        source: Source config dict with 'field', 'transform', and optional 'scale'.

    Returns:
        The transformed value, or None if the field is missing.
    """
    field = source.get('field')
    if not field or field not in headers:
        return None

    raw_value = row[headers.index(field)]

    # Reject complex types (some endpoints return nested objects)
    if isinstance(raw_value, (dict, list)):
        return None

    transform_name = source.get('transform', 'safe_int')
    scale = source.get('scale', 1)

    return apply_transform(raw_value, transform_name, scale)


def extract_derived_field(
    row: List[Any],
    headers: List[str],
    source: Dict[str, Any],
) -> Optional[int]:
    """Extract a derived field (e.g. FGM - FG3M for 2-point FGM).

    If the source has a ``derived.subtract_field`` key, the base field
    value is reduced by the subtraction field value.
    """
    base_value = extract_field(row, headers, source)
    derived = source.get('derived')
    if not derived or base_value is None:
        return base_value

    subtract_field = derived.get('subtract_field')
    if subtract_field and subtract_field in headers:
        subtract_raw = row[headers.index(subtract_field)]
        if subtract_raw is not None:
            try:
                return base_value - round(float(subtract_raw))
            except (ValueError, TypeError):
                pass
    return base_value


# ============================================================================
# BATCH EXTRACTION
# ============================================================================

def extract_columns_from_result(
    api_result: Dict[str, Any],
    columns: Dict[str, Dict[str, Any]],
    entity: Literal['player', 'team'],
    entity_id_field: str,
    result_set_name: Optional[str] = None,
) -> Dict[int, Dict[str, Any]]:
    """Extract all mapped columns from an API result for every entity.

    Args:
        api_result: Raw API JSON with ``resultSets``.
        columns: ``{canonical_col_name: source_config}`` — typically a
                 subset of SOURCES filtered for a specific endpoint.
        entity: 'player' or 'team'.
        entity_id_field: API header name for the entity ID (e.g. 'PLAYER_ID').
        result_set_name: If given, only process this result set.

    Returns:
        ``{entity_id: {col_name: value, ...}, ...}``
    """
    all_entities: Dict[int, Dict[str, Any]] = {}

    for rs in api_result.get('resultSets', []):
        if result_set_name and rs['name'] != result_set_name:
            continue

        headers = rs['headers']
        if entity_id_field not in headers:
            continue

        id_idx = headers.index(entity_id_field)

        for row in rs['rowSet']:
            entity_id = row[id_idx]
            if entity_id is None:
                continue

            values = {}
            for col_name, source in columns.items():
                # Skip columns with transformation pipelines — handled separately
                if 'transformation' in source:
                    continue

                if source.get('derived'):
                    values[col_name] = extract_derived_field(row, headers, source)
                else:
                    values[col_name] = extract_field(row, headers, source)

            all_entities[entity_id] = values

    return all_entities


# ============================================================================
# COLUMN FILTERING
# ============================================================================

def get_simple_columns(
    columns: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Filter to only columns with direct field extraction (no transformation pipeline)."""
    return {
        name: src for name, src in columns.items()
        if 'transformation' not in src
    }


def get_pipeline_columns(
    columns: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Filter to only columns that require a transformation pipeline."""
    return {
        name: src for name, src in columns.items()
        if 'transformation' in src
    }


def get_entity_id_field(entity: str) -> str:
    """Return the standard NBA API header name for an entity's ID."""
    return 'PLAYER_ID' if entity == 'player' else 'TEAM_ID'
