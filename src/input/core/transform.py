"""
The Glass - ETL Transform Module

Source-agnostic type converters and transformation pipeline engine.
Type converters turn raw API values into DB-ready values.
The pipeline engine executes multi-step transformations defined in config.
"""

import logging
import time
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Literal, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# TYPE CONVERTERS
# ============================================================================

def safe_int(value: Any, scale: int = 1) -> Optional[int]:
    """Convert value to scaled integer, returning None for unparseable input."""
    if value is None:
        return None
    try:
        return round(float(value) * scale)
    except (ValueError, TypeError):
        return None


def safe_float(value: Any, scale: int = 1) -> Optional[int]:
    """Convert value to scaled float (stored as integer), returning None for unparseable input."""
    if value is None:
        return None
    try:
        return round(float(value) * scale)
    except (ValueError, TypeError):
        return None


def safe_str(value: Any) -> Optional[str]:
    """Safely convert to string, returning None for empty/NaN."""
    if value is None or value == '':
        return None
    try:
        if isinstance(value, float) and value != value:  # NaN check
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def parse_height(height_str: Any) -> Optional[int]:
    """Parse height string (e.g. '6-10') to total inches. Returns None on failure."""
    if not height_str or height_str == '' or height_str == 'None':
        return None
    try:
        s = str(height_str)
        if '-' in s:
            feet, inches = s.split('-')
            return int(feet) * 12 + int(inches)
        return int(float(s)) if s else None
    except (ValueError, AttributeError):
        return None


def parse_birthdate(date_str: Any) -> Optional[date]:
    """Parse birthdate string to date object. Tries multiple formats."""
    if not date_str or date_str == '' or str(date_str).lower() == 'nan':
        return None
    raw = str(date_str).split('.')[0]  # strip fractional seconds
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def format_season(from_year: Any) -> Optional[str]:
    """Convert FROM_YEAR (e.g. 2012) to season string (e.g. '2012-13')."""
    if from_year is None or from_year == '' or str(from_year).lower() == 'nan':
        return None
    try:
        year = int(from_year)
        return f"{year}-{str(year + 1)[-2:]}"
    except (ValueError, TypeError):
        return None


# ============================================================================
# TRANSFORM DISPATCH
# ============================================================================

TRANSFORMS: Dict[str, Callable] = {
    'safe_int': safe_int,
    'safe_float': safe_float,
    'safe_str': safe_str,
    'parse_height': parse_height,
    'parse_birthdate': parse_birthdate,
    'format_season': format_season,
}


def apply_transform(value: Any, transform_name: str, scale: int = 1) -> Any:
    """Apply a named transform to a value.

    For safe_int / safe_float the *scale* argument is forwarded.
    All other transforms ignore it.
    """
    func = TRANSFORMS.get(transform_name)
    if func is None:
        raise ValueError(f"Unknown transform: {transform_name}")
    if transform_name in ('safe_int', 'safe_float'):
        return func(value, scale=scale)
    return func(value)


# ============================================================================
# PIPELINE ENGINE
# ============================================================================
# Executes multi-step transformation pipelines defined declaratively in
# provider config SOURCES entries.  Each pipeline is a list of operations
# applied sequentially to API response data.
#
# Supported operation types:
#   extract            – pull field(s) from a result set, keyed by entity ID
#   multi_league_extract – make multiple API calls and merge results
#   filter             – keep only rows matching filter criteria
#   aggregate          – reduce per-entity lists to single values (sum, avg, etc.)
#   scale              – multiply all values by a constant
#   multiply           – multiply two extracted fields per entity
#   db_copy            – copy a value from another column in the DB

def execute_pipeline(
    pipeline_config: Dict[str, Any],
    api_fetcher: Callable,
    entity: Literal['player', 'team'],
    season: str,
    season_type_name: str,
) -> Dict[int, Any]:
    """Execute a transformation pipeline and return ``{entity_id: value}``.

    Args:
        pipeline_config: The ``transformation`` dict from a SOURCES entry.
        api_fetcher: Callable ``(endpoint, params, execution_tier) -> raw_result``
            that handles the actual API call (provided by the runner).
        entity: 'player' or 'team'.
        season: Season string.
        season_type_name: e.g. 'Regular Season'.

    Returns:
        Dict mapping entity ID to the final computed value.
    """
    endpoint = pipeline_config['endpoint']
    execution_tier = pipeline_config.get('tier', 'league')
    operations = pipeline_config['operations']
    endpoint_params = pipeline_config.get('params', {})

    # Determine if any operation needs API data
    needs_api = any(op.get('type') not in ('db_copy',) for op in operations)

    api_result = None
    if needs_api:
        api_result = api_fetcher(endpoint, endpoint_params, execution_tier)

    data: Dict[int, Any] = {}
    for op in operations:
        op_type = op['type']
        if op_type == 'extract':
            data = _op_extract(api_result, op, entity)
        elif op_type == 'multi_league_extract':
            data = _op_multi_league_extract(op, api_fetcher, endpoint, entity, season, season_type_name)
        elif op_type == 'filter':
            data = _op_filter(data, op)
        elif op_type == 'aggregate':
            data = _op_aggregate(data, op)
        elif op_type == 'scale':
            data = _op_scale(data, op)
        elif op_type == 'multiply':
            data = _op_multiply(data, op)
        elif op_type == 'db_copy':
            data = _op_db_copy(op)
        else:
            raise ValueError(f"Unknown pipeline operation: {op_type}")

    return data


# ============================================================================
# PIPELINE OPERATIONS (private)
# ============================================================================

def _entity_id_field(entity: str) -> str:
    """Return the API header name for the entity's ID column."""
    return 'PLAYER_ID' if entity == 'player' else 'TEAM_ID'


def _op_extract(
    api_result: Dict[str, Any],
    op: Dict[str, Any],
    entity: str,
) -> Dict[int, Any]:
    """Extract a field from a specific result set, keyed by entity ID.

    Supports optional ``filter_field`` / ``filter_values`` to keep only
    matching rows, and ``fields`` (dict) for multi-field extraction.
    """
    target_rs = op.get('result_set')
    id_field = _entity_id_field(entity)

    for rs in api_result.get('resultSets', []):
        if target_rs and rs['name'] != target_rs:
            continue

        headers = rs['headers']
        if id_field not in headers:
            continue

        id_idx = headers.index(id_field)
        rows = rs['rowSet']

        # Optional row-level filter
        filter_field = op.get('filter_field')
        filter_values = op.get('filter_values')

        # Multi-field extraction (for multiply pipelines)
        if 'fields' in op:
            result: Dict[int, Dict[str, Any]] = {}
            field_map = op['fields']  # {alias: api_field}
            for row in rows:
                if filter_field and filter_values:
                    if filter_field in headers and row[headers.index(filter_field)] not in filter_values:
                        continue
                eid = row[id_idx]
                entry = result.setdefault(eid, {alias: [] for alias in field_map})
                for alias, api_field in field_map.items():
                    if api_field in headers:
                        entry[alias].append(row[headers.index(api_field)])
            return result

        # Single-field extraction
        field = op['field']
        if field not in headers:
            return {}
        field_idx = headers.index(field)

        result = {}
        for row in rows:
            if filter_field and filter_values:
                if filter_field in headers and row[headers.index(filter_field)] not in filter_values:
                    continue
            eid = row[id_idx]
            val = row[field_idx]
            if eid in result:
                # Multiple matching rows — accumulate in a list for later aggregation
                existing = result[eid]
                if isinstance(existing, list):
                    existing.append(val)
                else:
                    result[eid] = [existing, val]
            else:
                result[eid] = val
        return result

    return {}


def _op_multi_league_extract(
    op: Dict[str, Any],
    api_fetcher: Callable,
    base_endpoint: str,
    entity: str,
    season: str,
    season_type_name: str,
) -> Dict[int, Any]:
    """Make multiple API calls with different params and sum results per entity."""
    field = op['field']
    result_set = op.get('result_set')
    calls = op['calls']
    id_field = _entity_id_field(entity)

    totals: Dict[int, int] = {}

    for call_params in calls:
        api_result = api_fetcher(base_endpoint, call_params, 'league')
        for rs in api_result.get('resultSets', []):
            if result_set and rs['name'] != result_set:
                continue
            headers = rs['headers']
            if id_field not in headers or field not in headers:
                continue
            id_idx = headers.index(id_field)
            field_idx = headers.index(field)
            for row in rs['rowSet']:
                eid = row[id_idx]
                val = safe_int(row[field_idx])
                if val is not None:
                    totals[eid] = totals.get(eid, 0) + val
            break

    return totals


def _op_filter(data: Dict[int, Any], op: Dict[str, Any]) -> Dict[int, Any]:
    """Keep only entries matching filter criteria."""
    field = op['field']
    values = set(op['values'])
    return {eid: v for eid, v in data.items() if v in values}


def _op_aggregate(data: Dict[int, Any], op: Dict[str, Any]) -> Dict[int, Any]:
    """Reduce list values to a single value per entity."""
    method = op.get('method', 'sum')
    result = {}
    for eid, val in data.items():
        if isinstance(val, list):
            nums = [v for v in val if v is not None]
            if method == 'sum':
                result[eid] = sum(safe_int(v) or 0 for v in nums)
            elif method == 'avg':
                result[eid] = round(sum(float(v) for v in nums) / len(nums)) if nums else None
            else:
                raise ValueError(f"Unknown aggregate method: {method}")
        else:
            result[eid] = safe_int(val) if val is not None else None
    return result


def _op_scale(data: Dict[int, Any], op: Dict[str, Any]) -> Dict[int, Any]:
    """Multiply all values by a constant factor."""
    factor = op['factor']
    return {
        eid: round(float(v) * factor) if v is not None else None
        for eid, v in data.items()
    }


def _op_multiply(data: Dict[int, Any], op: Dict[str, Any]) -> Dict[int, Any]:
    """Multiply two extracted fields per entity. Expects data from multi-field extract."""
    fields = op['fields']
    should_round = op.get('round', True)
    result = {}
    for eid, field_data in data.items():
        if not isinstance(field_data, dict):
            continue
        vals = []
        for f in fields:
            raw = field_data.get(f)
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            vals.append(raw)
        if all(v is not None for v in vals):
            product = float(vals[0]) * float(vals[1])
            result[eid] = round(product) if should_round else product
        else:
            result[eid] = None
    return result


def _op_db_copy(op: Dict[str, Any]) -> Dict[int, Any]:
    """Copy values from another DB column. Caller must populate during load phase."""
    # Returns an empty dict — the load module resolves db_copy at write time
    return {}
