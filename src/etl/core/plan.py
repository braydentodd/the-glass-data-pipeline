"""
The Glass - Call Group Builder

Transforms column configuration into executable API call groups for any
data provider.  A "call group" is a batch of columns that can be satisfied
by a single API call.

Functions accept provider-specific config (provider_key, endpoints) as
parameters rather than importing from a specific source, keeping this
module source-agnostic.

Column schema and provider source mappings live in the unified config
(src/etl/config.py).
"""

import logging
from typing import Any, Dict, List, Optional

from src.etl.definitions import DB_COLUMNS, TYPE_TRANSFORMS

logger = logging.getLogger(__name__)


# ============================================================================
# INTERNAL HELPERS
# ============================================================================

def _enrich_source(source: Dict[str, Any], col_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Add a default transform to a source based on column type if not already set."""
    enriched = {**source}
    if 'transform' not in enriched and 'pipeline' not in enriched and 'multi_call' not in enriched:
        base_type = col_meta.get('type', '').split('(')[0]
        enriched['transform'] = TYPE_TRANSFORMS.get(base_type, 'safe_int')
    if 'removed_refresh_mode' not in enriched:
        enriched['removed_refresh_mode'] = col_meta.get('removed_refresh_mode', 'null_only')
    return enriched


def _get_provider_source(
    col_meta: Dict[str, Any],
    entity: str,
    provider_key: str,
) -> Optional[Dict[str, Any]]:
    """Extract a provider's source definition for an entity from a column's metadata."""
    provider_sources = (col_meta.get('sources') or {}).get(provider_key)
    if not provider_sources:
        return None
    return provider_sources.get(entity)


# ============================================================================
# ENDPOINT AVAILABILITY
# ============================================================================

def is_endpoint_available(
    endpoint_name: str,
    season: str,
    endpoints: Dict[str, Dict[str, Any]],
) -> bool:
    """Check whether an endpoint has data for the given season."""
    ep = endpoints.get(endpoint_name)
    if not ep:
        return False
    min_season = ep.get('min_season')
    if min_season is None:
        return True
    return season >= min_season


# ============================================================================
# COLUMN QUERIES
# ============================================================================

def get_columns_for_endpoint(
    endpoint_name: str,
    entity: str,
    provider_key: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Find all columns whose provider source maps to the given endpoint.

    Returns ``{col_name: enriched_source_dict}`` with default transforms injected.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_provider_source(col_meta, entity, provider_key)
        if not source:
            continue

        ep = source.get('endpoint')
        if not ep:
            ep = source.get('pipeline', {}).get('endpoint')
        if ep != endpoint_name:
            continue

        if params:
            source_params = source.get('params', {})
            if not all(source_params.get(k) == v for k, v in params.items()):
                continue

        matched[col_name] = _enrich_source(source, col_meta)

    return matched


def get_all_sources_for_entity(
    entity: str,
    provider_key: str,
    endpoints: Dict[str, Dict[str, Any]],
    season: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return every column with a provider source for the given entity.

    If *season* is provided, excludes endpoints not available for that season.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_provider_source(col_meta, entity, provider_key)
        if not source:
            continue

        if season:
            ep = source.get('endpoint') or source.get('pipeline', {}).get('endpoint', '')
            if not is_endpoint_available(ep, season, endpoints):
                continue

        matched[col_name] = _enrich_source(source, col_meta)

    return matched


# ============================================================================
# EXECUTION TIER RESOLUTION
# ============================================================================

def tier_for_endpoint(
    endpoint: str,
    endpoints: Dict[str, Dict[str, Any]],
) -> str:
    """Get the default execution tier for an endpoint."""
    return endpoints.get(endpoint, {}).get('execution_tier', 'league')


def tier_for_source(
    source: Dict[str, Any],
    endpoint: str,
    endpoints: Dict[str, Dict[str, Any]],
) -> str:
    """Resolve execution tier from a source config or the endpoint default."""
    tier = source.get('tier')
    if tier:
        return tier
    pipeline = source.get('pipeline', {})
    if pipeline.get('tier'):
        return pipeline['tier']
    return tier_for_endpoint(endpoint, endpoints)


# ============================================================================
# CALL GROUP BUILDING
# ============================================================================

def build_call_groups(
    entity: str,
    season: str,
    provider_key: str,
    endpoints: Dict[str, Dict[str, Any]],
    scope: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Group all columns for *entity* into API call batches.

    Walks DB_COLUMNS, groups simple/derived columns that share the same
    (endpoint, params) so each batch requires exactly one API call.
    Multi-call, pipeline, and team_call columns get their own entries.

    Args:
        scope: If set, only include columns whose scope list contains
               this value (e.g. ``'entity'`` or ``'stats'``).

    Returns a list of dicts, each with:
        endpoint, params, tier, columns ({col_name: enriched_source})
    """
    simple_groups: Dict[tuple, Dict[str, Dict[str, Any]]] = {}
    special: List[Dict[str, Any]] = []

    for col_name, col_meta in DB_COLUMNS.items():
        if scope and scope not in col_meta.get('scope', []):
            continue

        source = _get_provider_source(col_meta, entity, provider_key)
        if not source:
            continue

        enriched = _enrich_source(source, col_meta)

        ep = enriched.get('endpoint')
        if not ep:
            ep = enriched.get('pipeline', {}).get('endpoint')
        if not ep:
            continue
        if not is_endpoint_available(ep, season, endpoints):
            continue

        if 'multi_call' in enriched or 'pipeline' in enriched:
            special.append({
                'endpoint': ep,
                'params': enriched.get('params', {}),
                'tier': tier_for_source(enriched, ep, endpoints),
                'columns': {col_name: enriched},
            })
        elif enriched.get('tier') == 'team_call':
            special.append({
                'endpoint': ep,
                'params': {},
                'tier': 'team_call',
                'columns': {col_name: enriched},
            })
        else:
            params = enriched.get('params', {})
            key = (ep, frozenset(sorted(params.items())))
            simple_groups.setdefault(key, {})[col_name] = enriched

    groups: List[Dict[str, Any]] = []

    for (ep, frozen_params), cols in simple_groups.items():
        removed_refresh_mode = 'always' if any(
            src.get('removed_refresh_mode') == 'always' for src in cols.values()
        ) else 'null_only'
        groups.append({
            'endpoint': ep,
            'params': dict(frozen_params),
            'tier': tier_for_endpoint(ep, endpoints),
            'columns': cols,
            'removed_refresh_mode': removed_refresh_mode,
        })

    # Merge team_call columns that share the same endpoint into one group
    team_call_merged: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for item in special:
        if item['tier'] == 'team_call':
            team_call_merged.setdefault(item['endpoint'], {}).update(item['columns'])
        else:
            groups.append(item)
    for ep, cols in team_call_merged.items():
        groups.append({
            'endpoint': ep,
            'params': {},
            'tier': 'team_call',
            'columns': cols,
            'removed_refresh_mode': 'null_only',
        })

    return groups
