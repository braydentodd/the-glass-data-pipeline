"""
The Glass - NBA Source Resolver

Resolves the NBA provider's configuration into operational instructions.
Maps entity ID fields, filters columns by endpoint, groups columns into
API call batches, and provides team ID lookups.

All query logic that interprets the NBA source mappings lives here --
nba_api/config.py remains pure data.
"""

import logging
from typing import Any, Dict, List, Optional

from src.db import db_connection
from src.etl.definitions import DB_COLUMNS, TYPE_TRANSFORMS
from src.etl.sources.nba_api.config import (
    API_FIELD_NAMES,
    DB_SCHEMA,
    ENDPOINTS,
)

logger = logging.getLogger(__name__)


# ============================================================================
# INTERNAL HELPERS
# ============================================================================

def _enrich_source(source: Dict[str, Any], col_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Add default transform to a source based on column type if not already set."""
    enriched = {**source}
    if 'transform' not in enriched and 'pipeline' not in enriched and 'multi_call' not in enriched:
        base_type = col_meta.get('type', '').split('(')[0]
        enriched['transform'] = TYPE_TRANSFORMS.get(base_type, 'safe_int')
    return enriched


def _get_nba_source(col_meta: Dict[str, Any], entity: str) -> Optional[Dict[str, Any]]:
    """Extract the NBA source for a given entity from a column's metadata."""
    nba_sources = (col_meta.get('sources') or {}).get('nba')
    if not nba_sources:
        return None
    return nba_sources.get(entity)


# ============================================================================
# ENTITY ID RESOLUTION
# ============================================================================

def get_entity_id_field(entity: str) -> str:
    """Return the NBA API header name for a given entity's ID column."""
    return API_FIELD_NAMES['entity_id'][entity]


# ============================================================================
# TEAM ID LOOKUP  (lazy-loaded from database, cached)
# ============================================================================

_team_ids_cache: Optional[Dict[str, int]] = None


def get_team_ids() -> Dict[str, int]:
    """Load team abbr->nba_api_id mapping from the database.

    Cached after first call so only one query per process lifetime.
    """
    global _team_ids_cache
    if _team_ids_cache is None:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT nba_api_id, abbr FROM {DB_SCHEMA}.teams "
                    f"ORDER BY nba_api_id"
                )
                _team_ids_cache = {row[1]: int(row[0]) for row in cur.fetchall()}
    return _team_ids_cache


# ============================================================================
# ENDPOINT AVAILABILITY
# ============================================================================

def is_endpoint_available(endpoint_name: str, season: str) -> bool:
    """Check whether an endpoint has data for the given season."""
    ep = ENDPOINTS.get(endpoint_name)
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
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Find all columns whose NBA source maps to the given endpoint.

    Walks DB_COLUMNS and returns ``{col_name: enriched_source_dict}`` with
    default transforms injected.  Columns with ``multi_call`` or ``pipeline``
    sources are included so the runner can classify them.

    Args:
        endpoint_name: NBA API endpoint (e.g. ``'leaguedashplayerstats'``).
        entity:        ``'player'`` or ``'team'``.
        params:        Optional param filter -- only include columns whose
                       source params are a superset of these.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_nba_source(col_meta, entity)
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
    season: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return every column with an NBA source for the given entity.

    If *season* is provided, excludes endpoints that aren't available
    for that season.
    """
    matched: Dict[str, Dict[str, Any]] = {}

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_nba_source(col_meta, entity)
        if not source:
            continue

        if season:
            ep = source.get('endpoint') or source.get('pipeline', {}).get('endpoint', '')
            if not is_endpoint_available(ep, season):
                continue

        matched[col_name] = _enrich_source(source, col_meta)

    return matched


# ============================================================================
# EXECUTION TIER RESOLUTION
# ============================================================================

def tier_for_endpoint(endpoint: str) -> str:
    """Get the default execution tier for an endpoint."""
    return ENDPOINTS.get(endpoint, {}).get('execution_tier', 'league')


def tier_for_source(source: Dict[str, Any], endpoint: str) -> str:
    """Resolve execution tier from a source config or endpoint default."""
    tier = source.get('tier')
    if tier:
        return tier
    pipeline = source.get('pipeline', {})
    if pipeline.get('tier'):
        return pipeline['tier']
    return tier_for_endpoint(endpoint)


# ============================================================================
# CALL GROUP BUILDING
# ============================================================================

def build_call_groups(
    entity: str,
    season: str,
) -> List[Dict[str, Any]]:
    """Group all columns for *entity* into API call batches.

    Walks DB_COLUMNS, groups simple/derived columns that share the same
    (endpoint, params) so each batch requires exactly one API call.
    Multi-call, pipeline, and team_call columns get their own entries.

    Returns a list of dicts, each with:
        endpoint, params, tier, columns ({col_name: enriched_source})
    """
    simple_groups: Dict[tuple, Dict[str, Dict[str, Any]]] = {}
    special: List[Dict[str, Any]] = []

    for col_name, col_meta in DB_COLUMNS.items():
        source = _get_nba_source(col_meta, entity)
        if not source:
            continue

        enriched = _enrich_source(source, col_meta)

        ep = enriched.get('endpoint')
        if not ep:
            ep = enriched.get('pipeline', {}).get('endpoint')
        if not ep:
            continue
        if not is_endpoint_available(ep, season):
            continue

        if 'multi_call' in enriched or 'pipeline' in enriched:
            special.append({
                'endpoint': ep,
                'params': enriched.get('params', {}),
                'tier': tier_for_source(enriched, ep),
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
        groups.append({
            'endpoint': ep,
            'params': dict(frozen_params),
            'tier': tier_for_endpoint(ep),
            'columns': cols,
        })

    # Merge team_call columns that share the same endpoint
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
        })

    return groups
