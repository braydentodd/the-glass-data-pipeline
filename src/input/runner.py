"""
The Glass - ETL Runner (Orchestrator)

Central orchestrator for the NBA ETL pipeline.  Groups column sources by
(endpoint, params), dispatches API calls based on execution tier
(league / team / team_call / player), extracts data through the core
modules, and writes to the database.

No classes -- all state is passed as function arguments.

Usage:
    python -m input.runner                         # current season, Regular
    python -m input.runner --season 2023-24        # specific season
    python -m input.runner --season-type 2          # Playoffs
    python -m input.runner --entity team            # teams only
    python -m input.runner --endpoint leaguedashptstats
"""

import argparse
import logging
import time
import warnings
from typing import Any, Dict, List, Literal, Optional

warnings.filterwarnings(
    'ignore',
    message='Failed to return connection to pool',
    module='urllib3',
)

from src.db import db_connection, quote_col
from src.input.config import DB_COLUMNS, TYPE_TRANSFORMS
from src.input.core.extract import (
    extract_columns_from_result,
    extract_derived_field,
    extract_field,
    get_multi_call_columns,
    get_pipeline_columns,
    get_simple_columns,
)
from src.input.core.load import bulk_upsert
from src.input.core.transform import apply_transform, execute_pipeline, safe_int
from src.input.core.db import ensure_tables, get_table_name
from src.input.sources.nba_api.client import (
    build_endpoint_params,
    create_api_call,
    load_endpoint_class,
    with_retry,
)
from src.input.sources.nba_api.config import (
    API_CONFIG,
    DB_SCHEMA,
    ENDPOINTS,
    SEASON_CONFIG,
    SEASON_TYPES,
    get_columns_for_endpoint,
    get_entity_id_field,
    get_team_ids,
    is_endpoint_available,
)
from src.input.sources.nba_api.sources import NBA_SOURCES

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# CALL GROUP BUILDING
# ============================================================================

def _build_call_groups(
    entity: str,
    season: str,
) -> List[Dict[str, Any]]:
    """Group all columns for *entity* into API call batches.

    Walks NBA_SOURCES, groups simple/derived columns that share the same
    (endpoint, params) so each batch requires exactly one API call.
    Multi-call, pipeline, and team_call columns get their own entries.

    Returns a list of dicts, each with:
        endpoint, params, tier, columns ({col_name: enriched_source})
    """
    # Key: (endpoint, frozen_params) -> {col_name: source}
    simple_groups: Dict[tuple, Dict[str, Dict[str, Any]]] = {}
    special: List[Dict[str, Any]] = []

    for col_name, sources in NBA_SOURCES.items():
        source = sources.get(entity)
        if not source:
            continue

        # Enrich with default transform
        enriched = {**source}
        if 'transform' not in enriched and 'pipeline' not in enriched and 'multi_call' not in enriched:
            col_meta = DB_COLUMNS.get(col_name, {})
            base_type = col_meta.get('type', '').split('(')[0]
            enriched['transform'] = TYPE_TRANSFORMS.get(base_type, 'safe_int')

        # Determine the endpoint
        ep = enriched.get('endpoint')
        if not ep:
            pipeline = enriched.get('pipeline', {})
            ep = pipeline.get('endpoint')
        if not ep:
            continue
        if not is_endpoint_available(ep, season):
            continue

        # Multi-call, pipeline, and team_call get their own groups
        if 'multi_call' in enriched or 'pipeline' in enriched:
            special.append({
                'endpoint': ep,
                'params': enriched.get('params', {}),
                'tier': _tier_for_source(enriched, ep),
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

    # Build result list
    groups: List[Dict[str, Any]] = []

    for (ep, frozen_params), cols in simple_groups.items():
        groups.append({
            'endpoint': ep,
            'params': dict(frozen_params),
            'tier': _tier_for_endpoint(ep),
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


def _tier_for_source(source: Dict[str, Any], endpoint: str) -> str:
    """Resolve execution tier from a source config or endpoint default."""
    tier = source.get('tier')
    if tier:
        return tier
    pipeline = source.get('pipeline', {})
    if pipeline.get('tier'):
        return pipeline['tier']
    return _tier_for_endpoint(endpoint)


def _tier_for_endpoint(endpoint: str) -> str:
    """Get the default execution tier for an endpoint."""
    return ENDPOINTS.get(endpoint, {}).get('execution_tier', 'league')


# ============================================================================
# EXECUTION DISPATCHER
# ============================================================================

def _execute_group(
    group: Dict[str, Any],
    entity: str,
    season: str,
    season_type: int,
    season_type_name: str,
    failed: List[Dict[str, Any]],
) -> int:
    """Execute a single call group and return rows written."""
    endpoint = group['endpoint']
    params = group['params']
    tier = group['tier']
    columns = group['columns']

    simple = get_simple_columns(columns)
    multi_call = get_multi_call_columns(columns)
    pipelines = get_pipeline_columns(columns)

    param_label = ' '.join(f'{k}={v}' for k, v in sorted(params.items()))
    logger.info(
        'Processing %s %s %s %s [%s]',
        season, season_type_name, endpoint, entity, param_label,
    )

    written = 0

    if tier == 'team_call':
        written += _execute_team_call(
            endpoint, columns, entity, season,
            season_type, season_type_name, failed,
        )
    elif tier in ('team', 'player'):
        # Per-entity endpoints are handled via pipeline engine
        for col_name, source in pipelines.items():
            written += _execute_pipeline_column(
                col_name, source, entity, season,
                season_type, season_type_name, failed,
            )
        for col_name, source in multi_call.items():
            written += _execute_multi_call_column(
                col_name, source, entity, season,
                season_type, season_type_name, failed,
            )
    else:
        # League-wide: one API call covers all simple + derived columns
        if simple:
            written += _execute_league_wide(
                endpoint, params, simple, entity, season,
                season_type, season_type_name, failed,
            )
        for col_name, source in multi_call.items():
            written += _execute_multi_call_column(
                col_name, source, entity, season,
                season_type, season_type_name, failed,
            )
        for col_name, source in pipelines.items():
            written += _execute_pipeline_column(
                col_name, source, entity, season,
                season_type, season_type_name, failed,
            )

    return written


# ============================================================================
# LEAGUE-WIDE  (single API call -> all entities)
# ============================================================================

def _execute_league_wide(
    endpoint: str,
    params: Dict[str, Any],
    columns: Dict[str, Dict[str, Any]],
    entity: str,
    season: str,
    season_type: int,
    season_type_name: str,
    failed: List[Dict[str, Any]],
) -> int:
    """One API call returns all entities -- extract, transform, write."""
    EndpointClass = load_endpoint_class(endpoint)
    if EndpointClass is None:
        return 0

    full_params = build_endpoint_params(
        endpoint, season, season_type_name, entity, params,
    )
    api_call = create_api_call(EndpointClass, full_params, endpoint_name=endpoint)

    try:
        result = with_retry(api_call)
    except Exception as exc:
        logger.error('League-wide %s failed: %s', endpoint, exc)
        failed.append({'endpoint': endpoint, 'params': params, 'error': str(exc)})
        return 0

    id_field = get_entity_id_field(entity)
    rows = extract_columns_from_result(result, columns, entity, id_field)

    return _write_rows(entity, 'stats', rows, season, season_type)


# ============================================================================
# MULTI-CALL  (N API calls with different params, sum per entity)
# ============================================================================

def _execute_multi_call_column(
    col_name: str,
    source: Dict[str, Any],
    entity: str,
    season: str,
    season_type: int,
    season_type_name: str,
    failed: List[Dict[str, Any]],
) -> int:
    """Make multiple API calls and sum the field per entity."""
    endpoint = source['endpoint']
    field = source['field']
    multi_call_params = source['multi_call']
    result_set = source.get('result_set')
    id_field = get_entity_id_field(entity)

    EndpointClass = load_endpoint_class(endpoint)
    if EndpointClass is None:
        return 0

    totals: Dict[int, int] = {}

    for extra_params in multi_call_params:
        full_params = build_endpoint_params(
            endpoint, season, season_type_name, entity, extra_params,
        )
        api_call = create_api_call(EndpointClass, full_params, endpoint_name=endpoint)

        try:
            result = with_retry(api_call)
        except Exception as exc:
            logger.warning(
                'Multi-call %s %s failed for params %s: %s',
                endpoint, col_name, extra_params, exc,
            )
            continue

        for rs in result.get('resultSets', []):
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

    if not totals:
        return 0
    rows = {eid: {col_name: val} for eid, val in totals.items()}
    return _write_rows(entity, 'stats', rows, season, season_type)


# ============================================================================
# PIPELINE  (multi-step transformation via core/transform engine)
# ============================================================================

def _execute_pipeline_column(
    col_name: str,
    source: Dict[str, Any],
    entity: str,
    season: str,
    season_type: int,
    season_type_name: str,
    failed: List[Dict[str, Any]],
) -> int:
    """Execute a transformation pipeline for a single column."""
    pipeline_config = source['pipeline']

    def api_fetcher(ep, extra_params, tier):
        EndpointClass = load_endpoint_class(ep)
        if EndpointClass is None:
            return {'resultSets': []}
        full_params = build_endpoint_params(
            ep, season, season_type_name, entity, extra_params,
        )
        api_call = create_api_call(EndpointClass, full_params, endpoint_name=ep)
        return with_retry(api_call)

    try:
        result = execute_pipeline(
            pipeline_config, api_fetcher, entity, season, season_type_name,
        )
    except Exception as exc:
        logger.error('Pipeline %s failed: %s', col_name, exc)
        failed.append({'column': col_name, 'error': str(exc)})
        return 0

    if not result:
        return 0
    rows = {eid: {col_name: val} for eid, val in result.items()}
    return _write_rows(entity, 'stats', rows, season, season_type)


# ============================================================================
# TEAM-CALL  (30 per-team calls -> aggregate per player)
# ============================================================================

def _execute_team_call(
    endpoint: str,
    columns: Dict[str, Dict[str, Any]],
    entity: str,
    season: str,
    season_type: int,
    season_type_name: str,
    failed: List[Dict[str, Any]],
) -> int:
    """Per-team calls returning player-level data (e.g. on/off court).

    Aggregates across teams for traded players using per-column
    aggregation setting (sum or minute_weighted).
    """
    first_source = next(iter(columns.values()))
    result_set_name = first_source.get('result_set', 'PlayersOffCourtTeamPlayerOnOffSummary')
    player_id_field = first_source.get('player_id_field', 'VS_PLAYER_ID')
    minutes_field = 'MIN'

    EndpointClass = load_endpoint_class(endpoint)
    if EndpointClass is None:
        return 0

    base_params = build_endpoint_params(
        endpoint, season, season_type_name, entity,
    )
    delay = API_CONFIG.get('rate_limit_delay', 0.6)
    consecutive_failures = 0
    threshold = API_CONFIG.get('max_consecutive_failures', 5)

    player_team_rows: Dict[int, List[Dict[str, Any]]] = {}
    team_ids = list(get_team_ids().values())

    for idx, team_id in enumerate(team_ids):
        params = {**base_params, 'team_id': team_id}
        api_call = create_api_call(EndpointClass, params, endpoint_name=endpoint)

        try:
            result = with_retry(api_call)
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            logger.warning('Team %d failed for %s: %s', team_id, endpoint, exc)
            if consecutive_failures >= threshold:
                logger.error(
                    'Aborting %s after %d consecutive failures',
                    endpoint, consecutive_failures,
                )
                break
            continue

        for rs in result.get('resultSets', []):
            if rs['name'] != result_set_name:
                continue
            headers = rs['headers']
            if player_id_field not in headers:
                continue
            pid_idx = headers.index(player_id_field)
            for row in rs['rowSet']:
                pid = row[pid_idx]
                if pid is not None:
                    player_team_rows.setdefault(pid, []).append(
                        dict(zip(headers, row))
                    )

        if delay > 0 and idx < len(team_ids) - 1:
            time.sleep(delay)

    if not player_team_rows:
        return 0

    rows: Dict[int, Dict[str, Any]] = {}

    for player_id, team_rows in player_team_rows.items():
        total_minutes = sum(float(r.get(minutes_field) or 0) for r in team_rows)
        values: Dict[str, Any] = {}

        for col_name, source in columns.items():
            nba_field = source.get('field')
            scale = source.get('scale', 1)
            transform_name = source.get('transform', 'safe_int')
            aggregation = source.get('aggregation', 'sum')

            if aggregation == 'minute_weighted' and total_minutes > 0:
                weighted_sum = 0.0
                for r in team_rows:
                    val = r.get(nba_field)
                    mins = float(r.get(minutes_field) or 0)
                    if val is not None and mins > 0:
                        weighted_sum += float(val) * mins
                raw = weighted_sum / total_minutes
            else:
                raw = sum(float(r.get(nba_field) or 0) for r in team_rows)

            values[col_name] = apply_transform(raw, transform_name, scale)

        rows[player_id] = values

    return _write_rows(entity, 'stats', rows, season, season_type)


# ============================================================================
# DATABASE WRITE
# ============================================================================

def _write_rows(
    entity: str,
    scope: str,
    rows: Dict[int, Dict[str, Any]],
    season: str,
    season_type: int,
) -> int:
    """Write extracted rows to the database via upsert.

    Adds nba_api_id, season, and season_type to each row for the
    conflict key, then delegates to bulk_upsert.
    """
    if not rows:
        return 0

    table = get_table_name(entity, scope, DB_SCHEMA)

    # Determine conflict columns from TABLES config
    from src.input.config import TABLES
    table_name = table.split('.', 1)[1]
    table_meta = TABLES[table_name]
    conflict_columns = table_meta['unique_key']

    # Build tuples: add identity columns to each row
    all_cols: set = set()
    for vals in rows.values():
        all_cols.update(vals.keys())

    # Ensure identity columns are present
    identity_cols = set(conflict_columns) - all_cols
    data_cols = sorted(all_cols)
    columns = list(conflict_columns) + data_cols

    data = []
    for entity_id, vals in rows.items():
        identity_values = []
        for ck in conflict_columns:
            if ck == 'nba_api_id':
                identity_values.append(str(entity_id))
            elif ck == 'season':
                identity_values.append(season)
            elif ck == 'season_type':
                identity_values.append(str(season_type))
            else:
                identity_values.append(None)

        row_values = [vals.get(c) for c in data_cols]
        data.append(tuple(identity_values + row_values))

    with db_connection() as conn:
        return bulk_upsert(conn, table, columns, data, conflict_columns)


# ============================================================================
# ORCHESTRATOR
# ============================================================================

def run_etl(
    entity: str = 'all',
    endpoint_filter: Optional[str] = None,
    season: Optional[str] = None,
    season_type: int = 1,
) -> None:
    """Main ETL entry point.

    Args:
        entity:          'player', 'team', or 'all'.
        endpoint_filter: If set, only process this one endpoint.
        season:          e.g. '2024-25'.  Defaults to current season.
        season_type:     1=Regular, 2=Playoffs, 3=PlayIn.
    """
    season = season or SEASON_CONFIG['current_season']
    st_info = SEASON_TYPES.get(season_type, SEASON_TYPES[1])
    season_type_name = st_info['name']

    logger.info('ETL starting: season=%s type=%s entity=%s', season, season_type_name, entity)

    # Ensure schema + tables exist
    ensure_tables(DB_SCHEMA)

    entities = ['player', 'team'] if entity == 'all' else [entity]
    failed: List[Dict[str, Any]] = []
    total_rows = 0

    for ent in entities:
        groups = _build_call_groups(ent, season)

        # Optionally filter to a single endpoint
        if endpoint_filter:
            groups = [g for g in groups if g['endpoint'] == endpoint_filter]

        logger.info(
            'Entity %s: %d call groups to process', ent, len(groups),
        )

        for group in groups:
            rows = _execute_group(
                group, ent, season, season_type, season_type_name, failed,
            )
            total_rows += rows

    logger.info('ETL complete: %d total rows written', total_rows)

    if failed:
        logger.warning('%d failures:', len(failed))
        for f in failed:
            logger.warning('  %s', f)


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description='The Glass - NBA ETL Pipeline')
    parser.add_argument('--season', type=str, default=None)
    parser.add_argument('--season-type', type=int, default=1, choices=[1, 2, 3])
    parser.add_argument('--entity', type=str, default='all', choices=['player', 'team', 'all'])
    parser.add_argument('--endpoint', type=str, default=None)
    args = parser.parse_args()

    run_etl(
        entity=args.entity,
        endpoint_filter=args.endpoint,
        season=args.season,
        season_type=args.season_type,
    )


if __name__ == '__main__':
    main()
