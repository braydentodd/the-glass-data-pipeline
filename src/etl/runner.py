"""
The Glass - ETL Runner (Orchestrator)

Central orchestrator for the ETL pipeline.  Dispatches API calls based on
endpoint execution tiers (league / team / team_call / player), routes
results through core extraction, and writes to the database.

Usage:
    python -m src.etl.runner                        # current season, Regular
    python -m src.etl.runner --season 2023-24       # specific season
    python -m src.etl.runner --season-type 2         # Playoffs
    python -m src.etl.runner --entity team           # teams only
    python -m src.etl.runner --endpoint leaguedashptstats  # single endpoint
"""

import argparse
import logging
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

# Suppress urllib3 keep-alive pool warnings (harmless with stats.nba.com)
warnings.filterwarnings(
    'ignore',
    message='Failed to return connection to pool',
    module='urllib3',
)

from src.db import db_connection, ensure_schema, quote_col
from src.etl.config import DB_COLUMNS
from src.etl.core.extract import (
    extract_columns_from_result,
    extract_derived_field,
    extract_field,
    get_entity_id_field,
    get_pipeline_columns,
    get_simple_columns,
)
from src.etl.core.load import BulkDatabaseWriter
from src.etl.core.transform import apply_transform, execute_pipeline
from src.etl.nba_api.client import (
    build_endpoint_params,
    create_api_call,
    load_endpoint_class,
    with_retry,
)
from src.etl.nba_api.config import (
    API_CONFIG,
    API_FIELD_NAMES,
    DB_SCHEMA,
    ENDPOINTS,
    PARALLEL_CONFIG,
    RETRY_CONFIG,
    SEASON_CONFIG,
    SEASON_TYPES,
    SOURCES,
    TABLES_CONFIG,
    TEAM_IDS,
    get_columns_for_endpoint,
    get_table_name,
    is_endpoint_available,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# ETL CONTEXT
# ============================================================================

class ETLContext:
    """Mutable state container passed through the entire pipeline."""

    def __init__(self) -> None:
        self.failed_endpoints: List[Dict[str, Any]] = []
        self.api_result_cache: Dict[str, Any] = {}

    def add_failed_endpoint(self, info: Dict[str, Any]) -> None:
        self.failed_endpoints.append(info)

    def cache_key(self, endpoint: str, params: Dict[str, Any], team_id: Optional[int] = None) -> str:
        """Deterministic cache key from endpoint name + sorted params."""
        parts = [endpoint]
        if team_id is not None:
            parts.append(f'team={team_id}')
        for k in sorted(params):
            if not k.startswith('_'):
                parts.append(f'{k}={params[k]}')
        return '|'.join(parts)


# ============================================================================
# PARALLEL EXECUTOR
# ============================================================================

class ParallelAPIExecutor:
    """ThreadPoolExecutor wrapper with tier-aware worker counts."""

    def __init__(self, tier: str = 'league') -> None:
        self.max_workers = PARALLEL_CONFIG.get(tier, {}).get('max_workers', 1)

    def execute_batch(
        self,
        tasks: List[Dict[str, Any]],
        description: str = 'Batch',
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Run *tasks* in parallel and return (results, errors).

        Each task dict must have 'id' and 'func' (a zero-arg callable).
        """
        results: Dict[str, Any] = {}
        errors: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_map = {
                pool.submit(self._run_task, t): t for t in tasks
            }
            for future in as_completed(future_map):
                task = future_map[future]
                try:
                    results[task['id']] = future.result()
                except Exception as exc:
                    errors.append({'task_id': task['id'], 'error': str(exc)})
                    if 'timeout' not in str(exc).lower():
                        logger.warning('Task %s failed: %s', task['id'], exc)

        return results, errors

    @staticmethod
    def _run_task(task: Dict[str, Any]) -> Any:
        delay = API_CONFIG.get('rate_limit_delay', 0.6)
        time.sleep(delay)
        return task['func']()


# ============================================================================
# ENDPOINT EXECUTION — DISPATCHER
# ============================================================================

def execute_endpoint(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: Literal['player', 'team'] = 'player',
    season_type: int = 1,
    season_type_name: str = 'Regular Season',
) -> int:
    """Universal dispatcher — routes to the correct execution strategy.

    Returns the number of rows written.
    """
    table = get_table_name(entity, 'stats')

    # Collect columns from SOURCES for this endpoint + entity + params
    cols = get_columns_for_endpoint(endpoint_name, entity, params=endpoint_params)
    simple = get_simple_columns(cols)
    pipelines = get_pipeline_columns(cols)

    if not simple and not pipelines:
        return 0

    # Detect execution tier from column source configs
    tier = _resolve_tier(cols, endpoint_name)

    param_label = ' '.join(f'{k}={v}' for k, v in sorted(endpoint_params.items()) if not k.startswith('_'))
    logger.info('Processing %s %s %s %s [%s]', season, season_type_name, endpoint_name, entity, param_label)

    # Dispatch based on tier
    if tier == 'team_call':
        return _execute_team_call(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, cols,
        )
    elif tier == 'team':
        return _execute_per_team(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, simple, pipelines,
        )
    else:
        # league (default) — single API call returns all entities
        return _execute_league_wide(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, simple, pipelines,
        )


def _resolve_tier(cols: Dict[str, Dict[str, Any]], endpoint_name: str) -> str:
    """Determine execution tier from column sources or endpoint config."""
    for source in cols.values():
        tier = source.get('execution_tier')
        if tier:
            return tier
    ep = ENDPOINTS.get(endpoint_name, {})
    return ep.get('execution_tier', 'league')


# ============================================================================
# LEAGUE-WIDE (single API call)
# ============================================================================

def _execute_league_wide(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: str,
    table: str,
    season_type: int,
    season_type_name: str,
    simple_cols: Dict[str, Dict[str, Any]],
    pipeline_cols: Dict[str, Dict[str, Any]],
) -> int:
    """One API call returns all entities — extract, transform, write."""
    EndpointClass = load_endpoint_class(endpoint_name)
    if EndpointClass is None:
        return 0

    params = build_endpoint_params(endpoint_name, season, season_type_name, entity, endpoint_params)
    api_call = create_api_call(EndpointClass, params, endpoint_name=endpoint_name)

    try:
        result = with_retry(api_call)
    except Exception as exc:
        logger.error('League-wide %s failed: %s', endpoint_name, exc)
        ctx.add_failed_endpoint({'endpoint': endpoint_name, 'params': endpoint_params, 'error': str(exc)})
        return 0

    entity_id_field = get_entity_id_field(entity)

    # Extract simple (direct-field) columns
    rows = extract_columns_from_result(
        result, simple_cols, entity, entity_id_field,
    )

    # Write to DB
    return _write_rows(table, rows, entity, season, season_type)


# ============================================================================
# PER-TEAM (30 API calls per endpoint, aggregate across teams)
# ============================================================================

def _execute_per_team(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: str,
    table: str,
    season_type: int,
    season_type_name: str,
    simple_cols: Dict[str, Dict[str, Any]],
    pipeline_cols: Dict[str, Dict[str, Any]],
) -> int:
    """30 per-team API calls, aggregate results across teams."""
    EndpointClass = load_endpoint_class(endpoint_name)
    if EndpointClass is None:
        return 0

    base_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, endpoint_params)
    entity_id_field = get_entity_id_field(entity)
    delay = API_CONFIG.get('rate_limit_delay', 0.6)
    consecutive_failures = 0
    threshold = API_CONFIG.get('max_consecutive_failures', 5)

    aggregated: Dict[int, Dict[str, int]] = {}  # {entity_id: {col: sum_value}}

    team_ids = list(TEAM_IDS.values())
    for idx, team_id in enumerate(team_ids):
        params = {**base_params, 'team_id': team_id}
        api_call = create_api_call(EndpointClass, params, endpoint_name=endpoint_name)

        try:
            result = with_retry(api_call)
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            logger.warning('Team %d failed for %s: %s', team_id, endpoint_name, exc)
            if consecutive_failures >= threshold:
                logger.error('Aborting %s after %d consecutive failures', endpoint_name, consecutive_failures)
                break
            continue

        # Extract and sum across teams
        team_rows = extract_columns_from_result(result, simple_cols, entity, entity_id_field)
        for eid, vals in team_rows.items():
            if eid not in aggregated:
                aggregated[eid] = {col: 0 for col in simple_cols}
            for col, val in vals.items():
                if val is not None and isinstance(val, (int, float)):
                    aggregated[eid][col] += val

        if delay > 0 and idx < len(team_ids) - 1:
            time.sleep(delay)

    return _write_rows(table, aggregated, entity, season, season_type)


# ============================================================================
# TEAM-CALL (30 per-team calls returning player-level data keyed by VS_PLAYER_ID)
# ============================================================================

def _execute_team_call(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: str,
    table: str,
    season_type: int,
    season_type_name: str,
    cols: Dict[str, Dict[str, Any]],
) -> int:
    """Per-team calls that return player rows (e.g. teamplayeronoffsummary).

    Aggregates across teams for traded players using source-level
    ``aggregation`` setting (``sum`` or ``minute_weighted``).
    """
    # Discover result_set and player_id_field from first column's source config
    first_source = next(iter(cols.values()))
    result_set_name = first_source.get('result_set', 'PlayersOffCourtTeamPlayerOnOffSummary')
    player_id_field = first_source.get('player_id_field', 'VS_PLAYER_ID')
    minutes_field = 'MIN'

    EndpointClass = load_endpoint_class(endpoint_name)
    if EndpointClass is None:
        return 0

    base_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, endpoint_params)
    delay = API_CONFIG.get('rate_limit_delay', 0.6)
    consecutive_failures = 0
    threshold = API_CONFIG.get('max_consecutive_failures', 5)

    # Phase 1: Collect raw row dicts per player across all 30 teams
    player_team_rows: Dict[int, List[Dict[str, Any]]] = {}

    team_ids = list(TEAM_IDS.values())
    for idx, team_id in enumerate(team_ids):
        params = {**base_params, 'team_id': team_id}
        api_call = create_api_call(EndpointClass, params, endpoint_name=endpoint_name)

        try:
            result = with_retry(api_call)
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            logger.warning('Team %d failed for %s: %s', team_id, endpoint_name, exc)
            if consecutive_failures >= threshold:
                logger.error('Aborting %s after %d failures', endpoint_name, consecutive_failures)
                break
            continue

        # Find the target result set
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
                    player_team_rows.setdefault(pid, []).append(dict(zip(headers, row)))

        if delay > 0 and idx < len(team_ids) - 1:
            time.sleep(delay)

    if not player_team_rows:
        return 0

    # Phase 2: Aggregate across teams per player
    rows: Dict[int, Dict[str, Any]] = {}

    for player_id, team_rows in player_team_rows.items():
        total_minutes = sum(float(r.get(minutes_field) or 0) for r in team_rows)
        values: Dict[str, Any] = {}

        for col_name, source in cols.items():
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

    return _write_rows(table, rows, entity, season, season_type)


# ============================================================================
# DATABASE WRITE HELPER
# ============================================================================

def _write_rows(
    table: str,
    rows: Dict[int, Dict[str, Any]],
    entity: str,
    season: str,
    season_type: int,
) -> int:
    """Write extracted rows to the database via UPDATE.

    Each row updates the matching (entity_id, season, season_type) composite key.
    """
    if not rows:
        return 0

    entity_id_col = 'player_id' if entity == 'player' else 'team_id'

    with db_connection() as conn:
        cursor = conn.cursor()
        updated = 0

        for entity_id, vals in rows.items():
            if not vals:
                continue
            set_clause = ', '.join(f'{quote_col(c)} = %s' for c in vals)
            set_clause += ', updated_at = NOW()'
            values = list(vals.values()) + [entity_id, season, season_type]

            cursor.execute(
                f'UPDATE {table} SET {set_clause} '
                f'WHERE {entity_id_col} = %s AND year = %s AND season_type = %s',
                values,
            )
            if cursor.rowcount > 0:
                updated += 1

        conn.commit()
        return updated


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def run_etl(
    season: Optional[str] = None,
    season_type: int = 1,
    entity: Optional[str] = None,
    endpoint_filter: Optional[str] = None,
) -> None:
    """Top-level ETL entry point.

    Iterates over all endpoints (or one if *endpoint_filter* is given),
    executing each for the specified season + season_type.
    """
    season = season or SEASON_CONFIG['current_season']
    season_type_meta = SEASON_TYPES.get(season_type)
    if not season_type_meta:
        logger.error('Unknown season_type: %d', season_type)
        return
    season_type_name = season_type_meta['name']

    # Ensure schema columns are up to date before any writes
    ensure_schema(DB_SCHEMA, TABLES_CONFIG, DB_COLUMNS)

    ctx = ETLContext()
    entities = [entity] if entity else ['player', 'team']

    # Build endpoint → param-combos list from SOURCES
    endpoint_combos = _collect_endpoint_param_combos(entities)

    if endpoint_filter:
        endpoint_combos = {
            k: v for k, v in endpoint_combos.items() if k == endpoint_filter
        }

    total_written = 0
    for ep_name, combo_list in endpoint_combos.items():
        if not is_endpoint_available(ep_name, season):
            continue
        ep_meta = ENDPOINTS.get(ep_name, {})
        ep_entities = ep_meta.get('entity_types', ['player', 'team'])

        for ent in entities:
            if ent not in ep_entities:
                continue
            for params in combo_list:
                written = execute_endpoint(
                    ctx, ep_name, params, season,
                    entity=ent, season_type=season_type,
                    season_type_name=season_type_name,
                )
                total_written += written

    # Retry failed endpoints once
    if ctx.failed_endpoints:
        logger.info('Retrying %d failed endpoint(s)...', len(ctx.failed_endpoints))
        for info in list(ctx.failed_endpoints):
            execute_endpoint(
                ctx, info['endpoint'], info.get('params', {}), season,
                season_type=season_type, season_type_name=season_type_name,
            )

    logger.info('ETL complete — %d total rows written for %s %s', total_written, season, season_type_name)


def _collect_endpoint_param_combos(
    entities: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """Scan SOURCES to find every unique (endpoint, params) combination needed.

    Returns ``{endpoint_name: [params_dict, ...]}``.
    """
    seen: Dict[str, set] = {}
    result: Dict[str, List[Dict[str, Any]]] = {}

    for _col, entity_map in SOURCES.items():
        for ent in entities:
            source = entity_map.get(ent)
            if not source:
                continue
            # Skip pipeline-only columns (their own endpoint is internal)
            if 'transformation' in source and 'field' not in source:
                continue
            ep = source.get('endpoint')
            if not ep:
                continue
            params = source.get('params', {})
            frozen = tuple(sorted(params.items()))
            seen.setdefault(ep, set())
            if frozen not in seen[ep]:
                seen[ep].add(frozen)
                result.setdefault(ep, []).append(params)

    return result


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description='The Glass - NBA ETL Pipeline')
    parser.add_argument('--season', type=str, default=None, help='Season string, e.g. 2024-25')
    parser.add_argument('--season-type', type=int, default=1, help='1=Regular, 2=Playoffs, 3=PlayIn')
    parser.add_argument('--entity', type=str, default=None, choices=['player', 'team'])
    parser.add_argument('--endpoint', type=str, default=None, help='Run a single endpoint')
    args = parser.parse_args()

    run_etl(
        season=args.season,
        season_type=args.season_type,
        entity=args.entity,
        endpoint_filter=args.endpoint,
    )


if __name__ == '__main__':
    main()
