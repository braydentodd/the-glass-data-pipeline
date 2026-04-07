"""
The Glass - ETL Runner

Orchestrates the ETL pipeline by wiring provider-specific config (NBA API)
to the source-agnostic core modules:

  - core/resolver.py: call group building, column lookups
  - core/extract.py:  field extraction from API responses
  - core/transform.py: type conversion, pipeline engine, aggregation
  - core/load.py:     database writes via upsert

Usage:
    python -m etl.runner                          # current season, Regular
    python -m etl.runner --season 2023-24         # specific season
    python -m etl.runner --season-type 2          # Playoffs
    python -m etl.runner --entity team            # teams only
    python -m etl.runner --endpoint leaguedashptstats
"""

import argparse
import logging
import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

warnings.filterwarnings(
    'ignore',
    message='Failed to return connection to pool',
    module='urllib3',
)

from src.db import db_connection
from src.etl.config import ETL_CONFIG
from src.etl.core.db import ensure_tables
from src.etl.core.extract import (
    extract_columns_from_result,
    extract_raw_rows,
    extract_single_field,
    get_multi_call_columns,
    get_pipeline_columns,
    get_simple_columns,
)
from src.etl.core.load import write_entity_rows
from src.etl.core.progress import (
    complete_run,
    create_run,
    fail_run,
    find_resumable_run,
    get_pending_progress_ids,
    mark_group_completed,
    mark_group_failed,
    mark_group_started,
    register_groups,
    update_run_completed_groups,
)
from src.etl.core.resolver import build_call_groups
from src.etl.core.transform import aggregate_team_rows, execute_pipeline
from src.etl.sources.nba_api.client import (
    build_endpoint_params,
    create_api_call,
    load_endpoint_class,
    with_retry,
)
from src.etl.sources.nba_api.config import (
    API_CONFIG,
    API_FIELD_NAMES,
    DB_SCHEMA,
    ENDPOINTS,
    SEASON_CONFIG,
    SEASON_TYPES,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

PROVIDER_KEY = 'nba'


# ============================================================================
# EXECUTION CONTEXT
# ============================================================================

@dataclass
class ExecutionContext:
    """Bundles everything the execution engine needs from the provider."""

    entity: str
    scope: str
    season: str
    season_type: int
    season_type_name: str
    entity_id_field: str
    db_schema: str
    api_fetcher: Callable
    team_ids: Dict[str, int] = field(default_factory=dict)
    rate_limit_delay: float = 1.2
    max_consecutive_failures: int = 5


# ============================================================================
# SOURCE-SPECIFIC HELPERS
# ============================================================================

def _make_nba_fetcher(season: str, season_type_name: str, entity: str):
    """Create an api_fetcher closure that wraps the NBA API client."""
    def fetch(endpoint: str, extra_params: Dict[str, Any] = None):
        EndpointClass = load_endpoint_class(endpoint)
        if EndpointClass is None:
            return None
        full_params = build_endpoint_params(
            endpoint, season, season_type_name, entity, extra_params or {},
        )
        api_call = create_api_call(
            EndpointClass, full_params, endpoint_name=endpoint,
        )
        return with_retry(api_call)
    return fetch


def _get_team_ids() -> Dict[str, int]:
    """Load team abbr->nba_api_id mapping from the database."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT nba_api_id, abbr FROM {DB_SCHEMA}.teams "
                f"ORDER BY nba_api_id"
            )
            return {row[1]: int(row[0]) for row in cur.fetchall()}


# ============================================================================
# CALL GROUP EXECUTION
# ============================================================================

def _execute_group(
    group: Dict[str, Any],
    ctx: ExecutionContext,
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
        ctx.season, ctx.season_type_name, endpoint, ctx.entity, param_label,
    )

    written = 0

    if tier == 'team_call':
        written += _execute_team_call(endpoint, columns, ctx, failed)
    elif tier in ('team', 'player'):
        for col_name, source in pipelines.items():
            written += _execute_pipeline_column(col_name, source, ctx, failed)
        for col_name, source in multi_call.items():
            written += _execute_multi_call_column(col_name, source, ctx, failed)
    else:
        if simple:
            written += _execute_league_wide(endpoint, params, simple, ctx, failed)
        for col_name, source in multi_call.items():
            written += _execute_multi_call_column(col_name, source, ctx, failed)
        for col_name, source in pipelines.items():
            written += _execute_pipeline_column(col_name, source, ctx, failed)

    return written


def _execute_league_wide(
    endpoint: str,
    params: Dict[str, Any],
    columns: Dict[str, Dict[str, Any]],
    ctx: ExecutionContext,
    failed: List[Dict[str, Any]],
) -> int:
    """One API call returns all entities -- extract, transform, write."""
    try:
        result = ctx.api_fetcher(endpoint, params)
    except Exception as exc:
        logger.error('League-wide %s failed: %s', endpoint, exc)
        failed.append({'endpoint': endpoint, 'params': params, 'error': str(exc)})
        return 0

    if result is None:
        return 0

    rows = extract_columns_from_result(
        result, columns, ctx.entity, ctx.entity_id_field,
    )
    return write_entity_rows(
        ctx.entity, ctx.scope, rows, ctx.season, ctx.season_type, ctx.db_schema,
    )


def _execute_multi_call_column(
    col_name: str,
    source: Dict[str, Any],
    ctx: ExecutionContext,
    failed: List[Dict[str, Any]],
) -> int:
    """Make multiple API calls and sum the field per entity."""
    endpoint = source['endpoint']
    api_field = source['field']
    multi_call_params = source['multi_call']
    result_set = source.get('result_set')

    totals: Dict[int, int] = {}

    for extra_params in multi_call_params:
        try:
            result = ctx.api_fetcher(endpoint, extra_params)
        except Exception as exc:
            logger.warning(
                'Multi-call %s %s failed for params %s: %s',
                endpoint, col_name, extra_params, exc,
            )
            continue

        if result is None:
            continue

        field_vals = extract_single_field(
            result, api_field, ctx.entity_id_field, result_set,
        )
        for eid, val in field_vals.items():
            totals[eid] = totals.get(eid, 0) + val

    if not totals:
        return 0
    rows = {eid: {col_name: val} for eid, val in totals.items()}
    return write_entity_rows(
        ctx.entity, ctx.scope, rows, ctx.season, ctx.season_type, ctx.db_schema,
    )


def _execute_pipeline_column(
    col_name: str,
    source: Dict[str, Any],
    ctx: ExecutionContext,
    failed: List[Dict[str, Any]],
) -> int:
    """Execute a transformation pipeline for a single column."""
    pipeline_config = source['pipeline']

    def pipeline_fetcher(ep, extra_params, tier):
        try:
            return ctx.api_fetcher(ep, extra_params)
        except Exception:
            return {'resultSets': []}

    try:
        result = execute_pipeline(
            pipeline_config, pipeline_fetcher, ctx.entity,
            ctx.season, ctx.season_type_name,
            entity_id_field=ctx.entity_id_field,
        )
    except Exception as exc:
        logger.error('Pipeline %s failed: %s', col_name, exc)
        failed.append({'column': col_name, 'error': str(exc)})
        return 0

    if not result:
        return 0
    rows = {eid: {col_name: val} for eid, val in result.items()}
    return write_entity_rows(
        ctx.entity, ctx.scope, rows, ctx.season, ctx.season_type, ctx.db_schema,
    )


def _execute_team_call(
    endpoint: str,
    columns: Dict[str, Dict[str, Any]],
    ctx: ExecutionContext,
    failed: List[Dict[str, Any]],
) -> int:
    """Per-team calls returning player-level data (e.g. on/off court).

    Aggregates across teams for traded players using per-column
    aggregation setting (sum or minute_weighted).
    """
    first_source = next(iter(columns.values()))
    result_set_name = first_source.get(
        'result_set', 'PlayersOffCourtTeamPlayerOnOffSummary',
    )
    player_id_field = first_source.get('player_id_field', 'VS_PLAYER_ID')
    minutes_field = 'MIN'

    consecutive_failures = 0
    player_team_rows: Dict[int, list] = {}
    team_ids = list(ctx.team_ids.values())

    for idx, team_id in enumerate(team_ids):
        try:
            result = ctx.api_fetcher(endpoint, {'team_id': team_id})
            consecutive_failures = 0
        except Exception as exc:
            consecutive_failures += 1
            logger.warning('Team %d failed for %s: %s', team_id, endpoint, exc)
            if consecutive_failures >= ctx.max_consecutive_failures:
                logger.error(
                    'Aborting %s after %d consecutive failures',
                    endpoint, consecutive_failures,
                )
                break
            continue

        if result is None:
            continue

        new_rows = extract_raw_rows(result, player_id_field, result_set_name)
        for pid, rows_list in new_rows.items():
            player_team_rows.setdefault(pid, []).extend(rows_list)

        if ctx.rate_limit_delay > 0 and idx < len(team_ids) - 1:
            time.sleep(ctx.rate_limit_delay)

    if not player_team_rows:
        return 0

    rows = aggregate_team_rows(player_team_rows, columns, minutes_field)
    return write_entity_rows(
        ctx.entity, ctx.scope, rows, ctx.season, ctx.season_type, ctx.db_schema,
    )


# ============================================================================
# SEASON HELPERS
# ============================================================================

def _get_season_range(current_season: str) -> List[str]:
    """Build the list of seasons covered by the retention window.

    Returns seasons from oldest to newest (e.g. ``['2018-19', ..., '2024-25']``).
    """
    end_year = int(current_season.split('-')[0]) + 1
    count = ETL_CONFIG['retention_seasons']
    seasons: List[str] = []
    for i in range(count):
        y = end_year - count + i
        seasons.append(f"{y}-{str(y + 1)[-2:]}")
    return seasons


# ============================================================================
# PROGRESS / RESUME
# ============================================================================

def _resolve_work(
    conn: Any,
    entity: str,
    season: str,
    season_type: int,
    groups: List[Dict[str, Any]],
    run_type: str,
) -> Tuple[int, List[Tuple[Dict[str, Any], int]]]:
    """Determine the run_id and work items for an entity.

    If auto_resume is enabled and an interrupted run exists for the same
    (season, season_type, entity), resumes from the last pending group.
    Otherwise creates a fresh run and registers all groups.

    Returns (run_id, [(group_dict, progress_id), ...]).
    """
    if ETL_CONFIG['auto_resume']:
        run_id = find_resumable_run(conn, DB_SCHEMA, season, season_type, entity)
        if run_id:
            logger.info('Resuming interrupted run %d for %s', run_id, entity)
            pending = get_pending_progress_ids(conn, DB_SCHEMA, run_id)
            pending_lookup = {(ep, cols): pid for pid, ep, cols in pending}
            work_items: List[Tuple[Dict[str, Any], int]] = []
            for group in groups:
                col_key = ','.join(sorted(group.get('columns', {}).keys())) or None
                key = (group['endpoint'], col_key)
                if key in pending_lookup:
                    work_items.append((group, pending_lookup[key]))
            logger.info('Resuming with %d pending groups', len(work_items))
            return run_id, work_items

    run_id = create_run(
        conn, DB_SCHEMA, run_type, season, season_type, entity, len(groups),
    )
    progress_ids = register_groups(conn, DB_SCHEMA, run_id, groups, entity)
    return run_id, list(zip(groups, progress_ids))


# ============================================================================
# SHARED EXECUTION ENGINE
# ============================================================================

def _run_groups(
    run_type: str,
    scope: str,
    entities: List[str],
    seasons: List[str],
    season_type: int,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
) -> int:
    """Execute call groups for a given scope across entities and seasons.

    Handles progress tracking, resume support, and per-group error isolation.
    """
    total_rows = 0

    for season in seasons:
        for ent in entities:
            groups = build_call_groups(
                ent, season, PROVIDER_KEY, ENDPOINTS, scope=scope,
            )
            if endpoint_filter:
                groups = [g for g in groups if g['endpoint'] == endpoint_filter]
            if not groups:
                continue

            logger.info(
                '%s: %s %s — %d call groups', run_type, ent, season, len(groups),
            )

            ctx = ExecutionContext(
                entity=ent,
                scope=scope,
                season=season,
                season_type=season_type,
                season_type_name=season_type_name,
                entity_id_field=API_FIELD_NAMES['entity_id'][ent],
                db_schema=DB_SCHEMA,
                api_fetcher=_make_nba_fetcher(season, season_type_name, ent),
                team_ids=team_ids,
                rate_limit_delay=API_CONFIG.get('rate_limit_delay', 1.2),
                max_consecutive_failures=API_CONFIG.get('max_consecutive_failures', 5),
            )

            with db_connection() as conn:
                run_id, work_items = _resolve_work(
                    conn, ent, season, season_type, groups, run_type,
                )

                entity_rows = 0
                try:
                    for group, progress_id in work_items:
                        mark_group_started(conn, DB_SCHEMA, progress_id)
                        try:
                            rows = _execute_group(group, ctx, failed)
                            entity_rows += rows
                            mark_group_completed(conn, DB_SCHEMA, progress_id, rows)
                        except Exception as exc:
                            logger.error('Group %s failed: %s', group['endpoint'], exc)
                            mark_group_failed(conn, DB_SCHEMA, progress_id, str(exc))
                            failed.append({
                                'endpoint': group['endpoint'], 'error': str(exc),
                            })

                    total_rows += entity_rows
                    update_run_completed_groups(conn, DB_SCHEMA, run_id)
                    complete_run(conn, DB_SCHEMA, run_id, entity_rows)
                except Exception as exc:
                    fail_run(conn, DB_SCHEMA, run_id, str(exc))
                    raise

    return total_rows


# ============================================================================
# ETL PHASES
# ============================================================================

def _discover_entities(
    entities: List[str],
    season: str,
    season_type: int,
    season_type_name: str,
    team_ids: Dict[str, int],
    failed: List[Dict[str, Any]],
) -> int:
    """Phase 1: Populate entity tables (players, teams) from current season."""
    logger.info('Phase: discover_entities')
    return _run_groups(
        'discover', 'entity', entities, [season],
        season_type, season_type_name, team_ids, None, failed,
    )


def _backfill(
    entities: List[str],
    seasons: List[str],
    season_type: int,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
) -> int:
    """Phase 2: Fill stats for all seasons in the retention window."""
    logger.info('Phase: backfill (%d seasons)', len(seasons))
    return _run_groups(
        'backfill', 'stats', entities, seasons,
        season_type, season_type_name, team_ids, endpoint_filter, failed,
    )


def _update_current(
    entities: List[str],
    season: str,
    season_type: int,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
) -> int:
    """Phase 3: Refresh stats for the current season only."""
    logger.info('Phase: update_current')
    return _run_groups(
        'update', 'stats', entities, [season],
        season_type, season_type_name, team_ids, endpoint_filter, failed,
    )


def _prune_stale(
    entities: List[str],
    oldest_season: str,
    db_schema: str,
) -> int:
    """Phase 4: Delete stats rows older than the retention window,
    then remove orphaned entity rows with no remaining stats."""
    logger.info('Phase: prune_stale (before %s)', oldest_season)
    pruned = 0
    from src.etl.config import TABLES
    with db_connection() as conn:
        with conn.cursor() as cur:
            # Prune old stats rows
            for table_name, meta in TABLES.items():
                if meta['scope'] != 'stats':
                    continue
                qualified = f"{db_schema}.{table_name}"
                cur.execute(
                    f"DELETE FROM {qualified} WHERE season < %s",
                    (oldest_season,),
                )
                count = cur.rowcount
                if count:
                    logger.info('Pruned %d rows from %s', count, qualified)
                    pruned += count

            # Prune orphaned entity rows (no stats remaining)
            for table_name, meta in TABLES.items():
                if meta['scope'] != 'entity':
                    continue
                entity_type = meta['entity']
                if entity_type not in entities:
                    continue
                stats_table = None
                for st_name, st_meta in TABLES.items():
                    if st_meta['scope'] == 'stats' and st_meta['entity'] == entity_type:
                        stats_table = f"{db_schema}.{st_name}"
                        break
                if not stats_table:
                    continue
                entity_qualified = f"{db_schema}.{table_name}"
                cur.execute(
                    f"DELETE FROM {entity_qualified} e "
                    f"WHERE NOT EXISTS ("
                    f"  SELECT 1 FROM {stats_table} s WHERE s.entity_id = e.id"
                    f")",
                )
                count = cur.rowcount
                if count:
                    logger.info('Pruned %d orphaned entities from %s', count, entity_qualified)
                    pruned += count
    return pruned


# ============================================================================
# ORCHESTRATOR
# ============================================================================

VALID_PHASES = {'full', 'discover', 'backfill', 'update', 'prune'}


def run_etl(
    phase: str = 'full',
    entity: str = 'all',
    endpoint_filter: Optional[str] = None,
    season: Optional[str] = None,
    season_type: int = 1,
) -> None:
    """Main ETL entry point.

    Args:
        phase:           Execution phase — 'full', 'discover', 'backfill',
                         'update', or 'prune'.
        entity:          'player', 'team', or 'all'.
        endpoint_filter: If set, only process this one endpoint.
        season:          e.g. '2024-25'.  Defaults to current season.
        season_type:     1=Regular, 2=Playoffs, 3=PlayIn.
    """
    if phase not in VALID_PHASES:
        raise ValueError(f"Invalid phase '{phase}'. Must be one of {VALID_PHASES}")

    season = season or SEASON_CONFIG['current_season']
    st_info = SEASON_TYPES.get(season_type, SEASON_TYPES[1])
    season_type_name = st_info['name']

    logger.info(
        'ETL starting: phase=%s season=%s type=%s entity=%s',
        phase, season, season_type_name, entity,
    )

    ensure_tables(DB_SCHEMA)

    entities = ['player', 'team'] if entity == 'all' else [entity]
    team_ids = _get_team_ids()
    failed: List[Dict[str, Any]] = []
    total_rows = 0

    season_range = _get_season_range(season)
    oldest_season = season_range[0]

    if phase in ('full', 'discover'):
        total_rows += _discover_entities(
            entities, season, season_type, season_type_name, team_ids, failed,
        )

    if phase in ('full', 'backfill'):
        total_rows += _backfill(
            entities, season_range, season_type, season_type_name,
            team_ids, endpoint_filter, failed,
        )

    if phase in ('full', 'update'):
        total_rows += _update_current(
            entities, season, season_type, season_type_name,
            team_ids, endpoint_filter, failed,
        )

    if phase in ('full', 'prune'):
        total_rows += _prune_stale(entities, oldest_season, DB_SCHEMA)

    logger.info('ETL complete: %d total rows written/pruned', total_rows)

    if failed:
        logger.warning('%d failures:', len(failed))
        for f in failed:
            logger.warning('  %s', f)


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description='The Glass - NBA ETL Pipeline')
    parser.add_argument(
        '--phase', type=str, default='full',
        choices=sorted(VALID_PHASES),
        help='ETL phase to run (default: full)',
    )
    parser.add_argument('--season', type=str, default=None)
    parser.add_argument('--season-type', type=int, default=1, choices=[1, 2, 3])
    parser.add_argument('--entity', type=str, default='all', choices=['player', 'team', 'all'])
    parser.add_argument('--endpoint', type=str, default=None)
    args = parser.parse_args()

    run_etl(
        phase=args.phase,
        entity=args.entity,
        endpoint_filter=args.endpoint,
        season=args.season,
        season_type=args.season_type,
    )


if __name__ == '__main__':
    main()
