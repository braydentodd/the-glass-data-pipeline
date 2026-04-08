"""
The Glass - ETL Execution Engine

Executes a single call group against the NBA API, routing to the correct
strategy based on the group's execution tier:

  - league_wide:  one API call returns all entities at once
  - team / player: per-team API calls with aggregation (e.g. on/off court)
  - team_call:    one per-team call returning player-level data

This module is the execution layer.  Orchestration (which groups to run,
in what order, for which seasons) lives in runner.py.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List

from src.etl.core.extract import (
    extract_columns_from_result,
    extract_raw_rows,
    extract_single_field,
    get_multi_call_columns,
    get_pipeline_columns,
    get_simple_columns,
)
from src.etl.core.load import write_entity_rows
from src.etl.core.transform import aggregate_team_rows, execute_pipeline

logger = logging.getLogger(__name__)


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
# EXECUTION STRATEGIES
# ============================================================================

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
    result_set_name = first_source.get('result_set')
    player_id_field = first_source.get('player_id_field')
    minutes_field = first_source.get('minutes_field', 'MIN')

    if not result_set_name or not player_id_field:
        logger.error(
            'team_call columns for %s missing required result_set or player_id_field',
            endpoint,
        )
        return 0

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
# DISPATCHER
# ============================================================================

def execute_group(
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
