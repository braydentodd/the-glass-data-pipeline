"""
The Glass - ETL Runner

Source-agnostic orchestrator that wires provider-specific config to the
generic core modules:

  - core/call_planner.py: call group building, column lookups
  - core/extract.py:  field extraction from API responses
  - core/transform.py: type conversion, pipeline engine, aggregation
  - core/load.py:     database writes via upsert

Usage:
    python -m etl.runner --source nba_api                       # full run
    python -m etl.runner --source nba_api --season 2023-24      # specific season
    python -m etl.runner --source nba_api --season-type po       # Playoffs
    python -m etl.runner --source nba_api --entity team         # teams only
    python -m etl.runner --source nba_api --endpoint leaguedashptstats
"""

import argparse
import importlib
import logging
import warnings
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
load_dotenv()

from src.core.db import db_connection, quote_col
from src.etl.definitions import ETL_CONFIG
from src.etl.core.db import ensure_tables, prune_stale
from src.etl.core.config_validation import validate_config
from src.etl.core.executor import ExecutionContext, execute_group
from src.etl.core.load import seed_empty_stats
from src.etl.core.progress_tracker import (
    complete_run,
    fail_run,
    mark_group_completed,
    mark_group_failed,
    mark_group_started,
    resolve_work,
    update_run_completed_groups,
)
from src.etl.core.plan import build_call_groups
from src.etl.definitions import SOURCES, get_source_id_column

warnings.filterwarnings(
    'ignore',
    message='Failed to return connection to pool',
    module='urllib3',
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def _load_source(source_key: str):
    """Dynamically import a source's config and client modules."""
    if source_key not in SOURCES:
        raise ValueError(
            f"Unknown source '{source_key}'. "
            f"Registered sources: {sorted(SOURCES)}"
        )
    config_mod = importlib.import_module(f'src.etl.sources.{source_key}.config')
    client_mod = importlib.import_module(f'src.etl.sources.{source_key}.client')
    return config_mod, client_mod


# ============================================================================
# SOURCE-SPECIFIC HELPERS
# ============================================================================

def _get_team_ids(db_schema: str, source_id_col: str) -> Dict[str, int]:
    """Load team abbr -> source_id mapping from the database."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {quote_col(source_id_col)}, abbr "
                f"FROM {db_schema}.teams "
                f"ORDER BY {quote_col(source_id_col)}"
            )
            return {row[1]: int(row[0]) for row in cur.fetchall()}


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
# SHARED EXECUTION ENGINE
# ============================================================================

def _run_groups(
    run_type: str,
    scope: str,
    entities: List[str],
    seasons: List[str],
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
    *,
    provider_key: str,
    endpoints: dict,
    api_field_names: dict,
    db_schema: str,
    api_config: dict,
    make_fetcher,
) -> int:
    """Execute call groups for a given scope across entities and seasons.

    Handles progress tracking, resume support, and per-group error isolation.
    """
    total_rows = 0

    for season in seasons:
        for ent in entities:
            groups = build_call_groups(
                ent, season, provider_key, endpoints, scope=scope,
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
                entity_id_field=api_field_names['entity_id'][ent],
                db_schema=db_schema,
                api_fetcher=make_fetcher(season, season_type_name, ent),
                team_ids=team_ids,
                rate_limit_delay=api_config.get('rate_limit_delay', 1.2),
                max_consecutive_failures=api_config.get('max_consecutive_failures', 5),
                id_aliases=api_field_names.get('id_aliases', {}),
            )

            with db_connection() as conn:
                run_id, work_items = resolve_work(
                    conn, db_schema, ent, season, season_type, groups, run_type,
                    ETL_CONFIG['auto_resume'],
                )

                entity_rows = 0
                try:
                    for group, progress_id in work_items:
                        mark_group_started(conn, db_schema, progress_id)
                        try:
                            rows = execute_group(group, ctx, failed)
                            entity_rows += rows
                            mark_group_completed(conn, db_schema, progress_id, rows)
                        except Exception as exc:
                            logger.error('Group %s failed: %s', group['endpoint'], exc)
                            mark_group_failed(conn, db_schema, progress_id, str(exc))
                            failed.append({
                                'endpoint': group['endpoint'], 'error': str(exc),
                            })

                    total_rows += entity_rows
                    update_run_completed_groups(conn, db_schema, run_id)
                    complete_run(conn, db_schema, run_id, entity_rows)
                except Exception as exc:
                    fail_run(conn, db_schema, run_id, str(exc))
                    raise

    return total_rows


# ============================================================================
# ETL PHASES
# ============================================================================

def _discover_entities(
    entities: List[str],
    season: str,
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    failed: List[Dict[str, Any]],
    **source_kw,
) -> int:
    """Phase 1: Populate entity tables (players, teams) from current season."""
    logger.info('Phase: discover_entities')
    return _run_groups(
        'discover', 'entity', entities, [season],
        season_type, season_type_name, team_ids, None, failed,
        **source_kw,
    )


def _backfill(
    entities: List[str],
    seasons: List[str],
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
    **source_kw,
) -> int:
    """Phase 2: Fill stats for all seasons in the retention window."""
    logger.info('Phase: backfill (%d seasons)', len(seasons))
    return _run_groups(
        'backfill', 'stats', entities, seasons,
        season_type, season_type_name, team_ids, endpoint_filter, failed,
        **source_kw,
    )


def _update_current(
    entities: List[str],
    season: str,
    season_type: str,
    season_type_name: str,
    team_ids: Dict[str, int],
    endpoint_filter: Optional[str],
    failed: List[Dict[str, Any]],
    **source_kw,
) -> int:
    """Phase 3: Refresh stats for the current season only."""
    logger.info('Phase: update_current')
    return _run_groups(
        'update', 'stats', entities, [season],
        season_type, season_type_name, team_ids, endpoint_filter, failed,
        **source_kw,
    )


# ============================================================================
# ORCHESTRATOR
# ============================================================================

VALID_PHASES = {'full', 'discover', 'backfill', 'update', 'prune'}


def run_etl(
    source: str,
    phase: str = 'full',
    entity: str = 'all',
    endpoint_filter: Optional[str] = None,
    season: Optional[str] = None,
    season_type: str = 'rs',
) -> None:
    """Main ETL entry point.

    Args:
        source:          Registered source key (e.g. ``'nba_api'``).
        phase:           Execution phase — 'full', 'discover', 'backfill',
                         'update', or 'prune'.
        entity:          'player', 'team', or 'all'.
        endpoint_filter: If set, only process this one endpoint.
        season:          e.g. '2024-25'.  Defaults to current season.
        season_type:     'rs'=Regular Season, 'po'=Playoffs, 'pi'=PlayIn.
    """
    if phase not in VALID_PHASES:
        raise ValueError(f"Invalid phase '{phase}'. Must be one of {VALID_PHASES}")

    config_mod, client_mod = _load_source(source)
    source_meta = SOURCES[source]
    league = source_meta['leagues'][0]
    db_schema = league
    source_id_col = get_source_id_column(league)

    season_config = config_mod.SEASON_CONFIG
    season_types = config_mod.SEASON_TYPES
    endpoints = config_mod.ENDPOINTS
    endpoints_schema = config_mod.ENDPOINTS_SCHEMA
    api_config = config_mod.API_CONFIG
    api_field_names = config_mod.API_FIELD_NAMES

    season = season or season_config['current_season']
    st_info = season_types.get(season_type, season_types['rs'])
    season_type_name = st_info['name']

    logger.info(
        'ETL starting: source=%s phase=%s season=%s type=%s entity=%s',
        source, phase, season, season_type_name, entity,
    )

    # Trigger provider-specific validation if defined
    if hasattr(config_mod, 'validate_provider_config'):
        config_mod.validate_provider_config()

    validate_config(endpoints, endpoints_schema)
    ensure_tables(db_schema)

    # provider_key is the league name, matching the keys in DB_COLUMNS sources
    provider_key = league

    source_kw = dict(
        provider_key=provider_key,
        endpoints=endpoints,
        api_field_names=api_field_names,
        db_schema=db_schema,
        api_config=api_config,
        make_fetcher=client_mod.make_fetcher,
    )

    entities = ['team', 'player'] if entity == 'all' else [entity]
    team_ids = _get_team_ids(db_schema, source_id_col)
    failed: List[Dict[str, Any]] = []
    total_rows = 0

    season_range = _get_season_range(season)
    oldest_season = season_range[0]

    if phase in ('full', 'discover'):
        total_rows += _discover_entities(
            entities, season, season_type, season_type_name, team_ids, failed,
            **source_kw,
        )
        # Always seed RS records for new entities — PO/PI records are created
        # only when those season types are explicitly backfilled.
        for ent in entities:
            total_rows += seed_empty_stats(ent, season, 'rs', db_schema)

    if phase in ('full', 'backfill'):
        total_rows += _backfill(
            entities, season_range, season_type, season_type_name,
            team_ids, endpoint_filter, failed,
            **source_kw,
        )

    if phase in ('full', 'update'):
        total_rows += _update_current(
            entities, season, season_type, season_type_name,
            team_ids, endpoint_filter, failed,
            **source_kw,
        )

    if phase in ('full', 'prune'):
        total_rows += prune_stale(entities, oldest_season, db_schema)

    logger.info('ETL complete: %d total rows written/pruned', total_rows)

    if failed:
        logger.warning('%d failures:', len(failed))
        for f in failed:
            logger.warning('  %s', f)


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description='The Glass - ETL Pipeline')
    parser.add_argument(
        '--source', type=str, required=True,
        choices=sorted(SOURCES),
        help='Data source to run (e.g. nba_api)',
    )
    parser.add_argument(
        '--phase', type=str, default='full',
        choices=sorted(VALID_PHASES),
        help='ETL phase to run (default: full)',
    )
    parser.add_argument('--season', type=str, default=None)
    parser.add_argument('--season-type', type=str, default='rs', choices=['rs', 'po', 'pi'])
    parser.add_argument('--entity', type=str, default='all', choices=['player', 'team', 'all'])
    parser.add_argument('--endpoint', type=str, default=None)
    args = parser.parse_args()

    run_etl(
        source=args.source,
        phase=args.phase,
        entity=args.entity,
        endpoint_filter=args.endpoint,
        season=args.season,
        season_type=args.season_type,
    )


if __name__ == '__main__':
    main()
