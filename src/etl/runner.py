"""
The Glass - ETL Runner

Thin orchestrator that wires the NBA API source to the generic execution
engine.  All business logic lives in core modules:

  - core/execute.py: source-agnostic call group execution
  - core/extract.py: field extraction from API responses
  - core/transform.py: type conversion, pipeline engine, aggregation
  - core/load.py: database writes via upsert
  - sources/nba_api/resolver.py: call group building, column lookups

Usage:
    python -m etl.runner                         # current season, Regular
    python -m etl.runner --season 2023-24        # specific season
    python -m etl.runner --season-type 2          # Playoffs
    python -m etl.runner --entity team            # teams only
    python -m etl.runner --endpoint leaguedashptstats
"""

import argparse
import logging
import warnings
from typing import Any, Dict, List, Optional

warnings.filterwarnings(
    'ignore',
    message='Failed to return connection to pool',
    module='urllib3',
)

from src.etl.core.db import ensure_tables
from src.etl.core.execute import ExecutionContext, execute_group
from src.etl.sources.nba_api.client import (
    build_endpoint_params,
    create_api_call,
    load_endpoint_class,
    with_retry,
)
from src.etl.sources.nba_api.config import (
    API_CONFIG,
    DB_SCHEMA,
    SEASON_CONFIG,
    SEASON_TYPES,
)
from src.etl.sources.nba_api.resolver import (
    build_call_groups,
    get_entity_id_field,
    get_team_ids,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# ============================================================================
# SOURCE-SPECIFIC API FETCHER
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

    ensure_tables(DB_SCHEMA)

    entities = ['player', 'team'] if entity == 'all' else [entity]
    failed: List[Dict[str, Any]] = []
    total_rows = 0

    for ent in entities:
        groups = build_call_groups(ent, season)

        if endpoint_filter:
            groups = [g for g in groups if g['endpoint'] == endpoint_filter]

        logger.info('Entity %s: %d call groups to process', ent, len(groups))

        ctx = ExecutionContext(
            entity=ent,
            season=season,
            season_type=season_type,
            season_type_name=season_type_name,
            entity_id_field=get_entity_id_field(ent),
            db_schema=DB_SCHEMA,
            api_fetcher=_make_nba_fetcher(season, season_type_name, ent),
            team_ids=get_team_ids(),
            rate_limit_delay=API_CONFIG.get('rate_limit_delay', 1.2),
            max_consecutive_failures=API_CONFIG.get('max_consecutive_failures', 5),
        )

        for group in groups:
            rows = execute_group(group, ctx, failed)
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
