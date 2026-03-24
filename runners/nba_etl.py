import os
import sys
import time
import warnings
import argparse
import threading
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress urllib3's "trying to put unkeyed connection" warning.
# stats.nba.com occasionally responds with Connection: close even when we
# request keep-alive; urllib3 then can't return the socket to its pool and
# emits this warning.  It's harmless — the connection is simply discarded.
warnings.filterwarnings(
    "ignore",
    message="Failed to return connection to pool",
    module="urllib3",
)
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional, Tuple, Callable, Literal
from io import StringIO
from nba_api.stats.endpoints import (
    commonplayerinfo,
    leaguedashplayerstats, leaguedashteamstats,
)

# Workaround for stats.nba.com rejecting non-browser clients.
# See issue #633: older nba_api versions would open a session with
# minimal headers, and the remote server would close subsequent
# connections.  Release 1.11.4 added a permanent fix, but we apply the
# patch here too so that existing environments (like venv-installed
# 1.10.x) continue to work.
try:
    from nba_api.stats.library import http as _stats_http
    from nba_api.library import http as _base_http

    _NBA_STATS_HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "stats.nba.com",
        "Origin": "https://www.nba.com",
        "Pragma": "no-cache",
        "Referer": "https://www.nba.com/",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    }
    _stats_http.STATS_HEADERS = _NBA_STATS_HEADERS
    _stats_http.NBAStatsHTTP.headers = _NBA_STATS_HEADERS
    # reset any existing session so new headers are picked up
    _stats_http.NBAStatsHTTP._session = None
    _base_http.NBAHTTP._session = None
except ImportError:
    pass

# Configuration data (pure data structures)
from config.nba_etl import (
    NBA_CONFIG, TEAM_IDS,
    DB_SCHEMA, TABLES_CONFIG, DB_COLUMNS, SEASON_TYPE_CONFIG,
    ENDPOINTS_CONFIG,
    PARALLEL_EXECUTION,
    API_CONFIG, RETRY_CONFIG, DB_OPERATIONS
)

# Reusable utilities and helpers
from lib.db import ensure_schema
from lib.nba_etl import (
    infer_execution_tier_from_endpoint,
    get_columns_by_endpoint,
    safe_int, safe_float, safe_str, parse_height, parse_birthdate, format_season,
    get_entity_id_field, get_endpoint_config, is_endpoint_available_for_season,
    with_retry, create_api_call, load_endpoint_class,
    get_primary_key, get_table_name, ENDPOINT_TRACKER_TABLE,
    quote_column, get_db_connection, return_db_connection, db_connection,
    get_season, get_season_year, build_endpoint_params,
    # Schema
    generate_schema_ddl,
    # Param helpers
    extract_filter_params,
    # Column helpers
    _is_column_available_for_season, get_games_column_for_endpoint,
    # Backfill helpers
    get_non_backfilled_player_ids_for_season,
    get_player_ids_for_season,
    get_active_teams,
    get_columns_for_endpoint_params,
    get_backfill_status,
    update_backfill_status,
    get_missing_data_for_retry,
    execute_null_zero_cleanup,
    run_pt_indicator_cascade,
    validate_data_integrity,
    log_missing_data_to_tracker,
    calculate_current_season,
    get_endpoint_processing_order,
    get_endpoint_parameter_combinations,
    ensure_endpoint_tracker_coverage,
    reset_current_season_endpoints,
    reset_historical_endpoints,
    mark_backfill_complete,
    # Transformation engine
    execute_transformation_pipeline,
    # Custom exceptions
    APISessionExhausted, DatabaseConnectionError, ConfigurationError, DataValidationError,
    # Config context manager
    override_nba_config,
    # Connection pool management
    close_connection_pool
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

RATE_LIMIT_DELAY = API_CONFIG['rate_limit_delay']
MAX_WORKERS_LEAGUE = PARALLEL_EXECUTION['league']['max_workers']
MAX_WORKERS_TEAM = PARALLEL_EXECUTION['team']['max_workers']
MAX_WORKERS_PLAYER = PARALLEL_EXECUTION['player']['max_workers']


if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Global verbose mode flag
VERBOSE_MODE = False

def log_endpoint_processing(season: str, season_type_name: str, endpoint: str, 
                           params: Optional[Dict[str, Any]] = None, 
                           columns: Optional[List[str]] = None,
                           scope: Optional[str] = None) -> None:
    """
    Unified logging function for endpoint processing.
    Format: Processing {season} {season-type} {endpoint} [{params}] (columns)
    
    Args:
        season: Season string (e.g., '2024-25')
        season_type_name: Season type name (e.g., 'Regular Season', 'Playoffs')
        endpoint: Endpoint name
        params: Optional parameters dict
        columns: Optional list of column names
        scope: Optional scope (league/team/player)
    """
    param_desc = ""
    if params:
        param_parts = []
        for key, value in sorted(params.items()):
            if not key.startswith('_'):  # Skip internal params
                param_parts.append(f"{key}={value}")
        if param_parts:
            param_desc = f" [{', '.join(param_parts)}]"
    
    columns_str = f" ({', '.join(columns)})" if columns else ""
    
    print(f"Processing {season} {season_type_name} {endpoint}{param_desc}{columns_str}")

def log_verbose_data(entity_id: Any, column: str, api_value: Any, db_value: Any, 
                     season: str, season_type: int) -> None:
    """
    Log API and DB values for a single entity/column/season/season_type combo.
    Only called when VERBOSE_MODE is enabled.
    
    Format: [entity_id] {column}: API={api_value} → DB={db_value} ({season} type={season_type})
    """
    if VERBOSE_MODE:
        print(f"  [{entity_id}] {column}: API={api_value} → DB={db_value} ({season} type={season_type})")

# ============================================================================
# ETL CONTEXT - State Management
# ============================================================================

class ETLContext:
    """
    Context object for ETL execution state.
    
    WHY: Eliminates global state, making code testable and thread-safe.
    Passed through entire call chain to provide failed endpoint management.
    
    Usage:
        ctx = ETLContext()
        run_daily_etl(ctx=ctx)
    """
    
    def __init__(self) -> None:
        self.parallel_executor: Optional[Any] = None
        self.failed_endpoints: List[Dict[str, Any]] = []
        self.api_result_cache: Dict[str, Any] = {}  # Cache API results to avoid duplicate calls
    
    def init_parallel_executor(self, max_workers: Optional[int] = None, endpoint_tier: Optional[str] = None) -> None:
        """Initialize parallel executor with specified max workers."""
        if self.parallel_executor is None:
            self.parallel_executor = ParallelAPIExecutor(
                max_workers=max_workers,
                endpoint_tier=endpoint_tier
            )
    
    def add_failed_endpoint(self, endpoint_info: Dict[str, Any]) -> None:
        """Add failed endpoint to retry queue."""
        self.failed_endpoints.append(endpoint_info)

class ParallelAPIExecutor:
    def __init__(self, max_workers: Optional[int] = None, endpoint_tier: Optional[int] = None) -> None:
        # Auto-select worker count based on tier
        if max_workers is None and endpoint_tier:
            if endpoint_tier == 'league':
                max_workers = MAX_WORKERS_LEAGUE
            elif endpoint_tier == 'team':
                max_workers = MAX_WORKERS_TEAM
            elif endpoint_tier == 'player':
                max_workers = MAX_WORKERS_PLAYER
            else:
                max_workers = MAX_WORKERS_PLAYER  # Default to cautious
        
        self.max_workers = max_workers or MAX_WORKERS_PLAYER
        self.endpoint_tier = endpoint_tier
        self.results = {}
        self.errors = []
        
    def execute_batch(self, tasks: List[Dict[str, Any]], description: str = "Batch", progress_callback: Optional[Callable] = None) -> Tuple[Dict, List, List]:
        """
        Execute a batch of API calls with tier-appropriate strategy.
        
        TIER 1 (league): All at once, max workers (10)
        TIER 2 (team): All at once, high workers (10)
        TIER 3 (player): Batched with cooldowns (100 per batch, 30s cooldown)
        
        Args:
            tasks: List of dicts with 'id', 'func', 'description', 'max_retries'
            description: Overall batch description for logging
            progress_callback: Optional function called after each task completes
            
        Returns:
            Tuple of (results_dict, errors_list, failed_task_ids)
        """
        
        results = {}
        errors = []
        failed_ids = []
        
        # NOTE: Player tier (TIER 3) should NOT use this class - use subprocesses instead!
        # This executor only handles TIER 1 (league) and TIER 2 (team) endpoints
        # Execute all tasks at once with parallelism
        results, errors, failed_ids = self._execute_task_batch(tasks, progress_callback)
        
        if errors:
            print(f"  {len(errors)} tasks failed out of {len(tasks)}")
            
        return results, errors, failed_ids
    
    def _execute_task_batch(self, tasks: List[Dict[str, Any]], progress_callback: Optional[Callable] = None) -> Tuple[Dict, List, List]:
        """Execute a single batch of tasks in parallel (internal helper)."""
        results = {}
        errors = []
        failed_ids = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._execute_with_retry, task): task
                for task in tasks
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                task_id = task['id']
                
                try:
                    result = future.result()
                    results[task_id] = result
                    
                    if progress_callback:
                        progress_callback(1)
                    
                except Exception as e:
                    errors.append({'task_id': task_id, 'error': str(e)})
                    failed_ids.append(task_id)
                    # Only log non-timeout errors (timeouts are expected, will retry)
                    if "timeout" not in str(e).lower():
                        logger.warning(f"Task {task_id} failed: {e}")
                    
                    if progress_callback:
                        progress_callback(1)
        
        return results, errors, failed_ids
    
    def _execute_with_retry(self, task: Dict[str, Any]) -> Any:
        """Execute a single task with retry logic."""
        func = task['func']
        max_retries = task.get('max_retries', RETRY_CONFIG['max_retries'])
        timeout = task.get('timeout', API_CONFIG['timeout_default'])
        
        for attempt in range(1, max_retries + 1):
            try:
                # Acquire rate limit token
                time.sleep(API_CONFIG['rate_limit_delay'])
                
                # Execute the API call
                result = func(timeout)
                return result
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = attempt * (RETRY_CONFIG['backoff_base'] // API_CONFIG['backoff_divisor'])  # Exponential: 2s, 4s, 6s with backoff_base=10
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed after {max_retries} attempts: {str(e)}")
        
        raise Exception(f"Task returned None after {max_retries} attempts")


class BulkDatabaseWriter:
    """Optimized bulk database writer using PostgreSQL COPY."""
    
    def __init__(self, conn: Any, batch_size: Optional[int] = None) -> None:
        """
        Args:
            conn: psycopg2 connection
            batch_size: Number of rows per batch (from config if not specified)
        """
        self.conn = conn
        self.batch_size = batch_size or DB_OPERATIONS['bulk_insert_batch_size']
        
    def bulk_upsert(
        self,
        table: str,
        columns: List[str],
        data: List[tuple],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> None:
        """
        Perform bulk UPSERT using execute_values with ON CONFLICT.
        Faster than individual inserts but slower than COPY.
        
        Args:
            table: Table name
            columns: List of column names
            data: List of tuples (one per row)
            conflict_columns: Columns for conflict detection (e.g., ['player_id', 'year'])
            update_columns: Columns to update on conflict (None = all non-conflict columns)
        """
        if not data:
            return 0
            
        cursor = self.conn.cursor()
        
        # Determine update columns
        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]
        
        # Build UPSERT statement with quoted column names
        cols_str = ', '.join([quote_column(c) for c in columns])
        conflict_str = ', '.join([quote_column(c) for c in conflict_columns])
        update_str = ', '.join([f"{quote_column(c)} = EXCLUDED.{quote_column(c)}" for c in update_columns])
        
        query = f"""
            INSERT INTO {table} ({cols_str})
            VALUES %s
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}, updated_at = NOW()
        """
        
        # Execute in batches
        total_rows = len(data)
        inserted = 0
        
        for i in range(0, total_rows, self.batch_size):
            batch = data[i:i + self.batch_size]
            try:
                execute_values(cursor, query, batch, page_size=self.batch_size)
                inserted += len(batch)
                
                if i > 0 and i % (self.batch_size * 10) == 0:
                    print(f"  Batch progress: {inserted}/{total_rows} rows")
                    
            except Exception as e:
                logger.error(f"Batch failed at row {i}: {e}")
                # Try to continue with next batch
                self.conn.rollback()
                continue
        
        self.conn.commit()
        return inserted
    
    def bulk_copy(self, table: str, columns: List[str], data: List[tuple]) -> None:
        """
        Ultra-fast bulk insert using PostgreSQL COPY.
        Note: Does not handle conflicts - use for initial loads only.
        
        Args:
            table: Table name
            columns: List of column names
            data: List of tuples (one per row)
        """
        if not data:
            return 0
            
        cursor = self.conn.cursor()
        
        # Create CSV buffer
        buffer = StringIO()
        for row in data:
            # Convert None to \N (PostgreSQL NULL marker)
            csv_row = '\t'.join([str(v) if v is not None else '\\N' for v in row])
            buffer.write(csv_row + '\n')
        
        # Rewind buffer
        buffer.seek(0)
        
        # Execute COPY
        try:
            cursor.copy_from(buffer, table, columns=columns, null='\\N')
            self.conn.commit()
            return len(data)
        except Exception as e:
            print(f"  COPY failed: {str(e)}")
            self.conn.rollback()
            raise



# ============================================================================
# ENDPOINT EXECUTION (Auto-dispatches to league-wide or per-team)
# ============================================================================

def execute_endpoint(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: Literal['player', 'team'] = 'player',
    table: Optional[str] = None,
    season_type: int = 1,
    season_type_name: str = 'Regular Season',
    description: Optional[str] = None,
    suppress_logs: bool = False,
    player_ids: Optional[List[int]] = None
) -> None:
    """
    Universal config-driven endpoint executor - auto-dispatches based on config.
    
    Automatically routes to appropriate execution strategy:
    - League-wide: 1 API call returns all entities -> _execute_league_wide_endpoint()
    - Per-team: 30 API calls (one per team), aggregated -> _execute_per_team_endpoint()
    - Per-player: Handled elsewhere via subprocesses
    
    The underscore-prefixed functions (_execute_*) are internal - don't call directly.
    """
    # Default to stats table for entity if not specified
    if table is None:
        table = get_table_name(entity, 'stats')
    
    if description is None:
        description = f"{endpoint_name}"
    
    # Append measure type to description if present (e.g., "leaguedashptstats.Possessions")
    # Check params in priority order
    param_value = (endpoint_params.get('pt_measure_type') or 
                   endpoint_params.get('measure_type_detailed_defense') or 
                   endpoint_params.get('defense_category'))
    if param_value:
        description = f"{endpoint_name} ({param_value})"
    
    if not suppress_logs:
        print(f"Fetching {description} - {season_type_name}...")
    
    # Get columns from config, filtered by parameter type if provided
    # Use helper to extract only params that exist in endpoint_params
    filter_kwargs = extract_filter_params(endpoint_params)
    cols = get_columns_by_endpoint(endpoint_name, entity, table=table, **filter_kwargs)
    if not cols:
        # No direct extraction columns - this endpoint uses transformations only
        # It should be processed via update_transformation_columns() instead
        return 0
    
    # CHECK: Does this endpoint require special execution routing?
    # Look for execution_tier in any column's source config
    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
    needs_team_call_for_players = any(
        col.get(source_key, {}).get('execution_tier') == 'team_call'
        for col in cols.values()
    )
    needs_team_iteration = any(
        col.get(source_key, {}).get('execution_tier') == 'team'
        for col in cols.values()
    )

    # Pass column sources to parameter builder for season-type-specific overrides
    col_sources = [col.get(source_key) for col in cols.values() if col.get(source_key)]
    endpoint_params = build_endpoint_params(
        endpoint_name, season, season_type_name, entity,
        endpoint_params, col_sources=col_sources
    )

    if needs_team_call_for_players:
        # TEAM-CALL-FOR-PLAYERS: 30 per-team calls, player rows aggregated by VS_PLAYER_ID
        # e.g. teamplayeronoffsummary — off-court team ratings keyed to individual players
        return _execute_team_call_for_players_endpoint(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols, player_ids
        )
    elif needs_team_iteration:
        # PER-TEAM EXECUTION: Loop through all 30 teams and aggregate results
        return _execute_per_team_endpoint(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols, player_ids
        )
    else:
        # LEAGUE-WIDE EXECUTION: Single API call returns all entities
        return _execute_league_wide_endpoint(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols, player_ids
        )


def _execute_league_wide_endpoint(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: str,
    table: str,
    season_type: int,
    season_type_name: str,
    description: str,
    cols: Dict[str, Dict],
    player_ids: Optional[List[int]] = None
) -> None:
    """Execute league-wide endpoint (1 API call returns all entities)."""
    
    # Both tables now store year as season format VARCHAR:
    # - player_season_stats: stores season string like '2024-25'
    # - team_season_stats: stores season string like '2024-25'
    year_value = season  # Use full season string for both: '2024-25'
    
    # Track consecutive failures for automatic restart
    FAILURE_THRESHOLD = API_CONFIG.get('api_failure_threshold', 3)
    RESTART_ENABLED = API_CONFIG.get('api_restart_enabled', True)
    
    # Check if we have a failure counter in context, create if not
    if not hasattr(ctx, '_league_endpoint_failures'):
        ctx._league_endpoint_failures = 0
    
    with db_connection() as conn:
        cursor = conn.cursor()
        
        EndpointClass = load_endpoint_class(endpoint_name)
        if EndpointClass is None:
            return 0
        
        try:
            # Parameters are already built by execute_endpoint before dispatch;
            # endpoint_params here is the fully resolved dict — use directly.
            api_call = create_api_call(
                EndpointClass,
                endpoint_params,
                endpoint_name=endpoint_name
            )
            result = api_call()
            
            # Success - reset failure counter
            ctx._league_endpoint_failures = 0
            
            # Handle multiple result sets
            all_records = []
            for rs in result['resultSets']:
                headers = rs['headers']
                for row in rs['rowSet']:
                    entity_id = row[0]  # PLAYER_ID or TEAM_ID
                    
                    # Extract each stat from API response using config
                    values = []
                    for stat_name, stat_cfg in cols.items():
                        source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
                        source = stat_cfg.get(source_key, {})
                        nba_field = source.get('field')
                        scale = source.get('scale', 1)
                        transform_name = source.get('transform', 'safe_int')
                        
                        if nba_field not in headers:
                            raw_value = None
                        else:
                            raw_value = row[headers.index(nba_field)]
                            
                            # Handle nested structures (dicts, lists) - convert to None
                            # Some endpoints return complex data types that can't be directly stored
                            if isinstance(raw_value, (dict, list)):
                                raw_value = None
                        
                        if transform_name == 'safe_int':
                            value = safe_int(raw_value, scale)
                        elif transform_name == 'safe_float':
                            value = safe_float(raw_value, scale)
                        else:
                            value = safe_int(raw_value, scale)
                        
                        # Final validation: ensure value is primitive (not dict/list)
                        # This catches any edge cases where transformation didn't handle complex types
                        if isinstance(value, (dict, list)):
                            value = None
                        
                        # Verbose logging: API value → DB value
                        log_verbose_data(entity_id, stat_name, raw_value, value, season, season_type)
                            
                        values.append(value)
                    
                    values.extend([entity_id, year_value, season_type])
                    all_records.append(tuple(values))
            
            # Filter to specific player_ids if provided (for targeted backfill)
            if player_ids is not None and entity == 'player':
                all_records = [rec for rec in all_records if rec[-3] in player_ids]
            
            # Bulk update (requires base stats to exist first)
            if all_records:
                entity_id_col = 'player_id' if entity == 'player' else 'team_id'
                set_clause = ', '.join([f"{quote_column(col)} = %s" for col in cols.keys()])
                
                updated = 0
                for record in all_records:
                    cursor.execute(f"""
                        UPDATE {table}
                        SET {set_clause}, updated_at = NOW()
                        WHERE {entity_id_col} = %s 
                        AND year = %s AND season_type = %s
                    """, record)
                    
                    if cursor.rowcount > 0:
                        updated += 1
                
                conn.commit()
                cursor.close()
                return updated
            else:
                conn.commit()
                cursor.close()
                return 0
            
        except Exception as e:
            logger.error(f"Failed {description}: {str(e)}")
            
            # Increment failure counter
            ctx._league_endpoint_failures += 1
            
            # Check if we should trigger automatic restart
            if RESTART_ENABLED and ctx._league_endpoint_failures >= FAILURE_THRESHOLD:
                # Commit any pending work before restart
                try:
                    conn.commit()
                except:
                    pass
                finally:
                    try:
                        cursor.close()
                    except:
                        pass
                
                _trigger_automatic_restart(
                    reason="Session exhaustion in league-wide endpoint",
                    progress_msg=f"Failed {ctx._league_endpoint_failures} consecutive API calls to {endpoint_name}",
                    threshold=FAILURE_THRESHOLD,
                    failures=ctx._league_endpoint_failures
                )
            
            conn.rollback()
            
            # Add to retry queue for end-of-ETL retry
            ctx.add_failed_endpoint({
                'function': '_execute_league_wide_endpoint',
                'args': (ctx, endpoint_name, endpoint_params, season, entity, table, season_type, season_type_name, description, cols)
            })
            print(f"  {description} queued for retry at end of ETL")
            
            # Close connections if restart didn't happen
            try:
                cursor.close()
            except:
                pass
            
            return 0


def _execute_per_team_endpoint(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: str,
    table: str,
    season_type: int,
    season_type_name: str,
    description: str,
    cols: Dict[str, Dict],
    player_ids: Optional[List[int]] = None
) -> None:
    """
    Execute per-team endpoint (30 API calls, one per team).
    Aggregates results across all teams for each entity.
    
    Example: teamdashptshots requires team_id parameter
    - Call endpoint 30 times (once per team)
    - Aggregate player stats across teams (for traded players)
    - Write aggregated results to database

    """
    # Both player and team tables use season format
    year_value = season  # Use full season string for both: '2024-25'

    EndpointClass = load_endpoint_class(endpoint_name)
    if EndpointClass is None:
        return 0
    
    base_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, endpoint_params)
    
    # Add additional parameters commonly used in per-team endpoints
    base_params.update({
        'league_id': API_CONFIG['league_id'],
        'last_n_games': API_CONFIG['last_n_games'],
        'month': API_CONFIG['month'],
        'opponent_team_id': API_CONFIG['opponent_team_id'],
        'period': API_CONFIG['period']
    })
    
    # Get result_set name and filters from first column's config
    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
    first_col_source = next(iter(cols.values())).get(source_key, {})
    
    # Determine result set based on endpoint and config
    result_set_name = first_col_source.get('result_set')
    if not result_set_name:
        # Fallback to 'General' if not specified in DB_COLUMNS
        result_set_name = 'General'
    
    # Check if ANY column needs result set subtraction for shot distance filtering
    # If result_set_subtract is specified: calculate close shots as ALL - 10ft+
    # IMPORTANT: Check ALL columns, not just first one, since batch may mix close/total columns
    subtract_result_set = None
    for col_cfg in cols.values():
        col_source = col_cfg.get(source_key, {})
        if col_source.get('result_set_subtract'):
            subtract_result_set = col_source.get('result_set_subtract')
            break  # Found one, that's enough
    
    # Aggregate results across all teams: {entity_id: {stat_name: value}}
    entity_stats = {}
    entity_stats_subtract = {} if subtract_result_set else None
    entity_games = {}  # Track games played for per-game conversion
    
    # Check if we need to track games for conversion
    convert_per_game = endpoint_params.get('_convert_per_game', False)
    games_field = endpoint_params.get('_games_field', 'GP') if convert_per_game else None
    
    # Get team IDs
    team_ids = list(TEAM_IDS.values())
    
    # Get per-call delay: endpoint-specific override > global rate_limit_delay
    endpoint_config = get_endpoint_config(endpoint_name)
    per_call_delay = API_CONFIG.get('rate_limit_delay', 1.0)
    if endpoint_config and 'retry_config' in endpoint_config:
        per_call_delay = endpoint_config['retry_config'].get('per_call_delay', per_call_delay)
    
    # Track consecutive failures for automatic restart
    FAILURE_THRESHOLD = API_CONFIG.get('api_failure_threshold', 3)
    RESTART_ENABLED = API_CONFIG.get('api_restart_enabled', True)
    consecutive_failures = 0
    
    # Loop through teams
    for idx, team_id in enumerate(team_ids):
        
        try:
            # Call API with team_id using rate limiting and retry protection
            # Remove internal flags from params before calling API
            params = {**base_params, 'team_id': team_id}
            # Strip internal flags (prefixed with _) before API call
            api_params = {k: v for k, v in params.items() if not k.startswith('_')}
            
            try:
                api_call = create_api_call(
                    EndpointClass,
                    api_params,
                    endpoint_name=endpoint_name
                )
                result = api_call()
            except TypeError as te:
                # More detailed error for debugging
                logger.warning(f"Failed team {team_id}: {te} | Endpoint: {EndpointClass.__name__} | Params: {list(api_params.keys())}")
                continue
            except Exception as api_error:
                error_msg = str(api_error)
                raise  # Re-raise to hit outer except block
            
            # Process main result set and optionally subtract result set
            for result_set_to_process, stats_dict in [(result_set_name, entity_stats), (subtract_result_set, entity_stats_subtract)]:
                if result_set_to_process is None:
                    continue
                    
                # Find the correct result set
                for rs in result['resultSets']:
                    if rs['name'] != result_set_to_process:
                        continue
                    
                    headers = rs['headers']
                    entity_id_field = get_entity_id_field(entity)
                    
                    if entity_id_field not in headers:
                        continue
                    
                    entity_id_idx = headers.index(entity_id_field)
                    
                    # Get games index if needed for conversion
                    games_idx = None
                    if games_field and games_field in headers:
                        games_idx = headers.index(games_field)
                    
                    # Process each row
                    for row in rs['rowSet']:
                        entity_id = row[entity_id_idx]
                        
                        # Filter by defender_distance_category (contested vs open)
                        passes_filter = True
                        if 'defender_distance_category' in first_col_source:
                            # Defender distance filtering not needed - handled via filter_values in transformation config
                            pass
                        
                        if not passes_filter:
                            continue
                        
                        # Initialize entity if first time seeing it
                        if entity_id not in stats_dict:
                            stats_dict[entity_id] = {col: 0 for col in cols.keys()}
                        
                        # Track games for this entity (for conversion)
                        if games_idx is not None and stats_dict == entity_stats:
                            # Only track from main result set, not subtract set
                            games = safe_int(row[games_idx], 1)
                            if entity_id not in entity_games:
                                entity_games[entity_id] = games
                        
                        # Extract and aggregate stats
                        for col_name, col_cfg in cols.items():
                            source = col_cfg.get(source_key, {})
                            field_name = source.get('field')
                            transform_name = source.get('transform', 'safe_int')
                            scale = source.get('scale', 1)
                            
                            if field_name and field_name in headers:
                                raw_value = row[headers.index(field_name)]
                                
                                # Handle nested structures (dicts, lists) - convert to None
                                if isinstance(raw_value, (dict, list)):
                                    raw_value = None
                                
                                if transform_name == 'safe_int':
                                    value = safe_int(raw_value, scale)
                                elif transform_name == 'safe_float':
                                    value = safe_float(raw_value, scale)
                                else:
                                    value = safe_int(raw_value, scale)
                                
                                # AGGREGATE: Sum across teams and defender distance rows
                                # Double-check value is actually a number before aggregating
                                if value is not None and isinstance(value, (int, float)):
                                    stats_dict[entity_id][col_name] += value
                                elif value is not None:
                                    logger.warning(f"Column '{col_name}' got non-numeric value: {type(value)}, skipping")
                    
                    break  # Found the result set, no need to check others
            
            # Reset failure counter on successful API call
            consecutive_failures = 0
            
            # Enforce delay between calls to avoid rate limiting (skip delay after last team)
            if per_call_delay > 0 and idx < len(team_ids) - 1:
                time.sleep(per_call_delay)
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            consecutive_failures += 1
            
            # Get team name for better context
            team_name = "Unknown"
            if team_id in TEAM_IDS.values():
                for abbr, tid in TEAM_IDS.items():
                    if tid == team_id:
                        team_name = abbr
                        break
            
            print(f"  Failed team {team_id} ({team_name}): {error_type}: {error_msg[:100]} | Endpoint: {endpoint_name} | Season: {season}")
            
            # AUTO-RESTART: If hit failure threshold, trigger subprocess restart
            if RESTART_ENABLED and consecutive_failures >= FAILURE_THRESHOLD:
                progress_msg = f"Progress: {idx}/{len(team_ids)} teams processed in {endpoint_name} for {season}"
                _trigger_automatic_restart(f"Session exhaustion in {endpoint_name}", progress_msg, FAILURE_THRESHOLD, consecutive_failures)
            
            continue
    
    # CONVERSION: If using PerGame mode, multiply stat values by games played
    if convert_per_game and entity_stats and entity_games:
        print(f"  Converting PerGame to totals using {games_field}...")
        for entity_id in entity_stats:
            if entity_id in entity_games:
                games = entity_games[entity_id]
                if games > 0:
                    # Multiply all stat values by games played to convert per-game to totals
                    for col_name in cols.keys():
                        entity_stats[entity_id][col_name] = int(entity_stats[entity_id][col_name] * games)
    
    # If result_set_subtract was specified, calculate final values: ALL - 10ft+
    # This gives us close (<10ft) shots by subtracting far (10ft+) from all shots
    # IMPORTANT: Only subtract for columns that have result_set_subtract configured
    if subtract_result_set and entity_stats_subtract:
        for entity_id in entity_stats:
            for col_name, col_cfg in cols.items():
                # Check if THIS specific column needs subtraction
                col_source = col_cfg.get(source_key, {})
                if col_source.get('result_set_subtract'):
                    # This column needs subtraction (close shots)
                    all_shots = entity_stats[entity_id].get(col_name, 0)
                    far_shots = entity_stats_subtract.get(entity_id, {}).get(col_name, 0)
                    entity_stats[entity_id][col_name] = max(0, all_shots - far_shots)
                # Else: column stays as-is (total shots from main result set)
    
    # Filter to specific player_ids if provided (for targeted backfill)
    if player_ids is not None and entity == 'player':
        entity_stats = {eid: stats for eid, stats in entity_stats.items() if eid in player_ids}
    
    with db_connection() as conn:
        cursor = conn.cursor()
        
        try:
            set_clause = ', '.join([f"{quote_column(col)} = %s" for col in cols.keys()])
            
            updated = 0
            for entity_id, stats in entity_stats.items():
                # Validate and clean values before inserting
                values = []
                for col in cols.keys():
                    val = stats[col]
                    # Ensure no dict/list objects slip through
                    if isinstance(val, (dict, list)):
                        logger.warning(f"Column '{col}' has dict/list value for entity {entity_id}, converting to None")
                        val = None
                    
                    # Verbose logging: show aggregated value being written to DB
                    log_verbose_data(entity_id, col, val, val, season, season_type)
                    
                    values.append(val)
                
                values.extend([entity_id, year_value, season_type])
                
                cursor.execute(f"""
                    UPDATE {table}
                    SET {set_clause}, updated_at = NOW()
                    WHERE {'player_id' if entity == 'player' else 'team_id'} = %s 
                    AND year = %s AND season_type = %s
                """, tuple(values))
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            return {'updated': updated, 'data_found': bool(entity_stats)}
                
        except Exception as e:
            logger.error(f"Failed {description} (aggregation): {str(e)}")
            conn.rollback()
            return {'updated': 0, 'data_found': False}
        finally:
            cursor.close()


def _execute_team_call_for_players_endpoint(
    ctx: ETLContext,
    endpoint_name: str,
    endpoint_params: Dict[str, Any],
    season: str,
    entity: str,
    table: str,
    season_type: int,
    season_type_name: str,
    description: str,
    cols: Dict[str, Dict],
    player_ids: Optional[List[int]] = None
) -> int:
    """
    Execute a per-team endpoint that returns player-level data.

    Use case: teamplayeronoffsummary — requires team_id but rows are keyed by
    VS_PLAYER_ID (not TEAM_ID).  Calls all 30 teams and **aggregates** across
    teams for traded players:

    * ``aggregation: 'sum'``              – GP, MIN are summed.
    * ``aggregation: 'minute_weighted'``  – OFF_RATING, DEF_RATING are averaged
      weighted by minutes, the industry-standard approach (Basketball Reference,
      Cleaning the Glass).  Approximation vs. possessions is negligible.

    Column player_source must include:
        'result_set'      — name of the result set (e.g. PlayersOffCourtTeamPlayerOnOffSummary)
        'player_id_field' — header name of the player ID column (e.g. VS_PLAYER_ID)
        'field'           — the stat field to extract
        'aggregation'     — 'sum' or 'minute_weighted'
    """
    year_value = season
    source_key = f'{entity}_source'

    # Determine result set name and player ID field from first column's config
    first_source = next(
        (col.get(source_key, {}) for col in cols.values() if col.get(source_key)), {}
    )
    result_set_name = first_source.get('result_set', 'PlayersOffCourtTeamPlayerOnOffSummary')
    player_id_field = first_source.get('player_id_field', 'VS_PLAYER_ID')

    EndpointClass = load_endpoint_class(endpoint_name)
    if EndpointClass is None:
        return 0

    # Rate limiting & failure tracking (mirrors _execute_per_team_endpoint)
    endpoint_config = get_endpoint_config(endpoint_name)
    per_call_delay = API_CONFIG.get('rate_limit_delay', 0.6)
    if endpoint_config and 'retry_config' in endpoint_config:
        per_call_delay = endpoint_config['retry_config'].get('per_call_delay', per_call_delay)

    FAILURE_THRESHOLD = API_CONFIG.get('api_failure_threshold', 3)
    consecutive_failures = 0

    # ------------------------------------------------------------------
    # Phase 1: Collect ALL rows per player across all 30 teams.
    # Traded players will have one row per team they played on.
    # {player_id: [row_dict, row_dict, ...]}
    # ------------------------------------------------------------------
    player_team_rows: Dict[int, List[Dict[str, Any]]] = {}

    team_ids = list(TEAM_IDS.values())
    for idx, team_id in enumerate(team_ids):
        # Look up team abbreviation for logging
        team_abbr = next((abbr for abbr, tid in TEAM_IDS.items() if tid == team_id), '???')

        params = {**endpoint_params, 'team_id': team_id}
        api_call = create_api_call(EndpointClass, params, endpoint_name=endpoint_name)

        try:
            result = api_call()
            consecutive_failures = 0  # reset on success

            result_sets = result.get('resultSets', [])
            target_rs = next((rs for rs in result_sets if rs['name'] == result_set_name), None)
            if target_rs is None:
                continue

            headers = target_rs['headers']
            rows = target_rs['rowSet']

            if player_id_field not in headers:
                continue

            pid_idx = headers.index(player_id_field)

            for row in rows:
                player_id = row[pid_idx]
                if not player_id:
                    continue
                player_team_rows.setdefault(player_id, []).append(dict(zip(headers, row)))

        except Exception as e:
            consecutive_failures += 1
            logger.warning(
                f"Failed {endpoint_name} for team {team_id} ({team_abbr}): "
                f"{type(e).__name__}: {str(e)[:100]} "
                f"[{consecutive_failures}/{FAILURE_THRESHOLD} consecutive failures]"
            )
            if consecutive_failures >= FAILURE_THRESHOLD:
                logger.error(
                    f"Aborting {endpoint_name}: {consecutive_failures} consecutive failures. "
                    f"Collected {len(player_team_rows)} players from {idx + 1}/{len(team_ids)} teams."
                )
                break
            continue

        # Rate-limit between team calls (skip after last team)
        if per_call_delay > 0 and idx < len(team_ids) - 1:
            time.sleep(per_call_delay)

    if not player_team_rows:
        logger.warning(f"No player data collected from {endpoint_name} for {season} {season_type_name}")
        return 0

    # ------------------------------------------------------------------
    # Phase 2: Aggregate across teams per player.
    #   - 'sum' columns (GP, MIN): simple addition
    #   - 'minute_weighted' columns (OFF_RATING, DEF_RATING):
    #       weighted_avg = Σ(rating_i × min_i) / Σ(min_i)
    # ------------------------------------------------------------------
    # Pre-compute which NBA field holds minutes (used for weighting).
    # The 'MIN' field is always present in teamplayeronoffsummary results.
    MINUTES_FIELD = 'MIN'

    with db_connection() as conn:
        cursor = conn.cursor()

        all_records = []
        for player_id, team_rows in player_team_rows.items():
            # Compute total minutes once (used as weight denominator)
            total_minutes = sum((r.get(MINUTES_FIELD) or 0) for r in team_rows)

            values = []
            for stat_name, stat_cfg in cols.items():
                source = stat_cfg.get(source_key, {})
                nba_field = source.get('field')
                scale = source.get('scale', 1)
                transform_name = source.get('transform', 'safe_int')
                aggregation = source.get('aggregation', 'sum')

                if aggregation == 'minute_weighted' and total_minutes > 0:
                    # Weighted average: Σ(value_i × minutes_i) / Σ(minutes_i)
                    weighted_sum = 0.0
                    for r in team_rows:
                        val = r.get(nba_field)
                        mins = r.get(MINUTES_FIELD) or 0
                        if val is not None and mins > 0:
                            weighted_sum += float(val) * float(mins)
                    raw_value = weighted_sum / total_minutes
                else:
                    # Default: sum across teams (GP, MIN, or fallback)
                    raw_value = sum((r.get(nba_field) or 0) for r in team_rows)

                if transform_name == 'safe_int':
                    value = safe_int(raw_value, scale)
                elif transform_name == 'safe_float':
                    value = safe_float(raw_value, scale)
                else:
                    value = safe_int(raw_value, scale)

                values.append(value)

            values.extend([player_id, year_value, season_type])
            all_records.append(tuple(values))

        # Filter to specific player_ids if provided (backfill mode)
        if player_ids is not None:
            all_records = [rec for rec in all_records if rec[-3] in player_ids]

        if not all_records:
            conn.commit()
            cursor.close()
            return 0

        entity_id_col = 'player_id' if entity == 'player' else 'team_id'
        set_clause = ', '.join([f"{quote_column(col)} = %s" for col in cols.keys()])

        updated = 0
        for record in all_records:
            cursor.execute(f"""
                UPDATE {table}
                SET {set_clause}, updated_at = NOW()
                WHERE {entity_id_col} = %s
                AND year = %s AND season_type = %s
            """, record)
            if cursor.rowcount > 0:
                updated += 1

        conn.commit()
        cursor.close()
        return updated




def apply_transformation(
    ctx: ETLContext,
    column_name: str,
    transform: Dict[str, Any],
    season: str,
    entity: Literal['player', 'team'] = 'player',
    table: Optional[str] = None,
    season_type: int = 1,
    season_type_name: str = 'Regular Season',
    source_config: Optional[Dict] = None,
    player_ids: Optional[List[int]] = None
) -> Dict[int, Any]:
    """
    Execute transformation using unified pipeline engine.
    
    All transformations are now config-driven pipelines of operations.
    Legacy transform types have been removed - all configs must use 'pipeline' format.
    """
    # Merge endpoint and execution_tier from source_config if not in transform
    if source_config:
        if 'endpoint' in source_config and 'endpoint' not in transform:
            transform = dict(transform)
            transform['endpoint'] = source_config['endpoint']
        if 'execution_tier' in source_config and 'execution_tier' not in transform:
            if not isinstance(transform, dict) or transform is source_config.get('transformation'):
                transform = dict(transform)
            transform['execution_tier'] = source_config['execution_tier']
    
    transform_type = transform.get('type', 'pipeline')
    
    # Only pipeline type is supported now
    if transform_type == 'pipeline':
        result = execute_transformation_pipeline(
            ctx, transform, season, entity, season_type, season_type_name, player_ids=player_ids
        )
        return result
    
    # Unsupported legacy types
    else:
        raise ValueError(
            f"Transformation type '{transform_type}' is no longer supported. "
            f"Please migrate to 'pipeline' format. See UNIFIED_ENGINE.md for migration guide."
        )




def _trigger_automatic_restart(reason: str, progress_msg: str, threshold: int, failures: int) -> None:
    """
    Trigger automatic subprocess restart to get fresh API session.
    Raises APISessionExhausted to be caught by main loop.
    """
    logger.warning("[RESTART] Automatic restart triggered")
    logger.warning(f"  Reason: {reason}")
    logger.warning(f"  Consecutive failures: {failures} (threshold: {threshold})")
    logger.warning(f"  Progress: {progress_msg}")
    logger.warning("[RESTART] Raising APISessionExhausted exception...")
    raise APISessionExhausted(f"{reason}: {failures} consecutive failures")


def ensure_schema_exists() -> None:
    """Create database schema if it doesn't exist, then auto-sync columns."""
    
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Check if tables exist
        players_table = get_table_name('player', 'entity')
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{players_table}'
            )
        """)
        
        if not cursor.fetchone()[0]:
            logger.info("Creating database schema...")
            schema_ddl = generate_schema_ddl()
            cursor.execute(schema_ddl)
            conn.commit()
            logger.info("Schema created successfully")
        
        cursor.close()
        
        # Auto-add any new columns defined in config
        ensure_schema(DB_SCHEMA, TABLES_CONFIG, DB_COLUMNS, conn=conn)




def update_player_rosters(ctx: ETLContext) -> Tuple[int, int, List[int]]:
    """
    FAST daily roster update:
    1. Fetch player stats (current + last season) - 2 API calls, very fast
    2. Fetch team rosters to get team_id + jersey_number - 30 API calls, ~30 seconds
    3. Only fetch height/weight/birthdate for NEW players (rare)
    
    This completes in ~2-3 minutes instead of 20 minutes.
    Height/weight/birthdate for existing players updated annually on August 1st.
    
    Returns: (players_added, players_updated) for progress bar adjustment
    """
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    current_season = get_season()
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW
    # This is the SOURCE OF TRUTH for current team assignments
    logger.info("Fetching commonteamroster...")
    try:
        from nba_api.stats.static import teams
        from nba_api.stats.endpoints import commonteamroster
        nba_teams = teams.get_teams()
        
        # OPTIMIZATION: Parallel roster fetching (30 teams -> ~10 seconds instead of 30)
        # TIER 2: Per-team endpoint (30 API calls) - use high parallelism
        parallel_executor = ctx.parallel_executor
        
        # Build tasks for parallel execution
        tasks = []
        for team in nba_teams:
            # Lambda must accept timeout parameter (passed by executor)
            tasks.append({
                'id': team['id'],
                'func': lambda timeout, tid=team['id'], tn=team['full_name']: (
                    commonteamroster.CommonTeamRoster(
                        team_id=tid, 
                        season=current_season,
                        timeout=timeout
                    ),
                    tn
                ),
                'description': f"Roster for {team['abbreviation']}",
                'max_retries': 3
            })
        
        # Execute in parallel
        results, errors, failed_ids = parallel_executor.execute_batch(
            tasks, 
            description=f"Team rosters for {current_season}"
        )
        
        # Process results (no need to update progress here - already done in callback)
        for team_id, result in results.items():
            try:
                roster_endpoint, team_name = result
                roster_df = roster_endpoint.get_data_frames()[0]
                
                for _, player_row in roster_df.iterrows():
                    player_id = player_row['PLAYER_ID']
                    player_name = player_row['PLAYER']
                    
                    # Add player from roster (SOURCE OF TRUTH)
                    # Use DB column names from config (jersey_number not jersey)
                    all_players[player_id] = {
                        'player_id': player_id,
                        'team_id': team_id,  # Use team from roster
                        'name': player_name,
                        'jersey_number': safe_int(player_row.get('NUM')),  # SMALLINT column — safe_int handles NaN → None
                        'weight_lbs': None,  # Will get from annual ETL or commonplayerinfo for new players
                        'age': None
                    }
                
            except Exception as e:
                print(f"\u26a0\ufe0f  WARNING - Failed to process roster for team {team_id}: {e}")
        
    except Exception as e:
        print(f"\u26a0\ufe0f  WARNING - Failed to fetch current rosters: {e}")
        import traceback
        print(traceback.format_exc())
    
    # Get existing players from database to identify NEW players
    with db_connection() as conn:
        cursor = conn.cursor()
        players_table = get_table_name('player', 'entity')
        cursor.execute(f"SELECT player_id FROM {players_table}")
        existing_player_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
    
    # Identify NEW players (not in database)
    new_player_ids = [pid for pid in all_players.keys() if pid not in existing_player_ids]
    
    # Discover ALL detail fields from DB_COLUMNS for entity table (maps to players DB table)
    # This includes fields from commonplayerinfo API that update daily/annually
    detail_fields = {}  # {db_column_name: {'api_field': 'API_FIELD', 'transform': 'safe_int'}}
    
    for col_name, col_config in DB_COLUMNS.items():
        # Skip if col_config is not a dict (defensive programming)
        if not isinstance(col_config, dict):
            continue
        if (col_config.get('table') == 'entity' and 
            col_config.get('update_frequency') in ['daily', 'annual'] and
            col_config.get('api') and
            col_config.get('player_source')):
            
            player_source = col_config['player_source']
            if player_source.get('endpoint') == 'commonplayerinfo':
                detail_fields[col_name] = {
                    'api_field': player_source.get('field'),
                    'transform': player_source.get('transform', 'safe_str')
                }
    
    # Process new players ONE AT A TIME:
    # 1. Fetch player details
    # 2. Insert player into database
    # 3. Backfill all historical stats for that player
    # 4. Move to next player
    # This ensures atomic operations - if ETL crashes, only complete players are in the database
    if new_player_ids:
        logger.info(f"Processing {len(new_player_ids)} new players...")
        
        # RATE LIMITING: Process in batches with cooldown periods to avoid overwhelming API
        BATCH_SIZE = API_CONFIG['roster_batch_size']
        BATCH_COOLDOWN = API_CONFIG['roster_batch_cooldown']
        FAILURE_THRESHOLD = API_CONFIG.get('api_failure_threshold', 3)
        RESTART_ENABLED = API_CONFIG.get('api_restart_enabled', True)
        
        players_table = get_table_name('player', 'entity')
        detail_col_names = sorted(detail_fields.keys())
        insert_col_names = ['player_id', 'name', 'team_id'] + detail_col_names
        
        # Track consecutive failures for automatic subprocess restart
        consecutive_failures = 0
        
        with db_connection() as conn:
            cursor = conn.cursor()
            
            for idx, player_id in enumerate(new_player_ids, 1):
                player_data = all_players[player_id]
                player_name = player_data.get('name', 'Unknown')
                
                logger.info(f"[{idx}/{len(new_player_ids)}] {player_name} (ID: {player_id})")
                
                # Step 1: Fetch player details from commonplayerinfo
                # OPTIMIZATION: Extract FROM_YEAR to start backfill from player's rookie season
                rookie_year = None
                detail_fetch_failed = False
                detail_fetch_error = None
                
                try:
                    info_endpoint = commonplayerinfo.CommonPlayerInfo(
                        player_id=player_id,
                        timeout=API_CONFIG['timeout_default']
                    )
                    player_df = info_endpoint.get_data_frames()[0]
                    
                    if not player_df.empty:
                        row = player_df.iloc[0]
                        
                        # Extract FROM_YEAR (player's first NBA season) for optimized backfill
                        # FROM_YEAR is the STARTING year of the season (e.g., 2012 for 2012-13 season)
                        # Add 1 to get the ending year (rookie_year) for season calculations
                        from_year_raw = row.get('FROM_YEAR')
                        if from_year_raw:
                            try:
                                rookie_year = int(from_year_raw) + 1  # Convert start year to end year
                            except (ValueError, TypeError):
                                logger.warning(f"Could not parse FROM_YEAR: {from_year_raw}")
                        
                        # Extract values using config - store with DB column names
                        for db_col_name, field_config in detail_fields.items():
                            api_field = field_config['api_field']
                            transform_name = field_config['transform']
                            raw_value = row.get(api_field)
                            
                            # Apply transformation
                            if transform_name == 'safe_int':
                                player_data[db_col_name] = safe_int(raw_value)
                            elif transform_name == 'safe_float':
                                player_data[db_col_name] = safe_float(raw_value)
                            elif transform_name == 'safe_str':
                                player_data[db_col_name] = safe_str(raw_value)
                            elif transform_name == 'parse_height':
                                player_data[db_col_name] = parse_height(raw_value)
                            elif transform_name == 'parse_birthdate':
                                player_data[db_col_name] = parse_birthdate(raw_value)
                            elif transform_name == 'format_season':
                                player_data[db_col_name] = format_season(raw_value)
                            else:
                                player_data[db_col_name] = raw_value
                    
                except Exception as e:
                    detail_fetch_failed = True
                    detail_fetch_error = str(e)
                    consecutive_failures += 1  # Track consecutive failures
                    logger.warning(f"Failed to fetch details: {e}")
                    logger.warning("Will insert with basic info only (name, team, jersey)")
                    
                    # AUTO-RESTART: If hit failure threshold, trigger subprocess restart
                    if RESTART_ENABLED and consecutive_failures >= FAILURE_THRESHOLD:
                        # Commit current work before restart
                        conn.commit()
                        cursor.close()
                        
                        progress_msg = f"Progress: {players_added} players added, {idx}/{len(new_player_ids)} processed"
                        _trigger_automatic_restart("Session exhaustion in roster update", progress_msg, FAILURE_THRESHOLD, consecutive_failures)
                        
                # Reset consecutive failures counter on successful fetch
                if not detail_fetch_failed:
                    consecutive_failures = 0
                
                # Step 2: Insert player into database
                try:
                    insert_row = [player_id, player_data['name'], player_data['team_id']]
                    for col in detail_col_names:
                        insert_row.append(player_data.get(col))
                    
                    # Insert player (if detail fetch failed, player will have NULL values for detail fields)
                    # They can be populated later via populate_player_details.py script
                    cursor.execute(f"""
                        INSERT INTO {players_table} ({', '.join(insert_col_names)}, backfilled)
                        VALUES ({', '.join(['%s'] * len(insert_col_names))}, false)
                        ON CONFLICT (player_id) DO NOTHING
                    """, insert_row)
                    
                    if detail_fetch_failed:
                        logger.warning(f"Warning: Failed to fetch details for {player_data['name']}, inserted with NULL values")
                    
                    conn.commit()
                    players_added += 1
                    
                    # Note: Wingspan and other entity details should be populated via
                    # a separate one-time script or annual ETL, NOT during backfill
                    # (backfill is for stats tables, not entity tables)
                    
                except Exception as e:
                    logger.error(f"Failed to insert player: {e}")
                    conn.rollback()
                    continue  # Skip to next player
                
                # Add rate limiting delay between player fetches to avoid overwhelming API
                if idx < len(new_player_ids):
                    time.sleep(API_CONFIG.get('rate_limit_delay', 0.6))
            
            cursor.close()
    
    # Update existing players' team assignments
    with db_connection() as conn:
        cursor = conn.cursor()
        
        update_players_data = []
        
        players_table = get_table_name('player', 'entity')
        cursor.execute(f"SELECT player_id, team_id, jersey_number FROM {players_table}")
        existing_players = {row[0]: {'team_id': row[1], 'jersey_number': row[2]} for row in cursor.fetchall()}
        
        for player_id, player_data in all_players.items():
            if player_id in existing_players and player_id not in new_player_ids:
                existing = existing_players[player_id]
                # Check if team or jersey number changed
                if (existing['team_id'] != player_data['team_id'] or
                        existing['jersey_number'] != player_data.get('jersey_number')):
                    players_updated += 1
                
                # Update team_id and jersey_number — both come from commonteamroster
                # and can change (trades, number changes). Other detail fields
                # (rookie_year, height, etc.) are stable and handled by the annual ETL.
                update_players_data.append((
                    player_data['team_id'],
                    player_data.get('jersey_number'),
                    player_id
                ))
        
        # Bulk update existing players (team_id + jersey_number)
        if update_players_data:
            update_sql = f"""
                UPDATE {players_table}
                SET team_id = %s, jersey_number = %s, updated_at = NOW()
                WHERE player_id = %s
            """
            cursor.executemany(update_sql, update_players_data)
        
        # Clear team_id for players NOT in current rosters (they were traded or inactive)
        roster_player_ids = list(all_players.keys())
        if roster_player_ids:
            cursor.execute(f"""
                UPDATE {players_table}
                SET team_id = NULL, updated_at = NOW()
                WHERE player_id != ALL(%s) AND team_id IS NOT NULL
            """, (roster_player_ids,))
            cleared_count = cursor.rowcount
            if cleared_count > 0:
                logger.info(f"Cleared team_id for {cleared_count} players no longer on rosters")    
        
        conn.commit()
        cursor.close()
    
    return players_added, players_updated, new_player_ids


def update_basic_stats(ctx: ETLContext, entity: Literal['player', 'team'], skip_zero_stats: bool = False, player_ids: Optional[List[int]] = None, season_type: Optional[int] = None, params: Optional[Dict[str, Any]] = None) -> bool:
    """
    Update season statistics for all entities (players or teams).
    
    WHY: Consolidates update_player_stats + update_team_stats into one config-driven function.
    Eliminates 460 lines of duplication by using entity parameter.
    
    Args:
        ctx: ETLContext for state management
        entity: 'player' or 'team'
        skip_zero_stats: If True, don't add zero-stat records for roster players (player-only, backfill mode)
        player_ids: Optional list of player IDs to filter to (for backfill mode - only process these specific players)
        season_type: Optional season type code to process only that type (for backfill). If None, processes all types.
        params: Optional parameters dict to determine which stats to fetch:
                - None or {} → Basic stats only (default/base endpoint)
                - {'measure_type_detailed_defense': 'Advanced'} → Advanced stats only
        
    Returns:
        True if successful
        
    Usage:
        update_basic_stats(ctx, 'player')  # Basic stats, all players, all season types
        update_basic_stats(ctx, 'player', params=None)  # Basic stats only
        update_basic_stats(ctx, 'player', params={'measure_type_detailed_defense': 'Advanced'})  # Advanced stats only
        update_basic_stats(ctx, 'player', season_type=1)  # All players, Regular Season only
        update_basic_stats(ctx, 'player', player_ids=[1629632])  # Specific player(s)
        update_basic_stats(ctx, 'team')
    """
    # CRITICAL: Always read from config (don't cache) - backfill modifies NBA_CONFIG dynamically
    current_season = get_season()
    current_year = get_season_year()

    conn = get_db_connection()
    cursor = conn.cursor()

    # Entity-specific configuration
    if entity == 'player':
        endpoint_name = 'leaguedashplayerstats'
        EndpointClass = leaguedashplayerstats.LeagueDashPlayerStats
        table = get_table_name('player', 'stats')
        id_field = 'PLAYER_ID'
        year_value = current_season  # Use season string ('2024-25')
        
        # Get valid entity IDs from database (optionally filtered to specific players)
        players_table = get_table_name('player', 'entity')
        if player_ids:
            # Backfill mode: only process specific players
            cursor.execute(f"SELECT player_id, team_id FROM {players_table} WHERE player_id = ANY(%s)", (player_ids,))
            all_entities = cursor.fetchall()
            valid_entity_ids = {row[0] for row in all_entities}
        else:
            # Normal mode: process all players
            cursor.execute(f"SELECT player_id, team_id FROM {players_table}")
            all_entities = cursor.fetchall()
            valid_entity_ids = {row[0] for row in all_entities}
    else:  # team
        endpoint_name = 'leaguedashteamstats'
        EndpointClass = leaguedashteamstats.LeagueDashTeamStats
        table = get_table_name('team', 'stats')
        id_field = 'TEAM_ID'
        year_value = current_season  # Use season string ('2024-25')
        
        # Get valid entity IDs from config
        valid_entity_ids = set(list(TEAM_IDS.values()))
        all_entities = None  # Not needed for teams
    
    # Get columns from config based on params
    # params is None or {} → basic stats only (default/base endpoint)
    # params has measure_type_detailed_defense='Advanced' → advanced stats only
    # CRITICAL: ALWAYS use extract_filter_params to ensure proper column filtering
    # This ensures that when we request advanced stats, we EXCLUDE basic stats columns,
    # and when we request basic stats, we EXCLUDE advanced stats columns.
    filter_kwargs = extract_filter_params(params)
    all_cols = get_columns_by_endpoint(endpoint_name, entity, table=table, **filter_kwargs)
    
    if params and params.get('measure_type_detailed_defense') == 'Advanced':
        # Advanced stats endpoint
        fetch_basic = False
        fetch_advanced = True
    else:
        # Basic stats endpoint (default)
        fetch_basic = True
        fetch_advanced = False
    
    # CRITICAL: Exclude primary key from all_cols (it's added explicitly to avoid duplicates)
    entity_id_field = get_primary_key(entity)
    all_cols = {k: v for k, v in all_cols.items() if k != entity_id_field}

    # For teams, dynamically add opp_* virtual columns from each base column's
    # opponent_source config. The opponent DataFrame is merged into df below,
    # bringing OPP_-prefixed fields into every row. We build the virtual entries
    # here so the record-building loop writes them without needing separate
    # team_source definitions in DB_COLUMNS.
    if entity == 'team':
        opp_cols = {}
        for col_name, col_config in list(all_cols.items()):
            opp_source = col_config.get('opponent_source')
            if opp_source and isinstance(opp_source, dict):
                opp_col_name = f'opp_{col_name}'
                opp_cols[opp_col_name] = {
                    **col_config,
                    'team_source': opp_source,
                    'player_source': None,
                    'opponent_source': None,  # prevent recursion
                }
        all_cols.update(opp_cols)

    if season_type is not None:
        # Backfill mode: process only the specified season type
        season_type_name = next(
            (name for name, config in SEASON_TYPE_CONFIG.items() if config['season_code'] == season_type),
            'Regular Season'
        )
        season_types = [(season_type_name, season_type, None)]
    else:
        # Daily ETL mode: process all season types
        season_types = [(name, config['season_code'], config.get('minimum_season')) 
                        for name, config in SEASON_TYPE_CONFIG.items()]
    
    total_updated = 0
    
    for season_type_name, season_type_code, min_season in season_types:
        # Skip season type if current season is before minimum_season
        if min_season:
            min_year = int('20' + min_season.split('-')[1])
            current_year_num = int('20' + current_season.split('-')[1])
            if current_year_num < min_year:
                continue
        
        try:
            print(f"Fetching {endpoint_name} - {season_type_name}...")
            
            df = None
            
            # Fetch stats based on params
            df = None
            
            if fetch_basic:
                # Fetch basic stats only
                @with_retry(endpoint_name=endpoint_name)
                def fetch_basic_stats(timeout: int = API_CONFIG['timeout_bulk']) -> Any:
                    time.sleep(API_CONFIG['rate_limit_delay'])
                    return EndpointClass(
                        season=current_season,
                        season_type_all_star=season_type_name,
                        per_mode_detailed=API_CONFIG['per_mode_detailed'],
                        timeout=timeout
                    ).get_data_frames()[0]
                
                df = fetch_basic_stats()            
                if df.empty:
                    continue
            
            elif fetch_advanced:
                # Fetch advanced stats only
                @with_retry(endpoint_name=endpoint_name)
                def fetch_advanced_stats(timeout: int = API_CONFIG['timeout_bulk']) -> Any:
                    time.sleep(API_CONFIG['rate_limit_delay'])
                    return EndpointClass(
                        season=current_season,
                        season_type_all_star=season_type_name,
                        measure_type_detailed_defense='Advanced',
                        per_mode_detailed=API_CONFIG['per_mode_detailed'],
                        timeout=timeout
                    ).get_data_frames()[0]
                
                df = fetch_advanced_stats()
                if df.empty:
                    continue
            
            # If no data was fetched, skip
            if df is None or df.empty:
                continue
            
            # Fetch opponent stats for teams
            if entity == 'team':
                try:
                    @with_retry(endpoint_name=endpoint_name)
                    def fetch_opponent_stats(timeout: int = API_CONFIG['timeout_bulk']) -> Any:
                        time.sleep(API_CONFIG['rate_limit_delay'])
                        return EndpointClass(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Opponent',
                            per_mode_detailed=API_CONFIG['per_mode_detailed'],
                            timeout=timeout
                        ).get_data_frames()[0]
                    
                    opp_df = fetch_opponent_stats()                    
                    if not opp_df.empty:
                        df = df.merge(opp_df, on='TEAM_ID', how='left', suffixes=('', '_OPP'))
                except Exception as e:
                    logger.warning(f"Could not fetch opponent stats: {e}")
            
            # Remove duplicates
            df = df.drop_duplicates(subset=[id_field], keep='first')
            
            # Track entities with stats
            entities_with_stats = set()
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                entity_id = row[id_field]
                
                # Skip if not valid
                if entity_id not in valid_entity_ids:
                    continue
                
                entities_with_stats.add(entity_id)
                
                # Build record: ID + year + season_type + stats
                record_values = [entity_id, year_value, season_type_code]
                
                # Add stats from config (sorted for consistency)
                for col_name in sorted(all_cols.keys()):
                    col_config = all_cols[col_name]
                    source = col_config.get(f'{entity}_source')
                    
                    if not source or not isinstance(source, dict):
                        record_values.append(0)
                        continue
                    
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Handle calculated fields (e.g., "FGM - 3FGM")
                    if any(op in field_name for op in ['+', '-', '*', '/']):
                        if ' - ' in field_name:
                            left, right = field_name.split(' - ')
                            left_val = safe_int(row.get(left.strip(), 0))
                            right_val = safe_int(row.get(right.strip(), 0))
                            value = max(0, left_val - right_val)
                        elif ' + ' in field_name:
                            left, right = field_name.split(' + ')
                            left_val = safe_int(row.get(left.strip(), 0))
                            right_val = safe_int(row.get(right.strip(), 0))
                            value = left_val + right_val
                        else:
                            value = 0
                    else:
                        # Apply transform
                        raw_value = row.get(field_name)
                        
                        # If field is missing from API response, check if it's available for this season
                        if raw_value is None:
                            # If column isn't available for this season (e.g., putbacks before 2013-14),
                            # leave as NULL instead of defaulting to 0
                            if not _is_column_available_for_season(col_name, current_season):
                                value = None
                                record_values.append(value)
                                continue
                            else:
                                # Field should exist but is missing - default to 0
                                value = 0
                        else:
                            # Field exists, apply transformation
                            raw_value_to_transform = raw_value
                        
                        if raw_value is not None:
                            transform_name = source.get('transform', 'safe_int')
                            scale = source.get('scale', 1)
                            
                            if transform_name == 'safe_int':
                                value = safe_int(raw_value, scale=scale)
                            elif transform_name == 'safe_float':
                                value = safe_float(raw_value, scale=scale)
                            else:
                                value = safe_int(raw_value, scale=scale)
                            
                            # Verbose logging: API value → DB value for basic stats
                            log_verbose_data(entity_id, col_name, raw_value, value, current_season, season_type_code)
                    
                    record_values.append(value)
                
                records.append(tuple(record_values))
            
            # Add zero-stat records for players without stats (Regular Season only)
            # CRITICAL: Only create zero-stat records when processing BASIC stats (which includes games column)
            # Do NOT create records when processing advanced stats, as this would insert games=NULL
            has_games_column = 'games' in all_cols
            if entity == 'player' and season_type_code == 1 and not skip_zero_stats and has_games_column:
                entities_without_stats = valid_entity_ids - entities_with_stats
                if entities_without_stats:
                    for entity_id in entities_without_stats:
                        team_id = next((t for p, t in all_entities if p == entity_id), None)
                        if not team_id:
                            continue
                        
                        zero_values = [entity_id, year_value, season_type_code]
                        for col_name in sorted(all_cols.keys()):
                            # For zero-stat records with games=0, use NULL instead of 0
                            # (players not on active roster should have NULLs, not zeros)
                            if col_name == 'games':
                                zero_values.append(0)
                            else:
                                zero_values.append(None)  # NULL for all other stats
                        
                        records.append(tuple(zero_values))
            
            # Bulk insert or update
            if records:
                entity_id_field = get_primary_key(entity)
                
                # Guard against empty column list (would cause SQL syntax error)
                if not all_cols:
                    print(f"\u26a0\ufe0f  WARNING - No columns configured for {entity} in {table}, skipping insert")
                    continue
                
                # CRITICAL FIX: When processing advanced stats (has_games_column=False), 
                # use pure UPDATE instead of INSERT...ON CONFLICT.
                # 
                # Why: INSERT...ON CONFLICT fails when games column is not included because:
                # 1. games column has NOT NULL constraint
                # 2. games column has no DEFAULT value
                # 3. INSERT portion tries to create row without games, causing NULL constraint violation
                #
                # Solution: Use pure UPDATE for advanced stats since row already exists from basic stats
                if has_games_column:
                    # BASIC STATS: Use INSERT...ON CONFLICT (row might not exist yet)
                    db_columns = [entity_id_field, 'year', 'season_type'] + sorted(all_cols.keys())
                    columns_str = ', '.join(quote_column(col) for col in db_columns)
                    
                    update_clauses = [
                        f"{quote_column(col)} = EXCLUDED.{quote_column(col)}" for col in sorted(all_cols.keys())
                    ]
                    update_str = ',\n                        '.join(update_clauses)
                    
                    sql = f"""
                        INSERT INTO {table} (
                            {columns_str}
                        ) VALUES %s
                        ON CONFLICT ({entity_id_field}, year, season_type) DO UPDATE SET
                            {update_str},
                            updated_at = NOW()
                    """
                    
                    execute_values(cursor, sql, records)
                else:
                    # ADVANCED STATS: Use pure UPDATE (row must already exist from basic stats)
                    # Build parameterized UPDATE for batch execution
                    update_clauses = [
                        f"{quote_column(col)} = data.{quote_column(col)}" for col in sorted(all_cols.keys())
                    ]
                    update_str = ',\n                        '.join(update_clauses)
                    
                    # Create temporary table with new values
                    db_columns = [entity_id_field, 'year', 'season_type'] + sorted(all_cols.keys())
                    columns_str = ', '.join(quote_column(col) for col in db_columns)
                    
                    sql = f"""
                        UPDATE {table}
                        SET {update_str},
                            updated_at = NOW()
                        FROM (VALUES %s) AS data({columns_str})
                        WHERE {table}.{entity_id_field} = data.{entity_id_field}
                          AND {table}.year = data.year
                          AND {table}.season_type = data.season_type
                    """
                    
                    execute_values(cursor, sql, records)
                
                conn.commit()
                total_updated += len(records)
        
        except Exception as e:
            print(f"\u274c ERROR - Error fetching {season_type_name} stats: {e}")

    # Always return the connection to the pool, even if an exception propagates
    # out of the season-type loop (e.g. APISessionExhausted).
    try:
        cursor.close()
    finally:
        return_db_connection(conn)

    return True


def _run_post_endpoint_processing(
    ctx: ETLContext,
    endpoint: str,
    season: str,
    season_type: int,
    entity: str,
    params: Dict[str, Any],
    season_type_name: str = 'Regular Season'
) -> None:
    """
    Shared post-processing after an endpoint writes data.

    Handles:
    1. Remaining transformation columns (mixed endpoints with both direct + transform cols)
    2. NULL/zero cleanup
    3. pt_indicator cascade
    4. Data integrity validation

    Extracted from run_endpoint_backfill to eliminate duplication across scope branches.

    Args:
        ctx: ETL context
        endpoint: Endpoint name
        season: Season string (e.g., '2024-25')
        season_type: Season type code (1=Regular, 2=Playoffs, 3=PlayIn)
        entity: Entity type ('player' or 'team')
        params: Parameter dictionary for this endpoint call
        season_type_name: Season type name for API calls
    """
    # ====================================================================
    # STEP 0: Process remaining transformation columns (if any)
    # Some endpoints have BOTH direct extraction AND transformation columns
    # (e.g. leaguedashptstats has direct touches + transformation dribbles).
    # Direct cols were already written by execute_endpoint; now handle transforms.
    # ====================================================================
    try:
        # Check if this endpoint has transformation columns for this entity
        source_key = f'{entity}_source'
        has_transforms = False
        for col_name, col_meta in DB_COLUMNS.items():
            if not isinstance(col_meta, dict):
                continue
            source = col_meta.get(source_key)
            if not source or not isinstance(source, dict):
                continue
            transform = source.get('transformation')
            if not transform:
                continue
            col_ep = transform.get('endpoint') or source.get('endpoint')
            if col_ep == endpoint:
                has_transforms = True
                break

        if has_transforms:
            transform_count = update_transformation_columns(
                ctx=ctx,
                season=season,
                entity=entity,
                season_type=season_type,
                season_type_name=season_type_name,
                endpoint=endpoint
            )
            if transform_count:
                print(f"  -> Updated {transform_count} {entity}s via transformation pipeline")
    except APISessionExhausted:
        raise
    except Exception as e:
        print(f"  WARNING: Transformation processing failed: {e}")

    # ====================================================================
    # STEP 1: NULL/Zero Cleanup (Config-Driven)
    # ====================================================================
    print(f"  -> NULL/Zero cleanup...")
    try:
        rows_cleaned = execute_null_zero_cleanup(endpoint, season, season_type, entity, params)
        print(f"  -> Cleaned {rows_cleaned} rows")
    except Exception as e:
        print(f"  WARNING: NULL cleanup failed: {e}")

    # ====================================================================
    # STEP 2: pt_indicator Cascade
    # ====================================================================
    try:
        filter_kwargs_cascade = extract_filter_params(params)
        endpoint_cols_cascade = get_columns_by_endpoint(endpoint, entity, **filter_kwargs_cascade)
        indicator_written = next(
            (c for c in endpoint_cols_cascade
             if DB_COLUMNS.get(c, {}).get('pt_indicator') == 'yes'),
            None
        )
        if indicator_written:
            cascade_rows = run_pt_indicator_cascade(indicator_written, season, season_type, entity)
            if cascade_rows:
                print(f"  -> Cascade ({indicator_written}): fixed {cascade_rows} rows")
    except Exception as e:
        print(f"  WARNING: pt_indicator cascade failed: {e}")

    # ====================================================================
    # STEP 3: Data Integrity Validation
    # ====================================================================
    print(f"  -> Validating data integrity...")
    try:
        validation_failures = validate_data_integrity(endpoint, season, season_type, entity, params)

        if validation_failures:
            print(f"  WARNING: {len(validation_failures)} {entity}(s) with validation failures")
            log_missing_data_to_tracker(endpoint, season, season_type, validation_failures, params, entity)
        else:
            print(f"  -> Validation passed")
            log_missing_data_to_tracker(endpoint, season, season_type, {}, params, entity)

    except Exception as e:
        print(f"  WARNING: Validation failed: {e}")
        update_backfill_status(endpoint, season, season_type, None, params=params, entity=entity)


def run_endpoint_backfill(
    ctx: ETLContext,
    endpoint: str,
    season: str,
    season_type: int,
    scope: str,
    params: Optional[Dict[str, Any]] = None,
    entity: Optional[Literal['player', 'team']] = None,
    backfill_mode: bool = True
) -> bool:
    """
    Process a single endpoint for one season/season_type with specific parameters.

    Handles league-wide, team-by-team, or player-by-player processing based on scope.
    Updates endpoint_tracker to track progress.

    Args:
        ctx: ETL context
        endpoint: Endpoint name to process
        season: Season string (e.g., '2024-25')
        season_type: Season type code (1=Regular, 2=Playoffs, 3=PlayIn)
        scope: Endpoint scope ('league', 'team', 'player')
        params: Parameter dictionary (e.g., {'pt_measure_type': 'Possessions'})
        entity: Optional entity type override ('player' or 'team'). If not provided,
                determined from endpoint config.
        backfill_mode: If True, only process players with backfilled=false (default: True)

    Returns:
        True if successful, False if failed
    """
    if params is None:
        params = {}
    
    # Initialize backfill_player_ids at the start so it's available in all code paths
    backfill_player_ids = None
    
    # Get current season for comparison
    current_season = calculate_current_season()
    
    season_type_name = next(
        (name for name, cfg in SEASON_TYPE_CONFIG.items() if cfg['season_code'] == season_type),
        'Regular Season'
    )
    
    # Build parameter display string
    param_desc = ""
    if params:
        param_parts = []
        for key, value in sorted(params.items()):
            if not key.startswith('_'):  # Skip internal params
                param_parts.append(f"{key}={value}")
        if param_parts:
            param_desc = f" [{', '.join(param_parts)}]"
    
    # Determine entity type - use provided entity or infer from config
    if entity is None:
        # Get endpoint config to determine entity type BEFORE any status updates
        endpoint_config = get_endpoint_config(endpoint)
        
        # Determine entity type from endpoint configuration
        entity_types = endpoint_config.get('entity_types', [])
        if not entity_types:
            # Fallback: infer from endpoint name
            entity = 'player' if 'player' in endpoint.lower() else 'team'
        else:
            # Use first entity type from config
            entity = entity_types[0]
    
    try:
        # ====================================================================
        # STEP 1: Check if already complete - skip if so
        # ====================================================================
        # DISABLED: Retry loop for missing_data (user requested to skip retries)
        # Will only reprocess 'in_progress' or new rows
        
        status = get_backfill_status(endpoint, season, season_type, params, entity)
        if status and status.get('status') == 'complete':
            # Already complete - skip this combination
            return True
        
        # Mark as in_progress
        update_backfill_status(endpoint, season, season_type, 'in_progress', params=params, entity=entity)
        
        # Temporarily override config to use this season
        year = int('20' + season.split('-')[1])
        
        with override_nba_config(current_season=season, current_season_year=year):
            
            if scope == 'league':
                # IMPORTANT CONTRACT: leaguedashplayerstats/leaguedashteamstats MUST run
                # before all other endpoints — they INSERT the base rows; everything else
                # only UPDATEs. backfill_all_endpoints processes endpoints in config order,
                # so these base endpoints must appear first in ENDPOINTS_CONFIG.
                updated_count = 0  # Ensure always defined, even for base-stat INSERT endpoints

                # Get columns that will be populated
                columns = get_columns_for_endpoint_params(endpoint, params, entity)
                
                # Log using unified logging function
                log_endpoint_processing(season, season_type_name, endpoint, params, columns, scope)
                
                # SPECIAL CASE: leaguedashplayerstats and leaguedashteamstats are the BASE endpoints
                # They must INSERT records first, then other endpoints UPDATE them
                if endpoint in ['leaguedashplayerstats', 'leaguedashteamstats']:
                    # Use update_basic_stats which handles INSERT properly
                    # This creates the base records with games, minutes, and basic stats
                    # skip_zero_stats=True: Only insert players who actually played this season
                    # (don't create empty records for all players in database)
                    # season_type: Process only this specific season type (don't loop through all)
                    # params: Pass through to only fetch stats matching these parameters
                    update_basic_stats(ctx, entity, skip_zero_stats=True, season_type=season_type, params=params)
                else:
                    # Check if this endpoint has ANY direct extraction columns FOR THESE PARAMS
                    # If ALL columns are transformations, route to transformation pipeline
                    # Use helper to extract only params that exist in the dict
                    filter_kwargs = extract_filter_params(params)
                    direct_extraction_cols = get_columns_by_endpoint(endpoint, entity, **filter_kwargs)
                    
                    # Get player_ids for targeted backfill if this is player entity
                    # CRITICAL: For current season (include_current_season=True), process ALL active players
                    # For historical seasons (backfill_mode=True), only process non-backfilled players
                    if entity == 'player' and backfill_mode and season != current_season:
                        # Historical backfill: Only process non-backfilled players
                        backfill_player_ids = [pid for pid, _ in get_non_backfilled_player_ids_for_season(season, season_type)]
                        if backfill_player_ids:
                            print(f"  -> Backfilling {len(backfill_player_ids)} players with missing data")
                        else:
                            logger.debug(f"No backfill players found for {season} type={season_type}")
                    else:
                        # Current season OR team entity: Process all entities (no filtering)
                        logger.debug(f"Skipping backfill filter - processing all {entity}s for {season}")
                    
                    if not direct_extraction_cols:
                        # ALL columns are transformations - use transformation pipeline
                        
                        updated_count = update_transformation_columns(
                            ctx=ctx,
                            season=season,
                            entity=entity,
                            season_type=season_type,
                            season_type_name=season_type_name,
                            endpoint=endpoint,
                            player_ids=backfill_player_ids
                        )
                        print(f"  -> Updated {updated_count} {entity}s via transformation pipeline")
                    else:
                        # Has direct extraction columns - use execute_endpoint()
                        
                        updated_count = execute_endpoint(
                            ctx=ctx,
                            endpoint_name=endpoint,
                            endpoint_params=params,
                            season=season,
                            entity=entity,
                            season_type=season_type,
                            season_type_name=season_type_name,
                            description=f"{endpoint}{param_desc}",
                            player_ids=backfill_player_ids
                        )
                
                # Count how many entities were processed
                # For both transformation and direct extraction endpoints with player_ids filter, use the returned count
                # For direct extraction without filtering, query the database
                if backfill_player_ids is not None:
                    # Using player_ids filter - use the actual update count from the operation
                    count = updated_count
                else:
                    # Direct extraction endpoint - query the database to count records
                    with db_connection() as conn:
                        cursor = conn.cursor()
                        table = get_table_name(entity, 'stats')
                        # Both tables store year as season string (e.g. '2024-25')
                        cursor.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE year = %s AND season_type = %s",
                            (season, season_type)
                        )
                        count = cursor.fetchone()[0]
                        cursor.close()
                
                if entity == 'player':
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         player_successes=count, players_total=count, params=params, entity='player')
                    print(f"Updated {count} players")
                else:
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         team_successes=count, teams_total=count, params=params, entity='team')
                    print(f"Updated {count} teams")
                
                _run_post_endpoint_processing(ctx, endpoint, season, season_type, entity, params, season_type_name)
                
                return True
                
            elif scope == 'team':
                # Team-by-team processing
                # First check if this endpoint has ANY direct extraction columns FOR THESE PARAMS
                # If ALL columns are transformations, route to transformation pipeline
                filter_kwargs = extract_filter_params(params)
                direct_extraction_cols = get_columns_by_endpoint(endpoint, entity, **filter_kwargs)
                
                # Get columns that will be populated (for logging)
                columns = get_columns_for_endpoint_params(endpoint, params, entity)
                log_endpoint_processing(season, season_type_name, endpoint, params, columns, scope)
                
                if not direct_extraction_cols:
                    # ALL columns are transformations - use transformation pipeline
                    # (e.g. teamdashptshots: all 12 columns are filter_aggregate transforms)
                    updated_count = update_transformation_columns(
                        ctx=ctx,
                        season=season,
                        entity=entity,
                        season_type=season_type,
                        season_type_name=season_type_name,
                        endpoint=endpoint
                    )
                    
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         team_successes=updated_count, teams_total=updated_count, params=params, entity='team')
                    print(f"Updated {updated_count} teams via transformation pipeline")
                else:
                    # Has direct extraction columns - process team-by-team via execute_endpoint
                    teams = get_active_teams()
                    total_teams = len(teams)
                    successes = 0
                    
                    print(f"  Processing {total_teams} teams...")
                    for team_id in teams:
                        try:
                            # Call endpoint with team_id parameter (merge with params)
                            combined_params = {**params, 'team_id': team_id}
                            execute_endpoint(
                                ctx=ctx,
                                endpoint_name=endpoint,
                                endpoint_params=combined_params,
                                season=season,
                                entity=entity,
                                season_type=season_type,
                                season_type_name=season_type_name,
                                description=f"{endpoint}{param_desc} (Team {team_id})"
                            )
                            successes += 1
                            update_backfill_status(endpoint, season, season_type, 'in_progress',
                                                 team_successes=successes, teams_total=total_teams, params=params, entity='team')
                        except APISessionExhausted:
                            # Must propagate — don't swallow the restart signal
                            raise
                        except Exception as e:
                            print(f"    Team {team_id} failed: {e}")
                            if API_CONFIG['api_failure_threshold'] == 1:
                                # Immediate restart on any failure
                                update_backfill_status(endpoint, season, season_type, 'failed',
                                                     team_successes=successes, teams_total=total_teams, params=params, entity='team')
                                return False
                    
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         team_successes=successes, teams_total=total_teams, params=params, entity='team')
                    print(f"Updated {successes} teams")
                
                _run_post_endpoint_processing(ctx, endpoint, season, season_type, entity, params, season_type_name)
                
                return True
                
            elif scope == 'player':
                # Player-by-player processing
                #
                # EXPLANATION: For per-player endpoints with transformations (like playerdashptshots):
                # - The log shows column names BEFORE processing: "cont_close_2fgm, cont_close_2fga, ..."
                # - Then it processes the FIRST column (cont_close_2fgm) which makes API calls to all players
                # - Progress markers appear: "Progress: 50/119 players", "Progress: 100/119 players"
                # - After ALL players are processed for cont_close_2fgm, it prints the next column name
                # - Subsequent columns (cont_close_2fga, open_close_2fgm, etc.) are computed from the SAME
                #   API data already fetched, so they complete instantly without additional API calls
                # - That's why only the FIRST column shows progress markers, and the rest appear after
                #
                # Get player_ids for targeted backfill if in backfill mode
                if entity == 'player' and backfill_mode:
                    backfill_player_ids = [pid for pid, _ in get_non_backfilled_player_ids_for_season(season, season_type)]
                    if backfill_player_ids:
                        print(f"  -> Backfilling {len(backfill_player_ids)} players with missing data")
                    else:
                        print(f"  -> No players need backfilling for {season} {season_type_name}")
                
                # First check if this endpoint uses transformation pipeline
                # Use extract_filter_params to properly handle params for column filtering
                filter_kwargs = extract_filter_params(params)
                direct_extraction_cols = get_columns_by_endpoint(endpoint, entity, **filter_kwargs)
                
                if not direct_extraction_cols:
                    # ALL columns are transformations - use transformation pipeline
                    # Get columns that will be populated (transformation columns)
                    columns = get_columns_for_endpoint_params(endpoint, params, entity)
                    
                    log_endpoint_processing(season, season_type_name, endpoint, params, columns, scope)
                    
                    updated_count = update_transformation_columns(
                        ctx=ctx,
                        season=season,
                        entity=entity,
                        season_type=season_type,
                        season_type_name=season_type_name,
                        endpoint=endpoint,
                        player_ids=backfill_player_ids
                    )
                    
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         player_successes=updated_count, players_total=updated_count, params=params, entity='player')
                    print(f"Updated {updated_count} players")
                else:
                    # Has direct extraction columns - process player-by-player
                    # Use backfill_player_ids if available, otherwise get all players
                    if backfill_player_ids is not None:
                        player_team_ids = [(pid, 0) for pid in backfill_player_ids]
                    else:
                        player_team_ids = get_player_ids_for_season(season, season_type)
                    
                    total_players = len(player_team_ids)
                    successes = 0
                    
                    # Get columns that will be populated
                    columns = get_columns_for_endpoint_params(endpoint, params, entity)
                    
                    log_endpoint_processing(season, season_type_name, endpoint, params, columns, scope)
                    
                    for player_id, team_id in player_team_ids:
                        try:
                            # Call endpoint with player_id parameter (merge with params)
                            combined_params = {**params, 'player_id': player_id}
                            execute_endpoint(
                                ctx=ctx,
                                endpoint_name=endpoint,
                                endpoint_params=combined_params,
                                season=season,
                                entity='player',
                                season_type=season_type,
                                season_type_name=season_type_name,
                                description=f"{endpoint}{param_desc} (Player {player_id})",
                                suppress_logs=True  # Don't log each individual player API call
                            )
                            successes += 1
                            if successes % 50 == 0:  # Progress update every 50 players
                                update_backfill_status(endpoint, season, season_type, 'in_progress',
                                                     player_successes=successes, players_total=total_players, params=params, entity='player')
                                print(f"    Progress: {successes}/{total_players} players")
                        except Exception as e:
                            print(f"    Player {player_id} failed: {e}")
                            if API_CONFIG['api_failure_threshold'] == 1:
                                # Immediate restart on any failure
                                update_backfill_status(endpoint, season, season_type, 'failed',
                                                     player_successes=successes, players_total=total_players, params=params, entity='player')
                                return False
                    
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         player_successes=successes, players_total=total_players, params=params, entity='player')
                    print(f"Updated {successes} players")
                
                _run_post_endpoint_processing(ctx, endpoint, season, season_type, entity, params, season_type_name)
                
                return True
    except APISessionExhausted:
        # Re-raise session exhaustion to trigger automatic restart (exit code 42)
        raise
    except Exception as e:
        logger.error(f"Failed: {e}")
        logger.error(traceback.format_exc())
        update_backfill_status(endpoint, season, season_type, 'failed', params=params, entity=entity)
        return False


def backfill_all_endpoints(
    ctx: ETLContext,
    start_season: str,
    include_current_season: bool = False,
    backfill_mode: bool = True,
    entities: Optional[List[str]] = None
) -> None:
    """
    Endpoint-by-endpoint backfill orchestrator.

    Processes endpoints in order (league → team → player), tracking progress
    in endpoint_tracker so it can resume after interruption.

    Args:
        ctx: ETL context
        start_season: First season to process (required — provided by caller)
        include_current_season: If True, only process current season (daily ETL mode).
                                If False, process historical seasons up to prev season.
        backfill_mode: If True, only process players with backfilled=false.
        entities: If provided, restrict processing to these entity types
                  (e.g. ['player'] for player-only, ['team'] for team-only).
                  None means all entity types (default).
    """
    import json

    current_season = calculate_current_season()

    # Determine season range
    if include_current_season:
        # Daily ETL: only current season
        all_seasons = [start_season]
        max_backfill_season = start_season
        print(f"\n{'='*70}")
        print("[DAILY ETL] Processing current season only")
        print(f"{'='*70}")
    else:
        # Backfill: up to previous season (current season handled separately)
        current_year = int('20' + current_season.split('-')[1])
        max_backfill_year = current_year - 1
        max_backfill_season = f"{max_backfill_year-1}-{str(max_backfill_year)[-2:]}"

        start_year = int('20' + start_season.split('-')[1])
        all_seasons = []
        for year in range(start_year, max_backfill_year + 1):
            all_seasons.append(f"{year-1}-{str(year)[-2:]}")

        print(f"\n{'='*70}")
        print("[START] BACKFILL: Endpoint-by-Endpoint Processing")
        print(f"  Season Range: {start_season} to {max_backfill_season}")
        print(f"  Current Season: {current_season} (handled separately)")
        print(f"{'='*70}")

    # Get ordered endpoint list — include team-specific endpoints (e.g. leaguedashteamstats)
    endpoints = get_endpoint_processing_order(include_team_endpoints=True)

    # Load already-complete combinations for fast skip (player and team entities)
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT endpoint, year, season_type, params, entity
            FROM {ENDPOINT_TRACKER_TABLE}
            WHERE status = 'complete'
              AND (missing_data IS NULL OR missing_data = 'null'::jsonb)
        """)
        completed_combinations = set()
        for row in cursor.fetchall():
            ep_name, year, season_type, params_str, entity = row
            try:
                params_normalized = json.dumps(json.loads(params_str), sort_keys=True) if params_str and params_str != '{}' else '{}'
            except Exception:
                params_normalized = params_str or '{}'
            completed_combinations.add((ep_name, year, season_type, params_normalized, entity))
        cursor.close()

    # Count total work for progress reporting
    total_combinations = 0
    already_done = 0
    for endpoint_name in endpoints:
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint_name, {})
        min_season = endpoint_config.get('min_season')
        entity_types = endpoint_config.get('entity_types', ['player'])
        if entities is not None:
            entity_types = [e for e in entity_types if e in entities]
        for entity in entity_types:
            param_combinations = get_endpoint_parameter_combinations(endpoint_name, entity)
            for params in param_combinations:
                params_str = json.dumps(params, sort_keys=True) if params else '{}'
                for season in all_seasons:
                    season_year = int('20' + season.split('-')[1])
                    if min_season and season_year < int('20' + min_season.split('-')[1]):
                        continue
                    for _, config in SEASON_TYPE_CONFIG.items():
                        minimum_season = config.get('minimum_season')
                        if minimum_season and season_year < int('20' + minimum_season.split('-')[1]):
                            continue
                        total_combinations += 1
                        if (endpoint_name, season, config['season_code'], params_str, entity) in completed_combinations:
                            already_done += 1

    if not include_current_season:
        print(f"  Total work: {total_combinations} combinations ({already_done} already complete)")
        print(f"{'='*70}")

    total_processed = already_done
    total_failed = 0

    # Process each endpoint across all entity types it serves
    for endpoint_name in endpoints:
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint_name, {})
        min_season = endpoint_config.get('min_season')
        entity_types = endpoint_config.get('entity_types', ['player'])
        if entities is not None:
            entity_types = [e for e in entity_types if e in entities]
        if not entity_types:
            continue

        scope = infer_execution_tier_from_endpoint(endpoint_name)

        for entity in entity_types:
            param_combinations = get_endpoint_parameter_combinations(endpoint_name, entity)

            # Check if any work remains for this endpoint/entity combination
            has_incomplete = False
            for params in param_combinations:
                params_str = json.dumps(params, sort_keys=True) if params else '{}'
                for season in all_seasons:
                    season_year = int('20' + season.split('-')[1])
                    if min_season and season_year < int('20' + min_season.split('-')[1]):
                        continue
                    for _, config in SEASON_TYPE_CONFIG.items():
                        minimum_season = config.get('minimum_season')
                        if minimum_season and season_year < int('20' + minimum_season.split('-')[1]):
                            continue
                        if (endpoint_name, season, config['season_code'], params_str, entity) not in completed_combinations:
                            has_incomplete = True
                            break
                    if has_incomplete:
                        break
                if has_incomplete:
                    break

            if not has_incomplete:
                continue

            param_label = f" ({len(param_combinations)} param combos)" if len(param_combinations) > 1 else ""
            print(f"\n{'='*70}")
            print(f"ENDPOINT: {endpoint_name} (scope: {scope}, entity: {entity}){param_label}")
            print(f"{'='*70}")

            for params in param_combinations:
                params_str = json.dumps(params, sort_keys=True) if params else '{}'

                for season in all_seasons:
                    season_year = int('20' + season.split('-')[1])
                    if min_season and season_year < int('20' + min_season.split('-')[1]):
                        continue

                    for season_type_name, config in SEASON_TYPE_CONFIG.items():
                        season_type = config['season_code']
                        minimum_season = config.get('minimum_season')
                        if minimum_season and season_year < int('20' + minimum_season.split('-')[1]):
                            continue

                        if (endpoint_name, season, season_type, params_str, entity) in completed_combinations:
                            continue

                        print()
                        try:
                            success = run_endpoint_backfill(
                                ctx, endpoint_name, season, season_type, scope, params,
                                entity=entity,
                                backfill_mode=backfill_mode
                            )
                        except APISessionExhausted:
                            raise  # Let it propagate for exit code 42

                        if success:
                            total_processed += 1
                            completed_combinations.add((endpoint_name, season, season_type, params_str, entity))
                        else:
                            total_failed += 1
                            if API_CONFIG['api_failure_threshold'] == 1:
                                raise RuntimeError(
                                    f"Backfill failed at {endpoint_name}/{entity} {season} {season_type_name}. "
                                    "Run again to resume."
                                )

    print(f"\n{'='*70}")
    print("BACKFILL COMPLETE")
    print(f"  Processed: {total_processed} | Failed: {total_failed}")
    print(f"{'='*70}")


def update_transformation_columns(
    ctx: ETLContext,
    season: str,
    entity: Literal['player', 'team'] = 'player',
    table: Optional[str] = None,
    season_type: int = 1,
    season_type_name: str = 'Regular Season',
    endpoint: Optional[str] = None,
    player_ids: Optional[List[int]] = None
) -> int:
    """
    Universal transformation executor with GROUPED EXECUTION for efficiency.
    Groups transformations by endpoint to avoid redundant API calls.
    
    Example: Instead of 12 separate playerdashptshots calls (2 hours),
    makes ONE call and extracts all 12 values (10 minutes).
    
    This replaces ALL specialized functions with 100% config-driven execution.
    
    Args:
        season: Season string (e.g., '2024-25')
        entity: 'player' or 'team'
        table: Target database table
        season_type: Season type code (1=Regular, 2=Playoffs, 3=PlayIn)
        season_type_name: Season type name for API calls
        endpoint: Optional endpoint name to filter which columns to process (for endpoint-by-endpoint backfill)
    """
    # Derive year from season string
    season_year = int('20' + season.split('-')[1])
    
    # Default table if not provided
    if table is None:
        table = get_table_name(entity, contents='stats')
    
    # Both player and team tables use season format
    year_value = season  # '2007-08'

    # Build the ordered list of transformation columns BEFORE opening a DB connection.
    # The discovery loop only reads DB_COLUMNS (config), no live connection needed.
    all_transforms: List[str] = []

    for col_name, col_meta in DB_COLUMNS.items():
        if not isinstance(col_meta, dict):
            continue

        source_key = f'{entity}_source'
        source_config = col_meta.get(source_key)
        if not source_config or isinstance(source_config, str):
            continue

        transform = source_config.get('transformation')
        if not transform:
            continue

        # Filter by endpoint if specified (for endpoint-by-endpoint backfill)
        if endpoint:
            transform_endpoint = transform.get('endpoint') or source_config.get('endpoint')
            if transform_endpoint != endpoint:
                continue

        execution_tier = source_config.get('execution_tier') or transform.get('execution_tier', 'league')
        if entity == 'team' and execution_tier == 'player':
            # Exception: filter_aggregate transforms can work for both via team_endpoint
            if transform.get('type') != 'filter_aggregate' or not transform.get('team_endpoint'):
                continue

        # Skip transforms that explicitly target a different entity
        transform_entity = transform.get('entity')
        if transform_entity is not None and transform_entity != entity:
            continue

        all_transforms.append(col_name)

    if not all_transforms:
        return 0

    total_updated = 0

    # Open a single DB connection that spans the entire transform loop + NULL cleanup.
    # get_db_connection/return_db_connection used directly (not the with-block context
    # manager) so the connection stays alive across the full function body.
    conn = get_db_connection()
    cursor = conn.cursor()

    # Execute each transformation — API-result caching in apply_transformation
    # prevents redundant calls when multiple columns share an endpoint.
    for col_name in all_transforms:
        try:
            source_key = f'{entity}_source'
            source_config = DB_COLUMNS[col_name][source_key]
            transform = source_config['transformation']

            endpoint_name = transform.get('endpoint') or source_config.get('endpoint')
            endpoint_params_from_source = source_config.get('params', {})
            if endpoint_name and not is_endpoint_available_for_season(endpoint_name, season, endpoint_params_from_source):
                continue
            
            print(f"    {col_name}")

            try:
                data = apply_transformation(ctx, col_name, transform, season, entity, table, season_type, season_type_name, source_config, player_ids=player_ids)
                    
            except APISessionExhausted:
                # Must re-raise — swallowing this causes the dead session to be retried
                # for every remaining column, each one silently failing, and then the
                # endpoint getting marked 'complete' despite partial data.
                raise
            except Exception as transform_error:
                print(f"    {col_name}: Transformation failed: {transform_error}")
                import traceback
                traceback.print_exc()
                
                # Log errors for all affected players
                if entity == 'player':
                    # Transformation failed for this column
                    print(f"      Transformation failed: {str(transform_error)}")
                
                continue

            if not isinstance(data, dict):
                print(f"  WARNING: Data is not a dict, skipping")
                continue

            if not data:
                print(f"  WARNING: Data is empty, skipping")
                continue

            updated = 0
            for entity_id, value in data.items():
                if isinstance(value, dict):
                    print(f"      Skipping entity {entity_id}: value is dict")
                    continue

                # Verbose logging: transformation result → DB value
                log_verbose_data(entity_id, col_name, value, value, season, season_type)

                if entity == 'player':
                    cursor.execute(f"""
                        UPDATE {table}
                        SET {quote_column(col_name)} = %s, updated_at = NOW()
                        WHERE player_id = %s AND year = %s::text AND season_type = %s
                    """, (value, entity_id, year_value, season_type))
                else:
                    cursor.execute(f"""
                        UPDATE {table}
                        SET {quote_column(col_name)} = %s, updated_at = NOW()
                        WHERE team_id = %s AND year = %s::text AND season_type = %s
                    """, (value, entity_id, year_value, season_type))

                if cursor.rowcount > 0:
                    updated += 1
                
            conn.commit()
            total_updated += updated

        except APISessionExhausted:
            # Must re-raise — do not let session exhaustion be swallowed here either.
            # The DB connection will be cleaned up by the finally-equivalent below.
            conn.rollback()
            cursor.close()
            return_db_connection(conn)
            raise
        except Exception as e:
            print(f"    Error processing {col_name}: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            
            # Log errors for all affected players
            # Transformation endpoint failed for this column
            print("    Transformation endpoint failed")
    
    # CLEANUP: NULL/0 handling driven by pt_indicator config
    # Rules:
    # 1. If indicator_col = 0: Set all nullable dependent columns to NULL
    # 2. If indicator_col > 0 AND NULL: Set to 0 (legit zero)
    try:
        # Collect all transformation columns processed in this call, grouped by pt_indicator
        indicator_groups: Dict[str, List[str]] = {}

        for col_name, col_def in DB_COLUMNS.items():
            if not isinstance(col_def, dict):
                continue
            if col_def.get('table') not in ('stats', 'both'):
                continue
            source = col_def.get(f'{entity}_source')
            if not source or 'transformation' not in source:
                continue
            if not _is_column_available_for_season(col_name, season):
                continue
            if endpoint:
                transform_config = source.get('transformation', {})
                if transform_config.get('endpoint') != endpoint:
                    continue

            indicator = col_def.get('pt_indicator')
            if not indicator or indicator == 'yes':
                continue
            indicator_groups.setdefault(indicator, []).append(col_name)

        rows_updated = 0
        for games_col, col_names in indicator_groups.items():
            nullable = [c for c in col_names if DB_COLUMNS[c].get('nullable', True)]

            # Rule 1: indicator = 0 → NULL all nullable columns
            if nullable:
                set_null = ', '.join(f"{quote_column(c)} = NULL" for c in nullable)
                cursor.execute(
                    f"""
                    UPDATE {table}
                    SET {set_null}, updated_at = NOW()
                    WHERE year = %s::text AND season_type = %s AND {games_col} = 0
                    """,
                    (year_value, season_type),
                )
                rows_updated += cursor.rowcount

            # Rule 2: indicator > 0 → fill NULL with 0
            set_zero = ', '.join(
                f"{quote_column(c)} = COALESCE({quote_column(c)}, 0)" for c in col_names
            )
            cursor.execute(
                f"""
                UPDATE {table}
                SET {set_zero}, updated_at = NOW()
                WHERE year = %s::text AND season_type = %s AND {games_col} > 0
                """,
                (year_value, season_type),
            )
            rows_updated += cursor.rowcount

        if rows_updated:
            print(f"  -> Cleaned {rows_updated} rows (pt_indicator-driven)")
        conn.commit()

    except Exception as e:
        print(f"  -> Failed NULL cleanup: {e}")
        conn.rollback()
    
    cursor.close()
    return_db_connection(conn)

    return total_updated


def run_daily_etl(ctx: ETLContext) -> None:
    """
    Main daily ETL orchestrator — clean 7-step flow.

    Step 1: Update rosters (add new players with backfilled=false, update team assignments)
    Step 2: Find earliest rookie_year among non-backfilled players
    Step 3: Ensure endpoint_tracker has rows for all historical seasons
    Step 4: Backfill historical data (all endpoints, earliest → prev season)
    Step 5: Post-historical: fetch wingspan → mark backfilled=true → reset historical endpoints
    Step 6: Ensure endpoint_tracker has rows for current season
    Step 7: Backfill current season → reset current season endpoints to 'ready'
    """
    print("THE GLASS - DAILY ETL STARTED")
    print("=" * 70)
    start_time = time.time()

    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()

        # Initialize parallel executor for roster fetching (30 teams)
        ctx.init_parallel_executor(max_workers=10, endpoint_tier='team')

        # ================================================================
        # STEP 1: Update rosters — adds new players, updates team assignments
        # No backfill logic here; new players get backfilled=false and are
        # handled by steps 2-5 below.
        # ================================================================
        print("\n[STEP 1] Updating player rosters...")
        players_added, players_updated, new_player_ids = update_player_rosters(ctx)
        print(f"  Added: {players_added} | Updated: {players_updated}")

        # ================================================================
        # STEP 2: Find earliest rookie_year among non-backfilled players
        # ================================================================
        with db_connection() as conn:
            players_table = get_table_name('player', 'entity')
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT MIN(rookie_year)
                FROM {players_table}
                WHERE backfilled = FALSE AND rookie_year IS NOT NULL
            """)
            earliest_rookie_year = cursor.fetchone()[0]
            cursor.close()

        current_season = calculate_current_season()

        if earliest_rookie_year:
            print(f"\n[STEP 2] Found non-backfilled players — earliest rookie year: {earliest_rookie_year}")

            # Compute previous season string (last season before current)
            current_year = int('20' + current_season.split('-')[1])
            prev_year = current_year - 1
            prev_season = f"{prev_year - 1}-{str(prev_year)[-2:]}"

            # ============================================================
            # STEP 3: Ensure endpoint_tracker has all rows for historical range
            # ============================================================
            print(f"\n[STEP 3] Ensuring endpoint_tracker coverage: {earliest_rookie_year} → {prev_season}")
            ensure_endpoint_tracker_coverage(earliest_rookie_year, end_season=prev_season)

            # Also ensure team coverage from the full backfill start — teams don't have a
            # rookie year concept, so we want their data going back as far as each endpoint
            # supports (tracker rows for earlier seasons are created here if missing).
            backfill_start = NBA_CONFIG['backfill_start_season']
            if backfill_start < earliest_rookie_year:
                ensure_endpoint_tracker_coverage(backfill_start, end_season=prev_season)

            # ============================================================
            # STEP 4: Backfill historical player data (non-backfilled players only)
            # ============================================================
            print(f"\n[STEP 4] Backfilling historical player data...")
            backfill_all_endpoints(
                ctx,
                start_season=earliest_rookie_year,
                include_current_season=False,
                backfill_mode=True,
                entities=['player']
            )

            # ============================================================
            # STEP 4b: Backfill historical team data
            # Teams don't have a backfilled=false concept; the tracker tracks completion.
            # Once complete, team historical rows stay 'complete' until explicitly reset.
            # ============================================================
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {ENDPOINT_TRACKER_TABLE}
                    WHERE entity = 'team' AND year < %s
                      AND (status IS NULL OR status != 'complete')
                """, (current_season,))
                incomplete_team_count = cursor.fetchone()[0]
                cursor.close()

            if incomplete_team_count > 0:
                print(f"\n[STEP 4b] Backfilling historical team data ({incomplete_team_count} incomplete rows)...")
                backfill_all_endpoints(
                    ctx,
                    start_season=backfill_start,
                    include_current_season=False,
                    backfill_mode=False,
                    entities=['team']
                )
            else:
                print(f"\n[STEP 4b] Team historical data already complete — skipping")

            # ============================================================
            # STEP 5: Post-historical cleanup
            #   a) Fetch wingspan for newly backfilled players
            #   b) Mark players as backfilled=true
            #   c) Reset historical endpoint statuses to 'ready'
            # ============================================================
            print(f"\n[STEP 5] Post-historical cleanup...")

            print("  Fetching wingspan data for non-backfilled players...")
            wingspan_updated, wingspan_total = update_wingspan_from_combine(
                ctx, only_unbackfilled=True, start_season=earliest_rookie_year
            )
            if wingspan_total > 0:
                print(f"  Updated wingspan for {wingspan_updated}/{wingspan_total} players")

            print("  Marking players as backfilled=TRUE...")
            mark_backfill_complete(earliest_rookie_year=earliest_rookie_year, current_season=current_season)

        else:
            print("\n[STEP 2] No non-backfilled players found — skipping historical backfill (steps 2-5)")

        # ================================================================
        # STEP 6: Ensure endpoint_tracker has rows for current season
        # ================================================================
        print(f"\n[STEP 6] Ensuring endpoint_tracker coverage for current season: {current_season}")
        ensure_endpoint_tracker_coverage(current_season, end_season=current_season)

        # ================================================================
        # STEP 7: Backfill current season (player + team), then reset to 'ready'
        # ================================================================
        print(f"\n[STEP 7] Processing current season (player + team): {current_season}")
        backfill_all_endpoints(
            ctx,
            start_season=current_season,
            include_current_season=True,
            backfill_mode=False
        )

        # Mark players whose only season is the current season as backfilled.
        # Step 4 skips them (historical only), and mark_backfill_complete in Step 5
        # returns early because their current-season endpoints aren't complete yet.
        # This step closes that gap: once the current season is processed, any player
        # whose rookie_year == current_season has nothing historical left to backfill.
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE players
                SET backfilled = TRUE
                WHERE backfilled = FALSE
                AND rookie_year = %s
            """, (current_season,))
            newly_backfilled = cursor.rowcount
            conn.commit()
            cursor.close()
        if newly_backfilled > 0:
            print(f"  Marked {newly_backfilled} current-season-only rookies as backfilled")

        print(f"\n  Resetting current season endpoints to 'ready' for tomorrow's run...")
        reset_current_season_endpoints(current_season)

        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print(f"DAILY ETL COMPLETE — {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        print("=" * 70)

    except APISessionExhausted:
        raise  # Let it propagate to main for exit code 42

    except Exception as e:
        elapsed = time.time() - start_time
        print("=" * 70)
        print(f"DAILY ETL FAILED — {elapsed:.1f}s")
        print(f"Error: {e}")
        print("=" * 70)
        raise


def retry_failed_endpoints(ctx: ETLContext) -> None:
    """
    Retry all endpoints that failed during ETL.\n    Uses ctx.failed_endpoints list populated during execution.
    
    Args:
        ctx: ETLContext containing failed endpoints to retry
    """
    if not ctx.failed_endpoints:
        return
    
    print("=" * 70)
    print(f"RETRYING {len(ctx.failed_endpoints)} FAILED ENDPOINTS")
    print("=" * 70)
    
    # Copy and clear the queue (in case retries add more failures)
    endpoints_to_retry = ctx.failed_endpoints.copy()
    ctx.failed_endpoints = []
    
    success_count = 0
    failed_count = 0
    
    for i, endpoint_info in enumerate(endpoints_to_retry, 1):
        func_name = endpoint_info['function']
        args = endpoint_info['args']
        description = args[7] if len(args) > 7 else "Unknown endpoint"
        
        print(f"\nRetry {i}/{len(endpoints_to_retry)}: {description}")
        
        try:
            if func_name == '_execute_league_wide_endpoint':
                result = _execute_league_wide_endpoint(*args)
                if result > 0:
                    print(f"  Retry succeeded - updated {result} records")
                    success_count += 1
                else:
                    print("  ✗ Retry failed - no data returned")
                    failed_count += 1
            # Add more function types here if needed
        except Exception as e:
            print(f"  ✗ Retry failed: {e}")
            failed_count += 1
    
    print("\n" + "=" * 70)
    print(f"RETRY COMPLETE: {success_count} succeeded, {failed_count} failed")
    print("=" * 70)
    
    return success_count, failed_count

def cleanup_inactive_players(ctx: ETLContext) -> int:
    print("Cleaning up inactive players...")
    
    with db_connection() as conn:
        cursor = conn.cursor()
        
        current_year = NBA_CONFIG['current_season_year']
        
        # Calculate season strings for last 2 years (e.g., if current is 2025, check 2024-25 and 2023-24)
        last_season = f"{current_year - 1}-{str(current_year)[-2:]}"
        two_seasons_ago = f"{current_year - 2}-{str(current_year - 1)[-2:]}"
        
        # Find players with NO RECORD AT ALL in the last 2 seasons
        players_table = get_table_name('player', 'entity')
        player_stats_table = get_table_name('player', 'stats')
        cursor.execute(f"""
            SELECT p.player_id, p.name 
            FROM {players_table} p
            WHERE NOT EXISTS (
                SELECT 1 FROM {player_stats_table} s
                WHERE s.player_id = p.player_id
                AND s.year IN (%s, %s)
            )
        """, (last_season, two_seasons_ago))
        
        players_to_delete = cursor.fetchall()
        
        if players_to_delete:
            player_ids_to_delete = tuple(p[0] for p in players_to_delete)
            players_table = get_table_name('player', 'entity')
            cursor.execute(f"""
                DELETE FROM {players_table}  
                WHERE player_id IN %s
            """, (player_ids_to_delete,))
            
            deleted_count = cursor.rowcount
        else:
            deleted_count = 0
        
        conn.commit()
        cursor.close()
        
        return deleted_count

def update_all_player_details(ctx: ETLContext) -> None:
    """
    Config-driven player details updater.
    Discovers which fields to update from DB_COLUMNS (update_frequency='annual', table='entity').
    """
    # Discover fields from config BEFORE opening any DB connection.
    annual_fields = {}
    endpoints_needed = set()

    for col_name, col_config in DB_COLUMNS.items():
        if not isinstance(col_config, dict):
            continue
        if (col_config.get('table') == 'entity' and
                col_config.get('update_frequency') == 'annual' and
                col_config.get('api') and
                col_config.get('player_source')):

            player_source = col_config['player_source']
            endpoint = player_source.get('endpoint')

            if endpoint:
                annual_fields[col_name] = {
                    'endpoint': endpoint,
                    'field': player_source.get('field'),
                    'transform': player_source.get('transform', 'safe_str')
                }
                endpoints_needed.add(endpoint)

    if not annual_fields:
        print("No annual fields configured")
        return 0, 0

    print(f"Updating {len(annual_fields)} annual fields: {', '.join(sorted(annual_fields.keys()))}")

    players_table = get_table_name('player', 'entity')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT player_id, name FROM {players_table} ORDER BY player_id")
        all_players = cursor.fetchall()
    except Exception:
        cursor.close()
        return_db_connection(conn)
        raise

    total_players = len(all_players)
    updated_count = 0
    failed_count = 0
    consecutive_failures = 0
    retry_queue = []

    def _fetch_and_update(player_id: int) -> bool:
        """Fetch API data and write one player's detail fields. Returns True on success."""
        endpoint_data = {}
        for endpoint in endpoints_needed:
            if endpoint == 'commonplayerinfo':
                info_df = commonplayerinfo.CommonPlayerInfo(
                    player_id=player_id, timeout=API_CONFIG['timeout_default']
                ).get_data_frames()[0]
                if not info_df.empty:
                    endpoint_data[endpoint] = info_df.iloc[0]

        if not endpoint_data:
            return False

        values = {}
        for col_name, field_config in annual_fields.items():
            ep = field_config['endpoint']
            if ep not in endpoint_data:
                continue
            row = endpoint_data[ep]
            raw_value = row.get(field_config['field'])
            transform_name = field_config['transform']
            if transform_name == 'safe_int':
                values[col_name] = safe_int(raw_value)
            elif transform_name == 'safe_float':
                values[col_name] = safe_float(raw_value)
            elif transform_name == 'parse_height':
                values[col_name] = parse_height(raw_value)
            elif transform_name == 'parse_birthdate':
                values[col_name] = parse_birthdate(raw_value)
            else:
                values[col_name] = safe_str(raw_value)

        if not values:
            return False

        set_clauses = [f"{col} = %s" for col in values] + ["updated_at = NOW()"]
        cursor.execute(
            f"UPDATE {players_table} SET {', '.join(set_clauses)} WHERE player_id = %s",
            list(values.values()) + [player_id]
        )
        return True

    try:
        for idx, (player_id, player_name) in enumerate(all_players):
            if idx > 0 and idx % 50 == 0:
                consecutive_failures = 0

            if consecutive_failures >= API_CONFIG['max_consecutive_failures']:
                print(f"WARNING - Taking {API_CONFIG['cooldown_after_batch_seconds']}s break")
                time.sleep(API_CONFIG['cooldown_after_batch_seconds'])
                consecutive_failures = 0

            for attempt in range(RETRY_CONFIG['max_retries']):
                try:
                    if _fetch_and_update(player_id):
                        updated_count += 1
                        consecutive_failures = 0
                    time.sleep(RATE_LIMIT_DELAY)
                    break
                except Exception:
                    consecutive_failures += 1
                    if attempt >= RETRY_CONFIG['max_retries'] - 1:
                        failed_count += 1
                        retry_queue.append((player_id, player_name))

        if retry_queue:
            print(f"\n  Retrying {len(retry_queue)} failed players...")
            for player_id, player_name in retry_queue:
                try:
                    if _fetch_and_update(player_id):
                        updated_count += 1
                        failed_count -= 1
                        print(f"  Retry success: {player_name}")
                except Exception as e:
                    logger.warning(f"Retry failed: {player_name} - {e}")
                time.sleep(RATE_LIMIT_DELAY)

        conn.commit()
    finally:
        cursor.close()
        return_db_connection(conn)

    print(f"Updated {updated_count}/{total_players} players ({len(retry_queue)} retries)")
    if failed_count > 0:
        print(f"WARNING - Failed to update {failed_count} players after all retries")

    return updated_count, failed_count


def update_wingspan_from_combine(ctx: ETLContext, only_unbackfilled: bool = False, start_season: Optional[str] = None) -> Tuple[int, int]:
    """
    Fetch wingspan data from NBA Draft Combine (DraftCombinePlayerAnthro endpoint).
    Searches all available seasons back to 2003 (combine_start_year) or specified start_season.
    If a player has wingspan data from multiple years, keeps the most recent.
    
    Args:
        ctx: ETLContext instance
        only_unbackfilled: If True, only fetch for players with backfilled=FALSE
        start_season: If provided, fetch combine data starting from 6 seasons before this season (e.g., '2021-22')
    
    Returns: (updated_count, total_checked)
    """
    from nba_api.stats.endpoints import DraftCombinePlayerAnthro
    
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Get all players who need wingspan data
        players_table = get_table_name('player', 'entity')
        if only_unbackfilled:
            cursor.execute(f"SELECT player_id FROM {players_table} WHERE wingspan_inches IS NULL AND backfilled = FALSE")
        else:
            cursor.execute(f"SELECT player_id FROM {players_table} WHERE wingspan_inches IS NULL")
        players_needing_wingspan = {row[0] for row in cursor.fetchall()}
        cursor.close()
    
    if not players_needing_wingspan:
        print("All players already have wingspan data")
        return 0, 0
    
    # Fetch combine data from all seasons (most recent first to get latest data)
    current_year = NBA_CONFIG['current_season_year']
    
    # Calculate start year: 6 seasons before start_season if provided, else combine_start_year
    if start_season:
        # Parse season like '2021-22' to get year 2021
        start_year_from_season = int(start_season.split('-')[0])
        # Go back 6 years
        start_year = start_year_from_season - 6
        print(f"  Fetching combine data starting 6 seasons before {start_season} (from {start_year})")
    else:
        start_year = NBA_CONFIG['combine_start_year']
    
    # Store wingspan data: {player_id: (wingspan, season_year)}
    wingspan_data = {}
    
    # Iterate from most recent to oldest (so we keep most recent data)
    for year in range(current_year, start_year - 1, -1):
        season = f"{year}-{str(year + 1)[-2:]}"
        
        try:
            endpoint = DraftCombinePlayerAnthro(season_year=season, timeout=10)
            time.sleep(API_CONFIG['rate_limit_delay'])
            result = endpoint.get_dict()
            
            for rs in result['resultSets']:
                player_id_idx = rs['headers'].index('PLAYER_ID')
                wingspan_idx = rs['headers'].index('WINGSPAN')
                
                for row in rs['rowSet']:
                    player_id = row[player_id_idx]
                    wingspan = row[wingspan_idx]
                    
                    # Only process players we need AND who have wingspan data
                    if player_id in players_needing_wingspan and wingspan is not None:
                        # Keep most recent data (first occurrence in reverse chronological order)
                        if player_id not in wingspan_data:
                            wingspan_data[player_id] = (wingspan, year)            
        except Exception as e:
            logger.warning(f"Failed to fetch {season}: {e}")
            continue
    
    # Update database with found wingspan data
    updated_count = 0
    players_table = get_table_name('player', 'entity')
    print(f"  Updating wingspan data in {players_table} table...")
    
    # Reopen connection for updates (previous cursor was closed after initial query)
    with db_connection() as update_conn:
        update_cursor = update_conn.cursor()
        
        for player_id, (wingspan, year) in wingspan_data.items():
            try:
                # Round to nearest inch
                wingspan_inches = round(wingspan)
                
                update_cursor.execute(f"""
                    UPDATE {players_table} 
                    SET wingspan_inches = %s, updated_at = NOW()
                    WHERE player_id = %s
                """, (wingspan_inches, player_id))
                
                if update_cursor.rowcount > 0:
                    updated_count += 1
                    if updated_count <= 3:  # Show first 3 updates
                        print(f"    Updated player {player_id}: wingspan_inches = {wingspan_inches} (from {year})")
            except Exception as e:
                print(f"  Failed to update player {player_id}: {e}")
                continue
        
        update_conn.commit()
        
        # VERIFY: Check that updates were written to players table
        if updated_count > 0:
            print(f"\n  Verifying updates in {players_table}...")
            sample_ids = list(wingspan_data.keys())[:3]
            if sample_ids:
                update_cursor.execute(f"""
                    SELECT player_id, wingspan_inches
                    FROM {players_table}
                    WHERE player_id = ANY(%s)
                """, (sample_ids,))
                verified = update_cursor.fetchall()
                for pid, ws in verified:
                    print(f"    Player {pid}: wingspan_inches = {ws}")
        
        update_cursor.close()
    
    return updated_count, len(players_needing_wingspan)


def run_annual_etl(ctx: ETLContext) -> None:
    """
    Annual maintenance ETL (runs August 1st each year).
    
    Args:
        ctx: ETLContext instance for state management
    
    Steps:
    1. Delete inactive players (no stats in last 2 seasons)
    2. Update wingspan from NBA Draft Combine
    3. Update all player details (height, weight, birthdate)
    """
    print("="*70)
    print("THE GLASS - ANNUAL ETL STARTED")
    print("="*70)
    
    try:
        print("Step 1: Cleaning up inactive players...")
        deleted_count = cleanup_inactive_players(ctx)
        print(f"  Removed {deleted_count} inactive players\n")
        
        print("Step 2: Updating wingspan from Draft Combine...")
        wingspan_updated, wingspan_total = update_wingspan_from_combine(ctx, only_unbackfilled=False)
        print(f"  Updated {wingspan_updated}/{wingspan_total} players\n")
        
        print("Step 3: Updating all player details...")
        details_updated, details_failed = update_all_player_details(ctx)
        print(f"  Updated {details_updated} players, {details_failed} failed\n")
        
        print("="*70)
        print("ANNUAL ETL COMPLETED SUCCESSFULLY")
        print(f"Total: {deleted_count} deleted, {wingspan_updated} wingspans, {details_updated} details")
        print("="*70)
        
    except Exception as e:
        print(f"ANNUAL ETL FAILED: {e}")
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='The Glass ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python src/etl.py              # Daily ETL (default)
  python src/etl.py --annual     # Annual maintenance (Aug 1st)
  python src/etl.py --verbose    # Daily ETL with verbose logging
        '''
    )

    parser.add_argument('--annual', '-a', action='store_true',
                        help='Run annual ETL (cleanup + player details)')
    parser.add_argument('--year', type=int,
                        help='Annual only: Override current year')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        globals()['VERBOSE_MODE'] = True
        print("[VERBOSE MODE ENABLED]")

    ctx = ETLContext()

    try:
        if args.annual:
            if args.year:
                season = f"{args.year-1}-{str(args.year)[-2:]}"
                with override_nba_config(current_season_year=args.year, current_season=season):
                    run_annual_etl(ctx=ctx)
            else:
                run_annual_etl(ctx=ctx)
        else:
            run_daily_etl(ctx=ctx)

    except APISessionExhausted as e:
        logger.warning(f"API session exhausted: {e}")
        logger.warning("Exiting with code 42 to trigger automatic restart...")
        close_connection_pool()
        sys.exit(42)

    except KeyboardInterrupt:
        logger.info("ETL interrupted by user")
        close_connection_pool()
        sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error in ETL: {e}")
        logger.error(traceback.format_exc())
        close_connection_pool()
        sys.exit(1)

    finally:
        close_connection_pool()
