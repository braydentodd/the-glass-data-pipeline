import os
import sys
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional, Tuple, Callable, Literal
from io import StringIO
from nba_api.stats.endpoints import (
    commonplayerinfo,
    leaguedashplayerstats, leaguedashteamstats,
)

# Configuration data (pure data structures)
from config.etl import (
    NBA_CONFIG, TEAM_IDS,
    DB_COLUMNS, SEASON_TYPE_CONFIG,
    PARALLEL_EXECUTION,
    API_CONFIG, RETRY_CONFIG, DB_OPERATIONS
)

# Reusable utilities and helpers
from lib.etl import (
    infer_execution_tier_from_endpoint,
    get_columns_by_endpoint,
    safe_int, safe_float, safe_str, parse_height, parse_birthdate, format_season,
    get_entity_id_field, get_endpoint_config, is_endpoint_available_for_season,
    with_retry, create_api_call,
    get_primary_key, get_table_name,
    quote_column, get_db_connection,
    get_season, get_season_year, build_endpoint_params
)

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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

class RateLimiter:
    
    def __init__(self, requests_per_second: float = 2.5) -> None:
        """
        Args:
            requests_per_second: Default 2.5 = 150 req/min (confirmed max sustained rate)
                                Note: With single-threaded execution, batch cooldowns control
                                the actual rate. This is a safety net for edge cases.
        """
        self.delay = 1.0 / requests_per_second
        self.last_request_time = 0
        self.lock = threading.Lock()
        self.request_times = []  # Sliding window
        
    def acquire(self) -> None:
        with self.lock:
            now = time.time()
            
            # Clean old requests from sliding window
            self.request_times = [t for t in self.request_times if now - t < self.window_size]
            
            # Calculate wait time
            if self.request_times:
                # Ensure minimum delay between requests
                time_since_last = now - self.request_times[-1]
                if time_since_last < self.delay:
                    wait_time = self.delay - time_since_last
                    time.sleep(wait_time)
                    now = time.time()
            
            # Record this request
            self.request_times.append(now)
            self.last_request_time = now

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
                        print(f"  Task {task_id} failed: {str(e)[:80]}")
                    
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
                print(f"  Batch failed at row {i}: {str(e)[:100]}")
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
    suppress_logs: bool = False
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
    pt_measure_type = endpoint_params.get('pt_measure_type')
    measure_detailed = endpoint_params.get('measure_type_detailed_defense')
    defense_category = endpoint_params.get('defense_category')
    if pt_measure_type:
        description = f"{endpoint_name} ({pt_measure_type})"
    elif measure_detailed:
        description = f"{endpoint_name} ({measure_detailed})"
    elif defense_category:
        description = f"{endpoint_name} ({defense_category})"
    
    if not suppress_logs:
        print(f"Fetching {description} - {season_type_name}...")
    
    # Get columns from config, filtered by parameter type if provided
    pt_measure_type = endpoint_params.get('pt_measure_type')
    measure_detailed = endpoint_params.get('measure_type_detailed_defense')
    defense_category = endpoint_params.get('defense_category')
    
    cols = get_columns_by_endpoint(
        endpoint_name, entity, table=table, 
        pt_measure_type=pt_measure_type,
        measure_type_detailed_defense=measure_detailed,
        defense_category=defense_category
    )
    if not cols:
        # No direct extraction columns - this endpoint uses transformations only
        # It should be processed via update_transformation_columns() instead
        return 0
    
    # CHECK: Does this endpoint require per-team execution?
    # Look for execution_tier='team' in any column's source config
    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
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
    
    if needs_team_iteration:
        # PER-TEAM EXECUTION: Loop through all 30 teams and aggregate results
        return _execute_per_team_endpoint(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols
        )
    else:
        # LEAGUE-WIDE EXECUTION: Single API call returns all entities
        return _execute_league_wide_endpoint(
            ctx, endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols
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
    cols: Dict[str, Dict]
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
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Dynamically import endpoint class
        from importlib import import_module
        module_name = f"nba_api.stats.endpoints.{endpoint_name.lower()}"
        
        try:
            module = import_module(module_name)
            
            # Find the endpoint class
            endpoint_classes = [
                name for name in dir(module)
                if name[0].isupper() and not name.startswith('_')
                and not name.endswith('Nullable')
                and name not in ['NBAStatsHTTP', 'Endpoint', 'LeagueID', 'Season', 'SeasonTypeAllStar', 
                               'PerModeSimple', 'PerModeDetailed', 'LastNGames', 'Month', 'Period', 'GameSegment',
                               'Location', 'Outcome', 'SeasonSegment', 'Conference', 'Division',
                               'GameSegmentNullable', 'LocationNullable', 'OutcomeNullable',
                               'SeasonSegmentNullable', 'ConferenceNullable', 'DivisionNullable',
                               'DefenseCategory', 'MeasureTypeDetailedDefense', 'PtMeasureType',
                               'PaceAdjust', 'Rank', 'PlusMinus']
            ]
            
            if not endpoint_classes:
                print(f"❌ ERROR: No endpoint class found in {module_name}")
                return 0
            
            class_name = endpoint_classes[0]
            EndpointClass = getattr(module, class_name)
            
        except (ImportError, AttributeError) as e:
            print(f"❌ ERROR: Could not import endpoint from {module_name}: {e}")
            return 0
        
        # Build parameters using centralized logic
        all_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, endpoint_params)
        
        # Call endpoint with rate limiting and retry protection
        api_call = create_api_call(
            EndpointClass,
            all_params,
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
                        
                    values.append(value)
                
                values.extend([entity_id, year_value, season_type])
                all_records.append(tuple(values))
        
        # Debug: Log if year_value doesn't match expected season
        if all_records:
            sample_record_year = all_records[0][-2]  # year is second to last
            if str(sample_record_year) != str(year_value):
                print(f"⚠️  WARNING: Expected year {year_value} but got {sample_record_year} for endpoint {endpoint_name}")
        
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
            conn.close()
            return updated
        else:
            conn.commit()
            cursor.close()
            conn.close()
            return 0
        
    except Exception as e:
        print(f"ERROR - Failed {description}: {str(e)}")
        
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
                    conn.close()
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
            conn.close()
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
    cols: Dict[str, Dict]
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
    from importlib import import_module
    
    # Import endpoint class
    module_name = f"nba_api.stats.endpoints.{endpoint_name.lower()}"
    try:
        module = import_module(module_name)
        import inspect
        
        # Find the main endpoint class dynamically (config-driven, no hardcoding!)
        # Look for classes that:
        # 1. Are classes (not functions or variables)
        # 2. Start with uppercase (PascalCase naming convention)
        # 3. Don't start with underscore (private)
        # 4. Are defined in this module (not imported base classes)
        # 5. Have callable constructors (can be instantiated)
        endpoint_classes = [
            name for name in dir(module)
            if name[0].isupper() 
            and not name.startswith('_')
            and not name.endswith('Nullable')
            and inspect.isclass(getattr(module, name))
            and hasattr(getattr(module, name), '__module__')
            and getattr(module, name).__module__ == module.__name__
        ]
        
        if not endpoint_classes:
            print(f"❌ ERROR: No endpoint class found in {module_name}")
            return 0
        
        EndpointClass = getattr(module, endpoint_classes[0])
        
    except (ImportError, AttributeError) as e:
        print(f"❌ ERROR: Could not import {module_name}: {e}")
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
    
    # Get per-call delay from endpoint config (for rate limiting)
    endpoint_config = get_endpoint_config(endpoint_name)
    per_call_delay = 0.0
    if endpoint_config and 'retry_config' in endpoint_config:
        per_call_delay = endpoint_config['retry_config'].get('per_call_delay', 0.0)
    
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
                print(f"  Failed team {team_id}: {te} | Endpoint: {EndpointClass.__name__} | Params: {list(api_params.keys())}")
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
                                    print(f"  WARNING: Column '{col_name}' got non-numeric value: {type(value)}, skipping")
                    
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
    
    conn = get_db_connection()
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
                    print(f"  WARNING: Column '{col}' has dict/list value for entity {entity_id}, converting to None")
                    val = None
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
        print(f"ERROR - Failed {description} (aggregation): {str(e)}")
        conn.rollback()
        return {'updated': 0, 'data_found': False}
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# TRANSFORMATION ROUTER
# ============================================================================

def apply_transformation(
    ctx: ETLContext,
    column_name: str,
    transform: Dict[str, Any],
    season: str,
    entity: Literal['player', 'team'] = 'player',
    table: Optional[str] = None,
    season_type: int = 1,
    season_type_name: str = 'Regular Season',
    source_config: Optional[Dict] = None
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
        from lib.etl import execute_transformation_pipeline
        result = execute_transformation_pipeline(
            ctx, transform, season, entity, season_type, season_type_name
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
    Exits with code 42 to signal external restart wrapper.
    """
    print(f"\n[RESTART] Automatic restart triggered")
    print(f"  Reason: {reason}")
    print(f"  Consecutive failures: {failures} (threshold: {threshold})")
    print(f"  Progress: {progress_msg}")
    print(f"[RESTART] Exiting with code 42 to trigger restart...")
    
    import sys
    sys.exit(42)  # Signal to external restart script


def ensure_schema_exists() -> None:
    """Create database schema if it doesn't exist (first-time setup)"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if tables exist
    players_table = get_table_name('player', 'entity')
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{players_table}'
        )
    """)
    
    if cursor.fetchone()[0]:
        cursor.close()
        conn.close()
        return
    
    print("Creating database schema...")
    
    # Generate and execute schema DDL
    from lib.etl import generate_schema_ddl
    schema_ddl = generate_schema_ddl()
    cursor.execute(schema_ddl)
    conn.commit()
    
    print("Schema created successfully")
    
    cursor.close()
    conn.close()




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
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    current_season = get_season()
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW
    # This is the SOURCE OF TRUTH for current team assignments
    print("Fetching commonteamroster...")
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
                        'jersey_number': safe_str(player_row.get('NUM')),  # Use DB column name from config
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
    conn = get_db_connection()
    cursor = conn.cursor()
    players_table = get_table_name('player', 'entity')
    cursor.execute(f"SELECT player_id FROM {players_table}")
    existing_player_ids = {row[0] for row in cursor.fetchall()}
    
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
        print(f"Processing {len(new_player_ids)} new players...")
        
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
        
        for idx, player_id in enumerate(new_player_ids, 1):
            player_data = all_players[player_id]
            player_name = player_data.get('name', 'Unknown')
            
            print(f"[{idx}/{len(new_player_ids)}] {player_name} (ID: {player_id})")
            
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
                            print(f"  Could not parse FROM_YEAR: {from_year_raw}")
                    
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
                detail_fetch_error = str(e)[:500]
                consecutive_failures += 1  # Track consecutive failures
                print(f"  WARNING - Failed to fetch details: {e}")
                print("  Will insert with basic info only (name, team, jersey)")
                
                # AUTO-RESTART: If hit failure threshold, trigger subprocess restart
                if RESTART_ENABLED and consecutive_failures >= FAILURE_THRESHOLD:
                    # Commit current work before restart
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
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
                    print(f"  Warning: Failed to fetch details for {player_data['name']}, inserted with NULL values")
                
                conn.commit()
                players_added += 1
                
                # Note: Wingspan and other entity details should be populated via
                # a separate one-time script or annual ETL, NOT during backfill
                # (backfill is for stats tables, not entity tables)
                
            except Exception as e:
                print(f"  ERROR - Failed to insert player: {e}")
                conn.rollback()
                continue  # Skip to next player
            
            # Add rate limiting delay between player fetches to avoid overwhelming API
            if idx < len(new_player_ids):
                time.sleep(API_CONFIG.get('rate_limit_delay', 0.6))
                
    update_players_data = []
    
    players_table = get_table_name('player', 'entity')
    cursor.execute(f"SELECT player_id, team_id FROM {players_table}")
    existing_players = {row[0]: row[1] for row in cursor.fetchall()}
    
    for player_id, player_data in all_players.items():
        if player_id in existing_players and player_id not in new_player_ids:
            # Existing player (not newly added) - check if team changed
            if existing_players[player_id] != player_data['team_id']:
                players_updated += 1
            
            # Only update team_id for existing players
            # Detail fields (rookie_year, height, etc.) are already set and shouldn't be overwritten
            update_players_data.append((player_data['team_id'], player_id))
    
    # Bulk update existing players (team_id only)
    if update_players_data:
        update_sql = """
            UPDATE players 
            SET team_id = %s, updated_at = NOW()
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
            print(f"  Cleared team_id for {cleared_count} players no longer on rosters")    
    conn.commit()
    cursor.close()
    conn.close()
    
    # If new players were added, ensure endpoint tracker coverage from earliest rookie year
    if new_player_ids:
        print(f"\nEnsuring endpoint tracker coverage for new players...")
        try:
            from lib.etl import ensure_endpoint_tracker_coverage
            
            # Get earliest rookie year from newly added players
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MIN(rookie_year) 
                FROM players 
                WHERE player_id = ANY(%s) AND rookie_year IS NOT NULL
            """, (new_player_ids,))
            earliest_rookie_year = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if earliest_rookie_year:
                ensure_endpoint_tracker_coverage(earliest_rookie_year)
            else:
                # Fallback to current season if no rookie_year found
                ensure_endpoint_tracker_coverage(NBA_CONFIG['current_season'])
                
        except Exception as tracker_error:
            print(f"  WARNING: Failed to ensure tracker coverage: {tracker_error}")
    
    # NEW BACKFILL: Endpoint-by-endpoint systematic processing
    # Will automatically resume where it left off using endpoint_tracker
    print(f"\nRunning endpoint-by-endpoint backfill...")
    try:
        backfill_all_endpoints(ctx)
        
        # After backfill completes, fetch wingspan for all non-backfilled players
        print(f"\nFetching wingspan data for non-backfilled players...")
        wingspan_updated, wingspan_total = update_wingspan_from_combine(ctx, only_unbackfilled=True)
        if wingspan_total > 0:
            print(f"  Updated wingspan for {wingspan_updated}/{wingspan_total} players")
        
    except Exception as e:
        error_msg = str(e)[:500]
        print(f"  WARNING - Backfill failed: {error_msg}")
        print(f"  Run again to resume where it left off")
    
    return players_added, players_updated, new_player_ids


def update_basic_stats(ctx: ETLContext, entity: Literal['player', 'team'], skip_zero_stats: bool = False, player_ids: Optional[List[int]] = None, season_type: Optional[int] = None) -> bool:
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
        
    Returns:
        True if successful
        
    Usage:
        update_basic_stats(ctx, 'player')  # All players, all season types
        update_basic_stats(ctx, 'player', season_type=1)  # All players, Regular Season only
        update_basic_stats(ctx, 'player', player_ids=[1629632])  # Specific player(s)
        update_basic_stats(ctx, 'team')
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # CRITICAL: Always read from config (don't cache) - backfill modifies NBA_CONFIG dynamically
    current_season = get_season()
    current_year = get_season_year()
    
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
    
    # Get columns from config
    # - basic_cols: Basic stats (no special parameters needed)
    # - advanced_cols: Advanced stats (require measure_type_detailed_defense='Advanced')
    # Both are inserted if available for the season
    basic_cols = get_columns_by_endpoint(endpoint_name, entity, table=table)
    advanced_cols = get_columns_by_endpoint(endpoint_name, entity, table=table, measure_type_detailed_defense='Advanced')
    
    # Combine both basic and advanced columns for INSERT
    # Advanced stats are now available from 2003-04, so include them
    all_cols = {**basic_cols, **advanced_cols}
    
    # CRITICAL: Exclude primary key from all_cols (it's added explicitly to avoid duplicates)
    entity_id_field = get_primary_key(entity)
    all_cols = {k: v for k, v in all_cols.items() if k != entity_id_field}
    
    # Process season types - either specific one or all from config
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
            
            # Fetch basic stats
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
            
            # Fetch advanced stats if configured AND available for this season
            adv_field_names = set()
            for col, cfg in advanced_cols.items():
                src = cfg.get(f'{entity}_source', {})
                params = src.get('params', {})
                if params.get('measure_type_detailed_defense') == 'Advanced' and src.get('field'):
                    adv_field_names.add(src['field'])
            
            if adv_field_names:
                try:
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
                    
                    adv_df = fetch_advanced_stats()                    
                    if not adv_df.empty:
                        # Build merge columns: ID + advanced fields
                        merge_cols = [id_field] + (['TEAM_ID'] if entity == 'player' else []) + sorted(list(adv_field_names))
                        merge_on = [id_field] + (['TEAM_ID'] if entity == 'player' else [])
                        
                        # Verify columns exist before merge
                        missing_cols = [c for c in merge_cols if c not in adv_df.columns]
                        if missing_cols:
                            print(f"  Advanced stats missing columns: {missing_cols}")
                        else:
                            df = df.merge(adv_df[merge_cols], on=merge_on, how='left')
                except Exception as e:
                    print(f"  Warning: Could not fetch advanced stats: {e}")
            
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
                    print(f"  Warning: Could not fetch opponent stats: {e}")
            
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
                            from lib.etl import _is_column_available_for_season
                            # If column isn't available for this season (e.g., putbacks before 2013-14),
                            # leave as NULL instead of defaulting to 0
                            if not _is_column_available_for_season(col_name, current_season):
                                value = None
                                record_values.append(value)
                                continue
                            # Advanced stats: log missing fields for debugging
                            elif field_name in adv_field_names:
                                # Only log once per season to avoid spam
                                if entity_id == list(valid_entity_ids)[0]:
                                    print(f"Advanced field {field_name} missing from DataFrame")
                                value = 0
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
                    
                    record_values.append(value)
                
                records.append(tuple(record_values))
            
            # Add zero-stat records for players without stats (Regular Season only)
            if entity == 'player' and season_type_code == 1 and not skip_zero_stats:
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
            
            # Bulk insert
            if records:
                entity_id_field = get_primary_key(entity)
                db_columns = [entity_id_field, 'year', 'season_type'] + sorted(all_cols.keys())
                
                # Guard against empty column list (would cause SQL syntax error)
                if not all_cols:
                    print(f"\u26a0\ufe0f  WARNING - No columns configured for {entity} in {table}, skipping insert")
                    continue
                
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
                conn.commit()
                total_updated += len(records)
        
        except Exception as e:
            print(f"\u274c ERROR - Error fetching {season_type_name} stats: {e}")
    
    cursor.close()
    conn.close()
    
    return True


def run_endpoint_backfill(
    ctx: ETLContext,
    endpoint: str,
    season: str,
    season_type: int,
    scope: str,
    params: Optional[Dict[str, Any]] = None,
    entity: Optional[Literal['player', 'team']] = None
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
                determined from endpoint config. Use this when calling from team backfill
                to ensure dual-entity endpoints track correctly.
        
    Returns:
        True if successful, False if failed
    """
    from lib.etl import (
        update_backfill_status,
        get_player_ids_for_season,
        get_active_teams,
        get_endpoint_config,
        get_columns_for_endpoint_params,
        get_missing_data_for_retry,
        execute_null_zero_cleanup,
        validate_data_integrity,
        log_missing_data_to_tracker,
        get_backfill_status
    )
    
    if params is None:
        params = {}
    
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
        original_season = NBA_CONFIG['current_season']
        original_year = NBA_CONFIG['current_season_year']
        year = int('20' + season.split('-')[1])
        NBA_CONFIG['current_season'] = season
        NBA_CONFIG['current_season_year'] = year
        
        try:
            
            if scope == 'league':
                # Get columns that will be populated
                columns = get_columns_for_endpoint_params(endpoint, params, entity)
                columns_str = f" ({', '.join(columns)})" if columns else ""
                
                # Log in requested format: Processing {season} {season-type} {endpoint} [{params}] (columns)
                print(f"Processing {season} {season_type_name} {endpoint}{param_desc}{columns_str}")
                
                # SPECIAL CASE: leaguedashplayerstats and leaguedashteamstats are the BASE endpoints
                # They must INSERT records first, then other endpoints UPDATE them
                if endpoint in ['leaguedashplayerstats', 'leaguedashteamstats']:
                    # Use update_basic_stats which handles INSERT properly
                    # This creates the base records with games, minutes, and basic stats
                    # skip_zero_stats=True: Only insert players who actually played this season
                    # (don't create empty records for all players in database)
                    # season_type: Process only this specific season type (don't loop through all)
                    update_basic_stats(ctx, entity, skip_zero_stats=True, season_type=season_type)
                else:
                    # Check if this endpoint has ANY direct extraction columns
                    # If ALL columns are transformations, route to transformation pipeline
                    direct_extraction_cols = get_columns_by_endpoint(endpoint, entity)
                    
                    if not direct_extraction_cols:
                        # ALL columns are transformations - use transformation pipeline
                        print(f"  -> Processing via transformation pipeline (all columns are transformations)...")
                        updated_count = update_transformation_columns(
                            ctx=ctx,
                            season=season,
                            entity=entity,
                            season_type=season_type,
                            season_type_name=season_type_name
                        )
                        print(f"  -> Updated {updated_count} {entity}s via transformation pipeline")
                    else:
                        # Has direct extraction columns - use execute_endpoint()
                        execute_endpoint(
                            ctx=ctx,
                            endpoint_name=endpoint,
                            endpoint_params=params,
                            season=season,
                            entity=entity,
                            season_type=season_type,
                            season_type_name=season_type_name,
                            description=f"{endpoint}{param_desc}"
                        )
                
                # Count how many entities were processed
                # Query the database to count records inserted/updated
                conn = get_db_connection()
                cursor = conn.cursor()
                table = get_table_name(entity, 'stats')
                # Both tables use VARCHAR for year column
                year_field_value = season if entity == 'player' else str(year)
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE year = %s AND season_type = %s",
                    (year_field_value, season_type)
                )
                count = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                if entity == 'player':
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         player_successes=count, players_total=count, params=params, entity='player')
                    print(f"Updated {count} players")
                else:
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         team_successes=count, teams_total=count, params=params, entity='team')
                    print(f"Updated {count} teams")
                
                # SKIP transformation processing during backfill
                # Transformation endpoints will be processed as separate endpoints in order
                # This prevents duplicate API calls and ensures proper ordering
                
                # ====================================================================
                # STEP 4: NULL/Zero Cleanup (Config-Driven)
                # ====================================================================
                print(f"  -> NULL/Zero cleanup...")
                try:
                    rows_cleaned = execute_null_zero_cleanup(endpoint, season, season_type, entity)
                    print(f"  -> Cleaned {rows_cleaned} rows")
                except Exception as e:
                    print(f"  WARNING: NULL cleanup failed: {e}")
                
                # ====================================================================
                # STEP 5: Data Integrity Validation
                # ====================================================================
                print(f"  -> Validating data integrity...")
                try:
                    validation_failures = validate_data_integrity(endpoint, season, season_type, entity)
                    
                    if validation_failures:
                        print(f"  WARNING: {len(validation_failures)} {entity}(s) with validation failures")
                        # Log failures to tracker (removes 'complete' status)
                        log_missing_data_to_tracker(endpoint, season, season_type, validation_failures, params, entity)
                    else:
                        print(f"  -> Validation passed")
                        # Clear missing_data and mark complete
                        log_missing_data_to_tracker(endpoint, season, season_type, {}, params, entity)
                        
                except Exception as e:
                    print(f"  WARNING: Validation failed: {e}")
                    # On validation error, don't mark complete
                    update_backfill_status(endpoint, season, season_type, None, params=params, entity=entity)
                
                return True
                
            elif scope == 'team':
                # Team-by-team processing
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
                
                # ====================================================================
                # NULL/Zero Cleanup (Config-Driven)
                # ====================================================================
                print(f"  -> NULL/Zero cleanup...")
                try:
                    rows_cleaned = execute_null_zero_cleanup(endpoint, season, season_type, entity)
                    print(f"  -> Cleaned {rows_cleaned} rows")
                except Exception as e:
                    print(f"  WARNING: NULL cleanup failed: {e}")
                
                # ====================================================================
                # Data Integrity Validation
                # ====================================================================
                print(f"  -> Validating data integrity...")
                try:
                    validation_failures = validate_data_integrity(endpoint, season, season_type, entity)
                    
                    if validation_failures:
                        print(f"  WARNING: {len(validation_failures)} {entity}(s) with validation failures")
                        # Log failures to tracker (removes 'complete' status)
                        log_missing_data_to_tracker(endpoint, season, season_type, validation_failures, params, entity)
                    else:
                        print(f"  -> Validation passed")
                        # Clear missing_data and mark complete
                        log_missing_data_to_tracker(endpoint, season, season_type, {}, params, entity)
                        
                except Exception as e:
                    print(f"  WARNING: Validation failed: {e}")
                    # On validation error, don't mark complete
                    update_backfill_status(endpoint, season, season_type, None, params=params, entity=entity)
                
                return True
                
            elif scope == 'player':
                # Player-by-player processing
                # First check if this endpoint uses transformation pipeline
                direct_extraction_cols = get_columns_by_endpoint(endpoint, entity)
                
                if not direct_extraction_cols:
                    # ALL columns are transformations - use transformation pipeline
                    # Get columns that will be populated (transformation columns)
                    columns = get_columns_for_endpoint_params(endpoint, params, entity)
                    columns_str = f" ({', '.join(columns)})" if columns else ""
                    
                    print(f"Processing {season} {season_type_name} {endpoint}{param_desc}{columns_str}")
                    print(f"  -> Processing via transformation pipeline (all columns are transformations)...")
                    
                    updated_count = update_transformation_columns(
                        ctx=ctx,
                        season=season,
                        entity=entity,
                        season_type=season_type,
                        season_type_name=season_type_name
                    )
                    
                    update_backfill_status(endpoint, season, season_type, 'complete',
                                         player_successes=updated_count, players_total=updated_count, params=params, entity='player')
                    print(f"Updated {updated_count} players")
                else:
                    # Has direct extraction columns - process player-by-player
                    player_team_ids = get_player_ids_for_season(season, season_type)
                    total_players = len(player_team_ids)
                    successes = 0
                    
                    # Get columns that will be populated
                    columns = get_columns_for_endpoint_params(endpoint, params, entity)
                    columns_str = f" ({', '.join(columns)})" if columns else ""
                    
                    # Log in requested format: Processing {season} {season-type} {endpoint} [{params}] (columns)
                    print(f"Processing {season} {season_type_name} {endpoint}{param_desc}{columns_str}")
                    
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
                
                # ====================================================================
                # NULL/Zero Cleanup (Config-Driven)
                # ====================================================================
                print(f"  -> NULL/Zero cleanup...")
                try:
                    rows_cleaned = execute_null_zero_cleanup(endpoint, season, season_type, entity)
                    print(f"  -> Cleaned {rows_cleaned} rows")
                except Exception as e:
                    print(f"  WARNING: NULL cleanup failed: {e}")
                
                # ====================================================================
                # Data Integrity Validation
                # ====================================================================
                print(f"  -> Validating data integrity...")
                try:
                    validation_failures = validate_data_integrity(endpoint, season, season_type, entity)
                    
                    if validation_failures:
                        print(f"  WARNING: {len(validation_failures)} {entity}(s) with validation failures")
                        # Log failures to tracker (removes 'complete' status)
                        log_missing_data_to_tracker(endpoint, season, season_type, validation_failures, params, entity)
                    else:
                        print(f"  -> Validation passed")
                        # Clear missing_data and mark complete
                        log_missing_data_to_tracker(endpoint, season, season_type, {}, params, entity)
                        
                except Exception as e:
                    print(f"  WARNING: Validation failed: {e}")
                    # On validation error, don't mark complete
                    update_backfill_status(endpoint, season, season_type, None, params=params, entity=entity)
                
                return True
                
        finally:
            # Always restore original config
            NBA_CONFIG['current_season'] = original_season
            NBA_CONFIG['current_season_year'] = original_year
            
    except Exception as e:
        print(f"Failed: {e}")
        update_backfill_status(endpoint, season, season_type, 'failed', params=params, entity=entity)
        return False


def backfill_all_endpoints(ctx: ETLContext, start_season: Optional[str] = None) -> None:
    """
    NEW ENDPOINT-BY-ENDPOINT BACKFILL ORCHESTRATOR.
    
    Processes all endpoints systematically:
    1. Get ordered endpoint list (league → team → player)
    2. For each endpoint:
       - Process ALL seasons for that endpoint
       - League scope first (efficient batch processing)
       - Then teams (per-team calls)
       - Finally players (per-player calls)
    3. Track progress in endpoint_tracker
    4. Mark players as backfilled when ALL endpoints complete
    
    Args:
        ctx: ETL context
        start_season: First season to process (default: NBA_CONFIG['backfill_start_season'])
    """
    from lib.etl import (
        get_endpoint_processing_order,
        calculate_current_season,
        get_backfill_status,
        get_columns_for_endpoint_params
    )
    from config.etl import ENDPOINTS_CONFIG, SEASON_TYPE_CONFIG
    
    if start_season is None:
        start_season = NBA_CONFIG['backfill_start_season']
    
    current_season = calculate_current_season()
    
    # CRITICAL: Backfill stops at PREVIOUS season (current handled by daily ETL)
    # Calculate previous season
    current_year = int('20' + current_season.split('-')[1])
    max_backfill_year = current_year - 1
    max_backfill_season = f"{max_backfill_year-1}-{str(max_backfill_year)[-2:]}"
    
    # Parse season years
    start_year = int('20' + start_season.split('-')[1])
    
    # Generate all seasons UP TO previous season (not including current)
    all_seasons = []
    for year in range(start_year, max_backfill_year + 1):
        season_str = f"{year-1}-{str(year)[-2:]}"
        all_seasons.append(season_str)
    
    # Get ordered endpoint list
    endpoints = get_endpoint_processing_order()
    
    print(f"\n{'='*70}")
    print("[START] BACKFILL: Endpoint-by-Endpoint Processing")
    print(f"{'='*70}")
    print(f"  Endpoints: {len(endpoints)}")
    print(f"  Season Range: {start_season} to {max_backfill_season}")
    print(f"  Current Season: {current_season} (handled by daily ETL)")
    print(f"  Total Work: {len(endpoints)} endpoints x {len(all_seasons)} seasons x 3 types")
    print(f"{'='*70}")
    
    total_processed = 0
    total_failed = 0
    
    # OPTIMIZATION: Check if we can skip to first incomplete work
    # This makes restarts nearly instant by avoiding iteration through completed work
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT endpoint, year, season_type, params
        FROM endpoint_tracker
        WHERE status = 'complete' AND entity = 'player' AND (missing_data IS NULL OR missing_data = 'null'::jsonb)
        ORDER BY endpoint, year, season_type, params
    """)
    completed_combinations = {
        (row[0], row[1], row[2], row[3]) 
        for row in cursor.fetchall()
    }
    cursor.close()
    conn.close()
    
    # Count how many we can skip
    total_combinations = 0
    for endpoint_name in endpoints:
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint_name, {})
        min_season = endpoint_config.get('min_season')
        entity_types = endpoint_config.get('entity_types', ['player'])
        from lib.etl import get_endpoint_parameter_combinations
        param_combinations = get_endpoint_parameter_combinations(endpoint_name, entity_types[0])
        
        for params in param_combinations:
            for season in all_seasons:
                season_year = int('20' + season.split('-')[1])
                if min_season:
                    min_year = int('20' + min_season.split('-')[1])
                    if season_year < min_year:
                        continue
                
                for season_type_name, config in SEASON_TYPE_CONFIG.items():
                    season_type = config['season_code']
                    minimum_season = config.get('minimum_season')
                    if minimum_season:
                        min_type_year = int('20' + minimum_season.split('-')[1])
                        if season_year < min_type_year:
                            continue
                    
                    import json
                    params_str = json.dumps(params, sort_keys=True) if params else '{}'
                    if (endpoint_name, season, season_type, params_str) in completed_combinations:
                        total_processed += 1
                    total_combinations += 1
    
    if total_processed > 0:
        print(f"Skipping {total_processed}/{total_combinations} already-complete combinations")
        print()
    
    # Process each endpoint completely before moving to next
    for endpoint_name in endpoints:
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint_name, {})
        min_season = endpoint_config.get('min_season')
        entity_types = endpoint_config.get('entity_types', ['player'])
        
        # Infer scope from endpoint name
        from lib.etl import infer_execution_tier_from_endpoint, get_endpoint_parameter_combinations
        scope = infer_execution_tier_from_endpoint(endpoint_name)
        
        # Get all parameter combinations for this endpoint
        # This discovers different pt_measure_type, measure_type_detailed_defense, etc.
        param_combinations = get_endpoint_parameter_combinations(endpoint_name, entity_types[0])
        
        # Check if there's ANY incomplete work for this endpoint before printing header
        # This prevents empty endpoint sections in the output
        import json
        has_incomplete_work = False
        for params in param_combinations:
            params_str = json.dumps(params, sort_keys=True) if params else '{}'
            for season in all_seasons:
                season_year = int('20' + season.split('-')[1])
                
                # Skip if before min_season
                if min_season:
                    min_year = int('20' + min_season.split('-')[1])
                    if season_year < min_year:
                        continue
                
                for season_type_name, config in SEASON_TYPE_CONFIG.items():
                    season_type = config['season_code']
                    minimum_season = config.get('minimum_season')
                    
                    # Skip if season type doesn't exist yet
                    if minimum_season:
                        min_type_year = int('20' + minimum_season.split('-')[1])
                        if season_year < min_type_year:
                            continue
                    
                    # Check if this combination needs processing
                    if (endpoint_name, season, season_type, params_str) not in completed_combinations:
                        has_incomplete_work = True
                        break
                
                if has_incomplete_work:
                    break
            
            if has_incomplete_work:
                break
        
        # Skip this endpoint entirely if all work is complete
        if not has_incomplete_work:
            continue
        
        # Print header only if we have work to do
        if len(param_combinations) > 1:
            print(f"\n{'='*70}")
            print(f"ENDPOINT: {endpoint_name} (scope: {scope}, {len(param_combinations)} param combinations)")
            print(f"{'='*70}")
        else:
            print(f"\n{'='*70}")
            print(f"ENDPOINT: {endpoint_name} (scope: {scope})")
            print(f"{'='*70}")
        
        # Process each parameter combination
        for params in param_combinations:
            param_desc = ""
            if params:
                # Show main parameter for display
                for key in ['pt_measure_type', 'measure_type_detailed_defense', 'defense_category']:
                    if key in params:
                        param_desc = f" [{params[key]}]"
                        break
            
            # Process all seasons for this endpoint+params combination
            for season in all_seasons:
                season_year = int('20' + season.split('-')[1])
                
                # Skip if before min_season
                if min_season:
                    min_year = int('20' + min_season.split('-')[1])
                    if season_year < min_year:
                        continue
                
                # Process all season types for this season
                # Track if we printed anything for this season to control spacing
                printed_for_season = False
                
                for season_type_idx, (season_type_name, config) in enumerate(SEASON_TYPE_CONFIG.items()):
                    season_type = config['season_code']
                    minimum_season = config.get('minimum_season')
                    
                    # Skip if season type doesn't exist yet
                    if minimum_season:
                        min_type_year = int('20' + minimum_season.split('-')[1])
                        if season_year < min_type_year:
                            continue
                    
                    # Check if already complete (quick in-memory check now)
                    import json
                    params_str = json.dumps(params, sort_keys=True) if params else '{}'
                    if (endpoint_name, season, season_type, params_str) in completed_combinations:
                        # Skip silently - already counted in summary above
                        continue
                    
                    # Add spacing before processing (only if we've printed something already)
                    if printed_for_season:
                        print()
                    
                    # Process this endpoint/season/season_type/params combination
                    success = run_endpoint_backfill(ctx, endpoint_name, season, season_type, scope, params)
                    printed_for_season = True
                    
                    if success:
                        total_processed += 1
                        # Add to completed set so we skip it on next iteration
                        completed_combinations.add((endpoint_name, season, season_type, params_str))
                    else:
                        total_failed += 1
                        if API_CONFIG['api_failure_threshold'] == 1:
                            print(f"\n{'='*70}")
                            print("BACKFILL STOPPED - Failure threshold reached (1)")
                            print(f"{'='*70}")
                            print(f"Processed: {total_processed}")
                            print(f"Failed: {total_failed}")
                            print(f"Resume by running again - will pick up where it left off")
                            # Raise exception to stop entire ETL and force restart
                            param_str = f" {params}" if params else ""
                            raise RuntimeError(
                                f"Backfill failed at {endpoint_name}{param_str} {season} {season_type_name}. "
                                "Run ETL again to resume from this point."
                            )
    
    print(f"\n{'='*70}")
    print("BACKFILL COMPLETE")
    print(f"{'='*70}")
    print(f"Processed: {total_processed}")
    print(f"Failed: {total_failed}")
    
    # Mark players as backfilled=true when ALL endpoints complete for them
    print("\nMarking players as backfilled...")
    try:
        from lib.etl import mark_backfill_complete
        mark_backfill_complete()
    except Exception as e:
        print(f"  WARNING: Failed to mark players as backfilled: {e}")


def update_transformation_columns(
    ctx: ETLContext,
    season: str,
    entity: Literal['player', 'team'] = 'player',
    table: Optional[str] = None,
    season_type: int = 1,
    season_type_name: str = 'Regular Season'
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
    """
    # Derive year from season string
    season_year = int('20' + season.split('-')[1])
    
    # Default table if not provided
    if table is None:
        table = get_table_name(entity, contents='stats')
    
    # Both player and team tables use season format
    year_value = season  # '2007-08'
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Discover transformation groups dynamically from DB_COLUMNS
    # Group transformations by their 'group' parameter
    groups_map = {}  # group_name -> list of (col_name, transform)
    ungrouped_transforms = []
    
    for col_name, col_meta in DB_COLUMNS.items():
        # Skip if col_meta is not a dict (defensive programming)
        if not isinstance(col_meta, dict):
            continue
        # Check if column has a source for this entity
        # DB_COLUMNS uses player_source, team_source, opponent_source to indicate applicability
        source_key = f'{entity}_source'
        source_config = col_meta.get(source_key)
        if not source_config:
            continue
        
        # Skip if source_config is just a string (endpoint name without transformation)
        if isinstance(source_config, str):
            continue
        
        # Check if this source has a transformation
        transform = source_config.get('transformation')
        if not transform:
            continue
        
        # Get execution_tier from source_config or transformation
        execution_tier = source_config.get('execution_tier') or transform.get('execution_tier', 'league')
        
        if entity == 'team' and execution_tier == 'player':
            # Exception: filter_aggregate transforms can work for both via team_endpoint
            if transform.get('type') != 'filter_aggregate' or not transform.get('team_endpoint'):
                continue
        
        # Check if transformation specifies a DIFFERENT entity (e.g., player-only transform when we're running for teams)
        # If no entity is specified, it applies to all entities in DB_COLUMNS
        transform_entity = transform.get('entity')
        if transform_entity is not None and transform_entity != entity:
            continue
        
        # Check if transformation belongs to a group
        group_name = transform.get('group')
        if group_name:
            if group_name not in groups_map:
                groups_map[group_name] = []
            groups_map[group_name].append((col_name, transform))
        else:
            ungrouped_transforms.append(col_name)
    
    # Convert groups_map to list format
    applicable_groups = [(group_name, group_transforms) for group_name, group_transforms in groups_map.items()]
    
    total_transforms = sum(len(group_transforms) for _, group_transforms in applicable_groups) + len(ungrouped_transforms)
    
    if total_transforms == 0:
        return 0
    
    total_updated = 0
    
    total_updated = 0
    
    # Execute all transformations through unified pipeline
    # Automatic caching prevents duplicate API calls for same endpoint+params
    all_transforms = []
    
    # Collect all transforms (grouped and ungrouped)
    for group_name, group_transforms in applicable_groups:
        for col_name, transform in group_transforms:
            all_transforms.append(col_name)
    
    all_transforms.extend(ungrouped_transforms)
    
    # Execute each transformation - caching happens automatically
    for col_name in all_transforms:
        try:
            source_key = f'{entity}_source'
            source_config = DB_COLUMNS[col_name][source_key]
            transform = source_config['transformation']

            if transform.get('group'):
                continue

            endpoint_name = transform.get('endpoint') or source_config.get('endpoint')
            endpoint_params_from_source = source_config.get('params', {})
            if endpoint_name and not is_endpoint_available_for_season(endpoint_name, season, endpoint_params_from_source):
                continue
            
            print(f"    {col_name}")

            try:
                data = apply_transformation(ctx, col_name, transform, season, entity, table, season_type, season_type_name, source_config)
                
                # DEBUG: Show what data was returned
                if isinstance(data, dict):
                    print(f"  DEBUG: Transformation returned {len(data)} entities")
                    if len(data) > 0:
                        # Show first 3 values
                        sample_items = list(data.items())[:3]
                        print(f"  DEBUG: Sample values: {dict(sample_items)}")
                        # Check for any non-zero values
                        non_zero = sum(1 for v in data.values() if v and v != 0)
                        print(f"  DEBUG: Non-zero values: {non_zero}/{len(data)}")
                else:
                    print(f"  DEBUG: Transformation returned non-dict: {type(data)}")
                    
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

        except Exception as e:
            print(f"    Error processing {col_name}: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            
            # Log errors for all affected players
            # Transformation endpoint failed for this column
            print("    Transformation endpoint failed")
    
    # CLEANUP: Convert NULLs to 0 for any row with minutes > 0
    # This ensures data quality - if a player/team played, missing stats should be 0, not NULL
    # IMPORTANT: Only convert NULL to 0 for columns that are actually available in this season
    try:
        from lib.etl import _is_column_available_for_season
        
        # Get all transformation columns (columns with transformations in config)
        transform_cols = [
            col_name for col_name, col_def in DB_COLUMNS.items()
            if isinstance(col_def, dict)
            and col_def.get('table') == table 
            and f'{entity}_source' in col_def 
            and 'transformation' in col_def[f'{entity}_source']
            and _is_column_available_for_season(col_name, season)  # Only include if available for this season
        ]
        
        if transform_cols:
            # Build SET clause to update all NULL transformation columns to 0
            set_clause = ", ".join([f"{quote_column(col)} = COALESCE({quote_column(col)}, 0)" for col in transform_cols])
            
            if entity == 'player':
                cursor.execute(f"""
                    UPDATE {table}
                    SET {set_clause}, updated_at = NOW()
                    WHERE year = %s::text AND season_type = %s AND minutes_x10 > 0
                """, (year_value, season_type))
            else:  # team
                cursor.execute(f"""
                    UPDATE {table}
                    SET {set_clause}, updated_at = NOW()
                    WHERE year = %s::text AND season_type = %s AND minutes_x10 > 0
                """, (year_value, season_type))
            
            nulls_fixed = cursor.rowcount
            if nulls_fixed > 0:
                print(f"  Fixed {nulls_fixed} NULL values to 0")
            conn.commit()
            
    except Exception as e:
        print(f"  Failed NULL cleanup: {e}")
        conn.rollback()
    
    cursor.close()
    conn.close()
    
    return total_updated


def update_advanced_stats(ctx: ETLContext, entity: Literal['player', 'team'], season: Optional[str] = None, 
                         season_types_to_process: Optional[set] = None) -> None:
    """
    FULLY CONFIG-DRIVEN ADVANCED STATS ETL for both players and teams.
    Automatically discovers which endpoints to call from DB_COLUMNS.
    No hardcoding - adding new stats only requires config updates.
    
    This consolidates update_player_advanced_stats and update_team_advanced_stats
    using a single entity parameter to handle both cases.
    
    Args:
        ctx: ETLContext for state management
        entity: 'player' or 'team' - determines which entity to process
        season: Season string (defaults to current season)
        season_types_to_process: Optional set of season_type codes (1, 2, 3) to process
                                If None, processes all season types. Used to skip empty season types.
    
    Total time: ~8-10 minutes per season (players), ~2-3 minutes (teams)
    """
    if season is None:
        season = get_season()
    
    # Determine entity-specific settings
    source_key = f'{entity}_source'
    table = get_table_name(entity, 'stats')
    basic_endpoint = 'leaguedashplayerstats' if entity == 'player' else 'leaguedashteamstats'
    
    start_time = time.time()
    
    try:
        # PHASE 1: Discover and execute league-wide/per-team endpoints using generic executor
        # Group endpoints by (endpoint_name, params) for deduplication
        endpoint_calls = {}
        
        for col_name, col_config in DB_COLUMNS.items():
            # Skip if col_config is not a dict (defensive programming)
            if not isinstance(col_config, dict):
                continue
            # Get entity source configuration
            entity_source = col_config.get(source_key, {})
            
            # Skip if not a dict (some fields have string sources)
            if not isinstance(entity_source, dict):
                continue
            
            endpoint_name = entity_source.get('endpoint')
            
            # Skip if no endpoint
            if not endpoint_name:
                continue
            
            # Skip basic stats endpoint (handled by update_basic_stats)
            if endpoint_name == basic_endpoint:
                continue
            
            # Check if this endpoint has data for this season (check params for Advanced stats)
            endpoint_params_from_source = entity_source.get('params', {})
            if not is_endpoint_available_for_season(endpoint_name, season, endpoint_params_from_source):
                continue
            
            # Skip if not API column
            if not col_config.get('api', False):
                continue
            
            # Skip annual fields (handled by annual ETL)
            if col_config.get('update_frequency') == 'annual':
                continue
            
            # Skip endpoints with transformations (handled by update_transformation_columns)
            if entity_source.get('transformation'):
                continue
            
            # For players: Skip per-player endpoints (handled by transformations)
            # Per-player endpoints require player_id parameter and must be run in subprocesses
            if entity == 'player':
                tier = infer_execution_tier_from_endpoint(endpoint_name)
                if tier == 'player':
                    continue  # Will be handled by transformations
            
            # Get params from config (new consistent structure)
            params = entity_source.get('params', {})
            
            # Create unique key for this endpoint call
            params_tuple = tuple(sorted(params.items()))
            key = (endpoint_name, params_tuple)
            
            if key not in endpoint_calls:
                endpoint_calls[key] = params
        
        total_updated = 0
        
        # Loop through all season types (from config)
        for season_type_name, season_type_config in SEASON_TYPE_CONFIG.items():
            season_type_code = season_type_config['season_code']
            min_season = season_type_config.get('minimum_season')
            
            # Skip if not in season_types_to_process set (optimization for backfill)
            if season_types_to_process is not None and season_type_code not in season_types_to_process:
                continue
            
            # Skip season type if current season is before minimum_season
            if min_season:
                min_year = int('20' + min_season.split('-')[1])
                season_year = int('20' + season.split('-')[1])
                if season_year < min_year:
                    continue
            
            # Execute all discovered endpoints
            for (endpoint_name, params_tuple), params in sorted(endpoint_calls.items()):
                # Generate description from params
                pt_measure = params.get('pt_measure_type')
                measure_detailed = params.get('measure_type_detailed_defense')
                defense_cat = params.get('defense_category')
                
                if pt_measure:
                    description = f"{endpoint_name}.{pt_measure}"
                elif measure_detailed:
                    description = f"{endpoint_name} ({measure_detailed})"
                elif defense_cat:
                    description = f"{endpoint_name} ({defense_cat})"
                else:
                    description = f"{endpoint_name}"
                
                # Use execute_endpoint - auto-dispatches to league-wide or per-team
                try:
                    updated = execute_endpoint(
                        ctx,
                        endpoint_name=endpoint_name,
                        endpoint_params=params,
                        season=season,
                        entity=entity,
                        table=table,
                        season_type=season_type_code,
                        season_type_name=season_type_name,
                        description=description
                    )
                    if updated:
                        total_updated += updated
                except Exception as endpoint_error:
                    print(f"    {endpoint_name} failed: {endpoint_error}")
                    
                    # Log errors for all columns that depend on this endpoint
                    if entity == 'player':
                        affected_columns = []
                        for col_name, col_config in DB_COLUMNS.items():
                            if not isinstance(col_config, dict):
                                continue
                            entity_source = col_config.get(source_key, {})
                            if isinstance(entity_source, dict) and entity_source.get('endpoint') == endpoint_name:
                                # Check if params match
                                col_params = entity_source.get('params', {})
                                if dict(params_tuple) == col_params:
                                    affected_columns.append(col_name)
                        
                        # Get all players for this season/season_type
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        try:
                            cursor.execute(f"""
                                SELECT player_id FROM {table}
                                WHERE year = %s AND season_type = %s
                            """, (season, season_type_code))
                            print(f"          Endpoint {endpoint_name} failed for {cursor.rowcount} players: {str(endpoint_error)}")
                        finally:
                            cursor.close()
                            conn.close()
                    
                    continue
            
            # PHASE 2: Apply all configured transformations
            # For players: replaces update_shooting_tracking_bulk, update_putbacks_per_player, update_onoff_stats
            # For teams: handles all complex team stats requiring post-processing
            # 100% config-driven from TRANSFORMATIONS dict
            update_transformation_columns(
                ctx,
                season=season,
                entity=entity,
                table=table,
                season_type=season_type_code,
                season_type_name=season_type_name
            )
        
        elapsed = time.time() - start_time
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"ERROR - Advanced {entity} stats failed after {elapsed:.1f}s: {e}")
        raise


def run_daily_etl(ctx: ETLContext, backfill_start: Optional[int] = None, backfill_end: Optional[int] = None) -> None:
    """
    Main daily ETL orchestrator.
    Now includes advanced stats (~10 minutes total).
    
    Args:
        ctx: ETLContext for state management (required)
        backfill_start: Start year for historical backfill (None = no backfill)
        backfill_end: End year for backfill (None = current season)
    """
    print("=" * 70)
    if backfill_start:
        print(f"THE GLASS - ETL BACKFILL {backfill_start}-{backfill_end or NBA_CONFIG['current_season_year']}")
    else:
        print("THE GLASS - DAILY ETL STARTED")
    print("=" * 70)
    start_time = time.time()
    
    # If backfill requested, process multiple seasons
    if backfill_start:
        current_year = NBA_CONFIG['current_season_year']
        end_year = backfill_end or current_year
        num_seasons = end_year - backfill_start + 1
        
        print(f"Backfill: Processing {num_seasons} seasons from {backfill_start} to {end_year}")
        
        for year in range(backfill_start, end_year + 1):
            season = f"{year-1}-{str(year)[-2:]}"
            print("="*70)
            print(f"Processing season {year - backfill_start + 1}/{num_seasons}: {season} (year={year})")
            print("="*70)
            
            try:
                # Temporarily override current season config for this backfill iteration
                original_season = NBA_CONFIG['current_season']
                original_year = NBA_CONFIG['current_season_year']
                NBA_CONFIG['current_season'] = season
                NBA_CONFIG['current_season_year'] = year
                
                # STEP 1: Player Stats (reuse existing function)
                # Skip adding zero-stat records in backfill - only update players who played
                update_basic_stats(ctx, 'player', skip_zero_stats=True)
                
                # STEP 2: Team Stats (reuse existing function)
                update_basic_stats(ctx, 'team')
                
                # STEP 3: Advanced stats (only if endpoints available for this season)
                # Check using a representative advanced stats endpoint
                if is_endpoint_available_for_season('leaguedashptstats', season):
                    try:
                        update_advanced_stats(ctx, 'player', season=season)
                        update_advanced_stats(ctx, 'team', season=season)
                    except Exception as e:
                        print(f"    ⚠️ Failed advanced stats: {e}")
                
                # STEP 4: Transformation columns
                # Config-driven: loop through all season types, respecting minimum_season
                try:
                    for season_type_name, season_type_config in SEASON_TYPE_CONFIG.items():
                        season_type_id = season_type_config['season_code']
                        min_season = season_type_config.get('minimum_season')
                        
                        # Skip season type if current season is before minimum_season
                        if min_season:
                            min_year = int('20' + min_season.split('-')[1])
                            season_year = int('20' + season.split('-')[1])
                            if season_year < min_year:
                                continue
                        
                        # Player transformations
                        updated_player = update_transformation_columns(
                            ctx, season, 'player', 
                            season_type=season_type_id, 
                            season_type_name=season_type_name
                        )
                        if updated_player > 0:
                            print(f"    Updated {updated_player} player transformation columns ({season_type_name})")
                        
                        # Team transformations (if any exist in config)
                        updated_team = update_transformation_columns(
                            ctx, season, 'team',
                            season_type=season_type_id,
                            season_type_name=season_type_name
                        )
                        if updated_team > 0:
                            print(f"    Updated {updated_team} team transformation columns ({season_type_name})")
                except Exception as e:
                    print(f"    ⚠️ Failed transformations: {e}")
                
                # Restore original config
                NBA_CONFIG['current_season'] = original_season
                NBA_CONFIG['current_season_year'] = original_year
                
                print("Season {} complete".format(season))
                
            except Exception as e:
                print(f"❌ Failed to process season {season}: {e}")
                import traceback
                print(traceback.format_exc())
                # Restore config even on error
                NBA_CONFIG['current_season'] = original_season
                NBA_CONFIG['current_season_year'] = original_year
                continue
        
        # Close progress bars
        ctx.close()
        
        elapsed = time.time() - start_time
        print("=" * 70)
        print(f"BACKFILL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        print("=" * 70)
        return
    
    # Normal daily ETL (current season only)
    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()
        
        # Initialize parallel executor for roster fetching (per-team tier: 30 API calls)
        ctx.init_parallel_executor(max_workers=10, endpoint_tier='team')
        
        # STEP 1: Player Rosters (now includes atomic backfill for each new player)
        # New players are added one at a time with immediate backfill
        # This calls backfill_all_endpoints() which uses endpoint_tracker
        players_added, players_updated, new_player_ids = update_player_rosters(ctx)
        
        # STEP 2: Process current season using backfill orchestrator
        # This uses endpoint_tracker for all operations and can resume on restart
        current_season = get_season()
        
        print(f"\n{'='*70}")
        print(f"[DAILY ETL] Processing current season: {current_season}")
        print(f"{'='*70}")
        print("Using endpoint_tracker - can resume if interrupted")
        
        # Run backfill for current season only
        # This will process all endpoints (basic stats, advanced stats, transformations)
        # and track progress in endpoint_tracker
        from lib.etl import ensure_endpoint_tracker_coverage
        
        # Ensure endpoint_tracker has entries for current season
        ensure_endpoint_tracker_coverage(current_season)
        
        # Run the backfill orchestrator for current season only
        backfill_all_endpoints(ctx, start_season=current_season)
        
        print(f"\n[DAILY ETL] Current season processing complete")
        
        # STEP 3: Clean up NULL/0 values for players after ALL stats are loaded
        # This ensures proper data quality: games=0 -> NULLs, games>0 -> 0s
        print("Cleaning up NULL/0 values for current season...")
        try:
            from lib.etl import get_columns_for_null_cleanup
            
            year_value = current_season
            
            # Get columns eligible for NULL to 0 conversion (respects min_season)
            stat_columns = get_columns_for_null_cleanup(current_season, entity='player')
            
            if stat_columns:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # Convert NULLs to 0 for players who played
                set_clause_nulls = ', '.join([f'"{col}" = COALESCE("{col}", 0)' for col in stat_columns])
                cursor.execute(f"""
                    UPDATE player_season_stats
                    SET {set_clause_nulls}
                    WHERE year = %s 
                    AND games > 0
                """, (year_value,))
                
                # Convert 0s to NULLs for players who didn't play
                set_clause_zeros = ', '.join([f'"{col}" = NULLIF("{col}", 0)' for col in stat_columns])
                cursor.execute(f"""
                    UPDATE player_season_stats
                    SET {set_clause_zeros}
                    WHERE year = %s 
                    AND games = 0
                """, (year_value,))
                
                conn.commit()
                cursor.close()
                conn.close()
                
                print("NULL/0 cleanup complete")
        except Exception as e:
            print(f"Failed to clean up NULL/0 values: {e}")
        
        # STEP 4: Reset current season endpoints to 'ready' for tomorrow's run
        # Historical seasons remain marked as 'complete'
        try:
            from lib.etl import reset_current_season_endpoints
            print("\nResetting current season endpoint statuses for tomorrow...")
            reset_current_season_endpoints(current_season)
        except Exception as e:
            print(f"WARNING: Failed to reset current season endpoints: {e}")
        
        # Close progress bars
        ctx.close()
        
        elapsed = time.time() - start_time
        print("=" * 70)
        print(f"DAILY ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        print("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print("=" * 70)
        print(f"DAILY ETL FAILED - {elapsed:.1f}s")
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
    
    conn = get_db_connection()
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
    conn.close()
    
    return deleted_count

def update_all_player_details(ctx: ETLContext) -> None:
    """
    Config-driven player details updater.
    Discovers which fields to update from DB_COLUMNS (update_frequency='annual', table='entity').
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Discover fields from config: annual update frequency, entity table, has player_source
    annual_fields = {}
    endpoints_needed = set()
    
    for col_name, col_config in DB_COLUMNS.items():
        # Skip if col_config is not a dict (defensive programming)
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
    
    # Get all players in database
    players_table = get_table_name('player', 'entity')
    cursor.execute(f"SELECT player_id, name FROM {players_table} ORDER BY player_id")
    all_players = cursor.fetchall()
    total_players = len(all_players)
    
    updated_count = 0
    failed_count = 0
    consecutive_failures = 0
    retry_queue = []
    
    for idx, (player_id, player_name) in enumerate(all_players):
        if idx > 0 and idx % 50 == 0:
            consecutive_failures = 0
        
        if consecutive_failures >= API_CONFIG['max_consecutive_failures']:
            print(f"WARNING - Taking {API_CONFIG['cooldown_after_batch_seconds']}s break after {API_CONFIG['max_consecutive_failures']} consecutive failures")
            time.sleep(API_CONFIG['cooldown_after_batch_seconds'])
            consecutive_failures = 0
        
        # Try to fetch details with exponential backoff
        for attempt in range(RETRY_CONFIG['max_retries']):
            try:
                # Fetch data from all needed endpoints
                endpoint_data = {}
                for endpoint in endpoints_needed:
                    if endpoint == 'commonplayerinfo':
                        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=API_CONFIG['timeout_default'])
                        info_df = player_info.get_data_frames()[0]
                        if not info_df.empty:
                            endpoint_data[endpoint] = info_df.iloc[0]
                
                if not endpoint_data:
                    break
                
                # Extract values using config
                values = {}
                for col_name, field_config in annual_fields.items():
                    endpoint = field_config['endpoint']
                    if endpoint in endpoint_data:
                        row = endpoint_data[endpoint]
                        api_field = field_config['field']
                        transform_name = field_config['transform']
                        
                        # Apply transformation
                        raw_value = row.get(api_field)
                        if transform_name == 'safe_int':
                            values[col_name] = safe_int(raw_value)
                        elif transform_name == 'safe_float':
                            values[col_name] = safe_float(raw_value)
                        elif transform_name == 'safe_str':
                            values[col_name] = safe_str(raw_value)
                        elif transform_name == 'parse_height':
                            values[col_name] = parse_height(raw_value)
                        elif transform_name == 'parse_birthdate':
                            values[col_name] = parse_birthdate(raw_value)
                        else:
                            values[col_name] = raw_value
                
                # Build dynamic UPDATE statement
                if values:
                    set_clauses = [f"{col} = %s" for col in values.keys()]
                    set_clauses.append("updated_at = NOW()")
                    
                    sql = f"""
                        UPDATE players
                        SET {', '.join(set_clauses)}
                        WHERE player_id = %s
                    """
                    
                    cursor.execute(sql, list(values.values()) + [player_id])
                    updated_count += 1
                    consecutive_failures = 0
                    time.sleep(RATE_LIMIT_DELAY)
                break
                
            except Exception:
                consecutive_failures += 1
                if attempt >= RETRY_CONFIG['max_retries'] - 1:
                    failed_count += 1
                    retry_queue.append((player_id, player_name))    
    # Retry failed players at the end
    if retry_queue:
        print(f"\n  Retrying {len(retry_queue)} failed players...")
        for player_id, player_name in retry_queue:
            try:
                # Fetch data from all needed endpoints
                endpoint_data = {}
                for endpoint in endpoints_needed:
                    if endpoint == 'commonplayerinfo':
                        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=API_CONFIG['timeout_default'])
                        info_df = player_info.get_data_frames()[0]
                        if not info_df.empty:
                            endpoint_data[endpoint] = info_df.iloc[0]
                
                if not endpoint_data:
                    continue
                
                # Extract values using config
                values = {}
                for col_name, field_config in annual_fields.items():
                    endpoint = field_config['endpoint']
                    if endpoint in endpoint_data:
                        row = endpoint_data[endpoint]
                        api_field = field_config['field']
                        transform_name = field_config['transform']
                        
                        # Apply transformation
                        raw_value = row.get(api_field)
                        if transform_name == 'safe_int':
                            values[col_name] = safe_int(raw_value)
                        elif transform_name == 'safe_float':
                            values[col_name] = safe_float(raw_value)
                        elif transform_name == 'safe_str':
                            values[col_name] = safe_str(raw_value)
                        elif transform_name == 'parse_height':
                            values[col_name] = parse_height(raw_value)
                        elif transform_name == 'parse_birthdate':
                            values[col_name] = parse_birthdate(raw_value)
                        else:
                            values[col_name] = raw_value
                
                # Build dynamic UPDATE statement
                if values:
                    set_clauses = [f"{col} = %s" for col in values.keys()]
                    set_clauses.append("updated_at = NOW()")
                    
                    sql = f"""
                        UPDATE players
                        SET {', '.join(set_clauses)}
                        WHERE player_id = %s
                    """
                    
                    cursor.execute(sql, list(values.values()) + [player_id])
                    updated_count += 1
                    failed_count -= 1
                    print(f"Retry success: {player_name}")
                    
            except Exception as e:
                print(f"  ✗ Retry failed: {player_name} - {str(e)[:100]}")
            
            time.sleep(RATE_LIMIT_DELAY)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Updated {updated_count}/{total_players} players ({len(retry_queue)} retries)")
    if failed_count > 0:
        print(f"WARNING - Failed to update {failed_count} players after all retries")
    
    return updated_count, failed_count


def update_wingspan_from_combine(ctx: ETLContext, only_unbackfilled: bool = False) -> Tuple[int, int]:
    """
    Fetch wingspan data from NBA Draft Combine (DraftCombinePlayerAnthro endpoint).
    Searches all available seasons back to 2003 (combine_start_year).
    If a player has wingspan data from multiple years, keeps the most recent.
    
    Args:
        ctx: ETLContext instance
        only_unbackfilled: If True, only fetch for players with backfilled=FALSE
    
    Returns: (updated_count, total_checked)
    """
    from nba_api.stats.endpoints import DraftCombinePlayerAnthro
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all players who need wingspan data
    players_table = get_table_name('player', 'entity')
    if only_unbackfilled:
        cursor.execute(f"SELECT player_id FROM {players_table} WHERE wingspan_inches IS NULL AND backfilled = FALSE")
    else:
        cursor.execute(f"SELECT player_id FROM {players_table} WHERE wingspan_inches IS NULL")
    players_needing_wingspan = {row[0] for row in cursor.fetchall()}
    
    if not players_needing_wingspan:
        print("All players already have wingspan data")
        cursor.close()
        conn.close()
        return 0, 0
    
    # Fetch combine data from all seasons (most recent first to get latest data)
    current_year = NBA_CONFIG['current_season_year']
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
            print(f"  Failed to fetch {season}: {str(e)[:50]}")
            continue
    
    # Update database with found wingspan data
    updated_count = 0
    print(f"  Updating wingspan data in {players_table} table...")
    for player_id, (wingspan, year) in wingspan_data.items():
        try:
            # Round to nearest inch
            wingspan_inches = round(wingspan)
            
            players_table = get_table_name('player', 'entity')
            cursor.execute(f"""
                UPDATE {players_table} 
                SET wingspan_inches = %s, updated_at = NOW()
                WHERE player_id = %s
            """, (wingspan_inches, player_id))
            
            if cursor.rowcount > 0:
                updated_count += 1
                if updated_count <= 3:  # Show first 3 updates
                    print(f"    Updated player {player_id}: wingspan_inches = {wingspan_inches} (from {year})")
        except Exception as e:
            print(f"  Failed to update player {player_id}: {e}")
            continue
    
    conn.commit()
    
    # VERIFY: Check that updates were written to players table
    if updated_count > 0:
        print(f"\n  Verifying updates in {players_table}...")
        sample_ids = list(wingspan_data.keys())[:3]
        if sample_ids:
            cursor.execute(f"""
                SELECT player_id, wingspan_inches
                FROM {players_table}
                WHERE player_id = ANY(%s)
            """, (sample_ids,))
            verified = cursor.fetchall()
            for pid, ws in verified:
                print(f"    Player {pid}: wingspan_inches = {ws}")
    cursor.close()
    conn.close()
    
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
    finally:
        ctx.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='The Glass ETL - Daily updates, historical backfill, and annual maintenance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python src/etl.py                          # Daily ETL (default)
  python src/etl.py --annual                 # Annual maintenance (Aug 1st)
  python src/etl.py --annual --name-range A-J  # Annual in batches
  python src/etl.py --backfill 2020          # Backfill from 2020
  python src/etl.py --backfill 2015 --end 2020  # Backfill 2015-2020
        '''
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--annual', '-a', action='store_true', help='Run annual ETL (cleanup + player details)')
    mode_group.add_argument('--backfill', '-b', type=int, help='Backfill from this year (e.g., 2020 for 2019-20 season)')
    
    # Annual-specific options
    parser.add_argument('--name-range', choices=['A-J', 'K-Z'], help='Annual only: Process players in this name range')
    parser.add_argument('--year', type=int, help='Annual only: Specific year to process')
    
    # Backfill options
    parser.add_argument('--end', type=int, help='Backfill only: End year (defaults to current season)')
    
    # General options
    parser.add_argument('--no-check', action='store_true', help='Skip missing data check')
    
    args = parser.parse_args()
    
    # Route to appropriate ETL mode
    # Initialize ETLContext
    ctx = ETLContext()
    
    if args.annual:
        # ANNUAL ETL MODE
        
        # If year specified, update NBA_CONFIG for that season
        if args.year:
            NBA_CONFIG['current_season_year'] = args.year
            NBA_CONFIG['current_season'] = f"{args.year-1}-{str(args.year)[-2:]}"
        
        run_annual_etl(ctx=ctx)
        
    elif args.backfill:
        # BACKFILL MODE
        backfill_start = args.backfill
        backfill_end = args.end
        
        # Check for environment variables if args not provided (for GitHub Actions)
        if not backfill_start and os.getenv('BACKFILL_START_YEAR'):
            try:
                backfill_start = int(os.getenv('BACKFILL_START_YEAR'))
            except (ValueError, TypeError):
                pass
        
        if not backfill_end and os.getenv('BACKFILL_END_YEAR'):
            try:
                backfill_end = int(os.getenv('BACKFILL_END_YEAR'))
            except (ValueError, TypeError):
                pass
        
        run_daily_etl(
            ctx=ctx,
            backfill_start=backfill_start,
            backfill_end=backfill_end
        )
        
    else:
        # DAILY ETL MODE (default)
        run_daily_etl(ctx=ctx)