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
    safe_int, safe_float, safe_str, parse_height, parse_birthdate,
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
                        print(f"  ❌ Task {task_id} failed: {str(e)[:80]}")
                    
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
                print(f"  ⚠️ Batch failed at row {i}: {str(e)[:100]}")
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
            print(f"  ❌ COPY failed: {str(e)}")
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
    description: Optional[str] = None
) -> None:
    """
    Universal config-driven endpoint executor - auto-dispatches based on config.
    
    Automatically routes to appropriate execution strategy:
    - League-wide: 1 API call returns all entities → _execute_league_wide_endpoint()
    - Per-team: 30 API calls (one per team), aggregated → _execute_per_team_endpoint()
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
        print(f"  No columns configured for {endpoint_name} - skipping")
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
    
    # Both tables store year as VARCHAR:
    # - player_season_stats: stores season string like '2024-25'
    # - team_season_stats: stores year integer as string like '2025'
    player_stats_table = get_table_name('player', 'stats')
    if table == player_stats_table:
        year_value = season  # Use full season string: '2024-25'
    else:
        # team_season_stats uses ending year as string: '2025'
        year_value = str(int('20' + season.split('-')[1]))
    
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
            return updated
        else:
            conn.commit()
            return 0
        
    except Exception as e:
        print(f"❌ ERROR - Failed {description}: {str(e)}")
        conn.rollback()
        
        # Add to retry queue for end-of-ETL retry
        ctx.add_failed_endpoint({
            'function': '_execute_league_wide_endpoint',
            'args': (ctx, endpoint_name, endpoint_params, season, entity, table, season_type, season_type_name, description, cols)
        })
        print(f"  {description} queued for retry at end of ETL")
        
        return 0
    finally:
        cursor.close()
        conn.close()


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
    # Both tables store year as VARCHAR:
    # - player_season_stats: stores season string like '2024-25'
    # - team_season_stats: stores year integer as string like '2025'
    player_stats_table = get_table_name('player', 'stats')
    if table == player_stats_table:
        year_value = season  # Use full season string: '2024-25'
    else:
        # team_season_stats uses ending year as string: '2025'
        year_value = str(int('20' + season.split('-')[1]))
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
    entity_games_played = {}  # Track games played for per-game conversion
    
    # Check if we need to track games_played for conversion
    convert_per_game = endpoint_params.get('_convert_per_game', False)
    games_field = endpoint_params.get('_games_field', 'GP') if convert_per_game else None
    
    # Get team IDs
    team_ids = list(TEAM_IDS.values())
    
    # Get per-call delay from endpoint config (for rate limiting)
    endpoint_config = get_endpoint_config(endpoint_name)
    per_call_delay = 0.0
    if endpoint_config and 'retry_config' in endpoint_config:
        per_call_delay = endpoint_config['retry_config'].get('per_call_delay', 0.0)
    
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
                    
                    # Get games_played index if needed for conversion
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
                        
                        # Track games_played for this entity (for conversion)
                        if games_idx is not None and stats_dict == entity_stats:
                            # Only track from main result set, not subtract set
                            games_played = safe_int(row[games_idx], 1)
                            if entity_id not in entity_games_played:
                                entity_games_played[entity_id] = games_played
                        
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
                                    print(f"  ⚠️ WARNING: Column '{col_name}' got non-numeric value: {type(value)}, skipping")
                    
                    break  # Found the result set, no need to check others
            
            # Enforce delay between calls to avoid rate limiting (skip delay after last team)
            if per_call_delay > 0 and idx < len(team_ids) - 1:
                time.sleep(per_call_delay)
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            
            # Get team name for better context
            team_name = "Unknown"
            if team_id in TEAM_IDS.values():
                for abbr, tid in TEAM_IDS.items():
                    if tid == team_id:
                        team_name = abbr
                        break
            
            print(f"  Failed team {team_id} ({team_name}): {error_type}: {error_msg[:100]} | Endpoint: {endpoint_name} | Season: {season}")
            continue
    
    # CONVERSION: If using PerGame mode, multiply stat values by games played
    if convert_per_game and entity_stats and entity_games_played:
        print(f"  Converting PerGame to totals using {games_field}...")
        for entity_id in entity_stats:
            if entity_id in entity_games_played:
                games_played = entity_games_played[entity_id]
                if games_played > 0:
                    # Multiply all stat values by games played to convert per-game → totals
                    for col_name in cols.keys():
                        entity_stats[entity_id][col_name] = int(entity_stats[entity_id][col_name] * games_played)
    
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
                    print(f"  ⚠️ WARNING: Column '{col}' has dict/list value for entity {entity_id}, converting to None")
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
        print(f"❌ ERROR - Failed {description} (aggregation): {str(e)}")
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
        
        players_table = get_table_name('player', 'entity')
        detail_col_names = sorted(detail_fields.keys())
        insert_col_names = ['player_id', 'name', 'team_id'] + detail_col_names
        
        for idx, player_id in enumerate(new_player_ids, 1):
            player_data = all_players[player_id]
            player_name = player_data.get('name', 'Unknown')
            
            print(f"[{idx}/{len(new_player_ids)}] {player_name} (ID: {player_id})")
            
            # Step 1: Fetch player details from commonplayerinfo
            # OPTIMIZATION: Extract FROM_YEAR to start backfill from player's rookie season
            rookie_year = None
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
                            print(f"  ⚠️  Could not parse FROM_YEAR: {from_year_raw}")
                    
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
                        else:
                            player_data[db_col_name] = raw_value
                
            except Exception as e:
                print(f"  ⚠️  WARNING - Failed to fetch details: {e}")
                print(f"  → Will insert with basic info only (name, team, jersey)")
            
            # Step 2: Insert player into database
            try:
                insert_row = [player_id, player_data['name'], player_data['team_id']]
                for col in detail_col_names:
                    insert_row.append(player_data.get(col))
                
                cursor.execute(f"""
                    INSERT INTO {players_table} ({', '.join(insert_col_names)})
                    VALUES ({', '.join(['%s'] * len(insert_col_names))})
                    ON CONFLICT (player_id) DO NOTHING
                """, insert_row)
                conn.commit()
                players_added += 1
                
                # Step 2.5: Fetch wingspan from combine data if available
                try:
                    from nba_api.stats.endpoints import DraftCombinePlayerAnthro
                    
                    # Check exactly 5 draft combines on and before player's first NBA season
                    # Example: If rookie season is 2024-25 (FROM_YEAR=2024), check combines 2024, 2023, 2022, 2021, 2020
                    wingspan_found = False
                    
                    # Calculate starting year for combine search (rookie_year - 1 = FROM_YEAR)
                    if rookie_year:
                        first_combine_year = rookie_year - 1  # FROM_YEAR (start of rookie season)
                    else:
                        first_combine_year = get_season_year() - 1  # Default to current year if no rookie year
                    
                    # Check 5 combines: first_combine_year, -1, -2, -3, -4
                    for year_offset in range(5):
                        combine_year = first_combine_year - year_offset
                        combine_season = f"{combine_year}-{str(combine_year + 1)[-2:]}"
                        
                        try:
                            endpoint = DraftCombinePlayerAnthro(season_year=combine_season, timeout=10)
                            time.sleep(0.6)  # Rate limit
                            result = endpoint.get_dict()
                            
                            for rs in result['resultSets']:
                                player_id_idx = rs['headers'].index('PLAYER_ID')
                                wingspan_idx = rs['headers'].index('WINGSPAN')
                                
                                for row in rs['rowSet']:
                                    if row[player_id_idx] == player_id and row[wingspan_idx] is not None:
                                        wingspan_inches = round(row[wingspan_idx])
                                        cursor.execute(f"""
                                            UPDATE {players_table}
                                            SET wingspan_inches = %s, updated_at = NOW()
                                            WHERE player_id = %s
                                        """, (wingspan_inches, player_id))
                                        conn.commit()
                                        wingspan_found = True
                                        break
                                
                                if wingspan_found:
                                    break
                            
                            if wingspan_found:
                                break
                        
                        except Exception:
                            continue  # Try next year
                
                except Exception as e:
                    print(f"  ⚠️  Could not fetch wingspan: {str(e)[:50]}")
                
            except Exception as e:
                print(f"  ❌ ERROR - Failed to insert player: {e}")
                conn.rollback()
                continue  # Skip to next player
            
            # Step 3: Backfill all historical stats for this player
            # OPTIMIZATION: Start from player's rookie season (FROM_YEAR) instead of 2003-04
            try:
                
                backfill_new_players(ctx, [player_id], start_year=rookie_year)
            except Exception as e:
                print(f"  ⚠️  WARNING - Backfill failed: {e}")
                print(f"  → Player is in database but historical stats incomplete")
            
            # Add delay between players to avoid overwhelming API
            if idx < len(new_player_ids):
                time.sleep(API_CONFIG.get('rate_limit_delay', 0.6))
                
    update_players_data = []
    
    players_table = get_table_name('player', 'entity')
    cursor.execute(f"SELECT player_id, team_id FROM {players_table}")
    existing_players = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Get ALL detail column names from config (these are actual DB column names)
    detail_col_names = sorted(detail_fields.keys())
    
    for player_id, player_data in all_players.items():
        if player_id in existing_players and player_id not in new_player_ids:
            # Existing player (not newly added) - check if team changed
            if existing_players[player_id] != player_data['team_id']:
                players_updated += 1
            
            # Build update tuple dynamically: [team_id, detail_fields..., player_id]
            update_row = [player_data['team_id']]
            for col in detail_col_names:
                update_row.append(player_data.get(col))
            update_row.append(player_id)  # WHERE clause
            update_players_data.append(tuple(update_row))
    
    # Bulk update existing players (team_id and detail field changes)
    if update_players_data:
        # Build UPDATE statement dynamically - all columns come from config
        update_col_names = ['team_id'] + detail_col_names
        set_clause = ', '.join([f"{quote_column(col)} = %s" for col in update_col_names])
        
        update_sql = f"""
            UPDATE players 
            SET {set_clause}, updated_at = NOW()
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
    
    return players_added, players_updated, new_player_ids


def update_basic_stats(ctx: ETLContext, entity: Literal['player', 'team'], skip_zero_stats: bool = False, player_ids: Optional[List[int]] = None) -> bool:
    """
    Update season statistics for all entities (players or teams).
    
    WHY: Consolidates update_player_stats + update_team_stats into one config-driven function.
    Eliminates 460 lines of duplication by using entity parameter.
    
    Args:
        ctx: ETLContext for state management
        entity: 'player' or 'team'
        skip_zero_stats: If True, don't add zero-stat records for roster players (player-only, backfill mode)
        player_ids: Optional list of player IDs to filter to (for backfill mode - only process these specific players)
        
    Returns:
        True if successful
        
    Usage:
        update_basic_stats(ctx, 'player')  # All players
        update_basic_stats(ctx, 'player', player_ids=[1629632])  # Specific player(s)
        update_basic_stats(ctx, 'team')
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = get_season()
    current_year = get_season_year()
    
    # Entity-specific configuration
    if entity == 'player':
        endpoint_name = 'leaguedashplayerstats'
        EndpointClass = leaguedashplayerstats.LeagueDashPlayerStats
        table = get_table_name('player', 'stats')
        id_field = 'PLAYER_ID'
        year_value = current_season  # Players use season string ('2024-25')
        
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
        year_value = current_year  # Teams use year integer (2025)
        
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
    
    # Process all season types from config - extract season_code from dict
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
                        
                        # Debug: Check if field is missing for advanced stats
                        if raw_value is None and field_name in adv_field_names:
                            # Only log once per season to avoid spam
                            if entity_id == list(valid_entity_ids)[0]:
                                print(f"⚠️ Advanced field {field_name} missing from DataFrame")
                            raw_value = 0
                        elif raw_value is None:
                            raw_value = 0
                        
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
                            # For zero-stat records, all stats should be 0 or NULL
                            # Don't use default from config as it might be complex types
                            zero_values.append(0)
                        
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


def backfill_new_players(ctx: ETLContext, player_ids: List[int], start_year: Optional[int] = None) -> None:
    """
    Backfill all historical seasons for newly added players.
    Reuses update_basic_stats() for DRY principle - no duplicate logic!
    
    Args:
        ctx: Parent ETL context
        player_ids: List of player IDs to backfill
        start_year: First season year to backfill (defaults to year from NBA_CONFIG['backfill_start_season'])
    """
    if not player_ids:
        return
    
    if start_year is None:
        # Extract ending year from season string (e.g., '2003-04' -> 2004)
        season_str = NBA_CONFIG['backfill_start_season']
        start_year = int('20' + season_str.split('-')[1])
    
    current_year = get_season_year()
    
    # Temporarily save original season config
    original_season = NBA_CONFIG['current_season']
    original_year = NBA_CONFIG['current_season_year']
    
    # Track which seasons have data (skip advanced stats for seasons with no basic stats)
    seasons_with_data = set()
    
    try:
        # Iterate through each historical season (year-by-year)
        for year in range(start_year, current_year + 1):
            season = f"{year-1}-{str(year)[-2:]}"
            
            # Temporarily override config so update_basic_stats() uses this season
            NBA_CONFIG['current_season'] = season
            NBA_CONFIG['current_season_year'] = year
            
            print(f"{season}")
            
            # Reuse existing function - DRY!
            # For historical seasons: skip_zero_stats=True (don't create empty records)
            # For current season: skip_zero_stats=False (create record even if GP=0)
            is_current_season = (year == current_year)
            update_basic_stats(ctx, 'player', skip_zero_stats=not is_current_season, player_ids=player_ids)
            
            # Check which season types have games played (GP > 0) to avoid wasting API calls
            # Only fetch advanced stats for season types where player actually played
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT season_type, SUM(games_played) as total_gp
                FROM player_season_stats 
                WHERE player_id = ANY(%s) AND year = %s
                GROUP BY season_type
                HAVING SUM(games_played) > 0
            """, (player_ids, season))
            active_season_types = {row[0] for row in cursor.fetchall()}
            cursor.close()
            conn.close()
            
            if active_season_types:
                seasons_with_data.add((season, tuple(sorted(active_season_types))))
            else:
                print(f"No games played - skipping season")
            
            # No delay needed between seasons when processing single player sequentially
            # (season_delay is 0 for one-player-at-a-time processing)
            if year < current_year and API_CONFIG['season_delay'] > 0:
                time.sleep(API_CONFIG['season_delay'])
            
            # Backfill advanced transformations for seasons with data (including current season)
            if active_season_types:
                try:
                    # Get the season types that have data for this season
                    season_types = active_season_types
                    
                    # PHASE 1: League-wide advanced stats (leaguedashptdefend, leaguedashptstats, etc.)
                    update_advanced_stats(ctx, 'player', season=season, season_types_to_process=season_types)
                    
                    # PHASE 2: Per-player transformations (playerdashptshots, playerdashptreb, etc.)
                    # These are handled through transformation columns and require per-player API calls
                    for season_type_code in season_types:
                        season_type_name = next(
                            (name for name, cfg in SEASON_TYPE_CONFIG.items() if cfg['season_code'] == season_type_code),
                            'Regular Season'
                        )
                        try:
                            update_transformation_columns(
                                ctx, 
                                season=season, 
                                entity='player', 
                                season_type=season_type_code,
                                season_type_name=season_type_name
                            )
                        except Exception as e:
                            print(f"  ⚠️ Failed transformations for {season} {season_type_name}: {e}")
                    
                    # Explicit commit after season completes (redundant but ensures all data is saved)
                    conn = get_db_connection()
                    conn.commit()
                    
                    # Convert NULLs to 0s for ALL stat columns where players played games
                    # If games_played > 0, all stat NULLs should be 0 (player played but didn't record that stat)
                    try:
                        # Build list of all stat columns from DB_COLUMNS (exclude non-stats)
                        stat_columns = [
                            col_name for col_name, col_def in DB_COLUMNS.items()
                            if col_def.get('table') == 'stats' and col_def.get('type', '').startswith(('INTEGER', 'SMALLINT', 'BIGINT', 'FLOAT', 'REAL', 'NUMERIC'))
                        ]
                        # Build SET clause dynamically with quoted column names (for names starting with numbers)
                        set_clause = ', '.join([f'"{col}" = COALESCE("{col}", 0)' for col in stat_columns])
                        
                        cursor = conn.cursor()
                        cursor.execute(f"""
                            UPDATE player_season_stats
                            SET {set_clause}
                            WHERE player_id = ANY(%s) 
                            AND year = %s 
                            AND games_played > 0
                        """, (player_ids, season))
                        conn.commit()
                        cursor.close()
                    except Exception as e:
                        print(f"  ⚠️ Failed to convert NULLs to 0s for {season}: {e}")
                    finally:
                        conn.close()
                    
                except Exception as e:
                    print(f"  ⚠️ Failed advanced stats for {season}: {e}")
    
    finally:
        # Always restore original config
        NBA_CONFIG['current_season'] = original_season
        NBA_CONFIG['current_season_year'] = original_year


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
    
    # Determine correct year value for WHERE clause
    # player_season_stats uses full season string ('2007-08')
    # team_season_stats uses ending year as string ('2008')
    player_stats_table = get_table_name('player', 'stats')
    if table == player_stats_table:
        year_value = season  # '2007-08'
    else:
        year_value = str(season_year)  # '2008'
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Discover transformation groups dynamically from DB_COLUMNS
    # Group transformations by their 'group' parameter
    groups_map = {}  # group_name -> list of (col_name, transform)
    ungrouped_transforms = []
    
    for col_name, col_meta in DB_COLUMNS.items():
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

            data = apply_transformation(ctx, col_name, transform, season, entity, table, season_type, season_type_name, source_config)

            if not isinstance(data, dict):
                continue

            updated = 0
            for entity_id, value in data.items():
                if isinstance(value, dict):
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
            conn.rollback()
    
    # CLEANUP: Convert NULLs to 0 for any row with minutes > 0
    # This ensures data quality - if a player/team played, missing stats should be 0, not NULL
    try:
        # Get all transformation columns (columns with transformations in config)
        transform_cols = [
            col_name for col_name, col_def in DB_COLUMNS.items()
            if col_def.get('table') == table 
            and f'{entity}_source' in col_def 
            and 'transformation' in col_def[f'{entity}_source']
        ]
        
        if transform_cols:
            # Build SET clause to update all NULL transformation columns to 0
            set_clause = ", ".join([f"{quote_column(col)} = COALESCE({quote_column(col)}, 0)" for col in transform_cols])
            
            if entity == 'player':
                cursor.execute(f"""
                    UPDATE {table}
                    SET {set_clause}, updated_at = NOW()
                    WHERE year = %s::text AND season_type = %s AND minutes_x10 > 0
                """, (season_year, season_type))
            else:  # team
                cursor.execute(f"""
                    UPDATE {table}
                    SET {set_clause}, updated_at = NOW()
                    WHERE year = %s::text AND season_type = %s AND minutes_x10 > 0
                """, (season_year, season_type))
            
            nulls_fixed = cursor.rowcount
            if nulls_fixed > 0:
                print(f"  Fixed {nulls_fixed} NULL values → 0")
            conn.commit()
            
    except Exception as e:
        print(f"  ❌ Failed NULL cleanup: {e}")
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
                    print(f"  Skipping {season_type_name} for {season} (minimum season: {min_season})")
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
        print(f"❌ ERROR - Advanced {entity} stats failed after {elapsed:.1f}s: {e}")
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
        
        
        # STEP 5: Team advanced stats
        # Multi-call aggregates: 8 shooting zone endpoints
        # Other team endpoints: 4 calls
        # Team putbacks: 30 calls (1 per team)
        
        # Initialize parallel executor for roster fetching (per-team tier: 30 API calls)
        ctx.init_parallel_executor(max_workers=10, endpoint_tier='team')
        
        # STEP 1: Player Rosters (now includes atomic backfill for each new player)
        # New players are added one at a time with immediate backfill
        players_added, players_updated, new_player_ids = update_player_rosters(ctx)
        
        # # STEP 1a: Update wingspan for new players (if any were added)
        if players_added > 0:
            update_wingspan_from_combine(ctx)
        
        # NOTE: Backfill is now done INSIDE update_player_rosters() for each player atomically
        # No separate backfill_new_players() call needed here anymore
        
        # STEP 2: Player Stats
        update_basic_stats(ctx, 'player')
        
        # STEP 3: Team Stats
        update_basic_stats(ctx, 'team')
        
        # STEP 4: Player Advanced Stats
        update_advanced_stats(ctx, 'player')
        
        # STEP 5: Team Advanced Stats
        update_advanced_stats(ctx, 'team')
        
        # STEP 6: Retry any failed endpoints
        # Give API time to stabilize, then retry any endpoints that failed
        if ctx.failed_endpoints:
            print(f"\nWaiting {API_CONFIG['cooldown_after_batch_seconds']} seconds before retrying failed endpoints...")
            time.sleep(API_CONFIG['cooldown_after_batch_seconds'])
            retry_failed_endpoints(ctx)
        
        # Close progress bars
        ctx.close()
        
        elapsed = time.time() - start_time
        print("=" * 70)
        print(f"DAILY ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        print("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print("=" * 70)
        print(f"❌ DAILY ETL FAILED - {elapsed:.1f}s")
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
                    print(f"  ✓ Retry succeeded - updated {result} records")
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
            print(f"⚠️ WARNING - Taking {API_CONFIG['cooldown_after_batch_seconds']}s break after {API_CONFIG['max_consecutive_failures']} consecutive failures")
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
        print(f"⚠️ WARNING - Failed to update {failed_count} players after all retries")
    
    return updated_count, failed_count


def update_wingspan_from_combine(ctx: ETLContext) -> None:
    """
    Fetch wingspan data from NBA Draft Combine (DraftCombinePlayerAnthro endpoint).
    Searches all available seasons back to 2002-03.
    If a player has wingspan data from multiple years, keeps the most recent.
    
    Returns: (updated_count, total_checked)
    """
    from nba_api.stats.endpoints import DraftCombinePlayerAnthro
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all players who need wingspan data
    players_table = get_table_name('player', 'entity')
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
            print(f"  ⚠️ Failed to fetch {season}: {str(e)[:50]}")
            continue
    
    # Update database with found wingspan data
    updated_count = 0
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
        except Exception as e:
            print(f"  ⚠️ Failed to update player {player_id}: {e}")
            continue
    
    conn.commit()
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
        wingspan_updated, wingspan_total = update_wingspan_from_combine(ctx)
        print(f"  Updated {wingspan_updated}/{wingspan_total} players\n")
        
        print("Step 3: Updating all player details...")
        details_updated, details_failed = update_all_player_details(ctx)
        print(f"  Updated {details_updated} players, {details_failed} failed\n")
        
        print("="*70)
        print("ANNUAL ETL COMPLETED SUCCESSFULLY")
        print(f"Total: {deleted_count} deleted, {wingspan_updated} wingspans, {details_updated} details")
        print("="*70)
        
    except Exception as e:
        print(f"❌ ANNUAL ETL FAILED: {e}")
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