"""
THE GLASS - Daily ETL
Handles player/team stats, rosters, and advanced metrics.

Usage:
    python src/etl.py                          # Daily update
    python src/etl.py --backfill 2020          # Backfill from 2020
    python src/etl.py --backfill 2015 --end 2020  # Backfill range
    python src/etl.py --annual                 # Annual maintenance (Aug 1)

Config-driven 3-tier execution: league/team/player tiers auto-detected from endpoint names.
"""

import os
import sys
import time
import argparse
import psycopg2
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
from typing import List, Dict, Any
from io import StringIO
from nba_api.stats.endpoints import (
    commonplayerinfo,
    leaguedashplayerstats, leaguedashteamstats,
)

# Load environment variables FIRST (before importing config)
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import config module (single unified config)
try:
    from config.etl import (
        NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA, TABLES,
        DB_COLUMNS, SEASON_TYPE_MAP, TEST_MODE_CONFIG,
        infer_execution_tier_from_endpoint,
        get_columns_by_endpoint,
        get_columns_by_entity,
        get_primary_key, get_composite_keys, get_all_key_fields,
        safe_int, safe_float, safe_str, parse_height, parse_birthdate,
        PARALLEL_EXECUTION, SUBPROCESS_CONFIG,
        API_CONFIG, RETRY_CONFIG, DB_OPERATIONS,
        API_FIELD_NAMES,
        get_entity_id_field, get_entity_name_field,
        DEFENDER_DISTANCE_API_MAP
    )
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from config.etl import (
        NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA, TABLES,
        DB_COLUMNS, SEASON_TYPE_MAP, TEST_MODE_CONFIG,
        infer_execution_tier_from_endpoint,
        get_columns_by_endpoint,
        get_columns_by_entity,
        get_primary_key, get_composite_keys, get_all_key_fields,
        safe_int, safe_float, safe_str, parse_height, parse_birthdate,
        PARALLEL_EXECUTION, SUBPROCESS_CONFIG,
        API_CONFIG, RETRY_CONFIG, DB_OPERATIONS,
        API_FIELD_NAMES,
        get_entity_id_field, get_entity_name_field,
        DEFENDER_DISTANCE_API_MAP
    )


RATE_LIMIT_DELAY = API_CONFIG['rate_limit_delay']
MAX_WORKERS_LEAGUE = PARALLEL_EXECUTION['league']['max_workers']
MAX_WORKERS_TEAM = PARALLEL_EXECUTION['team']['max_workers']
MAX_WORKERS_PLAYER = PARALLEL_EXECUTION['player']['max_workers']

_transaction_tracker = None
_rate_limiter = None
_parallel_executor = None


# ============================================================================
# SUBPROCESS EXECUTION
# ============================================================================

def _handle_subprocess_failure(player_ids, error_message, failures_list):
    """Helper to append failure entries for all players in a batch."""
    for player_id in player_ids:
        failures_list.append({'player_id': player_id, 'error': error_message})

def _run_player_endpoint_batch_worker(endpoint_module, endpoint_class, player_ids, endpoint_params, delay, queue=None, progress_queue=None):
    """Subprocess worker for per-player endpoint execution."""
    from importlib import import_module
    import time
    import sys
    
    # Import endpoint dynamically
    try:
        module = import_module(endpoint_module)
        EndpointClass = getattr(module, endpoint_class)
    except Exception as e:
        # Critical error - can't even import the endpoint
        print(f"[SUBPROCESS ERROR] Import FAILED: {e}", file=sys.stderr)
        if queue is not None:
            queue.put({
                'successes': [],
                'failures': [{'player_id': pid, 'error': f'Import failed: {e}'} for pid in player_ids],
                'total': len(player_ids)
            })
        return
    
    results = {'successes': [], 'failures': [], 'total': len(player_ids)}
    
    for idx, player_id in enumerate(player_ids):
        try:
            # API call with player_id
            params = {'player_id': player_id, **endpoint_params}
            response = EndpointClass(**params).get_dict()
            
            results['successes'].append({
                'player_id': player_id,
                'data': response
            })
            
        except Exception as e:
            results['failures'].append({
                'player_id': player_id,
                'error': str(e)
            })
            # Log first few failures for debugging
            if len(results['failures']) <= SUBPROCESS_CONFIG['failure_log_limit']:
                print(f"Subprocess error for player {player_id}: {e}", file=sys.stderr)
        
        # Send progress update after each call (success or failure)
        if progress_queue is not None:
            try:
                progress_queue.put(1)  # Signal 1 completed call
            except:
                pass  # Ignore queue errors
        
        # Simple delay between calls
        time.sleep(delay)
    
    # Put results in queue if provided (subprocess mode)
    if queue is not None:
        try:
            import sys
            sys.stderr.flush()  # Force immediate output
            
            queue.put(results, block=True, timeout=None)  # Explicit parameters for clarity
            sys.stderr.flush()
        except Exception as e:
            print(f"[SUBPROCESS ERROR] Failed to put results in queue: {e}", file=sys.stderr)
            sys.stderr.flush()
            raise
    sys.stderr.flush()
    return results


def execute_player_endpoint_in_subprocesses(endpoint_module, endpoint_class, player_ids, endpoint_params, description="Per-Player Endpoint"):
    """Execute per-player endpoint via subprocesses to bypass API rate limits."""
    from multiprocessing import Process, Manager
    import threading
    
    players_per_subprocess = SUBPROCESS_CONFIG['players_per_subprocess']
    delay = SUBPROCESS_CONFIG['delay_between_calls']
    timeout = SUBPROCESS_CONFIG['subprocess_timeout']
    
    all_successes = []
    all_failures = []
    
    # Split players into batches for subprocesses
    for batch_idx in range(0, len(player_ids), players_per_subprocess):
        batch_players = player_ids[batch_idx:batch_idx + players_per_subprocess]
        batch_num = batch_idx // players_per_subprocess + 1
        
        try:
            # Use Manager for queues to avoid buffer size limitations
            # Regular Queue() can block on put() if data is too large!
            manager = Manager()
            queue = manager.Queue()
            progress_queue = manager.Queue()
            
            # Start a thread to monitor progress updates from subprocess
            progress_stop = threading.Event()
            def monitor_progress():
                while not progress_stop.is_set():
                    try:
                        # Wait for progress update with timeout
                        progress_queue.get(timeout=0.5)
                        track_transaction(1)  # Track API call
                    except:
                        pass  # Timeout or queue empty - continue monitoring
            
            progress_thread = threading.Thread(target=monitor_progress, daemon=True)
            progress_thread.start()
            
            # Call worker directly - no wrapper needed (worker is module-level, can be pickled)
            process = Process(
                target=_run_player_endpoint_batch_worker,
                args=(endpoint_module, endpoint_class, batch_players, endpoint_params, delay, queue, progress_queue)
            )
            process.start()
            
            # Wait for subprocess to complete with timeout
            process.join(timeout=timeout)
            
            # Stop progress monitoring
            progress_stop.set()
            progress_thread.join(timeout=2)
            
            # Check if subprocess is still running (timed out)
            if process.is_alive():
                log(f"  ❌ Subprocess {batch_num} timed out after {timeout}s", "ERROR")
                process.terminate()
                process.join(timeout=5)
                _handle_subprocess_failure(batch_players, 'Timeout', all_failures)
                continue
            
            if process.exitcode != 0:
                log(f"  ❌ Subprocess {batch_num} crashed (exit code {process.exitcode})", "ERROR")
                _handle_subprocess_failure(batch_players, 'Subprocess crashed', all_failures)
                continue
            
            # Get results from queue
            try:
                batch_results = queue.get(block=True, timeout=SUBPROCESS_CONFIG['queue_timeout'])
                all_successes.extend(batch_results['successes'])
                all_failures.extend(batch_results['failures'])
            except Exception as e:
                _handle_subprocess_failure(batch_players, f'Queue error: {e}', all_failures)
                continue
            
        except Exception as e:
            log(f"  ❌ Subprocess {batch_num} error: {e}", "ERROR")
            _handle_subprocess_failure(batch_players, f'Subprocess error: {e}', all_failures)
    
    total_failure = len(all_failures)
    if total_failure > 0:
        log(f"  {total_failure} failures", "WARNING")
    
    return {'successes': all_successes, 'failures': all_failures}


# ============================================================================
# ETL OPTIMIZATION CLASSES
# ============================================================================

class RateLimiter:
    """Thread-safe rate limiter for API calls."""
    
    def __init__(self, requests_per_second=2.5):
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
        self.window_size = API_CONFIG['rate_limiter_window_size']
        
    def acquire(self):
        """Block until it's safe to make another API request."""
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
            
    def get_current_rate(self):
        """Get current requests per minute for monitoring."""
        with self.lock:
            now = time.time()
            recent = [t for t in self.request_times if now - t < 60]
            return len(recent)


class ParallelAPIExecutor:
    """Parallel API executor for league/team tier endpoints."""
    
    def __init__(self, max_workers=None, rate_limiter=None, log_func=None, endpoint_tier=None):
        """
        Args:
            max_workers: Number of parallel threads (None = auto-detect from tier)
            rate_limiter: Shared RateLimiter instance
            log_func: Logging function (uses print if None)
            endpoint_tier: 'league', 'team', or 'player' (None = use max_workers directly)
        """
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
        self.rate_limiter = rate_limiter or RateLimiter()
        self.log = log_func or print
        self.results = {}
        self.errors = []
        
    def execute_batch(self, tasks: List[Dict[str, Any]], description="Batch", progress_callback=None):
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
            self.log(f"  {len(errors)} tasks failed out of {len(tasks)}")
            
        return results, errors, failed_ids
    
    def _execute_task_batch(self, tasks: List[Dict[str, Any]], progress_callback=None):
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
                        self.log(f"  ❌ Task {task_id} failed: {str(e)[:80]}")
                    
                    if progress_callback:
                        progress_callback(1)
        
        return results, errors, failed_ids
    
    def _execute_with_retry(self, task):
        """Execute a single task with retry logic."""
        func = task['func']
        max_retries = task.get('max_retries', RETRY_CONFIG['max_retries'])
        timeout = task.get('timeout', API_CONFIG['timeout_default'])
        
        for attempt in range(1, max_retries + 1):
            try:
                # Acquire rate limit token
                self.rate_limiter.acquire()
                
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
    
    def __init__(self, conn, batch_size=None, log_func=None):
        """
        Args:
            conn: psycopg2 connection
            batch_size: Number of rows per batch (from config if not specified)
            log_func: Logging function
        """
        self.conn = conn
        self.batch_size = batch_size or DB_OPERATIONS['bulk_insert_batch_size']
        self.log = log_func or log
        
    def bulk_upsert(self, table: str, columns: List[str], data: List[tuple], 
                    conflict_columns: List[str], update_columns: List[str] = None):
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
        
        # Build UPSERT statement
        cols_str = ', '.join(columns)
        conflict_str = ', '.join(conflict_columns)
        update_str = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_columns])
        
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
                    self.log(f"  Batch progress: {inserted}/{total_rows} rows")
                    
            except Exception as e:
                self.log(f"  ⚠️ Batch failed at row {i}: {str(e)[:100]}")
                # Try to continue with next batch
                self.conn.rollback()
                continue
        
        self.conn.commit()
        return inserted
    
    def bulk_copy(self, table: str, columns: List[str], data: List[tuple]):
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
            self.log(f"  ❌ COPY failed: {str(e)}")
            self.conn.rollback()
            raise


class TransactionTracker:
    """Simple transaction counter with persistent bottom-line display."""
    
    def __init__(self, description="ETL"):
        """
        Args:
            description: Label for the transaction tracker
        """
        self.description = description
        self.total = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._last_line = ""
        self._stop_refresh = threading.Event()
        self._refresh_thread = None
        
        # Start background refresh thread to update time every second
        self._start_refresh_thread()
        
    def _start_refresh_thread(self):
        """Start background thread to refresh display every second."""
        def refresh_loop():
            while not self._stop_refresh.is_set():
                self._update_display()
                time.sleep(1)
        
        self._refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        self._refresh_thread.start()
        
    def increment(self, count=1):
        """Increment counter and update display."""
        with self.lock:
            self.total += count
            self._update_display()
    
    def _update_display(self):
        """Update the persistent bottom line."""
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        tx_per_sec = self.total / elapsed if elapsed > 0 else 0
        
        line = f"{self.description}: {self.total:,} tx | {minutes:02d}:{seconds:02d} | {tx_per_sec:.1f} tx/s"
        
        # Clear previous line and write new one (carriage return moves cursor to start)
        spaces_needed = max(0, len(self._last_line) - len(line))
        sys.stdout.write('\r' + line + ' ' * spaces_needed)
        sys.stdout.flush()
        self._last_line = line
    
    def close(self):
        """Finalize the display."""
        self._stop_refresh.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=2)
        with self.lock:
            sys.stdout.write('\n')
            sys.stdout.flush()


def retry_api_call(api_func, description, max_retries=None, backoff_base=None, timeout=None, use_timeout_param=False):
    """Generic retry wrapper with exponential backoff."""
    global _rate_limiter
    max_retries = max_retries or RETRY_CONFIG['max_retries']
    backoff_base = backoff_base or RETRY_CONFIG['backoff_base']
    timeout = timeout or API_CONFIG['timeout_default']
    
    for attempt in range(max_retries):
        try:
            result = api_func(timeout) if use_timeout_param else api_func()
            # Use rate limiter instead of direct sleep to respect global rate limiting
            if _rate_limiter:
                _rate_limiter.acquire()
            else:
                time.sleep(RATE_LIMIT_DELAY)
            return result
        except Exception as retry_error:
            if attempt < max_retries - 1:
                wait_time = backoff_base * (attempt + 1)
                log(f"Attempt {attempt + 1}/{max_retries} failed for {description}, retrying in {wait_time}s...", "WARN")
                time.sleep(wait_time)
            else:
                raise retry_error


def handle_etl_error(e, operation_name, conn=None):
    """Standardized ETL error handling with rollback."""
    log(f"Failed {operation_name}: {e}", "ERROR")
    import traceback
    log(traceback.format_exc(), "ERROR")
    if conn:
        conn.rollback()
        log("  Rolled back transaction - continuing ETL", "WARN")


def build_endpoint_params(endpoint_name, season, season_type_name, entity='player', custom_params=None):
    """
    Build standardized parameters for NBA API endpoints based on endpoint type.
    Centralizes parameter selection logic to ensure consistency across execution paths.
    
    Args:
        endpoint_name: Name of the endpoint (e.g., 'leaguedashptstats', 'leaguehustlestatsteam')
        season: Season string (e.g., '2024-25')
        season_type_name: Season type name (e.g., 'Regular Season')
        entity: 'player' or 'team' (for endpoints with player_or_team parameter)
        custom_params: Additional endpoint-specific parameters to merge
        
    Returns:
        Dict of parameters ready for endpoint call
    """
    params = {
        'season': season,
        'timeout': API_CONFIG['timeout_default']
    }
    
    # Add parameters based on endpoint patterns
    if 'leaguedashpt' in endpoint_name or 'leaguedash' in endpoint_name:
        params['per_mode_simple'] = API_CONFIG['per_mode_simple']
        params['season_type_all_star'] = season_type_name
        # Only add player_or_team for leaguedashptstats
        if endpoint_name == 'leaguedashptstats':
            params['player_or_team'] = API_CONFIG['player_or_team_player'] if entity == 'player' else API_CONFIG['player_or_team_team']
    elif 'hustle' in endpoint_name:
        params['per_mode_time'] = API_CONFIG['per_mode_time']
        params['season_type_all_star'] = season_type_name
    else:
        # Default: include common parameters
        params['season_type_all_star'] = season_type_name
    
    # Merge custom parameters (overrides defaults if conflicts)
    if custom_params:
        params.update(custom_params)
    
    return params


def execute_generic_endpoint(endpoint_name, endpoint_params, season, 
                            entity='player', table='player_season_stats', 
                            season_type=1, season_type_name='Regular Season', description=None):
    """
    Universal config-driven endpoint executor for any NBA API endpoint.
    Automatically detects and handles per-team endpoints (execution_tier='team').
    
    Execution strategy (inferred from config):
    - League-wide: 1 API call returns all entities
    - Per-team: 30 API calls (one per team), results aggregated
    - Per-player: Handled elsewhere via subprocesses
    """
    # Derive year from season string
    season_year = int('20' + season.split('-')[1])
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
    
    log(f"Fetching {description} - {season_type_name}...")
    
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
        log(f"  No columns configured for {endpoint_name} - skipping")
        return 0
    
    # CHECK: Does this endpoint require per-team execution?
    # Look for execution_tier='team' in any column's source config
    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
    needs_team_iteration = any(
        col.get(source_key, {}).get('execution_tier') == 'team'
        for col in cols.values()
    )
    
    if needs_team_iteration:
        # PER-TEAM EXECUTION: Loop through all 30 teams and aggregate results
        return _execute_per_team_endpoint(
            endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols
        )
    else:
        # LEAGUE-WIDE EXECUTION: Single API call returns all entities
        return _execute_league_wide_endpoint(
            endpoint_name, endpoint_params, season,
            entity, table, season_type, season_type_name, description, cols
        )


def _execute_league_wide_endpoint(endpoint_name, endpoint_params, season,
                                  entity, table, season_type, season_type_name, description, cols):
    """Execute league-wide endpoint (1 API call returns all entities)."""
    # Both tables store year as VARCHAR:
    # - player_season_stats: stores season string like '2024-25'
    # - team_season_stats: stores year integer as string like '2025'
    if table == 'player_season_stats':
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
                               'PaceAdjust', 'Rank', 'PlusMinus', 'TeamPlayerOnOffSummary']
            ]
            
            if not endpoint_classes:
                log(f"  ERROR: No endpoint class found in {module_name}", "ERROR")
                return 0
            
            class_name = endpoint_classes[0]
            EndpointClass = getattr(module, class_name)
            
        except (ImportError, AttributeError) as e:
            log(f"  ERROR: Could not import endpoint from {module_name}: {e}", "ERROR")
            return 0
        
        # Build parameters using centralized logic
        all_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, endpoint_params)
        
        # Call endpoint
        endpoint = EndpointClass(**all_params)
        result = endpoint.get_dict()
        
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
                        raw_value = 0
                    else:
                        raw_value = row[headers.index(nba_field)]
                    
                    if transform_name == 'safe_int':
                        value = safe_int(raw_value, scale)
                    elif transform_name == 'safe_float':
                        value = safe_float(raw_value, scale)
                    else:
                        value = safe_int(raw_value, scale)
                        
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
            track_transaction(1)
            return updated
        else:
            conn.commit()
            track_transaction(1)
            return 0
        
    except Exception as e:
        handle_etl_error(e, description, conn)
        return 0
    finally:
        cursor.close()
        conn.close()


def _execute_per_team_endpoint(endpoint_name, endpoint_params, season,
                               entity, table, season_type, season_type_name, description, cols):
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
    if table == 'player_season_stats':
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
            log(f"  ERROR: No endpoint class found in {module_name}", "ERROR")
            return 0
        
        EndpointClass = getattr(module, endpoint_classes[0])
        
    except (ImportError, AttributeError) as e:
        log(f"  ERROR: Could not import {module_name}: {e}", "ERROR")
        return 0
    
    # Build parameters using centralized logic, then add per-team specific params
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
    
    # Get team IDs - filter for test mode if applicable
    team_ids = list(TEAM_IDS.values())
    team_ids = filter_teams_for_test_mode(team_ids)
    
    # Loop through teams
    for team_id in team_ids:
        try:
            if _rate_limiter:
                _rate_limiter.acquire()
            
            # Call API with team_id
            params = {**base_params, 'team_id': team_id}
            try:
                result = EndpointClass(**params).get_dict()
            except TypeError as te:
                # More detailed error for debugging
                log(f"  ⚠ Failed team {team_id}: {te} | Endpoint: {EndpointClass.__name__} | Params: {list(params.keys())}", "WARNING")
                continue
            
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
                    
                    # Process each row
                    for row in rs['rowSet']:
                        entity_id = row[entity_id_idx]
                        
                        # Filter by defender_distance_category (contested vs open)
                        passes_filter = True
                        if 'defender_distance_category' in first_col_source:
                            category = first_col_source['defender_distance_category']
                            
                            # Find the defense distance field
                            defense_field = None
                            if 'CLOSE_DEF_DIST_RANGE' in headers:
                                defense_field = 'CLOSE_DEF_DIST_RANGE'
                            elif 'SHOT_DEFENSE_CATEGORY' in headers:
                                defense_field = 'SHOT_DEFENSE_CATEGORY'
                            
                            if defense_field and category in DEFENDER_DISTANCE_API_MAP:
                                dist_value = row[headers.index(defense_field)]
                                allowed_buckets = DEFENDER_DISTANCE_API_MAP[category]
                                if dist_value not in allowed_buckets:
                                    passes_filter = False
                        
                        if not passes_filter:
                            continue
                        
                        # Initialize entity if first time seeing it
                        if entity_id not in stats_dict:
                            stats_dict[entity_id] = {col: 0 for col in cols.keys()}
                        
                        # Extract and aggregate stats
                        for col_name, col_cfg in cols.items():
                            source = col_cfg.get(source_key, {})
                            field_name = source.get('field')
                            transform_name = source.get('transform', 'safe_int')
                            scale = source.get('scale', 1)
                            
                            if field_name and field_name in headers:
                                raw_value = row[headers.index(field_name)]
                                
                                if transform_name == 'safe_int':
                                    value = safe_int(raw_value, scale)
                                elif transform_name == 'safe_float':
                                    value = safe_float(raw_value, scale)
                                else:
                                    value = safe_int(raw_value, scale)
                                
                                # AGGREGATE: Sum across teams and defender distance rows
                                stats_dict[entity_id][col_name] += value
                    
                    break  # Found the result set, no need to check others
            
            track_transaction(1)  # Track API call
            
        except Exception as e:
            log(f"  ⚠ Failed team {team_id}: {e}", "WARNING")
            continue
    
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
    
    # Write aggregated results to database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        set_clause = ', '.join([f"{quote_column(col)} = %s" for col in cols.keys()])
        
        updated = 0
        for entity_id, stats in entity_stats.items():
            values = [stats[col] for col in cols.keys()]
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
        return updated
        
    except Exception as e:
        handle_etl_error(e, f"{description} (aggregation)", conn)
        return 0
    finally:
        cursor.close()
        conn.close()


def log(message, level="INFO"):
    """Log message above the transaction tracker."""
    global _transaction_tracker
    if _transaction_tracker:
        # Clear tracker line completely
        with _transaction_tracker.lock:
            if _transaction_tracker._last_line:
                # Clear the line and move cursor to start
                sys.stdout.write('\r' + ' ' * len(_transaction_tracker._last_line) + '\r')
                sys.stdout.flush()
            # Print the message with newline
            sys.stdout.write(message + '\n')
            sys.stdout.flush()
            # Redraw tracker on same line (no newline)
            _transaction_tracker._update_display()
    else:
        print(message)


def track_transaction(count=1):
    """Track a transaction."""
    global _transaction_tracker
    if _transaction_tracker is not None:
        _transaction_tracker.increment(count=count)


def quote_column(col_name):
    if col_name[0].isdigit() or not col_name.replace('_', '').isalnum():
        return f'"{col_name}"'
    return col_name


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        application_name='the_glass_etl',
        options=f'-c statement_timeout={DB_OPERATIONS["statement_timeout_ms"]}'
    )


# ============================================================================
# TEST MODE HELPERS
# ============================================================================

def is_test_mode():
    """Check if ETL is running in test mode."""
    return os.getenv('ETL_TEST_MODE') == '1'

def get_test_player_id():
    """Get test player ID from config."""
    return TEST_MODE_CONFIG['player_id']

def get_test_team_id():
    """Get test team ID from config."""
    return TEST_MODE_CONFIG['team_id']

def get_season():
    """Get season string - test season if in test mode, current season otherwise."""
    if is_test_mode():
        return TEST_MODE_CONFIG['season']
    return NBA_CONFIG['current_season']

def get_season_year():
    """Get season year from season string (e.g., '2024-25' -> 2025)."""
    season = get_season()
    # Extract ending year from season string (e.g., '2024-25' -> 2025)
    return int('20' + season.split('-')[1])

def filter_players_for_test_mode(player_ids):
    """Filter player list to only test player if in test mode."""
    if not is_test_mode():
        return player_ids
    test_id = get_test_player_id()
    return [test_id] if test_id in player_ids else []

def filter_teams_for_test_mode(team_ids):
    """Filter team list to only test team if in test mode."""
    if not is_test_mode():
        return team_ids
    test_id = get_test_team_id()
    return [test_id] if test_id in team_ids else []


# ============================================================================
# TRANSFORMATION ENGINE - Config-Driven Post-Processing
# ============================================================================

def get_player_ids_for_season(season, season_type):
    """
    Get all distinct player IDs for a given season and season type.
    Consolidates repeated SQL query pattern used in transformation functions.
    
    Args:
        season: Season string (e.g., '2024-25')
        season_type: Season type code (1=Regular, 2=Playoffs, 3=PlayIn)
        
    Returns:
        List of player IDs sorted in ascending order
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT pss.player_id
        FROM player_season_stats pss
        WHERE pss.year = %s AND pss.season_type = %s
        ORDER BY pss.player_id
    """, (season, season_type))
    
    player_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return player_ids

def _get_endpoint_class(endpoint_name):
    """
    Convert endpoint name to proper PascalCase class name.
    NBA API uses specific PascalCase (e.g., PlayerDashPtShots, not Playerdashptshots).
    """
    from importlib import import_module
    
    module_name = f"nba_api.stats.endpoints.{endpoint_name.lower()}"
    module = import_module(module_name)
    
    # Try to find the class by checking module attributes
    # The class name should start with the same letters but in PascalCase
    # Must match ENTIRE name (not just starts with)
    for attr_name in dir(module):
        if attr_name.lower() == endpoint_name.lower() and attr_name[0].isupper():
            return getattr(module, attr_name)
    
    # Fallback: raise error with available classes
    available_classes = [name for name in dir(module) if name[0].isupper() and not name.startswith('_')]
    raise AttributeError(
        f"Could not find endpoint class for '{endpoint_name}'. "
        f"Available classes in module: {', '.join(available_classes)}"
    )

def apply_transformation(column_name, transform, season, entity='player', table='player_season_stats', season_type=1, season_type_name='Regular Season', source_config=None):
    """
    Apply config-driven transformation to extract/calculate a column value.
    
    Transformation types:
    - simple_extract: Extract single field from API result
    - arithmetic_subtract: Subtract two API values
    - filter_aggregate: Filter rows by field value, aggregate
    - multi_call_aggregate: Make multiple API calls, aggregate results
    - calculated_rating: Formula-based calculation
    
    Args:
        column_name: Name of column to transform
        transform: Transformation config dict from DB_COLUMNS
        season: Season string (e.g., '2024-25')
        entity: 'player' or 'team'
        table: Target database table
        season_type: Season type code (1=Regular, 2=Playoffs, 3=PlayIn)
        season_type_name: Season type name for API calls
        source_config: Full source config dict (contains endpoint at outer level)
        
    Returns:
        Dict of {entity_id: value} or raises exception
    """
    # Inject endpoint and execution_tier from outer source_config into transform if not present
    if source_config:
        if 'endpoint' in source_config and 'endpoint' not in transform:
            transform = dict(transform)  # Create copy to avoid modifying original
            transform['endpoint'] = source_config['endpoint']
        if 'execution_tier' in source_config and 'execution_tier' not in transform:
            if not isinstance(transform, dict) or transform is source_config.get('transformation'):
                transform = dict(transform)  # Create copy if not already
            transform['execution_tier'] = source_config['execution_tier']
    
    transform_type = transform['type']
    execution_tier = transform.get('execution_tier')
    
    # Route to appropriate transformation function based on type and execution tier
    if transform_type == 'simple_extract':
        # Check execution tier to determine which variant to call
        if entity == 'player' and execution_tier == 'player':
            return _apply_simple_extract_per_player(transform, season, season_type, season_type_name)
        elif entity == 'player' and execution_tier == 'team':
            return _apply_simple_extract_per_team(transform, season, season_type, season_type_name)
        else:
            return _apply_simple_extract_league_wide(transform, season, entity, season_type, season_type_name)
    
    elif transform_type == 'team_aggregate':
        return _apply_team_aggregate(transform, season, entity, season_type, season_type_name)
    
    elif transform_type == 'arithmetic_subtract':
        # Check execution tier to determine which variant to call
        if entity == 'player' and execution_tier == 'player':
            return _apply_arithmetic_subtract_per_player(transform, season, season_type, season_type_name)
        elif entity == 'team' and execution_tier == 'team':
            return _apply_arithmetic_subtract_per_team(transform, season, season_type, season_type_name)
        else:
            return _apply_arithmetic_subtract_league_wide(transform, season, entity, season_type, season_type_name)
    
    elif transform_type == 'filter_aggregate':
        # Check execution tier to determine which variant to call
        if entity == 'player' and execution_tier == 'player':
            return _apply_filter_aggregate_per_player(transform, season, season_type, season_type_name)
        else:
            return _apply_filter_aggregate_team(transform, season, entity, season_type, season_type_name)
    
    elif transform_type == 'multi_call_aggregate':
        return _apply_multi_call_aggregate(transform, season, season_year, entity, season_type, season_type_name)
    
    elif transform_type == 'calculated_rating':
        return _apply_calculated_rating(transform, season, season_year, entity, season_type, season_type_name)
    
    else:
        raise ValueError(f"Unknown transformation type: {transform_type}")


def _apply_simple_extract_league_wide(transform, season, entity, season_type=1, season_type_name='Regular Season'):
    """Extract single field from API result for league-wide endpoints."""
    # For league-wide endpoints, call directly
    endpoint_name = transform['endpoint']
    EndpointClass = _get_endpoint_class(endpoint_name)
    
    # Build parameters from config
    params = {'season': season, 'timeout': API_CONFIG['timeout_default']}
    params.update(transform.get('params', {}))
    
    # Call API
    result = EndpointClass(**params).get_dict()
    
    # Extract data
    data = {}
    for rs in result['resultSets']:
        headers = rs['headers']
        for row in rs['rowSet']:
            entity_id = row[0]  # First column is always ID
            field_value = row[headers.index(transform['field'])]
            
            # Apply transform if specified
            if transform.get('transform') == 'scale_1000':
                field_value = safe_int(field_value, 1000)
            
            data[entity_id] = field_value
    
    return data

def _apply_simple_extract_per_player(transform, season, season_type=1, season_type_name='Regular Season'):
    """Extract single field from API result for per-player endpoints."""
    # Get all players for this season type
    player_ids = get_player_ids_for_season(season, season_type)
    
    # Execute via subprocesses
    EndpointClass = _get_endpoint_class(transform["endpoint"])
    endpoint_params = build_endpoint_params(transform['endpoint'], season, season_type_name, 'player', transform.get('endpoint_params', {}))
    
    results = execute_player_endpoint_in_subprocesses(
        endpoint_module=f'nba_api.stats.endpoints.{transform["endpoint"].lower()}',
        endpoint_class=EndpointClass.__name__,
        player_ids=player_ids,
        endpoint_params=endpoint_params,
        description=f'Transformation: {transform.get("description", transform["endpoint"])}'
    )
    
    # Process results
    data = {}
    result_set_name = transform['result_set']
    filter_spec = transform.get('filter', {})
    field_name = transform['field']
    
    for success in results['successes']:
        player_id = success['player_id']
        result = success['data']  # Subprocess worker stores response as 'data'
        
        # Find matching result set and extract field
        for rs in result['resultSets']:
            if rs['name'] == result_set_name:
                headers = rs['headers']
                for row in rs['rowSet']:
                    row_dict = dict(zip(headers, row))
                    # Check if row matches filter
                    matches_filter = all(row_dict.get(k) == v for k, v in filter_spec.items())
                    if matches_filter:
                        data[player_id] = row_dict.get(field_name, 0)
                        break
                break
        
        # Default to 0 if not found
        if player_id not in data:
            data[player_id] = 0
        
        track_transaction(1)
    
    # Handle failures
    for failure in results['failures']:
        data[failure['player_id']] = 0
        track_transaction(1)
    
    return data


def _apply_simple_extract_per_team(transform, season, season_type=1, season_type_name='Regular Season'):
    """Extract single field from API result for per-team endpoints (e.g., teamplayeronoffdetails)."""
    global _rate_limiter, _transaction_tracker
    
    EndpointClass = _get_endpoint_class(transform['endpoint'])
    endpoint_params = build_endpoint_params(transform['endpoint'], season, season_type_name, 'player', transform.get('endpoint_params', {}))
    
    result_set_name = transform['result_set']
    field_name = transform['field']
    
    # Get transform function if specified
    transform_func = None
    scale = 1
    if transform.get('transform') == 'safe_int':
        transform_func = safe_int
        scale = transform.get('scale', 1)
    elif transform.get('transform') == 'safe_float':
        transform_func = safe_float
        scale = transform.get('scale', 1)
    
    data = {}
    
    # Get team IDs - filter for test mode if applicable
    team_ids = list(TEAM_IDS.values())
    team_ids = filter_teams_for_test_mode(team_ids)
    
    # Loop through teams
    for team_id in team_ids:
        if _rate_limiter:
            _rate_limiter.acquire()
        
        try:
            # Call API with team_id
            params = {**endpoint_params, 'team_id': team_id}
            result = EndpointClass(**params).get_dict()
            
            # Find the result set
            for rs in result['resultSets']:
                if rs['name'] == result_set_name:
                    headers = rs['headers']
                    if field_name not in headers:
                        continue
                    
                    field_idx = headers.index(field_name)
                    
                    # Get player ID field name from transform config, default to 'PLAYER_ID'
                    player_id_field = transform.get('player_id_field', 'PLAYER_ID')
                    player_id_idx = headers.index(player_id_field) if player_id_field in headers else 0
                    
                    for row in rs['rowSet']:
                        player_id = row[player_id_idx]
                        
                        # Skip non-numeric player IDs (e.g., "On/Off Court" summary rows)
                        if not isinstance(player_id, int):
                            try:
                                player_id = int(player_id)
                            except (ValueError, TypeError):
                                continue  # Skip this row
                        
                        field_value = row[field_idx]
                        
                        # Apply transform if specified
                        if transform_func:
                            field_value = transform_func(field_value, scale)
                        
                        data[player_id] = field_value
                    break
            
            track_transaction(1)  # Track API call
                
        except Exception as e:
            log(f"⚠ Failed team {team_id}: {e}", "WARNING")
            continue
    
    return data


def _apply_team_aggregate(transform, season, entity, season_type=1, season_type_name='Regular Season'):
    """
    Call per-team endpoint and AGGREGATE results for players on multiple teams.
    Used for stats like contested rebounds where a player may have played for multiple teams.
    
    Example: Player traded mid-season
    - Team A: 50 OREB_CONTEST
    - Team B: 30 OREB_CONTEST
    - Season total: 80 OREB_CONTEST (summed)
    """
    global _rate_limiter
    
    EndpointClass = _get_endpoint_class(transform['endpoint'])
    endpoint_params = build_endpoint_params(transform['endpoint'], season, season_type_name, 'player', transform.get('endpoint_params', {}))
    
    result_set_name = transform['result_set']
    field_name = transform['field']
    
    # Dictionary to aggregate values: {player_id: total_value}
    player_totals = {}
    
    # Get team IDs - filter for test mode if applicable
    team_ids = list(TEAM_IDS.values())
    team_ids = filter_teams_for_test_mode(team_ids)
    
    # Loop through teams
    for team_id in team_ids:
        if _rate_limiter:
            _rate_limiter.acquire()
        
        try:
            # Call API with team_id
            params = {**endpoint_params, 'team_id': team_id}
            result = EndpointClass(**params).get_dict()
            
            # Find the result set and extract player data
            for rs in result['resultSets']:
                if rs['name'] == result_set_name:
                    headers = rs['headers']
                    if field_name not in headers:
                        continue
                    
                    field_idx = headers.index(field_name)
                    player_id_idx = headers.index('PLAYER_ID') if 'PLAYER_ID' in headers else 0
                    
                    for row in rs['rowSet']:
                        player_id = row[player_id_idx]
                        field_value = row[field_idx] or 0  # Handle None
                        
                        # Convert to int
                        try:
                            field_value = int(field_value)
                        except (ValueError, TypeError):
                            field_value = 0
                        
                        # AGGREGATE: Sum across all teams for this player
                        if player_id in player_totals:
                            player_totals[player_id] += field_value
                        else:
                            player_totals[player_id] = field_value
                    break
            
            track_transaction(1)
                
        except Exception as e:
            log(f"⚠ Failed team {team_id}: {e}", "WARNING")
            continue
    
    return player_totals


def _apply_arithmetic_subtract_league_wide(transform, season, entity, season_type=1, season_type_name='Regular Season'):
    """Subtract two API values for league-wide endpoints."""
    # For league-wide endpoints, call directly
    endpoint_name = transform['endpoint']
    EndpointClass = _get_endpoint_class(endpoint_name)
    
    subtract_specs = transform['subtract']
    all_values = []
    
    # Fetch each value separately
    for spec in subtract_specs:
        params = {'season': season, 'timeout': API_CONFIG['timeout_default']}
        params.update(spec.get('params', {}))
        
        result = EndpointClass(**params).get_dict()
        
        # Extract values
        values = {}
        for rs in result['resultSets']:
            if 'result_set' in spec and rs['name'] != spec['result_set']:
                continue
                
            headers = rs['headers']
            for row in rs['rowSet']:
                entity_id = row[0]
                
                # Apply filter if specified
                if 'filter' in spec:
                    matches = all(row[headers.index(k)] == v for k, v in spec['filter'].items() if k in headers)
                    if not matches:
                        continue
                
                field_value = row[headers.index(spec['field'])] or 0
                values[entity_id] = values.get(entity_id, 0) + field_value
        
        all_values.append(values)
    
    # Subtract: first - second
    result_data = {}
    for entity_id in all_values[0].keys():
        val1 = all_values[0].get(entity_id, 0)
        val2 = all_values[1].get(entity_id, 0) if len(all_values) > 1 else 0
        result_data[entity_id] = max(0, val1 - val2)
    
    return result_data

def _apply_arithmetic_subtract_per_player(transform, season, season_type=1, season_type_name='Regular Season'):
    """Subtract two API values for per-player endpoints using subprocess execution."""
    # Get all players for this season type
    player_ids = get_player_ids_for_season(season, season_type)
    
    # Execute via subprocesses
    EndpointClass = _get_endpoint_class(transform["endpoint"])
    # Build parameters - note: if transform has custom season_type_param, it should be in endpoint_params
    endpoint_params = build_endpoint_params(transform['endpoint'], season, season_type_name, 'player', transform.get('endpoint_params', {}))
    
    results = execute_player_endpoint_in_subprocesses(
        endpoint_module=f'nba_api.stats.endpoints.{transform["endpoint"].lower()}',
        endpoint_class=EndpointClass.__name__,
        player_ids=player_ids,
        endpoint_params=endpoint_params,
        description=f'Transformation: {transform.get("description", transform["endpoint"])}'
    )
    
    # Process results - extract both values and subtract
    data = {}
    subtract_specs = transform['subtract']
    
    for success in results['successes']:
        player_id = success['player_id']
        result = success['data']  # Subprocess worker stores response as 'data'
        
        # Extract both values from the result
        values = []
        for spec in subtract_specs:
            result_set_name = spec['result_set']
            filter_spec = spec.get('filter', {})
            field_name = spec['field']
            value = 0
            
            # Find matching result set and extract field
            for rs in result['resultSets']:
                if rs['name'] == result_set_name:
                    headers = rs['headers']
                    for row in rs['rowSet']:
                        row_dict = dict(zip(headers, row))
                        # Check if row matches filter
                        matches_filter = all(row_dict.get(k) == v for k, v in filter_spec.items())
                        if matches_filter:
                            value = row_dict.get(field_name, 0)
                            break
                    break
            values.append(value)
        
        # Subtract: first - second
        data[player_id] = max(0, values[0] - (values[1] if len(values) > 1 else 0))
        track_transaction(1)
    
    # Handle failures
    for failure in results['failures']:
        data[failure['player_id']] = 0
        track_transaction(1)
    
    return data


def _apply_arithmetic_subtract_per_team(transform, season, season_type=1, season_type_name='Regular Season'):
    """Subtract two API values for per-team endpoints (e.g., teamdashptshots)."""
    global _rate_limiter
    
    EndpointClass = _get_endpoint_class(transform['endpoint'])
    endpoint_params = build_endpoint_params(transform['endpoint'], season, season_type_name, 'team', transform.get('endpoint_params', {}))
    
    subtract_specs = transform['subtract']
    data = {}
    
    # Get team IDs - filter for test mode if applicable
    team_ids = list(TEAM_IDS.values())
    team_ids = filter_teams_for_test_mode(team_ids)
    
    # Loop through teams
    for team_id in team_ids:
        if _rate_limiter:
            _rate_limiter.acquire()
        
        try:
            # Add team_id to parameters
            params = dict(endpoint_params)
            params['team_id'] = team_id
            
            # Call API
            result = EndpointClass(**params).get_dict()
            
            # Extract values from each subtract spec
            values = []
            for spec in subtract_specs:
                result_set_name = spec['result_set']
                filter_spec = spec.get('filter', {})
                field_name = spec['field']
                value = 0
                
                # Find matching result set and extract field
                for rs in result['resultSets']:
                    if rs['name'] == result_set_name:
                        headers = rs['headers']
                        for row in rs['rowSet']:
                            row_dict = dict(zip(headers, row))
                            # Check if row matches filter
                            matches_filter = all(row_dict.get(k) == v for k, v in filter_spec.items())
                            if matches_filter:
                                value = row_dict.get(field_name, 0) or 0
                                break
                        break
                values.append(value)
            
            # Apply formula - default is simple subtraction
            formula = transform.get('formula', 'a - b')
            if formula == '(a + b) - (c + d)' and len(values) >= 4:
                data[team_id] = max(0, (values[0] + values[1]) - (values[2] + values[3]))
            elif len(values) >= 2:
                data[team_id] = max(0, values[0] - values[1])
            else:
                data[team_id] = values[0] if values else 0
            
            track_transaction(1)
            
        except Exception as e:
            log(f"  Failed team {team_id}: {e}", "WARNING")
            data[team_id] = 0
            track_transaction(1)
    
    return data


def _apply_filter_aggregate_team(transform, season, entity, season_type=1, season_type_name='Regular Season'):
    """
    Filter rows by field value, then aggregate - for team endpoints.
    
    Config format (flat): {
        'filter_field': 'FIELD_NAME',
        'filter_values': ['value1', 'value2'],
        'aggregate': 'sum',  # or 'count'
        'field': 'STAT_FIELD'
    }
    """
    from importlib import import_module
    
    # Choose endpoint based on entity
    if entity == 'team':
        endpoint_name = transform.get('team_endpoint') or transform.get('endpoint')
        result_set_name = transform.get('team_result_set') or transform.get('result_set')
    else:
        endpoint_name = transform.get('endpoint')
        result_set_name = transform.get('result_set')
    
    # If endpoint_name is None, this is likely a grouped transformation being called incorrectly
    if not endpoint_name:
        raise ValueError(f"No endpoint specified for filter_aggregate transformation. This may be a grouped transformation that should not be called directly.")
    
    # For team endpoints, call directly
    EndpointClass = _get_endpoint_class(endpoint_name)
    
    data = {}
    
    # Extract filter/aggregate config (flat format only)
    filter_field = transform['filter_field']
    filter_values = transform['filter_values']
    filter_operator = 'equals'  # Standard for filter_aggregate
    agg_field = transform['field']
    agg_function = transform.get('aggregate', 'sum')
    
    # Get team IDs - filter for test mode if applicable
    team_ids = list(TEAM_IDS.values())
    team_ids = filter_teams_for_test_mode(team_ids)
    
    # Make API calls per team
    for team_id in team_ids:
        try:
            params = {'team_id': team_id, 'season': season, 'season_type_all_star': season_type_name, 'timeout': API_CONFIG['timeout_default']}
            # Use team_endpoint_params if available, otherwise fall back to endpoint_params
            if entity == 'team' and 'team_endpoint_params' in transform:
                params.update(transform.get('team_endpoint_params', {}))
            else:
                params.update(transform.get('endpoint_params', {}))
            result = EndpointClass(**params).get_dict()
            
            # Find matching result set
            for rs in result['resultSets']:
                if rs['name'] == result_set_name:
                    headers = rs['headers']
                    total = 0
                    
                    for row in rs['rowSet']:
                        field_value = row[headers.index(filter_field)]
                        
                        # Apply filter based on operator
                        matches = False
                        if filter_operator == 'startswith':
                            matches = any(str(field_value).startswith(fv) for fv in filter_values)
                        elif filter_operator == 'contains':
                            matches = any(fv in str(field_value) for fv in filter_values)
                        elif filter_operator == 'equals':
                            matches = field_value in filter_values
                        
                        if matches:
                            if agg_function == 'sum':
                                total += row[headers.index(agg_field)] or 0
                            elif agg_function == 'count':
                                total += 1
                    
                    data[team_id] = total
                    break
            
            time.sleep(RATE_LIMIT_DELAY)
            track_transaction(1)
            
        except Exception as e:
            log(f"  Failed team {team_id}: {e}", "WARN")
            data[team_id] = 0
    
    return data


def _apply_filter_aggregate_per_player(transform, season, season_type=1, season_type_name='Regular Season'):
    """
    Filter & aggregate for per-player endpoints using subprocess execution.
    
    Config format (flat): {
        'filter_field': 'FIELD_NAME',
        'filter_values': ['value1', 'value2'],
        'aggregate': 'sum',  # or 'count'
        'field': 'STAT_FIELD'
    }
    """
    # Get all players for this season type
    player_ids = get_player_ids_for_season(season, season_type)
    
    # Execute via subprocesses
    EndpointClass = _get_endpoint_class(transform["endpoint"])
    # Build parameters - note: if transform has custom season_type_param, it should be in endpoint_params
    endpoint_params = build_endpoint_params(transform['endpoint'], season, season_type_name, 'player', transform.get('endpoint_params', {}))
    
    results = execute_player_endpoint_in_subprocesses(
        endpoint_module=f'nba_api.stats.endpoints.{transform["endpoint"].lower()}',
        endpoint_class=EndpointClass.__name__,
        player_ids=player_ids,
        endpoint_params=endpoint_params,
        description=f'Transformation: {transform.get("description", transform["endpoint"])}'
    )
    
    # Process results
    data = {}
    result_set_name = transform['result_set']
    
    # Extract filter/aggregate config (flat format only)
    filter_field = transform['filter_field']
    filter_values = transform['filter_values']
    filter_operator = 'equals'  # Standard for filter_aggregate
    agg_field = transform['field']
    agg_function = transform.get('aggregate', 'sum')
    
    for success in results['successes']:
        player_id = success['player_id']
        result = success['data']  # Subprocess worker stores response as 'data'
        
        # Find matching result set and filter
        for rs in result['resultSets']:
            if rs['name'] == result_set_name:
                headers = rs['headers']
                total = 0
                
                for row in rs['rowSet']:
                    field_value = row[headers.index(filter_field)]
                    
                    # Apply filter based on operator
                    matches = False
                    if filter_operator == 'startswith':
                        matches = any(str(field_value).startswith(fv) for fv in filter_values)
                    elif filter_operator == 'contains':
                        matches = any(fv in str(field_value) for fv in filter_values)
                    elif filter_operator == 'equals':
                        matches = field_value in filter_values
                    
                    if matches:
                        if agg_function == 'sum':
                            total += row[headers.index(agg_field)] or 0
                        elif agg_function == 'count':
                            total += 1
                
                data[player_id] = total
                break
        
        track_transaction(1)
    
    # Handle failures
    for failure in results['failures']:
        data[failure['player_id']] = 0
        track_transaction(1)
    
    return data


def _apply_multi_call_aggregate(transform, season, entity, season_type=1, season_type_name='Regular Season'):
    """Make multiple API calls and aggregate results."""
    endpoint_name = transform['endpoint']
    EndpointClass = _get_endpoint_class(endpoint_name)
    
    # Initialize parallel executor for league-wide endpoints
    global _parallel_executor, _rate_limiter
    if _parallel_executor is None:
        if _rate_limiter is None:
            _rate_limiter = RateLimiter(requests_per_second=1.67)
        _parallel_executor = ParallelAPIExecutor(
            max_workers=MAX_WORKERS_LEAGUE,
            rate_limiter=_rate_limiter,
            log_func=log
        )
    
    # Build tasks for all API calls
    tasks = []
    for idx, call_params in enumerate(transform['calls']):
        def make_call(params=call_params, stype_name=season_type_name):
            full_params = {
                'season': season,
                'per_mode_simple': 'Totals',
                'season_type_all_star': stype_name,
                'timeout': 20
            }
            full_params.update(params)
            return EndpointClass(**full_params).get_dict()
        
        tasks.append({
            'id': f'call_{idx}',
            'func': make_call,
            'description': f'{endpoint_name} call {idx+1}',
            'max_retries': 3
        })
    
    # Execute in parallel
    results, errors, failed_ids = _parallel_executor.execute_batch(
        tasks,
        description=f"Multi-call aggregate: {transform.get('description', endpoint_name)}"
    )
    
    # Aggregate results
    aggregated_data = {}
    field_name = transform['field']
    
    for call_id, result in results.items():
        for rs in result['resultSets']:
            headers = rs['headers']
            for row in rs['rowSet']:
                entity_id = row[0]
                field_value = row[headers.index(field_name)] or 0
                aggregated_data[entity_id] = aggregated_data.get(entity_id, 0) + field_value
    
    return aggregated_data


def _apply_calculated_rating(transform, season, entity, season_type=1, season_type_name='Regular Season'):
    """Calculate rating using formula."""
    endpoint_name = transform['endpoint']
    EndpointClass = _get_endpoint_class(endpoint_name)
    
    data = {}
    
    # Get team IDs - filter for test mode if applicable
    team_ids = list(TEAM_IDS.values())
    team_ids = filter_teams_for_test_mode(team_ids)
    
    # Make API calls per team
    for team_id in team_ids:
        try:
            result = EndpointClass(
                team_id=team_id,
                season=season,
                per_mode_detailed=API_CONFIG['per_mode_detailed'],
                season_type_all_star=season_type_name
            ).get_dict()
            
            # Find result set
            for rs in result['resultSets']:
                if rs['name'] == transform['result_set']:
                    headers = rs['headers']
                    
                    for row in rs['rowSet']:
                        player_id = row[headers.index('VS_PLAYER_ID')]
                        
                        # Calculate possessions if needed
                        if 'possession_formula' in transform:
                            fga = row[headers.index('FGA')] or 0
                            oreb = row[headers.index('OREB')] or 0
                            tov = row[headers.index('TOV')] or 0
                            fta = row[headers.index('FTA')] or 0
                            poss = fga - oreb + tov + (0.44 * fta)
                        else:
                            poss = 1
                        
                        # Skip if no possessions
                        if poss <= 0:
                            continue
                        
                        # Get field values
                        pts = row[headers.index('PTS')] or 0
                        plus_minus = row[headers.index('PLUS_MINUS')] or 0
                        
                        # Evaluate formula
                        formula = transform['formula']
                        if 'PLUS_MINUS' in formula:
                            value = int(eval(formula, {'PTS': pts, 'POSS': poss, 'PLUS_MINUS': plus_minus}))
                        else:
                            value = int(eval(formula, {'PTS': pts, 'POSS': poss}))
                        
                        data[player_id] = value
                    
                    break
            
            time.sleep(RATE_LIMIT_DELAY)
            track_transaction(1)
            
        except Exception as e:
            log(f"  Failed team {team_id}: {e}", "WARN")
    
    return data


def ensure_schema_exists():
    """Create database schema if it doesn't exist (first-time setup)"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if tables exist
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'players'
        )
    """)
    
    if cursor.fetchone()[0]:
        cursor.close()
        conn.close()
        return
    
    log("Creating database schema...")
    
    # Use centralized schema DDL from config
    cursor.execute(DB_SCHEMA['create_schema_sql'])
    conn.commit()
    
    log("Schema created successfully")
    
    cursor.close()
    conn.close()


def update_player_rosters():
    """
    FAST daily roster update:
    1. Fetch player stats (current + last season) - 2 API calls, very fast
    2. Fetch team rosters to get team_id + jersey_number - 30 API calls, ~30 seconds
    3. Only fetch height/weight/birthdate for NEW players (rare)
    
    This completes in ~2-3 minutes instead of 20 minutes.
    Height/weight/birthdate for existing players updated annually on August 1st.
    
    Returns: (players_added, players_updated) for progress bar adjustment
    """
    global _rate_limiter, _parallel_executor
    
    # Initialize rate limiter if needed
    if _rate_limiter is None:
        _rate_limiter = RateLimiter(requests_per_second=2.5)  # 150 req/min confirmed max
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    current_season = get_season()
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW
    # This is the SOURCE OF TRUTH for current team assignments
    log("Fetching commonteamroster...")
    try:
        from nba_api.stats.static import teams
        from nba_api.stats.endpoints import commonteamroster
        nba_teams = teams.get_teams()
        
        # OPTIMIZATION: Parallel roster fetching (30 teams -> ~10 seconds instead of 30)
        # TIER 2: Per-team endpoint (30 API calls) - use high parallelism
        global _parallel_executor
        if _parallel_executor is None:
            _parallel_executor = ParallelAPIExecutor(
                max_workers=MAX_WORKERS_TEAM,  # TIER 2: 10 workers for 30 teams
                rate_limiter=_rate_limiter,
                log_func=log
            )
        
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
        results, errors, failed_ids = _parallel_executor.execute_batch(
            tasks, 
            description=f"Team rosters for {current_season}",
            progress_callback=lambda count: track_transaction(count)  # Update tracker after each team roster call
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
                    all_players[player_id] = {
                        'player_id': player_id,
                        'team_id': team_id,  # Use team from roster
                        'name': player_name,
                        'jersey': safe_str(player_row.get('NUM')),
                        'weight': None,  # Will get from annual ETL or commonplayerinfo for new players
                        'age': None
                    }
                
                track_transaction(1)  # Track team roster API call
            except Exception as e:
                log(f"  WARNING - Failed to process roster for team {team_id}: {e}", "WARN")
        
    except Exception as e:
        log(f"WARNING - Failed to fetch current rosters: {e}", "WARN")
        import traceback
        log(traceback.format_exc(), "WARN")
    
    # Get existing players from database to identify NEW players
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT player_id FROM {TABLES[1]}")  # TABLES[1] = 'players'
    existing_player_ids = {row[0] for row in cursor.fetchall()}
    
    # Identify NEW players (not in database)
    new_player_ids = [pid for pid in all_players.keys() if pid not in existing_player_ids]
    
    if new_player_ids:
        log(f"Inserting {len(new_player_ids)} new players...")
        
        # OPTIMIZATION: Parallel player detail fetching
        tasks = []
        for player_id in new_player_ids:
            player_name = all_players[player_id].get('name', 'Unknown')
            # Lambda must accept timeout parameter (passed by executor)
            tasks.append({
                'id': player_id,
                'func': lambda timeout, pid=player_id: commonplayerinfo.CommonPlayerInfo(
                    player_id=pid,
                    timeout=timeout
                ),
                'description': f"Details for {player_name}",
                'max_retries': 3
            })
        
        # Execute in parallel (TIER 3: per-player endpoint, use cautious parallelism)
        detail_executor = ParallelAPIExecutor(
            max_workers=MAX_WORKERS_PLAYER,  # TIER 3: commonplayerinfo is per-player
            rate_limiter=_rate_limiter,
            log_func=log
        )
        
        results, errors, failed_ids = detail_executor.execute_batch(
            tasks,
            description="New player details",
            # progress_callback removed - using track_transaction() directly  # Update progress as tasks complete
        )
        
        # Process results (no need to update progress - already done in callback)
        for player_id, info_endpoint in results.items():
            try:
                player_df = info_endpoint.get_data_frames()[0]
                if not player_df.empty:
                    pd = player_df.iloc[0]
                    all_players[player_id].update({
                        'birthdate': parse_birthdate(pd.get('BIRTHDATE')),
                        'height': parse_height(pd.get('HEIGHT')),
                        'weight': safe_int(pd.get('WEIGHT')),
                        'jersey': safe_str(pd.get('JERSEY')),
                        'years_experience': safe_int(pd.get('SEASON_EXP')),
                        'pre_nba_team': safe_str(pd.get('SCHOOL'))
                    })
                    track_transaction(1)  # Track player detail fetch
            except Exception as e:
                log(f"  WARNING - Failed to process details for player {player_id}: {e}", "WARN")
                track_transaction(1)  # Track even on error
        
        if errors:
            log(f"WARNING - Could not fetch details for {len(errors)}/{len(new_player_ids)} new players", "WARN")
            log("  These players will still be added with basic info (name, team, jersey from roster)", "WARN")
    
    # First, clear team_id for all players (they'll be re-assigned if still on roster)
    # In test mode, only clear test team to avoid affecting other data
    if is_test_mode():
        test_team_id = get_test_team_id()
        cursor.execute("UPDATE players SET team_id = NULL, updated_at = NOW() WHERE team_id = %s", (test_team_id,))
    else:
        cursor.execute("UPDATE players SET team_id = NULL, updated_at = NOW()")
    conn.commit()
    track_transaction(1)  # Track bulk update
    
    # TEST MODE: Filter to only test player before database operations
    if is_test_mode():
        test_player_id = get_test_player_id()
        all_players = {pid: pdata for pid, pdata in all_players.items() if pid == test_player_id}
    
    bulk_writer = BulkDatabaseWriter(conn, batch_size=DB_OPERATIONS['bulk_insert_batch_size'], log_func=log)
    
    # Separate new players from updates
    new_players_data = []
    update_players_data = []
    
    cursor.execute(f"SELECT player_id, team_id FROM {TABLES[1]}")  # TABLES[1] = 'players'
    existing_players = {row[0]: row[1] for row in cursor.fetchall()}
    
    # TEST MODE: Already filtered above at line 2034-2037, no need to filter again
    
    for player_id, player_data in all_players.items():
        if player_id in existing_players:
            # Existing player - check if team changed
            if existing_players[player_id] != player_data['team_id']:
                players_updated += 1
            
            # Prepare update data
            if 'birthdate' in player_data:
                update_players_data.append((
                    player_data['team_id'], player_data['jersey'],
                    player_data.get('weight'), player_data.get('height'),
                    player_data.get('pre_nba_team'), player_data.get('birthdate'),
                    player_id  # WHERE clause
                ))
            else:
                update_players_data.append((
                    player_data['team_id'], player_data['jersey'],
                    player_id  # WHERE clause
                ))
        else:
            # New player
            players_added += 1
            if 'birthdate' in player_data:
                new_players_data.append((
                    player_id, player_data['name'], player_data['team_id'],
                    player_data['jersey'], player_data.get('weight'),
                    player_data.get('height'), player_data.get('pre_nba_team'),
                    player_data.get('birthdate')
                ))
            else:
                new_players_data.append((
                    player_id, player_data['name'], player_data['team_id'],
                    player_data['jersey']
                ))
    
    # Bulk update existing players (team_id and jersey changes)
    if update_players_data:
        # Separate by column count (with/without bio details)
        detailed_updates = [p for p in update_players_data if len(p) == 7]
        basic_updates = [p for p in update_players_data if len(p) == 3]
        
        if detailed_updates:
            # Update with bio fields (for new players that got details)
            cursor.executemany(
                """
                UPDATE players 
                SET team_id = %s, jersey_number = %s, weight_lbs = %s, 
                    height_inches = %s, pre_nba_team = %s, birthdate = %s, 
                    updated_at = NOW()
                WHERE player_id = %s
                """,
                detailed_updates
            )
            track_transaction(len(detailed_updates))
        
        if basic_updates:
            # Update only team_id and jersey (most common case)
            cursor.executemany(
                """
                UPDATE players 
                SET team_id = %s, jersey_number = %s, updated_at = NOW()
                WHERE player_id = %s
                """,
                basic_updates
            )
            track_transaction(len(basic_updates))
    
    # Bulk insert new players (with and without details)
    if new_players_data:
        # Separate by column count (with/without extra details)
        detailed = [p for p in new_players_data if len(p) == 8]
        basic = [p for p in new_players_data if len(p) == 4]
        
        if detailed:
            bulk_writer.bulk_upsert(
                'players',
                ['player_id', 'name', 'team_id', 'jersey_number', 'weight_lbs', 
                 'height_inches', 'pre_nba_team', 'birthdate'],
                detailed,
                conflict_columns=['player_id'],
                update_columns=['team_id', 'jersey_number', 'weight_lbs', 
                              'height_inches', 'pre_nba_team', 'birthdate']
            )
            players_added += len(detailed)
            track_transaction(len(detailed))  # Track DB writes
        
        if basic:
            bulk_writer.bulk_upsert(
                'players',
                ['player_id', 'name', 'team_id', 'jersey_number'],
                basic,
                conflict_columns=['player_id'],
                update_columns=['team_id', 'jersey_number']
            )
            players_added += len(basic)
            track_transaction(len(basic))  # Track DB writes
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return players_added, players_updated


def update_player_stats(skip_zero_stats=False):
    """
    Update season statistics for all players (Basic Stats from leaguedashplayerstats)
    Uses db_config.DB_COLUMNS for all field mappings - NO HARDCODING!
    
    Args:
        skip_zero_stats: If True, don't add zero-stat records for roster players (backfill mode)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = get_season()
    current_year = get_season_year()
    
    # Get all columns from leaguedashplayerstats endpoint using config
    # Filter to only player_season_stats table columns (excludes 'name' which is players-only)
    basic_cols = get_columns_by_endpoint('leaguedashplayerstats', 'player', table='player_season_stats')
    
    # Get advanced stats columns (these require measure_type_detailed_defense='Advanced')
    advanced_cols = get_columns_by_endpoint('leaguedashplayerstats', 'player', table='player_season_stats', 
                                           measure_type_detailed_defense='Advanced')
    
    # Combine basic and advanced columns for database insertion
    all_cols = {**basic_cols, **advanced_cols}
    
    # Get valid player IDs from database (all players on rosters)
    cursor.execute(f"SELECT player_id, team_id FROM {TABLES[1]}")  # TABLES[1] = 'players'
    all_players = cursor.fetchall()
    valid_player_ids = {row[0] for row in all_players}
    
    # TEST MODE: Filter to only test player
    if is_test_mode():
        test_player_id = get_test_player_id()
        valid_player_ids = {test_player_id} if test_player_id in valid_player_ids else set()
    
    # Process all season types from config
    season_types = [(name, code) for name, code in SEASON_TYPE_MAP.items()]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            log(f"Fetching leaguedashplayerstats - {season_type_name}...")
            
            # Fetch basic stats
            df = retry_api_call(
                lambda: leaguedashplayerstats.LeagueDashPlayerStats(
                    season=current_season,
                    season_type_all_star=season_type_name,
                    per_mode_detailed=API_CONFIG['per_mode_detailed'],
                    timeout=API_CONFIG['timeout_bulk']
                ).get_data_frames()[0],
                f"{season_type_name} basic stats"
            )
            track_transaction(1)  # Track the API call
            
            if df.empty:
                continue
            
            # Fetch advanced stats if we have advanced columns in config
            # Dynamically extract field names from config instead of hardcoding
            adv_field_names = set()
            for col, cfg in advanced_cols.items():
                src = cfg.get('player_source', {})
                # Check if this column needs Advanced measure type
                params = src.get('params', {})
                if params.get('measure_type_detailed_defense') == 'Advanced' and src.get('field'):
                    adv_field_names.add(src['field'])
            
            if adv_field_names:
                try:
                    adv_df = retry_api_call(
                        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed=API_CONFIG['per_mode_detailed'],
                            timeout=API_CONFIG['timeout_bulk']
                        ).get_data_frames()[0],
                        f"{season_type_name} advanced stats"
                    )
                    track_transaction(1)  # Track the API call
                    
                    if not adv_df.empty:
                        # Build merge columns dynamically: ID columns + advanced stat fields
                        merge_cols = ['PLAYER_ID', 'TEAM_ID'] + sorted(list(adv_field_names))
                        df = df.merge(
                            adv_df[merge_cols], 
                            on=['PLAYER_ID', 'TEAM_ID'], 
                            how='left'
                        )
                except Exception as e:
                    log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
            
            # Track which players have stats from API
            players_with_stats = set()
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                player_id = row['PLAYER_ID']
                
                # Skip if not in our database
                if player_id not in valid_player_ids:
                    continue
                
                players_with_stats.add(player_id)
                
                # Build record using config - start with fixed fields
                # Note: team_id removed from player_season_stats (now in players table)
                record_values = [
                    player_id,
                    current_season,  # Use season string ('2025-26'), not year integer (2026)
                    season_type_code,
                ]
                
                # Add stats from config in sorted order for consistency
                for col_name in sorted(all_cols.keys()):
                    col_config = all_cols[col_name]
                    
                    # Extract value using config-driven source and transform
                    # Config structure has player_source at top level, not inside data_source
                    source = col_config.get('player_source')
                    if not source or not isinstance(source, dict):
                        record_values.append(0)
                        continue
                    
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Handle calculated fields (e.g., "FGM - 3fgM")
                    if any(op in field_name for op in ['+', '-', '*', '/']):
                        field_name = field_name.strip()
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
                        # Get raw value and apply transform
                        raw_value = row.get(field_name, 0)
                        transform_name = source.get('transform', 'safe_int')
                        scale = source.get('scale', 1)
                        
                        if transform_name == 'safe_int':
                            value = safe_int(raw_value, scale=scale)
                        elif transform_name == 'safe_float':
                            value = safe_float(raw_value, scale=scale)
                        elif transform_name == 'safe_str':
                            value = safe_str(raw_value)
                        elif transform_name == 'parse_height':
                            value = parse_height(raw_value)
                        elif transform_name == 'parse_birthdate':
                            value = parse_birthdate(raw_value)
                        else:
                            value = safe_int(raw_value, scale=scale)
                    
                    record_values.append(value)
                
                records.append(tuple(record_values))
            
            # Add zero-stat records for players on rosters who didn't play (Regular Season only)
            if season_type_code == 1 and not skip_zero_stats:
                players_without_stats = valid_player_ids - players_with_stats
                if players_without_stats:
                    for player_id in players_without_stats:
                        team_id = next((t for p, t in all_players if p == player_id), None)
                        if not team_id:
                            continue
                        
                        # Build zero-stat record using config (no team_id - removed in multi-table refactor)
                        zero_values = [player_id, current_season, season_type_code]
                        
                        # Add zeros for all stats (sorted order matches record_values above)
                        for col_name in sorted(all_cols.keys()):
                            col_config = all_cols[col_name]
                            # Use default value from config, or 0 if not specified
                            default_val = col_config.get('default', 0)
                            zero_values.append(default_val)
                        
                        records.append(tuple(zero_values))
            
            # Bulk insert using config-driven column names
            if records:
                # Build column list from config (sorted to match record order)
                # Quote column names that start with numbers (2fgm, 2fga, 3fgm, 3fga)
                db_columns = ['player_id', 'year', 'season_type'] + sorted(all_cols.keys())
                columns_str = ', '.join(quote_column(col) for col in db_columns)
                
                # Build UPDATE SET clause from config (exclude keys)
                update_clauses = [
                    f"{quote_column(col)} = EXCLUDED.{quote_column(col)}" for col in sorted(all_cols.keys())
                ]
                update_str = ',\n                        '.join(update_clauses)
                
                # Execute bulk insert
                sql = f"""
                    INSERT INTO player_season_stats (
                        {columns_str}
                    ) VALUES %s
                    ON CONFLICT (player_id, year, season_type) DO UPDATE SET
                        {update_str},
                        updated_at = NOW()
                """
                
                execute_values(
                    cursor,
                    sql,
                    records
                )
                conn.commit()
                total_updated += len(records)
        
        except Exception as e:
            log(f"❌ ERROR - Error fetching {season_type_name} stats: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    return True


def update_team_stats():
    """
    Update season statistics for all teams (leaguedashteamstats + opponent stats)
    Uses db_config.DB_COLUMNS for all field mappings - NO HARDCODING!
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = get_season()
    current_year = get_season_year()
    
    # Get valid team IDs from config (numeric IDs, not abbreviations)
    valid_team_ids = set(TEAM_IDS.values())
    
    # TEST MODE: Filter to only test team
    if is_test_mode():
        test_team_id = get_test_team_id()
        valid_team_ids = {test_team_id} if test_team_id in valid_team_ids else set()
    
    # Get team stats columns from config (basic + advanced)
    basic_cols = get_columns_by_endpoint('leaguedashteamstats', 'team')
    advanced_cols = get_columns_by_endpoint('leaguedashteamstats', 'team', measure_type_detailed_defense='Advanced')
    team_cols = {**basic_cols, **advanced_cols}  # Merge both sets of columns
    opp_cols = get_columns_by_entity('opponent')
    
    # Process all season types from config
    season_types = [(name, code) for name, code in SEASON_TYPE_MAP.items()]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            log(f"Fetching leaguedashteamstats - {season_type_name}...")
            
            # Fetch basic stats
            df = retry_api_call(
                lambda: leaguedashteamstats.LeagueDashTeamStats(
                    season=current_season,
                    season_type_all_star=season_type_name,
                    per_mode_detailed=API_CONFIG['per_mode_detailed'],
                    timeout=API_CONFIG['timeout_bulk']
                ).get_data_frames()[0],
                f"{season_type_name} team stats"
            )
            track_transaction(1)  # Track the API call
            
            if df.empty:
                continue
            
            # Fetch advanced stats
            # Dynamically extract field names from config instead of hardcoding
            adv_field_names = set()
            for col, cfg in advanced_cols.items():
                # Get team_source directly (not from data_source)
                src = cfg.get('team_source', {})
                # Get the field name from team_source if it has measure_type_detailed_defense='Advanced'
                # Check both direct field and params dictionary for consistency
                params = src.get('params', {})
                has_advanced = (src.get('measure_type_detailed_defense') == 'Advanced' or 
                               params.get('measure_type_detailed_defense') == 'Advanced')
                if has_advanced and src.get('field'):
                    adv_field_names.add(src['field'])
            
            if adv_field_names:
                try:
                    adv_df = retry_api_call(
                        lambda: leaguedashteamstats.LeagueDashTeamStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed=API_CONFIG['per_mode_detailed'],
                            timeout=API_CONFIG['timeout_bulk']
                        ).get_data_frames()[0],
                        f"{season_type_name} team advanced stats"
                    )
                    track_transaction(1)  # Track the API call
                    
                    if not adv_df.empty:
                        # Build merge columns dynamically: ID column + advanced stat fields
                        merge_cols = ['TEAM_ID'] + sorted(list(adv_field_names))
                        df = df.merge(
                            adv_df[merge_cols], 
                            on='TEAM_ID',
                            how='left'
                        )
                except Exception as e:
                    log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
            
            # Fetch opponent stats (what opponents did against each team)
            try:
                max_retries = RETRY_CONFIG['max_retries']
                for attempt in range(max_retries):
                    try:
                        opp_stats = leaguedashteamstats.LeagueDashTeamStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Opponent',
                            per_mode_detailed=API_CONFIG['per_mode_detailed'],
                            timeout=API_CONFIG['timeout_bulk']
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        opp_df = opp_stats.get_data_frames()[0]
                        track_transaction(1)  # Track the API call
                        break
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            wait_time = RETRY_CONFIG['backoff_base'] * (attempt + 1)
                            log(f"Attempt {attempt + 1}/{max_retries} failed for {season_type_name} team opponent stats, retrying in {wait_time}s...", "WARN")
                            time.sleep(wait_time)
                        else:
                            raise retry_error
                
                if not opp_df.empty:
                    # Merge opponent stats - they come with OPP_ prefix from API
                    df = df.merge(opp_df, on='TEAM_ID', how='left', suffixes=('', '_OPP'))
            except Exception as e:
                log(f"Warning: Could not fetch opponent stats: {e}", "WARN")
            
            # Remove duplicates (some seasons return duplicate team entries)
            df = df.drop_duplicates(subset=['TEAM_ID'], keep='first')
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                team_id = row['TEAM_ID']
                
                # Skip if not valid team
                if team_id not in valid_team_ids:
                    continue
                
                # Build record using config - start with fixed fields
                record_values = [
                    team_id,
                    current_year,
                    season_type_code,
                ]
                
                # Add team stats from config (sorted for consistency)
                for col_name in sorted(team_cols.keys()):
                    col_config = team_cols[col_name]
                    
                    # Extract value using config-driven source and transform
                    # Config structure has team_source at top level, not inside data_source
                    source = col_config.get('team_source')
                    if not source or not isinstance(source, dict):
                        record_values.append(0)
                        continue
                    
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Handle calculated fields (e.g., "FGM - 3fgM")
                    if any(op in field_name for op in ['+', '-', '*', '/']):
                        field_name = field_name.strip()
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
                        # Get raw value and apply transform
                        raw_value = row.get(field_name, 0)
                        transform_name = source.get('transform', 'safe_int')
                        scale = source.get('scale', 1)
                        
                        if transform_name == 'safe_int':
                            value = safe_int(raw_value, scale=scale)
                        elif transform_name == 'safe_float':
                            value = safe_float(raw_value, scale=scale)
                        elif transform_name == 'safe_str':
                            value = safe_str(raw_value)
                        elif transform_name == 'parse_height':
                            value = parse_height(raw_value)
                        elif transform_name == 'parse_birthdate':
                            value = parse_birthdate(raw_value)
                        else:
                            value = safe_int(raw_value, scale=scale)
                    
                    record_values.append(value)
                
                # Add opponent stats from config (sorted for consistency)
                for col_name in sorted(opp_cols.keys()):
                    col_config = opp_cols[col_name]
                    
                    # Extract value using config-driven source and transform
                    # Config structure has opponent_source at top level, not inside data_source
                    source = col_config.get('opponent_source')
                    if not source or not isinstance(source, dict):
                        record_values.append(0)
                        continue
                    
                    # Opponent stats come with OPP_ prefix from API
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Map to OPP_ prefixed version for opponent endpoint
                    opp_field = f'OPP_{field_name}' if not field_name.startswith('OPP_') else field_name
                    
                    # Handle calculated fields (e.g., "FGM - 3fgM")
                    if any(op in opp_field for op in ['+', '-', '*', '/']):
                        opp_field = opp_field.strip()
                        if ' - ' in opp_field:
                            left, right = opp_field.split(' - ')
                            left_val = safe_int(row.get(left.strip(), 0))
                            right_val = safe_int(row.get(right.strip(), 0))
                            value = max(0, left_val - right_val)
                        elif ' + ' in opp_field:
                            left, right = opp_field.split(' + ')
                            left_val = safe_int(row.get(left.strip(), 0))
                            right_val = safe_int(row.get(right.strip(), 0))
                            value = left_val + right_val
                        else:
                            value = 0
                    else:
                        # Get raw value and apply transform
                        raw_value = row.get(opp_field, 0)
                        transform_name = source.get('transform', 'safe_int')
                        scale = source.get('scale', 1)
                        
                        if transform_name == 'safe_int':
                            value = safe_int(raw_value, scale=scale)
                        elif transform_name == 'safe_float':
                            value = safe_float(raw_value, scale=scale)
                        elif transform_name == 'safe_str':
                            value = safe_str(raw_value)
                        elif transform_name == 'parse_height':
                            value = parse_height(raw_value)
                        elif transform_name == 'parse_birthdate':
                            value = parse_birthdate(raw_value)
                        else:
                            value = safe_int(raw_value, scale=scale)
                    
                    record_values.append(value)
                
                records.append(tuple(record_values))
            
            # Bulk insert using config-driven column names
            if records:
                # Build column list from config (sorted to match record order)
                # Opponent columns need opp_ prefix in database
                # Quote column names that start with numbers
                team_stat_cols = sorted(team_cols.keys())
                opp_stat_cols = [f'opp_{col}' for col in sorted(opp_cols.keys())]
                all_stat_cols = team_stat_cols + opp_stat_cols
                db_columns = ['team_id', 'year', 'season_type'] + all_stat_cols
                columns_str = ', '.join(quote_column(col) for col in db_columns)
                
                # Build UPDATE SET clause from config (exclude keys)
                update_clauses = [f"{quote_column(col)} = EXCLUDED.{quote_column(col)}" for col in all_stat_cols]
                update_str = ',\n                        '.join(update_clauses)
                
                # Execute bulk insert
                sql = f"""
                    INSERT INTO team_season_stats (
                        {columns_str}
                    ) VALUES %s
                    ON CONFLICT (team_id, year, season_type) DO UPDATE SET
                        {update_str},
                        updated_at = NOW()
                """
                
                execute_values(
                    cursor,
                    sql,
                    records
                )
                conn.commit()
                total_updated += len(records)
        
        except Exception as e:
            log(f"❌ ERROR - Error fetching {season_type_name} stats: {e}", "ERROR")
    
    # Process per-team endpoints (e.g., teamdashptshots requires team_id parameter)
    # Discover which endpoints need per-team execution from config
    per_team_endpoints = {}
    for col_name, col_config in DB_COLUMNS.items():
        if col_config.get('table') not in ['team_season_stats', 'stats']:
            continue
        
        team_source = col_config.get('team_source', {})
        if not isinstance(team_source, dict):
            continue
            
        endpoint = team_source.get('endpoint')
        exec_tier = team_source.get('execution_tier')
        
        if exec_tier == 'team' and endpoint:
            # Get params if specified
            params = team_source.get('params', {})
            key = (endpoint, tuple(sorted(params.items())))
            if key not in per_team_endpoints:
                per_team_endpoints[key] = {'endpoint': endpoint, 'params': params}
    
    # Execute each per-team endpoint for each season type
    for (endpoint_name, _), endpoint_info in per_team_endpoints.items():
        for season_type_name, season_type_code in season_types:
            try:
                updated = execute_generic_endpoint(
                    endpoint_name=endpoint_name,
                    endpoint_params=endpoint_info['params'],
                    season=current_season,
                    entity='team',
                    table='team_season_stats',
                    season_type=season_type_code,
                    season_type_name=season_type_name,
                    description=f"{endpoint_name}"
                )
                total_updated += updated if updated else 0
            except Exception as e:
                log(f"❌ ERROR - Error processing {endpoint_name}: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    return True


def update_transformation_columns(season, entity='player', table='player_season_stats', season_type=1, season_type_name='Regular Season'):
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
        log(f"  No transformations configured for {entity}")
        return 0
    
    total_updated = 0
    
    # Execute grouped transformations (ONE API call per group!)
    for group_name, group_transforms in applicable_groups:
        try:
            # Extract column names and get endpoint info from first transform
            if not group_transforms:
                log(f"  Skipping empty group '{group_name}'")
                continue
            
            group_columns = [col_name for col_name, _ in group_transforms]
            first_col_name, first_transform = group_transforms[0]
            
            # Get source config to extract endpoint and execution_tier
            source_key = f'{entity}_source'
            first_source = DB_COLUMNS[first_col_name][source_key]
            # Endpoint can be at source level OR inside transformation dict
            endpoint_name = first_transform.get('endpoint') or first_source.get('endpoint')
            execution_tier = first_source.get('execution_tier') or first_transform.get('execution_tier', 'player')
            
            # Handle team-tier endpoints differently
            if execution_tier == 'team':
                
                # Check transformation type for this group
                transform_type = first_transform.get('type', 'simple_extract')
                needs_aggregation = (transform_type == 'team_aggregate')
                
                # Extract ALL column values by calling endpoint per team
                all_column_data = {col: {} for col in group_columns}
                EndpointClass = _get_endpoint_class(endpoint_name)
                
                # Use configurable season type parameter name (defaults to season_type_all_star)
                season_type_param = first_transform.get('season_type_param', 'season_type_all_star')
                endpoint_params = {'season': season, 'timeout': API_CONFIG['timeout_default']}
                # Load config params first (may contain defaults/hardcoded values)
                if 'endpoint_params' in first_transform:
                    endpoint_params.update(first_transform['endpoint_params'])
                # CRITICAL: Override season type with runtime value (fixes Issue #2 - repeating values)
                endpoint_params[season_type_param] = season_type_name
                
                # Get team IDs - filter for test mode if applicable
                team_ids = list(TEAM_IDS.values())
                team_ids = filter_teams_for_test_mode(team_ids)
                
                # Loop through teams
                for team_id in team_ids:
                    if _rate_limiter:
                        _rate_limiter.acquire()
                    
                    try:
                        params = {**endpoint_params, 'team_id': team_id}
                        result = EndpointClass(**params).get_dict()
                        
                        # Extract ALL columns from this result
                        for col_name in group_columns:
                            # Get transformation from DB_COLUMNS
                            source_key = f'{entity}_source'
                            transform = DB_COLUMNS[col_name][source_key]['transformation']
                            result_set_name = transform['result_set']
                            field_name = transform['field']
                            
                            for rs in result['resultSets']:
                                if rs['name'] == result_set_name:
                                    headers = rs['headers']
                                    if field_name not in headers:
                                        continue
                                    
                                    field_idx = headers.index(field_name)
                                    
                                    # Use custom player_id_field if specified, otherwise default to PLAYER_ID
                                    player_id_field = transform.get('player_id_field', 'PLAYER_ID')
                                    player_id_idx = headers.index(player_id_field) if player_id_field in headers else 0
                                    
                                    for row in rs['rowSet']:
                                        player_id = row[player_id_idx]
                                        
                                        # Filter out non-numeric player IDs (e.g., "On/Off Court" summary rows)
                                        try:
                                            player_id = int(player_id)
                                        except (ValueError, TypeError):
                                            continue
                                        
                                        field_value = row[field_idx] or 0
                                        
                                        # Apply transform if specified
                                        if transform.get('transform') == 'safe_int':
                                            field_value = safe_int(field_value, transform.get('scale', 1))
                                        elif transform.get('transform') == 'safe_float':
                                            field_value = safe_float(field_value, transform.get('scale', 1))
                                        else:
                                            # Convert to int for aggregation
                                            try:
                                                field_value = int(field_value)
                                            except (ValueError, TypeError):
                                                field_value = 0
                                        
                                        # If aggregation needed, SUM across teams; otherwise, replace
                                        if needs_aggregation:
                                            if player_id in all_column_data[col_name]:
                                                all_column_data[col_name][player_id] += field_value
                                            else:
                                                all_column_data[col_name][player_id] = field_value
                                        else:
                                            all_column_data[col_name][player_id] = field_value
                                    break
                        
                        track_transaction(1)
                        
                    except Exception as e:
                        log(f"⚠ Failed team {team_id}: {e}", "WARNING")
                        continue
                
                # Update database for ALL columns at once
                for col_name, data in all_column_data.items():
                    updated = 0
                    for player_id, value in data.items():
                        cursor.execute(f"""
                            UPDATE {table}
                            SET {quote_column(col_name)} = %s
                            WHERE player_id = %s AND year = %s AND season_type = %s
                        """, (value, player_id, season, season_type))
                        updated += cursor.rowcount
                    
                    conn.commit()
                    total_updated += updated
                
                continue
            
            # Handle player-tier endpoints with subprocess execution
            log(f"Fetching {endpoint_name} - {season_type_name}...")            
            # Execute subprocess ONCE for entire group
            EndpointClass = _get_endpoint_class(endpoint_name)
            
            # Get all players for this season type
            player_ids = get_player_ids_for_season(season, season_type)
            
            # TEST MODE: Filter to only test player
            player_ids = filter_players_for_test_mode(player_ids)
            
            if not player_ids:
                log(f"  No players to process (no data)")
                continue
            
            # Build endpoint parameters from config
            # Use configurable season type parameter name (defaults to season_type_all_star)
            season_type_param = first_transform.get('season_type_param', 'season_type_all_star')
            endpoint_params = {'season': season, 'timeout': API_CONFIG['timeout_default']}
            
            # Add group-specific parameters from first transform (all should have same params)
            if 'endpoint_params' in first_transform:
                endpoint_params.update(first_transform['endpoint_params'])
            
            # CRITICAL: Override season type with runtime value (fixes Issue #2 - repeating values)
            endpoint_params[season_type_param] = season_type_name
            
            # ONE subprocess call for ALL columns in this group
            results = execute_player_endpoint_in_subprocesses(
                endpoint_module=f'nba_api.stats.endpoints.{endpoint_name.lower()}',
                endpoint_class=EndpointClass.__name__,
                player_ids=player_ids,
                endpoint_params=endpoint_params,
                description=f'Group: {group_name} ({len(group_columns)} columns)'
            )
            
            # Extract ALL column values from the SINGLE result set
            all_column_data = {col: {} for col in group_columns}
            
            for success in results['successes']:
                player_id = success['player_id']
                result = success['data']
                
                # Process each column in the group
                for col_name in group_columns:
                    # Get transformation from DB_COLUMNS
                    source_key = f'{entity}_source'
                    transform = DB_COLUMNS[col_name][source_key]['transformation']
                    value = _extract_value_from_result(result, transform)
                    all_column_data[col_name][player_id] = value
                
                track_transaction(1)
            
            # Handle failures
            for failure in results['failures']:
                for col_name in group_columns:
                    all_column_data[col_name][failure['player_id']] = 0
                track_transaction(1)
            
            # Update database for ALL columns at once
            for col_name, data in all_column_data.items():
                updated = 0
                for player_id, value in data.items():
                    cursor.execute(f"""
                        UPDATE {table}
                        SET {quote_column(col_name)} = %s, updated_at = NOW()
                        WHERE player_id = %s AND year = %s AND season_type = %s
                    """, (value, player_id, season, season_type))
                    if cursor.rowcount > 0:
                        updated += 1
                
                conn.commit()
                total_updated += updated
            
        except Exception as e:
            import traceback
            endpoint_name = group_transforms[0][1].get('endpoint', group_name) if group_transforms else group_name
            log(f"  Failed endpoint '{endpoint_name}': {e}", "ERROR")
            log(f"  Group had {len(group_transforms) if 'group_transforms' in locals() else 'unknown'} transforms", "ERROR")
            log(f"  Traceback: {traceback.format_exc()}", "ERROR")
            conn.rollback()
    
    # Execute ungrouped transformations (original behavior)
    for col_name in ungrouped_transforms:
        try:
            # Get transformation from DB_COLUMNS
            source_key = f'{entity}_source'
            source_config = DB_COLUMNS[col_name][source_key]
            transform = source_config['transformation']
            
            # Double-check this isn't a grouped transformation (safety check)
            if transform.get('group'):
                log(f"  Skipping {col_name} - has group '{transform.get('group')}' but was in ungrouped list", "WARNING")
                continue
            
            data = apply_transformation(col_name, transform, season, entity, table, season_type, season_type_name, source_config)
            
            updated = 0
            for entity_id, value in data.items():
                if entity == 'player':
                    cursor.execute(f"""
                        UPDATE {table}
                        SET {quote_column(col_name)} = %s, updated_at = NOW()
                        WHERE player_id = %s AND year = %s::text AND season_type = %s
                    """, (value, entity_id, season_year, season_type))
                else:  # team
                    cursor.execute(f"""
                        UPDATE {table}
                        SET {quote_column(col_name)} = %s, updated_at = NOW()
                        WHERE team_id = %s AND year = %s::text AND season_type = %s
                    """, (value, entity_id, season_year, season_type))
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            total_updated += updated
            
        except Exception as e:
            log(f"  Failed {col_name}: {e}", "ERROR")
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
                log(f"  Converted NULLs to 0 for {nulls_fixed} {entity} records with minutes > 0")
            conn.commit()
            
    except Exception as e:
        log(f"  Failed NULL cleanup: {e}", "ERROR")
        conn.rollback()
    
    cursor.close()
    conn.close()
    
    return total_updated


def _extract_value_from_result(result, transform):
    """Extract a single value from API result based on transformation config."""
    transform_type = transform['type']
    
    if transform_type == 'simple_extract':
        result_set_name = transform['result_set']
        filter_spec = transform.get('filter', {})
        field_name = transform['field']
        
        for rs in result['resultSets']:
            if rs['name'] == result_set_name:
                headers = rs['headers']
                for row in rs['rowSet']:
                    row_dict = dict(zip(headers, row))
                    # Check filter
                    if all(row_dict.get(k) == v for k, v in filter_spec.items()):
                        return row_dict.get(field_name, 0)
        return 0
    
    elif transform_type == 'arithmetic_subtract':
        subtract_specs = transform['subtract']
        values = []
        
        for spec in subtract_specs:
            result_set_name = spec['result_set']
            filter_spec = spec.get('filter', {})
            field_name = spec['field']
            
            found = False
            for rs in result['resultSets']:
                if rs['name'] == result_set_name:
                    headers = rs['headers']
                    for row in rs['rowSet']:
                        row_dict = dict(zip(headers, row))
                        if all(row_dict.get(k) == v for k, v in filter_spec.items()):
                            values.append(row_dict.get(field_name, 0))
                            found = True
                            break
                    if found:
                        break
            
            # If no matching row found, append 0
            if not found:
                values.append(0)
        
        # Check if custom formula specified
        formula = transform.get('formula')
        if formula:
            # Support formulas like '(a + b) - (c + d)'
            if formula == '(a + b) - (c + d)' and len(values) >= 4:
                return max(0, (values[0] + values[1]) - (values[2] + values[3]))
            # Can add more formula patterns here as needed
        
        # Default: Subtract first - second (with safety check)
        if len(values) < 2:
            return 0
        return max(0, values[0] - values[1])
    
    elif transform_type == 'filter_aggregate':
        result_set_name = transform['result_set']
        filter_field = transform['filter_field']
        filter_values = transform['filter_values']
        agg_field = transform['field']
        
        total = 0
        for rs in result['resultSets']:
            if rs['name'] == result_set_name:
                headers = rs['headers']
                for row in rs['rowSet']:
                    row_dict = dict(zip(headers, row))
                    if row_dict.get(filter_field) in filter_values:
                        total += row_dict.get(agg_field, 0)
        return total
    
    return 0


def update_team_advanced_stats(season=None):
    """
    100% CONFIG-DRIVEN team advanced stats - automatically discovers ALL endpoints from DB_COLUMNS.
    NO HARDCODING - adding new team stats only requires updating db_config.py.
    """
    if season is None:
        season = get_season()
    
    season_year = int('20' + season.split('-')[1])
    
    if season_year < 2013:
        log("SKIP - Team tracking data not available before 2013-14 season")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Loop through all season types (from config)
        for season_type_name, season_type_code in SEASON_TYPE_MAP.items():
            
            # DISCOVER ALL TEAM ENDPOINTS FROM CONFIG (100% dynamic, no hardcoding!)
            # Group by (endpoint, params) to deduplicate API calls
            endpoint_calls = {}  # {(endpoint, params_tuple): {param_dict, description}}
            
            for col_name, col_config in DB_COLUMNS.items():
                team_source = col_config.get('team_source')
                if not team_source or not isinstance(team_source, dict):
                    continue
                
                endpoint_name = team_source.get('endpoint')
                if not endpoint_name:
                    continue
                
                # Skip endpoints already handled by update_team_stats()
                if endpoint_name == 'leaguedashteamstats':
                    continue
                
                # Skip annual fields (handled by annual ETL)
                if col_config.get('update_frequency') == 'annual':
                    continue
                
                # Skip transformation columns (handled by update_transformation_columns)
                if 'transformation' in team_source:
                    continue
                
                # Get params from config (new consistent structure)
                params = team_source.get('params', {})
                
                # Generate description
                pt_measure = params.get('pt_measure_type')
                measure_detailed = params.get('measure_type_detailed_defense')
                defense_cat = params.get('defense_category')
                
                if pt_measure:
                    desc = f"{pt_measure}"
                elif measure_detailed:
                    desc = f"{endpoint_name} ({measure_detailed})"
                elif defense_cat:
                    desc = f"{endpoint_name} ({defense_cat})"
                else:
                    desc = f"{endpoint_name}"
                
                # Create unique key for this endpoint+params combination
                params_tuple = tuple(sorted(params.items()))
                call_key = (endpoint_name, params_tuple)
                
                if call_key not in endpoint_calls:
                    endpoint_calls[call_key] = {'params': params, 'description': desc}
            
            # Execute all discovered endpoints
            for (endpoint_name, params_tuple), call_info in sorted(endpoint_calls.items()):
                execute_generic_endpoint(
                    endpoint_name=endpoint_name,
                    endpoint_params=call_info['params'],
                    season=season,
                    entity='team',
                    table='team_season_stats',
                    season_type=season_type_code,
                    season_type_name=season_type_name,
                    description=f"{call_info['description']} - {season_type_name}"
                )
            
            # TRANSFORMATIONS - All complex stats requiring post-processing
            # 100% config-driven from TRANSFORMATIONS dict
            update_transformation_columns(season, entity='team', table='team_season_stats',
                                         season_type=season_type_code, season_type_name=season_type_name)
        
    except Exception as e:
        log(f"Failed team advanced stats: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()

def update_player_advanced_stats(season=None):
    """
    FULLY CONFIG-DRIVEN ADVANCED STATS ETL - uses execute_generic_endpoint().
    Automatically discovers which endpoints to call from DB_COLUMNS.
    No hardcoding - adding new stats only requires config updates.
    
    Total time: ~8-10 minutes per season
    """
    if season is None:
        season = get_season()
    
    season_year = int('20' + season.split('-')[1])
    
    # Skip if before 2013-14 (tracking data not available)
    if season_year < 2013:
        log("SKIP - Tracking data not available before 2013-14 season")
        return
    
    start_time = time.time()
    
    try:
        # PHASE 1: Discover and execute league-wide endpoints using generic executor
        
        # Group endpoints by (endpoint_name, params) for deduplication
        endpoint_calls = {}
        
        for col_name, col_config in DB_COLUMNS.items():
            # Get player_source directly (not from )
            player_source = col_config.get('player_source', {})
            
            # Skip if not a dict (some fields have string sources)
            if not isinstance(player_source, dict):
                continue
            
            endpoint = player_source.get('endpoint')
            
            # Skip if no endpoint
            if not endpoint:
                continue
            
            # Skip basic stats endpoint (handled in update_player_stats)
            if endpoint == 'leaguedashplayerstats':
                continue
            
            # Skip if not API
            if not col_config.get('api', False):
                continue
            
            # Skip annual fields (handled by annual ETL)
            if col_config.get('update_frequency') == 'annual':
                continue
            
            # Skip endpoints with transformations (handled by update_transformation_columns)
            if player_source.get('transformation'):
                continue
            
            # CRITICAL: Skip per-player endpoints (handled by transformations in PHASE 2)
            # Per-player endpoints require player_id parameter and must be run in subprocesses
            tier = infer_execution_tier_from_endpoint(endpoint)
            if tier == 'player':
                continue  # Will be handled by transformations
            
            # Get params from config (new consistent structure)
            params = player_source.get('params', {})
            
            # Create unique key for this endpoint call
            params_tuple = tuple(sorted(params.items()))
            key = (endpoint, params_tuple)
            
            if key not in endpoint_calls:
                endpoint_calls[key] = params
        
        total_updated = 0
        
        # Loop through all season types (from config)
        for season_type_name, season_type_code in SEASON_TYPE_MAP.items():
            
            # Execute all discovered endpoints
            for (endpoint_name, params_tuple), params in sorted(endpoint_calls.items()):
                # Generate description from params
                pt_measure = params.get('pt_measure_type')
                measure_detailed = params.get('measure_type_detailed_defense')
                defense_cat = params.get('defense_category')
                
                if pt_measure:
                    description = f"{endpoint_name}.{pt_measure} - {season_type_name}"
                elif measure_detailed:
                    description = f"{endpoint_name}.{measure_detailed} - {season_type_name}"
                elif defense_cat:
                    description = f"{endpoint_name}.{defense_cat} - {season_type_name}"
                else:
                    description = f"{endpoint_name} - {season_type_name}"
                
                # Use generic executor - handles everything!
                updated = execute_generic_endpoint(
                    endpoint_name=endpoint_name,
                    endpoint_params=params,
                    season=season,
                    entity='player',
                    table='player_season_stats',
                    season_type=season_type_code,
                    season_type_name=season_type_name,
                    description=description
                )
                total_updated += updated
            
            # PHASE 2: Apply all configured transformations (formerly specialized functions)
            # This replaces: update_shooting_tracking_bulk, update_putbacks_per_player, update_onoff_stats
            # 100% config-driven from TRANSFORMATIONS dict
            update_transformation_columns(season, entity='player', table='player_season_stats', 
                                         season_type=season_type_code, season_type_name=season_type_name)
        
        elapsed = time.time() - start_time
        
    except Exception as e:
        elapsed = time.time() - start_time
        log(f"Advanced stats failed after {elapsed:.1f}s: {e}", "ERROR")
        raise


def run_nightly_etl(backfill_start=None, backfill_end=None, check_missing=True):
    """
    Main daily ETL orchestrator.
    Now includes advanced stats (~10 minutes total).
    
    Args:
        backfill_start: Start year for historical backfill (None = no backfill)
        backfill_end: End year for backfill (None = current season)
        check_missing: Check for missing data after update
    """
    log("=" * 70)
    if backfill_start:
        log(f"THE GLASS - ETL BACKFILL {backfill_start}-{backfill_end or NBA_CONFIG['current_season_year']}")
    else:
        log("THE GLASS - DAILY ETL STARTED")
    log("=" * 70)
    start_time = time.time()
    
    global _transaction_tracker
    
    # If backfill requested, process multiple seasons
    if backfill_start:
        current_year = NBA_CONFIG['current_season_year']
        end_year = backfill_end or current_year
        num_seasons = end_year - backfill_start + 1
        
        log(f"Backfill: Processing {num_seasons} seasons from {backfill_start} to {end_year}")
        
        # Create transaction tracker for backfill
        _transaction_tracker = TransactionTracker(description="Backfill")
        
        for year in range(backfill_start, end_year + 1):
            season = f"{year-1}-{str(year)[-2:]}"
            log("="*70)
            log(f"Processing season {year - backfill_start + 1}/{num_seasons}: {season} (year={year})")
            log("="*70)
            
            try:
                # Temporarily override current season config for this backfill iteration
                original_season = NBA_CONFIG['current_season']
                original_year = NBA_CONFIG['current_season_year']
                NBA_CONFIG['current_season'] = season
                NBA_CONFIG['current_season_year'] = year
                
                # STEP 1: Player Stats (reuse existing function)
                # Skip adding zero-stat records in backfill - only update players who played
                update_player_stats(skip_zero_stats=True)
                
                # STEP 2: Team Stats (reuse existing function)
                update_team_stats()
                
                # STEP 3: Advanced stats (only for 2013-14 onwards)
                if year >= 2014:
                    try:
                        update_player_advanced_stats(season, year)
                        update_team_advanced_stats(season, year)
                    except Exception as e:
                        log(f"    Failed advanced stats: {e}", "WARN")
                else:
                    log("Skipping advanced stats (pre-2013-14)")
                
                # Restore original config
                NBA_CONFIG['current_season'] = original_season
                NBA_CONFIG['current_season_year'] = original_year
                
                log("Season {} complete".format(season))
                
            except Exception as e:
                log(f"Failed to process season {season}: {e}", "ERROR")
                import traceback
                log(traceback.format_exc(), "ERROR")
                # Restore config even on error
                NBA_CONFIG['current_season'] = original_season
                NBA_CONFIG['current_season_year'] = original_year
                continue
        
        # Close progress bars
        _transaction_tracker.close()
        _transaction_tracker = None
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"BACKFILL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        return
    
    # Normal daily ETL (current season only)
    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()
        
        
        # STEP 5: Team advanced stats
        # Multi-call aggregates: 8 shooting zone endpoints
        # Other team endpoints: 4 calls
        # Team putbacks: 30 calls (1 per team)
        
        # Create transaction tracker
        _transaction_tracker = TransactionTracker(description="Daily ETL")
        
        # STEP 1: Player Rosters
        players_added, players_updated = update_player_rosters()
        
        # STEP 2: Player Stats
        update_player_stats()
        
        # STEP 3: Team Stats
        update_team_stats()
        
        # STEP 4: Player Advanced Stats
        
        update_player_advanced_stats()
        
        # STEP 5: Team Advanced Stats
        update_team_advanced_stats()
        
        # Close progress bars
        _transaction_tracker.close()
        _transaction_tracker = None
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"DAILY ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"DAILY ETL FAILED - {elapsed:.1f}s", "ERROR")
        log(f"Error: {e}", "ERROR")
        log("=" * 70)
        raise


        raise


# ============================================================================
# ANNUAL ETL FUNCTIONS
# ============================================================================
# Config-driven annual maintenance (runs August 1st each year)
# Uses DB_COLUMNS with update_frequency='annual' - NO SEPARATE CONFIG!
# All annual fields (wingspan, height, weight, birthdate) defined in DB_COLUMNS



def cleanup_inactive_players():
    """
    Delete players who have NO RECORD in the last 2 seasons.
    
    This matches the daily ETL logic:
    - Daily ETL creates a player_season_stats record for EVERY player on a roster
      (even if games_played = 0, they still get a record)
    - Annual ETL deletes players with NO RECORD at all in last 2 seasons
    
    This means:
    - Active roster players (even injured) = KEPT (have a record)
    - Historical players with recent games = KEPT (have a record)
    - Truly inactive players = DELETED (no record exists)
    
    NOTE: This count will differ from daily ETL roster count because:
    - Annual ETL: Checks ALL players in database (includes historical/retired players)
    - Daily ETL: Only processes players currently on team rosters (active players)
    """
    log("Cleaning up inactive players...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    current_year = NBA_CONFIG['current_season_year']
    
    # Find players with NO RECORD AT ALL in the last 2 seasons
    cursor.execute("""
        SELECT p.player_id, p.name 
        FROM players p
        WHERE NOT EXISTS (
            SELECT 1 FROM player_season_stats s
            WHERE s.player_id = p.player_id
            AND s.year >= %s
        )
    """, (current_year - 1,))
    
    players_to_delete = cursor.fetchall()
    
    if players_to_delete:
        player_ids_to_delete = tuple(p[0] for p in players_to_delete)
        cursor.execute(f"""
            DELETE FROM {TABLES[1]}  
            WHERE player_id IN %s
        """, (player_ids_to_delete,))
        
        deleted_count = cursor.rowcount
        track_transaction(deleted_count)
    else:
        deleted_count = 0
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return deleted_count


def update_annual_fields():
    """
    Config-driven annual field updater using DB_COLUMNS.
    Updates all fields with update_frequency='annual' (wingspan, height, weight, birthdate).
    
    This function is DEPRECATED - use update_wingspan_from_combine() and 
    update_all_player_details() directly as they already use DB_COLUMNS config.
    """
    log("update_annual_fields() is deprecated. Use specialized functions instead:", "WARN")
    log("  - update_wingspan_from_combine() for wingspan", "INFO")
    log("  - update_all_player_details() for height/weight/birthdate", "INFO")
    return {}


def update_all_player_details():
    """
    Fetch height, weight, birthdate for ALL players in the database.
    This is the SLOW operation (~16 minutes for 640 players).
    Only runs once per year on August 1st.
    
    NOTE: This updates ALL players in database (historical + current).
    Daily ETL only fetches details for NEW players on rosters.
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all players in database
    cursor.execute(f"SELECT player_id, name FROM {TABLES[1]} ORDER BY player_id")  # TABLES[1] = 'players'
    
    all_players = cursor.fetchall()
    
    total_players = len(all_players)
    
    updated_count = 0
    failed_count = 0
    consecutive_failures = 0
    retry_queue = []  # Players that failed all attempts - retry at end
    
    for idx, (player_id, player_name) in enumerate(all_players):
        # Take regular breaks every 50 players
        if idx > 0 and idx % PROGRESS_CONFIG['batch_check_interval'] == 0:
            consecutive_failures = 0
        
        # If we're seeing failures, take emergency break
        if consecutive_failures >= PROGRESS_CONFIG['consecutive_failure_threshold']:
            log(f"WARNING - Taking {PROGRESS_CONFIG['emergency_break_seconds']}s break after {PROGRESS_CONFIG['consecutive_failure_threshold']} consecutive failures", "WARN")
            time.sleep(PROGRESS_CONFIG['emergency_break_seconds'])
            consecutive_failures = 0
        
        # Try to fetch details with exponential backoff
        for attempt in range(RETRY_CONFIG['max_retries']):
            try:
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=API_CONFIG['timeout_default'])
                info_df = player_info.get_data_frames()[0]
                
                if not info_df.empty:
                    info_row = info_df.iloc[0]
                    
                    # Extract height, weight, birthdate
                    height_str = safe_str(info_row.get('HEIGHT'))
                    weight = safe_int(info_row.get('WEIGHT', 0))
                    birthdate = parse_birthdate(info_row.get('BIRTHDATE'))
                    
                    height_inches = parse_height(height_str)
                    
                    # Update database
                    cursor.execute("""
                        UPDATE players
                        SET height_inches = %s,
                            weight_lbs = %s,
                            birthdate = %s,
                            updated_at = NOW()
                        WHERE player_id = %s
                    """, (height_inches, weight, birthdate, player_id))
                    
                    updated_count += 1
                    consecutive_failures = 0
                    time.sleep(RATE_LIMIT_DELAY)
                break
                
            except Exception:
                consecutive_failures += 1
                if attempt >= RETRY_CONFIG['max_retries'] - 1:
                    failed_count += 1
                    retry_queue.append((player_id, player_name))
        
        track_transaction(1)
    
    # Retry failed players at the end (one more attempt each)
    if retry_queue:
        log(f"\n  Retrying {len(retry_queue)} failed players...")
        for player_id, player_name in retry_queue:
            try:
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=API_CONFIG['timeout_default'])
                info_df = player_info.get_data_frames()[0]
                
                if not info_df.empty:
                    info_row = info_df.iloc[0]
                    
                    height_str = safe_str(info_row.get('HEIGHT'))
                    weight = safe_int(info_row.get('WEIGHT', 0))
                    birthdate = parse_birthdate(info_row.get('BIRTHDATE'))
                    height_inches = parse_height(height_str)
                    
                    cursor.execute("""
                        UPDATE players
                        SET height_inches = %s,
                            weight_lbs = %s,
                            birthdate = %s,
                            updated_at = NOW()
                        WHERE player_id = %s
                    """, (height_inches, weight, birthdate, player_id))
                    
                    updated_count += 1
                    failed_count -= 1  # Remove from failed count
                    log(f"Retry success: {player_name}")
                    
            except Exception as e:
                log(f"  ✗ Retry failed: {player_name} - {str(e)[:100]}")
            
            time.sleep(RATE_LIMIT_DELAY)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"Updated {updated_count}/{total_players} players ({len(retry_queue)} retries)")
    if failed_count > 0:
        log(f"WARNING - Failed to update {failed_count} players after all retries", "WARN")
    
    return updated_count, failed_count


def update_wingspan_from_combine():
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
    cursor.execute(f"SELECT player_id FROM {TABLES[1]} WHERE wingspan_inches IS NULL")  # TABLES[1] = 'players'
    players_needing_wingspan = {row[0] for row in cursor.fetchall()}
    
    if not players_needing_wingspan:
        log("All players already have wingspan data")
        cursor.close()
        conn.close()
        return 0, 0
    
    log(f"Fetching combine wingspan data for {len(players_needing_wingspan)} players...")
    
    # Fetch combine data from all seasons (most recent first to get latest data)
    current_year = NBA_CONFIG['current_season_year']
    start_year = 2002  # Combine data starts around 2000-01
    
    # Store wingspan data: {player_id: (wingspan, season_year)}
    wingspan_data = {}
    
    # Iterate from most recent to oldest (so we keep most recent data)
    for year in range(current_year, start_year - 1, -1):
        season = f"{year}-{str(year + 1)[-2:]}"
        
        try:
            endpoint = DraftCombinePlayerAnthro(season_year=season, timeout=10)
            time.sleep(1.2)  # Rate limit
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
            
            track_transaction(1)
            
        except Exception as e:
            log(f"  Failed to fetch {season}: {str(e)[:50]}", "WARNING")
            continue
    
    # Update database with found wingspan data
    updated_count = 0
    for player_id, (wingspan, year) in wingspan_data.items():
        try:
            # Round to nearest inch
            wingspan_inches = round(wingspan)
            
            cursor.execute("""
                UPDATE players 
                SET wingspan_inches = %s, updated_at = NOW()
                WHERE player_id = %s
            """, (wingspan_inches, player_id))
            
            if cursor.rowcount > 0:
                updated_count += 1
        except Exception as e:
            log(f"  Failed to update player {player_id}: {e}", "WARNING")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"Updated wingspan for {updated_count}/{len(players_needing_wingspan)} players")
    
    return updated_count, len(players_needing_wingspan)


def run_annual_etl():
    """
    Config-driven annual maintenance ETL (runs August 1st each year).
    
    Uses DB_COLUMNS (update_frequency='annual') + ANNUAL_ETL_CONFIG.
    NO HARDCODING - all fields/endpoints/strategies from config!
    
    Steps:
    1. Delete inactive players (no stats in last 2 seasons)
    2. Update all fields marked update_frequency='annual' in DB_COLUMNS
       - Wingspan (from DraftCombinePlayerAnthro)
       - Height, weight, birthdate (from CommonPlayerInfo)
    """
    global _transaction_tracker
    
    log("="*70)
    log("THE GLASS - ANNUAL ETL STARTED")
    log("="*70)
    
    _transaction_tracker = TransactionTracker(description="Annual ETL")
    
    try:
        # Step 1: Cleanup inactive players
        deleted_count = cleanup_inactive_players()
        
        # Step 2: Update all annual fields (config-driven)
        results = update_annual_fields()
        
    except Exception as e:
        log(f"Annual ETL failed: {e}", "ERROR")
        log(traceback.format_exc(), "ERROR")
        raise
    finally:
        if _transaction_tracker:
            _transaction_tracker.close()
            _transaction_tracker = None


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
    parser.add_argument('--test', action='store_true', help='Test mode: run with single player & team')
    
    args = parser.parse_args()
    
    # Set global test mode flag
    if args.test:
        print("\n" + "="*70)
        print(f"TEST MODE: {TEST_MODE_CONFIG['player_name']} and the {TEST_MODE_CONFIG['team_name']}")
        print("="*70 + "\n")
        os.environ['ETL_TEST_MODE'] = '1'
    
    # Route to appropriate ETL mode
    if args.annual:
        # ANNUAL ETL MODE
        
        # If year specified, update NBA_CONFIG for that season
        if args.year:
            NBA_CONFIG['current_season_year'] = args.year
            NBA_CONFIG['current_season'] = f"{args.year-1}-{str(args.year)[-2:]}"
        
        run_annual_etl()
        
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
        
        run_nightly_etl(
            backfill_start=backfill_start,
            backfill_end=backfill_end,
            check_missing=not args.no_check
        )
        
    else:
        # DAILY ETL MODE (default)
        run_nightly_etl(check_missing=not args.no_check)

