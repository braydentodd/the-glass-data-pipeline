"""
THE GLASS - Daily ETL (Runs nightly)
Fast update that handles:
1. Player season statistics (current + last season) - FAST (2 API calls)
2. Team rosters + jersey numbers - FAST (30 API calls, ~30 seconds)
3. Team season statistics (current season) - FAST (6 API calls)
4. New player details (height, weight, birthdate) - RARE (only for new players)
5. Optional: Historical backfill

This runs DAILY. Height/weight/birthdate for existing players updated ANNUALLY on August 1st.

Usage:
    python src/etl.py                          # Run daily update (fast, ~2-3 minutes)
    python src/etl.py --backfill 2020          # Backfill from 2020 to present
    python src/etl.py --backfill 2015 --end 2020  # Backfill 2015-2020 only

OPTIMIZATION STRATEGIES:
1. Parallel API calls with semaphore-based rate limiting (3-5x speedup)
2. Endpoint optimization research and bulk fetching
3. Database bulk inserts with COPY and batching (50-70% faster writes)
"""

import os
import sys
import time
import argparse
import psycopg2
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from datetime import datetime
from psycopg2.extras import execute_values
from tqdm import tqdm
from typing import List, Dict, Callable, Any, Optional
from io import StringIO
from nba_api.stats.endpoints import (
    commonplayerinfo,
    leaguedashplayerstats, leaguedashteamstats,
    leaguedashptstats,
    leaguehustlestatsplayer, leaguehustlestatsteam, 
    leaguedashptdefend, leaguedashptteamdefend,
    playerdashboardbyshootingsplits, teamplayeronoffdetails
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

# Import config modules (works both with -m and direct execution)
try:
    from src.config_database import (
        NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA, SEASON_TYPE_MAP,
        DB_COLUMNS,
        get_columns_by_endpoint,
        get_columns_by_entity,
        get_columns_by_update_frequency,
        safe_int, safe_float, safe_str, parse_height, parse_birthdate
    )
except ImportError:
    from config_database import (
        NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA, SEASON_TYPE_MAP,
        DB_COLUMNS,
        get_columns_by_endpoint,
        get_columns_by_entity,
        get_columns_by_update_frequency,
        safe_int, safe_float, safe_str, parse_height, parse_birthdate
    )


RATE_LIMIT_DELAY = NBA_CONFIG['api_rate_limit_delay']

# Global progress bars (accessed by all ETL functions)
_overall_pbar = None
_group_pbar = None

# Global optimizations (shared across ETL functions)
_rate_limiter = None  # Initialized when needed
_parallel_executor = None  # Initialized when needed


# ============================================================================
# ETL OPTIMIZATION CLASSES
# ============================================================================

class RateLimiter:
    """
    Thread-safe rate limiter using semaphore and sliding window.
    Ensures we never exceed NBA API rate limits even with parallel requests.
    """
    
    def __init__(self, requests_per_second=1.67):
        """
        Args:
            requests_per_second: Default 1.67 = 100 req/min with safety margin
        """
        self.delay = 1.0 / requests_per_second
        self.last_request_time = 0
        self.lock = threading.Lock()
        self.request_times = []  # Sliding window
        self.window_size = 60  # 1 minute window
        
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
    """
    Executes NBA API calls in parallel with intelligent rate limiting.
    
    Features:
    - Thread pool with configurable workers (default 3)
    - Shared rate limiter to prevent 429 errors
    - Retry logic with exponential backoff
    - Progress tracking and error collection
    """
    
    def __init__(self, max_workers=3, rate_limiter=None, log_func=None):
        """
        Args:
            max_workers: Number of parallel threads (3-5 recommended)
            rate_limiter: Shared RateLimiter instance
            log_func: Logging function (uses print if None)
        """
        self.max_workers = max_workers
        self.rate_limiter = rate_limiter or RateLimiter()
        self.log = log_func or print
        self.results = {}
        self.errors = []
        
    def execute_batch(self, tasks: List[Dict[str, Any]], description="Batch", progress_callback=None):
        """
        Execute a batch of API calls in parallel.
        
        Args:
            tasks: List of dicts with 'id', 'func', 'description', 'max_retries'
            description: Overall batch description for logging
            progress_callback: Optional function called after each task completes
            
        Returns:
            Dict mapping task IDs to results
            
        Example:
            tasks = [
                {
                    'id': 1610612737,
                    'func': lambda: leaguedashplayerstats.LeagueDashPlayerStats(...),
                    'description': 'Player stats for Hawks',
                    'max_retries': 3
                },
                ...
            ]
        """
        
        results = {}
        errors = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._execute_with_retry, task): task
                for task in tasks
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                task_id = task['id']
                completed += 1
                
                try:
                    result = future.result()
                    results[task_id] = result
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(1)
                    
                except Exception as e:
                    errors.append({'task_id': task_id, 'error': str(e)})
                    self.log(f"  ❌ Task {task_id} failed: {str(e)[:80]}")
                    
                    # Call progress callback even on failure
                    if progress_callback:
                        progress_callback(1)
        
        if errors:
            self.log(f"Completed with {len(errors)} errors out of {len(tasks)} tasks")
            
        return results, errors
    
    def _execute_with_retry(self, task):
        """Execute a single task with retry logic."""
        func = task['func']
        max_retries = task.get('max_retries', 3)
        timeout = task.get('timeout', 20)
        
        for attempt in range(1, max_retries + 1):
            try:
                # Acquire rate limit token
                self.rate_limiter.acquire()
                
                # Execute the API call
                result = func(timeout)
                return result
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = attempt * 2  # Exponential: 2s, 4s, 6s
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed after {max_retries} attempts: {str(e)}")
        
        raise Exception(f"Task returned None after {max_retries} attempts")


class BulkDatabaseWriter:
    """
    Optimized database writer using PostgreSQL COPY and batch operations.
    
    Features:
    - COPY command for maximum insert speed
    - Automatic batching to prevent memory issues
    - Transaction management with savepoints
    - Conflict resolution (UPSERT)
    """
    
    def __init__(self, conn, batch_size=1000, log_func=None):
        """
        Args:
            conn: psycopg2 connection
            batch_size: Number of rows per batch (default 1000)
            log_func: Logging function
        """
        self.conn = conn
        self.batch_size = batch_size
        self.log = log_func or print
        
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
        cols_str = ', '.join(columns)
        try:
            cursor.copy_from(buffer, table, columns=columns, null='\\N')
            self.conn.commit()
            return len(data)
        except Exception as e:
            self.log(f"  ❌ COPY failed: {str(e)}")
            self.conn.rollback()
            raise


class EndpointOptimizer:
    """
    Research and optimize NBA API endpoint usage.
    
    Features:
    - Detect when league-wide endpoints can replace per-entity calls
    - Cache endpoint response schemas for analysis
    - Suggest optimization opportunities
    """
    
    def __init__(self, log_func=None):
        self.log = log_func or print
        self.endpoint_cache = {}
        
    def analyze_coverage(self, endpoint_name: str, response_df, entity_ids: List[int]):
        """
        Analyze if a league-wide endpoint covers all needed entities.
        
        Args:
            endpoint_name: Name of the endpoint
            response_df: DataFrame response from endpoint
            entity_ids: List of entity IDs we need data for
            
        Returns:
            Dict with coverage analysis
        """
        if response_df is None or response_df.empty:
            return {'coverage': 0, 'missing': entity_ids}
        
        # Detect ID column
        id_col = None
        for col in ['PLAYER_ID', 'TEAM_ID', 'PERSON_ID']:
            if col in response_df.columns:
                id_col = col
                break
        
        if not id_col:
            return {'coverage': 0, 'missing': entity_ids, 'error': 'No ID column found'}
        
        # Check coverage
        returned_ids = set(response_df[id_col].unique())
        needed_ids = set(entity_ids)
        missing_ids = needed_ids - returned_ids
        
        coverage = len(returned_ids & needed_ids) / len(needed_ids) if needed_ids else 0
        
        return {
            'coverage': coverage,
            'missing': list(missing_ids),
            'returned_count': len(returned_ids),
            'needed_count': len(needed_ids),
            'can_replace_per_entity': coverage >= 0.95  # 95% threshold
        }
    
    def suggest_bulk_endpoints(self):
        """
        Suggest NBA API endpoints that can fetch data in bulk.
        Based on analysis of NBA API documentation and testing.
        """
        bulk_opportunities = {
            'player_stats': {
                'current': 'Per-player calls to commonplayerinfo (480 calls)',
                'optimized': 'leaguedashplayerstats returns ALL players (1 call)',
                'savings': '479 API calls',
                'implemented': True
            },
            'team_shooting': {
                'current': 'Per-team calls to teamdashptshots (30 calls)',
                'optimized': 'leaguedashptstats returns ALL teams (6 calls for all zones)',
                'savings': '24 API calls',
                'implemented': True
            },
            'team_defense': {
                'current': 'Per-team defense tracking (30 calls)',
                'optimized': 'leaguedashptteamdefend returns ALL teams (3 calls)',
                'savings': '27 API calls',
                'implemented': True
            },
            'player_shooting': {
                'current': 'Per-player shooting zones (480 calls)',
                'optimized': 'No bulk endpoint available - must use per-player',
                'savings': None,
                'implemented': True,
                'note': 'This is unavoidable - NBA API limitation'
            }
        }
        
        return bulk_opportunities


# ============================================================================
# ETL HELPER FUNCTIONS
# ============================================================================


def retry_api_call(api_func, description, max_retries=3, backoff_base=10, timeout=20, use_timeout_param=False):
    """
    Generic retry wrapper for API calls with exponential backoff.
    
    Args:
        api_func: Function that makes the API call and returns result
        description: Description for logging
        max_retries: Number of retry attempts
        backoff_base: Base wait time in seconds (multiplied by attempt number)
        timeout: Timeout in seconds (default 20) - passed to api_func if use_timeout_param=True
        use_timeout_param: If True, call api_func(timeout), else call api_func()
        
    Returns:
        Result from api_func
    """
    for attempt in range(max_retries):
        try:
            result = api_func(timeout) if use_timeout_param else api_func()
            time.sleep(RATE_LIMIT_DELAY)
            return result
        except Exception as retry_error:
            if attempt < max_retries - 1:
                wait_time = backoff_base * (attempt + 1)
                log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {description}, retrying in {wait_time}s...", "WARN")
                time.sleep(wait_time)
            else:
                raise retry_error


def handle_etl_error(e, operation_name, conn=None):
    """
    Standardized error handling for ETL operations.
    
    Args:
        e: The exception that occurred
        operation_name: Name of the operation for logging
        conn: Optional database connection to rollback
    """
    log(f"Failed {operation_name}: {e}", "ERROR")
    import traceback
    log(traceback.format_exc(), "ERROR")
    if conn:
        conn.rollback()
        log("  Rolled back transaction - continuing ETL", "WARN")


def check_emergency_brake(consecutive_failures, threshold=5, sleep_time=30):
    """
    Emergency brake: if too many consecutive failures, take a break.
    
    Args:
        consecutive_failures: Current count of consecutive failures
        threshold: Number of failures before taking a break (default 5)
        sleep_time: Seconds to sleep (default 30)
        
    Returns:
        0 (resets counter after sleeping) or original count
    """
    if consecutive_failures >= threshold:
        log(f"  WARNING - Taking {sleep_time}s break after {consecutive_failures} consecutive failures...", "WARN")
        time.sleep(sleep_time)
        return 0
    return consecutive_failures


def extract_putbacks_from_result(result):
    """
    Extract total putbacks from NBA API result containing shot type data.
    Sums FGM for all Putback and Tip shot types.
    
    Args:
        result: API response dict from playerdashboardbyshootingsplits or teamdashboardbyshootingsplits
        
    Returns:
        Total putback FGM
    """
    putback_total = 0
    
    for rs in result['resultSets']:
        if 'ShotType' in rs['name']:
            headers = rs['headers']
            fgm_idx = headers.index('FGM')
            
            for row in rs['rowSet']:
                shot_type = row[1]  # GROUP_VALUE column
                if any(keyword in shot_type for keyword in ['Putback', 'Tip']):
                    putback_total += row[fgm_idx] or 0
            break
    
    return putback_total


def update_stats_from_league_endpoint(endpoint_name, endpoint_params, season, season_year, 
                                       entity='player', table='player_season_stats', 
                                       description=None):
    """
    Generic function to fetch and update stats from any league-wide endpoint.
    Eliminates duplication across rebounding, hustle, defense, playmaking, etc.
    
    Args:
        endpoint_name: Name of the endpoint (e.g., 'leaguedashptstats')
        endpoint_params: Dict of parameters for the endpoint (e.g., {'pt_measure_type': 'Rebounding'})
        season: Season string (e.g., '2024-25')
        season_year: Season year (e.g., 2024)
        entity: 'player' or 'team'
        table: Target database table
        description: Human-readable description for logging
        
    Returns:
        Number of records updated
    """
    if description is None:
        description = f"{endpoint_name} ({entity})"
    
    log(f"Fetching {description} for {season}...")
    
    # Get columns from config
    cols = get_columns_by_endpoint(endpoint_name, entity, table=table)
    if not cols:
        log(f"  No columns configured for {endpoint_name}/{entity} - skipping")
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Dynamically import and call the endpoint
        if endpoint_name == 'leaguedashptstats':
            endpoint = leaguedashptstats.LeagueDashPtStats(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                player_or_team='Player' if entity == 'player' else 'Team',
                **endpoint_params
            )
        elif endpoint_name == 'leaguehustlestatsplayer':
            endpoint = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
                season=season,
                per_mode_time='Totals',
                season_type_all_star='Regular Season',
                **endpoint_params
            )
        elif endpoint_name == 'leaguedashptdefend':
            endpoint = leaguedashptdefend.LeagueDashPtDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                **endpoint_params
            )
        else:
            raise ValueError(f"Unsupported endpoint: {endpoint_name}")
        
        result = endpoint.get_dict()
        
        # Handle multiple result sets (some endpoints return multiple)
        all_records = []
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                entity_id = row[0]  # PLAYER_ID or TEAM_ID
                
                # Extract each stat from API response using config
                values = []
                for stat_name, stat_cfg in cols.items():
                    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
                    source = stat_cfg.get(source_key) or stat_cfg.get('source', {})
                    nba_field = source.get('field')
                    scale = source.get('scale', 1)
                    transform_name = source.get('transform', 'safe_int')
                    
                    raw_value = row[headers.index(nba_field)] if nba_field in headers else 0
                    
                    if transform_name == 'safe_int':
                        value = safe_int(raw_value, scale)
                    elif transform_name == 'safe_float':
                        value = safe_float(raw_value, scale)
                    else:
                        value = safe_int(raw_value, scale)
                        
                    values.append(value)
                
                # Build record: (stat1, stat2, ..., entity_id, season_year)
                values.extend([entity_id, season_year])
                all_records.append(tuple(values))
        
        # Bulk update using config-driven column names
        if all_records:
            set_clause = ', '.join([f"{col} = %s" for col in cols.keys()])
            
            updated = 0
            for record in all_records:
                cursor.execute(f"""
                    UPDATE {table}
                    SET {set_clause}, updated_at = NOW()
                    WHERE {'player_id' if entity == 'player' else 'team_id'} = %s 
                    AND year = %s AND season_type = 1
                """, record)
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            log(f"  {description}: {updated} {entity}s updated ({len(cols)} columns)")
            update_group_progress(1)  # Update progress after each league-wide call
            return updated
        else:
            log(f"  WARNING - No {description} data to update", "WARN")
            conn.commit()
            update_group_progress(1)  # Update progress even if no data
            return 0
        
    except Exception as e:
        handle_etl_error(e, description, conn)
        return 0
    finally:
        cursor.close()
        conn.close()


def log(message, level="INFO"):
    """Centralized logging - uses tqdm.write to avoid interfering with progress bars"""
    tqdm.write(message)


def update_group_progress(n=1, description=None):
    """Update the group progress bar (called from individual ETL functions)"""
    global _group_pbar, _overall_pbar
    if _group_pbar is not None:
        if description:
            _group_pbar.set_description(description)
        _group_pbar.update(n)
    # Also update overall progress with each transaction
    if _overall_pbar is not None:
        _overall_pbar.update(n)


def adjust_overall_progress(new_total):
    """
    Dynamically adjust the overall progress bar total.
    This allows us to refine estimates as we learn actual counts.
    """
    global _overall_pbar
    if _overall_pbar is not None:
        current = _overall_pbar.n
        _overall_pbar.total = new_total
        _overall_pbar.refresh()


def get_db_connection():
    """Create database connection with timeout protection"""
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        application_name='the_glass_etl',
        options='-c statement_timeout=120000'  # 120 second timeout per statement (increased for bulk updates)
    )
    return conn


def ensure_schema_exists():
    """Create database schema if it doesn't exist (first-time setup)"""
    log("Checking database schema...")
    
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
        log("Schema already exists")
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
        _rate_limiter = RateLimiter(requests_per_second=1.67)
    
    log("=" * 70)
    log("STEP 1: Updating Player Rosters")
    log("="* 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    current_season = NBA_CONFIG['current_season']
    
    log(f"Fetching ALL players with stats from current season ({current_season})...")
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW
    # This is the SOURCE OF TRUTH for current team assignments
    log("Fetching current team rosters from NBA API...")
    try:
        from nba_api.stats.static import teams
        from nba_api.stats.endpoints import commonteamroster
        nba_teams = teams.get_teams()
        
        # OPTIMIZATION: Parallel roster fetching (30 teams -> ~10 seconds instead of 30)
        global _parallel_executor
        if _parallel_executor is None:
            _parallel_executor = ParallelAPIExecutor(
                max_workers=4,  # 4 parallel workers for roster fetching
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
        results, errors = _parallel_executor.execute_batch(
            tasks, 
            description=f"Team rosters for {current_season}",
            progress_callback=update_group_progress  # Update progress bar as tasks complete
        )
        
        # Process results (no need to update progress here - already done in callback)
        for team_id, (roster_endpoint, team_name) in results.items():
            try:
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
                
            except Exception as e:
                log(f"  WARNING - Failed to process roster for team {team_id}: {e}", "WARN")
        
        log(f"Fetched current rosters: {len(all_players)} players ({len(errors)} teams failed)")
        
    except Exception as e:
        log(f"WARNING - Failed to fetch current rosters: {e}", "WARN")
    
    # Get existing players from database to identify NEW players
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT player_id FROM players")
    existing_player_ids = {row[0] for row in cursor.fetchall()}
    
    # Identify NEW players (not in database)
    new_player_ids = [pid for pid in all_players.keys() if pid not in existing_player_ids]
    
    if new_player_ids:
        log(f"Found {len(new_player_ids)} new players - fetching height/weight/birthdate...")
        
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
        
        # Execute in parallel (3 workers to be conservative)
        detail_executor = ParallelAPIExecutor(
            max_workers=3,
            rate_limiter=_rate_limiter,
            log_func=log
        )
        
        results, errors = detail_executor.execute_batch(
            tasks,
            description="New player details",
            progress_callback=update_group_progress  # Update progress as tasks complete
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
            except Exception as e:
                log(f"  WARNING - Failed to process details for player {player_id}: {e}", "WARN")
        
        if errors:
            log(f"WARNING - Could not fetch details for {len(errors)}/{len(new_player_ids)} new players", "WARN")
            log("  These players will still be added with basic info (name, team, jersey from roster)", "WARN")
    else:
        log("No new players found - all players already in database")
    
    # First, clear team_id for all players (they'll be re-assigned if still on roster)
    cursor.execute("UPDATE players SET team_id = NULL, updated_at = NOW()")
    conn.commit()
    
    bulk_writer = BulkDatabaseWriter(conn, batch_size=500, log_func=log)
    
    # Separate new players from updates
    new_players_data = []
    update_players_data = []
    
    cursor.execute("SELECT player_id, team_id FROM players")
    existing_players = {row[0]: row[1] for row in cursor.fetchall()}
    
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
        
        if basic:
            bulk_writer.bulk_upsert(
                'players',
                ['player_id', 'name', 'team_id', 'jersey_number'],
                basic,
                conflict_columns=['player_id'],
                update_columns=['team_id', 'jersey_number']
            )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"Roster update complete: {players_added} added, {players_updated} updated")
    
    return players_added, players_updated


def update_player_stats(skip_zero_stats=False):
    """
    Update season statistics for all players (Basic Stats from leaguedashplayerstats)
    Uses config_database.DB_COLUMNS for all field mappings - NO HARDCODING!
    
    Args:
        skip_zero_stats: If True, don't add zero-stat records for roster players (backfill mode)
    """
    log("=" * 70)
    log("STEP 2: Updating Player Stats (leaguedashplayerstats)")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    
    # Get all columns from leaguedashplayerstats endpoint using config
    # Filter to only player_season_stats table columns (excludes 'name' which is players-only)
    basic_cols = get_columns_by_endpoint('leaguedashplayerstats', 'player', table='player_season_stats')
    
    # Get valid player IDs from database (all players on rosters)
    cursor.execute("SELECT player_id, team_id FROM players")
    all_players = cursor.fetchall()
    valid_player_ids = {row[0] for row in all_players}
    
    # Process all season types
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
    ]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            # Fetch basic stats
            df = retry_api_call(
                lambda: leaguedashplayerstats.LeagueDashPlayerStats(
                    season=current_season,
                    season_type_all_star=season_type_name,
                    per_mode_detailed='Totals',
                    timeout=120
                ).get_data_frames()[0],
                f"{season_type_name} basic stats"
            )
            
            if df.empty:
                log(f"No {season_type_name} data for {current_season}")
                continue
            
            # Fetch advanced stats if we have advanced columns in config
            adv_cols = [col for col, cfg in basic_cols.items() 
                       if cfg.get('source', {}).get('field') in ['OFF_RATING', 'DEF_RATING', 'OREB_PCT', 'DREB_PCT']]
            if adv_cols:
                try:
                    adv_df = retry_api_call(
                        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed='Totals',
                            timeout=120
                        ).get_data_frames()[0],
                        f"{season_type_name} advanced stats"
                    )
                    
                    if not adv_df.empty:
                        df = df.merge(
                            adv_df[['PLAYER_ID', 'TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'OREB_PCT', 'DREB_PCT']], 
                            on=['PLAYER_ID', 'TEAM_ID'], 
                            how='left'
                        )
                except Exception as e:
                    log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
            
            log(f"Fetched {season_type_name} stats for {len(df)} players")
            
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
                    current_year,
                    season_type_code,
                ]
                
                # Add stats from config in sorted order for consistency
                for col_name in sorted(basic_cols.keys()):
                    col_config = basic_cols[col_name]
                    
                    # Extract value using config-driven source and transform
                    source = col_config.get('source')
                    if not source:
                        record_values.append(0)
                        continue
                    
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Handle calculated fields (e.g., "FGM - FG3M")
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
                        zero_values = [player_id, current_year, season_type_code]
                        
                        # Add zeros for all stats (sorted order matches record_values above)
                        for col_name in sorted(basic_cols.keys()):
                            col_config = basic_cols[col_name]
                            # Use default value from config, or 0 if not specified
                            default_val = col_config.get('default', 0)
                            zero_values.append(default_val)
                        
                        records.append(tuple(zero_values))
            
            # Bulk insert using config-driven column names
            if records:
                # Build column list from config (sorted to match record order)
                # Note: team_id removed from player_season_stats in multi-table refactor
                db_columns = ['player_id', 'year', 'season_type'] + sorted(basic_cols.keys())
                columns_str = ', '.join(db_columns)
                
                # Build UPDATE SET clause from config (exclude keys)
                update_clauses = [
                    f"{col} = EXCLUDED.{col}" for col in sorted(basic_cols.keys())
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
    
    log(f"Player stats complete: {total_updated} total records")
    return True


def update_team_stats():
    """
    Update season statistics for all teams (leaguedashteamstats + opponent stats)
    Uses config_database.DB_COLUMNS for all field mappings - NO HARDCODING!
    """
    log("=" * 70)
    log("STEP 3: Updating Team Stats (leaguedashteamstats + opponent)")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    
    # Get valid team IDs from config (numeric IDs, not abbreviations)
    valid_team_ids = set(TEAM_IDS.values())
    
    # Get team stats columns from config
    team_cols = get_columns_by_endpoint('leaguedashteamstats', 'team')
    opp_cols = get_columns_by_entity('opponent')
    
    # Process all season types
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
        ('PlayIn', 3),
    ]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            # Fetch basic stats
            df = retry_api_call(
                lambda: leaguedashteamstats.LeagueDashTeamStats(
                    season=current_season,
                    season_type_all_star=season_type_name,
                    per_mode_detailed='Totals',
                    timeout=120
                ).get_data_frames()[0],
                f"{season_type_name} team stats"
            )
            
            if df.empty:
                log(f"No {season_type_name} data for {current_season}")
                continue
            
            # Fetch advanced stats
            try:
                adv_df = retry_api_call(
                    lambda: leaguedashteamstats.LeagueDashTeamStats(
                        season=current_season,
                        season_type_all_star=season_type_name,
                        measure_type_detailed_defense='Advanced',
                        per_mode_detailed='Totals',
                        timeout=120
                    ).get_data_frames()[0],
                    f"{season_type_name} team advanced stats"
                )
                
                if not adv_df.empty:
                    df = df.merge(
                        adv_df[['TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'OREB_PCT', 'DREB_PCT']], 
                        on='TEAM_ID',
                        how='left'
                    )
            except Exception as e:
                log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
            
            # Fetch opponent stats (what opponents did against each team)
            try:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        opp_stats = leaguedashteamstats.LeagueDashTeamStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Opponent',
                            per_mode_detailed='Totals',
                            timeout=120
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        opp_df = opp_stats.get_data_frames()[0]
                        break
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            wait_time = 10 * (attempt + 1)
                            log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {season_type_name} team opponent stats, retrying in {wait_time}s...", "WARN")
                            time.sleep(wait_time)
                        else:
                            raise retry_error
                
                if not opp_df.empty:
                    # Merge opponent stats - they come with OPP_ prefix from API
                    df = df.merge(opp_df, on='TEAM_ID', how='left', suffixes=('', '_OPP'))
                    log(f"Fetched opponent stats for {len(opp_df)} teams")
            except Exception as e:
                log(f"Warning: Could not fetch opponent stats: {e}", "WARN")
            
            # Remove duplicates (some seasons return duplicate team entries)
            df = df.drop_duplicates(subset=['TEAM_ID'], keep='first')
            
            log(f"Fetched {season_type_name} stats for {len(df)} teams")
            
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
                    source = col_config.get('source')
                    if not source:
                        record_values.append(0)
                        continue
                    
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Handle calculated fields (e.g., "FGM - FG3M")
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
                    source = col_config.get('source')
                    if not source:
                        record_values.append(0)
                        continue
                    
                    # Opponent stats come with OPP_ prefix from API
                    field_name = source.get('field')
                    if not field_name:
                        record_values.append(0)
                        continue
                    
                    # Map to OPP_ prefixed version for opponent endpoint
                    opp_field = f'OPP_{field_name}' if not field_name.startswith('OPP_') else field_name
                    
                    # Handle calculated fields (e.g., "FGM - FG3M")
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
                team_stat_cols = sorted(team_cols.keys())
                opp_stat_cols = [f'opp_{col}' for col in sorted(opp_cols.keys())]
                all_stat_cols = team_stat_cols + opp_stat_cols
                db_columns = ['team_id', 'year', 'season_type'] + all_stat_cols
                columns_str = ', '.join(db_columns)
                
                # Build UPDATE SET clause from config (exclude keys)
                update_clauses = [f"{col} = EXCLUDED.{col}" for col in all_stat_cols]
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
    
    cursor.close()
    conn.close()
    
    log(f"Team stats complete: {total_updated} total records")
    return True


def update_shooting_tracking_bulk(season, season_year):
    """
    Shooting Tracking Stats - Fully config-driven (playerdashptshots endpoint)
    Per-player API calls for contested/open shooting by zone.
    Uses DB_COLUMNS config for all field mappings.
    
    Returns: Number of players actually processed (for progress bar adjustment)
    """
    log(f"Fetching Shooting Tracking (per-player) for {season}...")
    
    # Get shooting columns from config
    shooting_cols = get_columns_by_endpoint('playerdashptshots', 'player')
    if not shooting_cols:
        log("  No shooting tracking columns configured - skipping")
        return
    
    log(f"  Processing {len(shooting_cols)} shooting columns")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get active players (those with stats records)
    cursor.execute("""
        SELECT DISTINCT player_id 
        FROM player_season_stats
        WHERE year = %s AND season_type = 1
        ORDER BY player_id
    """, (season_year,))
    
    players = [row[0] for row in cursor.fetchall()]
    log(f"Found {len(players)} active players for shooting tracking")
    
    # Store player shooting data
    player_data = {}
    failed = 0
    
    try:
        from nba_api.stats.endpoints import playerdashptshots
        
        for idx, player_id in enumerate(players):
            
            try:
                stats = {}
                
                # 1. Contested rim (Restricted Area, 0-4 ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='0-2 Feet - Very Tight, 2-4 Feet - Tight',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'ClosestDefenderShooting':
                            for row in rs['rowSet']:
                                if 'Restricted Area' in str(row):
                                    stats['cont_rim_fgm'] = stats.get('cont_rim_fgm', 0) + (row[rs['headers'].index('FGM')] or 0)
                                    stats['cont_rim_fga'] = stats.get('cont_rim_fga', 0) + (row[rs['headers'].index('FGA')] or 0)
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 2. Open rim (Restricted Area, 4+ ft defender)  
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='4-6 Feet - Open, 6+ Feet - Wide Open',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'ClosestDefenderShooting':
                            for row in rs['rowSet']:
                                if 'Restricted Area' in str(row):
                                    stats['open_rim_fgm'] = stats.get('open_rim_fgm', 0) + (row[rs['headers'].index('FGM')] or 0)
                                    stats['open_rim_fga'] = stats.get('open_rim_fga', 0) + (row[rs['headers'].index('FGA')] or 0)
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 3. Contested 2PT (all zones, 0-4 ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='0-2 Feet - Very Tight, 2-4 Feet - Tight',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['cont_fg2m'] = row[rs['headers'].index('FG2M')] or 0
                                stats['cont_fg2a'] = row[rs['headers'].index('FG2A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 4. Open 2PT (all zones, 4+ ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='4-6 Feet - Open, 6+ Feet - Wide Open',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['open_fg2m'] = row[rs['headers'].index('FG2M')] or 0
                                stats['open_fg2a'] = row[rs['headers'].index('FG2A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 5. Contested 3PT (0-4 ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='3PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='0-2 Feet - Very Tight, 2-4 Feet - Tight',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['cont_fg3m'] = row[rs['headers'].index('FG3M')] or 0
                                stats['cont_fg3a'] = row[rs['headers'].index('FG3A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 6. Open 3PT (4+ ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='3PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='4-6 Feet - Open, 6+ Feet - Wide Open',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['open_fg3m'] = row[rs['headers'].index('FG3M')] or 0
                                stats['open_fg3a'] = row[rs['headers'].index('FG3A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                if stats:
                    player_data[player_id] = stats
                    
            except Exception as e:
                failed += 1
                if failed <= 5:  # Only log first 5 failures
                    log(f"  Failed player {player_id}: {e}", "WARN")
            
            # Update progress after each player (6 API calls per player)
            update_group_progress(1)
        
        # Update database using config-driven column names
        log("Updating database with shooting data...")
        updated = 0
        
        # Get shooting column names from config
        col_names = list(shooting_cols.keys())
        
        for player_id, stats in player_data.items():
            # Build dynamic SET clause from config columns
            set_values = []
            params = []
            
            for col_name in col_names:
                set_values.append(f"{col_name} = %s")
                params.append(stats.get(col_name, 0))
            
            # Add player_id and year to params
            params.extend([player_id, season_year])
            
            set_clause = ', '.join(set_values)
            
            cursor.execute(f"""
                UPDATE player_season_stats
                SET {set_clause}, updated_at = NOW()
                WHERE player_id = %s AND year = %s AND season_type = 1
            """, params)
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"Shooting tracking (playerdashptshots): {updated} players updated, {failed} failed")
        
    except Exception as e:
        handle_etl_error(e, "shooting tracking (playerdashptshots)", conn)
    finally:
        cursor.close()
        conn.close()


def update_playmaking_bulk(season, season_year):
    """
    Playmaking & Possession Stats - FULLY CONFIG-DRIVEN
    Automatically discovers which pt_measure_types are needed from DB_COLUMNS config.
    No hardcoding - config determines which API calls to make.
    """
    log(f"Fetching Playmaking & Possession stats for {season}...")
    
    # Discover all unique pt_measure_types from config for leaguedashptstats endpoint
    measure_types = set()
    for col_name, col_config in DB_COLUMNS.items():
        player_source = col_config.get('player_source', {})
        if player_source.get('endpoint') == 'leaguedashptstats':
            pt_measure = player_source.get('pt_measure_type')
            if pt_measure:
                measure_types.add(pt_measure)
    
    # Fetch each measure type
    for measure_type in sorted(measure_types):
        # Get columns for this measure type to build description
        cols = [col for col, cfg in DB_COLUMNS.items() 
                if cfg.get('player_source', {}).get('pt_measure_type') == measure_type]
        
        update_stats_from_league_endpoint(
            endpoint_name='leaguedashptstats',
            endpoint_params={'pt_measure_type': measure_type},
            season=season,
            season_year=season_year,
            description=f'{measure_type} stats ({", ".join(cols[:3])}{"..." if len(cols) > 3 else ""})'
        )


# Removed wrapper functions - player advanced stats now fully config-driven
# See update_player_advanced_stats() which discovers endpoints from DB_COLUMNS


def update_defense_stats_bulk(season, season_year):
    """
    Defense Tracking Stats - Fully config-driven (leaguedashptdefend endpoint)
    Fetches rim defense, 2PT defense, 3PT defense, and defensive FG%.
    Uses DB_COLUMNS config for all field mappings.
    """
    log(f"Fetching Defense tracking stats for {season}...")
    
    # Get defense columns from config
    defense_cols = get_columns_by_endpoint('leaguedashptdefend', 'player')
    if not defense_cols:
        log("  No defense tracking columns configured - skipping")
        return
    
    log(f"  Processing {len(defense_cols)} defense columns")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Store defense data: player_id -> {stat: value}
        player_data = {}
        
        # Call 1: Overall defense (for FG totals and real_def_fg_pct)
        defense = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Overall'
        )
        
        result = defense.get_dict()
        
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]  # CLOSE_DEF_PERSON_ID
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                # Total D_FGM/A (will subtract FG3 to get FG2)
                player_data[player_id]['total_def_fgm'] = row[headers.index('D_FGM')] or 0
                player_data[player_id]['total_def_fga'] = row[headers.index('D_FGA')] or 0
                
                # Real defensive FG% (D_FG_PCT from Overall category)
                d_fg_pct = row[headers.index('D_FG_PCT')]
                player_data[player_id]['real_def_fg_pct_x1000'] = safe_int(d_fg_pct, 1000)
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # Call 2: Rim defense (Less Than 6 Ft)
        defense_rim = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Less Than 6Ft'
        )
        
        result_rim = defense_rim.get_dict()
        
        for rs in result_rim['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                player_data[player_id]['d_rim_fgm'] = row[headers.index('FGM_LT_06')] or 0
                player_data[player_id]['d_rim_fga'] = row[headers.index('FGA_LT_06')] or 0
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # Call 3: 3PT defense
        defense_3pt = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='3 Pointers'
        )
        
        result_3pt = defense_3pt.get_dict()
        
        for rs in result_3pt['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                player_data[player_id]['d_3fgm'] = row[headers.index('FG3M')] or 0
                player_data[player_id]['d_3fga'] = row[headers.index('FG3A')] or 0
        
        # Calculate def_fg2 = total - fg3 (per config calculation)
        log("  Calculating FG2 defense and updating database...")
        
        # Build dynamic UPDATE query from config columns
        col_names = list(defense_cols.keys())
        
        updated = 0
        for player_id, stats in player_data.items():
            # Calculate FG2 = Total - FG3
            total_fgm = stats.get('total_def_fgm', 0)
            total_fga = stats.get('total_def_fga', 0)
            d_3fgm = stats.get('d_3fgm', 0)
            d_3fga = stats.get('d_3fga', 0)
            
            stats['d_2fgm'] = max(0, total_fgm - d_3fgm)
            stats['d_2fga'] = max(0, total_fga - d_3fga)
            
            # Build SET clause dynamically from config
            set_values = []
            params = []
            
            for col_name in col_names:
                set_values.append(f"{col_name} = %s")
                params.append(stats.get(col_name, 0))
            
            params.extend([player_id, season_year])
            set_clause = ', '.join(set_values)
            
            cursor.execute(f"""
                UPDATE player_season_stats
                SET {set_clause}, updated_at = NOW()
                WHERE player_id = %s AND year = %s AND season_type = 1
            """, params)
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"Defense tracking (leaguedashptdefend): {updated} players updated ({len(defense_cols)} columns)")
        update_group_progress(1)  # Update progress after defense tracking
        
    except Exception as e:
        handle_etl_error(e, "defense tracking (leaguedashptdefend)", conn)
    finally:
        cursor.close()
        conn.close()


def update_putbacks_per_player(season, season_year, skip_on_backfill=False):
    """
    Putbacks - Per-player (playerdashboardbyshootingsplits endpoint)
    
    Maps to: putbacks (sum of Putback + Tip shot FGM)
    Endpoint: PlayerDashboardByShootingSplits (per player, ~480 calls)
    
    RESILIENT: Implements retry logic for API instability
    - 2 attempts per player with backoff (2s, 5s)
    - Shorter timeout (20s) to fail fast on hangs
    - 2.5s delay between all requests to avoid rate limits (INCREASED)
    - Logs each player attempt for visibility
    - Continues on failure to complete ETL
    
    Args:
        skip_on_backfill: If True, skip this function (for backfill mode)
    """
    if skip_on_backfill:
        log(f"Skipping Putbacks (playerdashboardbyshootingsplits) for {season} (backfill mode - endpoint too unstable)")
        return
    
    log(f"Fetching Putbacks (playerdashboardbyshootingsplits) for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Only get active players with games played (players with 0 GP have no shot data)
    cursor.execute("""
        SELECT DISTINCT pss.player_id, p.name 
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        WHERE pss.year = %s AND pss.season_type = 1 AND pss.games_played > 0
        ORDER BY pss.player_id
    """, (season_year,))
    
    players = cursor.fetchall()
    
    # Get total count for context
    cursor.execute("""
        SELECT COUNT(*) FROM player_season_stats
        WHERE year = %s AND season_type = 1
    """, (season_year,))
    total_players = cursor.fetchone()[0]
    
    log(f"Found {len(players)} active players (out of {total_players} total)")
    
    updated = 0
    failed = 0
    consecutive_failures = 0
    retry_queue = []  # Players that failed all attempts - retry at end
    
    for idx, (player_id, player_name) in enumerate(players):
        success = False
        putbacks_value = 0
        
        # Emergency brake
        consecutive_failures = check_emergency_brake(consecutive_failures)
        
        # Try up to 2 times with backoff
        for attempt in range(1, 3):
            try:
                result = playerdashboardbyshootingsplits.PlayerDashboardByShootingSplits(
                    player_id=player_id,
                    season=season,
                    measure_type_detailed='Base',
                    per_mode_detailed='Totals',
                    timeout=20
                ).get_dict()
                
                putbacks_value = extract_putbacks_from_result(result)
                
                cursor.execute("""
                    UPDATE player_season_stats
                    SET putbacks = %s, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, (putbacks_value, player_id, season_year))
                
                if cursor.rowcount > 0:
                    updated += 1
                    success = True
                    consecutive_failures = 0
                break
                
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 if attempt == 1 else 5)
                else:
                    failed += 1
                    consecutive_failures += 1
        
        if not success:
            retry_queue.append((player_id, player_name))
        
        time.sleep(max(2.5, RATE_LIMIT_DELAY))
        update_group_progress(1)
    
    # Retry failed players at the end
    if retry_queue:
        log(f"\n  Retrying {len(retry_queue)} failed players...")
        for player_id, player_name in retry_queue:
            try:
                result = playerdashboardbyshootingsplits.PlayerDashboardByShootingSplits(
                    player_id=player_id,
                    season=season,
                    per_mode_detailed='Totals',
                    season_type_all_star='Regular Season',
                    timeout=20
                ).get_dict()
                
                putbacks_value = extract_putbacks_from_result(result)
                
                cursor.execute("""
                    UPDATE player_season_stats
                    SET putbacks = %s
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, (putbacks_value, player_id, season_year))
                updated += 1
                failed -= 1
                log(f"Retry success: {player_name} (ID: {player_id})")
                
            except Exception as e:
                log(f"  ✗ Retry failed: {player_name} - {str(e)[:100]}")
            
            time.sleep(max(2.5, RATE_LIMIT_DELAY))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"Putbacks (playerdashboardbyshootingsplits): {updated} players updated, {failed} failed")
    
    if failed > 0:
        log(f"  WARNING - {failed} players failed after all retries - continuing ETL", "WARN")


def update_onoff_stats(season, season_year):
    """
    On-Off Ratings - Config-driven (teamplayeronoffdetails endpoint)
    Fetches team offensive/defensive rating when player is OFF court.
    Per-team API calls (30 teams).
    """
    log(f"Fetching On-Off ratings for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    updated = 0
    failed = 0
    retry_queue = []  # Teams that failed all attempts - retry at end
    
    for team_id in TEAM_IDS.values():
        try:
            onoff = teamplayeronoffdetails.TeamPlayerOnOffDetails(
                team_id=team_id,
                season=season,
                per_mode_detailed='Totals',
                season_type_all_star='Regular Season'
            )
            
            result = onoff.get_dict()
            
            # Find the OFF court result set (player is off court)
            for rs in result['resultSets']:
                if rs['name'] == 'PlayersOffCourtTeamPlayerOnOffDetails':
                    headers = rs['headers']
                    
                    for row in rs['rowSet']:
                        player_id = row[headers.index('VS_PLAYER_ID')]
                        
                        # Get team stats when player is OFF court
                        pts = row[headers.index('PTS')] or 0
                        fga = row[headers.index('FGA')] or 0
                        fta = row[headers.index('FTA')] or 0
                        oreb = row[headers.index('OREB')] or 0
                        tov = row[headers.index('TOV')] or 0
                        plus_minus = row[headers.index('PLUS_MINUS')] or 0
                        
                        # Calculate possessions: FGA - OREB + TOV + 0.44*FTA
                        poss = fga - oreb + tov + (0.44 * fta)
                        
                        if poss > 0:
                            # Offensive rating: points per 100 possessions
                            off_rating = (pts / poss) * 100
                            
                            # Defensive rating: opponent points per 100 possessions
                            # Approximate: if team scores X and has +/- Y, opponents scored (X - Y)
                            opp_pts = pts - plus_minus
                            def_rating = (opp_pts / poss) * 100 if poss > 0 else 0
                            
                            # Scale by 10 for storage (per config)
                            tm_off_off_rating_x10 = int(off_rating * 10)
                            tm_off_def_rating_x10 = int(def_rating * 10)
                            
                            # Update using config column names
                            cursor.execute("""
                                UPDATE player_season_stats
                                SET tm_off_off_rating_x10 = %s, tm_off_def_rating_x10 = %s, updated_at = NOW()
                                WHERE player_id = %s AND year = %s AND season_type = 1
                            """, (tm_off_off_rating_x10, tm_off_def_rating_x10, player_id, season_year))
                            
                            if cursor.rowcount > 0:
                                updated += 1
            
            time.sleep(RATE_LIMIT_DELAY)
            update_group_progress(1)
            
        except Exception as e:
            log(f"  Failed team {team_id}: {e}", "WARN")
            failed += 1
            retry_queue.append(team_id)
    
    # Retry failed teams at the end (one more attempt each)
    if retry_queue:
        log(f"\n  Retrying {len(retry_queue)} failed teams...")
        for team_id in retry_queue:
            try:
                onoff = teamplayeronoffdetails.TeamPlayerOnOffDetails(
                    team_id=team_id,
                    season=season,
                    per_mode_detailed='Totals',
                    season_type_all_star='Regular Season'
                )
                
                result = onoff.get_dict()
                for rs in result['resultSets']:
                    if 'Off' in rs['name']:
                        headers = rs['headers']
                        for row in rs['rowSet']:
                            row_dict = dict(zip(headers, row))
                            player_id = safe_int(row_dict.get('PLAYER_ID'))
                            
                            if player_id == 0:
                                continue
                            
                            poss = safe_float(row_dict.get('OFF_POSS', 0))
                            pts = safe_int(row_dict.get('OFF_PTS', 0))
                            opp_pts = safe_int(row_dict.get('OFF_OPP_PTS', 0))
                            
                            if poss and poss > 0:
                                off_rating = (pts / poss) * 100
                                def_rating = (opp_pts / poss) * 100
                            else:
                                continue
                            
                            tm_off_off_rating_x10 = int(off_rating * 10)
                            tm_off_def_rating_x10 = int(def_rating * 10)
                            
                            cursor.execute("""
                                UPDATE player_season_stats
                                SET tm_off_off_rating_x10 = %s, tm_off_def_rating_x10 = %s, updated_at = NOW()
                                WHERE player_id = %s AND year = %s AND season_type = 1
                            """, (tm_off_off_rating_x10, tm_off_def_rating_x10, player_id, season_year))
                            
                            if cursor.rowcount > 0:
                                updated += 1
                
                failed -= 1  # Remove from failed count
                log(f"Retry success: Team {team_id}")
                
            except Exception as e:
                log(f"  ✗ Retry failed: Team {team_id} - {str(e)[:100]}")
            
            time.sleep(RATE_LIMIT_DELAY)
    
    try:
        conn.commit()
        log(f"On-Off ratings (teamplayeronoffdetails): {updated} players updated, {failed} teams failed")
    except Exception as commit_error:
        log(f"Failed to commit on-off stats: {commit_error}", "ERROR")
        conn.rollback()
        log("  Rolled back transaction - continuing ETL", "WARN")
    finally:
        cursor.close()
        conn.close()


def update_team_shooting_tracking(season, season_year, conn=None, cursor=None):
    """
    Get team shooting tracking in 6 league-wide calls
    Maps to: cont_rim_fgm/fga, open_rim_fgm/fga, cont_fg2m/fga, open_fg2m/fga, 
             cont_fg3m/fg3a, open_fg3m/fg3a
    
    Note: Mid-range stats are calculated in frontend as fg2 - rim
    
    Args:
        conn: Optional database connection to reuse (prevents deadlocks)
        cursor: Optional cursor to reuse
    """
    log(f"Fetching team shooting tracking (league-wide) for {season}...")
    
    # Use provided connection or create new one
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        cursor = conn.cursor()
        close_conn = True
    elif cursor is None:
        cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashteamptshot
        
        # Store team shooting data: team_id -> {stat: value}
        team_data = {}
        
        # 1-2: Contested rim (0-2 ft + 2-4 ft)
        log("  Fetching contested rim shots (teams)...")
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                general_range_nullable='Less Than 10 ft',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fgm_idx = headers.index('FGM')
            fga_idx = headers.index('FGA')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                team_data[team_id]['cont_rim_fgm'] = team_data[team_id].get('cont_rim_fgm', 0) + (row[fgm_idx] or 0)
                team_data[team_id]['cont_rim_fga'] = team_data[team_id].get('cont_rim_fga', 0) + (row[fga_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # 3-4: Open rim (4-6 ft + 6+ ft)
        log("  Fetching open rim shots (teams)...")
        for def_dist in ['4-6 Feet - Open', '6+ Feet - Wide Open']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                general_range_nullable='Less Than 10 ft',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fgm_idx = headers.index('FGM')
            fga_idx = headers.index('FGA')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                team_data[team_id]['open_rim_fgm'] = team_data[team_id].get('open_rim_fgm', 0) + (row[fgm_idx] or 0)
                team_data[team_id]['open_rim_fga'] = team_data[team_id].get('open_rim_fga', 0) + (row[fga_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # 5-8: All shots contested (0-4 ft total) and open (4+ ft total) to get FG2/FG3 splits
        # Need 4 separate calls because API doesn't support comma-separated values
        log("  Fetching contested all shots (teams)...")
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fg2m_idx = headers.index('FG2M')
            fg2a_idx = headers.index('FG2A')
            fg3m_idx = headers.index('FG3M')
            fg3a_idx = headers.index('FG3A')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                # Accumulate contested totals
                team_data[team_id]['cont_fg2m_total'] = team_data[team_id].get('cont_fg2m_total', 0) + (row[fg2m_idx] or 0)
                team_data[team_id]['cont_fg2a_total'] = team_data[team_id].get('cont_fg2a_total', 0) + (row[fg2a_idx] or 0)
                team_data[team_id]['cont_fg3m'] = team_data[team_id].get('cont_fg3m', 0) + (row[fg3m_idx] or 0)
                team_data[team_id]['cont_fg3a'] = team_data[team_id].get('cont_fg3a', 0) + (row[fg3a_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        log("  Fetching open all shots (teams)...")
        for def_dist in ['4-6 Feet - Open', '6+ Feet - Wide Open']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fg2m_idx = headers.index('FG2M')
            fg2a_idx = headers.index('FG2A')
            fg3m_idx = headers.index('FG3M')
            fg3a_idx = headers.index('FG3A')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                # Accumulate open totals
                team_data[team_id]['open_fg2m_total'] = team_data[team_id].get('open_fg2m_total', 0) + (row[fg2m_idx] or 0)
                team_data[team_id]['open_fg2a_total'] = team_data[team_id].get('open_fg2a_total', 0) + (row[fg2a_idx] or 0)
                team_data[team_id]['open_fg3m'] = team_data[team_id].get('open_fg3m', 0) + (row[fg3m_idx] or 0)
                team_data[team_id]['open_fg3a'] = team_data[team_id].get('open_fg3a', 0) + (row[fg3a_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Calculate mid-range: MR = All 2PT - Rim
        log(f"  Calculating mid-range and updating database (teams) - {len(team_data)} teams to process...")
        updated = 0
        for idx, (team_id, stats) in enumerate(team_data.items()):
            cont_rim_fgm = stats.get('cont_rim_fgm', 0)
            cont_rim_fga = stats.get('cont_rim_fga', 0)
            open_rim_fgm = stats.get('open_rim_fgm', 0)
            open_rim_fga = stats.get('open_rim_fga', 0)
            
            # Store fg2 totals directly (mr is calculated in frontend as fg2 - rim)
            cont_fg2m = stats.get('cont_fg2m_total', 0)
            cont_fg2a = stats.get('cont_fg2a_total', 0)
            open_fg2m = stats.get('open_fg2m_total', 0)
            open_fg2a = stats.get('open_fg2a_total', 0)
            
            cont_fg3m = stats.get('cont_fg3m', 0)
            cont_fg3a = stats.get('cont_fg3a', 0)
            open_fg3m = stats.get('open_fg3m', 0)
            open_fg3a = stats.get('open_fg3a', 0)
            
            cursor.execute("""
                UPDATE team_season_stats
                SET cont_rim_fgm = %s, cont_rim_fga = %s,
                    open_rim_fgm = %s, open_rim_fga = %s,
                    cont_fg2m = %s, cont_fg2a = %s,
                    open_fg2m = %s, open_fg2a = %s,
                    cont_fg3m = %s, cont_fg3a = %s,
                    open_fg3m = %s, open_fg3a = %s,
                    updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (
                cont_rim_fgm, cont_rim_fga, open_rim_fgm, open_rim_fga,
                cont_fg2m, cont_fg2a, open_fg2m, open_fg2a,
                cont_fg3m, cont_fg3a, open_fg3m, open_fg3a,
                team_id, season_year
            ))
            
            if cursor.rowcount > 0:
                updated += 1
        
        # Only commit if we created our own connection
        if close_conn:
            conn.commit()
        
        log(f"Team shooting tracking: {updated} teams updated")
        
    except Exception as e:
        log(f"Failed team shooting tracking: {e}", "ERROR")
    finally:
        # Only close if we created our own connection
        if close_conn:
            cursor.close()
            conn.close()


def update_team_defense_stats(season, season_year, conn=None, cursor=None):
    """
    Get team defensive stats in 3 league-wide calls
    Maps to: def_rim_fgm, def_rim_fga, def_fg2m, def_fg2a, def_fg3m, def_fg3a, real_def_fg_pct_x1000
    
    RESILIENT: Implements retry logic for API instability
    - 3 attempts per call with exponential backoff
    - 20s timeout to fail fast on hangs
    - Logs each attempt for visibility
    
    Args:
        conn: Optional database connection to reuse (prevents deadlocks)
        cursor: Optional cursor to reuse
    """
    log(f"Fetching team defensive tracking (league-wide) for {season}...")
    
    # Use provided connection or create new one
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        cursor = conn.cursor()
        close_conn = True
    elif cursor is None:
        cursor = conn.cursor()
        close_conn = False
    
    try:
        from nba_api.stats.endpoints import leaguedashptteamdefend
        
        # Store data from all 3 calls, then calculate def_fg2m/fg2a
        team_data = {}
        
        # 1. Overall defense - get total FGM/FGA and real_def_fg_pct_x1000
        result = retry_api_call(
            lambda timeout: leaguedashptteamdefend.LeagueDashPtTeamDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                defense_category='Overall',
                timeout=timeout
            ).get_dict(),
            "Overall defense",
            use_timeout_param=True
        )
        
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_fgm_total = row[headers.index('D_FGM')] or 0
                def_fga_total = row[headers.index('D_FGA')] or 0
                pct_plusminus = row[headers.index('PCT_PLUSMINUS')]
                
                real_def_fg_pct = safe_int(pct_plusminus, 1000)
                
                team_data[team_id] = {
                    'def_fgm_total': def_fgm_total,
                    'def_fga_total': def_fga_total,
                    'real_def_fg_pct': real_def_fg_pct
                }
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 2. Rim defense
        result_rim = retry_api_call(
            lambda timeout: leaguedashptteamdefend.LeagueDashPtTeamDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                defense_category='Less Than 10Ft',
                timeout=timeout
            ).get_dict(),
            "Rim defense",
            use_timeout_param=True
        )
        
        for rs in result_rim['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_rim_fgm = row[headers.index('FGM_LT_10')] or 0
                def_rim_fga = row[headers.index('FGA_LT_10')] or 0
                
                if team_id in team_data:
                    team_data[team_id]['def_rim_fgm'] = def_rim_fgm
                    team_data[team_id]['def_rim_fga'] = def_rim_fga
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 3. 3PT defense
        result_3pt = retry_api_call(
            lambda timeout: leaguedashptteamdefend.LeagueDashPtTeamDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                defense_category='3 Pointers',
                timeout=timeout
            ).get_dict(),
            "3PT defense",
            use_timeout_param=True
        )
        
        for rs in result_3pt['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_fg3m = row[headers.index('FG3M')] or 0
                def_fg3a = row[headers.index('FG3A')] or 0
                
                if team_id in team_data:
                    team_data[team_id]['def_fg3m'] = def_fg3m
                    team_data[team_id]['def_fg3a'] = def_fg3a
        
        # Now calculate def_fg2m/fg2a and update all defense stats
        log("  3PT defense API call completed successfully")
        log("  Calculating FG2 defense and preparing database updates...")
        updated = 0
        for idx, (team_id, stats) in enumerate(team_data.items()):
            def_fgm_total = stats.get('def_fgm_total', 0)
            def_fga_total = stats.get('def_fga_total', 0)
            def_fg3m = stats.get('def_fg3m', 0)
            def_fg3a = stats.get('def_fg3a', 0)
            
            # Calculate 2PT defense as total - 3PT
            def_fg2m = def_fgm_total - def_fg3m
            def_fg2a = def_fga_total - def_fg3a

            cursor.execute("""
                UPDATE team_season_stats
                SET def_rim_fgm = %s, def_rim_fga = %s,
                    def_fg2m = %s, def_fg2a = %s,
                    def_fg3m = %s, def_fg3a = %s,
                    real_def_fg_pct_x1000 = %s,
                    updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (
                stats.get('def_rim_fgm', 0), stats.get('def_rim_fga', 0),
                def_fg2m, def_fg2a,
                def_fg3m, def_fg3a,
                stats.get('real_def_fg_pct', 0),
                team_id, season_year
            ))
            
            if cursor.rowcount > 0:
                updated += 1
        
        log(f"  Committing {updated} team defense updates to database...")
        
        # Only commit if we created our own connection
        if close_conn:
            conn.commit()
        
        log(f"Team defense stats: {updated} teams updated")
        
    except Exception as e:
        log(f"Failed team defense stats: {e}", "ERROR")
    finally:
        # Only close if we created our own connection
        if close_conn:
            cursor.close()
            conn.close()


def update_team_putbacks(season, season_year):
    """
    Get team putback stats using TeamDashboardByShootingSplits
    Maps to: putbacks (sum of Putback + Tip shot FGM)
    
    Uses ShotTypeTeamDashboard result set to get:
    - Putback Dunk Shot
    - Putback Layup Shot
    - Tip Dunk Shot
    - Tip Layup Shot
    
    Note: Requires 30 API calls (one per team)
    
    RESILIENT: Implements retry logic for API stability
    - 3 attempts per team with exponential backoff (2s, 5s, 10s)
    - 20s timeout to fail fast on hangs
    - Logs each attempt for visibility
    - Continues on failure to complete ETL
    """
    log(f"Fetching team putbacks for {season} (per-team)...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import teamdashboardbyshootingsplits
        
        updated = 0
        failed = 0
        consecutive_failures = 0
        retry_queue = []  # Teams that failed all attempts - retry at end
        
        # Fetch for all 30 teams
        for team_id in TEAM_IDS.values():
            success = False
            consecutive_failures = check_emergency_brake(consecutive_failures)
            
            # Try up to 3 times with exponential backoff
            for attempt in range(1, 4):
                try:
                    result = teamdashboardbyshootingsplits.TeamDashboardByShootingSplits(
                        team_id=team_id,
                        season=season,
                        per_mode_detailed='Totals',
                        season_type_all_star='Regular Season',
                        timeout=20
                    ).get_dict()
                    
                    putback_total = extract_putbacks_from_result(result)
                    
                    cursor.execute("""
                        UPDATE team_season_stats 
                        SET putbacks = %s, updated_at = NOW()
                        WHERE team_id = %s AND year = %s AND season_type = 1
                    """, (putback_total, team_id, season_year))
                    
                    if cursor.rowcount > 0:
                        updated += 1
                    
                    success = True
                    consecutive_failures = 0
                    break
                    
                except Exception as e:
                    if attempt < 3:
                        backoff = 2 ** attempt
                        log(f"  Team {team_id} attempt {attempt}/3 failed, retrying in {backoff}s...", "WARN")
                        time.sleep(backoff)
                    else:
                        failed += 1
                        consecutive_failures += 1
            
            if not success:
                retry_queue.append(team_id)
            
            time.sleep(RATE_LIMIT_DELAY)
            update_group_progress(1)
        
        # Retry failed teams at the end
        if retry_queue:
            log(f"\n  Retrying {len(retry_queue)} failed teams...")
            for team_id in retry_queue:
                try:
                    result = teamdashboardbyshootingsplits.TeamDashboardByShootingSplits(
                        team_id=team_id,
                        season=season,
                        per_mode_detailed='Totals',
                        season_type_all_star='Regular Season',
                        timeout=20
                    ).get_dict()
                    
                    putback_total = extract_putbacks_from_result(result)
                    
                    cursor.execute("""
                        UPDATE team_season_stats
                        SET putbacks = %s, updated_at = NOW()
                        WHERE team_id = %s AND year = %s AND season_type = 1
                    """, (putback_total, team_id, season_year))
                    
                    if cursor.rowcount > 0:
                        updated += 1
                        failed -= 1
                        log(f"Retry success: Team {team_id}")
                    
                except Exception as e:
                    log(f"  ✗ Retry failed: Team {team_id} - {str(e)[:100]}")
                
                time.sleep(RATE_LIMIT_DELAY)
        
        conn.commit()
        log(f"Team putbacks: {updated} teams updated, {failed} failed (30 API calls + {len(retry_queue)} retries)")
        
        if failed > 0:
            log(f"  WARNING - {failed} teams failed after all retries - continuing ETL", "WARN")
        
    except Exception as e:
        log(f"Failed team putbacks: {e}", "ERROR")
        log(f"  Error details: {str(e)}", "ERROR")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_team_advanced_stats(season=None, season_year=None):
    """
    Update advanced tracking stats for teams
    Uses league-wide endpoints for team tracking data:
    - Shooting tracking (contested/open by zone)
    - Playmaking (pot_ast, touches)
    - Rebounding (contested rebounds)
    - Hustle stats (charges, deflections, contests)
    - Defense stats
    - Putbacks
    """
    if season is None:
        season = NBA_CONFIG['current_season']
        season_year = NBA_CONFIG['current_season_year']
    
    if season_year < 2013:
        log("SKIP - Team tracking data not available before 2013-14 season")
        return
    
    log("=" * 70)
    log("STEP 5: Updating Team Advanced Stats")
    log("=" * 70)
    
    # Check for competing ETL processes before starting
    log("Checking for competing ETL processes...")
    check_conn = get_db_connection()
    check_cursor = check_conn.cursor()
    check_cursor.execute("""
        SELECT COUNT(*) FROM pg_stat_activity 
        WHERE datname = 'the_glass_db' 
          AND application_name = 'the_glass_etl'
          AND state IN ('active', 'idle in transaction')
          AND pid != pg_backend_pid()
    """)
    competing = check_cursor.fetchone()[0]
    check_cursor.close()
    check_conn.close()
    
    if competing > 0:
        log(f"WARNING - Found {competing} other ETL process(es) running!", "WARN")
        log("  This may cause deadlocks. Consider waiting for them to finish.", "WARN")
        log("  Continuing anyway with statement timeout protection...", "WARN")
    else:
        log("No competing ETL processes found")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. SHOOTING TRACKING (6 calls) - pass connection to avoid deadlock
        update_team_shooting_tracking(season, season_year, conn=conn, cursor=cursor)
        conn.commit()  # Commit shooting tracking before next section
        log("  Shooting tracking committed")
        
        # 2. PLAYMAKING - FULLY CONFIG-DRIVEN (discovers measure types from config)
        log(f"Fetching team playmaking/possession/distance stats for {season}...")
        
        # Discover all unique pt_measure_types needed for teams from config
        measure_types = set()
        for col_name, col_config in DB_COLUMNS.items():
            team_source = col_config.get('team_source', {})
            if team_source.get('endpoint') == 'leaguedashptstats':
                pt_measure = team_source.get('pt_measure_type')
                if pt_measure:
                    measure_types.add(pt_measure)
        
        # Fetch each measure type dynamically
        for measure_type in sorted(measure_types):
            # Get columns for this measure type
            measure_cols = get_columns_by_endpoint('leaguedashptstats', 'team')
            measure_cols = {k: v for k, v in measure_cols.items() 
                          if v.get('team_source', {}).get('pt_measure_type') == measure_type}
            
            if not measure_cols:
                continue
                
            result = retry_api_call(
                lambda timeout: leaguedashptstats.LeagueDashPtStats(
                    season=season,
                    per_mode_simple='Totals',
                    season_type_all_star='Regular Season',
                    player_or_team='Team',
                    pt_measure_type=measure_type,
                    timeout=timeout
                ).get_dict(),
                f"Team {measure_type} stats",
                use_timeout_param=True
            )
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                
                # Build dynamic SET clause from config
                set_values = []
                params = []
                
                for col_name, col_config in measure_cols.items():
                    source = col_config.get('team_source', {})
                    field = source.get('field')
                    scale = source.get('scale', 1)
                    if field and field in headers:
                        set_values.append(f"{col_name} = %s")
                        value = row[headers.index(field)]
                        if scale != 1:
                            params.append(safe_int(value, scale) if value else 0)
                        else:
                            params.append(value or 0)
                
                params.extend([team_id, season_year])
                
                if set_values:
                    set_clause = ', '.join(set_values)
                    cursor.execute(f"""
                        UPDATE team_season_stats
                        SET {set_clause}, updated_at = NOW()
                        WHERE team_id = %s AND year = %s AND season_type = 1
                    """, params)
            
            conn.commit()
            log(f"  {measure_type} stats committed ({len(measure_cols)} columns)")
            time.sleep(RATE_LIMIT_DELAY)
        
        # 3. REBOUNDING - Config-driven
        reb_cols = get_columns_by_endpoint('leaguedashptstats', 'team')
        reb_cols = {k: v for k, v in reb_cols.items() if v.get('team_source', {}).get('pt_measure_type') == 'Rebounding'}
        
        log(f"Fetching team rebounding data for {season}...")
        result = retry_api_call(
            lambda timeout: leaguedashptstats.LeagueDashPtStats(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                player_or_team='Team',
                pt_measure_type='Rebounding',
                timeout=timeout
            ).get_dict(),
            "Team rebounding",
            use_timeout_param=True
        )
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        for row in rs['rowSet']:
            team_id = row[0]
            
            # Build dynamic SET clause from config
            set_values = []
            params = []
            
            for col_name, col_config in reb_cols.items():
                source = col_config.get('team_source', {})
                field = source.get('field')
                if field and field in headers:
                    set_values.append(f"{col_name} = %s")
                    value = row[headers.index(field)] or 0
                    params.append(value)
            
            params.extend([team_id, season_year])
            
            if set_values:
                set_clause = ', '.join(set_values)
                cursor.execute(f"""
                    UPDATE team_season_stats
                    SET {set_clause}, updated_at = NOW()
                    WHERE team_id = %s AND year = %s AND season_type = 1
                """, params)
        
        conn.commit()
        log("  Rebounding committed")
        time.sleep(RATE_LIMIT_DELAY)
        
        # 4. HUSTLE STATS - Config-driven
        hustle_cols = get_columns_by_endpoint('leaguehustlestatsteam', 'team')
        
        log(f"Fetching team hustle stats for {season}...")
        result = retry_api_call(
            lambda timeout: leaguehustlestatsteam.LeagueHustleStatsTeam(
                season=season,
                per_mode_time='Totals',
                season_type_all_star='Regular Season',
                timeout=timeout
            ).get_dict(),
            "Team hustle stats",
            use_timeout_param=True
        )
        
        for rs in result['resultSets']:
            if rs['name'] == 'HustleStatsTeam':
                headers = rs['headers']
                
                for row in rs['rowSet']:
                    team_id = row[headers.index('TEAM_ID')]
                    
                    # Build dynamic SET clause from config
                    set_values = []
                    params = []
                    
                    for col_name, col_config in hustle_cols.items():
                        source = col_config.get('team_source', {})
                        field = source.get('field')
                        if field and field in headers:
                            set_values.append(f"{col_name} = %s")
                            value = row[headers.index(field)] or 0
                            params.append(value)
                    
                    params.extend([team_id, season_year])
                    
                    if set_values:
                        set_clause = ', '.join(set_values)
                        cursor.execute(f"""
                            UPDATE team_season_stats
                            SET {set_clause}, updated_at = NOW()
                            WHERE team_id = %s AND year = %s AND season_type = 1
                        """, params)
        
        conn.commit()
        log("  Hustle stats committed")
        time.sleep(RATE_LIMIT_DELAY)
        
        # 5. DEFENSE STATS (3 calls) - pass connection to avoid deadlock
        update_team_defense_stats(season, season_year, conn=conn, cursor=cursor)
        conn.commit()  # Commit defense stats before next section
        log("  Defense stats committed")
        
        # 6. PUTBACKS (30 calls - one per team)
        update_team_putbacks(season, season_year)
        
        log("Team advanced stats updated successfully")
        
    except Exception as e:
        log(f"Failed team advanced stats: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_team_opponent_tracking(season=None, season_year=None):
    """
    Fetch opponent tracking stats for teams (opp_* columns)
    This mirrors team advanced stats but for opponent performance
    
    Maps to: opp_open_rim_fgm/fga, opp_cont_rim_fgm/fga, opp_touches, etc.
    """
    if season is None:
        season = NBA_CONFIG['current_season']
        season_year = NBA_CONFIG['current_season_year']
    
    if season_year < 2013:
        log("SKIP - Opponent tracking data not available before 2013-14 season")
        return
    
    log(f"Fetching team opponent tracking stats for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashplayerptshot
        
        # Note: NBA API doesn't have dedicated "opponent" endpoints for teams
        # Opponent stats are typically derived from defensive matchup data
        # For now, we'll fetch league-wide opponent stats and aggregate them
        
        # 1. Opponent shooting tracking (6 calls, same as player shooting)
        log("  Fetching opponent shooting tracking...")
        player_data = {}
        
        # Contested rim (0-2 ft + 2-4 ft)
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashplayerptshot.LeagueDashPlayerPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                general_range='Restricted Area',
                close_def_dist_range=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[headers.index('TEAM_ID')]
                fgm = row[headers.index('FGM')] or 0
                fga = row[headers.index('FGA')] or 0
                
                if team_id not in player_data:
                    player_data[team_id] = {}
                player_data[team_id]['opp_cont_rim_fgm'] = player_data[team_id].get('opp_cont_rim_fgm', 0) + fgm
                player_data[team_id]['opp_cont_rim_fga'] = player_data[team_id].get('opp_cont_rim_fga', 0) + fga
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Note: Full implementation would require all 6 calls like player shooting
        # For now, we're demonstrating the pattern
        # This is commented out to avoid excessive API calls in this demonstration
        
        log("  SKIP - Opponent tracking stats require defensive matchup data not available in current endpoints")
        log("  Skipping opponent advanced tracking for now")
        
    except Exception as e:
        log(f"Failed team opponent tracking: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_player_advanced_stats(season=None, season_year=None):
    """
    FULLY CONFIG-DRIVEN ADVANCED STATS ETL
    Automatically discovers which endpoints to call from DB_COLUMNS.
    No hardcoding - adding new stats only requires config updates.
    
    Total time: ~8-10 minutes per season
    """
    if season is None:
        season = NBA_CONFIG['current_season']
        season_year = NBA_CONFIG['current_season_year']
    
    # Skip if before 2013-14 (tracking data not available)
    if season_year < 2013:
        log("SKIP - Tracking data not available before 2013-14 season")
        return
    
    log("=" * 70)
    log("STEP 4: Updating Player Advanced Stats (CONFIG-DRIVEN)")
    log("=" * 70)
    start_time = time.time()
    
    try:
        # PHASE 1: Discover and fetch league-wide endpoints from config
        log("Discovering league-wide advanced tracking endpoints from config...")
        
        # Advanced/tracking endpoints only (exclude basic stats handled in STEP 2)
        ADVANCED_ENDPOINTS = {
            'leaguedashptstats',      # Playmaking, possessions, speed/distance, rebounding
            'leaguehustlestatsplayer', # Hustle stats
            'leaguedashptdefend',      # Defense tracking
        }
        
        # Group endpoints by (endpoint_name, params)
        endpoint_calls = {}
        
        for col_name, col_config in DB_COLUMNS.items():
            player_source = col_config.get('player_source', {})
            endpoint = player_source.get('endpoint')
            
            # Skip non-advanced endpoints
            if endpoint not in ADVANCED_ENDPOINTS:
                continue
            
            # Skip non-player columns
            if 'player' not in col_config.get('applies_to_entities', []):
                continue
            
            # Build unique key for this endpoint call
            pt_measure = player_source.get('pt_measure_type')
            if pt_measure:
                key = (endpoint, pt_measure)
            else:
                key = (endpoint, None)
            
            if key not in endpoint_calls:
                endpoint_calls[key] = []
            endpoint_calls[key].append(col_name)
        
        # Execute each unique endpoint call
        log(f"Found {len(endpoint_calls)} league-wide endpoint calls to make")
        for (endpoint, measure_type), cols in sorted(endpoint_calls.items()):
            params = {'pt_measure_type': measure_type} if measure_type else {}
            desc = f"{endpoint} ({measure_type or 'default'}) - {', '.join(cols[:3])}{'...' if len(cols) > 3 else ''}"
            
            update_stats_from_league_endpoint(
                endpoint_name=endpoint,
                endpoint_params=params,
                season=season,
                season_year=season_year,
                description=desc
            )
        
        # PHASE 2: Per-player endpoints (special handling required)
        # These require custom logic so can't use generic helper
        log("\nStarting per-player advanced stats (shooting tracking, defense, putbacks)...")
        update_shooting_tracking_bulk(season, season_year)   # ~480 players × 6 calls = 2880 calls
        update_defense_stats_bulk(season, season_year)       # 3 league-wide calls
        update_putbacks_per_player(season, season_year, skip_on_backfill=False)  # ~480 players × 1 call
        
        # PHASE 3: Team-based on/off ratings
        update_onoff_stats(season, season_year)              # 30 teams × 1 call
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"ADVANCED STATS COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        
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
    
    global _overall_pbar, _group_pbar
    
    # If backfill requested, process multiple seasons
    if backfill_start:
        current_year = NBA_CONFIG['current_season_year']
        end_year = backfill_end or current_year
        num_seasons = end_year - backfill_start + 1
        
        log(f"Backfill: Processing {num_seasons} seasons from {backfill_start} to {end_year}")
        
        # Calculate total steps for progress tracking
        # For each season: player stats (2 types) + team stats (2 types) + advanced (if >= 2014)
        total_steps = 0
        for year in range(backfill_start, end_year + 1):
            total_steps += 2  # player stats (regular + playoffs)
            total_steps += 2  # team stats (regular + playoffs)
            if year >= 2014:
                total_steps += 2  # advanced stats (player + team)
        
        # Create progress bars for backfill
        _overall_pbar = tqdm(total=total_steps, desc="Backfill Progress", 
                            position=1, leave=True, unit="step",
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
        _group_pbar = tqdm(total=0, desc="Initializing...", 
                          position=0, leave=True, unit="op",
                          bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}]')
        
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
                _group_pbar.reset(total=2)
                _group_pbar.set_description(f"[{season}] Player Stats")
                update_player_stats(skip_zero_stats=True)
                
                # STEP 2: Team Stats (reuse existing function)
                _group_pbar.reset(total=2)
                _group_pbar.set_description(f"[{season}] Team Stats")
                update_team_stats()
                
                # STEP 3: Advanced stats (only for 2013-14 onwards)
                if year >= 2014:
                    _group_pbar.reset(total=2)
                    _group_pbar.set_description(f"[{season}] Advanced Stats")
                    log(f"==> Advanced Stats for {season}")
                    try:
                        update_player_advanced_stats(season, year)
                        update_team_advanced_stats(season, year)
                    except Exception as e:
                        log(f"    Failed advanced stats: {e}", "WARN")
                else:
                    log(f"==> Skipping advanced stats (pre-2013-14)")
                
                # Restore original config
                NBA_CONFIG['current_season'] = original_season
                NBA_CONFIG['current_season_year'] = original_year
                
                log(f"Season {season} complete")
                
            except Exception as e:
                log(f"Failed to process season {season}: {e}", "ERROR")
                import traceback
                log(traceback.format_exc(), "ERROR")
                # Restore config even on error
                NBA_CONFIG['current_season'] = original_season
                NBA_CONFIG['current_season_year'] = original_year
                continue
        
        # Close progress bars
        _group_pbar.close()
        _overall_pbar.close()
        _overall_pbar = None
        _group_pbar = None
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"BACKFILL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        return
    
    # Normal daily ETL (current season only)
    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()
        
        # Calculate total transactions across all steps for accurate progress
        # These numbers are approximate based on typical workload
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get player count for accurate progress calculation
        cursor.execute("SELECT COUNT(*) FROM players")
        player_count = cursor.fetchone()[0] or 480  # Default if empty
        
        # Get active player count (GP > 0) for shooting stats
        cursor.execute("""
            SELECT COUNT(*) FROM player_season_stats 
            WHERE year = %s AND season_type = 1 AND games_played > 0
        """, (NBA_CONFIG['current_season_year'],))
        active_count = cursor.fetchone()[0] or player_count
        
        cursor.close()
        conn.close()
        
        # Transaction estimates per step:
        # 1. Rosters: 30 teams + new players (estimate 0-5) + updates (1 bulk)
        # 2. Player stats: 2 season types (2 bulk operations)
        # 3. Team stats: 3 season types (3 bulk operations)
        # 4. Player advanced: 6 league-wide calls + 0 shooting (already current) + 30 onoff
        # 5. Team advanced: 8 shooting + 1 playmaking + 1 rebounding + 1 hustle + 3 defense + 30 putbacks
        
        rosters_tx = 35  # Will adjust after Step 1
        player_stats_tx = 2
        team_stats_tx = 3
        player_advanced_tx = 36  # 6 league-wide + 30 onoff (shooting skipped if current)
        team_advanced_tx = 44  # 8 shooting + 3 other + 3 defense + 30 putbacks
        
        total_transactions = rosters_tx + player_stats_tx + team_stats_tx + player_advanced_tx + team_advanced_tx
        
        # Create two progress bars that stay at the bottom
        # position=0 for step (top), position=1 for overall (bottom)
        _overall_pbar = tqdm(total=total_transactions, desc="Overall ETL Progress", 
                            position=1, leave=True, unit="tx", 
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        _group_pbar = tqdm(total=0, desc="Initializing...", 
                          position=0, leave=True, unit="op",
                          bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        
        # STEP 1: Player Rosters
        _group_pbar.reset(total=rosters_tx)
        _group_pbar.set_description("STEP 1: Player Rosters")
        players_added, players_updated = update_player_rosters()
        
        # Adjust overall progress based on actual new players (not estimate)
        actual_rosters_tx = 30 + players_added + 1  # teams + new players + bulk update
        adjustment = actual_rosters_tx - rosters_tx
        if adjustment != 0:
            adjust_overall_progress(total_transactions + adjustment)
            total_transactions += adjustment
        
        # STEP 2: Player Stats
        _group_pbar.reset(total=player_stats_tx)
        _group_pbar.set_description("STEP 2: Player Stats")
        update_player_stats()
        
        # STEP 3: Team Stats
        _group_pbar.reset(total=team_stats_tx)
        _group_pbar.set_description("STEP 3: Team Stats")
        update_team_stats()
        
        # STEP 4: Player Advanced Stats
        # Get actual player count for shooting tracking progress
        check_conn = get_db_connection()
        check_cursor = check_conn.cursor()
        check_cursor.execute("""
            SELECT COUNT(DISTINCT player_id) FROM player_season_stats
            WHERE year = %s AND season_type = 1
        """, (NBA_CONFIG['current_season_year'],))
        shooting_players = check_cursor.fetchone()[0] or 0
        
        # Get player count for putbacks (only those with games_played > 0)
        check_cursor.execute("""
            SELECT COUNT(*) FROM player_season_stats
            WHERE year = %s AND season_type = 1 AND games_played > 0
        """, (NBA_CONFIG['current_season_year'],))
        putback_players = check_cursor.fetchone()[0] or 0
        
        check_cursor.close()
        check_conn.close()
        
        # Calculate actual transactions for Step 4:
        # 5 league-wide calls (playmaking x2, rebounding, hustle, defense)
        # + shooting_players (per-player shooting tracking)
        # + putback_players (per-player putbacks)
        # + 30 (onoff per team)
        actual_player_advanced_tx = 5 + shooting_players + putback_players + 30
        
        # Adjust overall progress to reflect actual player counts
        adjustment = actual_player_advanced_tx - player_advanced_tx
        if adjustment != 0:
            adjust_overall_progress(total_transactions + adjustment)
            total_transactions += adjustment
        
        _group_pbar.reset()
        _group_pbar.total = actual_player_advanced_tx
        _group_pbar.refresh()
        _group_pbar.set_description("STEP 4: Player Advanced Stats")
        
        update_player_advanced_stats()
        
        # STEP 5: Team Advanced Stats
        _group_pbar.reset(total=team_advanced_tx)
        _group_pbar.set_description("STEP 5: Team Advanced Stats")
        update_team_advanced_stats()
        
        # Close progress bars
        _group_pbar.close()
        _overall_pbar.close()
        _overall_pbar = None
        _group_pbar = None
        
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
# These run once per year on August 1st to maintain database hygiene
# and update player biographical data (height, weight, birthdate)


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
    log("=" * 70)
    log("STEP 1: Cleaning up inactive players")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    current_year = NBA_CONFIG['current_season_year']
    
    # First, log how many total players we have
    cursor.execute("SELECT COUNT(*) FROM players")
    total_before = cursor.fetchone()[0]
    log(f"Total players in database: {total_before}")
    
    # Find players with NO RECORD AT ALL in the last 2 seasons
    # This matches the daily ETL logic: if on roster, you get a record (even with games_played=0)
    # So we only delete players who have NO record (not just zero games)
    cursor.execute("""
        SELECT p.player_id, p.name 
        FROM players p
        WHERE NOT EXISTS (
            SELECT 1 FROM player_season_stats s
            WHERE s.player_id = p.player_id
            AND s.year >= %s
        )
    """, (current_year - 1,))  # Last 2 seasons: current_year and current_year-1
    
    players_to_delete = cursor.fetchall()
    
    if players_to_delete:
        log(f"Found {len(players_to_delete)} inactive players (no record in last 2 seasons)")
        
        # Delete them (will cascade to player_season_stats)
        player_ids_to_delete = tuple(p[0] for p in players_to_delete)
        cursor.execute("""
            DELETE FROM players 
            WHERE player_id IN %s
        """, (player_ids_to_delete,))
        
        deleted_count = cursor.rowcount
        log(f"Deleted {deleted_count} players and their historical stats")
        
        # Log remaining count
        cursor.execute("SELECT COUNT(*) FROM players")
        total_after = cursor.fetchone()[0]
        log(f"Players remaining in database: {total_after}")
        
        update_group_progress(deleted_count)
    else:
        log("No inactive players to remove")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return len(players_to_delete) if players_to_delete else 0


def update_all_player_details(name_range=None):
    """
    Fetch height, weight, birthdate for ALL players in the database.
    This is the SLOW operation (~16 minutes for 640 players).
    Only runs once per year on August 1st.
    
    NOTE: This updates ALL players in database (historical + current).
    Daily ETL only fetches details for NEW players on rosters.
    
    Args:
        name_range: Optional tuple ('A', 'J') or ('K', 'Z') to split into batches
    """
    log("=" * 70)
    if name_range:
        log(f"STEP 2: Updating player details (names {name_range[0]}-{name_range[1]})")
    else:
        log("STEP 2: Updating player details for all players")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all players in database (optionally filtered by name range)
    if name_range:
        start_letter, end_letter = name_range
        cursor.execute("""
            SELECT player_id, name FROM players 
            WHERE UPPER(SUBSTRING(name, 1, 1)) >= %s 
            AND UPPER(SUBSTRING(name, 1, 1)) <= %s
            ORDER BY name
        """, (start_letter.upper(), end_letter.upper()))
    else:
        cursor.execute("SELECT player_id, name FROM players ORDER BY player_id")
    
    all_players = cursor.fetchall()
    
    total_players = len(all_players)
    log(f"Found {total_players} total players in database to update")
    
    updated_count = 0
    failed_count = 0
    consecutive_failures = 0
    retry_queue = []  # Players that failed all attempts - retry at end
    
    for idx, (player_id, player_name) in enumerate(all_players):
        # Take regular breaks every 50 players
        if idx > 0 and idx % 50 == 0:
            consecutive_failures = 0
        
        # If we're seeing failures, take emergency break
        if consecutive_failures >= 3:
            log("WARNING - Taking 2min break after 3 consecutive failures", "WARN")
            time.sleep(120)
            consecutive_failures = 0
        
        # Try to fetch details with exponential backoff
        for attempt in range(3):
            try:
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=20)
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
                if attempt >= 2:
                    failed_count += 1
                    retry_queue.append((player_id, player_name))
        
        update_group_progress(1)
    
    # Retry failed players at the end (one more attempt each)
    if retry_queue:
        log(f"\n  Retrying {len(retry_queue)} failed players...")
        for player_id, player_name in retry_queue:
            try:
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=20)
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


def run_annual_etl(name_range=None):
    """
    Main annual ETL orchestrator.
    Runs once per year on August 1st.
    
    Args:
        name_range: Optional tuple ('A', 'J') or ('K', 'Z') to split into batches
    """
    log("=" * 70)
    if name_range:
        log(f"THE GLASS - ANNUAL ETL (Players {name_range[0]}-{name_range[1]})")
    else:
        log("THE GLASS - ANNUAL ETL STARTED")
    log("=" * 70)
    start_time = time.time()
    
    global _overall_pbar, _group_pbar
    
    try:
        # Calculate total transactions
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if name_range:
            start_letter, end_letter = name_range
            cursor.execute("""
                SELECT COUNT(*) FROM players 
                WHERE UPPER(SUBSTRING(name, 1, 1)) >= %s 
                AND UPPER(SUBSTRING(name, 1, 1)) <= %s
            """, (start_letter.upper(), end_letter.upper()))
        else:
            cursor.execute("SELECT COUNT(*) FROM players")
        
        player_count = cursor.fetchone()[0]
        
        cleanup_tx = player_count if not name_range else 0
        details_tx = player_count
        
        total_tx = cleanup_tx + details_tx
        
        cursor.close()
        conn.close()
        
        # Create progress bars (step on top, overall on bottom)
        _overall_pbar = tqdm(total=total_tx, desc="Overall Annual ETL", 
                            position=1, leave=True, unit="tx",
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
        _group_pbar = tqdm(total=0, desc="Initializing...", 
                         position=0, leave=True, unit="op",
                         bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
        
        # Step 1: Cleanup inactive players (only on first run)
        if not name_range:
            _group_pbar.reset(total=cleanup_tx)
            _group_pbar.set_description("STEP 1: Cleanup")
            deleted_count = cleanup_inactive_players()
            
            # Adjust overall progress based on actual deletions
            actual_cleanup_tx = deleted_count
            adjustment = actual_cleanup_tx - cleanup_tx
            if adjustment != 0:
                log(f"Adjusting progress: cleanup needed {deleted_count} vs estimated {cleanup_tx}")
                adjust_overall_progress(total_tx + adjustment)
                total_tx += adjustment
                # Also adjust details_tx since we deleted players
                details_tx -= deleted_count
        else:
            deleted_count = 0
            log("Skipping cleanup (only runs on first batch)")
        
        # Step 2: Update height, weight, birthdate for all remaining players
        _group_pbar.reset(total=details_tx)
        _group_pbar.set_description("STEP 2: Player Details")
        updated_count, failed_count = update_all_player_details(name_range)
        
        # Close progress bars
        _group_pbar.close()
        _overall_pbar.close()
        _overall_pbar = None
        _group_pbar = None
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"ANNUAL ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        if not name_range:
            log(f"  Deleted: {deleted_count} inactive players")
        log(f"  Updated: {updated_count} players")
        if failed_count > 0:
            log(f"  Failed: {failed_count} players")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"ANNUAL ETL FAILED - {elapsed:.1f}s", "ERROR")
        log(f"Error: {e}", "ERROR")
        log("=" * 70)
        raise


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
    mode_group.add_argument('--annual', action='store_true', help='Run annual ETL (cleanup + player details)')
    mode_group.add_argument('--backfill', type=int, help='Backfill from this year (e.g., 2020 for 2019-20 season)')
    
    # Annual-specific options
    parser.add_argument('--name-range', choices=['A-J', 'K-Z'], help='Annual only: Process players in this name range')
    parser.add_argument('--year', type=int, help='Annual only: Specific year to process')
    
    # Backfill options
    parser.add_argument('--end', type=int, help='Backfill only: End year (defaults to current season)')
    
    # General options
    parser.add_argument('--no-check', action='store_true', help='Skip missing data check')
    
    args = parser.parse_args()
    
    # Route to appropriate ETL mode
    if args.annual:
        # ANNUAL ETL MODE
        name_range = None
        if args.name_range == 'A-J':
            name_range = ('A', 'J')
        elif args.name_range == 'K-Z':
            name_range = ('K', 'Z')
        
        # If year specified, update NBA_CONFIG for that season
        if args.year:
            NBA_CONFIG['current_season_year'] = args.year
            NBA_CONFIG['current_season'] = f"{args.year-1}-{str(args.year)[-2:]}"
            log(f"Processing season: {NBA_CONFIG['current_season']} (year={args.year})")
        
        run_annual_etl(name_range)
        
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

