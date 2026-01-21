"""
The Glass ETL - Library Module

Reusable utilities, helpers, decorators, and query functions.
Extracted from config to maintain separation: lib = code, config = data.
"""
import time
import functools
import psycopg2
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Literal, Tuple, Callable

# Import all config data at module level (no circular dependency)
from config.etl import (
    TABLES_CONFIG, ENDPOINTS_CONFIG, DB_COLUMNS,
    RETRY_CONFIG, API_CONFIG, DB_CONFIG, DB_OPERATIONS, NBA_CONFIG
)


# ============================================================================
# TYPE CONVERTERS & PARSERS
# ============================================================================

def safe_int(value: Any, scale: int = 1) -> Optional[int]:
    """Convert value to scaled integer, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return None
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return None


def safe_float(value: Any, scale: int = 1) -> Optional[int]:
    """Convert value to scaled float (as integer), handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return None
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return None


def safe_str(value: Any) -> Optional[str]:
    """Safely convert to string"""
    if value is None or value == '' or (hasattr(value, '__len__') and len(value) == 0):
        return None
    return str(value)


def parse_height(height_str: Any) -> int:
    """Parse height from NBA API format to inches"""
    if not height_str or height_str == '' or height_str == 'None':
        return 0
    try:
        if '-' in str(height_str):
            feet, inches = str(height_str).split('-')
            return int(feet) * 12 + int(inches)
        else:
            return int(float(height_str))
    except (ValueError, AttributeError):
        return 0


def parse_birthdate(date_str: Any) -> Optional[date]:
    """Parse birthdate string to date"""
    if not date_str or date_str == '' or str(date_str).lower() == 'nan':
        return None
    try:
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                return datetime.strptime(str(date_str).split('.')[0], fmt).date()
            except Exception:
                continue
        return None
    except Exception:
        return None


def format_season(from_year: Any) -> Optional[str]:
    """Convert FROM_YEAR (e.g., 2012) to season format (e.g., 2012-13)"""
    if from_year is None or from_year == '' or str(from_year).lower() == 'nan':
        return None
    try:
        year = int(from_year)
        next_year = year + 1
        return f"{year}-{str(next_year)[-2:]}"
    except (ValueError, TypeError):
        return None


def execute_transform(value: Any, transform_name: str, scale: int = 1) -> Any:
    """Execute a named transform function on a value."""
    transform_functions = {
        'safe_int': lambda v, s: safe_int(v, scale=s),
        'safe_float': lambda v, s: safe_float(v, scale=s),
        'safe_str': lambda v, s: safe_str(v),
        'parse_height': lambda v, s: parse_height(v),
        'parse_birthdate': lambda v, s: parse_birthdate(v),
        'format_season': lambda v, s: format_season(v)
    }
    
    if transform_name not in transform_functions:
        raise ValueError(f"Unknown transform: {transform_name}")
    
    return transform_functions[transform_name](value, scale)


# ============================================================================
# CONFIG LOOKUP HELPERS
# ============================================================================

def infer_execution_tier_from_endpoint(endpoint_name: str) -> str:
    """Infer execution tier from endpoint name pattern."""
    endpoint_lower = endpoint_name.lower()
    
    # Check player before team to avoid misclassifying teamplayer* endpoints
    if endpoint_lower.startswith('playerdash') or endpoint_lower.startswith('commonplayer'):
        return 'player'
    
    if endpoint_lower.startswith('team'):
        return 'team'
    
    if endpoint_lower.startswith('league'):
        return 'league'
    
    return 'league'


def get_primary_key(entity: Literal['player', 'team']) -> str:
    """Get primary key field name for entity."""
    PRIMARY_KEYS = {
        'player': 'player_id',
        'team': 'team_id'
    }
    return PRIMARY_KEYS.get(entity, 'id')


def get_table_name(entity: Literal['player', 'team'], contents: Literal['entity', 'stats'] = 'stats') -> str:
    """Get table name for entity and content type."""
    for table_name, config in TABLES_CONFIG.items():
        if config['entity'] == entity and config['contents'] == contents:
            return table_name
    
    raise ValueError(f"No table found for entity='{entity}' and contents='{contents}'")


def get_stats_table_names() -> List[str]:
    """Get list of stats table names."""
    return [name for name, config in TABLES_CONFIG.items() if config['contents'] == 'stats']


def get_entity_table_names() -> List[str]:
    """Get list of entity table names."""
    return [name for name, config in TABLES_CONFIG.items() if config['contents'] == 'entity']


def get_composite_keys() -> List[str]:
    """Get composite key field names."""
    return ['year', 'season_type']


def get_all_key_fields(entity: Literal['player', 'team']) -> List[str]:
    """Get all key fields (primary + composite)."""
    return [get_primary_key(entity)] + get_composite_keys()


def get_entity_id_field(entity: Literal['player', 'team']) -> str:
    """Get NBA API field name for entity ID."""
    API_FIELD_NAMES = {
        'player': 'PLAYER_ID',
        'team': 'TEAM_ID'
    }
    return API_FIELD_NAMES.get(entity, 'ID')


def get_entity_name_field(entity: Literal['player', 'team']) -> str:
    """Get NBA API field name for entity name."""
    API_FIELD_NAMES = {
        'player': 'PLAYER_NAME',
        'team': 'TEAM_NAME'
    }
    return API_FIELD_NAMES.get(entity, 'NAME')


def get_endpoint_config(endpoint_name: str) -> Dict[str, Any]:
    """Get configuration for a specific endpoint."""
    return ENDPOINTS_CONFIG.get(endpoint_name, {})


def is_endpoint_available_for_season(
    endpoint_name: str, 
    season: str,
    params: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Check if an endpoint has data available for a given season.
    
    Args:
        endpoint_name: Name of the endpoint
        season: Season string (e.g., '2013-14')
        params: Optional parameters to check for parameter-specific minimum seasons
                (e.g., {'measure_type_detailed_defense': 'Advanced'})
    
    Returns:
        True if endpoint is available for this season with these parameters
    """
    config = get_endpoint_config(endpoint_name)
    min_season = config.get('min_season')
    
    if min_season is None:
        return True  # Available for all seasons
    
    min_year = int('20' + min_season.split('-')[1])
    season_year = int('20' + season.split('-')[1])
    
    return season_year >= min_year


# ============================================================================
# DATABASE QUERY HELPERS
# ============================================================================

def get_teams_from_db(db_config: Optional[Dict[str, Any]] = None) -> Dict[int, Tuple[str, str]]:
    """Fetch teams from database."""
    if db_config is None:
        db_config = DB_CONFIG
    
    conn = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )
    cursor = conn.cursor()
    cursor.execute("SELECT team_id, team_abbr, team_name FROM teams ORDER BY team_id")
    teams = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return teams


def get_editable_fields() -> List[str]:
    """Get list of user-editable fields from DB_COLUMNS config."""
    editable = []
    
    for col_name, col_meta in DB_COLUMNS.items():
        # Skip if col_meta is not a dict (defensive programming)
        if not isinstance(col_meta, dict):
            continue
        if col_meta.get('table') not in ['entity', 'both']:
            continue
            
        if not col_meta.get('nullable', False):
            continue
        
        is_non_api = not col_meta.get('api', False)
        is_annual = col_meta.get('update_frequency') == 'annual'
        
        if is_non_api or is_annual:
            editable.append(col_name)
    
    return editable


# ============================================================================
# COLUMN QUERY FUNCTIONS
# ============================================================================

def get_columns_by_endpoint(
    endpoint_name: str,
    entity: Literal['player', 'team', 'opponent'],
    table: Optional[str] = None,
    pt_measure_type: Optional[str] = None,
    measure_type_detailed_defense: Optional[str] = None,
    defense_category: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Get all columns that source from a specific endpoint.
    Filters by endpoint, entity, table, and parameter type if specified.
    """
    # Default to stats table for entity if not specified
    if table is None:
        if entity == 'player':
            table = get_table_name('player', 'stats')
        elif entity == 'team':
            table = get_table_name('team', 'stats')
        else:
            table = 'unknown'
    
    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
    matched_cols = {}
    
    # Get table type from config
    stats_tables = get_stats_table_names()
    entity_tables = get_entity_table_names()
    
    for col_name, col_meta in DB_COLUMNS.items():
        # Skip if col_meta is not a dict (defensive programming)
        if not isinstance(col_meta, dict):
            continue
        # Check if column belongs to the target table
        # 'table' field is a category, not actual table name:
        # - 'entity': ID/name/bio columns (ONLY in players/teams tables)
        # - 'both': columns in both entity and stats tables (e.g., player_id, team_id)
        # - 'stats': stat columns (ONLY in season_stats tables)
        col_table = col_meta.get('table')
        if table in stats_tables:
            # For stats tables, accept 'both' and 'stats' (NOT 'entity')
            if col_table not in ['both', 'stats']:
                continue
        elif table in entity_tables:
            # For entity tables, accept 'both' and 'entity'
            if col_table not in ['both', 'entity']:
                continue
        elif col_table != table:
            # For other tables, require exact match
            continue
        
        # Get source configuration
        source = col_meta.get(source_key)
        if not source:
            continue
        
        # Skip columns with string sources (config references like NBA_CONFIG["current_season"])
        if isinstance(source, str):
            continue
        
        # Check if endpoint matches
        if source.get('endpoint') != endpoint_name:
            continue
        
        # Filter by parameter type if specified
        # Check both direct parameters and nested params dict
        if pt_measure_type:
            source_pt = source.get('pt_measure_type') or source.get('params', {}).get('pt_measure_type')
            if source_pt != pt_measure_type:
                continue
        
        if measure_type_detailed_defense:
            source_measure = source.get('measure_type_detailed_defense') or source.get('params', {}).get('measure_type_detailed_defense')
            if source_measure != measure_type_detailed_defense:
                continue
        
        if defense_category:
            source_defense = source.get('defense_category') or source.get('params', {}).get('defense_category')
            if source_defense != defense_category:
                continue
        
        matched_cols[col_name] = col_meta
    
    return matched_cols



def get_opponent_columns() -> Dict[str, Dict[str, Any]]:
    """Get opponent-specific columns."""
    return {
        col_name: col_meta
        for col_name, col_meta in DB_COLUMNS.items()
        if isinstance(col_meta, dict) and 'opponent_source' in col_meta
    }


def get_column_list_for_insert(entity: Literal['player', 'team'] = 'player', include_opponent: bool = False) -> List[str]:
    """
    Get ordered list of column names for database INSERT.
    Excludes transformation-only columns.
    """
    table = get_table_name(entity, contents='stats')
    
    columns = []
    for col_name, col_meta in DB_COLUMNS.items():
        # Skip if col_meta is not a dict (defensive programming)
        if not isinstance(col_meta, dict):
            continue
        if col_meta.get('table') != table:
            continue
        
        # Include if column has entity source or opponent source (if requested)
        entity_source_key = f'{entity}_source'
        has_entity_source = col_meta.get(entity_source_key) is not None
        has_opponent_source = include_opponent and col_meta.get('opponent_source') is not None
        
        if has_entity_source or has_opponent_source:
            columns.append(col_name)
    
    return columns


# ============================================================================
# SCHEMA DDL GENERATION
# ============================================================================

def generate_schema_ddl() -> str:
    """
    Generate complete database schema DDL from DB_COLUMNS.
    Builds all CREATE TABLE statements with proper columns, constraints, and indexes.
    """
    # Define table metadata (primary keys, foreign keys, indexes)
    # Get table names from config
    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    player_stats_table = get_table_name('player', 'stats')
    team_stats_table = get_table_name('team', 'stats')
    
    table_metadata = {
        teams_table: {
            'primary_key': 'team_id',
            'additional_columns': [
                'team_name VARCHAR(100)',
                'team_abbr VARCHAR(3)',
                'team_city VARCHAR(100)',
                'year_founded INTEGER',
                'arena VARCHAR(100)',
                'owner VARCHAR(100)',
                'general_manager VARCHAR(100)',
                'head_coach VARCHAR(100)'
            ],
            'indexes': ['team_abbr', 'team_name']
        },
        players_table: {
            'primary_key': 'player_id',
            'additional_columns': [],
            'indexes': ['player_name', 'team_id', '(first_name, last_name)']
        },
        player_stats_table: {
            'primary_key': '(player_id, year, season_type)',
            'additional_columns': [],
            'foreign_keys': [
                f'FOREIGN KEY (player_id) REFERENCES {players_table}(player_id)',
                f'FOREIGN KEY (team_id) REFERENCES {teams_table}(team_id)'
            ],
            'indexes': ['player_id', 'team_id', 'year', 'season_type', '(player_id, year)', '(team_id, year)']
        },
        team_stats_table: {
            'primary_key': '(team_id, year, season_type)',
            'additional_columns': [],
            'foreign_keys': [
                f'FOREIGN KEY (team_id) REFERENCES {teams_table}(team_id)'
            ],
            'indexes': ['team_id', 'year', 'season_type', '(team_id, year)']
        }
    }
    
    ddl_statements = []
    
    # Generate CREATE TABLE for each table
    for table_name, metadata in table_metadata.items():
        ddl_statements.append(f"-- {table_name.upper()} TABLE")
        ddl_statements.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
        
        column_defs = []
        
        # Add table-specific columns from DB_COLUMNS
        stats_tables = get_stats_table_names()
        for col_name, col_meta in DB_COLUMNS.items():
            # Skip if col_meta is not a dict (defensive programming)
            if not isinstance(col_meta, dict):
                continue
            if col_meta.get('table') == table_name or (
                table_name in stats_tables and
                col_meta.get('table') == 'both'
            ):
                col_type = col_meta.get('type', 'INTEGER')  # Get type from column metadata
                nullable = 'NULL' if col_meta.get('nullable', True) else 'NOT NULL'
                column_defs.append(f"  {col_name} {col_type} {nullable}")
        
        # Add additional columns not in DB_COLUMNS
        for additional_col in metadata.get('additional_columns', []):
            column_defs.append(f"  {additional_col}")
        
        # Add primary key constraint
        pk = metadata['primary_key']
        column_defs.append(f"  PRIMARY KEY {pk}")
        
        # Add foreign key constraints
        for fk in metadata.get('foreign_keys', []):
            column_defs.append(f"  {fk}")
        
        ddl_statements.append(',\n'.join(column_defs))
        ddl_statements.append(");\n")
        
        # Add indexes
        for idx in metadata.get('indexes', []):
            if idx.startswith('('):
                # Composite index
                idx_name = f"idx_{table_name}_{'_'.join(idx.strip('()').replace(' ', '').split(','))}"
                ddl_statements.append(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} {idx};")
            else:
                # Single column index
                ddl_statements.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idx} ON {table_name} ({idx});")
        
        ddl_statements.append("")
    
    return '\n'.join(ddl_statements)


def generate_create_table_ddl() -> str:
    """Generate CREATE TABLE DDL (alias for generate_schema_ddl for compatibility)."""
    return generate_schema_ddl()


# ============================================================================
# DECORATORS: Standardized Error Handling & Rate Limiting
# ============================================================================

def with_retry(
    max_retries: Optional[int] = None,
    backoff_base: Optional[float] = None,
    timeout: Optional[int] = None,
    endpoint_name: Optional[str] = None
) -> Callable:
    """
    Decorator for automatic retry with exponential backoff.
    
    WHY: Unifies all retry patterns into one config-driven decorator.
    Replaces: retry_api_call(), ParallelAPIExecutor._execute_with_retry()
    
    Args:
        max_retries: Override RETRY_CONFIG['max_retries'] (default: 3)
        backoff_base: Override RETRY_CONFIG['backoff_base'] (default: 10)
        timeout: Override API_CONFIG['timeout_default'] (default: 60)
        endpoint_name: Optional endpoint name to lookup endpoint-specific config
        
    Usage:
        @with_retry(max_retries=5)
        def fetch_data(timeout=60):
            return endpoint.get_dict()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Resolve config values (endpoint-specific > decorator > global)
            _max_retries = max_retries
            _backoff_base = backoff_base
            _timeout = timeout
            
            # Check if endpoint has custom retry config
            if endpoint_name:
                endpoint_config = get_endpoint_config(endpoint_name)
                if endpoint_config and 'retry_config' in endpoint_config:
                    retry_cfg = endpoint_config['retry_config']
                    _max_retries = _max_retries or retry_cfg.get('max_retries')
                    _backoff_base = _backoff_base or retry_cfg.get('backoff_base')
                    _timeout = _timeout or retry_cfg.get('timeout')
            
            # Fall back to global config
            _max_retries = _max_retries or RETRY_CONFIG['max_retries']
            _backoff_base = _backoff_base or RETRY_CONFIG['backoff_base']
            _timeout = _timeout or API_CONFIG['timeout_default']
            
            # Inject timeout if function accepts it
            func_accepts_timeout = 'timeout' in func.__code__.co_varnames
            if func_accepts_timeout and 'timeout' not in kwargs:
                kwargs['timeout'] = _timeout
            
            # Retry loop with exponential backoff
            last_exception = None
            for attempt in range(_max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Build detailed error context
                    error_type = type(e).__name__
                    error_msg = str(e)
                    context_parts = []
                    
                    if endpoint_name:
                        context_parts.append(f"endpoint={endpoint_name}")
                    
                    # Extract key parameters if available
                    if kwargs:
                        key_params = {}
                        for key in ['season', 'team_id', 'player_id', 'season_type_all_star', 'measure_type_detailed_defense', 'pt_measure_type']:
                            if key in kwargs:
                                key_params[key] = kwargs[key]
                        if key_params:
                            context_parts.append(f"params={key_params}")
                    
                    # Diagnose error type
                    if 'Read timed out' in error_msg or 'timeout' in error_msg.lower():
                        diagnosis = "API timeout - server took too long to respond"
                        should_retry = True
                    elif 'Expecting value' in error_msg or 'JSONDecodeError' in error_type:
                        diagnosis = "Invalid JSON - likely HTML error page or empty response from NBA API"
                        # Don't retry JSON errors for per-player endpoints - usually means no data available
                        # Per-player endpoints: playerdash*, teamplayerdash*
                        is_per_player = endpoint_name and ('playerdash' in endpoint_name.lower() or 'teamplayerdash' in endpoint_name.lower())
                        should_retry = not is_per_player
                    elif 'ConnectionError' in error_type or 'ConnectionPool' in error_msg:
                        diagnosis = "Connection error - network or server issue"
                        should_retry = True
                    elif '400' in error_msg:
                        diagnosis = "Bad request - invalid parameter values or missing required parameter"
                        should_retry = False  # Don't retry bad requests
                    elif '404' in error_msg:
                        diagnosis = "Not found - endpoint or data doesn't exist for these parameters"
                        should_retry = False  # Don't retry 404s
                    elif '429' in error_msg or 'rate limit' in error_msg.lower():
                        diagnosis = "Rate limit exceeded - too many requests"
                        should_retry = True
                    elif 'TypeError' in error_type and 'unexpected keyword argument' in error_msg:
                        # For TypeError with unexpected keyword, show full message (not truncated)
                        # so we can see which parameter is the problem
                        diagnosis = error_msg
                        should_retry = False  # Don't retry parameter errors
                    else:
                        diagnosis = error_msg[:80] + ('...' if len(error_msg) > 80 else '')
                        should_retry = True
                    
                    context_str = ", " + ", ".join(context_parts) if context_parts else ""
                    
                    # Skip retry if error type indicates it won't help
                    if not should_retry:
                        msg = f"{func.__name__}{context_str} failed (no retry): {diagnosis}"
                        print(f"[INFO] {msg}")
                        break  # Exit retry loop
                    
                    if attempt < _max_retries - 1:
                        wait_time = _backoff_base * (attempt + 1)
                        msg = f"Attempt {attempt + 1}/{_max_retries} failed for {func.__name__}{context_str}: {diagnosis}, retrying in {wait_time}s..."
                        print(f"[WARN] {msg}")
                        time.sleep(wait_time)
                    else:
                        msg = f"All {_max_retries} attempts failed for {func.__name__}{context_str}: {diagnosis}"
                        print(f"[ERROR] {msg}")
            
            # All retries exhausted
            raise last_exception
        
        return wrapper
    return decorator


def create_api_call(
    endpoint_class: type,
    params: Dict[str, Any],
    endpoint_name: Optional[str] = None
) -> Callable:
    """
    Factory function to create a retry-wrapped API call with rate limiting.
    
    WHY: Centralizes API call pattern with retry logic and rate limiting.
    Rate limiting uses simple time.sleep() for sequential execution.
    
    Args:
        endpoint_class: NBA API endpoint class (e.g., LeagueDashPlayerStats)
        params: Parameters dict for the endpoint
        endpoint_name: Optional endpoint name for endpoint-specific config
    Returns:
        Callable that executes the API call with retry + rate limiting
        
    Usage:
        api_call = create_api_call(
            LeagueDashPlayerStats,
            {'season': '2024-25', 'timeout': 60},
            endpoint_name='leaguedashplayerstats'
        )
        result = api_call()
    """
    @with_retry(endpoint_name=endpoint_name)
    def _execute_api_call(timeout: Optional[int] = None) -> Any:
        """Execute the NBA API endpoint call."""
        # Apply rate limiting before API call
        time.sleep(API_CONFIG['rate_limit_delay'])
        
        # Merge timeout into params if provided
        call_params = params.copy()
        if timeout is not None:
            call_params['timeout'] = timeout
        
        # Remove internal flags that are not API parameters
        call_params.pop('_convert_per_game', None)
        call_params.pop('_games_field', None)
        
        # Try calling endpoint with all params
        # NBA API endpoints have inconsistent parameter support - some accept timeout/per_mode, others don't
        # This TypeError handling provides graceful fallback for any unexpected parameter
        try:
            return endpoint_class(**call_params).get_dict()
        except TypeError as e:
            # Some endpoints don't accept certain parameters
            # If we get a TypeError about unexpected keyword argument, identify which
            # parameter is causing the issue and retry without it
            error_msg = str(e)
            
            if 'unexpected keyword argument' in error_msg:
                # Extract the parameter name from error message
                # Error format: "...unexpected keyword argument 'param_name'"
                import re
                match = re.search(r"unexpected keyword argument '(\w+)'", error_msg)
                if match:
                    bad_param = match.group(1)
                    print(f"[INFO] Endpoint {endpoint_name or 'unknown'} doesn't accept '{bad_param}' parameter, retrying without it...")
                    # Remove from both dicts to prevent retry loops
                    call_params.pop(bad_param, None)
                    params.pop(bad_param, None)
                    return endpoint_class(**call_params).get_dict()
            
            # If we couldn't handle it, re-raise
            raise
    
    return _execute_api_call


# ============================================================================
# COLUMN VALUE EXTRACTION (NEW - eliminates ~320 lines of duplication)
# ============================================================================

def extract_column_value(
    row: Dict[str, Any],
    col_name: str,
    entity: Literal['player', 'team'],
    result_headers: List[str]
) -> Any:
    """
    Extract and transform a column value from an API result row.
    
    WHY: This function eliminates ~320 lines of duplicated extraction logic
    that appears in update_player_stats, update_team_stats, and
    update_transformation_columns (both player and team tiers).
    
    Args:
        row: API result row (list of values)
        col_name: Database column name
        entity: 'player' or 'team'
        result_headers: API result headers (column names)
        
    Returns:
        Transformed value ready for database insertion
        
    Usage:
        value = extract_column_value(
            row, 'GP', 'player', result_set.headers
        )
    """
    col_meta = DB_COLUMNS.get(col_name)
    if not col_meta:
        return None
    
    source_key = f'{entity}_source'
    source = col_meta.get(source_key, {})
    api_field = source.get('field')
    
    if not api_field or api_field not in result_headers:
        return None
    
    # Extract raw value from row
    field_idx = result_headers.index(api_field)
    raw_value = row[field_idx]
    
    # Apply transformation if specified
    transform = source.get('transform')
    scale = source.get('scale', 1)
    
    if transform:
        return execute_transform(raw_value, transform, scale)
    
    # Default: return raw value
    return raw_value


def extract_value_from_result(result: Dict[str, Any], transform: Dict[str, Any]) -> Any:
    """Extract a single value from API result based on transformation config.
    
    WHY: Pure utility function for extracting transformation values from API results.
    Handles simple_extract, arithmetic_subtract, and filter_aggregate patterns.
    
    Args:
        result: API result dict with 'resultSets' key
        transform: Transformation config dict with type and extraction parameters
        
    Returns:
        Extracted value based on transformation type
        
    Usage:
        value = extract_value_from_result(
            api_result,
            {'type': 'simple_extract', 'result_set': 'PlayerStats', 'field': 'PTS'}
        )
    """
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


# ============================================================================
# ERROR HANDLING
# ============================================================================

def handle_etl_error(e: Exception, operation_name: str, conn: Optional[Any] = None) -> None:
    """Standardized ETL error handling with rollback."""
    import traceback
    
    # Classify error for better diagnosis
    error_type = type(e).__name__
    error_msg = str(e)
    
    # Provide helpful diagnosis
    if 'Read timed out' in error_msg or 'timeout' in error_msg.lower():
        diagnosis = "API timeout - consider increasing timeout or checking NBA API status"
    elif 'Expecting value' in error_msg or 'JSONDecodeError' in error_type:
        diagnosis = "Invalid JSON response - NBA API may have returned error page. Check parameter values."
    elif '400' in error_msg:
        diagnosis = "Bad request - check if parameter values are valid for this season/endpoint"
    elif '404' in error_msg:
        diagnosis = "Not found - data may not exist for these parameters (e.g., wrong season range)"
    elif 'ConnectionError' in error_type:
        diagnosis = "Network error - check internet connection"
    else:
        diagnosis = f"{error_type}: {error_msg[:120]}"
    
    print(f"❌ ERROR - Failed {operation_name}: {diagnosis}")
    
    # Only show full traceback if it's not a common NBA API error
    if error_type not in ['JSONDecodeError', 'ReadTimeout', 'ConnectionError']:
        print(traceback.format_exc())
    
    if conn:
        conn.rollback()
        print("⚠️  Rolled back transaction - continuing ETL")



# ============================================================================
# DATABASE UTILITIES
# ============================================================================

def quote_column(col_name: str) -> str:
    """Quote column names that need quoting (start with digit or have special chars)."""
    if col_name[0].isdigit() or not col_name.replace('_', '').isalnum():
        return f'"{col_name}"'
    return col_name


def get_db_connection() -> Any:
    """Get PostgreSQL database connection with ETL-specific settings."""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        application_name='the_glass_etl',
        options=f'-c statement_timeout={DB_OPERATIONS["statement_timeout_ms"]}'
    )


def get_season() -> str:
    """Get current season"""
    return NBA_CONFIG['current_season']


def get_season_year() -> int:
    """Get season year (ending year of season)."""
    season = get_season()
    return int('20' + season.split('-')[1])




def get_columns_for_null_cleanup(
    season: str,
    entity: Literal['player', 'team'] = 'player'
) -> List[str]:
    """
    Get list of numeric stat columns that should have NULL→0 conversion for a given season.
    
    Only includes columns where:
    1. Column is a numeric stat column
    2. Column's source endpoint has no min_season OR season >= min_season
    
    This prevents converting NULLs to 0s for columns where data wasn't available
    (e.g., tracking stats before 2013-14 should stay NULL).
    
    Args:
        season: Season string (e.g., '2024-25')
        entity: Entity type ('player' or 'team')
        
    Returns:
        List of column names that should have NULL→0 conversion
    """
    season_year = int('20' + season.split('-')[1])
    source_key = f'{entity}_source' if entity in ['player', 'team'] else 'source'
    
    eligible_columns = []
    
    for col_name, col_def in DB_COLUMNS.items():
        # Skip if col_def is not a dict (defensive programming)
        if not isinstance(col_def, dict):
            continue
            
        # Must be a stats table column
        if col_def.get('table') != 'stats':
            continue
            
        # Must be numeric type
        col_type = col_def.get('type', '')
        if not col_type.startswith(('INTEGER', 'SMALLINT', 'BIGINT', 'FLOAT', 'REAL', 'NUMERIC')):
            continue
            
        # Skip games (never convert)
        if col_name == 'games':
            continue
            
        # Check if column has a source endpoint with min_season
        source = col_def.get(source_key, {})
        if not source:
            # No source = always eligible (likely a derived/calculated field)
            eligible_columns.append(col_name)
            continue
            
        endpoint_name = source.get('endpoint')
        if not endpoint_name:
            # No endpoint = always eligible
            eligible_columns.append(col_name)
            continue
            
        # Get endpoint config to check min_season
        endpoint_config = get_endpoint_config(endpoint_name)
        min_season = endpoint_config.get('min_season')
        
        if min_season is None:
            # No min_season restriction = always eligible
            eligible_columns.append(col_name)
        else:
            # Check if current season >= min_season
            min_year = int('20' + min_season.split('-')[1])
            if season_year >= min_year:
                # Season has data for this column
                eligible_columns.append(col_name)
            # else: season < min_season, keep NULLs (don't add to list)
    
    return eligible_columns


def _is_column_available_for_season(col_name: str, season: str) -> bool:
    """
    Check if a column is available for a given season (respects min_season).
    
    Args:
        col_name: Column name to check
        season: Season string (e.g., '2023-24')
    
    Returns:
        True if column should have data for this season
    """
    if col_name not in DB_COLUMNS:
        return False
    
    col_config = DB_COLUMNS[col_name]
    player_source = col_config.get('player_source') or col_config.get('team_source')
    
    if not player_source:
        return False
    
    # Get endpoint config to check min_season
    endpoint_name = player_source.get('endpoint')
    if not endpoint_name:
        return False
    
    endpoint_config = get_endpoint_config(endpoint_name)
    min_season = endpoint_config.get('min_season')
    
    if min_season is None:
        return True
    
    # Compare season years
    season_year = int('20' + season.split('-')[1])
    min_year = int('20' + min_season.split('-')[1])
    
    return season_year >= min_year


# ============================================================================
# BACKFILL TRACKER UTILITIES
# ============================================================================

def get_endpoint_processing_order(include_team_endpoints: bool = False) -> List[str]:
    """
    Get ordered list of endpoints to process for backfill.
    Order: league-wide → team-by-team → player-by-player.
    
    Args:
        include_team_endpoints: If True, includes team-specific endpoints.
                                If False, filters them out (for player backfill).
    
    Returns:
        List of endpoint names in processing order
    """
    league_endpoints = []
    team_endpoints = []
    player_endpoints = []
    
    for endpoint_name in ENDPOINTS_CONFIG.keys():
        # Infer tier from endpoint name pattern
        tier = infer_execution_tier_from_endpoint(endpoint_name)
        
        # Filter team-specific endpoints unless explicitly requested
        if not include_team_endpoints:
            if 'team' in endpoint_name.lower() and 'teamplayer' not in endpoint_name.lower():
                continue
        
        if tier == 'league':
            league_endpoints.append(endpoint_name)
        elif tier == 'team':
            team_endpoints.append(endpoint_name)
        elif tier == 'player':
            player_endpoints.append(endpoint_name)
    
    # Process league-wide first, then teams, then players
    return league_endpoints + team_endpoints + player_endpoints


def get_endpoint_parameter_combinations(endpoint_name: str, entity: Literal['player', 'team'] = 'player') -> List[Dict[str, Any]]:
    """
    Extract all unique parameter combinations needed for an endpoint from DB_COLUMNS config.
    
    For endpoints like leaguedashptstats that require parameters (pt_measure_type),
    this discovers all unique parameter sets by scanning which columns use which parameters.
    
    Args:
        endpoint_name: Name of the endpoint (e.g., 'leaguedashptstats')
        entity: Entity type ('player' or 'team')
        
    Returns:
        List of parameter dictionaries, e.g.:
        [
            {'pt_measure_type': 'Possessions'},
            {'pt_measure_type': 'Passing'},
            {'pt_measure_type': 'SpeedDistance'}
        ]
        Empty list if endpoint doesn't require parameters or has no columns configured.
    """
    import json
    
    source_key = f'{entity}_source'
    param_combinations = []
    
    # Scan all columns to find which ones use this endpoint
    for col_name, col_meta in DB_COLUMNS.items():
        if not isinstance(col_meta, dict):
            continue
            
        source_config = col_meta.get(source_key)
        if not source_config:
            continue
            
        # Check direct endpoint reference
        col_endpoint = None
        params = {}
        
        if isinstance(source_config, str):
            # Simple string reference: column uses this endpoint with no special params
            col_endpoint = source_config
        elif isinstance(source_config, dict):
            col_endpoint = source_config.get('endpoint')
            params = source_config.get('params', {})
            
            # Also check transformation if present
            transform = source_config.get('transformation')
            if transform:
                if not col_endpoint:
                    col_endpoint = transform.get('endpoint')
                # Check for endpoint_params in transformation (used for pipeline transforms)
                transform_endpoint_params = transform.get('endpoint_params', {})
                if transform_endpoint_params:
                    params = {**params, **transform_endpoint_params}
                # Also merge transformation params
                transform_params = transform.get('params', {})
                if transform_params:
                    params = {**params, **transform_params}
        
        # Skip if not this endpoint
        if col_endpoint != endpoint_name:
            continue
        
        # Extract relevant parameters (exclude internal ones like _convert_per_game)
        relevant_params = {k: v for k, v in params.items() if not k.startswith('_')}
        
        # Check if we've already seen this parameter combination
        param_str = json.dumps(relevant_params, sort_keys=True)
        if not any(json.dumps(p, sort_keys=True) == param_str for p in param_combinations):
            param_combinations.append(relevant_params)
    
    # If no parameter combinations found, return single empty dict (default params)
    if not param_combinations:
        param_combinations = [{}]
    
    return param_combinations


def get_columns_for_endpoint_params(endpoint_name: str, params: Dict[str, Any], entity: Literal['player', 'team'] = 'player') -> List[str]:
    """
    Get list of column names that will be populated by this endpoint+params combination.
    
    Args:
        endpoint_name: Name of the endpoint (e.g., 'leaguedashptstats')
        params: Parameter dictionary (e.g., {'pt_measure_type': 'Possessions'})
        entity: Entity type ('player' or 'team')
        
    Returns:
        List of column names that use this exact endpoint+params combination
    """
    import json
    
    source_key = f'{entity}_source'
    matching_columns = []
    
    # Normalize params for comparison
    params_normalized = json.dumps(params or {}, sort_keys=True)
    
    for col_name, col_meta in DB_COLUMNS.items():
        if not isinstance(col_meta, dict):
            continue
            
        source_config = col_meta.get(source_key)
        if not source_config or not isinstance(source_config, dict):
            continue
            
        # Check endpoint
        col_endpoint = source_config.get('endpoint')
        col_params = source_config.get('params', {})
        
        # Also check in transformation if present
        transform = source_config.get('transformation')
        if transform:
            if not col_endpoint:
                col_endpoint = transform.get('endpoint')
            # Check for endpoint_params in transformation (used for pipeline transforms)
            transform_endpoint_params = transform.get('endpoint_params', {})
            if transform_endpoint_params:
                col_params = {**col_params, **transform_endpoint_params}
            # Also check regular params in transformation
            transform_params = transform.get('params', {})
            if transform_params:
                col_params = {**col_params, **transform_params}
        
        if col_endpoint != endpoint_name:
            continue
        
        # Remove internal params (starting with _)
        col_params = {k: v for k, v in col_params.items() if not k.startswith('_')}
        col_params_normalized = json.dumps(col_params, sort_keys=True)
        
        if col_params_normalized == params_normalized:
            matching_columns.append(col_name)
    
    return matching_columns


def get_backfill_status(endpoint: str, season: str, season_type: int, params: Optional[Dict[str, Any]] = None, entity: str = 'player') -> Optional[Dict[str, Any]]:
    """
    Get backfill status for a specific endpoint/season/season_type/params/entity combination.
    
    Args:
        endpoint: Endpoint name
        season: Season string (e.g., '2024-25')
        season_type: Season type code (1=Regular, 2=Playoffs, 3=PlayIn)
        params: Parameter dictionary (e.g., {'pt_measure_type': 'Possessions'})
        entity: 'player' or 'team' to distinguish which stats table
        
    Returns:
        Dict with status info, or None if no tracker record exists
    """
    import json
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Convert params to JSON string for comparison (normalized)
    params_json = json.dumps(params or {}, sort_keys=True)
    
    cursor.execute("""
        SELECT endpoint, year, season_type, params, player_successes, players_total,
               team_successes, teams_total, updated_at, status, entity
        FROM backfill_endpoint_tracker
        WHERE endpoint = %s AND year = %s AND season_type = %s AND params = %s AND entity = %s
    """, (endpoint, season, season_type, params_json, entity))
    
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not row:
        return None
    
    return {
        'endpoint': row[0],
        'year': row[1],
        'season_type': row[2],
        'params': json.loads(row[3]) if row[3] else {},
        'player_successes': row[4],
        'players_total': row[5],
        'team_successes': row[6],
        'teams_total': row[7],
        'updated_at': row[8],
        'status': row[9],
        'entity': row[10]
    }


def update_backfill_status(
    endpoint: str,
    season: str,
    season_type: int,
    status: str,
    player_successes: int = 0,
    players_total: int = 0,
    team_successes: int = 0,
    teams_total: int = 0,
    params: Optional[Dict[str, Any]] = None,
    entity: str = 'player'
) -> None:
    """
    Update or insert backfill tracker record.
    
    Args:
        endpoint: Endpoint name
        season: Season string
        season_type: Season type code
        status: Status value (pending, in_progress, complete, failed)
        player_successes: Number of successful player calls
        players_total: Total players to process
        team_successes: Number of successful team calls
        teams_total: Total teams to process
        params: Parameter dictionary (e.g., {'pt_measure_type': 'Possessions'})
    """
    import json
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Convert params to JSON string (normalized)
    params_json = json.dumps(params or {}, sort_keys=True)
    
    cursor.execute("""
        INSERT INTO backfill_endpoint_tracker 
        (endpoint, year, season_type, params, entity, player_successes, players_total,
         team_successes, teams_total, status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (endpoint, year, season_type, params, entity)
        DO UPDATE SET
            player_successes = EXCLUDED.player_successes,
            players_total = EXCLUDED.players_total,
            team_successes = EXCLUDED.team_successes,
            teams_total = EXCLUDED.teams_total,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
    """, (endpoint, season, season_type, params_json, entity, player_successes, players_total,
          team_successes, teams_total, status))
    
    conn.commit()
    cursor.close()
    conn.close()


def mark_players_backfilled(player_ids: List[int]) -> None:
    """
    Mark players as fully backfilled (all endpoints for all seasons complete).
    
    Args:
        player_ids: List of player IDs to mark as backfilled
    """
    if not player_ids:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE players
        SET backfilled = TRUE
        WHERE player_id = ANY(%s)
    """, (player_ids,))
    
    conn.commit()
    cursor.close()
    conn.close()


def calculate_current_season() -> str:
    """
    Calculate current NBA season dynamically based on current date.
    
    Season starts in October, so:
    - Jan-Sep: Current year is the END year (2025 → '2024-25')
    - Oct-Dec: Current year is the START year (2024 → '2024-25')
    
    Returns:
        Season string (e.g., '2024-25')
    """
    from datetime import datetime
    
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    if current_month >= 10:  # Oct-Dec: start of new season
        start_year = current_year
    else:  # Jan-Sep: continuation of season
        start_year = current_year - 1
    
    end_year = start_year + 1
    return f"{start_year}-{str(end_year)[-2:]}"


def get_player_ids_for_season(season: str, season_type: int) -> List[Tuple[int, int]]:
    """
    Get list of (player_id, team_id) tuples for players who have stats in a season.
    Uses current team_id from players table for per-player API calls.
    
    Note: For traded players, TEAM_ID from NBA API represents their LAST team that season.
    For playoffs, this is always correct (no mid-playoff trades). For regular season,
    per-team endpoints already aggregate across all teams a player played for.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    player_stats_table = get_table_name('player', 'stats')
    players_table = get_table_name('player', 'entity')
    
    # Get player IDs from stats with current team_id from players table
    # Use 0 as fallback for free agents
    cursor.execute(f"""
        SELECT DISTINCT pss.player_id, COALESCE(p.team_id, 0) as team_id
        FROM {player_stats_table} pss
        LEFT JOIN {players_table} p ON pss.player_id = p.player_id
        WHERE pss.year = %s AND pss.season_type = %s
        ORDER BY pss.player_id
    """, (season, season_type))
    
    player_team_ids = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return player_team_ids


def get_active_teams() -> List[int]:
    """
    Get list of active NBA team IDs.
    
    Returns:
        List of team IDs (30 teams)
    """
    from config.etl import TEAM_IDS
    return list(TEAM_IDS.values())


# ============================================================================
# ENDPOINT UTILITIES
# ============================================================================


def get_endpoint_class(endpoint_name: str) -> type:
    """
    Dynamically import and return NBA API endpoint class.
    
    Args:
        endpoint_name: Name of endpoint (e.g., 'leaguedashplayerstats')
        
    Returns:
        Endpoint class type
        
    Raises:
        AttributeError: If endpoint class not found
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


def build_endpoint_params(
    endpoint_name: str,
    season: str,
    season_type_name: str,
    entity: Literal['player', 'team'] = 'player',
    custom_params: Optional[Dict[str, Any]] = None,
    col_sources: Optional[List[Dict]] = None
) -> Dict[str, Any]:
    """
    Build standardized parameters for NBA API endpoints dynamically using ENDPOINTS_CONFIG.
    Automatically selects correct parameter names and validates entity types.
    
    Args:
        endpoint_name: Name of the endpoint (e.g., 'leaguedashptstats', 'leaguehustlestatsteam')
        season: Season string (e.g., '2024-25')
        season_type_name: Season type name (e.g., 'Regular Season')
        entity: 'player' or 'team' (for endpoints with player_or_team parameter)
        custom_params: Additional endpoint-specific parameters to merge
        col_sources: Optional list of column source configs to check for season-type-specific params
        
    Returns:
        Dict of parameters ready for endpoint call
        
    Raises:
        ValueError: If endpoint not configured or entity type not supported
    """
    from config.etl import API_CONFIG
    
    # Get endpoint configuration
    endpoint_config = get_endpoint_config(endpoint_name)
    if not endpoint_config:
        raise ValueError(f"Endpoint '{endpoint_name}' not found in ENDPOINTS_CONFIG")
    
    # Validate entity type
    entity_types = endpoint_config.get('entity_types', [])
    if entity not in entity_types:
        raise ValueError(
            f"Endpoint '{endpoint_name}' does not support entity type '{entity}'. "
            f"Supported types: {entity_types}"
        )
    
    # Get timeout - check endpoint-specific retry_config first, then fall back to default
    timeout = API_CONFIG['timeout_default']
    if 'retry_config' in endpoint_config and 'timeout' in endpoint_config['retry_config']:
        timeout = endpoint_config['retry_config']['timeout']
    
    # Start with base parameters
    params = {
        'season': season,
        'timeout': timeout
    }
    
    # Add season_type parameter using configured parameter name
    season_type_param = endpoint_config.get('season_type_param')
    if season_type_param:
        params[season_type_param] = season_type_name
    
    # Add per_mode parameter using configured parameter name
    per_mode_param = endpoint_config.get('per_mode_param')
    if per_mode_param:
        if per_mode_param == 'per_mode_simple':
            per_mode_value = API_CONFIG['per_mode_simple']
        elif per_mode_param == 'per_mode_detailed':
            per_mode_value = API_CONFIG['per_mode_detailed']
        elif per_mode_param == 'per_mode_time':
            per_mode_value = API_CONFIG['per_mode_time']
        else:
            per_mode_value = None
        
        if per_mode_value:
            params[per_mode_param] = per_mode_value
    
    # Add player_or_team parameter if endpoint supports both entities
    if 'player' in entity_types and 'team' in entity_types:
        params['player_or_team'] = API_CONFIG['player_or_team_player'] if entity == 'player' else API_CONFIG['player_or_team_team']
    
    # Add any required parameters from config
    requires_params = endpoint_config.get('requires_params', [])
    for param_name in requires_params:
        if param_name not in params and param_name in API_CONFIG:
            params[param_name] = API_CONFIG[param_name]
    
    # Merge custom parameters (overrides defaults if conflicts)
    if custom_params:
        params.update(custom_params)
        
    return params


# ============================================================================
# UNIFIED TRANSFORMATION ENGINE (New Config-Driven Approach)
# ============================================================================

def execute_transformation_pipeline(
    ctx: Any,
    pipeline_config: Dict[str, Any],
    season: str,
    entity: Literal['player', 'team'],
    season_type: int = 1,
    season_type_name: str = 'Regular Season'
) -> Dict[int, Any]:
    """
    Execute a pipeline of transformation operations defined in config.
    
    WHY: Replaces 6 separate _apply_* functions with one config-driven engine.
    Add new transform types by adding operations to config, not code.
    
    Args:
        ctx: ETL context with rate limiter, etc.
        pipeline_config: Transform config with 'operations' list
        season: Season string (e.g., '2024-25')
        entity: 'player' or 'team'
        season_type: Season type ID (1=Regular, 2=Playoffs, 3=PlayIn)
        season_type_name: Season type name string
        
    Returns:
        Dict mapping entity_id to transformed value
        
    Config Format:
        {
            'type': 'pipeline',
            'execution_tier': 'league|team|player',
            'endpoint': 'endpoint_name',
            'operations': [
                {'op': 'extract', 'result_set': 'X', 'field': 'Y'},
                {'op': 'scale', 'factor': 10},
                {'op': 'filter', 'field': 'Z', 'values': [...]},
                {'op': 'aggregate', 'method': 'sum', 'group_by': 'player_id'},
                ...
            ]
        }
    """
    endpoint_name = pipeline_config['endpoint']
    execution_tier = pipeline_config.get('execution_tier', 'league')
    operations = pipeline_config['operations']
    endpoint_params = pipeline_config.get('endpoint_params', {})
    
    # Step 1: Get raw API data based on execution tier
    if execution_tier == 'league':
        api_results = _fetch_api_data_league(ctx, endpoint_name, season, season_type_name, entity, custom_params=endpoint_params)
    elif execution_tier == 'team':
        api_results = _fetch_api_data_per_team(ctx, endpoint_name, season, season_type_name, entity, custom_params=endpoint_params)
    elif execution_tier == 'player':
        api_results = _fetch_api_data_per_player(ctx, endpoint_name, season, season_type_name, entity, custom_params=endpoint_params)
    else:
        raise ValueError(f"Unknown execution_tier: {execution_tier}")
    
    # Step 2: Execute operation pipeline on API results
    data = {}
    for operation in operations:
        # Config uses 'type' key, not 'op' key
        op_type = operation.get('type')
        
        if op_type == 'extract':
            data = _operation_extract(api_results, operation, entity)
        elif op_type == 'filter':
            data = _operation_filter(data, operation)
        elif op_type == 'aggregate':
            data = _operation_aggregate(data, operation)
        elif op_type == 'scale':
            data = _operation_scale(data, operation)
        elif op_type == 'subtract':
            data = _operation_subtract(api_results, operation, entity)
        elif op_type == 'multiply':
            data = _operation_multiply(data, operation)
        elif op_type == 'divide':
            data = _operation_divide(data, operation)
        elif op_type == 'weighted_avg':
            data = _operation_weighted_avg(data, operation)
        else:
            raise ValueError(f"Unknown operation type: {op_type}")
    
    return data


def _fetch_api_data_league(ctx: Any, endpoint_name: str, season: str, 
                           season_type_name: str, entity: str,
                           custom_params: Optional[Dict[str, Any]] = None) -> Any:
    """Fetch data from league-wide endpoint (single API call)."""
    EndpointClass = get_endpoint_class(endpoint_name)
    params = build_endpoint_params(endpoint_name, season, season_type_name, entity, custom_params=custom_params)
    
    api_call = create_api_call(
        EndpointClass,
        params,
        endpoint_name=endpoint_name
    )
    
    result = api_call()
    return result


def _fetch_api_data_per_team(ctx: Any, endpoint_name: str, season: str,
                             season_type_name: str, entity: str,
                             custom_params: Optional[Dict[str, Any]] = None) -> List[Any]:
    """Fetch data from per-team endpoint (30 API calls, one per team)."""
    from config.etl import TEAM_IDS
    
    EndpointClass = get_endpoint_class(endpoint_name)
    base_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, custom_params=custom_params)
    
    
    
    # Create cache key (exclude team_id and internal params)
    cache_params = {k: v for k, v in base_params.items() 
                   if not k.startswith('_') and k != 'team_id'}
    cache_key = (endpoint_name, season, season_type_name, frozenset(cache_params.items()))
    
    # Check cache first
    if hasattr(ctx, 'api_result_cache') and cache_key in ctx.api_result_cache:
        return ctx.api_result_cache[cache_key]
    
    team_ids = list(TEAM_IDS.values())
    
    results = []
    for idx, team_id in enumerate(team_ids):
        params = {**base_params, 'team_id': team_id}
        
        api_call = create_api_call(
            EndpointClass,
            params,
            endpoint_name=endpoint_name
        )
        
        try:
            result = api_call()
            results.append(result)
            
            # Apply per-call delay if configured (prevent rate limiting)
            endpoint_config = get_endpoint_config(endpoint_name)
            if endpoint_config and "retry_config" in endpoint_config:
                delay = endpoint_config["retry_config"].get("per_call_delay", 0)
                if delay > 0:
                    time.sleep(delay)
        except Exception as e:
            print(f"⚠️  Failed to fetch {endpoint_name} for team {team_id}: {e}")
    
    
    # Store in cache
    if hasattr(ctx, 'api_result_cache'):
        ctx.api_result_cache[cache_key] = results
    
    return results


def _fetch_api_data_per_player(ctx: Any, endpoint_name: str, season: str,
                               season_type_name: str, entity: str,
                               custom_params: Optional[Dict[str, Any]] = None) -> List[Any]:
    """Fetch data from per-player endpoint (hundreds/thousands of API calls)."""
    from config.etl import SEASON_TYPE_CONFIG
    
    # Get season_type code from name
    season_type = SEASON_TYPE_CONFIG.get(season_type_name, {}).get('season_code', 1)
    
    # Get all (player_id, team_id) tuples for players who played in this season
    player_team_ids = get_player_ids_for_season(season, season_type)
    
    if not player_team_ids:
        # Only print message if fetching for multiple players (daily ETL)
        # For single-player backfill, this is expected noise
        if len(player_team_ids) != 1:
            print(f"  No players found for {season} {season_type_name}")
        return []
    
    
    EndpointClass = get_endpoint_class(endpoint_name)
    base_params = build_endpoint_params(endpoint_name, season, season_type_name, entity, custom_params=custom_params)
    
    # Check if this endpoint accepts team_id parameter (some per-player endpoints don't)
    endpoint_config = get_endpoint_config(endpoint_name)
    accepts_team_id = endpoint_config.get('accepts_team_id', True)  # Default to True for backwards compatibility
    
    # Create cache key (exclude player_id, team_id and internal params)
    cache_params = {k: v for k, v in base_params.items() 
                   if not k.startswith('_') and k not in ('player_id', 'team_id')}
    cache_key = (endpoint_name, season, season_type_name, frozenset(cache_params.items()))
    
    # Check cache first
    if hasattr(ctx, 'api_result_cache') and cache_key in ctx.api_result_cache:
        return ctx.api_result_cache[cache_key]
    
    results = []
    failed_players = 0
    consecutive_failures = 0
    
    # Get restart configuration
    FAILURE_THRESHOLD = API_CONFIG.get('api_failure_threshold', 3)
    RESTART_ENABLED = API_CONFIG.get('api_restart_enabled', True)
    
    for idx, (player_id, team_id) in enumerate(player_team_ids):
        # Progress update every 50 players
        if idx > 0 and idx % 50 == 0:
            print(f"    Progress: {idx}/{len(player_team_ids)} players ({failed_players} failed)")
        
        # Only add team_id if endpoint accepts it (some per-player endpoints don't)
        # AND if the endpoint_params don't already specify a team_id override (e.g., team_id=0)
        params = {**base_params, 'player_id': player_id}
        if accepts_team_id:
            # Check if transformation config specifies a team_id override
            # If team_id is already in base_params (from endpoint_params), use that
            # Otherwise, use the season_team_id from the database
            if 'team_id' not in base_params:
                params['team_id'] = team_id
        
        api_call = create_api_call(
            EndpointClass,
            params,
            endpoint_name=endpoint_name
        )
        
        try:
            result = api_call()
            results.append(result)
            consecutive_failures = 0  # Reset on success
            
            # Note: Rate limiting already applied in create_api_call()
            
        except TypeError as e:
            # Parameter errors - don't retry, these are configuration issues
            failed_players += 1
            consecutive_failures += 1
            error_msg = str(e)
            if 'required positional argument' in error_msg or 'unexpected keyword argument' in error_msg:
                print(f"    ⚠️  Parameter error for player {player_id}: {error_msg}")
                # Log the actual parameters that were passed
                print(f"       Params passed: {list(params.keys())}")
            else:
                print(f"    ⚠️  Failed player {player_id}: {error_msg[:80]}")
            
            # AUTO-RESTART: If hit failure threshold, trigger subprocess restart
            if RESTART_ENABLED and consecutive_failures >= FAILURE_THRESHOLD:
                # Import here to avoid circular dependency
                import sys
                import os
                progress_msg = f"Progress: {idx}/{len(player_team_ids)} players processed in {endpoint_name} for {season}"
                print(f"\n🔄 AUTOMATIC RESTART TRIGGERED:")
                print(f"   Reason: Session exhaustion in {endpoint_name}")
                print(f"   Hit {consecutive_failures} consecutive failures (threshold: {FAILURE_THRESHOLD})")
                print(f"   {progress_msg}")
                print(f"   Restarting process to get fresh API session\n")
                print("   Executing subprocess restart...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
                
        except Exception as e:
            failed_players += 1
            consecutive_failures += 1
            # Don't print every failure - too noisy for hundreds of players
            if failed_players <= 5:  # Only show first 5 failures
                print(f"    ⚠️  Failed player {player_id}: {str(e)[:80]}")
            
            # AUTO-RESTART: If hit failure threshold, trigger subprocess restart
            if RESTART_ENABLED and consecutive_failures >= FAILURE_THRESHOLD:
                # Import here to avoid circular dependency
                import sys
                import os
                progress_msg = f"Progress: {idx}/{len(player_team_ids)} players processed in {endpoint_name} for {season}"
                print(f"\n🔄 AUTOMATIC RESTART TRIGGERED:")
                print(f"   Reason: Session exhaustion in {endpoint_name}")
                print(f"   Hit {consecutive_failures} consecutive failures (threshold: {FAILURE_THRESHOLD})")
                print(f"   {progress_msg}")
                print(f"   Restarting process to get fresh API session\n")
                print("   Executing subprocess restart...")
                os.execv(sys.executable, [sys.executable] + sys.argv)
    
    if failed_players > 5:
        print(f"    ⚠️  ... and {failed_players - 5} more player failures")
    
    # Store in cache
    if hasattr(ctx, 'api_result_cache'):
        if not hasattr(ctx, 'api_result_cache'):
            ctx.api_result_cache = {}
        ctx.api_result_cache[cache_key] = results
    
    return results


# ============================================================================
# PIPELINE OPERATION HANDLERS
# ============================================================================

def _operation_extract(api_results: Any, op_config: Dict[str, Any], 
                      entity: str) -> Dict[int, Any]:
    """
    Extract field(s) from API result set with optional filtering.
    
    Config: {
        'type': 'extract',
        'result_set': 'ResultSetName',
        'field': 'FIELD_NAME',  # Single field extraction
        OR
        'fields': {'alias1': 'API_FIELD1', 'alias2': 'API_FIELD2'},  # Multiple fields
        'player_id_field': 'CUSTOM_ID_FIELD',  # Optional: override entity ID field
        'filter': {'COLUMN_NAME': 'expected_value'}  # Optional: filter rows (dict format)
        'filter_field': 'COLUMN_NAME',  # Optional: filter rows (field + values format)
        'filter_values': ['value1', 'value2']  # Optional: list of acceptable values
    }
    """
    result_set_name = op_config['result_set']
    
    # Support both single field and multiple fields
    single_field = op_config.get('field')
    multi_fields = op_config.get('fields', {})
    
    if not single_field and not multi_fields:
        raise ValueError("extract operation requires either 'field' or 'fields' parameter")
    
    # Allow config to override entity_id_field (e.g., VS_PLAYER_ID instead of PLAYER_ID)
    entity_id_field = op_config.get('player_id_field') or get_entity_id_field(entity)
    
    # Support two filter formats:
    # 1. 'filter': {'COLUMN_NAME': 'expected_value'} - single exact match
    # 2. 'filter_field' + 'filter_values' - match any value in list
    filter_conditions = op_config.get('filter', {})
    filter_field = op_config.get('filter_field')
    filter_values = op_config.get('filter_values', [])
    
    data = {}
    
    # Handle single result (league-wide)
    if not isinstance(api_results, list):
        api_results = [api_results]
    
    # Extract from each result
    for idx, result in enumerate(api_results):
        # Result might be an endpoint object or already a dict from create_api_call
        if hasattr(result, 'get_dict'):
            result_dict = result.get_dict()
        else:
            result_dict = result
        
        result_set = result_dict['resultSets']
        
        # Find matching result set
        target_set = None
        for rs in result_set:
            if rs['name'] == result_set_name:
                target_set = rs
                break
            continue
        
        # Handle missing result set
        if target_set is None:
            available_sets = [rs['name'] for rs in result_set] if result_set else []
            raise ValueError(
                f"Result set '{result_set_name}' not found in API response. "
                f"Available result sets: {available_sets}"
            )
        
        headers = target_set['headers']
        rows = target_set['rowSet']
        
        # For per-player/per-team endpoints, entity ID might not be in result set
        # Instead, it's in the parameters of the API call (e.g., PlayerID, TeamID)
        if entity_id_field in headers:
            entity_idx = headers.index(entity_id_field)
            entity_from_params = None
        else:
            # Entity ID not in result set - get from parameters
            entity_idx = None
            entity_from_params = result_dict.get('parameters', {}).get('PlayerID' if entity == 'player' else 'TeamID')
            if entity_from_params is None:
                # Skip this result if we can't determine entity ID
                print(f"    ⚠️  Skipping result - no {entity_id_field} in headers or parameters")
                continue
        
        # Get field indices for single or multiple field extraction
        if single_field:
            if single_field not in headers:
                print(f"    ⚠️  Required field '{single_field}' not found in API response")
                print(f"    Available headers: {headers}")
                continue
            field_idx = headers.index(single_field)
            field_indices = None
        else:
            # Check if all required fields are available
            missing_fields = [api_field for api_field in multi_fields.values() if api_field not in headers]
            if missing_fields:
                print(f"    ⚠️  Required fields not found in API response: {missing_fields}")
                print(f"    Available headers: {headers}")
                continue
            field_indices = {alias: headers.index(api_field) for alias, api_field in multi_fields.items()}
            field_idx = None
        
        # Get filter column indices
        filter_indices = {}
        
        # Process dict-style filters
        for filter_col in filter_conditions.keys():
            if filter_col in headers:
                filter_indices[filter_col] = headers.index(filter_col)
        
        # Process field+values style filter
        if filter_field and filter_field in headers:
            filter_field_idx = headers.index(filter_field)
        else:
            filter_field_idx = None
        
        for row in rows:
            # Apply dict-style filters if specified
            if filter_conditions:
                skip_row = False
                for filter_col, expected_value in filter_conditions.items():
                    if filter_col in filter_indices:
                        actual_value = row[filter_indices[filter_col]]
                        if actual_value != expected_value:
                            skip_row = True
                            break
                if skip_row:
                    continue
            
            # Apply field+values style filter
            if filter_field_idx is not None and filter_values:
                actual_value = row[filter_field_idx]
                if actual_value not in filter_values:
                    continue
            
            # Get entity ID from result set or parameters
            if entity_idx is not None:
                entity_id = row[entity_idx]
            else:
                entity_id = entity_from_params
            
            # Extract single field or multiple fields
            if single_field:
                value = row[field_idx]
                # Aggregate if entity appears multiple times (traded players, or multiple filter matches)
                if entity_id in data:
                    data[entity_id] = (data[entity_id] or 0) + (value or 0)
                else:
                    data[entity_id] = value
            else:
                # Multiple fields - store as dict per entity
                if entity_id not in data:
                    data[entity_id] = {}
                for alias, idx in field_indices.items():
                    value = row[idx]
                    # Aggregate if entity appears multiple times
                    if alias in data[entity_id]:
                        data[entity_id][alias] = (data[entity_id][alias] or 0) + (value or 0)
                    else:
                        data[entity_id][alias] = value
    
    return data


def _operation_aggregate(data: Dict[int, Any], op_config: Dict[str, Any]) -> Dict[int, Any]:
    """
    Aggregate data (sum, avg, count, min, max).
    
    Config: {'op': 'aggregate', 'method': 'sum|avg|count|min|max', 'group_by': 'player_id'}
    """
    method = op_config['method']
    
    if method == 'sum':
        # Already aggregated by entity_id in extract
        return data
    elif method == 'avg':
        # Would need count to compute average - implement when needed
        return data
    elif method == 'count':
        # Count non-null values
        return {k: 1 for k, v in data.items() if v is not None}
    else:
        return data


def _operation_scale(data: Dict[int, Any], op_config: Dict[str, Any]) -> Dict[int, Any]:
    """
    Scale numeric values by a factor.
    
    Config: {'op': 'scale', 'factor': 10}
    """
    factor = op_config['factor']
    return {
        entity_id: (value * factor if value is not None else None)
        for entity_id, value in data.items()
    }

def _operation_filter(data: Dict[int, Any], op_config: Dict[str, Any]) -> Dict[int, Any]:
    """Filter operation - placeholder for future implementation."""
    # TODO: Implement filter operation
    return data

def _operation_multiply(data: Dict[int, Any], op_config: Dict[str, Any]) -> Dict[int, Any]:
    """
    Multiply two fields together.
    
    Config:
        fields: [field1, field2] - fields to multiply
        round: bool - whether to round result (default True)
    
    Example: Calculate total dribbles from touches * avg_drib_per_touch
    """
    fields = op_config.get('fields', [])
    should_round = op_config.get('round', True)
    
    if len(fields) != 2:
        raise ValueError(f"multiply operation requires exactly 2 fields, got {len(fields)}")
    
    field1, field2 = fields
    result = {}
    
    for entity_id, values in data.items():
        val1 = values.get(field1, 0)
        val2 = values.get(field2, 0)
        
        if val1 is None or val2 is None:
            result[entity_id] = None
        else:
            product = val1 * val2
            result[entity_id] = round(product) if should_round else product
    
    return result

def _operation_divide(data: Dict[int, Any], op_config: Dict[str, Any]) -> Dict[int, Any]:
    """Divide operation - placeholder for future implementation."""
    # TODO: Implement divide operation
    return data

def _operation_weighted_avg(data: Dict[int, Any], op_config: Dict[str, Any]) -> Dict[int, Any]:
    """Weighted average operation - placeholder for future implementation."""
    # TODO: Implement weighted_avg operation
    return data

def _operation_subtract(api_results: Any, op_config: Dict[str, Any], 
                       entity: str) -> Dict[int, Any]:
    """
    Subtract fields from API results, supporting multiple sources with formula.
    
    Simple Config (legacy):
        {'type': 'subtract', 'minuend': {...}, 'subtrahend': {...}}
    
    Complex Config (new):
        {
            'type': 'subtract',
            'sources': [
                {'result_set': 'X', 'filter': {'FIELD': 'value'}, 'field': 'A'},
                {'result_set': 'Y', 'filter': {'FIELD': 'value'}, 'field': 'B'},
                ...
            ],
            'formula': '(a + b) - (c + d)'  # Variables are a,b,c,d,... matching source order
        }
    """
    # Check if using new multi-source format
    if 'sources' in op_config and 'formula' in op_config:
        sources = op_config['sources']
        formula = op_config['formula']
        
        # Extract data from each source
        source_data = []
        for source in sources:
            extract_config = {'type': 'extract', **source}
            data = _operation_extract(api_results, extract_config, entity)
            source_data.append(data)
        
        # Get all entity IDs across all sources
        all_entities = set()
        for data in source_data:
            all_entities.update(data.keys())
        
        # Evaluate formula for each entity
        result = {}
        for entity_id in all_entities:
            # Create variables a, b, c, d, ... from source data
            variables = {}
            for i, data in enumerate(source_data):
                var_name = chr(ord('a') + i)  # a, b, c, d, ...
                variables[var_name] = data.get(entity_id, 0) or 0
            
            # Evaluate formula (e.g., "(a + b) - (c + d)")
            try:
                value = eval(formula, {"__builtins__": {}}, variables)
                result[entity_id] = int(value) if value is not None else 0
            except Exception as e:
                print(f"  ⚠️ Formula evaluation failed for entity {entity_id}: {e}")
                result[entity_id] = 0
        
        return result
    
    # Legacy format: simple A - B
    else:
        minuend_config = {'type': 'extract', **op_config['minuend']}
        subtrahend_config = {'type': 'extract', **op_config['subtrahend']}
        
        minuend_data = _operation_extract(api_results, minuend_config, entity)
        subtrahend_data = _operation_extract(api_results, subtrahend_config, entity)
        
        # Subtract: A - B
        result = {}
        all_entities = set(minuend_data.keys()) | set(subtrahend_data.keys())
        
        for entity_id in all_entities:
            a = minuend_data.get(entity_id, 0) or 0
            b = subtrahend_data.get(entity_id, 0) or 0
            result[entity_id] = a - b
        
        return result