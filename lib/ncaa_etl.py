"""
The Glass NCAA - CBBD API Client & ETL Library

Reusable utilities for the NCAA ETL pipeline:
- CBBD API client (raw requests with Bearer auth)
- Dot-notation JSON field extraction
- Transform functions (int_x10, int_x100, int_x1000, etc.)
- Database CRUD helpers (upsert entities, upsert stats)
- Schema DDL generation from config

Shares DB connection infrastructure with NBA (lib/nba_etl.py).
"""
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Tuple

from config.ncaa_etl import (
    DB_CONFIG, CBBD_API_CONFIG, CBBD_ENDPOINTS,
    NCAA_CONFIG, DB_COLUMNS, DB_SCHEMA, TABLES_CONFIG,
    get_table_name, get_columns_for_endpoint, get_stats_columns, get_entity_columns,
    season_to_display, season_int_to_display,
)
from lib.db import db_connection, get_db_connection, quote_col as _quote

logger = logging.getLogger(__name__)

# ============================================================================
# CBBD API CLIENT
# ============================================================================

class CBBDClient:
    """
    HTTP client for CollegeBasketballData.com API.

    Uses raw requests with Bearer token auth.
    Tracks call count for budget monitoring.
    """

    def __init__(self, api_key: str = None):
        self.base_url = CBBD_API_CONFIG['base_url']
        self.api_key = api_key or CBBD_API_CONFIG['api_key']
        self.delay = CBBD_API_CONFIG['rate_limit_delay']
        self.timeout = CBBD_API_CONFIG['timeout']
        self.max_retries = CBBD_API_CONFIG['max_retries']
        self.backoff_base = CBBD_API_CONFIG['backoff_base']
        self.call_count = 0
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json',
        })

    def get(self, path: str, params: dict = None) -> Optional[list]:
        """
        Make a GET request to the CBBD API.

        Returns parsed JSON (list of dicts) or None on failure.
        Implements retry with exponential backoff.
        """
        url = f'{self.base_url}{path}'
        for attempt in range(self.max_retries):
            try:
                if attempt > 0 or self.call_count > 0:
                    time.sleep(self.delay)

                response = self.session.get(url, params=params, timeout=self.timeout)
                self.call_count += 1

                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '')
                    if 'json' not in content_type:
                        logger.warning(f"Non-JSON response from {path}: {content_type}")
                        return None
                    return response.json()
                elif response.status_code == 429:
                    wait = self.backoff_base * (2 ** attempt)
                    logger.warning(f"Rate limited on {path}, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"API error {response.status_code} on {path}: {response.text[:200]}")
                    if response.status_code >= 500:
                        time.sleep(self.backoff_base * (attempt + 1))
                    else:
                        return None

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on {path} (attempt {attempt + 1}/{self.max_retries})")
                time.sleep(self.backoff_base * (attempt + 1))
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on {path}: {e}")
                time.sleep(self.backoff_base)

        logger.error(f"All {self.max_retries} retries exhausted for {path}")
        return None

    def fetch_endpoint(self, endpoint_name: str, season: int,
                       team_id: int = None,
                       extra_params: dict = None) -> Optional[list]:
        """
        Fetch data from a named CBBD endpoint for a given season.

        Some endpoints (e.g., roster) require a team_id parameter.
        """
        ep_config = CBBD_ENDPOINTS[endpoint_name]
        params = {'season': season}
        if team_id is not None:
            params['teamId'] = team_id
        if extra_params:
            params.update(extra_params)

        data = self.get(ep_config['path'], params=params)
        if data is None:
            logger.error(f"Failed to fetch {endpoint_name} for season {season}")
            return None

        logger.info(f"Fetched {len(data)} records from {endpoint_name} (season={season})")
        return data

    def fetch_teams_list(self) -> Optional[list]:
        """Fetch the full teams list (no season param needed)."""
        return self.get('/teams')

# ============================================================================
# JSON FIELD EXTRACTION (dot-notation support)
# ============================================================================

def extract_field(record: dict, field_path: str) -> Any:
    """
    Extract a value from a nested dict using dot-notation.

    Examples:
        extract_field(record, 'points')                     → record['points']
        extract_field(record, 'teamStats.points.total')     → record['teamStats']['points']['total']
        extract_field(record, 'winShares.offensive')         → record['winShares']['offensive']
    """
    parts = field_path.split('.')
    value = record
    for part in parts:
        if value is None or not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


# ============================================================================
# TRANSFORM FUNCTIONS
# ============================================================================

def transform_value(value: Any, transform: str) -> Any:
    """
    Apply a transform to an API value for DB storage.

    Transforms:
        'int'        → round to nearest integer
        'float'      → keep as float
        'str'        → string conversion
        'int_x10'    → multiply by 10, round to int
        'int_x100'   → multiply by 100, round to int
        'int_x1000'  → multiply by 1000, round to int
    """
    if value is None:
        return None

    try:
        if transform == 'int':
            return int(round(float(value)))
        elif transform == 'float':
            return float(value)
        elif transform == 'str':
            return str(value).strip() if value else None
        elif transform == 'int_x10':
            return int(round(float(value) * 10))
        elif transform == 'int_x100':
            return int(round(float(value) * 100))
        elif transform == 'int_x1000':
            return int(round(float(value) * 1000))
        else:
            logger.warning(f"Unknown transform '{transform}', returning raw value")
            return value
    except (ValueError, TypeError):
        return None


# ============================================================================
# DATA EXTRACTION: API Response → DB-ready dicts
# ============================================================================

def extract_entity_data(records: list, entity_type: str, endpoint_name: str) -> Dict[int, dict]:
    """
    Extract entity data (players or teams table) from API response.

    Returns: {entity_id: {col_name: value, ...}}
    """
    columns = get_columns_for_endpoint(endpoint_name, entity_type)
    ep_config = CBBD_ENDPOINTS[endpoint_name]
    id_field = ep_config['id_field']
    source_key = f'{entity_type}_source'

    result = {}
    for record in records:
        entity_id = record.get(id_field)
        if entity_id is None:
            continue

        row = {}
        for col_name, col_meta in DB_COLUMNS.items():
            src = col_meta.get(source_key)
            if src and src.get('endpoint') == endpoint_name and col_meta['table'] in ('entity', 'both'):
                raw_value = extract_field(record, src['field'])
                row[col_name] = transform_value(raw_value, src['transform'])

        if row:
            result[entity_id] = row

    return result


def extract_stats_data(records: list, entity_type: str, endpoint_name: str,
                       season: int, season_type_code: int = 1,
                       d1_team_ids: set = None) -> Dict[int, dict]:
    """
    Extract stats data from API response.

    Returns: {entity_id: {col_name: value, ...}} with 'season' and 'season_type' always included.
    season is stored as VARCHAR(10) display format (e.g., '2024-25').

    If d1_team_ids is provided, only records matching those team IDs are included
    (filters out non-D1 teams).
    """
    ep_config = CBBD_ENDPOINTS[endpoint_name]
    id_field = ep_config['id_field']
    source_key = f'{entity_type}_source'

    result = {}
    for record in records:
        # D1 filter: skip non-D1 teams/players
        if d1_team_ids is not None:
            record_team_id = record.get('teamId')
            if record_team_id and int(record_team_id) not in d1_team_ids:
                continue
        entity_id = record.get(id_field)
        if entity_id is None:
            continue

        row = {
            'season': season_int_to_display(season),
            'season_type': season_type_code,
        }

        # Capture team_id for player stats so we can JOIN to team_season_stats later
        if entity_type == 'player':
            raw_team_id = record.get('teamId')
            if raw_team_id is not None:
                row['team_id'] = int(raw_team_id)

        for col_name, col_meta in DB_COLUMNS.items():
            if col_meta['table'] != 'stats':
                continue
            src = col_meta.get(source_key)
            if src and src.get('endpoint') == endpoint_name:
                raw_value = extract_field(record, src['field'])
                row[col_name] = transform_value(raw_value, src['transform'])

        # Also extract opponent stats for team entity
        if entity_type == 'team':
            for col_name, col_meta in DB_COLUMNS.items():
                if col_meta['table'] != 'stats':
                    continue
                opp = col_meta.get('opp_source')
                if opp and opp.get('endpoint') == endpoint_name:
                    raw_value = extract_field(record, opp['field'])
                    row[f'opp_{col_name}'] = transform_value(raw_value, opp['transform'])

        if len(row) > 2:  # More than just season/season_type
            result[entity_id] = row

    return result


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def _quote(col: str) -> str:
    """Quote a column name for PostgreSQL (handles names starting with digits)."""
    return f'"{col}"'


def upsert_entities(conn, entity_type: str, data: Dict[int, dict]) -> int:
    """
    Upsert entity records (players or teams).

    Uses INSERT ... ON CONFLICT DO UPDATE for atomic upserts.
    Returns number of rows affected.
    """
    if not data:
        return 0

    table = get_table_name(entity_type, 'entity')
    id_col = 'player_id' if entity_type == 'player' else 'team_id'

    # Collect all column names from the data
    all_cols = set()
    for row in data.values():
        all_cols.update(row.keys())
    all_cols.discard(id_col)  # ID is added separately
    cols = sorted(all_cols)

    col_list = ', '.join([_quote(id_col)] + [_quote(c) for c in cols])
    placeholders = ', '.join(['%s'] * (1 + len(cols)))
    update_clause = ', '.join(
        f'{_quote(c)} = EXCLUDED.{_quote(c)}'
        for c in cols
    )

    sql = f"""
    INSERT INTO {table} ({col_list})
    VALUES ({placeholders})
    ON CONFLICT ({_quote(id_col)}) DO UPDATE SET {update_clause}
    """

    rows = []
    for entity_id, row in data.items():
        values = tuple([entity_id] + [row.get(c) for c in cols])
        rows.append(values)

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {table} ({col_list}) VALUES %s "
            f"ON CONFLICT ({_quote(id_col)}) DO UPDATE SET {update_clause}",
            rows,
            page_size=500
        )
    conn.commit()

    logger.info(f"Upserted {len(rows)} {entity_type} entities into {table}")
    return len(rows)


def upsert_stats(conn, entity_type: str, data: Dict[int, dict]) -> int:
    """
    Upsert stats records.

    Primary key: (player_id/team_id, season, season_type).
    Uses INSERT ... ON CONFLICT DO UPDATE.
    Returns number of rows affected.
    """
    if not data:
        return 0

    table = get_table_name(entity_type, 'stats')
    id_col = 'player_id' if entity_type == 'player' else 'team_id'

    # Collect all column names
    all_cols = set()
    for row in data.values():
        all_cols.update(row.keys())
    all_cols.discard(id_col)
    all_cols.discard('season')
    all_cols.discard('season_type')
    # Filter to only known DB columns + opp_ prefixed columns
    valid_stats = set(get_stats_columns())
    # Also allow opp_ prefixed columns and team_id (a 'both' column needed for player stats JOINs)
    cols = sorted([c for c in all_cols if c in valid_stats or c.startswith('opp_') or c == 'team_id'])

    key_cols = [_quote(id_col), _quote('season'), _quote('season_type')]
    col_list = ', '.join(key_cols + [_quote(c) for c in cols])
    placeholders = ', '.join(['%s'] * (3 + len(cols)))
    update_clause = ', '.join(
        f'{_quote(c)} = EXCLUDED.{_quote(c)}'
        for c in cols
    )

    sql = f"""
    INSERT INTO {table} ({col_list})
    VALUES ({placeholders})
    ON CONFLICT ({_quote(id_col)}, {_quote('season')}, {_quote('season_type')}) DO UPDATE SET {update_clause}
    """

    rows = []
    with conn.cursor() as cur:
        for entity_id, row in data.items():
            season = row.get('season')
            season_type = row.get('season_type')
            if season is None or season_type is None:
                continue
            values = tuple([entity_id, season, season_type] + [row.get(c) for c in cols])
            rows.append(values)

        execute_values(
            cur,
            f"INSERT INTO {table} ({col_list}) VALUES %s "
            f"ON CONFLICT ({_quote(id_col)}, {_quote('season')}, {_quote('season_type')}) DO UPDATE SET {update_clause}",
            rows,
            page_size=500
        )
    conn.commit()
    count = len(rows)

    logger.info(f"Upserted {count} {entity_type} stats rows into {table}")
    return count


# ============================================================================
# SCHEMA DDL GENERATION
# ============================================================================

def generate_schema_ddl() -> str:
    """
    Generate complete NCAA database schema DDL from DB_COLUMNS config.

    Creates schema-qualified tables: ncaa.teams, ncaa.players,
    ncaa.player_season_stats, ncaa.team_season_stats.
    Uses season VARCHAR(10) + season_type SMALLINT as composite key with entity ID.
    """
    teams_table = get_table_name('team', 'entity')
    players_table = get_table_name('player', 'entity')
    pss_table = get_table_name('player', 'stats')
    tss_table = get_table_name('team', 'stats')

    ddl = []

    ddl.append(f"-- CREATE SCHEMA")
    ddl.append(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA};\n")

    def _col_def(col: str, meta: dict) -> str:
        nullable = 'NULL' if meta['nullable'] else 'NOT NULL'
        default = f" DEFAULT {meta['default']}" if meta.get('default') else ''
        return f"  {_quote(col)} {meta['type']} {nullable}{default}"

    # ---- teams ----
    ddl.append("-- NCAA TEAMS (entity table)")
    ddl.append(f"CREATE TABLE IF NOT EXISTS {teams_table} (")
    team_cols = ["  team_id INTEGER NOT NULL"]
    for col, meta in DB_COLUMNS.items():
        if col in ('team_id', 'player_id'):
            continue
        if meta['table'] not in ('entity', 'both'):
            continue
        if meta.get('team_source') is not None or col in ('notes', 'created_at', 'updated_at'):
            team_cols.append(_col_def(col, meta))
    team_cols.append("  PRIMARY KEY (team_id)")
    ddl.append(',\n'.join(team_cols))
    ddl.append(");\n")

    # ---- players ----
    ddl.append("-- NCAA PLAYERS (entity table)")
    ddl.append(f"CREATE TABLE IF NOT EXISTS {players_table} (")
    player_cols = ["  player_id INTEGER NOT NULL"]
    for col, meta in DB_COLUMNS.items():
        if col in ('player_id',):
            continue
        if meta['table'] not in ('entity', 'both'):
            continue
        if (meta.get('player_source') is not None or
            col in ('team_id', 'wingspan_inches', 'birthdate', 'hand',
                     'years_experience', 'notes', 'backfilled',
                     'created_at', 'updated_at')):
            player_cols.append(_col_def(col, meta))
    player_cols.append("  PRIMARY KEY (player_id)")
    ddl.append(',\n'.join(player_cols))
    ddl.append(");\n")

    # ---- player_season_stats ----
    ddl.append("-- NCAA PLAYER SEASON STATS")
    ddl.append(f"CREATE TABLE IF NOT EXISTS {pss_table} (")
    ps_cols = [
        "  player_id INTEGER NOT NULL",
        "  season VARCHAR(10) NOT NULL",
        "  \"season_type\" SMALLINT NOT NULL",
    ]
    for col, meta in DB_COLUMNS.items():
        if col in ('player_id', 'season', 'season_type'):
            continue
        if meta['table'] != 'stats':
            continue
        if (meta.get('player_source') is not None or
            meta.get('computed') or meta.get('computed_for_player') or
            col in ('created_at', 'updated_at')):
            ps_cols.append(_col_def(col, meta))
    ps_cols.append("  PRIMARY KEY (player_id, season, \"season_type\")")
    ps_cols.append(f"  FOREIGN KEY (player_id) REFERENCES {players_table}(player_id)")
    ddl.append(',\n'.join(ps_cols))
    ddl.append(");\n")
    ddl.append(f"CREATE INDEX IF NOT EXISTS idx_ncaa_pss_player ON {pss_table}(player_id);")
    ddl.append(f"CREATE INDEX IF NOT EXISTS idx_ncaa_pss_season ON {pss_table}(season);\n")

    # ---- team_season_stats ----
    ddl.append("-- NCAA TEAM SEASON STATS")
    ddl.append(f"CREATE TABLE IF NOT EXISTS {tss_table} (")
    ts_cols = [
        "  team_id INTEGER NOT NULL",
        "  season VARCHAR(10) NOT NULL",
        "  \"season_type\" SMALLINT NOT NULL",
    ]
    for col, meta in DB_COLUMNS.items():
        if col in ('team_id', 'season', 'season_type'):
            continue
        if meta['table'] != 'stats':
            continue
        if (meta.get('team_source') is not None or meta.get('computed') or
            col in ('created_at', 'updated_at')):
            ts_cols.append(_col_def(col, meta))
    # Opponent stat columns
    for col, meta in DB_COLUMNS.items():
        if meta['table'] == 'stats' and meta.get('opp_source') is not None:
            ts_cols.append(f"  {_quote('opp_' + col)} {meta['type']} NULL")
    ts_cols.append("  PRIMARY KEY (team_id, season, \"season_type\")")
    ts_cols.append(f"  FOREIGN KEY (team_id) REFERENCES {teams_table}(team_id)")
    ddl.append(',\n'.join(ts_cols))
    ddl.append(");\n")
    ddl.append(f"CREATE INDEX IF NOT EXISTS idx_ncaa_tss_team ON {tss_table}(team_id);")
    ddl.append(f"CREATE INDEX IF NOT EXISTS idx_ncaa_tss_season ON {tss_table}(season);\n")

    return '\n'.join(ddl)


# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_teams_from_db(conn=None) -> Dict[int, Tuple[str, str]]:
    """
    Get all NCAA teams from DB (excluding teams with NULL abbreviation).
    Returns: {team_id: (abbr, institution)}
    """
    teams_table = get_table_name('team', 'entity')
    close_conn = conn is None
    if conn is None:
        conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT team_id, abbr, institution
                FROM {teams_table}
                WHERE abbr IS NOT NULL AND abbr != ''
                ORDER BY institution
            """)
            return {
                r['team_id']: (r['abbr'] or '', r['institution'] or '')
                for r in cur.fetchall()
            }
    finally:
        if close_conn:
            conn.close()


def get_existing_seasons(conn, entity_type: str) -> list:
    """Get list of seasons already loaded for an entity type."""
    table = get_table_name(entity_type, 'stats')
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT season FROM {table} ORDER BY season")
        return [r[0] for r in cur.fetchall()]


def get_season_player_count(conn, season: str) -> int:
    """Get number of player stat records for a season (display format, e.g. '2024-25')."""
    stats_table = get_table_name('player', 'stats')
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {stats_table} WHERE season = %s",
            (season,)
        )
        return cur.fetchone()[0]
