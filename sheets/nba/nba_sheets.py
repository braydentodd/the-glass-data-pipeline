"""
The Glass — NBA Sheets Library (thin wrapper)

League-specific DB queries, entity fields, and percentile logic for NBA.
All shared display/formatting logic lives in lib/sheets_engine.py.

Call flow:
  runners/nba_sheets.py → lib.nba_sheets (this file)
                        → lib.sheets_engine (via init_engine at import time)
"""

import hashlib
import json
import logging
import time
from bisect import bisect_left, bisect_right
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from etl.nba.config import DB_CONFIG, DB_SCHEMA, NBA_CONFIG
from sheets.nba_sheets import (
    COLORS, COLOR_THRESHOLDS,
    DEFAULT_STAT_MODE,
    GOOGLE_SHEETS_CONFIG, SERVER_CONFIG,
    SECTION_CONFIG, SECTIONS, SHEETS_COLUMNS,
    SHEET_FORMATTING,
    STAT_CONSTANTS, SUBSECTIONS, SUBSECTION_DISPLAY_NAMES,
)
from etl.nba.lib import get_table_name

logger = logging.getLogger(__name__)


# ============================================================================
# PERCENTILE RANK FUNCTION (NBA: bisect-based, plain sorted lists)
# Must be defined BEFORE init_engine() so it can be registered.
# ============================================================================

def get_percentile_rank(value: Any, sorted_values: List, reverse: bool = False) -> float:
    """
    Calculate percentile rank using binary search on pre-sorted values.

    Uses midpoint of bisect_left/bisect_right to handle ties correctly.

    Args:
        value: The value to rank
        sorted_values: Pre-sorted list of all values
        reverse: True if lower is better (turnovers, fouls)

    Returns:
        Percentile rank 0-100
    """
    if not sorted_values or value is None or not isinstance(value, (int, float)):
        return 50.0

    n = len(sorted_values)
    if n == 1:
        return 50.0

    pos_left = bisect_left(sorted_values, value)
    pos_right = bisect_right(sorted_values, value)
    avg_pos = (pos_left + pos_right - 1) / 2.0

    if reverse:
        percentile = (1 - avg_pos / (n - 1)) * 100
    else:
        percentile = (avg_pos / (n - 1)) * 100

    return max(0, min(100, percentile))


# ============================================================================
# ENGINE INITIALISATION
# ============================================================================

# Minutes fields for NBA: basic + tracking + hustle tracking
_MINUTES_FIELDS = {
    'basic':    'minutes_x10',
    'tracking': 'tr_minutes_x10',
    'hustle':   'h_minutes_x10',
}

from lib import sheets_engine as _engine  # noqa: E402
from lib.sheets_engine import (  # noqa: E402  — re-export everything callers need
    SheetsConfigurationError,
    build_entity_row,
    build_formatting_requests,
    build_headers,
    build_merged_entity_row,
    build_sheet_columns,
    build_summary_rows,
    calculate_entity_stats,
    clear_cache,
    create_cell_format,
    create_text_format,
    evaluate_formula,
    format_section_header,
    format_stat_value,
    format_years_range,
    generate_percentile_columns,
    get_all_columns_with_percentiles,
    get_cached_stats,
    get_color_dict,
    get_color_for_percentile,
    get_color_for_raw,
    get_column_index,
    get_columns_by_filters,
    get_columns_for_section_and_entity,
    get_editable_fields,
    get_reverse_stats,
    set_cached_stats,
)

_engine.init_engine(
    sheets_columns=SHEETS_COLUMNS,
    section_config=SECTION_CONFIG,
    sections=SECTIONS,
    subsections=SUBSECTIONS,
    subsection_display_names=SUBSECTION_DISPLAY_NAMES,
    stat_constants=STAT_CONSTANTS,
    default_stat_mode=DEFAULT_STAT_MODE,
    colors=COLORS,
    color_thresholds=COLOR_THRESHOLDS,
    sheet_formatting=SHEET_FORMATTING,
    per_minute_mode='per_36',
    percentile_rank_fn=get_percentile_rank,
    league_key='nba',
    minutes_fields=_MINUTES_FIELDS,
)


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db_connection():
    """Create a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def _get_all_stat_db_fields() -> set:
    """Fetch all stat column names from player_season_stats table."""
    _EXCLUDE = {'player_id', 'year', 'season_type', 'updated_at'}
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'player_season_stats'",
            (DB_SCHEMA,)
        )
        fields = {r[0] for r in cur.fetchall()} - _EXCLUDE
        cur.close()
        conn.close()
        return fields
    except Exception:
        return set()


def _get_all_team_stat_db_fields() -> set:
    """Fetch all stat column names from team_season_stats table."""
    _EXCLUDE = {'team_id', 'year', 'season_type', 'updated_at'}
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'team_season_stats'",
            (DB_SCHEMA,)
        )
        fields = {r[0] for r in cur.fetchall()} - _EXCLUDE
        cur.close()
        conn.close()
        return fields
    except Exception:
        return set()


# Well-known entity table fields (not in stats table)
_PLAYER_ENTITY_FIELDS = {
    'player_id', 'name', 'team_id', 'height_inches', 'weight_lbs',
    'wingspan_inches', 'years_experience', 'age', 'jersey_number',
    'hand', 'notes', 'birthdate', 'updated_at',
}
_TEAM_ENTITY_FIELDS = {
    'team_id', 'team_abbr', 'team_name', 'notes', 'updated_at',
}

# Dynamically computed at import time
_ALL_STAT_DB_FIELDS = _get_all_stat_db_fields()
_ALL_TEAM_STAT_DB_FIELDS = _get_all_team_stat_db_fields()

_STAT_TABLE_FIELDS = _ALL_STAT_DB_FIELDS - _PLAYER_ENTITY_FIELDS - _TEAM_ENTITY_FIELDS
_STAT_TABLE_FIELDS = {f for f in _STAT_TABLE_FIELDS if not f[0].isupper()}

_TEAM_STAT_TABLE_FIELDS = _ALL_TEAM_STAT_DB_FIELDS - _TEAM_ENTITY_FIELDS
_TEAM_STAT_TABLE_FIELDS = {f for f in _TEAM_STAT_TABLE_FIELDS if not f[0].isupper()}


def _quote_col(col: str) -> str:
    """Quote a column name for SQL (handles digit-starting names)."""
    return f'"{col}"'


# ============================================================================
# YEAR / SEASON HELPERS
# ============================================================================

def _year_to_season(year: int) -> str:
    """Convert end-year integer to season string: 2026 → '2025-26'."""
    return f"{year - 1}-{str(year)[2:]}"


def _build_year_filter(years_config: Optional[dict], current_year: int,
                       season_type) -> Tuple:
    """Build SQL year filter clause and params tuple.

    current_year is an integer end-year (e.g. 2026 for the 2025-26 season).
    Converts to season strings matching the varchar year column.
    """
    if not years_config:
        seasons = tuple(_year_to_season(current_year - i) for i in range(1, 4))
        return "AND s.year IN %s", (seasons,)

    mode = years_config.get('mode', 'years')
    value = years_config.get('value', 3)
    include_current = years_config.get('include_current', False)

    if mode == 'career':
        return "", ()
    elif mode == 'years':
        start = 0 if include_current else 1
        seasons = tuple(_year_to_season(current_year - i) for i in range(start, start + value))
        return "AND s.year IN %s", (seasons,)
    elif mode == 'seasons':
        return "AND s.year IN %s", (tuple(value),)
    else:
        return "", ()


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_players_for_team(conn, team_abbr: str, section: str = 'current_stats',
                           years_config: Optional[dict] = None) -> List[dict]:
    """Fetch player data for a team with all stats needed for formula evaluation."""
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    p_fields_base = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                     if f not in ('updated_at', 'birthdate', 'age')]
    p_age_expr = 'EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))::int AS "age"'
    p_fields = p_fields_base + [p_age_expr]
    t_fields = ['t.team_abbr']

    if section == 'current_stats':
        s_fields = [f's.{_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        all_fields = p_fields + t_fields + s_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        INNER JOIN {teams_table} t ON p.team_id = t.team_id
        LEFT JOIN {stats_table} s
            ON s.player_id = p.player_id
            AND s.year = %s AND s.season_type = %s
        WHERE t.team_abbr = %s
        ORDER BY COALESCE(s.minutes_x10, 0) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type, team_abbr))
            return [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')
        all_fields = p_fields + t_fields + s_fields
        group_fields = p_fields_base + ['p.birthdate'] + t_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        INNER JOIN {teams_table} t ON p.team_id = t.team_id
        LEFT JOIN {stats_table} s
            ON s.player_id = p.player_id
            {year_filter}
            AND s.season_type IN ({st})
        WHERE t.team_abbr = %s
        GROUP BY {', '.join(group_fields)}
        ORDER BY SUM(COALESCE(s.minutes_x10, 0)) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            return [dict(r) for r in cur.fetchall()]


def fetch_all_players(conn, section: str = 'current_stats',
                      years_config: Optional[dict] = None) -> List[dict]:
    """Fetch all players league-wide for percentile population."""
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    p_fields_base = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                     if f not in ('updated_at', 'birthdate', 'age')]
    p_age_expr = 'EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))::int AS "age"'
    p_fields = p_fields_base + [p_age_expr]
    t_fields = ['t.team_abbr']

    if section == 'current_stats':
        s_fields = [f's.{_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        all_fields = p_fields + t_fields + s_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        LEFT JOIN {teams_table} t ON p.team_id = t.team_id
        LEFT JOIN {stats_table} s
            ON s.player_id = p.player_id
            AND s.year = %s AND s.season_type = %s
        WHERE s.player_id IS NOT NULL
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type))
            return [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')
        all_fields = p_fields + t_fields + s_fields
        group_fields = p_fields_base + ['p.birthdate'] + t_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        LEFT JOIN {teams_table} t ON p.team_id = t.team_id
        LEFT JOIN {stats_table} s
            ON s.player_id = p.player_id
            {year_filter}
            AND s.season_type IN ({st})
        WHERE s.player_id IS NOT NULL
        GROUP BY {', '.join(group_fields)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]


def fetch_team_stats(conn, team_abbr: str, section: str = 'current_stats',
                     years_config: Optional[dict] = None) -> dict:
    """Fetch team + opponent stats. Returns {'team': {...}, 'opponent': {...}}."""
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    stats_table = get_table_name('team', 'stats')
    teams_table = get_table_name('team', 'entity')

    if section == 'current_stats':
        query = f"""
        SELECT s.*, t.team_abbr, t.notes
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            AND s.year = %s AND s.season_type = %s
        WHERE t.team_abbr = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type, team_abbr))
            row = cur.fetchone()

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_TEAM_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')

        query = f"""
        SELECT t.team_abbr, t.notes, {', '.join(s_fields)}
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            {year_filter}
            AND s.season_type IN ({st})
        WHERE t.team_abbr = %s
        GROUP BY t.team_abbr, t.notes
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            row = cur.fetchone()

    if not row:
        return {'team': {}, 'opponent': {}}

    result = dict(row)
    team_data = {k: v for k, v in result.items() if not k.startswith('opp_')}
    opp_data = {k: v for k, v in result.items() if k.startswith('opp_')}
    for k, v in team_data.items():
        if k not in opp_data:
            opp_data[k] = v

    return {'team': team_data, 'opponent': opp_data}


def fetch_all_teams(conn, section: str = 'current_stats',
                    years_config: Optional[dict] = None) -> dict:
    """Fetch all teams for percentile population. Returns {'teams': [...], 'opponents': [...]}."""
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    stats_table = get_table_name('team', 'stats')
    teams_table = get_table_name('team', 'entity')

    if section == 'current_stats':
        query = f"""
        SELECT s.*, t.team_abbr
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            AND s.year = %s AND s.season_type = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type))
            rows = [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_TEAM_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')

        query = f"""
        SELECT t.team_abbr, {', '.join(s_fields)}
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            {year_filter}
            AND s.season_type IN ({st})
        GROUP BY t.team_abbr
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = [dict(r) for r in cur.fetchall()]

    teams = []
    opponents = []
    for row in rows:
        team_data = {k: v for k, v in row.items() if not k.startswith('opp_')}
        opp_data = {k: v for k, v in row.items() if k.startswith('opp_')}
        for k, v in team_data.items():
            if k not in opp_data:
                opp_data[k] = v
        teams.append(team_data)
        opponents.append(opp_data)

    return {'teams': teams, 'opponents': opponents}


# ============================================================================
# PERCENTILE CALCULATIONS (NBA: plain sorted lists, multi-category minutes)
# ============================================================================

def calculate_all_percentiles(all_entities: List[dict], entity_type: str,
                              mode: str = 'per_game',
                              custom_value: Any = None) -> dict:
    """
    Calculate percentile populations for all stat columns.

    Each stat value is weighted by the appropriate minutes for its stat_category:
      - basic    → minutes_x10 / 10
      - tracking → tr_minutes_x10 / 10
      - hustle   → h_minutes_x10 / 10
      - none     → no weighting

    Returns:
        Dict of {col_key: sorted_values_list} for bisect-based percentile lookups
    """
    all_calculated = []
    for entity in all_entities:
        stats = calculate_entity_stats(entity, entity_type, mode, custom_value)
        all_calculated.append((entity, stats))

    percentiles = {}
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('has_percentile', False):
            continue

        stat_cat = col_def.get('stat_category', 'none')
        minutes_field = _MINUTES_FIELDS.get(stat_cat)

        values = []
        for entity, stats in all_calculated:
            val = stats.get(col_key)
            if val is None or not isinstance(val, (int, float)):
                continue
            if minutes_field:
                raw_minutes = (entity.get(minutes_field, 0) or 0) / 10.0
                if raw_minutes <= 0:
                    continue
            values.append(val)

        if values:
            percentiles[col_key] = sorted(values)

    return percentiles


# ============================================================================
# CONFIG FOR EXPORT
# ============================================================================

def get_config_for_export(mode: str = 'per_100') -> dict:
    """Build JSON-serializable config for /api/config endpoint."""
    from etl.nba.lib import get_teams_from_db
    return _engine.get_config_for_export(
        league='nba',
        get_teams_fn=get_teams_from_db,
        id_column_key='nba_id',
        server_config=SERVER_CONFIG,
        google_sheets_config=GOOGLE_SHEETS_CONFIG,
        mode=mode,
    )


# ============================================================================
# API RESPONSE CACHE
# ============================================================================

_stat_cache: Dict[str, Tuple[float, Any]] = {}


def _cache_key(*args) -> str:
    """Build a deterministic cache key from arguments."""
    raw = json.dumps(args, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()
