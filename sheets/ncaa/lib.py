"""
The Glass — NCAA Sheets Library (thin wrapper)

League-specific DB queries, entity fields, and percentile logic for NCAA.
All shared display/formatting logic lives in lib/sheets_engine.py.

Key NCAA differences from NBA:
  - per_40 instead of per_36
  - Weighted-CDF percentile rank (minutes as weights, not just filters)
  - calculate_all_percentiles returns sorted (value, weight) tuples
  - INNER JOIN player stats (only players with stats appear)
  - 'season' column (varchar) instead of 'year'
  - _build_year_filter career mode can exclude current season
  - Entity fields: no 'age', different team fields (abbr/institution/conference/mascot)
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from etl.ncaa.config import DB_CONFIG, DB_SCHEMA, NCAA_CONFIG
from sheets.ncaa_sheets import (
    COLORS, COLOR_THRESHOLDS,
    DEFAULT_STAT_MODE,
    GOOGLE_SHEETS_CONFIG, SERVER_CONFIG,
    SECTION_CONFIG, SECTIONS, SHEETS_COLUMNS,
    SHEET_FORMATTING,
    STAT_CONSTANTS, SUBSECTIONS, SUBSECTION_DISPLAY_NAMES,
)
from etl.ncaa.lib import get_table_name
from db.lib import get_db_connection  # re-exported for callers

logger = logging.getLogger(__name__)


# ============================================================================
# PERCENTILE RANK FUNCTION (NCAA: minute-weighted CDF)
# Must be defined BEFORE init_engine() so it can be registered.
# ============================================================================

def get_percentile_rank(value: Any, sorted_weighted: List, reverse: bool = False) -> float:
    """
    Calculate minute-weighted percentile rank.

    Uses weighted CDF: for each entry below the value, accumulate its weight.
    Ties get the midpoint of their cumulative weight range.

    Args:
        value: The value to rank
        sorted_weighted: Sorted list of (value, weight) tuples
        reverse: True if lower is better (turnovers, fouls)

    Returns:
        Percentile rank 0-100
    """
    if not sorted_weighted or value is None or not isinstance(value, (int, float)):
        return 50.0

    n = len(sorted_weighted)
    if n == 1:
        return 50.0

    total_weight = sum(w for _, w in sorted_weighted)
    if total_weight <= 0:
        return 50.0

    weight_below = 0.0
    weight_equal = 0.0
    for v, w in sorted_weighted:
        if v < value:
            weight_below += w
        elif v == value:
            weight_equal += w
        elif v > value:
            break

    midpoint = weight_below + weight_equal / 2.0
    percentile = (midpoint / total_weight) * 100

    if reverse:
        percentile = 100 - percentile

    return max(0, min(100, percentile))


# ============================================================================
# ENGINE INITIALISATION
# ============================================================================

# Minutes fields for NCAA: basic only (no tracking/hustle data)
_MINUTES_FIELDS = {
    'basic': 'minutes_x10',
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
    percentile_rank_fn=get_percentile_rank,
    league_key='ncaa',
    minutes_fields=_MINUTES_FIELDS,
)


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def _get_all_stat_db_fields() -> set:
    """Fetch all stat column names from player_season_stats table."""
    _EXCLUDE = {'player_id', 'season', 'season_type', 'updated_at', 'team_id'}
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
    _EXCLUDE = {'team_id', 'season', 'season_type', 'updated_at'}
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
    'wingspan_inches', 'years_experience', 'jersey_number',
    'hand', 'notes', 'birthdate', 'updated_at',
}
# NOTE: No 'age' field (NCAA birthdate is always null)

_TEAM_ENTITY_FIELDS = {
    'team_id', 'abbr', 'institution', 'conference', 'mascot',
    'notes', 'updated_at',
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
    Converts to season strings matching the varchar season column.

    NCAA 'career' mode optionally excludes current season (birthdate is always null
    so current-year stats are unreliable for some schools).
    """
    if not years_config:
        seasons = tuple(_year_to_season(current_year - i) for i in range(1, 4))
        return "AND s.season IN %s", (seasons,)

    mode = years_config.get('mode', 'years')
    value = years_config.get('value', 3)
    include_current = years_config.get('include_current', False)

    if mode == 'career':
        if include_current:
            return "", ()
        else:
            current_season = _year_to_season(current_year)
            return "AND s.season != %s", (current_season,)
    elif mode == 'years':
        start = 0 if include_current else 1
        seasons = tuple(_year_to_season(current_year - i) for i in range(start, start + value))
        return "AND s.season IN %s", (seasons,)
    elif mode == 'seasons':
        return "AND s.season IN %s", (tuple(value),)
    else:
        return "", ()


# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_players_for_team(conn, team_abbr: str, section: str = 'current_stats',
                           years_config: Optional[dict] = None) -> List[dict]:
    """Fetch player data for a team with all stats needed for formula evaluation."""
    current_season = NCAA_CONFIG['current_season']
    current_year = NCAA_CONFIG['current_season_int']
    season_type = 1  # Regular season

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    p_fields_base = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                     if f not in ('updated_at', 'birthdate', 'years_experience')]
    exp_subquery = (
        f'(SELECT COUNT(DISTINCT ss.season) FROM {stats_table} ss '
        f'WHERE ss.player_id = p.player_id AND ss.season_type = 1) AS years_experience'
    )
    p_fields = p_fields_base + [exp_subquery]
    t_fields = ['t.abbr', 't.conference']

    if section == 'current_stats':
        s_fields = [f's.{_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        all_fields = p_fields + t_fields + s_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        INNER JOIN {teams_table} t ON p.team_id = t.team_id
        INNER JOIN {stats_table} s
            ON s.player_id = p.player_id
            AND s.season = %s AND s.season_type = %s
        WHERE t.abbr = %s
        ORDER BY COALESCE(s.minutes_x10, 0) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type, team_abbr))
            return [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.season) AS season')
        all_fields = p_fields + t_fields + s_fields
        group_fields = p_fields_base + t_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        INNER JOIN {teams_table} t ON p.team_id = t.team_id
        LEFT JOIN {stats_table} s
            ON s.player_id = p.player_id
            {year_filter}
            AND s.season_type IN ({st})
        WHERE t.abbr = %s
        GROUP BY {', '.join(group_fields)}
        ORDER BY SUM(COALESCE(s.minutes_x10, 0)) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            return [dict(r) for r in cur.fetchall()]


def fetch_all_players(conn, section: str = 'current_stats',
                      years_config: Optional[dict] = None) -> List[dict]:
    """Fetch all players league-wide for percentile population."""
    current_season = NCAA_CONFIG['current_season']
    current_year = NCAA_CONFIG['current_season_int']
    season_type = 1  # Regular season

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    p_fields_base = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                     if f not in ('updated_at', 'birthdate', 'years_experience')]
    exp_subquery = (
        f'(SELECT COUNT(DISTINCT ss.season) FROM {stats_table} ss '
        f'WHERE ss.player_id = p.player_id AND ss.season_type = 1) AS years_experience'
    )
    p_fields = p_fields_base + [exp_subquery]
    t_fields = ['t.abbr', 't.conference']

    if section == 'current_stats':
        s_fields = [f's.{_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        all_fields = p_fields + t_fields + s_fields

        query = f"""
        SELECT {', '.join(all_fields)}
        FROM {players_table} p
        LEFT JOIN {teams_table} t ON p.team_id = t.team_id
        INNER JOIN {stats_table} s
            ON s.player_id = p.player_id
            AND s.season = %s AND s.season_type = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type))
            return [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.season) AS season')
        all_fields = p_fields + t_fields + s_fields
        group_fields = p_fields_base + t_fields

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
    current_season = NCAA_CONFIG['current_season']
    current_year = NCAA_CONFIG['current_season_int']
    season_type = 1  # Regular season
    stats_table = get_table_name('team', 'stats')
    teams_table = get_table_name('team', 'entity')

    if section == 'current_stats':
        query = f"""
        SELECT s.*, t.abbr, t.institution, t.conference, t.notes
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            AND s.season = %s AND s.season_type = %s
        WHERE t.abbr = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type, team_abbr))
            row = cur.fetchone()

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_TEAM_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.season) AS season')

        query = f"""
        SELECT t.abbr, t.institution, t.conference, t.notes, {', '.join(s_fields)}
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            {year_filter}
            AND s.season_type IN ({st})
        WHERE t.abbr = %s
        GROUP BY t.abbr, t.institution, t.conference, t.notes
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
    current_season = NCAA_CONFIG['current_season']
    current_year = NCAA_CONFIG['current_season_int']
    season_type = 1  # Regular season
    stats_table = get_table_name('team', 'stats')
    teams_table = get_table_name('team', 'entity')

    if section == 'current_stats':
        query = f"""
        SELECT s.*, t.abbr, t.institution, t.conference
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            AND s.season = %s AND s.season_type = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_season, season_type))
            rows = [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_TEAM_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.season) AS season')

        query = f"""
        SELECT t.abbr, t.institution, t.conference, {', '.join(s_fields)}
        FROM {teams_table} t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            {year_filter}
            AND s.season_type IN ({st})
        GROUP BY t.abbr, t.institution, t.conference
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
# PERCENTILE CALCULATIONS (NCAA: minute-weighted sorted (value, weight) tuples)
# ============================================================================

def calculate_all_percentiles(all_entities: List[dict], entity_type: str,
                              mode: str = 'per_game',
                              custom_value: Any = None) -> dict:
    """
    Calculate minute-weighted percentile populations for all stat columns.

    Each stat value is weighted by minutes played for its stat_category:
      - basic → minutes_x10 / 10  (actual minutes as weight)
      - none  → weight = 1        (unweighted)

    Returns:
        Dict of {col_key: sorted list of (value, weight) tuples}
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

        entries = []
        for entity, stats in all_calculated:
            val = stats.get(col_key)
            if val is None or not isinstance(val, (int, float)):
                continue

            if minutes_field:
                raw_minutes = (entity.get(minutes_field, 0) or 0) / 10.0
                if raw_minutes <= 0:
                    continue
                entries.append((val, raw_minutes))
            else:
                entries.append((val, 1.0))

        if entries:
            percentiles[col_key] = sorted(entries, key=lambda x: x[0])

    return percentiles


# ============================================================================
# CONFIG FOR EXPORT
# ============================================================================

def get_config_for_export(mode: str = 'per_100') -> dict:
    """Build JSON-serializable config for /api/config endpoint."""
    from etl.ncaa.lib import get_teams_from_db
    return _engine.get_config_for_export(
        league='ncaa',
        get_teams_fn=get_teams_from_db,
        id_column_key='ncaa_id',
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
