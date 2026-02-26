"""
The Glass Sheets - Library Module

Reusable utilities for Google Sheets sync and API.
All functions operate on config data from config.sheets.
Follows same pattern as lib/etl.py: lib = code, config = data.
"""

import re
import logging
import time
import hashlib
import json
from typing import Dict, List, Optional, Any, Tuple
from bisect import bisect_left

import psycopg2
from psycopg2.extras import RealDictCursor

from config.etl import DB_CONFIG, NBA_CONFIG
from lib.etl import get_table_name
from config.sheets import (
    SHEETS_COLUMNS, SECTIONS, SECTION_CONFIG, SUBSECTIONS,
    STAT_CONSTANTS, COLORS, COLOR_THRESHOLDS,
    GOOGLE_SHEETS_CONFIG, API_CONFIG, SERVER_CONFIG,
    SHEET_FORMATTING,
)

logger = logging.getLogger(__name__)


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class SheetsConfigurationError(Exception):
    """Raised when config/sheets.py has invalid formula syntax."""
    pass


# ============================================================================
# FORMULA COMPILATION & VALIDATION
# ============================================================================

# Pre-compiled formulas: {col_key: {formula_type: compiled_code_or_str}}
_COMPILED_FORMULAS: Dict[str, Dict[str, Any]] = {}


def _sanitize_var_name(name: str) -> str:
    """Convert DB column name to valid Python identifier. e.g., 2fgm → _2fgm"""
    if name and name[0].isdigit():
        return f'_{name}'
    return name


def _sanitize_formula(formula_str: str) -> str:
    """Transform formula string for Python eval: prefix digit-leading vars with _."""
    if not formula_str:
        return formula_str
    return re.sub(r'\b(\d+[a-zA-Z_]\w*)', r'_\1', formula_str)


def _extract_formula_variables(formula_str: str) -> set:
    """Extract DB column names referenced in a formula string."""
    if not formula_str or not isinstance(formula_str, str):
        return set()
    tokens = re.findall(r'\b(\d*[a-zA-Z_]\w*)\b', formula_str)
    skip = {'STAT_CONSTANTS', 'True', 'False', 'None', 'max', 'min', 'abs', 'int', 'float'}
    return {t for t in tokens if t not in skip}


def _compile_formula_entry(formula: str, label: str) -> Any:
    """
    Compile a single formula string. Returns compiled code object or the raw
    string if it's a simple field lookup (no operators).
    """
    if not formula:
        return None
    # Simple field reference (no operators) → just return string for dict lookup
    if not any(op in formula for op in '+-*/('):
        return formula
    sanitized = _sanitize_formula(formula)
    try:
        return compile(sanitized, f'<{label}>', 'eval')
    except SyntaxError as e:
        raise SheetsConfigurationError(f"Invalid formula [{label}]: {formula!r} → {e}")


def _compile_all_formulas():
    """Pre-compile all formula strings at import time for ~10x eval speedup."""
    for col_key, col_def in SHEETS_COLUMNS.items():
        _COMPILED_FORMULAS[col_key] = {}
        for ftype in ('player_formula', 'team_formula', 'opponents_formula'):
            formula = col_def.get(ftype)
            if formula is not None:
                _COMPILED_FORMULAS[col_key][ftype] = _compile_formula_entry(
                    formula, f'{col_key}.{ftype}'
                )
        # Also compile mode_overrides formulas
        for mode, override in col_def.get('mode_overrides', {}).items():
            okey = f'{col_key}__override_{mode}'
            _COMPILED_FORMULAS[okey] = {}
            for ftype in ('player_formula', 'team_formula', 'opponents_formula'):
                formula = override.get(ftype)
                if formula is not None:
                    _COMPILED_FORMULAS[okey][ftype] = _compile_formula_entry(
                        formula, f'{okey}.{ftype}'
                    )


# Compile at import time — validates all formulas immediately
_compile_all_formulas()


# ============================================================================
# FORMULA EVALUATION
# ============================================================================

def _sanitize_entity_data(entity_data: dict) -> dict:
    """Sanitize entity data keys for formula eval (handle digit-starting names)."""
    result = {}
    for key, value in entity_data.items():
        result[_sanitize_var_name(key)] = value if value is not None else 0
    return result


def evaluate_formula(col_key: str, entity_data: dict,
                     entity_type: str = 'player', mode: str = 'per_game') -> Any:
    """
    Evaluate a column formula against entity data.

    Uses pre-compiled formulas for performance.
    Handles mode_overrides (e.g., FTR → FTA in totals mode).
    """
    col_def = SHEETS_COLUMNS.get(col_key)
    if not col_def:
        return None

    ftype = f'{entity_type}_formula'

    # Check for mode override first
    override = col_def.get('mode_overrides', {}).get(mode)
    if override:
        okey = f'{col_key}__override_{mode}'
        compiled = _COMPILED_FORMULAS.get(okey, {}).get(ftype)
    else:
        compiled = _COMPILED_FORMULAS.get(col_key, {}).get(ftype)

    if compiled is None:
        return None

    # Simple field lookup
    if isinstance(compiled, str):
        val = entity_data.get(compiled)
        if val is not None:
            return val
        # If formula starts with uppercase, it's a display literal (e.g. 'TEAM', 'OPPONENTS')
        if compiled and compiled[0].isupper():
            return compiled
        return 0

    # Evaluate compiled expression
    try:
        local_vars = _sanitize_entity_data(entity_data)
        local_vars['STAT_CONSTANTS'] = STAT_CONSTANTS
        return eval(compiled, {"__builtins__": {}}, local_vars)
    except ZeroDivisionError:
        return None
    except (TypeError, ValueError, NameError):
        return None
    except Exception as e:
        logger.debug(f"Formula eval error for {col_key}: {e}")
        return None


# ============================================================================
# STAT CALCULATION ENGINE
# ============================================================================

def _apply_scaling(raw_value: Any, mode: str, games: float, minutes: float,
                   possessions: float, custom_value: Any = None) -> Any:
    """Apply mode-based scaling to a raw stat value."""
    if raw_value is None or raw_value == 0:
        return raw_value

    if mode == 'per_game':
        return raw_value / max(games, 1)
    elif mode == 'per_36':
        return raw_value * STAT_CONSTANTS['default_per_minutes'] / max(minutes, 0.1)
    elif mode == 'per_100':
        return raw_value * STAT_CONSTANTS['default_per_possessions'] / max(possessions, 1)
    elif mode == 'per_minutes' and custom_value:
        return raw_value * custom_value / max(minutes, 0.1)
    elif mode == 'per_possessions' and custom_value:
        return raw_value * custom_value / max(possessions, 1)

    return raw_value


def calculate_entity_stats(entity_data: dict, entity_type: str = 'player',
                           mode: str = 'per_game', custom_value: Any = None) -> dict:
    """
    Calculate all stat values for an entity in a given mode.

    Returns dict of {col_key: calculated_value} for all applicable columns.
    """
    results = {}
    games = entity_data.get('games', 0) or 0
    minutes = (entity_data.get('minutes_x10', 0) or 0) / 10.0
    possessions = entity_data.get('possessions', 0) or 0

    for col_key, col_def in SHEETS_COLUMNS.items():
        ftype = f'{entity_type}_formula'

        # Check if this entity has a formula (default or override)
        override = col_def.get('mode_overrides', {}).get(mode)
        has_formula = (
            col_def.get(ftype) is not None or
            (override and override.get(ftype) is not None)
        )
        if not has_formula:
            continue

        # Evaluate the formula (mode_overrides handled inside evaluate_formula)
        raw_value = evaluate_formula(col_key, entity_data, entity_type, mode)

        if raw_value is None:
            results[col_key] = None
            continue

        scale = col_def.get('scale_with_mode', False)

        # If there's a mode override active, the override formula already produces
        # the correct raw value. Apply scaling to the overridden value too.
        if override:
            if mode == 'totals':
                results[col_key] = raw_value
            elif scale is True:
                results[col_key] = _apply_scaling(raw_value, mode, games, minutes, possessions, custom_value)
            elif scale == 'per_game_only':
                results[col_key] = raw_value / max(games, 1)
            else:
                results[col_key] = raw_value
            continue

        # Normal formula (no override)
        if mode == 'totals':
            results[col_key] = raw_value
        elif scale is True:
            results[col_key] = _apply_scaling(raw_value, mode, games, minutes, possessions, custom_value)
        elif scale == 'per_game_only':
            results[col_key] = raw_value / max(games, 1)
        else:
            results[col_key] = raw_value  # ratio/percentage — no scaling

    return results


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db_connection():
    """Create a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def _get_all_stat_db_fields() -> set:
    """
    Fetch all stat column names directly from the player_season_stats table.

    Queries the DB rather than deriving names from formula variables — the two
    don't always align (e.g. formula uses 'charges', DB column is 'charges_drawn').
    """
    _EXCLUDE = {'player_id', 'year', 'season_type', 'updated_at'}
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'player_season_stats'"
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

# Stat fields = formula variables that are NOT entity fields
_STAT_TABLE_FIELDS = _ALL_STAT_DB_FIELDS - _PLAYER_ENTITY_FIELDS - _TEAM_ENTITY_FIELDS
# Remove literal strings used as display labels
_STAT_TABLE_FIELDS = {f for f in _STAT_TABLE_FIELDS if not f[0].isupper()}


def _quote_col(col: str) -> str:
    """Quote a column name for SQL. Needed for digit-starting names like 2fgm."""
    return f'"{col}"'


def fetch_players_for_team(conn, team_abbr: str, section: str = 'current_stats',
                           years_config: Optional[dict] = None) -> List[dict]:
    """
    Fetch player data for a team with all stats needed for formula evaluation.
    100% config-driven — SQL columns derived from SHEETS_COLUMNS formulas.
    """
    current_season = NBA_CONFIG['current_season']       # season string e.g. '2025-26'
    current_year = NBA_CONFIG['current_season_year']    # integer end-year for arithmetic
    season_type = NBA_CONFIG['season_type']

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    # Entity fields — base set (safe for SELECT and GROUP BY)
    p_fields_base = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                     if f not in ('updated_at', 'birthdate', 'age')]
    # age is computed from birthdate; include birthdate in GROUP BY so the
    # expression is valid even in aggregated queries
    p_age_expr = 'EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate))::int AS "age"'
    p_fields = p_fields_base + [p_age_expr]
    t_fields = ['t.team_abbr']

    if section == 'current_stats':
        # Single season — direct join, no aggregation
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
        # Historical or postseason — aggregate across years
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')
        all_fields = p_fields + t_fields + s_fields
        # GROUP BY uses base fields + birthdate (so age expression is valid)
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

    if section == 'current_stats':
        query = f"""
        SELECT s.*, t.team_abbr
        FROM teams t
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

        # For aggregation, SUM all numeric stat columns
        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')

        query = f"""
        SELECT t.team_abbr, {', '.join(s_fields)}
        FROM teams t
        LEFT JOIN {stats_table} s
            ON s.team_id = t.team_id
            {year_filter}
            AND s.season_type IN ({st})
        WHERE t.team_abbr = %s
        GROUP BY t.team_abbr
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            row = cur.fetchone()

    if not row:
        return {'team': {}, 'opponent': {}}

    result = dict(row)
    team_data = {k: v for k, v in result.items() if not k.startswith('opp_')}
    opp_data = {k.replace('opp_', ''): v for k, v in result.items() if k.startswith('opp_')}
    # Also include non-opponent fields for opponent formula evaluation
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

    if section == 'current_stats':
        query = f"""
        SELECT s.*, t.team_abbr
        FROM teams t
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

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')

        query = f"""
        SELECT t.team_abbr, {', '.join(s_fields)}
        FROM teams t
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
        opp_data = {k.replace('opp_', ''): v for k, v in row.items() if k.startswith('opp_')}
        for k, v in team_data.items():
            if k not in opp_data:
                opp_data[k] = v
        teams.append(team_data)
        opponents.append(opp_data)

    return {'teams': teams, 'opponents': opponents}


def _year_to_season(year: int) -> str:
    """Convert end-year integer to season string: 2026 → '2025-26'."""
    return f"{year - 1}-{str(year)[2:]}"


def _build_year_filter(years_config: Optional[dict], current_year: int,
                       season_type) -> Tuple:
    """Build SQL year filter clause and params tuple.

    current_year is an integer end-year (e.g. 2026 for the 2025-26 season).
    Converts to season strings (e.g. '2025-26') to match the varchar year column.
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
        # value is already a list of season strings from the caller
        return "AND s.year IN %s", (tuple(value),)
    else:
        return "", ()


# ============================================================================
# PERCENTILE CALCULATIONS
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
      - none     → no weighting (games, minutes, years, entity info)

    Args:
        all_entities: List of raw entity data dicts (from fetch_all_*)
        entity_type: 'player', 'team', or 'opponents'
        mode: Stats mode for formula evaluation
        custom_value: Custom value for per_minutes/per_possessions modes

    Returns:
        Dict of {col_key: sorted_values_list} for percentile lookups
    """
    # Minutes field lookup by stat_category
    # (uses module-level _MINUTES_FIELD constant)

    # First, calculate stats for every entity and keep raw data for weighting
    all_calculated = []
    for entity in all_entities:
        stats = calculate_entity_stats(entity, entity_type, mode, custom_value)
        all_calculated.append((entity, stats))

    # Build sorted value arrays for each stat column with has_percentile
    percentiles = {}
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('has_percentile', False):
            continue

        stat_cat = col_def.get('stat_category', 'none')
        minutes_field = _MINUTES_FIELD.get(stat_cat)

        values = []
        for entity, stats in all_calculated:
            val = stats.get(col_key)
            if val is None:
                continue

            # Apply minute-weighting for stat categories that have a minutes field
            if minutes_field:
                raw_minutes = (entity.get(minutes_field, 0) or 0) / 10.0
                if raw_minutes > 0:
                    values.append(val * raw_minutes)
                # Skip entities with 0 minutes for this category
            else:
                values.append(val)

        if values:
            percentiles[col_key] = sorted(values)

    return percentiles


def get_percentile_rank(value: Any, sorted_values: List, reverse: bool = False,
                        weight: float = 1.0) -> float:
    """
    Calculate percentile rank using binary search on pre-sorted values.

    Args:
        value: The value to rank
        sorted_values: Pre-sorted list of all values (minute-weighted)
        reverse: True if lower is better (turnovers, fouls)
        weight: The minute weight to apply to value before ranking
                (must match the weighting used to build sorted_values)

    Returns:
        Percentile rank 0-100
    """
    if not sorted_values or value is None:
        return 50.0

    weighted_value = value * weight if weight != 1.0 else value

    n = len(sorted_values)
    # Count how many values this beats
    pos = bisect_left(sorted_values, weighted_value)

    if reverse:
        percentile = (1 - pos / n) * 100
    else:
        percentile = (pos / n) * 100

    return max(0, min(100, percentile))


# Minutes field lookup by stat_category — shared between population building and ranking
_MINUTES_FIELD = {
    'basic': 'minutes_x10',
    'tracking': 'tr_minutes_x10',
    'hustle': 'h_minutes_x10',
}


def _get_minute_weight(col_key: str, entity_data: dict) -> float:
    """Get the minute-weight for a column based on its stat_category.
    Returns 1.0 for 'none' category (no weighting)."""
    col_def = SHEETS_COLUMNS.get(col_key, {})
    stat_cat = col_def.get('stat_category', 'none')
    minutes_field = _MINUTES_FIELD.get(stat_cat)
    if not minutes_field:
        return 1.0
    raw_minutes = (entity_data.get(minutes_field, 0) or 0) / 10.0
    return max(raw_minutes, 0.0) or 1.0  # Avoid 0 weight; fall back to 1.0


# ============================================================================
# COLUMN FILTERING & SELECTION
# ============================================================================

def generate_percentile_columns() -> dict:
    """Auto-generate percentile column defs for all columns with has_percentile=True."""
    pct_columns = {}
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('has_percentile'):
            continue
        pct_key = f"{col_key}_pct"
        pct_columns[pct_key] = {
            'key': pct_key,
            'stat_category': col_def.get('stat_category', 'none'),
            'display_name': col_def['display_name'],  # Same name as base column
            'description': col_def.get('description', ''),
            'section': col_def['section'],
            'subsection': col_def.get('subsection'),
            'stat_mode': col_def['stat_mode'],
            'has_percentile': False,
            'is_stat': col_def.get('is_stat', False),
            'editable': False,
            'reverse_percentile': col_def.get('reverse_percentile', False),
            'scale_with_mode': False,
            'format': 'number',
            'decimal_places': 0,
            'is_generated_percentile': True,
            'base_stat': col_key,
            'player_formula': col_def.get('player_formula'),
            'team_formula': col_def.get('team_formula'),
            'opponents_formula': col_def.get('opponents_formula'),
        }
    return pct_columns


def get_all_columns_with_percentiles() -> dict:
    """Get SHEETS_COLUMNS plus auto-generated percentile columns."""
    all_cols = dict(SHEETS_COLUMNS)
    all_cols.update(generate_percentile_columns())
    return all_cols


def get_columns_by_filters(section=None, subsection=None, entity=None,
                           stat_mode=None, include_percentiles=False) -> dict:
    """
    Get columns matching specified filters.

    Args:
        section: Filter by section name
        subsection: Filter by subsection name
        entity: 'player', 'team', or 'opponents' — checks formula existence
        stat_mode: 'basic', 'advanced', or 'both'
        include_percentiles: Include auto-generated percentile columns
    """
    columns = get_all_columns_with_percentiles() if include_percentiles else SHEETS_COLUMNS
    filtered = {}

    for col_key, col_def in columns.items():
        if section and section not in col_def.get('section', []):
            continue
        if subsection and col_def.get('subsection') != subsection:
            continue
        if entity:
            fkey = f'{entity}_formula'
            if col_def.get(fkey) is None:
                continue
        if stat_mode:
            col_mode = col_def.get('stat_mode', 'both')
            if col_mode != 'both' and col_mode != stat_mode:
                continue
        filtered[col_key] = col_def

    return filtered


def get_columns_for_section_and_entity(section: str, entity: str,
                                       stat_mode: str = 'both',
                                       include_percentiles: bool = False) -> List[Tuple]:
    """
    Get ordered columns for a section and entity.
    Stats sections are ordered by SUBSECTIONS; others by definition order.
    """
    columns = get_columns_by_filters(
        section=section, entity=entity,
        stat_mode=stat_mode, include_percentiles=include_percentiles
    )
    section_config = SECTION_CONFIG.get(section, {})

    if section_config.get('is_stats_section'):
        subsec_groups = {}
        for col_key, col_def in columns.items():
            subsec = col_def.get('subsection')
            if subsec not in subsec_groups:
                subsec_groups[subsec] = []
            subsec_groups[subsec].append((col_key, col_def))
        ordered = []
        for subsec in SUBSECTIONS:
            if subsec in subsec_groups:
                ordered.extend(subsec_groups[subsec])
        return ordered
    else:
        return [(k, v) for k, v in columns.items()]


def build_sheet_columns(entity: str = 'player', stat_mode: str = 'both',
                        show_percentiles: bool = False,
                        sheet_type: str = 'team') -> List[Tuple]:
    """
    Build complete column structure for a sheet.

    Returns list of (column_key, column_def, visible, context_section) tuples.
    context_section is the section block this column lives in — used by
    build_entity_row to blank out wrong-section stats on a given row.

    Percentile columns are interleaved immediately after their base stat column.
    Columns with sheets='nba' are excluded on team sheets.

    Always builds with stat_mode='both' so all columns exist in the structure
    (needed for JS toggle ranges). The visible flag is set based on
    SHEET_FORMATTING['hide_advanced_columns'] — advanced columns start hidden.
    """
    fmt = SHEET_FORMATTING
    hide_advanced = fmt.get('hide_advanced_columns', True)

    # Get percentile column defs
    pct_columns = generate_percentile_columns()

    all_columns = []

    for section in SECTIONS:
        # Always fetch ALL columns (both basic + advanced) for structure
        section_cols = get_columns_for_section_and_entity(
            section=section, entity=entity,
            stat_mode='both', include_percentiles=False
        )
        for col_key, col_def in section_cols:
            # Skip columns not meant for this sheet type
            col_sheets = col_def.get('sheets', 'both')
            if col_sheets == 'nba' and sheet_type != 'nba':
                continue

            # Advanced stat columns start hidden when hide_advanced_columns is True
            col_mode = col_def.get('stat_mode', 'both')
            visible = True
            if hide_advanced and col_mode == 'advanced':
                visible = False

            all_columns.append((col_key, col_def, visible, section))

            # Interleave percentile column immediately after its base column
            pct_key = f"{col_key}_pct"
            if col_def.get('has_percentile') and pct_key in pct_columns:
                pct_def = pct_columns[pct_key]
                # Percentile columns are hidden by default
                pct_visible = show_percentiles
                all_columns.append((pct_key, pct_def, pct_visible, section))

    return all_columns


def get_column_index(column_key: str, columns_list: List[Tuple],
                     context_section: Optional[str] = None) -> Optional[int]:
    """
    Get 0-based index of a column in the columns list.
    If context_section given, finds the instance in that section block.
    Otherwise returns the first match.
    """
    for idx, entry in enumerate(columns_list):
        col_key = entry[0]
        if col_key == column_key:
            if context_section is None:
                return idx
            col_ctx = entry[3] if len(entry) > 3 else None
            if col_ctx == context_section:
                return idx
    return None


# ============================================================================
# HEADER BUILDING
# ============================================================================

def build_headers(columns_list: List[Tuple], mode: str = 'per_game',
                  team_name: str = '',
                  current_year: int = 0,
                  historical_config: Optional[dict] = None,
                  postseason_config: Optional[dict] = None,
                  hist_timeframe: str = '',
                  post_timeframe: str = '') -> dict:
    """
    Build header rows for Google Sheets (4-row layout).

    Row 0: Section headers (team name merged into 'entities' section — no "Names" header)
    Row 1: Subsection headers (hidden by default)
    Row 2: Column names (percentile cols get same name as their base column)
    Row 3: Empty filter row (auto-filter dropdowns applied here)

    Returns dict with row1 (sections), row2 (subsections), row3 (col names),
    and merges list.

    Uses context_section (4th tuple element) for section header grouping.

    If current_year is provided, format_section_header() produces full headers:
        "2025-26 Regular Season Stats"
        "Last 3 Regular Season Stats (2023-24 to 2025-26)"
        "Career Postseason Stats"
    Otherwise falls back to legacy hist_timeframe/post_timeframe prefix mode.
    """
    # Build section display names
    _section_display_overrides = {}
    if current_year:
        _section_display_overrides['current_stats'] = format_section_header(
            'current_stats', current_year=current_year)
        _section_display_overrides['historical_stats'] = format_section_header(
            'historical_stats', years_config=historical_config,
            current_year=current_year, is_postseason=False)
        _section_display_overrides['postseason_stats'] = format_section_header(
            'postseason_stats', years_config=postseason_config,
            current_year=current_year, is_postseason=True)
    else:
        # Legacy fallback
        if hist_timeframe:
            _section_display_overrides['historical_stats'] = f"{hist_timeframe} Historical"
        if post_timeframe:
            _section_display_overrides['postseason_stats'] = f"{post_timeframe} Postseason"

    row1, row2, row3 = [], [], []
    merges = []
    cur_section = cur_subsection = None
    sec_start = sub_start = 0

    for idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        # Use context_section (section block position) not col_def.section list
        section = entry[3] if len(entry) > 3 else (col_def.get('section', ['unknown'])[0])
        subsection = col_def.get('subsection')

        # Row 0: Section headers
        if section != cur_section:
            if cur_section is not None and sec_start < idx:
                if cur_section == 'entities':
                    # Team name goes here — no "Names" header
                    display = team_name
                else:
                    display = _section_display_overrides.get(
                        cur_section,
                        SECTION_CONFIG.get(cur_section, {}).get('display_name', cur_section),
                    )
                merges.append({'row': 0, 'start_col': sec_start, 'end_col': idx, 'value': display})
            cur_section = section
            sec_start = idx
            # First cell in section gets the header text (overwritten by merge)
            if section == 'entities':
                row1.append(team_name)
            else:
                row1.append(_section_display_overrides.get(
                    section,
                    SECTION_CONFIG.get(section, {}).get('display_name', section),
                ))
        else:
            row1.append('')

        # Row 1: Subsection headers
        sc = SECTION_CONFIG.get(section, {})
        if sc.get('is_stats_section') and subsection:
            if subsection != cur_subsection:
                if cur_subsection is not None and sub_start < idx:
                    merges.append({'row': 1, 'start_col': sub_start, 'end_col': idx, 'value': cur_subsection.title()})
                cur_subsection = subsection
                sub_start = idx
                row2.append(subsection.title())
            else:
                row2.append('')
        else:
            cur_subsection = None
            row2.append('')

        # Row 2: Column display names
        # Percentile columns use the same display_name as their base column
        override = col_def.get('mode_overrides', {}).get(mode)
        display_name = (override or {}).get('display_name', col_def.get('display_name', col_key))
        row3.append(display_name)

    # Close final merges
    n = len(columns_list)
    if cur_section:
        if cur_section == 'entities':
            display = team_name
        else:
            display = _section_display_overrides.get(
                cur_section,
                SECTION_CONFIG.get(cur_section, {}).get('display_name', cur_section),
            )
        merges.append({'row': 0, 'start_col': sec_start, 'end_col': n, 'value': display})
    if cur_subsection:
        merges.append({'row': 1, 'start_col': sub_start, 'end_col': n, 'value': cur_subsection.title()})

    return {
        'row1': row1, 'row2': row2, 'row3': row3,
        'merges': merges
    }


# ============================================================================
# ROW BUILDING
# ============================================================================

def build_entity_row(entity_data: dict, columns_list: List[Tuple],
                     percentiles: dict, entity_type: str = 'player',
                     mode: str = 'per_game', custom_value: Any = None,
                     years_str: str = '',
                     row_section: Optional[str] = None,
                     section_data: Optional[dict] = None) -> list:
    """
    Build a single data row for any entity type.

    Evaluates all formulas, applies scaling, calculates percentile rank,
    and formats values. 100% config-driven.

    Supports TWO modes:
    1. Legacy single-section mode (row_section set):
       Uses entity_data + percentiles for matching section, blanks others.
    2. Merged multi-section mode (section_data set):
       section_data = {section_name: (entity_data, percentiles, years_str)}
       Fills each stats-section column from its corresponding data.
       Non-stats columns use the first available entity_data.
    """
    if section_data:
        # Merged mode — pre-calculate stats per section
        calculated_by_section = {}
        for sec_name, (sec_entity, sec_pcts, sec_years) in section_data.items():
            calculated_by_section[sec_name] = calculate_entity_stats(
                sec_entity, entity_type, mode, custom_value
            )
        # For non-stats columns, use the first section's entity data
        first_section = next(iter(section_data))
        primary_entity = section_data[first_section][0]
        primary_calculated = calculated_by_section[first_section]
        primary_years = section_data[first_section][2]
    else:
        # Legacy single-section mode
        primary_entity = entity_data
        primary_calculated = calculate_entity_stats(entity_data, entity_type, mode, custom_value)
        primary_years = years_str

    row = []

    for entry in columns_list:
        col_key, col_def, visible = entry[0], entry[1], entry[2]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        is_stats_section = col_ctx_cfg.get('is_stats_section', False)

        if section_data and is_stats_section:
            # Merged mode — pick the right data for this section
            if col_ctx in section_data:
                sec_entity, sec_pcts, sec_years = section_data[col_ctx]
                calculated = calculated_by_section[col_ctx]
                pcts = sec_pcts
                ystr = sec_years
            else:
                row.append('')
                continue
        elif row_section and is_stats_section and col_ctx != row_section:
            # Legacy mode — blank out wrong-section columns
            row.append('')
            continue
        else:
            # Non-stats column or matching section
            calculated = primary_calculated if not section_data else primary_calculated
            pcts = percentiles if not section_data else (
                section_data[first_section][1] if section_data else percentiles
            )
            ystr = primary_years
            sec_entity = primary_entity

        if is_pct:
            base_key = col_def.get('base_stat', col_key.replace('_pct', ''))
            base_def = SHEETS_COLUMNS.get(base_key, {})
            value = calculated.get(base_key)

            if value is not None and base_key in pcts:
                reverse = base_def.get('reverse_percentile', False)
                weight = _get_minute_weight(base_key, sec_entity)
                rank = get_percentile_rank(value, pcts[base_key], reverse, weight)
                row.append(round(rank))
            else:
                row.append('')
            continue

        # Non-percentile column
        # Years column: show count of distinct years (already COUNT(DISTINCT s.year) from SQL)
        if col_key == 'years':
            # In merged mode, get the year count from the section's entity data
            if section_data and is_stats_section and col_ctx in section_data:
                year_count = section_data[col_ctx][0].get('year', '')
            elif not section_data:
                year_count = entity_data.get('year', '')
            else:
                year_count = ''
            # year count is already an integer from COUNT(DISTINCT s.year)
            row.append(year_count if year_count else '')
            continue

        # Info column (non-stat) — simple field lookup
        if not col_def.get('is_stat', False):
            use_entity = sec_entity if section_data and is_stats_section else primary_entity
            value = evaluate_formula(col_key, use_entity, entity_type, mode)
            if col_def.get('format') == 'height' and value:
                row.append(format_height(value))
            else:
                row.append(value if value else '')
            continue

        # Stat column — use pre-calculated value
        value = calculated.get(col_key)
        override = col_def.get('mode_overrides', {}).get(mode)
        active_def = override if override else col_def
        formatted = format_stat_value(value, active_def)
        row.append(formatted if formatted is not None else '')

    return row


# ============================================================================
# FORMATTING HELPERS
# ============================================================================

def format_stat_value(value: Any, col_def: dict) -> Any:
    """Format a stat value for display according to column definition."""
    if value is None:
        return ''
    if isinstance(value, (int, float)) and value == 0:
        return 0

    fmt = col_def.get('format', 'number')
    decimals = col_def.get('decimal_places', 1)

    if fmt == 'percentage':
        # Value is already 0-100 from formula (e.g., (turnovers/possessions)*100)
        # Some are 0-1 ratios — check magnitude
        if isinstance(value, (int, float)) and 0 < abs(value) < 1:
            value = value * 100
        rounded = round(value, decimals)
    else:
        rounded = round(value, decimals)

    # Return int if whole number
    if rounded == int(rounded):
        return int(rounded)
    return rounded


def format_height(inches: Any) -> str:
    """Format height in inches to feet-inches string. 80 → 6'8\"."""
    if not inches:
        return ''
    feet = int(inches // 12)
    remaining = int(inches % 12)
    return f"{feet}'{remaining}\""


def format_section_header(section: str, years_config: Optional[dict] = None,
                          current_year: int = 0,
                          is_postseason: bool = False) -> str:
    """
    Build the full section header display string.

    Current stats:   "2025-26 Regular Season Stats"
    Historical/Post: "Last 3 Regular Season Stats (2023-24 to 2025-26)"
                     "Career Regular Season Stats"
                     "Career Previous Regular Season Stats"
                     "Last 3 Previous Postseason Stats (2022-23 to 2024-25)"

    Args:
        section: 'current_stats', 'historical_stats', or 'postseason_stats'
        years_config: {mode, value, include_current} for hist/post
        current_year: End-year integer (e.g. 2026 for 2025-26 season)
        is_postseason: True for postseason sections
    """
    season_label = 'Postseason' if is_postseason else 'Regular Season'

    # Current stats: just "YYYY-YY Regular Season Stats"
    if section == 'current_stats':
        season_str = _year_to_season(current_year)
        return f"{season_str} {season_label} Stats"

    # Historical / Postseason sections
    mode = (years_config or {}).get('mode', 'years')
    value = (years_config or {}).get('value', 3)
    include_current = (years_config or {}).get('include_current', False)

    previous = '' if include_current else ' Previous'

    if mode == 'career':
        return f"Career{previous} {season_label} Stats"
    elif mode == 'years':
        start = 0 if include_current else 1
        end_year = current_year - start
        start_year = current_year - (start + value - 1)
        range_str = f" ({_year_to_season(start_year)} to {_year_to_season(end_year)})"
        return f"Last {value}{previous} {season_label} Stats{range_str}"
    elif mode == 'seasons':
        seasons = value if isinstance(value, list) else []
        if seasons:
            n = len(seasons)
            first = min(seasons)
            last = max(seasons)
            range_str = f" ({first} to {last})"
            return f"Last {n}{previous} {season_label} Stats{range_str}"
        return f"{season_label} Stats"
    else:
        return f"{season_label} Stats"


def format_years_range(years_config: Optional[dict], current_year: int) -> str:
    """
    Legacy wrapper — returns a prefix string for section headers.
    Kept for backward compatibility; prefer format_section_header() for full headers.
    """
    if not years_config:
        return 'Last 3 Years'
    mode = years_config.get('mode', 'years')
    if mode == 'career':
        return 'Career'
    elif mode == 'years':
        value = years_config.get('value', 3)
        return f'Last {value} Year{"s" if value != 1 else ""}'
    elif mode == 'since_season':
        season = years_config.get('season', years_config.get('value', ''))
        return f'Since {season}'
    elif mode == 'seasons':
        years = years_config.get('value', [])
        if years:
            first = min(years)
            last = max(years)
            return f"{_year_to_season(first)} – {_year_to_season(last)}"
        return ''
    return ''


# ============================================================================
# COLOR HELPERS
# ============================================================================

def get_color_for_percentile(percentile: float, reverse: bool = False) -> dict:
    """Get RGB color dict (values 0-1) for a percentile using red→yellow→green gradient."""
    if reverse:
        percentile = 100 - percentile
    percentile = max(0, min(100, percentile))

    red, yellow, green = COLORS['red'], COLORS['yellow'], COLORS['green']
    mid = COLOR_THRESHOLDS['mid']

    if percentile < mid:
        ratio = percentile / mid
        return {
            'red': red['red'] + (yellow['red'] - red['red']) * ratio,
            'green': red['green'] + (yellow['green'] - red['green']) * ratio,
            'blue': red['blue'] + (yellow['blue'] - red['blue']) * ratio,
        }
    else:
        ratio = (percentile - mid) / (COLOR_THRESHOLDS['high'] - mid)
        return {
            'red': yellow['red'] + (green['red'] - yellow['red']) * ratio,
            'green': yellow['green'] + (green['green'] - yellow['green']) * ratio,
            'blue': yellow['blue'] + (green['blue'] - yellow['blue']) * ratio,
        }


def get_color_dict(color_name: str) -> dict:
    """Get color dict from COLORS constant."""
    return COLORS.get(color_name, COLORS['white'])


def get_color_for_raw(color_dict: dict) -> dict:
    """Ensure a color dict has the right keys for Sheets API."""
    return {
        'red': color_dict.get('red', 0),
        'green': color_dict.get('green', 0),
        'blue': color_dict.get('blue', 0),
    }


# ============================================================================
# GOOGLE SHEETS FORMATTING REQUEST BUILDERS
# ============================================================================

def build_formatting_requests(ws_id: int, columns_list: List[Tuple],
                              header_merges: list, n_data_rows: int,
                              team_name: str,
                              percentile_cells: Optional[List[dict]] = None,
                              n_player_rows: int = 0) -> list:
    """
    Build ALL Google Sheets batch_update requests for a worksheet.
    100% config-driven from SHEET_FORMATTING.

    Args:
        ws_id: Worksheet ID
        columns_list: The column structure from build_sheet_columns
        header_merges: Merge info from build_headers
        n_data_rows: Number of data rows (players + team/opp)
        team_name: Full team name for display
        percentile_cells: List of {row, col, percentile, reverse} for shading
        n_player_rows: Number of player rows (for filter range; team/opp excluded)

    Returns:
        List of request dicts for spreadsheet.batch_update
    """
    fmt = SHEET_FORMATTING
    n_cols = len(columns_list)
    data_start = fmt['data_start_row']
    total_rows = data_start + n_data_rows
    header_end = fmt['data_start_row']  # Row after last header row
    border_weight = fmt['border_weight']
    header_border_color = get_color_for_raw(COLORS[fmt['header_border_color']])
    data_border_color = get_color_for_raw(COLORS[fmt['data_border_color']])
    wrap_strategy = fmt.get('wrap_strategy', 'CLIP')

    requests = []

    # ---- 1. Grid properties: frozen rows/cols, hide gridlines ----
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': ws_id,
                'gridProperties': {
                    'frozenRowCount': fmt['frozen_rows'],
                    'frozenColumnCount': fmt['frozen_cols'],
                    'hideGridlines': True,
                },
            },
            'fields': 'gridProperties(frozenRowCount,frozenColumnCount,hideGridlines)',
        }
    })

    # ---- 2. Section header row (row 0) — includes team name in entities section ----
    requests.append({
        'repeatCell': {
            'range': _range(ws_id, fmt['section_header_row'], fmt['section_header_row'] + 1, 0, n_cols),
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': get_color_for_raw(COLORS[fmt['header_bg']]),
                    'textFormat': {
                        'fontFamily': fmt['header_font'],
                        'fontSize': fmt['section_header_size'],
                        'bold': True,
                        'foregroundColor': get_color_for_raw(COLORS[fmt['header_fg']]),
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': wrap_strategy,
                },
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)',
        }
    })

    # Team name in entities section — centered, larger font
    entities_end = 0
    for idx, entry in enumerate(columns_list):
        ctx = entry[3] if len(entry) > 3 else None
        if ctx != 'entities':
            entities_end = idx
            break
    else:
        entities_end = n_cols
    if entities_end > 0:
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, fmt['section_header_row'], fmt['section_header_row'] + 1, 0, entities_end),
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontFamily': fmt['header_font'],
                            'fontSize': fmt['team_name_size'],
                            'bold': True,
                            'foregroundColor': get_color_for_raw(COLORS[fmt['header_fg']]),
                        },
                        'horizontalAlignment': 'CENTER',
                    },
                },
                'fields': 'userEnteredFormat(textFormat,horizontalAlignment)',
            }
        })

    # ---- 3. Subsection header row (row 1) ----
    requests.append({
        'repeatCell': {
            'range': _range(ws_id, fmt['subsection_header_row'], fmt['subsection_header_row'] + 1, 0, n_cols),
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': get_color_for_raw(COLORS[fmt['header_bg']]),
                    'textFormat': {
                        'fontFamily': fmt['header_font'],
                        'fontSize': fmt['subsection_header_size'],
                        'bold': True,
                        'foregroundColor': get_color_for_raw(COLORS[fmt['header_fg']]),
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': wrap_strategy,
                },
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)',
        }
    })

    # ---- 4. Column header row (row 2) ----
    requests.append({
        'repeatCell': {
            'range': _range(ws_id, fmt['column_header_row'], fmt['column_header_row'] + 1, 0, n_cols),
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': get_color_for_raw(COLORS[fmt['header_bg']]),
                    'textFormat': {
                        'fontFamily': fmt['header_font'],
                        'fontSize': fmt['column_header_size'],
                        'bold': True,
                        'foregroundColor': get_color_for_raw(COLORS[fmt['header_fg']]),
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': wrap_strategy,
                },
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)',
        }
    })

    # ---- 5. Filter row (row 3) — same header styling ----
    requests.append({
        'repeatCell': {
            'range': _range(ws_id, fmt['filter_row'], fmt['filter_row'] + 1, 0, n_cols),
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': get_color_for_raw(COLORS[fmt['header_bg']]),
                    'textFormat': {
                        'fontFamily': fmt['header_font'],
                        'fontSize': fmt['column_header_size'],
                        'foregroundColor': get_color_for_raw(COLORS[fmt['header_fg']]),
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': wrap_strategy,
                },
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)',
        }
    })

    # ---- 6. Data rows default styling (incl. CLIP wrap) ----
    if n_data_rows > 0:
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, data_start, total_rows, 0, n_cols),
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontFamily': fmt['data_font'],
                            'fontSize': fmt['data_size'],
                        },
                        'horizontalAlignment': fmt['default_h_align'],
                        'verticalAlignment': fmt['default_v_align'],
                        'wrapStrategy': wrap_strategy,
                    },
                },
                'fields': 'userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)',
            }
        })

    # ---- 7. Alternating row colors via addBanding (survives sorting) ----
    # Banding applies to player rows only (team/opp rows get separate styling)
    if n_player_rows > 0:
        requests.append({
            'addBanding': {
                'bandedRange': {
                    'range': _range(ws_id, data_start, data_start + n_player_rows, 0, n_cols),
                    'rowProperties': {
                        'firstBandColor': get_color_for_raw(COLORS[fmt['row_even_bg']]),
                        'secondBandColor': get_color_for_raw(COLORS[fmt['row_odd_bg']]),
                    },
                },
            }
        })
    # Team and opponent rows get white background
    if n_data_rows > n_player_rows:
        team_opp_start = data_start + n_player_rows
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, team_opp_start, total_rows, 0, n_cols),
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': get_color_for_raw(COLORS['white']),
                    },
                },
                'fields': 'userEnteredFormat.backgroundColor',
            }
        })

    # ---- 8. Left-aligned columns (data rows only) — config-driven ----
    for col_key in fmt.get('left_align_columns', []):
        col_idx = get_column_index(col_key, columns_list)
        if col_idx is not None and n_data_rows > 0:
            requests.append({
                'repeatCell': {
                    'range': _range(ws_id, data_start, total_rows, col_idx, col_idx + 1),
                    'cell': {
                        'userEnteredFormat': {'horizontalAlignment': 'LEFT'},
                    },
                    'fields': 'userEnteredFormat.horizontalAlignment',
                }
            })

    # ---- 8b. Bold columns (data rows only) — config-driven ----
    for col_key in fmt.get('bold_columns', []):
        col_idx = get_column_index(col_key, columns_list)
        if col_idx is not None and n_data_rows > 0:
            requests.append({
                'repeatCell': {
                    'range': _range(ws_id, data_start, total_rows, col_idx, col_idx + 1),
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {'bold': True},
                        },
                    },
                    'fields': 'userEnteredFormat.textFormat.bold',
                }
            })

    # ---- 9. Header merge cells ----
    for merge in header_merges:
        row = merge['row']  # Already 0-based (section=0, subsection=1)
        if merge['end_col'] - merge['start_col'] > 1:
            requests.append({
                'mergeCells': {
                    'range': _range(ws_id, row, row + 1,
                                    merge['start_col'], merge['end_col']),
                    'mergeType': 'MERGE_ALL',
                }
            })

    # ---- 10. Section borders (vertical) — weight 2, white in headers, black in data ----
    section_boundaries = _get_section_boundaries(columns_list)
    for boundary_col in section_boundaries:
        # Header portion — white border
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, 0, header_end, boundary_col, boundary_col + 1),
                'left': _border_style_v2(border_weight, header_border_color),
            }
        })
        # Data portion — black border
        if n_data_rows > 0:
            requests.append({
                'updateBorders': {
                    'range': _range(ws_id, data_start, total_rows, boundary_col, boundary_col + 1),
                    'left': _border_style_v2(border_weight, data_border_color),
                }
            })

    # ---- 11. Subsection borders — ONLY when advanced columns are visible ----
    # In basic mode (hide_advanced_columns=True) subsection borders are hidden
    # since basic columns form a single group; they only help when advanced
    # stats are toggled on and need visual separation.
    if not fmt.get('hide_advanced_columns', True):
        subsection_boundaries = _get_subsection_boundaries(columns_list)
        for boundary_col in subsection_boundaries:
            # Header portion — white border
            requests.append({
                'updateBorders': {
                    'range': _range(ws_id, 0, header_end, boundary_col, boundary_col + 1),
                    'left': _border_style_v2(border_weight, header_border_color),
                }
            })
            # Data portion — black border
            if n_data_rows > 0:
                requests.append({
                    'updateBorders': {
                        'range': _range(ws_id, data_start, total_rows, boundary_col, boundary_col + 1),
                        'left': _border_style_v2(border_weight, data_border_color),
                    }
                })

    # ---- 12. Horizontal borders between header rows — white, weight 2 ----
    for row_idx in [fmt['subsection_header_row'], fmt['column_header_row'], fmt['filter_row']]:
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, row_idx, row_idx + 1, 0, n_cols),
                'top': _border_style_v2(border_weight, header_border_color),
            }
        })

    # ---- 13. (Removed — no horizontal border between headers and data) ----

    # ---- 14. Border above team/opp rows (horizontal divider) — black ----
    if n_player_rows > 0 and n_data_rows > n_player_rows:
        team_row = data_start + n_player_rows
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, team_row, team_row + 1, 0, n_cols),
                'top': _border_style_v2(border_weight, data_border_color),
            }
        })

    # ---- 15. Auto-resize all columns, then enforce config-driven width overrides ----
    requests.append({
        'autoResizeDimensions': {
            'dimensions': {
                'sheetId': ws_id,
                'dimension': 'COLUMNS',
                'startIndex': 0,
                'endIndex': n_cols,
            },
        }
    })
    # Enforce column width overrides from config (only columns with numeric values)
    width_overrides = fmt.get('column_width_overrides', {})
    for col_key, width in width_overrides.items():
        if isinstance(width, (int, float)):
            col_idx = get_column_index(col_key, columns_list)
            if col_idx is not None:
                requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': ws_id,
                            'dimension': 'COLUMNS',
                            'startIndex': col_idx,
                            'endIndex': col_idx + 1,
                        },
                        'properties': {'pixelSize': int(width)},
                        'fields': 'pixelSize',
                    }
                })

    # ---- 16. Hide advanced stat columns ----
    if fmt.get('hide_advanced_columns', True):
        requests.extend(_build_hide_advanced_requests(ws_id, columns_list))

    # ---- 17. Hide percentile columns ----
    if fmt.get('hide_percentile_columns', True):
        requests.extend(_build_hide_percentile_requests(ws_id, columns_list))

    # ---- 18. Hide subsection row ----
    if fmt.get('hide_subsection_row', True):
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': ws_id,
                    'dimension': 'ROWS',
                    'startIndex': fmt['subsection_header_row'],
                    'endIndex': fmt['subsection_header_row'] + 1,
                },
                'properties': {'hiddenByUser': True},
                'fields': 'hiddenByUser',
            }
        })

    # ---- 19. Hide identity section columns ----
    if fmt.get('hide_identity_section', True):
        for idx, entry in enumerate(columns_list):
            col_ctx = entry[3] if len(entry) > 3 else None
            if col_ctx == 'identity':
                requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': ws_id,
                            'dimension': 'COLUMNS',
                            'startIndex': idx,
                            'endIndex': idx + 1,
                        },
                        'properties': {'hiddenByUser': True},
                        'fields': 'hiddenByUser',
                    }
                })

    # ---- 20. Auto-filter on filter row — excludes team/opp rows from sort ----
    filter_end = data_start + n_player_rows if n_player_rows > 0 else total_rows
    requests.append({
        'setBasicFilter': {
            'filter': {
                'range': _range(ws_id, fmt['filter_row'], filter_end, 0, n_cols),
            }
        }
    })

    # ---- 21. Percentile color shading ----
    if percentile_cells:
        requests.extend(_build_percentile_shading_requests(ws_id, percentile_cells))

    # ---- 21b. Column header tooltips (notes) from config 'description' field ----
    requests.extend(_build_tooltip_requests(ws_id, columns_list, fmt['column_header_row']))

    # ---- 22. Black background for cells where entity has no formula ----
    # If team_formula or opponents_formula is None, those cells get black bg
    if n_data_rows > n_player_rows:
        requests.extend(_build_null_formula_bg_requests(
            ws_id, columns_list, data_start, n_player_rows, n_data_rows
        ))

    # ---- 23. Delete extra rows and columns (resize to exact dimensions) ----
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': ws_id,
                'gridProperties': {
                    'rowCount': total_rows,
                    'columnCount': n_cols,
                },
            },
            'fields': 'gridProperties(rowCount,columnCount)',
        }
    })

    return requests


def _range(ws_id: int, start_row: int, end_row: int,
           start_col: int, end_col: int) -> dict:
    """Build a GridRange dict."""
    return {
        'sheetId': ws_id,
        'startRowIndex': start_row,
        'endRowIndex': end_row,
        'startColumnIndex': start_col,
        'endColumnIndex': end_col,
    }


def _border_style(border_config: dict) -> dict:
    """Build a border style dict from legacy config (backwards compat)."""
    return {
        'style': border_config.get('style', 'SOLID'),
        'color': get_color_for_raw(COLORS[border_config.get('color', 'black')]),
    }


def _border_style_v2(weight: int, color: dict) -> dict:
    """Build a border style dict with explicit weight and color."""
    # Google Sheets API uses 'style' with weight encoded as style name
    # weight 1 = SOLID, weight 2 = SOLID_MEDIUM, weight 3 = SOLID_THICK
    style_map = {1: 'SOLID', 2: 'SOLID_MEDIUM', 3: 'SOLID_THICK'}
    return {
        'style': style_map.get(weight, 'SOLID_MEDIUM'),
        'color': color,
    }


def _get_section_boundaries(columns_list: List[Tuple]) -> List[int]:
    """Get column indices where sections change (for vertical borders).
    Skips the boundary after the 'entities' section — entities gets no right border."""
    boundaries = []
    prev_section = None
    for idx, entry in enumerate(columns_list):
        col_ctx = entry[3] if len(entry) > 3 else None
        if col_ctx != prev_section and prev_section is not None:
            # Skip the border between entities and the next section
            if prev_section != 'entities':
                boundaries.append(idx)
        prev_section = col_ctx
    return boundaries


def _get_subsection_boundaries(columns_list: List[Tuple]) -> List[int]:
    """Get column indices where subsections change within stats sections."""
    boundaries = []
    prev_subsection = None
    prev_section = None
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        if not col_ctx_cfg.get('is_stats_section'):
            prev_subsection = None
            prev_section = col_ctx
            continue
        subsection = col_def.get('subsection')
        # New subsection within same section
        if subsection != prev_subsection and prev_subsection is not None and col_ctx == prev_section:
            boundaries.append(idx)
        prev_subsection = subsection
        prev_section = col_ctx
    return boundaries


def _build_hide_percentile_requests(ws_id: int, columns_list: List[Tuple]) -> list:
    """Build requests to hide all generated percentile columns."""
    requests = []
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        if col_def.get('is_generated_percentile', False):
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': ws_id,
                        'dimension': 'COLUMNS',
                        'startIndex': idx,
                        'endIndex': idx + 1,
                    },
                    'properties': {'hiddenByUser': True},
                    'fields': 'hiddenByUser',
                }
            })
    return requests


def _build_hide_advanced_requests(ws_id: int, columns_list: List[Tuple]) -> list:
    """Build requests to hide advanced stat columns."""
    requests = []
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        if col_ctx_cfg.get('is_stats_section') and col_def.get('stat_mode') == 'advanced':
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': ws_id,
                        'dimension': 'COLUMNS',
                        'startIndex': idx,
                        'endIndex': idx + 1,
                    },
                    'properties': {'hiddenByUser': True},
                    'fields': 'hiddenByUser',
                }
            })
    return requests


def _build_tooltip_requests(ws_id: int, columns_list: List[Tuple],
                            header_row: int) -> list:
    """Build requests to set notes (tooltips) on column header cells.
    Reads 'description' from each column definition in config."""
    requests = []
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        description = col_def.get('description')
        if not description:
            continue
        requests.append({
            'updateCells': {
                'range': _range(ws_id, header_row, header_row + 1, idx, idx + 1),
                'rows': [{
                    'values': [{
                        'note': description,
                    }],
                }],
                'fields': 'note',
            }
        })
    return requests


def _build_null_formula_bg_requests(ws_id: int, columns_list: List[Tuple],
                                     data_start: int, n_player_rows: int,
                                     n_data_rows: int) -> list:
    """
    Build requests to set black background on team/opponent row cells
    where the column's team_formula or opponents_formula is None.
    Config-driven: reads formula presence from column definitions.
    """
    black = get_color_for_raw(COLORS['black'])
    requests = []
    team_row = data_start + n_player_rows      # First row after players = team
    opp_row = data_start + n_player_rows + 1   # Second row after players = opponents

    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        # Team row: black bg if team_formula is None
        if col_def.get('team_formula') is None:
            requests.append({
                'repeatCell': {
                    'range': _range(ws_id, team_row, team_row + 1, idx, idx + 1),
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': black,
                        },
                    },
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })
        # Opponents row: black bg if opponents_formula is None
        if col_def.get('opponents_formula') is None:
            if opp_row < data_start + n_data_rows:
                requests.append({
                    'repeatCell': {
                        'range': _range(ws_id, opp_row, opp_row + 1, idx, idx + 1),
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': black,
                            },
                        },
                        'fields': 'userEnteredFormat.backgroundColor',
                    }
                })
    return requests


def _build_percentile_shading_requests(ws_id: int,
                                        percentile_cells: List[dict]) -> list:
    """Build cell background color requests for percentile shading.

    NOTE: percentile rank already accounts for reverse_percentile direction
    (get_percentile_rank inverts so high rank = good always).
    Do NOT pass reverse to get_color_for_percentile — that would double-invert.
    """
    requests = []
    for cell in percentile_cells:
        color = get_color_for_percentile(cell['percentile'])
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, cell['row'], cell['row'] + 1,
                                cell['col'], cell['col'] + 1),
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': get_color_for_raw(color),
                    },
                },
                'fields': 'userEnteredFormat.backgroundColor',
            }
        })
    return requests


def build_merged_entity_row(player_id, columns_list: List[Tuple],
                            current_data: Optional[dict],
                            historical_data: Optional[dict],
                            postseason_data: Optional[dict],
                            pct_curr: dict, pct_hist: dict, pct_post: dict,
                            entity_type: str = 'player',
                            mode: str = 'per_game',
                            hist_years: str = '', post_years: str = '') -> Tuple[list, List[dict]]:
    """
    Build a single merged data row with current + historical + postseason stats.

    Returns (row_values, percentile_cells) where percentile_cells is a list of
    {col: col_idx, percentile: rank, reverse: bool} dicts (row is set by caller).
    """
    section_data = {}
    if current_data:
        section_data['current_stats'] = (current_data, pct_curr, '')
    if historical_data:
        section_data['historical_stats'] = (historical_data, pct_hist, hist_years)
    if postseason_data:
        section_data['postseason_stats'] = (postseason_data, pct_post, post_years)

    # Use first available entity data for non-stats columns
    primary_entity = current_data or historical_data or postseason_data or {}

    row = build_entity_row(
        primary_entity, columns_list, {},
        entity_type=entity_type, mode=mode,
        section_data=section_data,
    )

    # Collect percentile info for shading
    percentile_cells = []
    for col_idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        if not col_def.get('has_percentile', False) and not is_pct:
            continue

        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        is_stats_section = col_ctx_cfg.get('is_stats_section', False)

        if is_stats_section:
            # Stats section — use the section-specific percentile populations
            if col_ctx in section_data:
                sec_entity, sec_pcts, _ = section_data[col_ctx]
                calculated = calculate_entity_stats(sec_entity, entity_type, mode)
                base_key = col_def.get('base_stat', col_key.replace('_pct', '')) if is_pct else col_key
                base_def = SHEETS_COLUMNS.get(base_key, col_def)
                value = calculated.get(base_key)

                if value is not None and base_key in sec_pcts:
                    reverse = base_def.get('reverse_percentile', False)
                    weight = _get_minute_weight(base_key, sec_entity)
                    rank = get_percentile_rank(value, sec_pcts[base_key], reverse, weight)
                    percentile_cells.append({
                        'col': col_idx,
                        'percentile': rank,
                        'reverse': reverse,
                    })
        else:
            # Non-stats section (player_info: age, height, weight, wingspan)
            # Use current_stats percentile population for player_info columns
            if 'current_stats' in section_data:
                sec_entity, sec_pcts, _ = section_data['current_stats']
            elif section_data:
                first_key = next(iter(section_data))
                sec_entity, sec_pcts, _ = section_data[first_key]
            else:
                continue
            calculated = calculate_entity_stats(sec_entity, entity_type, mode)
            base_key = col_def.get('base_stat', col_key.replace('_pct', '')) if is_pct else col_key
            base_def = SHEETS_COLUMNS.get(base_key, col_def)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('reverse_percentile', False)
                weight = _get_minute_weight(base_key, sec_entity)
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse, weight)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })

    return row, percentile_cells


def create_text_format(font_family=None, font_size=None, bold=False,
                       foreground_color='white') -> dict:
    """Create a text format dict for Google Sheets API."""
    fmt = {'foregroundColor': get_color_dict(foreground_color), 'bold': bold}
    if font_family:
        fmt['fontFamily'] = font_family
    if font_size:
        fmt['fontSize'] = font_size
    return fmt


def create_cell_format(background_color='white', text_format=None,
                       h_align='CENTER', v_align='MIDDLE', wrap='CLIP') -> dict:
    """Create a complete cell format dict for Google Sheets API."""
    cf = {
        'backgroundColor': get_color_dict(background_color),
        'horizontalAlignment': h_align,
        'verticalAlignment': v_align,
        'wrapStrategy': wrap
    }
    if text_format:
        cf['textFormat'] = text_format
    return cf


# ============================================================================
# API CONFIG EXPORT
# ============================================================================

def get_reverse_stats() -> List[str]:
    """Get list of stat column keys where lower is better."""
    return [k for k, v in SHEETS_COLUMNS.items() if v.get('reverse_percentile', False)]


def get_editable_fields() -> List[str]:
    """Get list of field names that users can edit (wingspan, notes, hand)."""
    fields = []
    for col_key, col_def in SHEETS_COLUMNS.items():
        if col_def.get('editable', False):
            # Get the actual DB field from the player_formula
            formula = col_def.get('player_formula')
            if formula and not any(op in formula for op in '+-*/('):
                fields.append(formula)
    return fields


def get_config_for_export(mode: str = 'per_100') -> dict:
    """
    Build JSON-serializable config for /api/config endpoint.
    Apps Script uses this as single source of truth — zero hardcoding in JS.

    Exports:
      - column_ranges:            section toggle ranges (team_sheet / nba_sheet)
      - advanced_column_ranges:   toggle advanced stat columns
      - percentile_column_ranges: toggle percentile columns
      - column_indices:           edit-detection indices (wingspan, notes, team)
    """
    from lib.etl import get_teams_from_db

    # --- NBA teams dict --------------------------------------------------
    teams_from_db = get_teams_from_db()
    nba_teams = {abbr: team_id for team_id, (abbr, name) in teams_from_db.items()}

    # --- Stat columns list -----------------------------------------------
    stat_columns = [k for k, v in SHEETS_COLUMNS.items() if v.get('is_stat', False)]

    # --- Build full column lists for both sheet types --------------------
    team_columns = build_sheet_columns(
        entity='player', stat_mode='both',
        show_percentiles=True, sheet_type='team'
    )
    nba_columns = build_sheet_columns(
        entity='player', stat_mode='both',
        show_percentiles=True, sheet_type='nba'
    )

    # --- Helper: find contiguous ranges of matching column indices --------
    def _contiguous_ranges(indices):
        """Convert sorted list of 0-based indices to list of
        {'start': 1-based, 'count': N} contiguous ranges."""
        if not indices:
            return []
        ranges = []
        start = indices[0]
        prev = indices[0]
        for idx in indices[1:]:
            if idx == prev + 1:
                prev = idx
            else:
                ranges.append({'start': start + 1, 'count': prev - start + 1})
                start = idx
                prev = idx
        ranges.append({'start': start + 1, 'count': prev - start + 1})
        return ranges

    # --- Section toggle ranges -------------------------------------------
    def _section_range(cols, section_name):
        indices = [i for i, entry in enumerate(cols)
                   if (entry[3] if len(entry) > 3 else None) == section_name]
        if not indices:
            return None
        return {'start': min(indices) + 1, 'count': len(indices)}  # 1-indexed

    column_ranges = {'team_sheet': {}, 'nba_sheet': {}}
    _sec_rename = {'analysis': 'notes'}
    for sec in ('current_stats', 'historical_stats', 'postseason_stats',
                'player_info', 'analysis'):
        key = _sec_rename.get(sec, sec.replace('_stats', ''))
        team_range = _section_range(team_columns, sec)
        nba_range = _section_range(nba_columns, sec)
        if team_range:
            column_ranges['team_sheet'][key] = team_range
        if nba_range:
            column_ranges['nba_sheet'][key] = nba_range

    # --- Advanced column ranges ------------------------------------------
    def _advanced_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('stat_mode') == 'advanced'
        ])

    advanced_column_ranges = {
        'team_sheet': _contiguous_ranges(_advanced_indices(team_columns)),
        'nba_sheet':  _contiguous_ranges(_advanced_indices(nba_columns)),
    }

    # --- Percentile column ranges ----------------------------------------
    def _percentile_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('is_generated_percentile', False)
        ])

    percentile_column_ranges = {
        'team_sheet': _contiguous_ranges(_percentile_indices(team_columns)),
        'nba_sheet':  _contiguous_ranges(_percentile_indices(nba_columns)),
    }

    # --- Column indices for edit detection (1-indexed for Sheets) --------
    wingspan_idx = get_column_index('wingspan', team_columns)
    notes_idx = get_column_index('notes', team_columns)
    nba_id_idx = get_column_index('nba_id', team_columns)
    team_col_idx = get_column_index('team', nba_columns)
    stats_start = None
    for i, entry in enumerate(team_columns):
        if entry[1].get('is_stat', False):
            stats_start = i + 1  # 1-indexed
            break

    return {
        'api_base_url': f"http://{SERVER_CONFIG['production_host']}:{SERVER_CONFIG['production_port']}",
        'sheet_id': GOOGLE_SHEETS_CONFIG.get('spreadsheet_id', ''),
        'nba_teams': nba_teams,
        'stat_columns': stat_columns,
        'reverse_stats': get_reverse_stats(),
        'editable_fields': get_editable_fields(),
        'column_indices': {
            'wingspan': (wingspan_idx or 0) + 1,
            'notes': (notes_idx or 0) + 1,
            'player_id': (nba_id_idx or 0) + 1,
            'team': (team_col_idx or 0) + 1,
            'stats_start': stats_start or 9,
        },
        'column_ranges': column_ranges,
        'advanced_column_ranges': advanced_column_ranges,
        'percentile_column_ranges': percentile_column_ranges,
        'colors': {
            'red': {'r': int(COLORS['red']['red'] * 255), 'g': int(COLORS['red']['green'] * 255), 'b': int(COLORS['red']['blue'] * 255)},
            'yellow': {'r': int(COLORS['yellow']['red'] * 255), 'g': int(COLORS['yellow']['green'] * 255), 'b': int(COLORS['yellow']['blue'] * 255)},
            'green': {'r': int(COLORS['green']['red'] * 255), 'g': int(COLORS['green']['green'] * 255), 'b': int(COLORS['green']['blue'] * 255)},
        },
        'color_thresholds': COLOR_THRESHOLDS,
        'layout': {
            'header_row_count': SHEET_FORMATTING['header_row_count'],
            'data_start_row': SHEET_FORMATTING['data_start_row'],
            'frozen_rows': SHEET_FORMATTING['frozen_rows'],
            'frozen_cols': SHEET_FORMATTING['frozen_cols'],
        },
        'sections': {k: v for k, v in SECTION_CONFIG.items()},
        'subsections': SUBSECTIONS,
    }


# ============================================================================
# API RESPONSE CACHE
# ============================================================================

_stat_cache: Dict[str, Tuple[float, Any]] = {}


def _cache_key(*args) -> str:
    """Build a deterministic cache key from arguments."""
    raw = json.dumps(args, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


def get_cached_stats(key: str) -> Optional[Any]:
    """Get cached stats if TTL hasn't expired."""
    if key in _stat_cache:
        timestamp, data = _stat_cache[key]
        if time.time() - timestamp < STAT_CONSTANTS['cache_ttl_seconds']:
            return data
        del _stat_cache[key]
    return None


def set_cached_stats(key: str, data: Any):
    """Cache stats with current timestamp."""
    _stat_cache[key] = (time.time(), data)


def clear_cache():
    """Clear the entire stats cache."""
    _stat_cache.clear()
