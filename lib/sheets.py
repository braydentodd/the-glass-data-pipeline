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
        return val if val is not None else 0

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
    Extract all unique DB column names referenced by stat formulas.
    These are the columns we need to SELECT/SUM from the stats table.
    """
    fields = set()
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('is_stat', False):
            continue
        for ftype in ('player_formula', 'team_formula', 'opponents_formula'):
            formula = col_def.get(ftype)
            if formula:
                fields.update(_extract_formula_variables(formula))
        # Also from mode_overrides
        for mode, override in col_def.get('mode_overrides', {}).items():
            for ftype in ('player_formula', 'team_formula', 'opponents_formula'):
                formula = override.get(ftype)
                if formula:
                    fields.update(_extract_formula_variables(formula))
    return fields


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
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    # Entity fields to select
    p_fields = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                if f not in ('updated_at', 'birthdate')]
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
            cur.execute(query, (current_year, season_type, team_abbr))
            return [dict(r) for r in cur.fetchall()]

    else:
        # Historical or postseason — aggregate across years
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')
        all_fields = p_fields + t_fields + s_fields
        group_fields = p_fields + t_fields

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
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']

    players_table = get_table_name('player', 'entity')
    teams_table = get_table_name('team', 'entity')
    stats_table = get_table_name('player', 'stats')

    p_fields = [f'p.{_quote_col(f)}' for f in sorted(_PLAYER_ENTITY_FIELDS)
                if f not in ('updated_at', 'birthdate')]
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
            cur.execute(query, (current_year, season_type))
            return [dict(r) for r in cur.fetchall()]

    else:
        st = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, st)

        s_fields = [f'SUM(s.{_quote_col(f)}) AS {_quote_col(f)}' for f in sorted(_STAT_TABLE_FIELDS)]
        s_fields.append('COUNT(DISTINCT s.year) AS year')
        all_fields = p_fields + t_fields + s_fields
        group_fields = p_fields + t_fields

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
            cur.execute(query, (current_year, season_type, team_abbr))
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
            cur.execute(query, (current_year, season_type))
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


def _build_year_filter(years_config: Optional[dict], current_year: int,
                       season_type) -> Tuple:
    """Build SQL year filter clause and params tuple."""
    if not years_config:
        years = [current_year - i for i in range(1, 4)]
        return "AND s.year IN %s", (tuple(years),)

    mode = years_config.get('mode', 'years')
    value = years_config.get('value', 3)
    include_current = years_config.get('include_current', False)

    if mode == 'career':
        return "AND s.year > 0", ()
    elif mode == 'years':
        start = 0 if include_current else 1
        years = [current_year - i for i in range(start, start + value)]
        return "AND s.year IN %s", (tuple(years),)
    elif mode == 'seasons':
        return "AND s.year IN %s", (tuple(value),)
    else:
        return "AND s.year > 0", ()


# ============================================================================
# PERCENTILE CALCULATIONS
# ============================================================================

def calculate_all_percentiles(all_entities: List[dict], entity_type: str,
                              mode: str = 'per_game',
                              custom_value: Any = None) -> dict:
    """
    Calculate percentile populations for all stat columns.

    Args:
        all_entities: List of raw entity data dicts (from fetch_all_*)
        entity_type: 'player', 'team', or 'opponents'
        mode: Stats mode for formula evaluation
        custom_value: Custom value for per_minutes/per_possessions modes

    Returns:
        Dict of {col_key: sorted_values_list} for percentile lookups
    """
    # First, calculate stats for every entity
    all_calculated = []
    for entity in all_entities:
        stats = calculate_entity_stats(entity, entity_type, mode, custom_value)
        all_calculated.append(stats)

    # Build sorted value arrays for each stat column with has_percentile
    percentiles = {}
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('has_percentile', False):
            continue
        values = []
        for stats in all_calculated:
            val = stats.get(col_key)
            if val is not None:
                values.append(val)
        if values:
            percentiles[col_key] = sorted(values)

    return percentiles


def get_percentile_rank(value: Any, sorted_values: List, reverse: bool = False) -> float:
    """
    Calculate percentile rank using binary search on pre-sorted values.

    Args:
        value: The value to rank
        sorted_values: Pre-sorted list of all values
        reverse: True if lower is better (turnovers, fouls)

    Returns:
        Percentile rank 0-100
    """
    if not sorted_values or value is None:
        return 50.0

    n = len(sorted_values)
    # Count how many values this beats
    pos = bisect_left(sorted_values, value)

    if reverse:
        percentile = (1 - pos / n) * 100
    else:
        percentile = (pos / n) * 100

    return max(0, min(100, percentile))


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
            'display_name': f"{col_def['display_name']}%",
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
                        show_percentiles: bool = False) -> List[Tuple]:
    """
    Build complete column structure for a sheet.

    Returns list of (column_key, column_def, visible, context_section) tuples.
    context_section is the section block this column lives in — used by
    build_entity_row to blank out wrong-section stats on a given row.

    Stats section columns (is_stats_section=True) appear once per section:
    a column with section=['current_stats','historical_stats','postseason_stats']
    appears three times, once in each block.
    Non-stats columns (player_info, entities, identity) appear once.
    """
    all_columns = []

    for section in SECTIONS:
        section_cols = get_columns_for_section_and_entity(
            section=section, entity=entity,
            stat_mode=stat_mode, include_percentiles=True
        )
        for col_key, col_def in section_cols:
            is_pct = col_def.get('is_generated_percentile', False)
            if is_pct:
                visible = show_percentiles
            else:
                visible = not show_percentiles or not col_def.get('has_percentile', False)
            all_columns.append((col_key, col_def, visible, section))

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

def build_headers(columns_list: List[Tuple], mode: str = 'per_game') -> dict:
    """
    Build header rows for Google Sheets.

    Returns dict with row1 (sections), row2 (subsections), row3 (column names),
    row4 (empty), and merges list.

    Uses context_section (4th tuple element) for section header grouping,
    so repeated stat columns (pts in current / pts in historical) display
    under the correct section header.
    """
    row1, row2, row3 = [], [], []
    merges = []
    cur_section = cur_subsection = None
    sec_start = sub_start = 0

    for idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        # Use context_section (section block position) not col_def.section list
        section = entry[3] if len(entry) > 3 else (col_def.get('section', ['unknown'])[0])
        subsection = col_def.get('subsection')

        # Row 1: Section headers
        if section != cur_section:
            if cur_section is not None and sec_start < idx:
                display = SECTION_CONFIG.get(cur_section, {}).get('display_name', cur_section)
                merges.append({'row': 0, 'start_col': sec_start, 'end_col': idx, 'value': display})
            cur_section = section
            sec_start = idx
            row1.append(SECTION_CONFIG.get(section, {}).get('display_name', section))
        else:
            row1.append('')

        # Row 2: Subsection headers
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

        # Row 3: Column display names (use mode override if applicable)
        override = col_def.get('mode_overrides', {}).get(mode)
        display_name = (override or {}).get('display_name', col_def.get('display_name', col_key))
        row3.append(display_name)

    # Close final merges
    n = len(columns_list)
    if cur_section:
        display = SECTION_CONFIG.get(cur_section, {}).get('display_name', cur_section)
        merges.append({'row': 0, 'start_col': sec_start, 'end_col': n, 'value': display})
    if cur_subsection:
        merges.append({'row': 1, 'start_col': sub_start, 'end_col': n, 'value': cur_subsection.title()})

    return {
        'row1': row1, 'row2': row2, 'row3': row3,
        'row4': [''] * n,
        'merges': merges
    }


# ============================================================================
# ROW BUILDING
# ============================================================================

def build_entity_row(entity_data: dict, columns_list: List[Tuple],
                     percentiles: dict, entity_type: str = 'player',
                     mode: str = 'per_game', custom_value: Any = None,
                     years_str: str = '',
                     row_section: Optional[str] = None) -> list:
    """
    Build a single data row for any entity type.

    Evaluates all formulas, applies scaling, calculates percentile rank,
    and formats values. 100% config-driven.

    Args:
        row_section: The section this row's data comes from
            (e.g. 'current_stats'). Stats-section columns whose
            context_section != row_section are left blank so that
            e.g. historical columns don't show current-season values.
    """
    # Pre-calculate all stats for this entity
    calculated = calculate_entity_stats(entity_data, entity_type, mode, custom_value)
    row = []

    for entry in columns_list:
        col_key, col_def, visible = entry[0], entry[1], entry[2]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        # Blank out stats-section columns that don't belong to this row's section
        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        if row_section and col_ctx_cfg.get('is_stats_section') and col_ctx != row_section:
            row.append('')
            continue

        if is_pct:
            # Percentile column — look up rank from pre-calculated populations
            base_key = col_def.get('base_stat', col_key.replace('_pct', ''))
            base_def = SHEETS_COLUMNS.get(base_key, {})
            value = calculated.get(base_key)

            if value is not None and base_key in percentiles:
                reverse = base_def.get('reverse_percentile', False)
                rank = get_percentile_rank(value, percentiles[base_key], reverse)
                row.append(round(rank))
            else:
                row.append('')
            continue

        # Non-percentile column
        if col_key == 'years':
            row.append(years_str or '')
            continue

        # Info column (non-stat) — simple field lookup
        if not col_def.get('is_stat', False):
            value = evaluate_formula(col_key, entity_data, entity_type, mode)
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


def format_years_range(years_config: Optional[dict], current_year: int) -> str:
    """Format year range for display. Returns e.g. '22-23, 23-24, 24-25' or 'Career'."""
    if not years_config:
        years = [current_year - i for i in range(1, 4)]
        return ', '.join(f"{str(y)[-2:]}-{str(y+1)[-2:]}" for y in sorted(years))

    mode = years_config.get('mode', 'years')
    if mode == 'career':
        return 'Career'
    elif mode == 'years':
        value = years_config.get('value', 3)
        include_current = years_config.get('include_current', False)
        start = 0 if include_current else 1
        years = [current_year - i for i in range(start, start + value)]
        return ', '.join(f"{str(y)[-2:]}-{str(y+1)[-2:]}" for y in sorted(years))
    elif mode == 'seasons':
        years = years_config.get('value', [])
        return ', '.join(f"{str(y)[-2:]}-{str(y+1)[-2:]}" for y in sorted(years))
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


def get_config_for_export(mode: str = 'per_game') -> dict:
    """
    Build JSON-serializable config for /api/config endpoint.
    Apps Script uses this as single source of truth — zero hardcoding in JS.
    """
    from lib.etl import get_teams_from_db

    # Build NBA teams dict
    teams_from_db = get_teams_from_db()
    nba_teams = {abbr: team_id for team_id, (abbr, name) in teams_from_db.items()}

    # Build stat columns list
    stat_columns = [k for k, v in SHEETS_COLUMNS.items() if v.get('is_stat', False)]

    # Build column ranges for section toggle (computed from actual column positions)
    columns = build_sheet_columns(entity='player', stat_mode='both', show_percentiles=False)
    nba_columns = build_sheet_columns(entity='player', stat_mode='both', show_percentiles=False)

    def _section_range(cols, section_name):
        # Find columns whose context_section == section_name
        indices = [i for i, entry in enumerate(cols)
                   if (entry[3] if len(entry) > 3 else None) == section_name]
        if not indices:
            return None
        return {'start': min(indices) + 1, 'count': len(indices)}  # 1-indexed for Sheets

    column_ranges = {
        'team_sheet': {},
        'nba_sheet': {},
    }
    for sec in ('current_stats', 'historical_stats', 'postseason_stats', 'player_info'):
        team_range = _section_range(columns, sec)
        if team_range:
            column_ranges['team_sheet'][sec.replace('_stats', '')] = team_range
            # NBA sheet shifted by 1 for Team column
            column_ranges['nba_sheet'][sec.replace('_stats', '')] = {
                'start': team_range['start'] + 1,
                'count': team_range['count']
            }

    # Column indices for edit detection
    wingspan_idx = get_column_index('wingspan', columns)
    notes_idx = get_column_index('notes', columns)
    nba_id_idx = get_column_index('nba_id', columns)
    stats_start = None
    for i, entry in enumerate(columns):
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
            'stats_start': stats_start or 9,
        },
        'column_ranges': column_ranges,
        'colors': {
            'red': {'r': int(COLORS['red']['red'] * 255), 'g': int(COLORS['red']['green'] * 255), 'b': int(COLORS['red']['blue'] * 255)},
            'yellow': {'r': int(COLORS['yellow']['red'] * 255), 'g': int(COLORS['yellow']['green'] * 255), 'b': int(COLORS['yellow']['blue'] * 255)},
            'green': {'r': int(COLORS['green']['red'] * 255), 'g': int(COLORS['green']['green'] * 255), 'b': int(COLORS['green']['blue'] * 255)},
        },
        'color_thresholds': COLOR_THRESHOLDS,
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
