import re
import logging
from typing import Dict, List, Optional, Any, Tuple
from bisect import bisect_left, bisect_right
from src.sheets.config import SHEETS_COLUMNS
from src.sheets.config import (SECTION_CONFIG, SECTIONS, SUBSECTIONS, STAT_CONSTANTS, DEFAULT_STAT_MODE, COLORS, COLOR_THRESHOLDS, SHEET_FORMATTING)
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


# Formula compilation is triggered by init_engine() — NOT at import time.


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
        # Distinguish "field exists but is NULL" from "field not in dataset"
        # NULL in DB → None → empty cell, no percentile color
        # Field absent for stat columns → 0 → safe default for calculations
        # Field absent for non-stat columns → '' → empty cell (e.g., notes on teams)
        if compiled in entity_data:
            # Non-nullable columns (games, seasons) → 0 instead of None
            if not col_def.get('nullable', True):
                return 0
            return None
        # Non-stat columns (notes, hand, etc.) should show empty, not 0
        if col_def.get('stat_category', 'none') == 'none':
            return ''
        return 0

    # Evaluate compiled expression
    try:
        # For nullable columns, return None if any source field is NULL in DB
        if col_def.get('nullable', True):
            formula_str = col_def.get(ftype)
            if formula_str:
                for var_name in _extract_formula_variables(formula_str):
                    if var_name in entity_data and entity_data[var_name] is None:
                        return None
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

def _eval_dynamic_formula(formula_str: str, entity_data: dict,
                          col_def: dict, mode: str) -> Any:
    """Evaluate a formula string directly against entity data with scaling.

    Used for dynamically-generated opponent columns that don't exist in
    SHEETS_COLUMNS and therefore aren't handled by calculate_entity_stats.
    """
    if not formula_str or not entity_data:
        return None
    try:
        # Check for None source fields
        if col_def.get('nullable', True):
            for var_name in _extract_formula_variables(formula_str):
                if var_name in entity_data and entity_data[var_name] is None:
                    return None

        # Simple field lookup or expression
        if formula_str.isidentifier():
            raw = entity_data.get(formula_str)
        else:
            local_vars = _sanitize_entity_data(entity_data)
            local_vars['STAT_CONSTANTS'] = STAT_CONSTANTS
            compiled = compile(formula_str, '<dynamic>', 'eval')
            raw = eval(compiled, {"__builtins__": {}}, local_vars)

        if raw is None:
            return None

        # Apply mode-based scaling
        scale = col_def.get('scale_with_mode', False)
        if not scale:
            return raw
        games = entity_data.get('games', 0) or 0
        minutes = (entity_data.get('minutes_x10', 0) or 0) / 10.0
        possessions = entity_data.get('possessions', 0) or 0
        if scale == 'per_game_only':
            return raw / max(games, 1)
        return _apply_scaling(raw, mode, games, minutes, possessions)
    except (ZeroDivisionError, TypeError, ValueError, NameError):
        return None


def _apply_scaling(raw_value: Any, mode: str, games: float, minutes: float,
                   possessions: float) -> Any:
    """Apply mode-based scaling to a raw stat value."""
    if raw_value is None or raw_value == 0:
        return raw_value

    if mode == 'per_game':
        return raw_value / max(games, 1)
    elif mode == f"per_{int(STAT_CONSTANTS.get('default_per_minute', 36))}":
        return raw_value * STAT_CONSTANTS.get('default_per_minute', 36.0) / max(minutes, 0.1)
    elif mode == 'per_100':
        return raw_value * STAT_CONSTANTS['default_per_possessions'] / max(possessions, 1)

    return raw_value


def calculate_entity_stats(entity_data: dict, entity_type: str = 'player',
                           mode: str = 'per_game') -> dict:
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
            if scale is True:
                results[col_key] = _apply_scaling(raw_value, mode, games, minutes, possessions)
            elif scale == 'per_game_only':
                results[col_key] = raw_value / max(games, 1)
            else:
                results[col_key] = raw_value
            continue

        # Normal formula (no override)
        if scale is True:
            results[col_key] = _apply_scaling(raw_value, mode, games, minutes, possessions)
        elif scale == 'per_game_only':
            results[col_key] = raw_value / max(games, 1)
        else:
            results[col_key] = raw_value  # ratio/percentage — no scaling

    return results


# ============================================================================
# DB HELPERS & FETCH FUNCTIONS — live in league-specific wrappers
# (nba_sheets_lib.py / ncaa_sheets_lib.py) because SQL differs per league.
# ============================================================================


def _quote_col(col: str) -> str:
    """Quote a column name for SQL. Needed for digit-starting names like 2fgm."""
    return f'"{col}"'


# _build_season_filter lives in league-specific wrappers because SQL column
# names differ (NBA uses 'season', NCAA uses 'season').


def calculate_all_percentiles(all_entities: List[dict], entity_type: str,
                              mode: str = 'per_game') -> dict:
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
        stats = calculate_entity_stats(entity, entity_type, mode)
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