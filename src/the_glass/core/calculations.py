import logging
from typing import Dict, Any, List, Optional
from src.sheets.config import SHEETS_COLUMNS, SECTION_CONFIG, STAT_CONSTANTS

logger = logging.getLogger(__name__)

# ============================================================================
# TUPLE-TREE EXPRESSION EVALUATOR
# ============================================================================

def evaluate_expression(expr, entity_data: dict,
                        context: Optional[dict] = None) -> Any:
    """Recursively evaluate an expression tree built by formulas.py.

    Args:
        expr: One of:
            - tuple tree: ('op', arg1, arg2, ...) from expression builders
            - str template: '{field}' -> field lookup
            - str literal: 'TEAM' (uppercase start) -> returned as-is
            - bare str: 'field_name' -> entity_data[field_name]
            - int/float -> literal number
            - None -> None
        entity_data: dict of entity field values from the database
        context: optional runtime values:
            - 'seasons_in_query': int
            - 'lookup_tables': {table: {key: {field: val}}}
            - 'team_players': list of player dicts for team_average
    """
    if expr is None:
        return None

    if isinstance(expr, (int, float)):
        return expr

    if isinstance(expr, str):
        if expr.startswith('{') and expr.endswith('}'):
            return entity_data.get(expr[1:-1])
        if expr and expr[0].isupper():
            return expr
        return entity_data.get(expr)

    if not isinstance(expr, tuple) or not expr:
        return None

    op = expr[0]

    if op == 'seasons_in_query':
        return (context or {}).get('seasons_in_query', 1)

    if op == 'add':
        values = [evaluate_expression(arg, entity_data, context) for arg in expr[1:]]
        if any(v is None for v in values):
            return None
        return sum(values)

    if op == 'subtract':
        a = evaluate_expression(expr[1], entity_data, context)
        b = evaluate_expression(expr[2], entity_data, context)
        if a is None or b is None:
            return None
        return a - b

    if op == 'multiply':
        a = evaluate_expression(expr[1], entity_data, context)
        b = evaluate_expression(expr[2], entity_data, context)
        if a is None or b is None:
            return None
        return a * b

    if op == 'divide':
        a = evaluate_expression(expr[1], entity_data, context)
        b = evaluate_expression(expr[2], entity_data, context)
        if a is None or b is None or b == 0:
            return None
        return a / b

    if op == 'lookup':
        key_field, table, target_field = expr[1], expr[2], expr[3]
        key_value = entity_data.get(key_field)
        if key_value is None:
            return None
        lookup_tables = (context or {}).get('lookup_tables', {})
        table_data = lookup_tables.get(table, {})
        entry = table_data.get(key_value)
        if entry is None:
            return None
        return entry.get(target_field)

    if op == 'team_average':
        field = expr[1]
        team_players = (context or {}).get('team_players', [])
        if not team_players:
            return None
        total_weight = 0.0
        weighted_sum = 0.0
        for p in team_players:
            val = p.get(field)
            minutes = (p.get('minutes_x10', 0) or 0) / 10.0
            if val is not None and minutes > 0:
                weighted_sum += val * minutes
                total_weight += minutes
        if total_weight == 0:
            return None
        return weighted_sum / total_weight

    logger.warning(f"Unknown expression operator: {op}")
    return None


# ============================================================================
# FORMULA EVALUATION
# ============================================================================

def evaluate_formula(col_key: str, entity_data: dict,
                     entity_type: str = 'player', mode: str = 'per_possession',
                     context: Optional[dict] = None) -> Any:
    """Evaluate a column's value expression against entity data.

    Resolves the expression from col_def['values'][entity_type] and
    walks the tuple tree via evaluate_expression().
    """
    col_def = SHEETS_COLUMNS.get(col_key)
    if not col_def:
        return None

    values = col_def.get('values', {})
    expr = values.get(entity_type)
    if expr is None:
        return None

    return evaluate_expression(expr, entity_data, context)


def _apply_scaling(raw_value: Any, mode: str, games: float, minutes: float,
                   possessions: float) -> Any:
    """Apply mode-based scaling to a raw stat value.

    Scaling factors are driven by STAT_CONSTANTS so changing the base
    (e.g. 40 mins -> 48 mins) only requires a config update.
    """
    if raw_value is None or raw_value == 0:
        return raw_value

    if mode == 'per_game':
        return raw_value / max(games, 1)
    elif mode == 'per_minute':
        per_min_base = STAT_CONSTANTS['default_per_minute']
        return raw_value * per_min_base / max(minutes, 0.1)
    elif mode == 'per_possession':
        per_poss_base = STAT_CONSTANTS['default_per_possessions']
        return raw_value * per_poss_base / max(possessions, 1)

    return raw_value


def calculate_entity_stats(entity_data: dict, entity_type: str = 'player',
                           mode: str = 'per_possession',
                           context: Optional[dict] = None) -> dict:
    """
    Calculate all stat values for an entity in a given mode.

    Returns dict of {col_key: calculated_value} for all applicable columns.
    """
    results = {}
    games = entity_data.get('games', 0) or 0
    minutes = (entity_data.get('minutes_x10', 0) or 0) / 10.0
    possessions = entity_data.get('possessions', 0) or 0

    for col_key, col_def in SHEETS_COLUMNS.items():
        values = col_def.get('values', {})
        if entity_type not in values:
            continue

        raw_value = evaluate_formula(col_key, entity_data, entity_type, mode, context)

        if raw_value is None:
            results[col_key] = None
            continue

        scale = col_def.get('scale_with_rate', False)

        if scale is True:
            results[col_key] = _apply_scaling(raw_value, mode, games, minutes, possessions)
        elif scale == 'per_game_only':
            results[col_key] = raw_value / max(games, 1)
        else:
            results[col_key] = raw_value

    return results


# ============================================================================
# DB HELPERS & FETCH FUNCTIONS — live in league-specific wrappers
# (nba_sheets_lib.py / ncaa_sheets_lib.py) because SQL differs per league.
# ============================================================================


def calculate_all_percentiles(all_entities: List[dict], entity_type: str,
                              mode: str = 'per_possession',
                              context: Optional[dict] = None) -> dict:
    """
    Calculate minute-weighted percentile populations for all stat columns.

    Columns in stats sections are weighted by minutes played.
    Non-stats columns (player_info, etc.) use weight = 1.

    Returns:
        Dict of {col_key: sorted list of (value, weight) tuples}
    """
    all_calculated = []
    for entity in all_entities:
        stats = calculate_entity_stats(entity, entity_type, mode, context)
        all_calculated.append((entity, stats))

    percentiles = {}
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('percentile'):
            continue

        is_stats = any(
            SECTION_CONFIG.get(s, {}).get('is_stats_section', False)
            for s in col_def.get('sections', [])
        )

        entries = []
        for entity, stats in all_calculated:
            val = stats.get(col_key)
            if val is None or not isinstance(val, (int, float)):
                continue

            if is_stats:
                raw_minutes = (entity.get('minutes_x10', 0) or 0) / 10.0
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