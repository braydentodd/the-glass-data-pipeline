from typing import List, Optional, Any
from src.sheets.config import SHEETS_COLUMNS
from src.sheets.config import (STAT_CONSTANTS, COLORS, COLOR_THRESHOLDS)

def _format_season_label(season_year: int) -> str:
    """Convert end-year integer to season string: 2026 -> '2025-26'."""
    return f"{season_year - 1}-{str(season_year)[2:]}"


def format_stat_value(value: Any, col_def: dict) -> Any:
    """Format a stat value for display according to column definition."""
    if value is None:
        # Non-nullable columns (games, seasons) show 0 instead of blank
        if not col_def.get('nullable', True):
            return 0
        return ''
    if isinstance(value, (int, float)) and value == 0:
        return 0

    fmt = col_def.get('format', 'number')
    decimals = col_def.get('decimal_places', 1)

    if fmt == 'percentage':
        # Value is already 0-100 from formula (e.g., (turnovers/possessions)*100)
        # Do NOT auto-scale — formulas are responsible for correct magnitude.
        rounded = round(value, decimals)
    else:
        rounded = round(value, decimals)

    # Return int if whole number
    if rounded == int(rounded):
        return int(rounded)
    return rounded


def format_height(inches: Any) -> str:
    """Format height in inches to feet-inches string. 80 → 6'8\", 78.5 → 6'6.5\"."""
    if not inches:
        return ''
    feet = int(inches // 12)
    remaining = inches % 12
    # Whole inches for individual players, 1 decimal for team averages
    if remaining == int(remaining):
        return f"{feet}'{int(remaining)}\""
    return f"{feet}'{remaining:.1f}\""


def format_section_header(section: str, historical_config: Optional[dict] = None,
                          current_season: int = 0,
                          is_postseason: bool = False,
                          mode: Optional[str] = None) -> str:
    """
    Build the full section header display string.

    Current stats:   "2025-26 Regular Season Stats per 100 Poss"
    Historical/Post: "Last 3 Regular Season Stats (2023-24 to 2025-26) per 36 Mins"
                     "Career Regular Season Stats per Game"

    Args:
        section: 'current_stats', 'historical_stats', or 'postseason_stats'
        historical_config: {mode, value, include_current} for hist/post
        current_season: End-year integer (e.g. 2026 for the 2025-26 season)
        is_postseason: True for postseason sections
        mode: Stats display mode ('per_game', 'per_48', 'per_100')
    """
    _MODE_LABELS = {
        'per_game': 'per Game',
        f"per_{int(STAT_CONSTANTS.get('default_per_minute', 36))}": f"per {int(STAT_CONSTANTS.get('default_per_minute', 36))} Mins",
        'per_100': 'per 100 Poss',
    }

    season_label = 'Postseason' if is_postseason else 'Regular Season'

    # Current stats: just "YYYY-YY Regular Season Stats (mode)"
    if section == 'current_stats':
        season_str = _format_season_label(current_season)
        header = f"{season_str} {season_label} Stats"
        mode_label = _MODE_LABELS.get(mode, '')
        return f"{header} {mode_label}" if mode_label else header

    # Historical / Postseason sections
    mode_cfg = (historical_config or {}).get('mode', 'seasons')
    value = (historical_config or {}).get('value', 3)
    include_current = (historical_config or {}).get('include_current', False)

    previous = '' if include_current else ' Previous'
    mode_label = _MODE_LABELS.get(mode, '')
    mode_suffix = f" {mode_label}" if mode_label else ''

    if mode_cfg == 'career':
        return f"Career{previous} {season_label} Stats{mode_suffix}"
    elif mode_cfg == 'seasons' and isinstance(value, int):
        start = 0 if include_current else 1
        end_season = current_season - start
        start_season = current_season - (start + value - 1)
        range_str = f" ({_format_season_label(start_season)} to {_format_season_label(end_season)})"
        return f"Last {value}{previous} {season_label} Stats{range_str}{mode_suffix}"
    elif mode_cfg == 'seasons' and isinstance(value, list):
        if value:
            n = len(value)
            first = min(value)
            last = max(value)
            range_str = f" ({first} to {last})"
            return f"Last {n}{previous} {season_label} Stats{range_str}{mode_suffix}"
        return f"{season_label} Stats{mode_suffix}"
    else:
        return f"{season_label} Stats{mode_suffix}"


def format_seasons_range(historical_config: Optional[dict], current_season: int) -> str:
    """
    Legacy wrapper — returns a prefix string for section headers.
    Kept for backward compatibility; prefer format_section_header() for full headers.
    """
    if not historical_config:
        return 'Last 3 Seasons'
    mode = historical_config.get('mode', 'seasons')
    if mode == 'career':
        return 'Career'
    elif mode == 'seasons':
        value = historical_config.get('value', 3)
        return f'Last {value} Season{"s" if value != 1 else ""}'
    elif mode == 'since_season':
        season = historical_config.get('season', historical_config.get('value', ''))
        return f'Since {season}'
    elif mode == 'seasons':
        seasons = historical_config.get('value', [])
        if seasons:
            first = min(seasons)
            last = max(seasons)
            return f"{_format_season_label(first)} – {_format_season_label(last)}"
        return ''
    return ''


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


