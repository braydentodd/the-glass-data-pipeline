from typing import List, Optional, Any
from src.publish.config import SHEETS_COLUMNS
from src.publish.config import (STAT_RATE_LABELS, COLORS, COLOR_THRESHOLDS)

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
    Historical/Post: "Previous 3 Regular Seasons Stats per 100 Poss"
                     "Previous Regular Season Stats per 100 Poss"  (1 season)

    Args:
        section: 'current_stats', 'historical_stats', or 'postseason_stats'
        historical_config: {mode, value} for hist/post
        current_season: End-year integer (e.g. 2026 for the 2025-26 season)
        is_postseason: True for postseason sections
        mode: Stats rate ('per_possession', 'per_minute', 'per_game')
    """
    season_label = 'Postseason' if is_postseason else 'Regular Season'

    # Current stats: just "YYYY-YY Regular Season Stats (rate)"
    if section == 'current_stats':
        season_str = _format_season_label(current_season)
        header = f"{season_str} {season_label} Stats"
        rate_label = STAT_RATE_LABELS.get(mode, '')
        return f"{header} {rate_label}" if rate_label else header

    # Historical / Postseason sections — never include current season
    mode_cfg = (historical_config or {}).get('mode', 'seasons')
    value = (historical_config or {}).get('value', 3)

    rate_label = STAT_RATE_LABELS.get(mode, '')
    rate_suffix = f" {rate_label}" if rate_label else ''

    if isinstance(value, int):
        if value == 1:
            return f"Previous {season_label} Stats{rate_suffix}"
        plural = 's' if not is_postseason else 's'
        return f"Previous {value} {season_label}{plural} Stats{rate_suffix}"
    elif isinstance(value, list) and value:
        n = len(value)
        if n == 1:
            return f"Previous {season_label} Stats{rate_suffix}"
        plural = 's' if not is_postseason else 's'
        return f"Previous {n} {season_label}{plural} Stats{rate_suffix}"
    else:
        return f"{season_label} Stats{rate_suffix}"


def format_seasons_range(historical_config: Optional[dict], current_season: int) -> str:
    """
    Returns a prefix string for section headers.
    """
    if not historical_config:
        return 'Previous 3 Seasons'
    mode = historical_config.get('mode', 'seasons')
    if mode == 'seasons':
        value = historical_config.get('value', 3)
        if isinstance(value, int):
            if value == 1:
                return 'Previous Season'
            return f'Previous {value} Seasons'
        elif isinstance(value, list):
            n = len(value)
            if n == 1:
                return 'Previous Season'
            return f'Previous {n} Seasons'
    return 'Previous 3 Seasons'


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
    return [k for k, v in SHEETS_COLUMNS.items() if v.get('percentile') == 'reverse']


def get_editable_fields() -> List[str]:
    """Get list of field names that users can edit (wingspan, notes, hand)."""
    fields = []
    for col_key, col_def in SHEETS_COLUMNS.items():
        if col_def.get('editable', False):
            # Get the actual DB field from the player value
            formula = col_def.get('values', {}).get('player')
            if formula and isinstance(formula, str):
                fields.append(formula)


