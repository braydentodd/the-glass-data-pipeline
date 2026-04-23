from typing import List, Optional, Any
from src.core.config import format_season_label
from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.definitions.config import (STAT_RATES)

def format_stat_value(value: Any, fmt: str, decimals: int, nullable: bool) -> Any:
    """Format a stat value for display according to column definition."""
    if value is None:
        # Non-nullable columns (games, seasons) show 0 instead of blank
        if not nullable:
            return 0
        return ''
    if isinstance(value, (int, float)) and value == 0:
        return 0

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
    Historical/Post: "Regular Season Stats per 100 Poss (Previous 3 Seasons)"
                     "Regular Season Stats per 100 Poss (Previous Season)"  (1 season)

    Args:
        section: 'current_stats', 'historical_stats', or 'postseason_stats'
        historical_config: {mode, value} for hist/post
        current_season: End-year integer (e.g. 2026 for the 2025-26 season)
        is_postseason: True for postseason sections
        mode: Stats rate ('per_possession', 'per_minute', 'per_game')
    """
    season_label = 'Postseason' if is_postseason else 'Regular Season'

    # Build the rate string ("per 100 Poss", "per Game")
    rate_str = ""
    if mode and mode in STAT_RATES:
        rate_info = STAT_RATES[mode]
        rate_val = rate_info.get('rate')
        short_label = rate_info.get('short_label', '')
        if rate_val is not None:
            rate_str = f" per {rate_val} {short_label}"
        else:
            rate_str = f" per {short_label}"

    # Current stats: "YYYY-YY Regular Season Stats per 100 Poss"
    if section == 'current_stats':
        season_str = format_season_label(current_season)
        return f"{season_str} {season_label} Stats{rate_str}"

    # Historical / Postseason sections
    mode_cfg = (historical_config or {}).get('mode', 'seasons')
    value = (historical_config or {}).get('value', 3)

    if isinstance(value, int):
        num = value
    elif isinstance(value, list) and value:
        num = len(value)
    else:
        return f"{season_label} Stats{rate_str}"

    if num == 1:
        timeframe_str = "(Previous Season)"
    else:
        timeframe_str = f"(Previous {num} Seasons)"

    return f"{season_label} Stats{rate_str} {timeframe_str}"


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


def get_reverse_stats() -> List[str]:
    """Get list of stat column keys where lower is better."""
    return [k for k, v in TAB_COLUMNS.items() if v.get('percentile') == 'reverse']


def get_editable_fields() -> List[str]:
    """Get list of field names that users can edit (wingspan, notes, hand)."""
    fields = []
    for col_key, col_def in TAB_COLUMNS.items():
        if col_def.get('editable', False):
            # Get the actual DB field from the player value
            formula = col_def.get('values', {}).get('player')
            if formula and isinstance(formula, str):
                fields.append(formula)



from src.publish.definitions.config import HEADER_ROWS, SHEET_FORMATTING

def _get_row_indexes():
    indexes = {}
    current_row = 0
    for row_key, rules in HEADER_ROWS.items():
        if row_key == 'sections':
            indexes['section_header_row'] = current_row
        elif row_key == 'subsections':
            indexes['subsection_header_row'] = current_row
        elif row_key == 'columns':
            indexes['column_header_row'] = current_row
        elif row_key == 'filters':
            indexes['filter_row'] = current_row
            
        current_row += 1
        
        if rules.get('divider_row_weight') and rules.get('divider_row_direction') == 'below':
            if row_key == 'sections':
                indexes['section_divider_row'] = current_row
            elif row_key == 'subsections':
                indexes['subsection_divider_row'] = current_row
            current_row += 1
            
    indexes['data_start_row'] = SHEET_FORMATTING.get('header_rows', current_row)
    return indexes

ROW_INDEXES = _get_row_indexes()
