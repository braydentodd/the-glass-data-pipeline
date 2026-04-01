import logging
from typing import Dict, List, Optional, Any, Tuple
from src.sheets.config import SHEETS_COLUMNS
from src.sheets.config import (SECTION_CONFIG, SECTIONS, SUBSECTIONS, STAT_CONSTANTS, DEFAULT_STAT_MODE, COLORS, COLOR_THRESHOLDS, SHEET_FORMATTING)


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
                              n_player_rows: int = 0,
                              sheet_type: str = 'team',
                              show_advanced: bool = False,
                              partial_update: bool = False) -> list:
    """
    Build ALL Google Sheets batch_update requests for a worksheet.
    100% config-driven from SHEET_FORMATTING.

    show_advanced overrides config default so that syncs respect the
    user's current toggle state.

    Args:
        ws_id: Worksheet ID
        columns_list: The column structure from build_sheet_columns
        header_merges: Merge info from build_headers
        n_data_rows: Number of data rows (players + team/opp)
        team_name: Full team name for display
        percentile_cells: List of {row, col, percentile, reverse} for shading
        n_player_rows: Number of player rows (for filter range; team/opp excluded)
        sheet_type: 'team', 'players', or 'teams'
        show_advanced: If True, keep advanced columns visible (override config)

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

    # Respect current toggle state: override config defaults
    hide_advanced = not show_advanced if show_advanced else fmt.get('hide_advanced_columns', True)
    hide_subsection_row = hide_advanced  # subsection row visibility matches advanced state

    # --- Fast path for partial update (mode / timeframe changes) ---------
    # Skip all structural formatting, resize, and widths; only reapply data-dependent pieces.
    if partial_update:
        fast = []
        # Banding (row count may have changed)
        if n_data_rows > 0:
            fast.append({
                'addBanding': {
                    'bandedRange': {
                        'range': _range(ws_id, data_start, data_start + n_data_rows, 0, n_cols),
                        'rowProperties': {
                            'firstBandColor': get_color_for_raw(COLORS[fmt['row_even_bg']]),
                            'secondBandColor': get_color_for_raw(COLORS[fmt['row_odd_bg']]),
                        },
                    },
                }
            })
        # Auto-filter (range depends on row count)
        filter_end = data_start + (n_player_rows if n_player_rows > 0 else n_data_rows)
        fast.append({
            'setBasicFilter': {
                'filter': {
                    'range': _range(ws_id, fmt['filter_row'], filter_end, 0, n_cols),
                }
            }
        })
        # Percentile shading
        if percentile_cells:
            fast.extend(_build_percentile_shading_requests(ws_id, percentile_cells))
        # Null-formula backgrounds for team/opp rows
        if sheet_type == 'team' and n_data_rows > n_player_rows:
            fast.extend(_build_null_formula_bg_requests(
                ws_id, columns_list, data_start, n_player_rows, n_data_rows
            ))
        return fast

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

    # ---- 6b. Clear stale borders from previous syncs ----
    # ws.clear() removes values but NOT formatting/borders.
    # If the roster size changed, old borders would persist at wrong positions.
    if n_data_rows > 0:
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, data_start, total_rows, 0, n_cols),
                'top': {'style': 'NONE'},
                'bottom': {'style': 'NONE'},
                'left': {'style': 'NONE'},
                'right': {'style': 'NONE'},
                'innerHorizontal': {'style': 'NONE'},
                'innerVertical': {'style': 'NONE'},
            }
        })

    # ---- 7. Alternating row colors via addBanding (survives sorting) ----
    # Banding covers ALL data rows including team/opponent rows
    if n_data_rows > 0:
        requests.append({
            'addBanding': {
                'bandedRange': {
                    'range': _range(ws_id, data_start, data_start + n_data_rows, 0, n_cols),
                    'rowProperties': {
                        'firstBandColor': get_color_for_raw(COLORS[fmt['row_even_bg']]),
                        'secondBandColor': get_color_for_raw(COLORS[fmt['row_odd_bg']]),
                    },
                },
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

    # ---- 10. Section borders (vertical) — white in all header rows, black in data ----
    section_boundaries = _get_section_boundaries(columns_list)
    for boundary_col in section_boundaries:
        # Header portion (all header rows) — white border
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

    # ---- 11. Subsection borders (always drawn under section borders — UI manages visibility) ----
    subsection_boundaries = _get_subsection_boundaries(columns_list)
    sub_hdr_row = fmt['subsection_header_row']  # 0-indexed row 1
    sub_border_weight = fmt.get('subsection_border_weight', 1)
    
    # We only draw a subsection border if it doesn't overlap a darker section border
    filtered_sub_boundaries = [b for b in subsection_boundaries if b not in section_boundaries]
    
    for boundary_col in filtered_sub_boundaries:
        # Header portion (from subsection row through filter row) — white border
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, sub_hdr_row, header_end, boundary_col, boundary_col + 1),
                'left': _border_style_v2(sub_border_weight, header_border_color),
            }
        })
        # Data portion — black border
        if n_data_rows > 0:
            requests.append({
                'updateBorders': {
                    'range': _range(ws_id, data_start, total_rows, boundary_col, boundary_col + 1),
                    'left': _border_style_v2(sub_border_weight, data_border_color),
                }
            })

    # ---- 12. Horizontal borders between header rows — white, weight 2 ----
    # Between section header (row 0) and subsection header (row 1)
    requests.append({
        'updateBorders': {
            'range': _range(ws_id, fmt['subsection_header_row'], fmt['subsection_header_row'] + 1, 0, n_cols),
            'top': _border_style_v2(border_weight, header_border_color),
        }
    })
    # Between subsection header (row 1) and column header (row 2)
    requests.append({
        'updateBorders': {
            'range': _range(ws_id, fmt['column_header_row'], fmt['column_header_row'] + 1, 0, n_cols),
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

    # ---- 15. Column widths: only set minimum_width columns (no blanket auto-resize) ----
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        min_width = col_def.get('minimum_width')
        if min_width == 'auto':
            # Auto-resize just this column
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': ws_id,
                        'dimension': 'COLUMNS',
                        'startIndex': idx,
                        'endIndex': idx + 1,
                    },
                }
            })
        elif isinstance(min_width, (int, float)):
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': ws_id,
                        'dimension': 'COLUMNS',
                        'startIndex': idx,
                        'endIndex': idx + 1,
                    },
                    'properties': {'pixelSize': int(min_width)},
                    'fields': 'pixelSize',
                }
            })

    # ---- 16. Hide advanced stat columns (respects current toggle state) ----
    if hide_advanced:
        requests.extend(_build_hide_advanced_requests(ws_id, columns_list))
    else:
        # Advanced visible → hide basic stat columns (swap behavior)
        requests.extend(_build_hide_basic_requests(ws_id, columns_list))

    # ---- 17. Hide base value columns (percentile companion columns are always visible) ----
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        if col_def.get('has_percentile', False) and not col_def.get('is_generated_percentile', False):
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

    # ---- 18. Hide subsection row (tied to advanced stats state) ----
    if hide_subsection_row:
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

    # ---- 19b. Hide columns without entity formula (e.g. jersey on teams) ----
    col_entity = 'team' if sheet_type == 'teams' else 'player'
    fkey = f'{col_entity}_formula'
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        # Non-stat columns without a formula for this entity get hidden
        if col_def.get('stat_category', 'none') == 'none' and col_def.get(fkey) is None:
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
    # Only for individual team sheets which have team/opponent rows.
    # Players and Teams sheets have summary rows instead — no black bg.
    if sheet_type == 'team' and n_data_rows > n_player_rows:
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
        if (subsection != prev_subsection and prev_subsection is not None
                and col_ctx == prev_section):
            boundaries.append(idx)
        prev_subsection = subsection
        prev_section = col_ctx
    return boundaries


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


def _build_hide_basic_requests(ws_id: int, columns_list: List[Tuple]) -> list:
    """Build requests to hide basic stat columns (when advanced mode is shown)."""
    requests = []
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        if col_ctx_cfg.get('is_stats_section') and col_def.get('stat_mode') == 'basic':
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
    Build requests to set black background on cells where the row's
    formula is None:
      - player rows where player_formula is None (team-only columns)
      - team row where team_formula is None
      - opponent row where opponents_formula is None
    Config-driven: reads formula presence from column definitions.
    """
    black = get_color_for_raw(COLORS['black'])
    requests = []
    team_row = data_start + n_player_rows
    opp_row = data_start + n_player_rows + 1

    for idx, entry in enumerate(columns_list):
        col_def = entry[1]

        # Black bg on player rows for team-only columns
        if col_def.get('player_formula') is None and n_player_rows > 0:
            requests.append({
                'repeatCell': {
                    'range': _range(ws_id, data_start, data_start + n_player_rows, idx, idx + 1),
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': black,
                        },
                    },
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })

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
                            hist_seasons: str = '', post_seasons: str = '',
                            opp_percentiles: Optional[dict] = None) -> Tuple[list, List[dict]]:
    """
    Build a single merged data row with current + historical + postseason stats.

    pct_curr/pct_hist/pct_post: {col_key: sorted_values}
    opp_percentiles: {col_key: {section: sorted_vals}}
    """
    section_data = {}
    if current_data:
        section_data['current_stats'] = (current_data, pct_curr, '')
    if historical_data:
        section_data['historical_stats'] = (historical_data, pct_hist, hist_seasons)
    if postseason_data:
        section_data['postseason_stats'] = (postseason_data, pct_post, post_seasons)

    primary_entity = current_data or historical_data or postseason_data or {}

    row = build_entity_row(
        primary_entity, columns_list, {},
        entity_type=entity_type, mode=mode,
        section_data=section_data,
    )

    # Collect percentile info for companion column shading.
    # Only companion columns (is_generated_percentile) get coloured backgrounds;
    # stat columns show plain numbers without colour.
    percentile_cells = []
    for col_idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        # Only process companion columns
        if not is_pct:
            continue

        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        is_stats_section = col_ctx_cfg.get('is_stats_section', False)
        base_key = col_def.get('base_stat', col_key.replace('_pct', ''))

        if is_stats_section:
            if col_ctx not in section_data:
                continue
            sec_entity, sec_pcts, _ = section_data[col_ctx]

            # Opponent companion: compute value from base opponent formula
            if col_def.get('is_opponent_col') and opp_percentiles:
                opp_pop = opp_percentiles.get(base_key, {}).get(col_ctx)
                if opp_pop is not None:
                    # Find base opponent column to get its formula
                    base_col_def = None
                    for e2 in columns_list:
                        if e2[0] == base_key:
                            base_col_def = e2[1]
                            break
                    if base_col_def:
                        formula_str = base_col_def.get('team_formula')
                        value = _eval_dynamic_formula(
                            formula_str, sec_entity, base_col_def, mode)
                        if value is not None:
                            reverse = base_col_def.get('reverse_percentile', False)
                            rank = PERCENTILE_RANK_FN(value, opp_pop, reverse)
                            row[col_idx] = round(rank)  # Fill companion value
                            percentile_cells.append({
                                'col': col_idx,
                                'percentile': rank,
                                'reverse': False,
                            })
                continue

            # Regular companion
            base_def = SHEETS_COLUMNS.get(base_key, col_def)
            calculated = calculate_entity_stats(sec_entity, entity_type, mode)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('reverse_percentile', False)
                rank = PERCENTILE_RANK_FN(value, sec_pcts[base_key], reverse)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })
        else:
            # Non-stats section — use current_stats percentile population
            if 'current_stats' in section_data:
                sec_entity, sec_pcts, _ = section_data['current_stats']
            elif section_data:
                first_key = next(iter(section_data))
                sec_entity, sec_pcts, _ = section_data[first_key]
            else:
                continue
            base_def = SHEETS_COLUMNS.get(base_key, col_def)
            calculated = calculate_entity_stats(sec_entity, entity_type, mode)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('reverse_percentile', False)
                rank = PERCENTILE_RANK_FN(value, sec_pcts[base_key], reverse)
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


# ============================================================================
# SUMMARY ROW BUILDING (Best, 75th, Average, 25th, Worst)
# ============================================================================

# Config-driven summary thresholds
SUMMARY_THRESHOLDS = [
    ('Best', 100),
    ('75th Percentile', 75),
    ('Average', 50),
    ('25th Percentile', 25),
    ('Worst', 0),
]


def _get_value_at_percentile(sorted_values: List, percentile: float,
                             reverse: bool = False) -> Any:
    """Get the interpolated value at a given percentile (0-100) from sorted values."""
    if not sorted_values:
        return None
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    # For reverse columns (lower = better), Best (100) → lowest value
    if reverse:
        percentile = 100 - percentile
    idx = percentile / 100 * (n - 1)
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    frac = idx - lower
    v_lower = sorted_values[lower]
    v_upper = sorted_values[upper]
    if not isinstance(v_lower, (int, float)) or not isinstance(v_upper, (int, float)):
        return None
    return v_lower * (1 - frac) + v_upper * frac


def build_summary_rows(columns_list: List[Tuple],
                       percentile_pops: dict,
                       mode: str = 'per_100',
                       opp_percentiles: Optional[dict] = None) -> Tuple[List[list], List[dict]]:
    """
    Build summary rows (Best, 75th, Average, 25th, Worst) for Teams/Players sheets.

    For each stat column, looks up the value at that percentile threshold.
    Non-stat columns are left blank except 'names' which gets the label.
    Generated percentile columns show the percentile level itself.

    Returns:
        (rows, percentile_cells) where rows is list of 5 row lists,
        and percentile_cells is list of {row, col, percentile} dicts
        (row index is relative — caller must add data_start offset).
    """
    rows = []
    pct_cells = []

    for label, pct_level in SUMMARY_THRESHOLDS:
        row = []
        for col_idx, entry in enumerate(columns_list):
            col_key, col_def = entry[0], entry[1]
            col_ctx = entry[3] if len(entry) > 3 else None

            # Names column gets the label
            if col_key == 'names':
                row.append(label)
                continue

            # Generated percentile columns show the percentile level
            if col_def.get('is_generated_percentile', False):
                row.append(pct_level)
                # Color this cell at its percentile level
                pct_cells.append({
                    'col': col_idx,
                    'percentile': pct_level,
                    'reverse': False,  # Already correct direction
                    'row_offset': len(rows),
                })
                continue

            # Non-stat, non-percentile, no-has_percentile columns are blank
            if (col_def.get('stat_category', 'none') == 'none'
                    and not col_def.get('is_generated_percentile', False)
                    and not col_def.get('has_percentile', False)):
                row.append('')
                continue

            # Opponent columns: use opp_percentiles populations
            if col_def.get('is_opponent_col') and opp_percentiles:
                col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
                if col_ctx_cfg.get('is_stats_section') and col_ctx:
                    opp_pop = opp_percentiles.get(col_key, {}).get(col_ctx)
                    if opp_pop:
                        reverse = col_def.get('reverse_percentile', False)
                        val = _get_value_at_percentile(opp_pop, pct_level, reverse)
                        if val is not None:
                            formatted = format_stat_value(val, col_def)
                            row.append(formatted if formatted is not None else '')
                            pct_cells.append({
                                'col': col_idx,
                                'percentile': pct_level,
                                'reverse': False,
                                'row_offset': len(rows),
                            })
                            continue
                row.append('')
                continue

            # Regular stat columns: look up in section-specific populations
            col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
            is_stats_section = col_ctx_cfg.get('is_stats_section', False)
            pop_key = f'{col_ctx}:{col_key}'

            # Stats-section columns: look up via section:col_key
            if is_stats_section and (
                    pop_key in percentile_pops or col_key in percentile_pops):
                sorted_vals = percentile_pops.get(pop_key,
                              percentile_pops.get(col_key))
                if sorted_vals:
                    reverse = col_def.get('reverse_percentile', False)
                    val = _get_value_at_percentile(sorted_vals, pct_level, reverse)
                    if val is not None:
                        formatted = format_stat_value(val, col_def)
                        row.append(formatted if formatted is not None else '')
                        pct_cells.append({
                            'col': col_idx,
                            'percentile': pct_level,
                            'reverse': False,
                            'row_offset': len(rows),
                        })
                        continue

            # Non-stats columns with has_percentile (player_info): direct col_key lookup
            if not is_stats_section and col_def.get('has_percentile', False):
                sorted_vals = percentile_pops.get(col_key)
                if sorted_vals:
                    reverse = col_def.get('reverse_percentile', False)
                    val = _get_value_at_percentile(sorted_vals, pct_level, reverse)
                    if val is not None:
                        formatted = format_stat_value(val, col_def)
                        row.append(formatted if formatted is not None else '')
                        pct_cells.append({
                            'col': col_idx,
                            'percentile': pct_level,
                            'reverse': False,
                            'row_offset': len(rows),
                        })
                        continue

            row.append('')

        rows.append(row)

    return rows, pct_cells


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


def get_config_for_export(league: str,
                          get_teams_fn,
                          id_column_key: str,
                          server_config: dict,
                          google_sheets_config: dict,
                          mode: str = 'per_100') -> dict:
    """
    Build JSON-serializable config for /api/config endpoint.
    Apps Script uses this as single source of truth — zero hardcoding in JS.

    League-agnostic: parameterized by league name ("nba" or "ncaa"),
    a team-fetching callable, and the ID column key.

    Exports:
      - column_ranges:            section toggle ranges (team_sheet / {league}_sheet)
      - advanced_column_ranges:   toggle advanced stat columns
      - percentile_column_ranges: toggle percentile columns
      - column_indices:           edit-detection indices (player_id, team, stats_start)
    """
    league_sheet = f'{league}_sheet'

    # --- Teams dict -------------------------------------------------------
    teams_from_db = get_teams_fn()
    league_teams = {abbr: team_id for team_id, (abbr, name) in teams_from_db.items()}

    # --- Stat columns list -----------------------------------------------
    stat_columns = [k for k, v in SHEETS_COLUMNS.items() if v.get('stat_category', 'none') != 'none']

    # --- Build full column lists for all sheet types --------------------
    team_columns = build_sheet_columns(
        entity='player', stat_mode='both', sheet_type='team'
    )
    league_columns = build_sheet_columns(
        entity='player', stat_mode='both', sheet_type='players'
    )
    teams_columns = build_sheet_columns(
        entity='team', stat_mode='both', sheet_type='teams'
    )

    # --- Helper: find contiguous ranges of matching column indices --------
    def _contiguous_ranges(indices):
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
        return {'start': min(indices) + 1, 'count': len(indices)}

    column_ranges = {'team_sheet': {}, league_sheet: {}, 'teams_sheet': {}}
    _sec_rename = {'analysis': 'notes'}
    for sec in ('current_stats', 'historical_stats', 'postseason_stats',
                'player_info', 'analysis'):
        key = _sec_rename.get(sec, sec.replace('_stats', ''))
        team_range = _section_range(team_columns, sec)
        league_range = _section_range(league_columns, sec)
        teams_range = _section_range(teams_columns, sec)
        if team_range:
            column_ranges['team_sheet'][key] = team_range
        if league_range:
            column_ranges[league_sheet][key] = league_range
        if teams_range:
            column_ranges['teams_sheet'][key] = teams_range

    # --- Advanced column ranges ------------------------------------------
    def _advanced_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('stat_mode') == 'advanced'
        ])

    advanced_column_ranges = {
        'team_sheet':  _contiguous_ranges(_advanced_indices(team_columns)),
        league_sheet:  _contiguous_ranges(_advanced_indices(league_columns)),
        'teams_sheet': _contiguous_ranges(_advanced_indices(teams_columns)),
    }

    # --- Basic column ranges (hidden when advanced mode is on) -----------
    def _basic_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('stat_mode') == 'basic'
        ])

    basic_column_ranges = {
        'team_sheet':  _contiguous_ranges(_basic_indices(team_columns)),
        league_sheet:  _contiguous_ranges(_basic_indices(league_columns)),
        'teams_sheet': _contiguous_ranges(_basic_indices(teams_columns)),
    }

    # --- Percentile column ranges ----------------------------------------
    def _percentile_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('is_generated_percentile', False)
        ])

    percentile_column_ranges = {
        'team_sheet':  _contiguous_ranges(_percentile_indices(team_columns)),
        league_sheet:  _contiguous_ranges(_percentile_indices(league_columns)),
        'teams_sheet': _contiguous_ranges(_percentile_indices(teams_columns)),
    }

    # --- Base value columns that have percentile counterparts ------------
    def _base_value_with_pct_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('has_percentile', False)
            and not col_def.get('is_generated_percentile', False)
        ])

    base_value_column_ranges = {
        'team_sheet':  _contiguous_ranges(_base_value_with_pct_indices(team_columns)),
        league_sheet:  _contiguous_ranges(_base_value_with_pct_indices(league_columns)),
        'teams_sheet': _contiguous_ranges(_base_value_with_pct_indices(teams_columns)),
    }

    # --- Vertical boundaries (for border management in toggles) -----------
    def _boundary_entries(cols, idx_list):
        return [{'col': b + 1, 'hp': bool(cols[b][1].get('has_percentile', False))}
                for b in idx_list]

    subsection_boundaries = {
        'team_sheet':  _boundary_entries(team_columns,  _get_subsection_boundaries(team_columns)),
        league_sheet:  _boundary_entries(league_columns, _get_subsection_boundaries(league_columns)),
        'teams_sheet': _boundary_entries(teams_columns,  _get_subsection_boundaries(teams_columns)),
    }

    section_boundaries = {
        'team_sheet':  _boundary_entries(team_columns,  _get_section_boundaries(team_columns)),
        league_sheet:  _boundary_entries(league_columns, _get_section_boundaries(league_columns)),
        'teams_sheet': _boundary_entries(teams_columns,  _get_section_boundaries(teams_columns)),
    }

    # --- Always-hidden columns per sheet type (1-indexed) ----------------
    def _always_hidden_indices(cols, entity_type):
        fkey = f'{entity_type}_formula'
        hidden = []
        for i, (ck, cd, v, cx) in enumerate(cols):
            if cd.get('stat_category', 'none') == 'none' and cd.get(fkey) is None:
                hidden.append(i + 1)
        return hidden

    always_hidden_columns = {
        'team_sheet':  _always_hidden_indices(team_columns, 'player'),
        'teams_sheet': _always_hidden_indices(teams_columns, 'team'),
        league_sheet:  [],
    }

    # --- Stats section column ranges -----------
    def _stats_section_range(cols):
        start = end = None
        for idx, (ck, cd, v, cx) in enumerate(cols):
            if SECTION_CONFIG.get(cx, {}).get('is_stats_section'):
                if start is None:
                    start = idx + 1
                end = idx + 1
        return {'start': start, 'end': end} if start else None

    stats_section_ranges = {
        'team_sheet':  _stats_section_range(team_columns),
        league_sheet:  _stats_section_range(league_columns),
        'teams_sheet': _stats_section_range(teams_columns),
    }

    # --- Per-column metadata for JS toggle logic -------------------------
    def _column_metadata(cols):
        meta = []
        for i, (ck, cd, v, cx) in enumerate(cols):
            sm = cd.get('stat_mode', 'both')
            is_stats = SECTION_CONFIG.get(cx, {}).get('is_stats_section', False)
            meta.append({
                'col': i + 1,
                'pct': bool(cd.get('is_generated_percentile')),
                'adv': sm == 'advanced',
                'bas': sm == 'basic',
                'stats': is_stats,
                'hp': bool(cd.get('has_percentile')),
                'sec': cx,
            })
        return meta

    column_metadata = {
        'team_sheet':  _column_metadata(team_columns),
        league_sheet:  _column_metadata(league_columns),
        'teams_sheet': _column_metadata(teams_columns),
    }

    # --- Per-column widths -----------------------------------------------
    def _column_widths(cols):
        widths = {}
        for i, (ck, cd, v, cx) in enumerate(cols):
            mw = cd.get('minimum_width')
            if mw is not None:
                widths[str(i + 1)] = mw
        return widths

    column_widths = {
        'team_sheet':  _column_widths(team_columns),
        league_sheet:  _column_widths(league_columns),
        'teams_sheet': _column_widths(teams_columns),
    }

    # --- Column indices for edit detection (1-indexed) ---
    id_idx = get_column_index(id_column_key, team_columns)
    team_col_idx = get_column_index('team', league_columns)
    stats_start = None
    for i, entry in enumerate(team_columns):
        if entry[1].get('stat_category', 'none') != 'none':
            stats_start = i + 1
            break

    # --- Editable columns (config-driven for Apps Script) ----------------
    editable_columns = []
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('editable', False):
            continue
        db_field = col_def.get('player_formula')
        if not db_field or any(op in db_field for op in '+-*/('):
            continue
        team_idx = get_column_index(col_key, team_columns)
        league_idx = get_column_index(col_key, league_columns)
        editable_columns.append({
            'col_key': col_key,
            'team_col_index': (team_idx or 0) + 1,
            f'{league}_col_index': (league_idx or 0) + 1 if league_idx is not None else None,
            'db_field': db_field,
            'display_name': col_def.get('display_name', col_key),
            'format': col_def.get('format', 'text'),
        })

    # --- Editable columns for teams_sheet ----
    teams_editable = []
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('editable', False):
            continue
        tf = col_def.get('team_formula')
        if tf and tf != 'TEAM' and not any(op in tf for op in '+-*/('):
            ti = get_column_index(col_key, teams_columns)
            if ti is not None:
                teams_editable.append({
                    'col_key': col_key,
                    'col_index': ti + 1,
                    'db_field': tf,
                    'display_name': col_def.get('display_name', col_key),
                })

    # Reverse mapping: team name → abbreviation
    team_name_to_abbr = {name: abbr for _, (abbr, name) in teams_from_db.items()}

    return {
        'api_base_url': f"http://{server_config['production_host']}:{server_config['production_port']}",
        'sheet_id': google_sheets_config.get('spreadsheet_id', ''),
        f'{league}_teams': league_teams,
        'team_name_to_abbr': team_name_to_abbr,
        'stat_columns': stat_columns,
        'reverse_stats': get_reverse_stats(),
        'editable_fields': get_editable_fields(),
        'editable_columns': editable_columns,
        'column_indices': {
            'player_id': (id_idx or 0) + 1,
            'team': (team_col_idx or 0) + 1,
            'stats_start': stats_start or 9,
        },
        'column_ranges': column_ranges,
        'advanced_column_ranges': advanced_column_ranges,
        'basic_column_ranges': basic_column_ranges,
        'percentile_column_ranges': percentile_column_ranges,
        'base_value_column_ranges': base_value_column_ranges,
        'section_boundaries': section_boundaries,
        'subsection_boundaries': subsection_boundaries,
        'always_hidden_columns': always_hidden_columns,
        'stats_section_ranges': stats_section_ranges,
        'column_metadata': column_metadata,
        'column_widths': column_widths,
        'default_stat_mode': DEFAULT_STAT_MODE,
        'subsection_row_index': SHEET_FORMATTING['subsection_header_row'] + 1,
        'teams_editable_columns': teams_editable,
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


def resolve_columns_for_league(league):
    """Resolve fully expanded SHEETS_COLUMNS into a league-specific flat dict."""
    from src.sheets.config import WIDTH_CLASSES
    resolved = {}

    for col_key, col_def in SHEETS_COLUMNS.items():
        leagues = col_def.get('leagues', ['nba', 'ncaa'])
        if league not in leagues:
            continue

        entry = {}
        _SKIP = {'leagues', 'formulas', 'width_class', 'width'}
        for k, v in col_def.items():
            if k not in _SKIP:
                entry[k] = v

        formulas = col_def.get('formulas', {})
        entry['player_formula'] = formulas.get('player')
        entry['team_formula'] = formulas.get('team')
        entry['opponents_formula'] = formulas.get('opponents')

        wc = col_def.get('width_class', 'auto')
        pw = WIDTH_CLASSES.get(wc)
        entry['minimum_width'] = pw if pw is not None else 'auto'

        resolved[col_key] = entry

    return resolved

