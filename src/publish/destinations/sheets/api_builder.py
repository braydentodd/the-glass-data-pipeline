import logging
from typing import Dict, List, Optional, Tuple
from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.definitions.config import (SECTION_CONFIG, SECTIONS, SUBSECTIONS, STAT_CONSTANTS, COLORS, COLOR_THRESHOLDS, SHEET_FORMATTING, WIDTH_CLASSES)
from src.publish.destinations.sheets.styles import get_color_for_percentile, get_color_for_raw, get_color_dict, get_border_style, create_cell_format, create_text_format
from src.publish.core.table_builder import get_column_index

def build_formatting_requests(ws_id: int, columns_list: List[Tuple],
                              header_merges: list, n_data_rows: int,
                              team_name: str,
                              percentile_cells: Optional[List[dict]] = None,
                              n_player_rows: int = 0,
                              tab_type: str = 'team',
                              show_advanced: bool = False,
                              partial_update: bool = False) -> list:
    """
    Build ALL Google Sheets batch_update requests for a worksheet.
    100% config-driven from SHEET_FORMATTING.

    show_advanced overrides config default so that syncs respect the
    user's current toggle state.

    Args:
        ws_id: Worksheet ID
        columns_list: The column structure from build_tab_columns
        header_merges: Merge info from build_headers
        n_data_rows: Number of data rows (players + team/opp)
        team_name: Full team name for display
        percentile_cells: List of {row, col, percentile, reverse} for shading
        n_player_rows: Number of player rows (for filter range; team/opp excluded)
        tab_type: 'individual_team', 'all_players', or 'all_teams'
        show_advanced: If True, keep advanced columns visible (override config)

    Returns:
        List of request dicts for spreadsheet.batch_update
    """
    fmt = SHEET_FORMATTING
    n_cols = len(columns_list)
    data_start = fmt['data_start_row']
    total_rows = data_start + n_data_rows
    header_end = fmt['data_start_row']  # Row after last header row
    column_border_weight = fmt.get('column_border_weight', 1)
    column_header_color = get_color_for_raw(COLORS[fmt.get('column_border_color_header', 'white')])
    column_data_color = get_color_for_raw(COLORS[fmt.get('column_border_color_data', 'black')])
    wrap_strategy = fmt.get('wrap_strategy', 'CLIP')

    # Subsection row is always visible
    hide_subsection_row = False

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
        if tab_type == 'individual_team' and n_data_rows > n_player_rows:
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

    # ---- 1b. Explicit default row heights (do this before specific divider heights) ----
    requests.append({
        'updateDimensionProperties': {
            'range': { 'sheetId': ws_id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': total_rows },
            'properties': {'pixelSize': fmt.get('row_height_default', 21)},
            'fields': 'pixelSize',
        }
    })
    requests.append({
        'updateDimensionProperties': {
            'range': { 'sheetId': ws_id, 'dimension': 'ROWS', 'startIndex': fmt['section_header_row'], 'endIndex': fmt['section_header_row'] + 1 },
            'properties': {'pixelSize': fmt.get('row_height_section_header', 25)},
            'fields': 'pixelSize',
        }
    })
    requests.append({
        'updateDimensionProperties': {
            'range': { 'sheetId': ws_id, 'dimension': 'ROWS', 'startIndex': fmt['filter_row'], 'endIndex': fmt['filter_row'] + 1 },
            'properties': {'pixelSize': fmt.get('row_height_filter', 12)},
            'fields': 'pixelSize',
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

    # ---- 5. Filter row — same header styling ----
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

    # ---- 6c. Header divider rows (section/subsection) ----
    divider_bg = get_color_for_raw(COLORS[fmt.get('header_divider_bg', 'dark_gray')])
    divider_height = fmt.get('header_divider_height', 2)
    for row_key in ('section_divider_row', 'subsection_divider_row'):
        row_idx = fmt.get(row_key)
        if row_idx is None:
            continue
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, row_idx, row_idx + 1, 0, n_cols),
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': divider_bg,
                        'textFormat': {'fontSize': 2},
                    },
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat)',
            }
        })
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': ws_id,
                    'dimension': 'ROWS',
                    'startIndex': row_idx,
                    'endIndex': row_idx + 1,
                },
                'properties': {'pixelSize': divider_height},
                'fields': 'pixelSize',
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

    # ---- 8. Per-column alignment and emphasis (data rows only) — column-definition-driven ----
    if n_data_rows > 0:
        for idx, entry in enumerate(columns_list):
            col_def = entry[1]
            col_align = col_def.get('align', 'center').upper()
            col_emphasis = col_def.get('emphasis')
            col_font_size = col_def.get('font_size')

            if col_align != fmt['default_h_align']:
                requests.append({
                    'repeatCell': {
                        'range': _range(ws_id, data_start, total_rows, idx, idx + 1),
                        'cell': {
                            'userEnteredFormat': {'horizontalAlignment': col_align},
                        },
                        'fields': 'userEnteredFormat.horizontalAlignment',
                    }
                })

            if col_emphasis == 'bold' or col_font_size is not None:
                text_format = {}
                fields = []
                if col_emphasis == 'bold':
                    text_format['bold'] = True
                    fields.append('userEnteredFormat.textFormat.bold')
                if col_font_size is not None:
                    text_format['fontSize'] = col_font_size
                    fields.append('userEnteredFormat.textFormat.fontSize')

                requests.append({
                    'repeatCell': {
                        'range': _range(ws_id, data_start, total_rows, idx, idx + 1),
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': text_format,
                            },
                        },
                        'fields': ','.join(fields),
                    }
                })

    # ---- 9. Header merge cells ----
    frozen_cols = fmt.get('frozen_cols', 0)
    for merge in header_merges:
        row = merge['row']  # Already 0-based (section=0, subsection=1)
        s, e = merge['start_col'], merge['end_col']
        if e - s <= 1:
            continue
        # Split merges that cross the frozen/non-frozen column boundary
        if s < frozen_cols < e:
            if frozen_cols - s > 1:
                requests.append({
                    'mergeCells': {
                        'range': _range(ws_id, row, row + 1, s, frozen_cols),
                        'mergeType': 'MERGE_ALL',
                    }
                })
            if e - frozen_cols > 1:
                requests.append({
                    'mergeCells': {
                        'range': _range(ws_id, row, row + 1, frozen_cols, e),
                        'mergeType': 'MERGE_ALL',
                    }
                })
        else:
            requests.append({
                'mergeCells': {
                    'range': _range(ws_id, row, row + 1, s, e),
                    'mergeType': 'MERGE_ALL',
                }
            })

    # ---- 10. Separator columns between sections/subsections ----
    header_separator_bg = get_color_for_raw(COLORS[fmt.get('header_separator_bg', 'white')])
    data_separator_bg = get_color_for_raw(COLORS[fmt.get('data_separator_bg', 'black')])
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        if not col_def.get('is_separator'):
            continue
        separator_type = col_def.get('separator_type', 'section')
        if separator_type == 'subsection':
            separator_width = fmt.get('subsection_separator_width', 2)
        else:
            separator_width = fmt.get('section_separator_width', 4)
        # Set narrow fixed width
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': ws_id,
                    'dimension': 'COLUMNS',
                    'startIndex': idx,
                    'endIndex': idx + 1,
                },
                'properties': {'pixelSize': separator_width},
                'fields': 'pixelSize',
            }
        })
        # Background for header rows
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, 0, data_start, idx, idx + 1),
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': header_separator_bg,
                    },
                },
                'fields': 'userEnteredFormat.backgroundColor',
            }
        })
        # Background for data rows
        if n_data_rows > 0:
            requests.append({
                'repeatCell': {
                    'range': _range(ws_id, data_start, total_rows, idx, idx + 1),
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': data_separator_bg,
                        },
                    },
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })

    # ---- 11. Column borders (skip frozen divider + stat/companion) ----
    frozen_cols = fmt.get('frozen_cols', 0)
    header_border_end = fmt['filter_row'] + 1
    for col_idx in range(1, n_cols):
        left_def = columns_list[col_idx - 1][1]
        right_def = columns_list[col_idx][1]
        if col_idx == frozen_cols:
            continue
        if right_def.get('is_generated_percentile'):
            continue
        if left_def.get('is_separator') or right_def.get('is_separator'):
            continue

        requests.append({
            'updateBorders': {
                'range': _range(ws_id, 0, header_border_end, col_idx, col_idx + 1),
                'left': _border_style(column_border_weight, column_header_color),
            }
        })
        if n_data_rows > 0:
            requests.append({
                'updateBorders': {
                    'range': _range(ws_id, data_start, total_rows, col_idx, col_idx + 1),
                    'left': _border_style(column_border_weight, column_data_color),
                }
            }
        )

    # ---- 11b. Clear all borders across horizontal divider rows ----
    # Ensures cleanly rendered separators without vertical border cut-throughs
    for row_key in ('section_divider_row', 'subsection_divider_row'):
        row_idx = fmt.get(row_key)
        if row_idx is not None:
            requests.append({
                'updateBorders': {
                    'range': _range(ws_id, row_idx, row_idx + 1, 0, n_cols),
                    'top': {'style': 'NONE'},
                    'bottom': {'style': 'NONE'},
                    'left': {'style': 'NONE'},
                    'right': {'style': 'NONE'},
                    'innerHorizontal': {'style': 'NONE'},
                    'innerVertical': {'style': 'NONE'},
                }
            })

    # ---- 14. Divider rows above team/opp or summary footers ----
    if n_player_rows > 0 and n_data_rows > n_player_rows:
        sep_row = data_start + n_player_rows
        if tab_type == 'individual_team':
            divider_bg = data_separator_bg
            divider_height = fmt.get('footer_divider_height', 4)
        else:
            divider_bg = get_color_for_raw(COLORS[fmt.get('footer_divider_bg', 'black')])
            divider_height = fmt.get('footer_divider_height', 4)
        requests.append({
            'repeatCell': {
                'range': _range(ws_id, sep_row, sep_row + 1, 0, n_cols),
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': divider_bg,
                        'textFormat': {'fontSize': 2},
                    },
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat)',
            }
        })
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': ws_id,
                    'dimension': 'ROWS',
                    'startIndex': sep_row,
                    'endIndex': sep_row + 1,
                },
                'properties': {'pixelSize': divider_height},
                'fields': 'pixelSize',
            }
        })
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, sep_row, sep_row + 1, 0, n_cols),
                'top': {'style': 'NONE'},
                'bottom': {'style': 'NONE'},
                'left': {'style': 'NONE'},
                'right': {'style': 'NONE'},
                'innerHorizontal': {'style': 'NONE'},
                'innerVertical': {'style': 'NONE'},
            }
        })

    # ---- 15. Column widths and percentile companion formatting ----
    pct_font_size = fmt.get('percentile_companion_font_size', 5)
    pct_width = fmt.get('percentile_companion_width', 10)
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        is_pct_companion = col_def.get('is_generated_percentile', False)
        wc = col_def.get('width_class')

        # Separator columns — width already handled in section 10
        if col_def.get('is_separator'):
            continue

        # Percentile companions: fixed width, small font, two-line display
        if is_pct_companion:
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': ws_id,
                        'dimension': 'COLUMNS',
                        'startIndex': idx,
                        'endIndex': idx + 1,
                    },
                    'properties': {'pixelSize': pct_width},
                    'fields': 'pixelSize',
                }
            })
            if n_data_rows > 0:
                requests.append({
                    'repeatCell': {
                        'range': _range(ws_id, data_start, total_rows, idx, idx + 1),
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {'fontSize': pct_font_size},
                                'verticalAlignment': 'MIDDLE',
                                'wrapStrategy': 'CLIP',
                            },
                        },
                        'fields': 'userEnteredFormat(textFormat.fontSize,verticalAlignment,wrapStrategy)',
                    }
                })
            continue

        # Regular columns: resolve pixel width from WIDTH_CLASSES or direct int
        if isinstance(wc, (int, float)):
            pixel_width = int(wc)
        elif isinstance(wc, str):
            pixel_width = WIDTH_CLASSES.get(wc)
        else:
            pixel_width = None

        if pixel_width is None:
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
        else:
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': ws_id,
                        'dimension': 'COLUMNS',
                        'startIndex': idx,
                        'endIndex': idx + 1,
                    },
                    'properties': {'pixelSize': pixel_width},
                    'fields': 'pixelSize',
                }
            })

    # ---- 16. Hide columns based on visibility flag from build_tab_columns ----
    # The visible flag (entry[2]) encodes: non-default rate → hidden,
    # advanced/basic mode toggle → hidden. This single loop replaces all
    # per-column hiding logic.
    for idx, entry in enumerate(columns_list):
        col_vis = entry[2]
        if not col_vis:
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

    # ---- 17. Hide subsection row (tied to advanced stats state) ----
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

    # ---- 19. Auto-filter on filter row — excludes team/opp rows from sort ----
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



    # ---- 21b. Bottom border on the last row of the sheet ----
    if n_data_rows > 0:
        requests.append({
            'updateBorders': {
                'range': _range(ws_id, total_rows - 1, total_rows, 0, n_cols),
                'bottom': _border_style(2, get_color_for_raw(COLORS['black']))
            }
        })

    # ---- 22. Black background for cells where entity has no formula ----
    # Only for individual team sheets which have team/opponent rows.
    # Players and Teams sheets have summary rows instead — no black bg.
    if tab_type == 'individual_team' and n_data_rows > n_player_rows:
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


def _border_style(weight: int, color: dict) -> dict:
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
    """Get column indices where subsections change within any section."""
    boundaries = []
    prev_subsection = None
    prev_section = None
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        subsection = col_def.get('subsection')
        if subsection is None:
            prev_subsection = None
            prev_section = col_ctx
            continue
        # New subsection within same section
        if (subsection != prev_subsection and prev_subsection is not None
                and col_ctx == prev_section):
            boundaries.append(idx)
        prev_subsection = subsection
        prev_section = col_ctx
    return boundaries


def _build_null_formula_bg_requests(ws_id: int, columns_list: List[Tuple],
                                     data_start: int, n_player_rows: int,
                                     n_data_rows: int) -> list:
    """
    Build requests to set black background on cells where the row's
    formula is None:
      - player rows where values.player is None (team-only columns)
      - team row where values.team is None
      - opponent row where values.opponents is None
    Config-driven: reads formula presence from column definitions.
    """
    black = get_color_for_raw(COLORS['black'])
    requests = []
    # +1 offset accounts for the separator row between players and team/opp
    team_row = data_start + n_player_rows + 1
    opp_row = data_start + n_player_rows + 2

    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        values = col_def.get('values', {})

        # Black bg on player rows for team-only columns
        if values.get('player') is None and n_player_rows > 0:
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

        # Team row: black bg if values.team is None
        if values.get('team') is None:
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
        # Opponents row: black bg if values.opponents is None
        if values.get('opponents') is None:
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


