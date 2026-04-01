import logging
from typing import Dict, List, Optional, Any, Tuple
from src.sheets.config import SHEETS_COLUMNS
from src.sheets.config import (SECTION_CONFIG, SECTIONS, SUBSECTIONS, STAT_CONSTANTS, DEFAULT_STAT_MODE, COLORS, COLOR_THRESHOLDS, SHEET_FORMATTING)
from .calculations import get_percentile_rank, evaluate_formula, calculate_entity_stats
from .formatting import format_section_header, format_stat_value, get_color_for_percentile, get_color_for_raw, format_seasons_range
def generate_percentile_columns() -> dict:
    """Auto-generate percentile companion column defs for all columns with has_percentile=True.

    Companion columns are narrow (10px), always visible, and display the
    percentile rank (0-100) with colour shading.  Headers merge across the
    stat + companion pair so the column name spans both.
    """
    pct_columns = {}
    for col_key, col_def in SHEETS_COLUMNS.items():
        if not col_def.get('has_percentile'):
            continue
        pct_key = f"{col_key}_pct"
        pct_columns[pct_key] = _make_companion_def(col_def, col_key, pct_key)
    return pct_columns


def _make_companion_def(base_def: dict, base_key: str,
                        pct_key: str = '') -> dict:
    """Create a percentile companion column definition from a base stat column.

    Used by generate_percentile_columns() for static columns and by
    _insert_opponent_columns() for dynamically-generated opponent columns.
    """
    if not pct_key:
        pct_key = f"{base_key}_pct"
    return {
        'key': pct_key,
        'stat_category': base_def.get('stat_category', 'none'),
        'display_name': '',  # Companion has no header text (merged with stat)
        'description': '',
        'section': base_def.get('section', ['current_stats']),
        'subsection': base_def.get('subsection'),
        'stat_mode': base_def.get('stat_mode', 'both'),
        'has_percentile': False,
        'editable': False,
        'reverse_percentile': base_def.get('reverse_percentile', False),
        'scale_with_mode': False,
        'format': 'number',
        'decimal_places': 0,
        'is_generated_percentile': True,
        'is_percentile_companion': True,
        'base_stat': base_key,
        'player_formula': base_def.get('player_formula'),
        'team_formula': base_def.get('team_formula'),
        'opponents_formula': base_def.get('opponents_formula'),
        'is_opponent_col': base_def.get('is_opponent_col', False),
        'minimum_width': SHEET_FORMATTING.get('percentile_companion_width', 10),
        'sheets': base_def.get('sheets', ['all_teams', 'all_players', 'teams']),
    }


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
        if stat_mode and stat_mode != 'both':
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
                        league_key: str = 'nba',
                        sheet_type: str = 'team') -> List[Tuple]:
    """
    Build complete column structure for a sheet.

    Returns list of (column_key, column_def, visible, context_section) tuples.

    Single set of columns per section — mode switching triggers a re-sync
    with the new mode rather than column visibility toggling.
    Percentile columns are interleaved immediately after their base stat column.
    Columns are filtered by their 'sheets' array.
    """
    fmt = SHEET_FORMATTING
    hide_advanced = fmt.get('hide_advanced_columns', True)

    # Mapping: sheet_type → key to look for in column 'sheets' array.
    #   'teams'  = individual team sheets (DAL, BOS, …)
    #   'all_teams' = the Teams aggregate sheet
    #   'all_players' = the Players aggregate sheet
    _SHEET_TYPE_KEY = {
        'team': 'teams',
        'players': 'all_players',
        league_key: 'all_players',  # e.g. 'nba' or 'ncaa'
        'teams': 'all_teams',
    }
    sheet_key = _SHEET_TYPE_KEY.get(sheet_type, 'teams')
    col_entity = 'team' if sheet_type == 'teams' else entity
    pct_columns = generate_percentile_columns()

    def _normalize_sheets(col_def):
        col_sheets = col_def.get('sheets', ['all_teams', 'all_players', 'teams'])
        if isinstance(col_sheets, str):
            if col_sheets == 'both':
                return ['all_teams', 'all_players', 'teams']
            elif col_sheets in ('players', league_key):
                return ['all_players']
            elif col_sheets == 'teams':
                return ['teams']
            elif col_sheets == 'all_teams':
                return ['all_teams']
            return ['all_teams', 'all_players', 'teams']
        return col_sheets

    all_columns = []

    for section in SECTIONS:
        section_cols = get_columns_for_section_and_entity(
            section=section, entity=None,
            stat_mode='both', include_percentiles=False
        )

        for col_key, col_def in section_cols:
            if sheet_key not in _normalize_sheets(col_def):
                continue

            # For the Teams aggregate sheet: skip columns with no team_formula entirely
            if sheet_type == 'teams' and col_def.get('team_formula') is None:
                continue

            col_stat_mode = col_def.get('stat_mode', 'both')
            visible = True
            if hide_advanced and col_stat_mode == 'advanced':
                visible = False
            elif not hide_advanced and col_stat_mode == 'basic':
                visible = False

            fkey = f'{col_entity}_formula'
            if not (col_def.get('stat_category', 'none') != 'none' or col_def.get(fkey) is not None):
                visible = False

            all_columns.append((col_key, col_def, visible, section))

            pct_key = f"{col_key}_pct"
            if col_def.get('has_percentile') and pct_key in pct_columns:
                pct_def = pct_columns[pct_key]
                all_columns.append((pct_key, pct_def, True, section))

    # --- Teams sheet: insert opponent columns ---
    if sheet_type == 'teams':
        all_columns = _insert_opponent_columns(
            all_columns, pct_columns, hide_advanced
        )

    return all_columns


def _insert_opponent_columns(columns: List[Tuple], pct_columns: dict,
                             hide_advanced: bool) -> List[Tuple]:
    """Insert opponent stat columns as a single 'opponent' subsection on the Teams sheet.

    Collects opponent columns per section and inserts them between defense and onoff.
    """
    # First pass: collect opponent columns grouped by section
    opp_by_section: dict = {}  # {ctx: [(opp_key, opp_def, vis, ctx), ...]}
    for entry in columns:
        col_key, col_def, vis, ctx = entry
        is_stats = SECTION_CONFIG.get(ctx, {}).get('is_stats_section', False)
        if not is_stats or col_def.get('stat_category', 'none') == 'none' or col_def.get('is_generated_percentile'):
            continue
        opp_formula = col_def.get('opponents_formula')
        if not opp_formula:
            continue

        col_mode = col_def.get('stat_mode', 'both')
        opp_def = dict(col_def)
        opp_def['display_name'] = f"O{col_def['display_name']}"
        opp_def['team_formula'] = opp_formula
        opp_def['opponents_formula'] = None
        opp_def['is_opponent_col'] = True
        opp_def['has_percentile'] = True
        opp_def['subsection'] = 'opponent'
        opp_key = f'opp_{col_key}'

        opp_vis = True
        if hide_advanced and col_mode == 'advanced':
            opp_vis = False
        elif not hide_advanced and col_mode == 'basic':
            opp_vis = False

        if ctx not in opp_by_section:
            opp_by_section[ctx] = []
        opp_by_section[ctx].append((opp_key, opp_def, opp_vis, ctx))

    # Second pass: rebuild columns, inserting opponent block between defense and onoff
    result: List[Tuple] = []
    prev_subsection = None
    prev_ctx = None

    for entry in columns:
        col_key, col_def, vis, ctx = entry
        subsection = col_def.get('subsection')
        is_stats = SECTION_CONFIG.get(ctx, {}).get('is_stats_section', False)

        # Detect transition away from defense within same section
        if (is_stats and prev_subsection == 'defense'
                and subsection not in ('defense', None)
                and prev_ctx == ctx):
            opp_entries = opp_by_section.get(ctx, [])
            if opp_entries:
                result.extend(opp_entries)

        # When switching sections, flush if defense was the last subsection
        if ctx != prev_ctx and prev_ctx is not None:
            if prev_subsection == 'defense':
                opp_entries = opp_by_section.get(prev_ctx, [])
                if opp_entries:
                    result.extend(opp_entries)

        result.append(entry)
        prev_subsection = subsection
        prev_ctx = ctx

    # Flush remaining if the last subsection was defense
    if prev_subsection == 'defense' and prev_ctx:
        opp_entries = opp_by_section.get(prev_ctx, [])
        if opp_entries and opp_entries[0] not in result:
            result.extend(opp_entries)

    return result


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
                  current_season: int = 0,
                  historical_config: Optional[dict] = None,
                  hist_timeframe: str = '',
                  post_timeframe: str = '') -> dict:
    """
    Build header rows for Google Sheets (4-row layout).

    Row 0: Section headers (one merge per section)
    Row 1: Subsection headers (hidden by default)
    Row 2: Column names
    Row 3: Empty filter row
    """
    # Pre-build section header text for the current mode
    _section_headers = {}
    _section_headers['current_stats'] = (
        format_section_header('current_stats', current_season=current_season, mode=mode)
        if current_season else SECTION_CONFIG.get('current_stats', {}).get('display_name', 'Current Stats')
    )
    _section_headers['historical_stats'] = (
        format_section_header('historical_stats', historical_config=historical_config,
                              current_season=current_season, is_postseason=False, mode=mode)
        if current_season else SECTION_CONFIG.get('historical_stats', {}).get('display_name', 'Historical Stats')
    )
    _section_headers['postseason_stats'] = (
        format_section_header('postseason_stats', historical_config=historical_config,
                              current_season=current_season, is_postseason=True, mode=mode)
        if current_season else SECTION_CONFIG.get('postseason_stats', {}).get('display_name', 'Postseason Stats')
    )

    row1, row2, row3 = [], [], []
    merges = []

    cur_section = None
    sec_start = 0
    cur_subsection = None
    sub_start = 0

    def _get_display(section):
        if section == 'entities':
            return team_name
        if section in _section_headers:
            return _section_headers[section]
        return SECTION_CONFIG.get(section, {}).get('display_name', section)

    for idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        section = entry[3] if len(entry) > 3 else (col_def.get('section', ['unknown'])[0])
        subsection = col_def.get('subsection')

        # Row 0: Section headers (grouped by section)
        if section != cur_section:
            if cur_section is not None and sec_start < idx:
                display = _get_display(cur_section)
                merges.append({'row': 0, 'start_col': sec_start, 'end_col': idx, 'value': display})
            # Close pending subsection merge before switching sections
            if cur_subsection is not None and sub_start < idx:
                sub_display = SUBSECTIONS.get(cur_subsection, cur_subsection.title())
                merges.append({'row': 1, 'start_col': sub_start, 'end_col': idx, 'value': sub_display})
            cur_section = section
            sec_start = idx
            row1.append(_get_display(section))
            # Reset subsection tracking on section change
            cur_subsection = None
            sub_start = idx
        else:
            row1.append('')

        # Row 1: Subsection headers
        sc = SECTION_CONFIG.get(section, {})
        if sc.get('is_stats_section') and subsection:
            if subsection != cur_subsection:
                if cur_subsection is not None and sub_start < idx:
                    sub_display = SUBSECTIONS.get(cur_subsection, cur_subsection.title())
                    merges.append({'row': 1, 'start_col': sub_start, 'end_col': idx, 'value': sub_display})
                cur_subsection = subsection
                sub_start = idx
                row2.append(SUBSECTIONS.get(subsection, subsection.title()))
            else:
                row2.append('')
        else:
            # Close pending subsection merge when leaving stats section
            if cur_subsection is not None and sub_start < idx:
                sub_display = SUBSECTIONS.get(cur_subsection, cur_subsection.title())
                merges.append({'row': 1, 'start_col': sub_start, 'end_col': idx, 'value': sub_display})
            cur_subsection = None
            row2.append('')

        # Row 2: Column display names
        override = col_def.get('mode_overrides', {}).get(mode)
        display_name = (override or {}).get('display_name', col_def.get('display_name', col_key))
        row3.append(display_name)

    # Close final merges
    n = len(columns_list)
    if cur_section:
        display = _get_display(cur_section)
        merges.append({'row': 0, 'start_col': sec_start, 'end_col': n, 'value': display})
    if cur_subsection:
        sub_display = SUBSECTIONS.get(cur_subsection, cur_subsection.title())
        merges.append({'row': 1, 'start_col': sub_start, 'end_col': n, 'value': sub_display})

    # ---- Merge column header (row 2) across stat + companion pairs ----
    # Each companion column is immediately after its base stat column.
    # Merge them so the stat name spans both columns.
    col_header_row = SHEET_FORMATTING.get('column_header_row', 2)
    filter_row_idx = SHEET_FORMATTING.get('filter_row', 3)
    for idx, entry in enumerate(columns_list):
        col_def = entry[1]
        if col_def.get('is_generated_percentile', False) and idx > 0:
            # Merge column header: stat name spans stat + companion
            merges.append({
                'row': col_header_row,
                'start_col': idx - 1,
                'end_col': idx + 1,
                'value': row3[idx - 1],  # stat's display name
            })
            # Merge filter row too (keeps auto-filter dropdown only on stat col)
            merges.append({
                'row': filter_row_idx,
                'start_col': idx - 1,
                'end_col': idx + 1,
                'value': '',
            })

    return {
        'row1': row1, 'row2': row2, 'row3': row3,
        'merges': merges
    }


# ============================================================================
# ROW BUILDING
# ============================================================================

def build_entity_row(entity_data: dict, columns_list: List[Tuple],
                     percentiles: dict, entity_type: str = 'player',
                     mode: str = 'per_game', seasons_str: str = '',
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
       section_data = {section_name: (entity_data, percentiles, seasons_str)}
       Fills each stats-section column from its corresponding data.
       Non-stats columns use the first available entity_data.
    """
    if section_data:
        # Merged mode — pre-calculate stats per section (supports composite keys like 'current_stats__per_100')
        calculated_by_section = {}
        for sec_name, (sec_entity, sec_pcts, sec_seasons) in section_data.items():
            # Extract mode from composite key: 'current_stats__per_100' → 'per_100'
            if '__' in sec_name:
                sec_mode = sec_name.split('__')[1]
            else:
                sec_mode = mode
            calculated_by_section[sec_name] = calculate_entity_stats(
                sec_entity, entity_type, sec_mode
            )
        # For non-stats columns, use the first section's entity data
        first_section = next(iter(section_data))
        primary_entity = section_data[first_section][0]
        primary_calculated = calculated_by_section[first_section]
        primary_seasons = section_data[first_section][2]
    else:
        # Legacy single-section mode
        primary_entity = entity_data
        primary_calculated = calculate_entity_stats(entity_data, entity_type, mode)
        primary_seasons = seasons_str

    row = []

    for entry in columns_list:
        col_key, col_def, visible = entry[0], entry[1], entry[2]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        col_ctx_cfg = SECTION_CONFIG.get(col_ctx, {})
        is_stats_section = col_ctx_cfg.get('is_stats_section', False)

        if section_data and is_stats_section:
            # Pick the right data for this section
            if col_ctx in section_data:
                sec_entity, sec_pcts, sec_seasons = section_data[col_ctx]
                calculated = calculated_by_section[col_ctx]
                pcts = sec_pcts
                ystr = sec_seasons
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
            ystr = primary_seasons
            sec_entity = primary_entity

        if is_pct:
            base_key = col_def.get('base_stat', col_key.replace('_pct', ''))
            base_def = SHEETS_COLUMNS.get(base_key, {})
            value = calculated.get(base_key)

            if value is not None and isinstance(value, (int, float)) and base_key in pcts:
                reverse = base_def.get('reverse_percentile', False)
                rank = PERCENTILE_RANK_FN(value, pcts[base_key], reverse)
                row.append(round(rank))
            else:
                row.append('')
            continue

        # Non-percentile column
        # Seasons column: show count of distinct seasons (already COUNT(DISTINCT s.season) from SQL)
        if col_key == 'seasons':
            # In merged mode, get the season count from the section's entity data
            if section_data and is_stats_section and col_ctx in section_data:
                season_count = section_data[col_ctx][0].get('season')
            elif not section_data:
                season_count = entity_data.get('season')
            else:
                season_count = None
            # season count is already an integer from COUNT(DISTINCT s.season)
            # Non-nullable: show 0 when missing (with percentile color)
            if season_count is None or season_count == '':
                row.append(0 if not col_def.get('nullable', True) else '')
            else:
                row.append(season_count)
            continue

        # Info column (non-stat) — simple field lookup
        if col_def.get('stat_category', 'none') == 'none':
            use_entity = sec_entity if section_data and is_stats_section else primary_entity
            value = evaluate_formula(col_key, use_entity, entity_type, mode)
            if value is None:
                row.append('')
            elif col_def.get('format') == 'measurement':
                row.append(format_height(value))
            else:
                row.append(value)
            continue

        # Dynamically-generated opponent column (Teams sheet) — eval directly
        if col_def.get('is_opponent_col'):
            formula_str = col_def.get('team_formula')
            value = _eval_dynamic_formula(formula_str, sec_entity, col_def, mode)
            override = col_def.get('mode_overrides', {}).get(mode)
            active_def = override if override else col_def
            formatted = format_stat_value(value, active_def)
            row.append(formatted if formatted is not None else '')
            continue

        # Stat column — use pre-calculated value
        value = calculated.get(col_key)
        override = col_def.get('mode_overrides', {}).get(mode)
        active_def = override if override else col_def
        formatted = format_stat_value(value, active_def)
        row.append(formatted if formatted is not None else '')

    return row

# ============================
# MOVED FROM FORMATTING.PY
# ============================
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
                            rank = get_percentile_rank(value, opp_pop, reverse)
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
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse)
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
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })

    return row, percentile_cells


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


