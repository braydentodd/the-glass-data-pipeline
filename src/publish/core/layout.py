from typing import List, Optional, Any, Tuple
from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.definitions.config import (SECTION_CONFIG, SECTIONS, SUBSECTIONS, SHEET_FORMATTING,
                                STAT_RATES, DEFAULT_STAT_RATE, SUMMARY_THRESHOLDS)
from .calculations import get_percentile_rank, evaluate_formula, calculate_entity_stats, evaluate_expression
from .formatting import format_section_header, format_stat_value, format_height


def _base_section(ctx: str) -> str:
    """Extract the base section name from a potentially composite context key.

    'current_stats__per_possession' -> 'current_stats'
    'entities' -> 'entities'
    """
    if ctx and '__' in ctx:
        return ctx.split('__')[0]
    return ctx

def generate_percentile_columns() -> dict:
    """Auto-generate percentile companion column defs for all columns with percentile set.

    Companion columns are narrow (10px), always visible, and display the
    percentile rank (0-100) with colour shading.  Headers merge across the
    stat + companion pair so the column name spans both.
    """
    pct_columns = {}
    for col_key, col_def in TAB_COLUMNS.items():
        if not col_def.get('percentile'):
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
        'display_name': '',
        'description': '',
        'sections': base_def.get('sections', ['current_stats']),
        'subsection': base_def.get('subsection'),
        'stats_mode': base_def.get('stats_mode', 'both'),
        'percentile': None,
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 0,
        'is_generated_percentile': True,
        'is_percentile_companion': True,
        'base_stat': base_key,
        'base_percentile': base_def.get('percentile', 'standard'),
        'values': base_def.get('values', {}),
        'is_opponent_col': base_def.get('is_opponent_col', False),
        'width_class': SHEET_FORMATTING.get('percentile_companion_width', 10),
        'tabs': base_def.get('tabs', ['teams', 'players', 'team']),
    }


def get_all_columns_with_percentiles() -> dict:
    """Get TAB_COLUMNS plus auto-generated percentile columns."""
    all_cols = dict(TAB_COLUMNS)
    all_cols.update(generate_percentile_columns())
    return all_cols


def get_columns_by_filters(section=None, subsection=None, entity=None,
                           stats_mode=None, include_percentiles=False) -> dict:
    """
    Get columns matching specified filters.

    Args:
        section: Filter by section name
        subsection: Filter by subsection name
        entity: 'player', 'team', or 'opponents' — checks values dict
        stats_mode: 'basic', 'advanced', or 'both'
        include_percentiles: Include auto-generated percentile columns
    """
    columns = get_all_columns_with_percentiles() if include_percentiles else TAB_COLUMNS
    filtered = {}

    for col_key, col_def in columns.items():
        if section and section not in col_def.get('sections', []):
            continue
        if subsection and col_def.get('subsection') != subsection:
            continue
        if entity:
            if entity not in col_def.get('values', {}):
                continue
        if stats_mode and stats_mode != 'both':
            col_mode = col_def.get('stats_mode', 'both')
            if col_mode != 'both' and col_mode != stats_mode:
                continue
        filtered[col_key] = col_def

    return filtered


def get_columns_for_section_and_entity(section: str, entity: str,
                                       stats_mode: str = 'both',
                                       include_percentiles: bool = False) -> List[Tuple]:
    """
    Get ordered columns for a section and entity.
    Stats sections are ordered by SUBSECTIONS; others by definition order.
    """
    columns = get_columns_by_filters(
        section=section, entity=entity,
        stats_mode=stats_mode, include_percentiles=include_percentiles
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


def build_tab_columns(entity: str = 'player', stats_mode: str = 'both',
                        tab_type: str = 'team',
                        default_mode: str = DEFAULT_STAT_RATE,
                        league: str = None) -> List[Tuple]:
    """
    Build complete column structure for a tab with rate tripling.

    Returns list of (column_key, column_def, visible, context_section) tuples.

    Stats sections are tripled — each appears once per STAT_RATE with a composite
    context key like 'current_stats__per_possession'. Only the default_mode variant
    is visible; others are hidden for instant rate switching via column show/hide.

    Percentile columns are interleaved immediately after their base stat column.
    Columns are filtered by their 'tabs' array and 'leagues' list.
    """
    fmt = SHEET_FORMATTING
    hide_advanced = fmt.get('hide_advanced_columns', True)

    _TAB_TYPE_KEY = {
        'team': 'team',
        'players': 'players',
        'teams': 'teams',
    }
    tab_key = _TAB_TYPE_KEY.get(tab_type, 'team')
    col_entity = 'teams' if tab_type == 'teams' else entity
    pct_columns = generate_percentile_columns()

    def _normalize_tabs(col_def):
        col_tabs = col_def.get('tabs', ['teams', 'players', 'team'])
        if isinstance(col_tabs, str):
            return [col_tabs]
        return col_tabs

    def _skip_column(col_def):
        """Return True if this column should be skipped for the current context."""
        if tab_key not in _normalize_tabs(col_def):
            return True
        if league and league not in col_def.get('leagues', []):
            return True
        if tab_type == 'teams':
            vals = col_def.get('values', {})
            if 'teams' not in vals and 'team' not in vals:
                return True
        return False

    def _append_section_columns(section, context_key, mode_visible):
        """Append columns for a section with given context key and base visibility."""
        section_cols = get_columns_for_section_and_entity(
            section=section, entity=None,
            stats_mode='both', include_percentiles=False
        )
        for col_key, col_def in section_cols:
            if _skip_column(col_def):
                continue

            col_stats_mode = col_def.get('stats_mode', 'both')
            visible = mode_visible
            if hide_advanced and col_stats_mode == 'advanced':
                visible = False
            elif not hide_advanced and col_stats_mode == 'basic':
                visible = False

            all_columns.append((col_key, col_def, visible, context_key))

            pct_key = f"{col_key}_pct"
            if col_def.get('percentile') and pct_key in pct_columns:
                pct_def = pct_columns[pct_key]
                all_columns.append((pct_key, pct_def, visible, context_key))

    all_columns = []

    for section in SECTIONS:
        section_cfg = SECTION_CONFIG.get(section, {})

        if section_cfg.get('is_stats_section'):
            # Triple: emit columns once per stat rate
            for stat_rate in STAT_RATES:
                context_key = f'{section}__{stat_rate}'
                mode_visible = (stat_rate == default_mode)
                _append_section_columns(section, context_key, mode_visible)
        else:
            # Non-stats sections: single copy, always visible
            section_cols = get_columns_for_section_and_entity(
                section=section, entity=None,
                stats_mode='both', include_percentiles=False
            )
            for col_key, col_def in section_cols:
                if _skip_column(col_def):
                    continue

                col_stats_mode = col_def.get('stats_mode', 'both')
                visible = True
                if hide_advanced and col_stats_mode == 'advanced':
                    visible = False
                elif not hide_advanced and col_stats_mode == 'basic':
                    visible = False

                all_columns.append((col_key, col_def, visible, section))

                pct_key = f"{col_key}_pct"
                if col_def.get('percentile') and pct_key in pct_columns:
                    pct_def = pct_columns[pct_key]
                    all_columns.append((pct_key, pct_def, visible, section))

    # --- Teams sheet: insert opponent columns ---
    if tab_type == 'teams':
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
        is_stats = SECTION_CONFIG.get(_base_section(ctx), {}).get('is_stats_section', False)
        if not is_stats or col_def.get('is_generated_percentile'):
            continue
        opp_expr = col_def.get('values', {}).get('opponents')
        if not opp_expr:
            continue

        col_mode = col_def.get('stats_mode', 'both')
        opp_def = dict(col_def)
        opp_def['display_name'] = f"O{col_key}"
        opp_values = {'team': opp_expr}
        opp_def['values'] = opp_values
        opp_def['is_opponent_col'] = True
        opp_def['percentile'] = 'standard'
        opp_def['subsection'] = 'opponent'
        opp_key = f'opp_{col_key}'

        # Opponent inherits full visibility from the base stat column.
        # `vis` already encodes rate toggle + advanced/basic mode.
        opp_vis = vis

        if ctx not in opp_by_section:
            opp_by_section[ctx] = []
        opp_by_section[ctx].append((opp_key, opp_def, opp_vis, ctx))

        # Generate percentile companion for opponent column
        opp_pct_key = f"{opp_key}_pct"
        opp_pct_def = _make_companion_def(opp_def, opp_key, opp_pct_key)
        opp_by_section[ctx].append((opp_pct_key, opp_pct_def, opp_vis, ctx))

    # Second pass: rebuild columns, inserting opponent block between defense and onoff
    result: List[Tuple] = []
    prev_subsection = None
    prev_ctx = None

    for entry in columns:
        col_key, col_def, vis, ctx = entry
        subsection = col_def.get('subsection')
        is_stats = SECTION_CONFIG.get(_base_section(ctx), {}).get('is_stats_section', False)

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

def build_headers(columns_list: List[Tuple], mode: str = 'per_possession',
                  team_name: str = '',
                  current_season: int = 0,
                  historical_config: Optional[dict] = None,
                  hist_timeframe: str = '',
                  post_timeframe: str = '') -> dict:
    """
    Build header rows for Google Sheets (4-row layout).

    Row 0: Section headers (one merge per section/mode variant)
    Row 1: Subsection headers (hidden by default)
    Row 2: Column names
    Row 3: Empty filter row

    Composite context keys like 'current_stats__per_possession' produce
    mode-specific section headers (e.g. "2024-25 Stats (per 100 Poss)").
    """
    row1, row2, row3 = [], [], []
    merges = []
    fmt = SHEET_FORMATTING

    cur_section = None
    sec_start = 0
    cur_subsection = None
    sub_start = 0

    def _get_display(section):
        if section == 'entities':
            return team_name
        base = _base_section(section)
        sec_mode = section.split('__')[1] if '__' in section else mode
        base_cfg = SECTION_CONFIG.get(base, {})
        if base_cfg.get('is_stats_section') and current_season:
            return format_section_header(
                base, current_season=current_season,
                historical_config=historical_config,
                is_postseason=(base == 'postseason_stats'),
                mode=sec_mode)
        return base_cfg.get('display_name', section)

    for idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        section = entry[3] if len(entry) > 3 else (col_def.get('sections', ['unknown'])[0])
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
        sc = SECTION_CONFIG.get(_base_section(section), {})
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

        # Row 2: Column display names — use mode from composite context key
        # Format: "{description}{spacer}{col_key}{spacer}{description}"
        # The spacer creates a wide string. CLIP truncation shows just the key;
        # clicking the cell reveals the full description in the formula bar.
        col_mode = section.split('__')[1] if '__' in section else mode
        override = col_def.get('mode_overrides', {}).get(col_mode)
        active_def = override if override else col_def
        description = active_def.get('description', col_def.get('description', ''))
        header_key = active_def.get('display_name', col_key)
        if col_def.get('is_generated_percentile', False):
            row3.append('')
        elif description:
            spacer = ' ' * fmt.get('header_description_spacer_count', 750)
            row3.append(f"{description}{spacer}{header_key}{spacer}{description}")
        else:
            row3.append(header_key)

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
                     mode: str = 'per_possession', seasons_str: str = '',
                     row_section: Optional[str] = None,
                     section_data: Optional[dict] = None,
                     context: Optional[dict] = None) -> list:
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
                sec_entity, entity_type, sec_mode, context
            )
        # For non-stats columns, use the first section's entity data
        first_section = next(iter(section_data))
        primary_entity = section_data[first_section][0]
        primary_calculated = calculated_by_section[first_section]
    else:
        # Legacy single-section mode
        primary_entity = entity_data
        primary_calculated = calculate_entity_stats(entity_data, entity_type, mode, context)

    row = []

    for entry in columns_list:
        col_key, col_def = entry[0], entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        col_ctx_cfg = SECTION_CONFIG.get(_base_section(col_ctx), {})
        is_stats_section = col_ctx_cfg.get('is_stats_section', False)

        if section_data and is_stats_section:
            # Pick the right data for this section
            if col_ctx in section_data:
                sec_entity, sec_pcts, _ = section_data[col_ctx]
                calculated = calculated_by_section[col_ctx]
                pcts = sec_pcts
            else:
                row.append('')
                continue
        elif row_section and is_stats_section and _base_section(col_ctx) != _base_section(row_section):
            # Legacy mode — blank out wrong-section columns
            row.append('')
            continue
        else:
            # Non-stats column or matching section
            calculated = primary_calculated if not section_data else primary_calculated
            pcts = percentiles if not section_data else (
                section_data[first_section][1] if section_data else percentiles
            )
            sec_entity = primary_entity

        if is_pct:
            base_key = col_def.get('base_stat', col_key.replace('_pct', ''))
            base_def = TAB_COLUMNS.get(base_key, {})
            value = calculated.get(base_key)

            if value is not None and isinstance(value, (int, float)) and base_key in pcts:
                reverse = base_def.get('percentile') == 'reverse'
                rank = get_percentile_rank(value, pcts[base_key], reverse)
                row.append(round(rank))
            else:
                row.append('')
            continue

        # Non-percentile column
        # Info column (non-stat section) — simple field lookup
        if not col_ctx_cfg.get('is_stats_section', False) and col_def.get('percentile') is None:
            use_entity = sec_entity if section_data and is_stats_section else primary_entity
            _col_mode = col_ctx.split('__')[1] if col_ctx and '__' in col_ctx else mode
            value = evaluate_formula(col_key, use_entity, entity_type, _col_mode, context)
            if value is None:
                row.append('')
            elif col_def.get('format') == 'measurement':
                row.append(format_height(value))
            else:
                row.append(value)
            continue

        # Dynamically-generated opponent column (Teams sheet) — eval directly
        if col_def.get('is_opponent_col'):
            opp_expr = col_def.get('values', {}).get('team')
            value = evaluate_expression(opp_expr, sec_entity)
            _col_mode = col_ctx.split('__')[1] if col_ctx and '__' in col_ctx else mode
            override = col_def.get('mode_overrides', {}).get(_col_mode)
            active_def = override if override else col_def
            formatted = format_stat_value(value, active_def)
            row.append(formatted if formatted is not None else '')
            continue

        # Stat column — use pre-calculated value
        value = calculated.get(col_key)
        _col_mode = col_ctx.split('__')[1] if col_ctx and '__' in col_ctx else mode
        override = col_def.get('mode_overrides', {}).get(_col_mode)
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
                            pct_by_rate: dict,
                            entity_type: str = 'player',
                            hist_seasons: str = '', post_seasons: str = '',
                            opp_percentiles: Optional[dict] = None,
                            context: Optional[dict] = None) -> Tuple[list, List[dict]]:
    """
    Build a single merged data row with current + historical + postseason stats.

    All 3 stat rates are written simultaneously via composite section keys
    (e.g. 'current_stats__per_possession'). Rate switching is handled by
    column visibility in the spreadsheet.

    pct_by_rate: {rate: {base_section: {col_key: sorted_values}}}
    opp_percentiles: {col_key: {composite_section: sorted_vals}}
    """
    # Build section_data with composite keys for all rates
    _SECTION_TO_DATA = {
        'current_stats': (current_data, ''),
        'historical_stats': (historical_data, hist_seasons),
        'postseason_stats': (postseason_data, post_seasons),
    }

    section_data = {}
    for rate_name in STAT_RATES:
        rate_pcts = pct_by_rate.get(rate_name, {})
        for base_section, (entity_data, seasons) in _SECTION_TO_DATA.items():
            if entity_data:
                composite_key = f'{base_section}__{rate_name}'
                section_pcts = rate_pcts.get(base_section, {})
                section_data[composite_key] = (entity_data, section_pcts, seasons)

    primary_entity = current_data or historical_data or postseason_data or {}

    row = build_entity_row(
        primary_entity, columns_list, {},
        entity_type=entity_type,
        section_data=section_data,
        context=context,
    )

    # Collect percentile info for companion column shading.
    percentile_cells = []
    for col_idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        col_ctx = entry[3] if len(entry) > 3 else None
        is_pct = col_def.get('is_generated_percentile', False)

        if not is_pct:
            continue

        col_ctx_cfg = SECTION_CONFIG.get(_base_section(col_ctx), {})
        is_stats_section = col_ctx_cfg.get('is_stats_section', False)
        base_key = col_def.get('base_stat', col_key.replace('_pct', ''))

        if is_stats_section:
            if col_ctx not in section_data:
                continue
            sec_entity, sec_pcts, _ = section_data[col_ctx]
            sec_mode = col_ctx.split('__')[1] if col_ctx and '__' in col_ctx else DEFAULT_STAT_RATE

            # Opponent companion: compute value from base opponent formula
            if col_def.get('is_opponent_col') and opp_percentiles:
                opp_pop = opp_percentiles.get(base_key, {}).get(_base_section(col_ctx))
                if opp_pop is not None:
                    base_col_def = None
                    for e2 in columns_list:
                        if e2[0] == base_key:
                            base_col_def = e2[1]
                            break
                    if base_col_def:
                        opp_expr = base_col_def.get('values', {}).get('team')
                        value = evaluate_expression(opp_expr, sec_entity)
                        if value is not None:
                            reverse = base_col_def.get('percentile') == 'reverse'
                            rank = get_percentile_rank(value, opp_pop, reverse)
                            row[col_idx] = round(rank)
                            percentile_cells.append({
                                'col': col_idx,
                                'percentile': rank,
                                'reverse': False,
                            })
                            # Also color the base opponent stat cell
                            if col_idx > 0:
                                percentile_cells.append({
                                    'col': col_idx - 1,
                                    'percentile': rank,
                                    'reverse': False,
                                })
                continue

            # Regular companion
            base_def = TAB_COLUMNS.get(base_key, col_def)
            calculated = calculate_entity_stats(sec_entity, entity_type, sec_mode, context)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('percentile') == 'reverse'
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })
                # Also color the base stat value cell
                if col_idx > 0:
                    percentile_cells.append({
                        'col': col_idx - 1,
                        'percentile': rank,
                        'reverse': reverse,
                    })
        else:
            # Non-stats section — use default mode's current_stats percentiles
            default_current_key = f'current_stats__{DEFAULT_STAT_RATE}'
            if default_current_key in section_data:
                sec_entity, sec_pcts, _ = section_data[default_current_key]
            elif section_data:
                first_key = next(iter(section_data))
                sec_entity, sec_pcts, _ = section_data[first_key]
            else:
                continue
            base_def = TAB_COLUMNS.get(base_key, col_def)
            calculated = calculate_entity_stats(sec_entity, entity_type, DEFAULT_STAT_RATE, context)
            value = calculated.get(base_key)

            if value is not None and base_key in sec_pcts:
                reverse = base_def.get('percentile') == 'reverse'
                rank = get_percentile_rank(value, sec_pcts[base_key], reverse)
                percentile_cells.append({
                    'col': col_idx,
                    'percentile': rank,
                    'reverse': reverse,
                })
                # Also color the base stat value cell
                if col_idx > 0:
                    percentile_cells.append({
                        'col': col_idx - 1,
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
                       mode: str = 'per_possession',
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

            # Name column gets the label
            if col_key == 'name':
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

            # Non-stat, non-percentile columns are blank
            if (not col_def.get('percentile')
                    and not col_def.get('is_generated_percentile', False)):
                row.append('')
                continue

            # Opponent columns: use opp_percentiles populations
            if col_def.get('is_opponent_col') and opp_percentiles:
                col_ctx_cfg = SECTION_CONFIG.get(_base_section(col_ctx), {})
                if col_ctx_cfg.get('is_stats_section') and col_ctx:
                    opp_pop = opp_percentiles.get(col_key, {}).get(_base_section(col_ctx))
                    if opp_pop:
                        reverse = col_def.get('percentile') == 'reverse'
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
            col_ctx_cfg = SECTION_CONFIG.get(_base_section(col_ctx), {})
            is_stats_section = col_ctx_cfg.get('is_stats_section', False)
            pop_key = f'{col_ctx}:{col_key}'

            # Stats-section columns: look up via section:col_key
            if is_stats_section and (
                    pop_key in percentile_pops or col_key in percentile_pops):
                sorted_vals = percentile_pops.get(pop_key,
                              percentile_pops.get(col_key))
                if sorted_vals:
                    reverse = col_def.get('percentile') == 'reverse'
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

            # Non-stats columns with percentile (player_info): direct col_key lookup
            if not is_stats_section and col_def.get('percentile'):
                sorted_vals = percentile_pops.get(col_key)
                if sorted_vals:
                    reverse = col_def.get('percentile') == 'reverse'
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


