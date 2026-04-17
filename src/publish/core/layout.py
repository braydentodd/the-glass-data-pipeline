from typing import List, Optional, Any, Tuple
from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.core.formatting import ROW_INDEXES
from src.publish.definitions.config import (SECTIONS_CONFIG, SUBSECTIONS, SHEET_FORMATTING,
                                STAT_RATES, DEFAULT_STAT_RATE, SUMMARY_THRESHOLDS)
from src.publish.core.calculations import get_percentile_rank, evaluate_formula, calculate_entity_stats, evaluate_expression
from src.publish.core.formatting import format_section_header, format_stat_value, format_height


import re
def _base_section(ctx: str) -> str:
    """Extract the base section name from a potentially composite context key.

    'current_stats__per_possession' -> 'current_stats'
    'historical_stats_3yr__per_possession' -> 'historical_stats'
    'entities' -> 'entities'
    """
    if not ctx:
        return ctx

    ctx_prefix = ctx.split('__')[0]
    
    # Match the mapped prefix cleanly against registered config sections
    for section in SECTIONS_CONFIG.keys():
        if ctx_prefix.startswith(section):
            return section
            
    return ctx_prefix


def _normalize_subsection_key(subsection: Optional[str]) -> Optional[str]:
    """Map subsection keys to canonical SUBSECTIONS keys (case-insensitive)."""
    if subsection is None:
        return None

    raw = str(subsection).strip()
    if not raw:
        return None

    raw_lower = raw.lower()
    for key in SUBSECTIONS.keys():
        if key.lower() == raw_lower:
            return key

    return raw


def _subsection_display_name(subsection: Optional[str]) -> str:
    """Resolve display name for a subsection key with sensible fallback."""
    key = _normalize_subsection_key(subsection)
    if key is None:
        return ''

    cfg = SUBSECTIONS.get(key, {})
    if 'display_name' in cfg:
        return cfg['display_name']
    return str(key).replace('_', ' ').title()


def _format_companion(rank: float, diff: Optional[float], base_def: dict) -> str:
    """Format a percentile companion cell as 'rank\\n+/-diff'.

    Displays the percentile rank on the first line and the over/under
    vs league average (50th percentile value) on the second line.
    """
    rank_str = f"{rank:.1f}"
    if '.' in rank_str:
        rank_str = rank_str.rstrip('0').rstrip('.')

    if diff is None:
        return rank_str

    decimals = base_def.get('decimal_places')
    if decimals is None:
        decimals = 1

    diff_str = f"{diff:+.{decimals}f}"
    if '.' in diff_str:
        diff_str = diff_str.rstrip('0').rstrip('.')

    if diff_str in ('+', '-'):
        diff_str = '+0'
    if diff_str == '-0':
        diff_str = '+0'

    return f"{rank_str}\n{diff_str}"


_SEPARATOR_DEF = {
    'is_separator': True,
    'separator_type': 'section',
    'description': '',
    'sections': [],
    'subsection': None,
    'tabs': ['all_teams', 'all_players', 'individual_team'],
    'stats_mode': 'both',
    'percentile': None,
    'editable': False,
    'scale_with_rate': False,
    'format': 'text',
    'decimal_places': None,
    'width_class': None,
    'leagues': ['nba', 'ncaa'],
    'default': None,
    'align': 'center',
    'emphasis': None,
    'values': {},
}


def _make_separator(context_key: str, visible: bool, separator_type: str = 'section') -> Tuple:
    """Create a separator column tuple for insertion between groups."""
    sep_def = dict(_SEPARATOR_DEF)
    sep_def['separator_type'] = separator_type
    if separator_type == 'subsection':
        sep_def['width_class'] = SHEET_FORMATTING.get('subsection_separator_width', 2)
    else:
        sep_def['width_class'] = SHEET_FORMATTING.get('section_separator_width', 4)
    return ('_sep', sep_def, visible, context_key)

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
        'tabs': base_def.get('tabs', ['all_teams', 'all_players', 'individual_team']),
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
    subsection_filter = _normalize_subsection_key(subsection) if subsection else None

    for col_key, col_def in columns.items():
        if section and section not in col_def.get('sections', []):
            continue
        col_subsection = _normalize_subsection_key(col_def.get('subsection'))
        if subsection_filter and col_subsection != subsection_filter:
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
                                       include_percentiles: bool = False,
                                       tab_type: str = None) -> List[Tuple]:
    """
    Get ordered columns for a section and entity.
    All sections with subsection-assigned columns are ordered by SUBSECTIONS;
    columns without a subsection come first in definition order.
    """
    columns = get_columns_by_filters(
        section=section, entity=entity,
        stats_mode=stats_mode, include_percentiles=include_percentiles
    )

    # Convert values object for opponent columns (only in stats sections)
    is_stats_section = SECTIONS_CONFIG.get(section, {}).get('stats_timeframe') is not None
    if tab_type in ['all_teams', 'teams'] and is_stats_section:
        opp_columns = {}
        for col_key, col_def in columns.items():
            opp_expr = col_def.get('values', {}).get('opponents')
            if opp_expr:
                opp_def = dict(col_def)
                opp_def['display_name'] = f"{col_key}"
                opp_def['values'] = {'team': opp_expr}
                opp_def['is_opponent_col'] = True
                opp_def['percentile'] = 'standard'
                opp_def['subsection'] = 'opponent'
                
                opp_key = f'opp_{col_key}'
                opp_columns[opp_key] = opp_def
                
                # Companion if needed
                if include_percentiles and 'percentile' in opp_def:
                    opp_pct_key = f"{opp_key}_pct"
                    opp_columns[opp_pct_key] = _make_companion_def(opp_def, opp_key, opp_pct_key)

        columns.update(opp_columns)

    # Separate columns with and without subsections
    no_subsec = []
    subsec_groups = {}
    for col_key, col_def in columns.items():
        subsec = _normalize_subsection_key(col_def.get('subsection'))
        if subsec is None:
            no_subsec.append((col_key, col_def))
        else:
            if subsec not in subsec_groups:
                subsec_groups[subsec] = []
            subsec_groups[subsec].append((col_key, col_def))

    # Columns without subsection first, then ordered by SUBSECTIONS
    ordered = list(no_subsec)
    ordered_subsections = set()
    for subsec, cfg in SUBSECTIONS.items():
        # Check if the subsection is applicable for this section and tab
        cfg_tabs = cfg.get('tabs', [])
        tab_match = True
        if tab_type:
            if tab_type == 'individual_team' and 'team' in cfg_tabs:
                tab_match = True
            elif tab_type in cfg_tabs:
                tab_match = True
            else:
                tab_match = False

        if section not in cfg.get('sections', []) or not tab_match:
            continue
        
        if subsec in subsec_groups:
            ordered.extend(subsec_groups[subsec])
            ordered_subsections.add(subsec)

    # Keep any unmapped subsection columns instead of dropping them.
    for subsec, subsec_cols in subsec_groups.items():
        if subsec not in ordered_subsections:
            ordered.extend(subsec_cols)
    return ordered


def build_tab_columns(entity: str = 'player', stats_mode: str = 'both',
                        tab_type: str = 'individual_team',
                        default_mode: str = DEFAULT_STAT_RATE,
                        league: str = None,
                        default_timeframe: int = 3) -> List[Tuple]:
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
        'individual_team': 'individual_team',
        'team': 'individual_team',
        'all_players': 'all_players',
        'players': 'all_players',
        'all_teams': 'all_teams',
        'teams': 'all_teams',
    }
    tab_key = _TAB_TYPE_KEY.get(tab_type, 'team')
    col_entity = 'all_teams' if tab_key == 'all_teams' else entity
    pct_columns = generate_percentile_columns()

    def _normalize_tabs(col_def):
        col_tabs = col_def.get('tabs', ['all_teams', 'all_players', 'individual_team'])
        if isinstance(col_tabs, str):
            return [col_tabs]
        return col_tabs

    def _skip_column(col_def):
        """Return True if this column should be skipped for the current context."""
        if tab_key not in _normalize_tabs(col_def):
            return True
        if league and league not in col_def.get('leagues', []):
            return True
        if tab_key == 'all_teams':
            vals = col_def.get('values', {})
            if 'all_teams' not in vals and 'teams' not in vals and 'team' not in vals:
                return True
        return False

    def _append_section_columns(section, context_key, mode_visible):
        """Append columns for a section with given context key and base visibility."""
        section_cols = get_columns_for_section_and_entity(
            section=section, entity=None,
            stats_mode='both', include_percentiles=False,
            tab_type=tab_key
        )
        prev_subsection_key = None
        for col_key, col_def in section_cols:
            if _skip_column(col_def):
                continue

            subsection_key = _normalize_subsection_key(col_def.get('subsection')) or '__none__'
            if prev_subsection_key is not None and subsection_key != prev_subsection_key:
                all_columns.append(_make_separator(context_key, mode_visible, 'subsection'))
            prev_subsection_key = subsection_key

            col_stats_mode = col_def.get('stats_mode', 'both')
            visible = mode_visible
            if hide_advanced and col_stats_mode == 'advanced':
                visible = False
            elif not hide_advanced and col_stats_mode == 'basic':
                visible = False

            all_columns.append((col_key, col_def, visible, context_key))

            pct_key = f"{col_key}_pct"
            if col_def.get('percentile'):
                pct_def = pct_columns.get(pct_key)
                if not pct_def:
                    pct_def = _make_companion_def(col_def, col_key, pct_key)
                all_columns.append((pct_key, pct_def, visible, context_key))

    all_columns = []
    
    sections = list(SECTIONS_CONFIG.keys())

    for idx, section in enumerate(sections):
        section_cfg = SECTIONS_CONFIG.get(section, {})
        is_last_section = (idx == len(sections) - 1)

        if section_cfg.get('stats_timeframe'):
            # Current stats just use the normal rate tripling
            if section == 'current_stats':
                for stat_rate in STAT_RATES:
                    context_key = f'{section}__{stat_rate}'
                    mode_visible = (stat_rate == default_mode)

                    _append_section_columns(section, context_key, mode_visible)
                    if not is_last_section:
                        all_columns.append(_make_separator(context_key, mode_visible, 'section'))
            else:
                # Historical and Postseason expand by rate AND timeframe
                from src.publish.definitions.config import HISTORICAL_TIMEFRAMES
                supported_years = list(HISTORICAL_TIMEFRAMES.keys())
                for y in supported_years:
                    for stat_rate in STAT_RATES:
                        context_key = f'{section}_{y}yr__{stat_rate}'
                        mode_visible = (stat_rate == default_mode and y == default_timeframe)
                        
                        _append_section_columns(section, context_key, mode_visible)
                        if not is_last_section:
                            all_columns.append(_make_separator(context_key, mode_visible, 'section'))

        else:
            # Non-stats sections: single copy, always visible
            _append_section_columns(section, section, True)
            if not is_last_section and section != 'entities':
                all_columns.append(_make_separator(section, True, 'section'))

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
    row1, row2, row3, row3_clean = [], [], [], []
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
        
        local_hist_config = historical_config
        m = re.search(r'_(?:[a-zA-Z]+_)?(\d+)yr(?:__|$)', section) or re.search(r'_(\d+)yr(?:__|$)', section)
        if m:
            local_hist_config = {'mode': 'seasons', 'value': int(m.group(1))}
            
        base_cfg = SECTIONS_CONFIG.get(base, {})
        if base_cfg.get('stats_timeframe') and current_season:
            return format_section_header(
                base, current_season=current_season,
                historical_config=local_hist_config,
                is_postseason=(base == 'postseason_stats'),
                mode=sec_mode)
        return base_cfg.get('display_name', section)

    for idx, entry in enumerate(columns_list):
        col_key, col_def = entry[0], entry[1]
        section = entry[3] if len(entry) > 3 else (col_def.get('sections', ['unknown'])[0])
        subsection = _normalize_subsection_key(col_def.get('subsection'))

        # Separator columns break merges and emit empty cells
        if col_def.get('is_separator'):
            sep_type = col_def.get('separator_type', 'section')
            if sep_type == 'section':
                if cur_section is not None and sec_start < idx:
                    merges.append({'row': ROW_INDEXES['section_header_row'], 'start_col': sec_start, 'end_col': idx, 'value': _get_display(cur_section)})
                cur_section = None
                sec_start = idx + 1
            if cur_subsection is not None and sub_start < idx:
                merges.append({'row': ROW_INDEXES['subsection_header_row'], 'start_col': sub_start, 'end_col': idx, 'value': _subsection_display_name(cur_subsection)})
            cur_subsection = None
            sub_start = idx + 1
            if sep_type == 'section':
                row1.append('')
            else:
                # If subsection separator, the section merge continues
                # but we emit an empty cell for row1 (won't be seen because it's merged)
                row1.append('')
            row2.append('')
            row3.append('')
            continue

        # Row 0: Section headers (grouped by section)
        if section != cur_section:
            if cur_section is not None and sec_start < idx:
                display = _get_display(cur_section)
                merges.append({'row': ROW_INDEXES['section_header_row'], 'start_col': sec_start, 'end_col': idx, 'value': display})
            # Close pending subsection merge before switching sections
            if cur_subsection is not None and sub_start < idx:
                sub_display = _subsection_display_name(cur_subsection)
                merges.append({'row': ROW_INDEXES['subsection_header_row'], 'start_col': sub_start, 'end_col': idx, 'value': sub_display})
            cur_section = section
            sec_start = idx
            row1.append(_get_display(section))
            # Reset subsection tracking on section change
            cur_subsection = None
            sub_start = idx
        else:
            row1.append('')

        # Row 1: Subsection headers (all sections with subsections)
        if subsection:
            if subsection != cur_subsection:
                if cur_subsection is not None and sub_start < idx:
                    sub_display = _subsection_display_name(cur_subsection)
                    merges.append({'row': ROW_INDEXES['subsection_header_row'], 'start_col': sub_start, 'end_col': idx, 'value': sub_display})
                cur_subsection = subsection
                sub_start = idx
                row2.append(_subsection_display_name(subsection))
            else:
                row2.append('')
        else:
            # Close pending subsection merge when entering a column with no subsection
            if cur_subsection is not None and sub_start < idx:
                sub_display = _subsection_display_name(cur_subsection)
                merges.append({'row': ROW_INDEXES['subsection_header_row'], 'start_col': sub_start, 'end_col': idx, 'value': sub_display})
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
            row3_clean.append('')
        elif description:
            spacer = ' ' * fmt.get('header_description_spacer_count', 750)
            row3.append(f"{description}{spacer}{header_key}{spacer}{description}")
            row3_clean.append(header_key)
        else:
            row3.append(header_key)
            row3_clean.append(header_key)

    # Close final merges
    n = len(columns_list)
    if cur_section:
        display = _get_display(cur_section)
        merges.append({'row': ROW_INDEXES['section_header_row'], 'start_col': sec_start, 'end_col': n, 'value': display})
    if cur_subsection:
        sub_display = _subsection_display_name(cur_subsection)
        merges.append({'row': ROW_INDEXES['subsection_header_row'], 'start_col': sub_start, 'end_col': n, 'value': sub_display})

    # ---- Merge column header (row 2) across stat + companion pairs ----
    # Each companion column is immediately after its base stat column.
    # Merge them so the stat name spans both columns.
    col_header_row = ROW_INDEXES['column_header_row']
    filter_row_idx = ROW_INDEXES['filter_row']
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
        'row1': row1, 'row2': row2, 'row3': row3, 'row3_clean': row3_clean,
        'merges': merges
    }


