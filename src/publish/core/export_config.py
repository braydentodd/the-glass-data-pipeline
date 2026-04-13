import json
import logging
import os
from pathlib import Path
from typing import Optional, Any, Dict
from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.definitions.config import (
    SECTION_CONFIG, GOOGLE_SHEETS_CONFIG, STAT_RATES,
    STAT_CONSTANTS, STAT_RATE_LABELS, DEFAULT_STAT_RATE, SHEET_FORMATTING, COLORS,
    COLOR_THRESHOLDS, SUBSECTIONS, WIDTH_CLASSES, MENU_CONFIG
)
from src.publish.destinations.sheets.layout import build_tab_columns, get_column_index
from src.publish.core.formatting import get_reverse_stats, get_editable_fields
from src.publish.destinations.sheets.payloads import _get_subsection_boundaries, _get_section_boundaries

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parents[4] / 'apps_script' / 'config'


def get_config_for_export(league: str,
                          get_teams_fn=None,
                          id_column_key: str = 'player_id',
                          google_sheets_config: dict = None) -> dict:
    """
    Build JSON-serializable config dict for Apps Script.

    Apps Script uses this as single source of truth — zero hardcoding in JS.
    League-agnostic: derives league-specific values from the league parameter
    when optional args are not provided.

    Exports:
      - column_ranges:            section toggle ranges (team_sheet / {league}_sheet)
      - advanced_column_ranges:   toggle advanced stat columns
      - percentile_column_ranges: toggle percentile columns
      - column_indices:           edit-detection indices (player_id, team, stats_start)
      - stat_rates:               available stat rates with display labels
      - sections:                 section config (display names, toggleability)
      - menu:                     menu structure config for Apps Script UI
    """
    if get_teams_fn is None:
        from src.publish.core.queries import get_teams_from_db
        get_teams_fn = lambda: get_teams_from_db(league)

    if google_sheets_config is None:
        google_sheets_config = GOOGLE_SHEETS_CONFIG.get(league, {})
    league_sheet = f'{league}_sheet'

    # --- Teams dict -------------------------------------------------------
    teams_from_db = get_teams_fn()
    league_teams = {abbr: team_id for team_id, (abbr, name) in teams_from_db.items()}

    # --- Stat columns list -----------------------------------------------
    stat_columns = [k for k, v in TAB_COLUMNS.items()
                    if any(SECTION_CONFIG.get(s, {}).get('is_stats_section') for s in v.get('sections', []))]

    # --- Build full column lists for all sheet types --------------------
    team_columns = build_tab_columns(
        entity='player', stats_mode='both', tab_type='individual_team',
        league=league
    )
    league_columns = build_tab_columns(
        entity='player', stats_mode='both', tab_type='all_players',
        league=league
    )
    teams_columns = build_tab_columns(
        entity='team', stats_mode='both', tab_type='all_teams',
        league=league
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
    for sec in ('current_stats', 'historical_stats', 'postseason_stats',
                'profile', 'evaluation'):
        team_range = _section_range(team_columns, sec)
        league_range = _section_range(league_columns, sec)
        teams_range = _section_range(teams_columns, sec)
        if team_range:
            column_ranges['team_sheet'][sec] = team_range
        if league_range:
            column_ranges[league_sheet][sec] = league_range
        if teams_range:
            column_ranges['teams_sheet'][sec] = teams_range

    # --- Advanced column ranges ------------------------------------------
    def _advanced_indices(cols):
        return sorted([
            i for i, (col_key, col_def, vis, ctx) in enumerate(cols)
            if col_def.get('stats_mode') == 'advanced'
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
            if col_def.get('stats_mode') == 'basic'
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
            if col_def.get('percentile') is not None
            and not col_def.get('is_generated_percentile', False)
        ])

    base_value_column_ranges = {
        'team_sheet':  _contiguous_ranges(_base_value_with_pct_indices(team_columns)),
        league_sheet:  _contiguous_ranges(_base_value_with_pct_indices(league_columns)),
        'teams_sheet': _contiguous_ranges(_base_value_with_pct_indices(teams_columns)),
    }

    # --- Vertical boundaries (for border management in toggles) -----------
    def _boundary_entries(cols, idx_list):
        return [{'col': b + 1, 'hp': bool(cols[b][1].get('percentile'))}
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
        hidden = []
        for i, (ck, cd, v, cx) in enumerate(cols):
            is_stat = SECTION_CONFIG.get(cx, {}).get('is_stats_section', False)
            if not is_stat and cd.get('values', {}).get(entity_type) is None:
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
            sm = cd.get('stats_mode', 'both')
            is_stats = SECTION_CONFIG.get(cx, {}).get('is_stats_section', False)
            meta.append({
                'col': i + 1,
                'pct': bool(cd.get('is_generated_percentile')),
                'adv': sm == 'advanced',
                'bas': sm == 'basic',
                'stats': is_stats,
                'hp': bool(cd.get('percentile')),
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
            wc = cd.get('width_class')
            if wc is not None:
                pw = WIDTH_CLASSES.get(wc)
                if pw is not None:
                    widths[str(i + 1)] = pw
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
        section_ctx = entry[3] if len(entry) > 3 else None
        if SECTION_CONFIG.get(section_ctx, {}).get('is_stats_section', False):
            stats_start = i + 1
            break

    # --- Editable columns (config-driven for Apps Script) ----------------
    editable_columns = []
    for col_key, col_def in TAB_COLUMNS.items():
        if not col_def.get('editable', False):
            continue
        db_field = col_def.get('values', {}).get('player')
        if not db_field or not isinstance(db_field, str):
            continue
        team_idx = get_column_index(col_key, team_columns)
        league_idx = get_column_index(col_key, league_columns)
        editable_columns.append({
            'col_key': col_key,
            'team_col_index': (team_idx or 0) + 1,
            f'{league}_col_index': (league_idx or 0) + 1 if league_idx is not None else None,
            'db_field': db_field,
            'display_name': col_def.get('description', col_key),
            'format': col_def.get('format', 'text'),
            'team_row_calc': col_def.get('team_row_calc'),
        })

    # --- Editable columns for teams_sheet ----
    teams_editable = []
    for col_key, col_def in TAB_COLUMNS.items():
        if not col_def.get('editable', False):
            continue
        tf = col_def.get('values', {}).get('team')
        if tf and isinstance(tf, str) and tf != 'TEAM':
            ti = get_column_index(col_key, teams_columns)
            if ti is not None:
                teams_editable.append({
                    'col_key': col_key,
                    'col_index': ti + 1,
                    'db_field': tf,
                    'display_name': col_def.get('description', col_key),
                })

    # Reverse mapping: team name → abbreviation
    team_name_to_abbr = {name: abbr for _, (abbr, name) in teams_from_db.items()}

    return {
        'sheet_id': google_sheets_config.get('spreadsheet_id', ''),
        'league': {
            'name': league.upper(),
            'slug': league,
            'teams_key': f'{league}_teams',
            'players_sheet_names': [league.upper(), 'PLAYERS'],
            'players_range_key': league_sheet,
            'edit_col_index_key': f'{league}_col_index',
        },
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
        'default_stat_rate': DEFAULT_STAT_RATE,
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
        'stat_rates': STAT_RATES,
        'stat_rate_labels': STAT_RATE_LABELS,
        'menu': MENU_CONFIG,
        'max_historical_timeframe': STAT_CONSTANTS.get('max_historical_years', 20),
    }


# ============================================================================
# CONFIG EXPORT — generate apps_script/config/<LEAGUE>_generated.js
# ============================================================================


def _build_rate_column_ranges(config: dict) -> dict:
    """Derive per-rate column index sets from column_metadata.

    Returns {sheet_key: {rate_name: [col_indices]}} where col_indices are
    1-indexed column numbers belonging to that stat rate variant.
    Non-rate columns (without '__' in sec) are omitted — they are always visible.
    """
    result = {}
    for sheet_key, meta_list in (config.get('column_metadata') or {}).items():
        by_rate = {}
        for meta in meta_list:
            sec = meta.get('sec', '')
            if '__' not in sec:
                continue
            _, rate = sec.split('__', 1)
            by_rate.setdefault(rate, []).append(meta['col'])
        result[sheet_key] = by_rate
    return result

def _build_stats_column_indices(config: dict) -> dict:
    """Derive full list of stats columns from column_metadata."""
    result = {}
    for sheet_key, meta_list in (config.get('column_metadata') or {}).items():
        result[sheet_key] = [m['col'] for m in meta_list if m.get('stats')]
    return result

def _build_advanced_column_indices(config: dict) -> dict:
    """Derive list of advanced stat columns from column_metadata."""
    result = {}
    for sheet_key, meta_list in (config.get('column_metadata') or {}).items():
        result[sheet_key] = [m['col'] for m in meta_list if m.get('adv')]
    return result

def _build_basic_column_indices(config: dict) -> dict:
    """Derive list of basic stat columns from column_metadata."""
    result = {}
    for sheet_key, meta_list in (config.get('column_metadata') or {}).items():
        result[sheet_key] = [m['col'] for m in meta_list if m.get('bas')]
    return result

def _build_section_column_indices(config: dict) -> dict:
    """Derive list of stat columns grouped by base section, from column_metadata."""
    result = {}
    for sheet_key, meta_list in (config.get('column_metadata') or {}).items():
        by_section = {}
        for m in meta_list:
            if not m.get('stats'):
                continue
            sec = m.get('sec', '').split('__')[0]
            if sec:
                by_section.setdefault(sec, []).append(m['col'])
        result[sheet_key] = by_section
    return result

def _build_editable_lookup(config: dict) -> dict:
    """Build a lookup for linked-cell propagation.

    Returns {col_key: {db_field, display_name, format, team_row_calc,
                       indices: {sheet_key: col_index}}}
    """
    lookup = {}
    for ec in config.get('editable_columns') or []:
        key = ec['col_key']
        entry = {
            'db_field': ec['db_field'],
            'display_name': ec['display_name'],
            'format': ec.get('format', 'text'),
            'team_row_calc': ec.get('team_row_calc'),
            'indices': {},
        }
        for k, v in ec.items():
            if k.endswith('_col_index') and v is not None:
                sheet_key = k.replace('_col_index', '_sheet')
                entry['indices'][sheet_key] = v
            elif k == 'team_col_index' and v is not None:
                entry['indices']['team_sheet'] = v
        lookup[key] = entry
    return lookup


def export_config(league: str) -> Path:
    """Generate config JS file and return its path."""
    config = get_config_for_export(league)

    config['rate_column_ranges'] = _build_rate_column_ranges(config)
    config['stats_column_indices'] = _build_stats_column_indices(config)
    config['advanced_column_indices'] = _build_advanced_column_indices(config)
    config['basic_column_indices'] = _build_basic_column_indices(config)
    config['section_column_indices'] = _build_section_column_indices(config)
    config['editable_lookup'] = _build_editable_lookup(config)

    # Optional optimizations to strip payload bloat
    config.pop('column_metadata', None)
    config.pop('column_widths', None)

    config_json = json.dumps(config, indent=2, ensure_ascii=False)

    output_file = OUTPUT_DIR / f'{league.lower()}.js'
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    js_content = (
        f'// Auto-generated by src.publish.core.export_config — do not edit.\n'
        f'// Re-generate: python -m src.publish.runner --league {league} --export-config\n'
        f'var CONFIG = {config_json};\n'
    )

    output_file.write_text(js_content, encoding='utf-8')
    logger.info('Wrote %s (%d bytes)', output_file, len(js_content))
    return output_file
