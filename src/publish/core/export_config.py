import json
import logging
import re
from pathlib import Path

from src.publish.definitions.columns import TAB_COLUMNS
from src.publish.definitions.config import (HEADER_ROWS, HISTORICAL_TIMEFRAMES, 
    SECTIONS_CONFIG,
    GOOGLE_SHEETS_CONFIG,
    STAT_RATES,
    DEFAULT_STAT_RATE,
    
)
from src.publish.core.table_builder import build_tab_columns, get_column_index

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parents[3] / 'apps_script' / 'apps_config'


def get_config_for_export(
    league: str,
    get_teams_fn=None,
    id_column_key: str = 'player_id',
    google_sheets_config: dict = None,
) -> dict:
    """
    Build JSON-serializable config dict for Apps Script.

    Apps Script uses this as single source of truth — zero hardcoding in JS.
    League-agnostic: derives league-specific values from the league parameter
    when optional args are not provided.

        Exports:
            - column_metadata:          normalized per-column visibility metadata
            - column_indices:           edit-detection indices (player_id, team, stats_start)
            - editable_lookup:          tab-specific editable column lookup
            - sheet_names:              sheet-name aliases for Apps Script routing
            - stat_rates:               available stat rates with display labels
            - sections:                 section config (display names, toggleability)
            - menu:                     menu structure config for Apps Script UI
    """
    if get_teams_fn is None:
        from src.publish.core.queries import get_teams_from_db
        def get_teams_fn():
            return get_teams_from_db(league)

    if google_sheets_config is None:
        google_sheets_config = GOOGLE_SHEETS_CONFIG.get(league, {})

    # --- Teams dict -------------------------------------------------------
    teams_from_db = get_teams_fn()
    team_name_to_abbr = {name: abbr for _, (abbr, name) in teams_from_db.items()}

    supported_years = list(HISTORICAL_TIMEFRAMES.keys())

    # --- Stat columns list -----------------------------------------------
    stat_columns = [k for k, v in TAB_COLUMNS.items()
                    if any(SECTIONS_CONFIG.get(s, {}).get('stats_timeframe') for s in v.get('sections', []))]

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

    def _column_metadata(cols):
        blocks = []
        current_block = None

        for i, (col_key, col_def, visible, context_key) in enumerate(cols):
            context = context_key or ''
            section_part, rate = (context.split('__', 1) + [None])[:2] if '__' in context else (context, None)
            timeframe = None
            base_section = section_part
            match = re.match(r'^(historical_stats|postseason_stats)_(\d+)yr$', section_part)
            if match:
                base_section = match.group(1)
                timeframe = int(match.group(2))
            if base_section.startswith('current_stats'):
                base_section = 'current_stats'

            sm = col_def.get('stats_mode', 'both')
            is_stats = SECTIONS_CONFIG.get(base_section, {}).get('stats_timeframe')
            
            # Compress contiguous columns that share the exact same visibility rules
            # into a single block to drastically shrink the config size.
            props = {
                'base_section': base_section,
                'rate': rate,
                'timeframe': timeframe,
                'advanced': sm == 'advanced',
                'basic': sm == 'basic',
                'is_stats_section': bool(is_stats),
                'is_separator': bool(col_def.get('is_separator')),
            }

            if not current_block or current_block['props'] != props:
                if current_block:
                    blocks.append(current_block)
                current_block = {
                    'start': i + 1,
                    'count': 1,
                    'props': props
                }
            else:
                current_block['count'] += 1

        if current_block:
            blocks.append(current_block)

        # Flatten blocks into dense arrays: [start, count, base_section, rate, timeframe, advanced, basic, is_stats_section, is_separator]
        flattened = []
        for b in blocks:
            p = b['props']
            # Using 1/0 for booleans to save space
            flat = [
                b['start'],
                b['count'],
                p['base_section'] or "",
                p['rate'] or "",
                p['timeframe'] if p['timeframe'] is not None else "",
                1 if p['advanced'] else 0,
                1 if p['basic'] else 0,
                1 if p['is_stats_section'] else 0,
                1 if p['is_separator'] else 0
            ]
            flattened.append(flat)

        return flattened

    column_metadata = {
        'team_tab':  _column_metadata(team_columns),
        'all_players_tab':  _column_metadata(league_columns),
        'all_teams_tab': _column_metadata(teams_columns),
    }

    # --- Column indices for edit detection (1-indexed) ---
    id_idx = get_column_index(id_column_key, team_columns)
    team_col_idx = get_column_index('team', league_columns)
    stats_start = None
    for i, entry in enumerate(team_columns):
        section_ctx = entry[3] if len(entry) > 3 else None
        if SECTIONS_CONFIG.get(section_ctx, {}).get('stats_timeframe'):
            stats_start = i + 1
            break

    # --- Editable Lookup (combining player and team editable configs) ---
    def _build_editable_lookup(columns_by_tab):
        lookup = {}

        for col_key, col_def in TAB_COLUMNS.items():
            editable_config = col_def.get('editable', False)
            if not editable_config:
                continue

            allowed_tabs = set(editable_config) if isinstance(editable_config, list) else {'player', 'all_teams'}
            values = col_def.get('values', {})
            entry = {
                'format': col_def.get('format', 'text'),
                'indices': {}
            }

            if 'player' in allowed_tabs and isinstance(values.get('player'), str):
                team_idx = get_column_index(col_key, columns_by_tab['team_tab'])
                players_idx = get_column_index(col_key, columns_by_tab['all_players_tab'])
                if team_idx is not None:
                    entry['indices']['team_tab'] = team_idx + 1
                if players_idx is not None:
                    entry['indices']['all_players_tab'] = players_idx + 1

            if 'all_teams' in allowed_tabs and isinstance(values.get('team'), str):
                teams_idx = get_column_index(col_key, columns_by_tab['all_teams_tab'])
                if teams_idx is not None:
                    entry['indices']['all_teams_tab'] = teams_idx + 1

            if entry['indices']:
                lookup[col_key] = entry

        return lookup

    editable_lookup = _build_editable_lookup({
        'team_tab': team_columns,
        'all_players_tab': league_columns,
        'all_teams_tab': teams_columns,
    })

    sections_export = {}
    for k, v in SECTIONS_CONFIG.items():
        sections_export[k] = {
            'display_name': v.get('menu_label') or k.replace('_', ' ').title(),
            'toggleable': v.get('toggleable', False),
            'stats_timeframe': v.get('stats_timeframe')
        }

    return {
        'sheet_id': google_sheets_config.get('spreadsheet_id', ''),
        'sheet_names': {
            'players': ['ALL_PLAYERS', 'PLAYERS'],
            'teams': ['ALL_TEAMS', 'TEAMS'],
        },
        'league': {
            'name': league.upper(),
            'slug': league,
        },
        'team_name_to_abbr': team_name_to_abbr,
        'stat_columns': stat_columns,
        'editable_lookup': editable_lookup,
        'column_indices': {
            'player_id': (id_idx or 0) + 1,
            'team': (team_col_idx or 0) + 1,
            'stats_start': stats_start or 9,
        },
        'column_metadata': column_metadata,
        'default_stat_rate': DEFAULT_STAT_RATE,
        'default_historical_timeframe': 3,
        'header_row_count': len(HEADER_ROWS),
        'sections': sections_export,
        'stat_rates': STAT_RATES,
        'supported_historical_timeframes': supported_years,
    }


# ============================================================================
# CONFIG EXPORT — generate apps_script/_config/<LEAGUE>.js
# ============================================================================




def export_config(league: str) -> Path:
    """Generate config JS file and return its path."""
    config = get_config_for_export(league)

    # To eliminate file bloat cleanly without breaking JSON validity,
    # extract the massive column_metadata and serialize it densely.
    meta_data_payload = config.pop('column_metadata')
    
    config_json = json.dumps(config, indent=2, ensure_ascii=False)
    compact_meta = json.dumps(meta_data_payload, separators=(',', ':'))
    
    # Insert it right before the final closing brace of the main config object
    insert_pos = config_json.rfind('}')
    config_json = (
        config_json[:insert_pos] 
        + ',\n  "column_metadata": ' + compact_meta + '\n'
        + config_json[insert_pos:]
    )

    output_file = OUTPUT_DIR / f'{league.lower()}.js'
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    js_content = (
        f"// Auto-generated by src.publish.core.export_config — do not edit.\n"
        f"// Re-generate: python -m src.publish.runner --league {league} --export-config\n"
        f"var CONFIG = {config_json};\n"
    )

    output_file.write_text(js_content, encoding='utf-8')
    logger.info('Wrote %s (%d bytes)', output_file, len(js_content))
    return output_file
