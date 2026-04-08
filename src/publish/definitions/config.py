"""
The Glass - Shared Sheets Configuration

Single source of truth for display logic, calculations, formatting, percentiles,
and spreadsheet settings across all leagues.
"""

import os
from typing import Dict

from src.publish.definitions.columns import TAB_COLUMNS

# ============================================================================
# GOOGLE SHEETS CONFIGURATION
# ============================================================================

GOOGLE_SHEETS_CONFIG = {
    'nba': {
        'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'spreadsheet_id': os.getenv('NBA_SPREADSHEET_ID'),
        'spreadsheet_name': os.getenv('NBA_SPREADSHEET_NAME', 'The Glass'),
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ],
    },
    'ncaa': {
        'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'spreadsheet_id': os.getenv('NCAA_SPREADSHEET_ID'),
        'spreadsheet_name': os.getenv('NCAA_SPREADSHEET_NAME', 'The Glass NCAA'),
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ],
    }
}

# ============================================================================
# STAT CALCULATION CONSTANTS
# ============================================================================

STAT_CONSTANTS = {
    'default_per_minute': 40.0,         # Minutes base for per-minute stats
    'default_per_possessions': 100.0,   # Possessions base for per-possession stats
    'cache_ttl_seconds': 300,           # API response cache TTL
    'max_historical_years': 20,         # Max seasons shown in UI timeframe toggle
}

# ============================================================================
# STAT RATE CONFIGURATION
# Stats rate = per_game / per_possession / per_minute (the rate scaling)
# Stats mode = advanced / basic (column visibility level)
# ============================================================================

STAT_RATES = ['per_possession', 'per_game', 'per_minute']
DEFAULT_STAT_RATE = 'per_possession'

STAT_RATE_LABELS = {
    'per_possession': f"per {int(STAT_CONSTANTS['default_per_possessions'])} Poss",
    'per_game': 'per Game',
    'per_minute': f"per {int(STAT_CONSTANTS['default_per_minute'])} Mins",
}

# ============================================================================
# COLORS & PERCENTILES
# ============================================================================

COLORS = {
    'red': {'red': 0.933, 'green': 0.294, 'blue': 0.169},
    'yellow': {'red': 0.988, 'green': 0.961, 'blue': 0.373},
    'green': {'red': 0.298, 'green': 0.733, 'blue': 0.090},
    'black': {'red': 0, 'green': 0, 'blue': 0},
    'white': {'red': 1, 'green': 1, 'blue': 1},
    'light_gray': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
    'dark_gray': {'red': 0.263, 'green': 0.263, 'blue': 0.263},
    'row_alt': {'red': 0.94, 'green': 0.94, 'blue': 0.94},
}

COLOR_THRESHOLDS = {
    'low': 0,    # 0% = pure red
    'mid': 50,   # 50% = pure yellow
    'high': 100, # 100% = pure green
}

# ============================================================================
# SHEET FORMATTING CONFIG
# ============================================================================

HEADER_ROW_COUNT = 4

SHEET_FORMATTING = {
    # Fonts
    'header_font': 'Staatliches',
    'data_font': 'Sofia Sans',

    # Font sizes
    'section_header_size': 12,
    'team_name_size': 15,
    'subsection_header_size': 11,
    'column_header_size': 10,
    'data_size': 10,

    # Header styling
    'header_bg': 'black',
    'header_fg': 'white',
    'header_description_mode': 'whiteout',
    'header_description_spacer_count': 750,

    # Data row alternating colors (uses addBanding so colors survive sorting)
    'row_even_bg': 'white',
    'row_odd_bg': 'row_alt',

    # Borders
    'border_weight': 2,
    'subsection_border_weight': 1,
    'header_border_color': 'white',
    'data_border_color': 'black',

    # Alignment
    'default_h_align': 'CENTER',
    'default_v_align': 'MIDDLE',
    'left_align_columns': ['names', 'notes'],
    'bold_columns': ['names'],

    # Overflow handling
    'wrap_strategy': 'CLIP',

    # Default visibility
    'hide_advanced_columns': True,
    'hide_subsection_row': True,
    'hide_identity_section': True,

    # percentile companion column formatting
    'percentile_companion_width': 10,      # pixels
    'percentile_companion_font_size': 5,   # pt

    # Layout — 4 header rows
    'section_header_row': 0,
    'subsection_header_row': 1,
    'column_header_row': 2,
    'filter_row': 3,
    'data_start_row': HEADER_ROW_COUNT,
    'header_row_count': HEADER_ROW_COUNT,

    # Freeze
    'frozen_rows': HEADER_ROW_COUNT,
    'frozen_cols': 1,

    # Row sections
    'row_sections': ['current_players', 'team_opponent'],

    # Rate limiting
    'sync_delay_seconds': 3,
}

# ============================================================================
# SECTION AND SUBSECTION DEFINITIONS
# ============================================================================

SECTION_CONFIG = {
    'entities': {
        'display_name': 'Names',
        'is_stats_section': False,
        'toggleable': False,
    },
    'player_info': {
        'display_name': 'Player Info',
        'is_stats_section': False,
        'toggleable': True,
    },
    'analysis': {
        'display_name': 'Analysis',
        'is_stats_section': False,
        'toggleable': True,
    },
    'current_stats': {
        'display_name': 'Current Stats',
        'is_stats_section': True,
        'toggleable': True,
    },
    'historical_stats': {
        'display_name': 'Historical Stats',
        'is_stats_section': True,
        'toggleable': True,
    },
    'postseason_stats': {
        'display_name': 'Postseason Stats',
        'is_stats_section': True,
        'toggleable': True,
    },
    'identity': {
        'display_name': 'ID',
        'is_stats_section': False,
        'toggleable': False,
    },
}

# ============================================================================
# MENU CONFIGURATION
# Drives the Apps Script "Display Settings" menu structure.
# ============================================================================

MENU_CONFIG = {
    'historical_timeframe': {
        'display_name': 'Historical Timeframe',
        'max_value': STAT_CONSTANTS['max_historical_years'],
    },
    'stats_rate': {
        'display_name': 'Stats Rate',
    },
    'stats_mode': {
        'display_name': 'Stats Mode',
        'show_label': 'Show Advanced',
        'hide_label': 'Show Basic',
    },
}

# Section order — left-to-right column layout
SECTIONS = [
    'entities',
    'player_info',
    'analysis',
    'current_stats',
    'historical_stats',
    'postseason_stats',
    'identity',
]

# Stat subsections and their display names (used in Row 2 subsection headers)
SUBSECTIONS = {
    'rates': 'Rates',                       # Games, Minutes, Possessions
    'scoring': 'Scoring',                   # Pts, TS%, fg2/3, Rim/Mid/3PT tracking, FT
    'ball_management': 'Ball Management',   # Touches, Assists, Potential Assists, Turnovers
    'rebounding': 'Rebounding',             # OREB%, DREB%, Contested OREB/DREB%, Putbacks
    'movement': 'Movement',                 # Offensive/Defensive distance traveled
    'defense': 'Defense',                   # Defended shots, Steals, Deflections, Blocks, Contests, Charges, Fouls
    'opponent': 'Opponent',                 # All opponent stats (Teams sheet only, between defense and on/off)
    'team_ratings': 'Team Ratings',         # Offensive/Defensive Rating, Off-court ratings
}

# ============================================================================
# COLUMN WIDTH CLASSES
# ============================================================================

WIDTH_CLASSES = {
    'auto': None,
    'measurement': 38,
    'four_char_dec': 32,
    'three_char_dec': 26,
    'two_char': 18,
}


# ============================================================================
# FIELD DERIVATION — extract DB column references from TAB_COLUMNS
# ============================================================================

# Entity type mapping: TAB_COLUMNS values key -> (entity, db_entity_type)
_VALUES_KEY_ENTITY = {
    'player': 'player',
    'team': 'team',
    'teams': 'team',
    'opponents': 'team',
}

_STATS_SECTIONS = frozenset(
    name for name, cfg in SECTION_CONFIG.items() if cfg['is_stats_section']
)


def _extract_db_refs(expr) -> set:
    """Walk an expression tree and return all referenced DB column names."""
    if expr is None:
        return set()
    if isinstance(expr, (int, float)):
        return set()
    if isinstance(expr, str):
        if expr.startswith('{') and expr.endswith('}'):
            return {expr[1:-1]}
        if expr and expr[0].isupper():
            return set()
        return {expr}
    if isinstance(expr, tuple):
        refs = set()
        for item in expr[1:]:
            refs |= _extract_db_refs(item)
        return refs
    return set()


def derive_db_fields() -> Dict[str, set]:
    """Derive the DB column sets needed by publish queries from TAB_COLUMNS.

    Returns a dict with keys:
        player_entity_fields, team_entity_fields, stat_fields, team_stat_fields
    """
    player_entity = set()
    team_entity = set()
    player_stats = set()
    team_stats = set()

    for col_def in TAB_COLUMNS.values():
        sections = set(col_def.get('sections', []))
        is_stats = bool(sections & _STATS_SECTIONS)
        values = col_def.get('values', {})

        for values_key, expr in values.items():
            entity_type = _VALUES_KEY_ENTITY.get(values_key)
            if entity_type is None:
                continue

            refs = _extract_db_refs(expr)
            if not refs:
                continue

            if is_stats:
                if entity_type == 'player':
                    player_stats |= refs
                else:
                    team_stats |= refs
            else:
                if entity_type == 'player':
                    player_entity |= refs
                else:
                    team_entity |= refs

    return {
        'player_entity_fields': player_entity,
        'team_entity_fields': team_entity,
        'stat_fields': player_stats,
        'team_stat_fields': team_stats,
    }
