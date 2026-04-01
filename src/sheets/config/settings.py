"""
The Glass - Shared Sheets Configuration

Single source of truth for display logic, calculations, formatting, percentiles,
and spreadsheet settings across all leagues.
"""

import os

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
    'default_per_minute': 36.0,         # Default minutes for per-minute stats across all leagues
    'default_per_possessions': 100.0,   # Default possessions for per-possession stats
    'cache_ttl_seconds': 300,           # API response cache TTL
}

# ============================================================================
# STAT MODE CONFIGURATION
# ============================================================================

_pm = int(STAT_CONSTANTS['default_per_minute'])
_pp = int(STAT_CONSTANTS['default_per_possessions'])

STAT_MODES = [f'per_{_pp}', 'per_game', f'per_{_pm}']
DEFAULT_STAT_MODE = f'per_{_pp}'

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
    'dark_gray': {'red': 67/255, 'green': 67/255, 'blue': 67/255},
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

    # Percentile companion column formatting
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
    },
    'player_info': {
        'display_name': 'Player Info',
        'is_stats_section': False,
    },
    'analysis': {
        'display_name': 'Analysis',
        'is_stats_section': False,
    },
    'current_stats': {
        'display_name': 'Current Stats',
        'is_stats_section': True,
    },
    'historical_stats': {
        'display_name': 'Historical Stats',
        'is_stats_section': True,
    },
    'postseason_stats': {
        'display_name': 'Postseason Stats',
        'is_stats_section': True,
    },
    'identity': {
        'display_name': 'ID',
        'is_stats_section': False,
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
    'rates': 'Rates',                 # Games, Minutes, Possessions
    'scoring': 'Scoring',             # Points, TS%, 2fg/3, Rim/Mid/3PT tracking, FT
    'ball_management': 'Ball Management', # Touches, Assists, Potential Assists, Turnovers
    'rebounding': 'Rebounding',       # OREB%, DREB%, Contested OREB/DREB%, Putbacks
    'movement': 'Movement',           # Offensive/Defensive distance traveled
    'defense': 'Defense',             # Defended shots, Steals, Deflections, Blocks, Contests, Charges, Fouls
    'opponent': 'Opponent',           # All opponent stats (Teams sheet only, between defense and on/off)
    'onoff': 'On/Off',                # Offensive/Defensive Rating, Off-court ratings
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
# COLUMN HELPERS & PROFILES
# ============================================================================

MINUTES_FIELD_MAP = {
    'none': 'minutes_x10',
    'basic': 'minutes_x10',
    'tracking': 'tr_minutes_x10',
    'hustle': 'h_minutes_x10',
    'onoff': 'off_minutes_x10',
}