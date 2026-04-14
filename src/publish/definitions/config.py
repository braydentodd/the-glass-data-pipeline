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

# ============================================================================
# COMPUTED ENTITY FIELDS
# Virtual fields derived from raw DB columns. Queries use the SQL expression
# and return the result aliased to the field name.
# ============================================================================

COMPUTED_ENTITY_FIELDS = {}

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

HEADER_ROW_COUNT = 6

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
    'header_description_spacer_count': 750,

    # Data row alternating colors (uses addBanding so colors survive sorting)
    'row_even_bg': 'white',
    'row_odd_bg': 'row_alt',

    # Borders
    'border_weight': 2,
    'subsection_border_weight': 1,
    'header_border_color': 'white',
    'data_border_color': 'black',
    'column_border_weight': 1,
    'column_border_color_header': 'white',
    'column_border_color_data': 'black',

    # Separator columns between sections/subsections
    'section_separator_width': 4,
    'subsection_separator_width': 2,
    'header_separator_bg': 'white',
    'data_separator_bg': 'black',

    # Header divider rows
    'header_divider_height': 2,
    'header_divider_bg': 'white',

    # Footer divider row
    'footer_divider_height': 4,
    'footer_divider_bg': 'black',

    # Row heights
    'row_height_section_header': 25,
    'row_height_filter': 12,
    'row_height_default': 21,

    # Alignment
    'default_h_align': 'CENTER',
    'default_v_align': 'MIDDLE',

    # Overflow handling
    'wrap_strategy': 'CLIP',

    # Default visibility
    'hide_advanced_columns': True,
    'hide_subsection_row': False,
    'hide_identity_section': True,

    # percentile companion column formatting
    'percentile_companion_width': 18,      # pixels (wider to fit rank + over/under)
    'percentile_companion_font_size': 5,   # pt

    # Layout — 4 header rows
    'section_header_row': 0,
    'section_divider_row': 1,
    'subsection_header_row': 2,
    'subsection_divider_row': 3,
    'column_header_row': 4,
    'filter_row': 5,
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
    'profile': {
        'display_name': 'Profile',
        'is_stats_section': False,
        'toggleable': True,
    },
    'evaluation': {
        'display_name': 'Evaluation',
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
        'display_name': 'Identity',
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
    'profile',
    'evaluation',
    'current_stats',
    'historical_stats',
    'postseason_stats',
    'identity',
]

# Subsections and their display names (used in Row 2 subsection headers)
SUBSECTIONS = {
    # Profile subsections
    'League': 'League',                     # Team, Conference, Jersey, Experience
    'Player': 'Player',                     # Age, Height, Weight, Wingspan, Handedness
    # Stats subsections
    'rates': 'Rates',                       # Games, Minutes, Possessions
    'scoring': 'Scoring',                   # Pts, TS%, fg2/3, Rim/Mid/3PT tracking, FT
    'ball_management': 'Ball Management',   # Touches, Assists, Potential Assists, Turnovers
    'rebounding': 'Rebounding',             # OREB%, DREB%, Contested OREB/DREB%, Putbacks
    'distance': 'Distance',                 # Offensive/Defensive distance traveled
    'defense': 'Defense',                   # Defended shots, Steals, Deflections, Blocks, Contests, Charges, Fouls
    'opponent': 'Opponent',                 # All opponent stats (Teams sheet only, between defense and on/off)
    'team_ratings': 'Team Ratings',         # Offensive/Defensive Rating, Off-court ratings
}

# ============================================================================
# COLUMN WIDTH CLASSES
# ============================================================================

WIDTH_CLASSES = {
    'auto': None,
    'measurement': 33,
    'four_char': 31,
    'four_char_dec': 31,
    'three_char_dec': 24,
    'two_char_dec': 19,
    'two_char': 19,
}

# Maps column values-dict keys to the entity type they represent
VALUES_KEY_ENTITY = {
    'player': 'player',
    'team': 'team',
    'teams': 'team',
    'all_teams': 'team',
    'opponents': 'team',
}


# ============================================================================
# SUMMARY THRESHOLDS
# Displayed at the bottom of Players/Teams sheets.
# ============================================================================

SUMMARY_THRESHOLDS = [
    ('Best', 100),
    ('75th', 75),
    ('Average', 50),
    ('25th', 25),
    ('Worst', 0),
]


# ============================================================================
# SCHEMA VALIDATORS
# ============================================================================

GOOGLE_SHEETS_CONFIG_SCHEMA = {
    'credentials_file': {'required': True, 'types': (str, type(None))},
    'spreadsheet_id': {'required': True, 'types': (str, type(None))},
    'spreadsheet_name': {'required': True, 'types': (str,)},
    'scopes': {'required': True, 'types': (list,)},
}

STAT_CONSTANTS_SCHEMA = {
    'default_per_minute': {'required': True, 'types': (float, int)},
    'default_per_possessions': {'required': True, 'types': (float, int)},
    'cache_ttl_seconds': {'required': True, 'types': (int,)},
    'max_historical_years': {'required': True, 'types': (int,)},
}

SHEET_FORMATTING_SCHEMA = {
    'header_font': {'required': True, 'types': (str,)},
    'data_font': {'required': True, 'types': (str,)},
    'section_header_size': {'required': True, 'types': (int,)},
    'team_name_size': {'required': True, 'types': (int,)},
    'subsection_header_size': {'required': True, 'types': (int,)},
    'column_header_size': {'required': True, 'types': (int,)},
    'data_size': {'required': True, 'types': (int,)},
    'header_bg': {'required': True, 'types': (str,)},
    'header_fg': {'required': True, 'types': (str,)},
    'header_description_spacer_count': {'required': True, 'types': (int,)},
    'row_even_bg': {'required': True, 'types': (str,)},
    'row_odd_bg': {'required': True, 'types': (str,)},
    'border_weight': {'required': True, 'types': (int,)},
    'subsection_border_weight': {'required': True, 'types': (int,)},
    'header_border_color': {'required': True, 'types': (str,)},
    'data_border_color': {'required': True, 'types': (str,)},
    'column_border_weight': {'required': True, 'types': (int,)},
    'column_border_color_header': {'required': True, 'types': (str,)},
    'column_border_color_data': {'required': True, 'types': (str,)},
    'section_separator_width': {'required': True, 'types': (int,)},
    'subsection_separator_width': {'required': True, 'types': (int,)},
    'header_separator_bg': {'required': True, 'types': (str,)},
    'data_separator_bg': {'required': True, 'types': (str,)},
    'header_divider_height': {'required': True, 'types': (int,)},
    'header_divider_bg': {'required': True, 'types': (str,)},
    'footer_divider_height': {'required': True, 'types': (int,)},
    'footer_divider_bg': {'required': True, 'types': (str,)},
    'row_height_section_header': {'required': True, 'types': (int,)},
    'row_height_filter': {'required': True, 'types': (int,)},
    'row_height_default': {'required': True, 'types': (int,)},
    'default_h_align': {'required': True, 'types': (str,)},
    'default_v_align': {'required': True, 'types': (str,)},
    'wrap_strategy': {'required': True, 'types': (str,)},
    'hide_advanced_columns': {'required': True, 'types': (bool,)},
    'hide_subsection_row': {'required': True, 'types': (bool,)},
    'hide_identity_section': {'required': True, 'types': (bool,)},
    'percentile_companion_width': {'required': True, 'types': (int,)},
    'percentile_companion_font_size': {'required': True, 'types': (int,)},
    'section_header_row': {'required': True, 'types': (int,)},
    'section_divider_row': {'required': True, 'types': (int,)},
    'subsection_header_row': {'required': True, 'types': (int,)},
    'subsection_divider_row': {'required': True, 'types': (int,)},
    'column_header_row': {'required': True, 'types': (int,)},
    'filter_row': {'required': True, 'types': (int,)},
    'data_start_row': {'required': True, 'types': (int,)},
    'header_row_count': {'required': True, 'types': (int,)},
    'frozen_rows': {'required': True, 'types': (int,)},
    'frozen_cols': {'required': True, 'types': (int,)},
    'row_sections': {'required': True, 'types': (list,)},
    'sync_delay_seconds': {'required': True, 'types': (int,)},
}

SECTION_CONFIG_SCHEMA = {
    'display_name': {'required': True, 'types': (str,)},
    'is_stats_section': {'required': True, 'types': (bool,)},
    'toggleable': {'required': True, 'types': (bool,)},
}

COLORS_SCHEMA = {
    'red': {'required': True, 'types': (int, float)},
    'green': {'required': True, 'types': (int, float)},
    'blue': {'required': True, 'types': (int, float)},
}

COLOR_THRESHOLDS_SCHEMA = {
    'low': {'required': True, 'types': (int, float)},
    'mid': {'required': True, 'types': (int, float)},
    'high': {'required': True, 'types': (int, float)},
}

MENU_CONFIG_SCHEMA = {
    'display_name': {'required': True, 'types': (str,)},
    'max_value': {'required': False, 'types': (int,)},
    'show_label': {'required': False, 'types': (str,)},
    'hide_label': {'required': False, 'types': (str,)},
}
