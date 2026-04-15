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
        'spreadsheet_name': 'The Glass - NBA',
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ],
    },
    'ncaa': {
        'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'spreadsheet_id': os.getenv('NCAA_SPREADSHEET_ID'),
        'spreadsheet_name': 'The Glass - NCAA',
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ],
    }
}

# ============================================================================
# STAT CALCULATION CONSTANTS
# ============================================================================

STAT_RATES = {
    'per_possession': {
        'label': 'per Poss',
        'rate': 100,
        'default': True
        },
    'per_game': {
        'label': 'per Game',
        'rate': 1,
        'default': False
    },
    'per_minute': {
        'label': 'per Min',
        'rate': 40,
        'default': False
    }
}

HISTORICAL_TIMEFRAMES = {
    1: 'Previous Season',
    3: 'Previous 3 Seasons',
    5: 'Previous 5 Seasons',
    7: 'Previous 7 Seasons',
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
    'light_gray': {'red': 0.94, 'green': 0.94, 'blue': 0.94},
}

COLOR_THRESHOLDS = {
    'low': 0,    # 0% = pure red
    'mid': 50,   # 50% = pure yellow
    'high': 100, # 100% = pure green
}

# ============================================================================
# SHEET FORMATTING CONFIG
# ============================================================================

HEADER_ROWS = {
    'sections': {
        'index': 1,
        'row_height': 25,
        'font_size': 12,
        'description_spacer_count': None,
        'divider_row_weight': 4,
        'divider_column_weight': 4,
        'column_a_font_size': 15,
    },
    'subsections': {
        'index': 3,
        'row_height': 21,
        'font_size': 11,
        'description_spacer_count': None,
        'divider_row_weight': 2,
        'divider_column_weight': 2,
        'column_a_font_size': 11,
    },
    'columns': {
        'index': 5,
        'row_height': 21,
        'font_size': 10,
        'description_spacer_count': 750,
        'divider_row_weight': None,
        'divider_column_weight': 1,
        'column_a_font_size': 10,
    },
    'filters': {
        'index': 6,
        'row_height': 12,
        'font_size': 10,
        'description_spacer_count': None,
        'divider_row_weight': 0,
        'divider_column_weight': 0,
        'column_a_font_size': 10,
    },
}

SHEET_FORMATTING = {
    # Fonts
    'header_font': 'Staatliches',
    'data_font': 'Sofia Sans',

    'header_bg': 'black',
    'header_fg': 'white',
    'data_row_even_bg': 'white',
    'data_row_odd_bg': 'light_gray',
    'data_fg': 'black',
    
    'footer_divider_height': 4,

    # Alignment
    'default_h_align': 'CENTER',
    'default_v_align': 'MIDDLE',

    # Overflow handling
    'wrap_strategy': 'CLIP',

    # Default visibility
    'hide_advanced_columns': True,

    # percentile companion column formatting
    'percentile_companion_width': 18,      # pixels (wider to fit rank + over/under)
    'percentile_companion_font_size': 5,   # pt


    # Rate limiting
    'sync_delay_seconds': 0
}

# ============================================================================
# SECTION AND SUBSECTION DEFINITIONS
# ============================================================================

SECTION_CONFIG = {
    'entities': {
        'display_name': tab_subject('name'),
        'is_stats_section': False,
        'toggleable': False
    },
    'profile': {
        'display_name': 'Profile',
        'is_stats_section': False,
        'toggleable': True
    },
    'evaluation': {
        'display_name': 'Evaluation',
        'is_stats_section': False,
        'toggleable': True
    },
    'current_stats': {
        'display_name': formatted_stats_section_name(),
        'is_stats_section': True,
        'toggleable': True
    },
    'historical_stats': {
        'display_name': formatted_stats_section_name(),
        'is_stats_section': True,
        'toggleable': True
    },
    'postseason_stats': {
        'display_name': formatted_stats_section_name(),
        'is_stats_section': True,
        'toggleable': True
    },
    'identity': {
        'display_name': 'Identity',
        'is_stats_section': False,
        'toggleable': False
    }
}

TABS_CONFIG = {
    'all_players': {
        'tab_name': 'Players',
        'footer': 'percentiles'
    },
    'all_teams': {
        'tab_name': 'Teams',
        'footer': 'percentiles'
    },
    'team': {
        'tab_name': tab_subject('abbr'),
        'footer': 'team/opponent'
    }
}

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
    'team_ratings': 'Team Ratings'         # Offensive/Defensive Rating, Off-court ratings
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
    'two_char': 19
}

# Maps column values-dict keys to the entity type they represent
VALUES_KEY_ENTITY = {
    'player': 'player',
    'team': 'team',
    'all_teams': 'team',
    'opponents': 'team'
}


# ============================================================================
# SUMMARY THRESHOLDS
# Displayed at the bottom of Players/Teams sheets.
# ============================================================================

SUMMARY_THRESHOLDS = [
    ('Best', 100),
    ('75th Percentile', 75),
    ('Average', 50),
    ('25th Percentile', 25),
    ('Worst', 0)
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
    'row_even_bg': {'required': True, 'types': (str,)},
    'row_odd_bg': {'required': True, 'types': (str,)},
    'border_weight': {'required': True, 'types': (int,)},
    'subsection_border_weight': {'required': True, 'types': (int,)},
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
    'show_label': {'required': False, 'types': (str,)},
    'hide_label': {'required': False, 'types': (str,)},
}