"""
The Glass - Shared Sheets Configuration

Single source of truth for display logic, calculations, formatting, percentiles,
and spreadsheet settings across all leagues.
"""

import os

from src.publish.definitions.formulas import tab_subject, formatted_stats_section_name

from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class ColumnContext:
    """Strongly typed grouping for mode-specific column mappings."""
    base_section: str
    rate: Optional[str] = None
    timeframe: Optional[int] = None
    

# ============================================================================
# GOOGLE SHEETS CONFIGURATION
# ============================================================================

GOOGLE_SHEETS_CONFIG = {
    'nba': {
        'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'spreadsheet_id': os.getenv('NBA_SPREADSHEET_ID'),
        'scopes': [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ],
    },
    'ncaa': {
        'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
        'spreadsheet_id': os.getenv('NCAA_SPREADSHEET_ID'),
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
        'short_label': 'Poss',
        'rate': 100,
        'default': True
        },
    'per_game': {
        'short_label': 'Game',
        'rate': None,
        'default': False
    },
    'per_minute': {
        'short_label': 'Min',
        'rate': 40,
        'default': False
    }
}

HISTORICAL_TIMEFRAMES = {
    1: '(Previous Season)',
    3: '(Previous 3 Seasons)',
    5: '(Previous 5 Seasons)',
    7: '(Previous 7 Seasons)',
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
        'row_height': 25,
        'font_size': 12,
        'description_spacer_count': None,
        'divider_row_weight': 4,
        'divider_row_direction': 'below',
        'divider_column_weight': 4,
        'divider_column_direction': 'right',
        'column_a_font_size': 15,
        'column_a_divider_column_weight': None,
    },
    'subsections': {
        'row_height': 21,
        'font_size': 11,
        'description_spacer_count': None,
        'divider_row_weight': 2,
        'divider_row_direction': 'below',
        'divider_column_weight': 2,
        'divider_column_direction': 'right',
        'column_a_font_size': 11,
        'column_a_divider_column_weight': None,
    },
    'columns': {
        'row_height': 21,
        'font_size': 10,
        'description_spacer_count': 750,
        'divider_row_weight': None,
        'divider_row_direction': None,
        'divider_column_weight': 1,
        'divider_column_direction': 'right',
        'column_a_font_size': 10,
        'column_a_divider_column_weight': None,
    },
    'filters': {
        'row_height': 12,
        'font_size': 10,
        'description_spacer_count': None,
        'divider_row_weight': None,
        'divider_row_direction': None,
        'divider_column_weight': 1,
        'divider_column_direction': 'right',
        'column_a_font_size': 10,
        'column_a_divider_column_weight': None,
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

    # Default settings
    'horizontal_align': 'CENTER',
    'vertical_align': 'MIDDLE',
    'wrap_strategy': 'CLIP',
    'hide_advanced_columns': True,
    
    'frozen_columns': 1,
    'frozen_rows': 6,
    'header_rows': 6,

    # percentile companion column formatting
    'percentile_companion_width': 18,      # pixels (wider to fit rank + over/under)
    'percentile_companion_font_size': 5,   # pt

    # Rate limiting
    'sync_delay_seconds': 0
}

# ============================================================================
# SECTION AND SUBSECTION DEFINITIONS
# ============================================================================

TABS_CONFIG = {
    'all_players': {
        'tab_name': 'Players',
        'move_to_front': True,
        'footer': 'percentiles',
        'footer_divider_row_height': 4
    },
    'all_teams': {
        'tab_name': 'Teams',
        'move_to_front': True,
        'footer': 'percentiles',
        'footer_divider_row_height': 4,
    },
    'team': {
        'tab_name': tab_subject('abbr'),
        'move_to_front': False,
        'footer': 'team/opponent',
        'footer_divider_row_height': 4
    }
}

SECTIONS_CONFIG = {
    'entities': {
        'display_name': tab_subject('name'),
        'menu_label': None,
        'stats_timeframe': None,
        'toggleable': False,
        'visible_by_default': True
    },
    'profile': {
        'display_name': 'Profile',
        'menu_label': 'Profile',
        'stats_timeframe': None,
        'toggleable': True,
        'visible_by_default': True
    },
    'evaluation': {
        'display_name': 'Evaluation',
        'menu_label': 'Evaluation',
        'stats_timeframe': None,
        'toggleable': True,
        'visible_by_default': True
    },
    'current_stats': {
        'display_name': formatted_stats_section_name(),
        'menu_label': 'Current Stats',
        'stats_timeframe': 'current',
        'toggleable': True,
        'visible_by_default': True
    },
    'historical_stats': {
        'display_name': formatted_stats_section_name(),
        'menu_label': 'Historical Stats',
        'stats_timeframe': 'historical',
        'toggleable': True,
        'visible_by_default': True
    },
    'postseason_stats': {
        'display_name': formatted_stats_section_name(),
        'menu_label': 'Postseason Stats',
        'stats_timeframe': 'historical',
        'toggleable': True,
        'visible_by_default': True
    },
    'identity': {
        'display_name': 'Identity',
        'menu_label': None,
        'stats_timeframe': None,
        'toggleable': False,
        'visible_by_default': False
    }
}

# Subsections and their display names (used in Row 2 subsection headers)
SUBSECTIONS = {
    'league': {
        'display_name': 'League',
        'sections': ['profile'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'player': {
        'display_name': 'Player',
        'sections': ['profile'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'rates': {
        'display_name': 'Rates',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'scoring': {
        'display_name': 'Scoring',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'ball_management': {
        'display_name': 'Ball Management',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'rebounding': {
        'display_name': 'Rebounding',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'distance': {
        'display_name': 'Distance',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'defense': {
        'display_name': 'Defense',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    },
    'opponent': {
        'display_name': 'Opponent',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_teams']
    },
    'team_ratings': {
        'display_name': 'Team Ratings',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'tabs': ['all_players', 'all_teams', 'team']
    }
}

# ============================================================================
# COLUMN WIDTH CLASSES
# ============================================================================

WIDTH_CLASSES = {
    'auto': None,
    'measurement': 34,
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
    'scopes': {'required': True, 'types': (list,)}
}

STAT_CONSTANTS_SCHEMA = {
    'default_per_minute': {'required': True, 'types': (float, int)},
    'default_per_possessions': {'required': True, 'types': (float, int)}
}

SHEET_FORMATTING_SCHEMA = {
    'header_font': {'required': True, 'types': (str,)},
    'data_font': {'required': True, 'types': (str,)},
    'header_bg': {'required': True, 'types': (str,)},
    'header_fg': {'required': True, 'types': (str,)},
    'wrap_strategy': {'required': True, 'types': (str,)},
    'hide_advanced_columns': {'required': True, 'types': (bool,)},
    'hide_identity_section': {'required': True, 'types': (bool,)},
    'percentile_companion_width': {'required': True, 'types': (int,)},
    'percentile_companion_font_size': {'required': True, 'types': (int,)},
    'frozen_rows': {'required': True, 'types': (int,)},
    'frozen_columns': {'required': True, 'types': (int,)},
    'sync_delay_seconds': {'required': True, 'types': (int,)}
}

SECTIONS_SCHEMA = {
    'display_name': {'required': True, 'types': (str,)},
    'is_stats_section': {'required': True, 'types': (bool,)},
    'toggleable': {'required': True, 'types': (bool,)}
}

COLORS_SCHEMA = {
    'red': {'required': True, 'types': (int, float)},
    'green': {'required': True, 'types': (int, float)},
    'blue': {'required': True, 'types': (int, float)}
}

COLOR_THRESHOLDS_SCHEMA = {
    'low': {'required': True, 'types': (int, float)},
    'mid': {'required': True, 'types': (int, float)},
    'high': {'required': True, 'types': (int, float)}
}

MENU_CONFIG_SCHEMA = {
    'display_name': {'required': True, 'types': (str,)},
    'show_label': {'required': False, 'types': (str,)},
    'hide_label': {'required': False, 'types': (str,)}
}
DEFAULT_STAT_RATE = next((k for k, v in STAT_RATES.items() if v.get('default', False)), 'per_game')
