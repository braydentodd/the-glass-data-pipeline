"""
The Glass - Shared Sheets Configuration

Single source of truth for display logic, calculations, formatting, percentiles,
and spreadsheet settings across all leagues.
"""

import os
from typing import Any, Dict

from src.publish.core.formulas import (
    add, subtract, multiply, divide, lookup, team_average, seasons_in_query
)

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
# COLUMN DEFINITIONS
# ============================================================================

SHEETS_COLUMNS: Dict[str, Any] = {
    'name': {
        'description': 'Name',
        'sections': ['entities'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': False,
        'scale_with_rate': False,
        'format': 'text',
        'decimal_places': None,
        'width_class': 'auto',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{name}',
            'team': 'TEAM',
            'teams': '{name}',
            'opponents': 'OPPONENTS'
        }
    },
    'team': {
        'description': 'Team Abbreviation',
        'sections': ['entities'],
        'subsection': None,
        'tabs': ['players'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': False,
        'scale_with_rate': False,
        'format': 'text',
        'decimal_places': None,
        'width_class': 'four_char',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': lookup('team_id', 'teams', 'abbr')
        }
    },
    'conf': {
        'description': 'Conference',
        'sections': ['entities'],
        'subsection': None,
        'tabs': ['teams'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': False,
        'scale_with_rate': False,
        'format': 'text',
        'decimal_places': None,
        'width_class': 'auto',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'teams': '{conf}'
        }
    },
    '#': {
        'description': 'Jersey Number',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['players', 'team'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': False,
        'scale_with_rate': False,
        'format': 'text',
        'decimal_places': None,
        'width_class': 'two_char',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{jersey_num}'
        }
    },
    'exp': {
        'description': 'Seasons with Playing Experience',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'two_char_dec',
        'leagues': ['nba', 'ncaa'],
        'team_row_display': 'average',
        'default': '0',
        'values': {
            'player': '{seasons_exp}',
            'team': team_average('seasons_exp'),
            'teams': team_average('seasons_exp')
        }
    },
    'age': {
        'description': 'Age',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'two_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{age}',
            'team': team_average('age'),
            'teams': team_average('age')
        },
    },
    'ht': {
        'description': 'Height in Feet\'Inches"',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'measurement',
        'decimal_places': 1,
        'width_class': 'measurement',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{height_ins}',
            'team': team_average('height_ins'),
            'teams': team_average('height_ins')
        }
    },
    'wt': {
        'description': 'Weight in Pounds',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{weight_lbs}',
            'team': team_average('weight_lbs'),
            'teams': team_average('weight_lbs')
        }
    },
    'ws': {
        'description': 'Wingspan in Feet\'Inches" (Editable)',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': True,
        'scale_with_rate': False,
        'format': 'measurement',
        'decimal_places': 1,
        'width_class': 'measurement',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{wingspan_ins}',
            'team': team_average('wingspan_ins'),
            'teams': team_average('wingspan_ins')
        }
    },
    '🖐️': {
        'description': 'Handedness (Editable)',
        'sections': ['player_info'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': True,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 0,
        'width_class': 'two_char',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{hand}'
        }
    },
    'notes': {
        'description': 'Analysis/Thoughts (Editable)',
        'sections': ['analysis'],
        'subsection': None,
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': True,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 0,
        'width_class': '500',
        'leagues': ['nba', 'ncaa'],
        'team_row_display': 'editable',
        'default': None,
        'values': {
            'player': '{notes}',
            'team': '{notes}',
            'teams': '{notes}'
        }
    },
    'szn': {
        'description': 'Seasons Played',
        'sections': ['historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 0,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': 0,
        'values': {
            'player': seasons_in_query,
            'team': seasons_in_query,
            'teams': seasons_in_query
        }
    },
    'gms': {
        'description': 'Games Played per Season',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': 0,
        'values': {
            'player': divide('games', seasons_in_query),
            'team': divide('games', seasons_in_query),
            'teams': divide('games', seasons_in_query)
        },
    },
    'min': {
        'description': 'Minutes Played per Game',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'team_row_display': 'team_value',
        'default': 0,
        'values': {
            'player': divide(divide('minutes_x10', 10), 'games'),
            'team': divide(divide('minutes_x10', 10), 'games'),
            'teams': divide(divide('minutes_x10', 10), 'games')
        }
    },
    'pace': {
        'description': 'Possessions per 48 Mins',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide(multiply('possessions', 48), divide('minutes_x10', 10)),
            'team': divide(multiply('possessions', 48), divide('minutes_x10', 10)),
            'teams': divide(multiply('possessions', 48), divide('minutes_x10', 10))
        }
    },
    'pts': {
        'description': 'Points',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': add(multiply('fg2m', 2), multiply('fg3m', 3), 'ftm'),
            'team': add(multiply('fg2m', 2), multiply('fg3m', 3), 'ftm'),
            'teams': add(multiply('fg2m', 2), multiply('fg3m', 3), 'ftm'),
            'opponents': add(multiply('opp_fg2m', 2), multiply('opp_fg3m', 3), 'opp_ftm')
        }
    },
    'p/ta': {
        'description': 'Points per "True" FGA (FGA + FTA per FT Trip Estimation) or TS% * 2',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide(add(multiply('fg2m', 2), multiply('fg3m', 3), 'ftm'), add('fg2a', 'fg3a', multiply(0.44, 'fta'))),
            'team': divide(add(multiply('fg2m', 2), multiply('fg3m', 3), 'ftm'), add('fg2a', 'fg3a', multiply(0.44, 'fta'))),
            'teams': divide(add(multiply('fg2m', 2), multiply('fg3m', 3), 'ftm'), add('fg2a', 'fg3a', multiply(0.44, 'fta'))),
            'opponents': divide(add(multiply('opp_fg2m', 2), multiply('opp_fg3m', 3), 'opp_ftm'), add('opp_fg2a', 'opp_fg3a', multiply(0.44, 'opp_fta')))
        }
    },
    '2a': {
        'description': 'Two-Point FGA',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{fg2a}',
            'team': '{fg2a}',
            'teams': '{fg2a}',
            'opponents': '{opp_fg2a}'
        }
    },
    'p/2': {
        'description': 'Points per 2A',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': multiply(2, divide('fg2m', 'fg2a')),
            'team': multiply(2, divide('fg2m', 'fg2a')),
            'teams': multiply(2, divide('fg2m', 'fg2a')),
            'opponents': multiply(2, divide('opp_fg2m', 'opp_fg2a'))
        }
    },
    'ora': {
        'description': 'Open 2A at the Rim (Closest Defender Beyond 4 Feet; Shot Taken Within 5 feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{open_rim_fga}',
            'team': '{open_rim_fga}',
            'teams': '{open_rim_fga}'
        }
    },
    'p/or': {
        'description': 'Points per Open 2A at the Rim (Closest Defender Beyond 4 Feet; Shot Taken Within 5 feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(2, divide('open_rim_fgm', 'open_rim_fga')),
            'team': multiply(2, divide('open_rim_fgm', 'open_rim_fga')),
            'teams': multiply(2, divide('open_rim_fgm', 'open_rim_fga'))
        }
    },
    'cra': {
        'description': 'Contested 2A at the Rim (Closest Defender Within 4 Feet; Shot Taken Within 5 feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{cont_rim_fga}',
            'team': '{cont_rim_fga}',
            'teams': '{cont_rim_fga}'
        }
    },
    'p/cr': {
        'description': 'Points per Contested 2A at the Rim (Closest Defender Within 4 Feet; Shot Taken Within 5 feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(2, divide('cont_rim_fgm', 'cont_rim_fga')),
            'team': multiply(2, divide('cont_rim_fgm', 'cont_rim_fga')),
            'teams': multiply(2, divide('cont_rim_fgm', 'cont_rim_fga'))
        }
    },
    'uar': {
        'description': 'Unassisted 2A Made at the Rim (Shot Taken Within 5 feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['ncaa'],
        'default': None,
        'values': {
            'player': '{unassisted_rim_fgm}',
            'team': '{unassisted_rim_fgm}',
            'teams': '{unassisted_rim_fgm}'
        }
    },
    'oma': {
        'description': 'Open 2A in the Mid-Range (Closest Defender Beyond 4 Feet; Shot Taken Beyond 5 feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': subtract('cont_fg2a', 'cont_rim_fga'),
            'team': subtract('cont_fg2a', 'cont_rim_fga'),
            'teams': subtract('cont_fg2a', 'cont_rim_fga')
        }
    },
    'p/om': {
        'description': 'Points per Open 2A in the Mid-Range (Closest Defender Beyond 4 Feet; Shot Taken Beyond 5 feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(2, divide(subtract('open_fg2m', 'open_rim_fgm'), subtract('open_fg2a', 'open_rim_fga'))),
            'team': multiply(2, divide(subtract('open_fg2m', 'open_rim_fgm'), subtract('open_fg2a', 'open_rim_fga'))),
            'teams': multiply(2, divide(subtract('open_fg2m', 'open_rim_fgm'), subtract('open_fg2a', 'open_rim_fga')))
        }
    },
    'cma': {
        'description': 'Contested 2A in the Mid-Range (Closest Defender Within 4 Feet; Shot Taken Beyond 5 feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': subtract('cont_fg2a', 'cont_rim_fga'),
            'team': subtract('cont_fg2a', 'cont_rim_fga'),
            'teams': subtract('cont_fg2a', 'cont_rim_fga')
        }
    },
    'p/cm': {
        'description': 'Points per Contested 2A in the Mid-Range (Closest Defender Within 4 Feet; Shot Taken Within 5 feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(2, divide(subtract('cont_fg2m', 'cont_rim_fgm'), subtract('cont_fg2a', 'cont_rim_fga'))),
            'team': multiply(2, divide(subtract('cont_fg2m', 'cont_rim_fgm'), subtract('cont_fg2a', 'cont_rim_fga'))),
            'teams': multiply(2, divide(subtract('cont_fg2m', 'cont_rim_fgm'), subtract('cont_fg2a', 'cont_rim_fga')))
        }
    },
    'uam': {
        'description': 'Unassisted 2A Made in the Mid-Range (Shot Taken Beyond 5 feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['ncaa'],
        'default': None,
        'values': {
            'player': subtract('unassisted_fg2m', 'unassisted_rim_fgm'),
            'team': subtract('unassisted_fg2m', 'unassisted_rim_fgm'),
            'teams': subtract('unassisted_fgm', 'unassisted_rim_fgm')
        }
    },
    '3a': {
        'description': 'Three-Point FGA',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{fg3a}',
            'team': '{fg3a}',
            'teams': '{fg3a}',
            'opponents': '{opp_fg3a}'
        }
    },
    'p/3': {
        'description': 'Points per 3A',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': multiply(3, divide('fg3m', 'fg3a')),
            'team': multiply(3, divide('fg3m', 'fg3a')),
            'teams': multiply(3, divide('fg3m', 'fg3a')),
            'opponents': multiply(3, divide('opp_fg3m', 'opp_fg3a'))
        }
    },
    'o3a': {
        'description': 'Open 3A (Closest Defender Beyond 4 Feet)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{open_fg3a}',
            'team': '{open_fg3a}',
            'teams': '{open_fg3a}'
        }
    },
    'p/o3': {
        'description': 'Points per Open 3A (Closest Defender Beyond 4 Feet)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(3, divide('open_fg3m', 'open_fg3a')),
            'team': multiply(3, divide('open_fg3m', 'open_fg3a')),
            'teams': multiply(3, divide('open_fg3m', 'open_fg3a'))
        }
    },
    'c3a': {
        'description': 'Contested 3A (Closest Defender Within 4 Feet)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{cont_fg3a}',
            'team': '{cont_fg3a}',
            'teams': '{cont_fg3a}'
        }
    },
    'p/c3': {
        'description': 'Points per Contested 3A (Closest Defender Within 4 Feet)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(3, divide('cont_fg3m', 'cont_fg3a')),
            'team': multiply(3, divide('cont_fg3m', 'cont_fg3a')),
            'teams': multiply(3, divide('cont_fg3m', 'cont_fg3a'))
        }
    },
    'ua3': {
        'description': 'Unassisted 3A Made',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['ncaa'],
        'default': None,
        'values': {
            'player': '{unassisted_fg3m}',
            'team': '{unassisted_fg3m}',
            'teams': '{unassisted_fg3m}'
        }
    },
    'ftr': {
        'description': 'Free throw Rate (FTA / FGA)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide('fta', add('fg2a', 'fg3a')),
            'team': divide('fta', add('fg2a', 'fg3a')),
            'teams': divide('fta', add('fg2a', 'fg3a')),
            'opponents': divide('opp_fta', add('opp_fg2a', 'opp_fg3a'))
        }
    },
    'p/ft': {
        'description': 'Points per FTA',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'two_char_dec',
        'leagues': ['nba', 'ncaa'],
        'team_row_display': 'team_value',
        'default': None,
        'values': {
            'player': divide('ftm', 'fta'),
            'team': divide('ftm', 'fta'),
            'teams': divide('ftm', 'fta'),
            'opponents': divide('opp_ftm', 'opp_fta')
        }
    },
    'dnk': {
        'description': 'Dunks',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{dunks}',
            'team': '{dunks}',
            'teams': '{dunks}'
        }
    },
    'tou': {
        'description': 'Instances of Holding the Ball',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{touches}',
            'team': '{touches}',
            'teams': '{touches}'
        }
    },
    'spt': {
        'description': 'Seconds per Touch',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'two_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': divide(multiply(60, 'time_on_ball'), 'touches'),
            'team': divide(multiply(60, 'time_on_ball'), 'touches'),
            'teams': divide(multiply(60, 'time_on_ball'), 'touches')
        }
    },
    '%trs': {
        'description': 'Percentage of Touches Resulting in a Shot',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(100, divide(add('fg2a', 'fg3a', multiply(0.44, 'fta')), 'touches')),
            'team': multiply(100, divide(add('fg2a', 'fg3a', multiply(0.44, 'fta')), 'touches')),
            'teams': multiply(100, divide(add('fg2a', 'fg3a', multiply(0.44, 'fta')), 'touches'))
        }
    },
    '%trp': {
        'description': 'Percentage of Touches Resulting in a Pass',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(100, divide('passes', 'touches')),
            'team': multiply(100, divide('passes', 'touches')),
            'teams': multiply(100, divide('passes', 'touches'))
        }
    },
    '%trt': {
        'description': 'Percentage of Touches Resulting in a Turnover',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(100, divide('turnovers', 'touches')),
            'team': multiply(100, divide('turnovers', 'touches')),
            'teams': multiply(100, divide('turnovers', 'touches'))
        }
    },
    'ast': {
        'description': 'Assists',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{assists}',
            'team': '{assists}',
            'teams': '{assists}',
            'opponents': '{opp_assists}'
        }
    },
    'past': {
        'description': 'Potential Assists (Passes that lead to a FGA)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{pot_assists}',
            'team': '{pot_assists}',
            'teams': '{pot_assists}'
        }
    },
    '2ast': {
        'description': 'Secondary Assists (Passes that Lead to an Assist)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'team_row_display': 'team_value',
        'default': None,
        'values': {
            'player': '{sec_assists}',
            'team': '{sec_assists}',
            'teams': '{sec_assists}'
        }
    },
    'tov': {
        'description': 'Turnovers',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'ball_management',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{turnovers}',
            'team': '{turnovers}',
            'opponents': '{opp_turnovers}'
        }
    },
    'or%': {
        'description': 'Offensive Rebound Percentage',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide('o_reb_pct_x1000', 10),
            'team': divide('o_reb_pct_x1000', 10),
            'teams': divide('o_reb_pct_x1000', 10)
        }
    },
    'cor%': {
        'description': 'Contested Offensive Rebound Percentage (Percentage of Offensive Rebounds that are Contested)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'team_row_display': 'team_value',
        'default': None,
        'values': {
            'player': divide('cont_o_rebs', 'o_rebs'),
            'team': divide('cont_o_rebs', 'o_rebs'),
            'teams': divide('cont_o_rebs', 'o_rebs')
        }
    },
    'dr%': {
        'description': 'Defensive Rebound Percentage',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide('d_reb_pct_x1000', 10),
            'team': divide('d_reb_pct_x1000', 10),
            'teams': divide('d_reb_pct_x1000', 10)
        }
    },
    'cdr%': {
        'description': 'Contested Defensive Rebound Percentage (Percentage of Defensive Rebounds that are Contested)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': divide('cont_d_rebs', 'd_rebs'),
            'team': divide('cont_d_rebs', 'd_rebs'),
            'teams': divide('cont_d_rebs', 'd_rebs')
        }
    },
    'pb%': {
        'description': 'Putbacks Percentage (Percentage of Offensive Rebounds that are Putbacks)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': divide('putbacks', 'o_rebs'),
            'team': divide('putbacks', 'o_rebs'),
            'teams': divide('putbacks', 'o_rebs')
        }
    },
        'odst': {
        'description': 'Offensive Distance Traveled in Miles',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'movement',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': divide('o_dist_x10', 10),
            'team': divide('o_dist_x10', 10),
            'teams': divide('o_dist_x10', 10)
        }
    },
    'ddst': {
        'description': 'Defensive Distance Traveled in Miles',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'movement',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': divide('d_dist_x10', 10),
            'team': divide('d_dist_x10', 10),
            'teams': divide('d_dist_x10', 10)
        }
    },
    'dra': {
        'description': 'Defended 2A at the Rim (Closest Defender for FGA Within 5 Feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{d_rim_fga}',
            'team': '{d_rim_fga}',
            'teams': '{d_rim_fga}'
        }
    },
    'p/dr': {
        'description': 'Points Allowed per Defended FGA at the Rim (Closest Defender for FGA Within 5 Feet of the Rim)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(2, divide('d_rim_fgm', 'd_rim_fga')),
            'team': multiply(2, divide('d_rim_fgm', 'd_rim_fga')),
            'teams': multiply(2, divide('d_rim_fgm', 'd_rim_fga'))
        }
    },
    'dma': {
        'description': 'Defended 2A in the Mid-Range (Closest Defender for FGA Beyond 5 Feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': subtract('d_fg2a', 'd_rim_fga'),
            'team': subtract('d_fg2a', 'd_rim_fga'),
            'teams': subtract('d_fg2a', 'd_rim_fga')
        }
    },
    'p/dm': {
        'description': 'Points Allowed per Defended FGA in the Mid-Range (Closest Defender for FGA Beyond 5 Feet of the Rim and Within the Three-Point Line)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(2, divide(subtract('d_fg2m', 'd_rim_fgm'), subtract('d_fg2a', 'd_rim_fga'))),
            'team': multiply(2, divide(subtract('d_fg2m', 'd_rim_fgm'), subtract('d_fg2a', 'd_rim_fga'))),
            'teams': multiply(2, divide(subtract('d_fg2m', 'd_rim_fgm'), subtract('d_fg2a', 'd_rim_fga')))
        }
    },
    'd3a': {
        'description': 'Defended 3A (Closest Defender)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{d_fg3a}',
            'team': '{d_fg3a}',
            'teams': '{d_fg3a}'
        }
    },
    'p/d3': {
        'description': 'Points Allowed per Defended 3A (Closest Defender)',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 2,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': multiply(3, divide('d_fg3m', 'd_fg3a')),
            'team': multiply(3, divide('d_fg3m', 'd_fg3a')),
            'teams': multiply(3, divide('d_fg3m', 'd_fg3a'))
        }
    },
    'cont': {
        'description': 'Shot Contests',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{contests}',
            'team': '{contests}',
            'teams': '{contests}'
        }
    },
    'blk': {
        'description': 'Blocks',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{blocks}',
            'team': '{blocks}',
            'teams': '{blocks}',
            'opponents': '{opp_blocks}'
        }
    },
    'defl': {
        'description': 'Deflections of the ball on Defense',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'advanced',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': '{deflections}',
            'team': '{deflections}',
            'teams': '{deflections}'
        }
    },
    'stl': {
        'description': 'Steals',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'basic',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['ncaa'],
        'default': None,
        'values': {
            'player': '{steals}',
            'team': '{steals}',
            'teams': '{steals}',
            'opponents': '{opp_steals}'
        }
    },
    'st+c': {
        'description': 'Steals + Charges Drawn',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': add('steals', 'charges_drawn'),
            'team': add('steals', 'charges_drawn'),
            'teams': add('steals', 'charges_drawn')
        }
    },
    'fls': {
        'description': 'Personal Fouls',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': True,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': '{fouls}',
            'team': '{fouls}',
            'teams': '{fouls}',
            'opponents': '{opp_fouls}'
        }
    },
    'w%': {
        'description': 'Win percentage',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'team_ratings',
        'tabs': ['teams', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'percentage',
        'decimal_places': 1,
        'width_class': 'three_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide('wins', 'games'),
            'team': divide('wins', 'games'),
            'teams': divide('wins', 'games')
        }
    },
    'ortg': {
        'description': 'Team Offensive Rating When On Court',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'team_ratings',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba', 'ncaa'],
        'team_row_display': 'team_value',
        'default': None,
        'values': {
            'player': divide('o_rtg_x10', 10),
            'team': divide('o_rtg_x10', 10),
            'teams': divide('o_rtg_x10', 10)
        }
    },
    'drtg': {
        'description': 'Team Defensive Rating When On Court',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'team_ratings',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba', 'ncaa'],
        'default': None,
        'values': {
            'player': divide('d_rtg_x10', 10),
            'team': divide('d_rtg_x10', 10),
            'teams': divide('d_rtg_x10', 10)
        }
    },
    'nooo': {
        'description': 'Net Offensive On/Off Team Rating',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'team_ratings',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'standard',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': subtract(divide('o_rtg_x10', 10), divide('off_o_rtg_x10', 10)),
            'team': subtract(divide('o_rtg_x10', 10), divide('off_o_rtg_x10', 10)),
            'teams': subtract(divide('o_rtg_x10', 10), divide('off_o_rtg_x10', 10))
        }
    },
    'ndoo': {
        'description': 'Net Defensive on/off Team Rating',
        'sections': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'team_ratings',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': 'reverse',
        'editable': False,
        'scale_with_rate': False,
        'format': 'number',
        'decimal_places': 1,
        'width_class': 'four_char_dec',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': subtract(divide('d_rtg_x10', 10), divide('off_d_rtg_x10', 10)),
            'team': subtract(divide('d_rtg_x10', 10), divide('off_d_rtg_x10', 10)),
            'teams': subtract(divide('d_rtg_x10', 10), divide('off_d_rtg_x10', 10))
        }
    },
    'ID#1': {
        'description': 'NBA_API Player ID',
        'sections': ['identity'],
        'subsection': 'nba',
        'tabs': ['teams', 'players', 'team'],
        'stats_mode': 'both',
        'percentile': None,
        'editable': False,
        'scale_with_rate': False,
        'format': 'text',
        'decimal_places': 0,
        'width_class': 'auto',
        'leagues': ['nba'],
        'default': None,
        'values': {
            'player': 'nba_api_id',
        }
    }
}