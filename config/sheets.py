"""
THE GLASS - Display Configuration
Single source of truth for all display logic, calculations, formatting, and percentiles.

This config drives:
- Sheet column generation (sheets_sync.py)
- Stat calculations (stat_engine.py)
- Percentile generation (auto-generated columns)
- Section organization (player_info, analysis, rates, scoring, distribution, rebounding, defense, onoff, identity)
"""

import os

# ============================================================================
# GOOGLE SHEETS CONFIGURATION
# ============================================================================

GOOGLE_SHEETS_CONFIG = {
    'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE'),
    'spreadsheet_id': os.getenv('GOOGLE_SPREADSHEET_ID'),
    'spreadsheet_name': os.getenv('GOOGLE_SPREADSHEET_NAME'),
    'scopes': [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ],
}

# ============================================================================
# API CONFIGURATION
# ============================================================================

API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', '5000')),
    'debug': os.getenv('API_DEBUG', 'False').lower() == 'true',
    'cors_enabled': True,
}

SERVER_CONFIG = {
    'production_host': os.getenv('PRODUCTION_HOST', ''),
    'production_port': int(os.getenv('PRODUCTION_PORT', '5000')),
    'ssh_user': os.getenv('SSH_USER', ''),
    'remote_dir': os.getenv('REMOTE_DIR', ''),
    'systemd_service': os.getenv('SYSTEMD_SERVICE', 'flask-api'),
}

# ============================================================================
# STAT CALCULATION CONSTANTS
# ============================================================================

STAT_CONSTANTS = {
    'game_length_minutes': 48.0,        # NBA game length
    'ts_fta_multiplier': 0.44,          # True shooting FTA coefficient
    'default_per_minutes': 36.0,        # Default minutes for per-minute stats
    'default_per_possessions': 100.0,   # Default possessions for per-possession stats
}

# ============================================================================
# SECTION AND SUBSECTION DEFINITIONS
# ============================================================================

# Section display configuration - defines how each section appears in sheets
SECTION_CONFIG = {
    'entities': {
        'display_name': 'Players',
    },
    'player_info': {
        'display_name': 'Player Info',
    },
    'analysis': {
        'display_name': 'Analysis',
    },
    'current_stats': {
        'display_name': 'Current Stats',
    },
    'historical_stats': {
        'display_name': 'Historical Stats',
    },
    'postseason_stats': {
        'display_name': 'Postseason Stats',
    },
    'identity': {
        'display_name': 'Identity',
    },
}

# Stat subsections - correspond to Row 2 subsection headers (within current_stats/historical_stats/postseason_stats)
SUBSECTIONS = [
    'rates',          # Games, Minutes, Possessions
    'scoring',        # Points, TS%, 2fg/3, Rim/Mid/3PT tracking, FT
    'distribution',   # Touches, Assists, Potential Assists, Turnovers
    'rebounding',     # OREB%, DREB%, Contested OREB/DREB%, Putbacks
    'movement',       # Offensive/Defensive distance traveled
    'defense',        # Defended shots, Steals, Deflections, Blocks, Contests, Charges, Fouls
    'onoff',          # Offensive/Defensive Rating, Off-court ratings
]

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
}

COLOR_THRESHOLDS = {
    'low': 0,    # 0% = pure red
    'mid': 50,   # 50% = pure yellow
    'high': 100, # 100% = pure green
}

# ============================================================================
# DISPLAY_COLUMNS - Master dictionary for all display columns
# ============================================================================
# Format:
# 'column_key': {
#     'key': 'column_key',                    # Must match key (for consistency)
#     'db_field': 'database_field_name',      # Maps to DB_COLUMNS
#     'display_name': 'PTS',                  # Column header in sheets
#     'section': ['current_stats', 'historical_stats'],  # List of sections where column appears
#     'subsection': 'scoring',                # Subsection within stats sections (or None)
#     'applies_to_entities': ['player', 'team', 'opponent'],  # Which entities get this column
#     'view_mode': 'both',                    # 'basic', 'advanced', or 'both'
#     'has_percentile': True,                 # Whether to generate percentile column
#     'editable': False,                      # Whether users can edit in sheets
#     'reverse_percentile': False,            # True for stats where lower is better
#     'format_as_percentage': False,          # Display as percentage
#     'decimal_places': 1,                    # Number of decimal places
#     'calculated': False,                    # Whether value is calculated from other fields
#     'calculation_formula': None,            # Formula for calculated fields
# }

DISPLAY_COLUMNS = {
    # ========================================================================
    # PLAYER INFO SECTION
    # ========================================================================
      'names': {
        'db_field': 'name',
        'display_name': 'Names',
        'section': ['entities'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'team': {
        'key': 'team',
        'db_field': 'team_abbr',
        'display_name': 'Team',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'jersey': {
        'key': 'jersey',
        'db_field': 'jersey_number',
        'display_name': '#',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'experience': {
        'key': 'experience',
        'db_field': 'years_experience',
        'display_name': 'Exp',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'age': {
        'key': 'age',
        'db_field': 'birthdate',  # Calculate age from birthdate
        'display_name': 'Age',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': True,  # Younger is better
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'height': {
        'key': 'height',
        'db_field': 'height_inches',
        'display_name': 'Height',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'weight': {
        'key': 'weight',
        'db_field': 'weight_lbs',
        'display_name': 'Weight',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'wingspan': {
        'key': 'wingspan',
        'db_field': 'wingspan_inches',
        'display_name': 'Wingspan',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'hand': {
        'key': 'hand',
        'db_field': 'hand',
        'display_name': 'Hand',
        'section': ['player_info'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    # ========================================================================
    # ANALYSIS SECTION
    # ========================================================================
    
    'notes': {
        'key': 'notes',
        'db_field': 'notes',
        'display_name': 'Notes',
        'section': ['analysis'],
        'subsection': None,
        'applies_to_entities': ['player', 'team'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    # ========================================================================
    # STATS SECTIONS - RATES SUBSECTION
    # ========================================================================
    'years': {
        'key': 'games',
        'db_field': 'years',
        'display_name': 'Yrs',
        'section': ['historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
        
    'games': {
        'key': 'games',
        'db_field': 'games_played',
        'display_name': 'Games',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    
    'minutes': {
        'key': 'minutes',
        'db_field': 'minutes_x10',
        'display_name': 'Minutes',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    'possessions': {
        'key': 'possessions',
        'db_field': None,
        'display_name': 'Poss',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '2fga + 3fga - off_rebounds + turnovers + (0.44 * fta)',
    },
    
    # ========================================================================
    # STATS SECTIONS - SCORING SUBSECTION
    # ========================================================================
    
    'points': {
        'key': 'points',
        'db_field': None,
        'display_name': 'PTS',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '(2fgm * 2) + (3fgm * 3) + ftm',
    },
    
    'ts_pct': {
        'key': 'ts_pct',
        'db_field': None,
        'display_name': 'TS%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'points / (2 * (2fga + 3fga + 0.44 * fta))',
    },
    
    '2fga': {
        'key': '2fga',
        'db_field': '2fga',
        'display_name': '2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'basic',  # Hidden on advanced
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    '2fg_pct': {
        'key': '2fg_pct',
        'db_field': None,
        'display_name': '2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'basic',  # Hidden on advanced
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '2fgm / 2fga',
    },
    
    '3fga': {
        'key': '3fga',
        'db_field': '3fga',
        'display_name': '3FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'basic',  # Hidden on advanced
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    '3fg_pct': {
        'key': '3fg_pct',
        'db_field': None,
        'display_name': '3FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'basic',  # Hidden on advanced
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '3fgm / 3fga',
    },
    
    'cont_rim_2fga': {
        'key': 'cont_rim_2fga',
        'db_field': 'cont_close_2fga',
        'display_name': 'Cont <10ft 2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'cont_rim_2fg_pct': {
        'key': 'cont_rim_2fg_pct',
        'db_field': None,
        'display_name': 'Cont <10ft 2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'cont_close_2fgm / cont_close_2fga',
    },
    
    'open_rim_2fga': {
        'key': 'open_rim_2fga',
        'db_field': 'open_close_2fga',
        'display_name': 'Open <10ft 2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'open_rim_2fg_pct': {
        'key': 'open_rim_2fg_pct',
        'db_field': None,
        'display_name': 'Open <10ft 2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'open_close_2fgm / open_close_2fga',
    },
    
    'cont_mid_2fga': {
        'key': 'cont_mid_2fga',
        'db_field': None,
        'display_name': 'Cont >10ft 2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'cont_2fga - cont_close_2fga',
    },
    
    'cont_mid_2fg_pct': {
        'key': 'cont_mid_2fg_pct',
        'db_field': None,
        'display_name': 'Cont >10ft 2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '(cont_2fgm - cont_close_2fgm) / (cont_2fga - cont_close_2fga)',
    },
    
    'open_mid_2fga': {
        'key': 'open_mid_2fga',
        'db_field': None,
        'display_name': 'Open >10ft 2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'open_2fga - open_close_2fga',
    },
    
    'open_mid_2fg_pct': {
        'key': 'open_mid_2fg_pct',
        'db_field': None,
        'display_name': 'Open >10ft 2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '(open_2fgm - open_close_2fgm) / (open_2fga - open_close_2fga)',
    },
    
    'cont_3fga': {
        'key': 'cont_3fga',
        'db_field': 'cont_3fga',
        'display_name': 'Cont 3FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'cont_3fg_pct': {
        'key': 'cont_3fg_pct',
        'db_field': None,
        'display_name': 'Cont 3FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'cont_3fgm / cont_3fga',
    },
    
    'open_3fga': {
        'key': 'open_3fga',
        'db_field': 'open_3fga',
        'display_name': 'Open 3FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'open_3fg_pct': {
        'key': 'open_3fg_pct',
        'db_field': None,
        'display_name': 'Open 3FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'open_3fgm / open_3fga',
    },
    
    'fta': {
        'key': 'fta',
        'db_field': 'fta',
        'display_name': 'FTA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'ft_pct': {
        'key': 'ft_pct',
        'db_field': None,
        'display_name': 'FT%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'ftm / fta',
    },
    
    # ========================================================================
    # STATS SECTIONS - DISTRIBUTION SUBSECTION (Ball Management)
    # ========================================================================
    
    'assists': {
        'key': 'assists',
        'db_field': 'assists',
        'display_name': 'AST',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'potential_assists': {
        'key': 'potential_assists',
        'db_field': 'pot_assists',
        'display_name': 'Pot AST',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'secondary_assists': {
        'key': 'secondary_assists',
        'db_field': 'sec_assists',
        'display_name': '2nd AST',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'passes': {
        'key': 'passes',
        'db_field': 'passes',
        'display_name': 'Passes',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'touches': {
        'key': 'touches',
        'db_field': 'touches',
        'display_name': 'Touches',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'time_on_ball': {
        'key': 'time_on_ball',
        'db_field': 'time_on_ball',
        'display_name': 'Time On Ball',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'turnovers': {
        'key': 'turnovers',
        'db_field': 'turnovers',
        'display_name': 'TO',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    # ========================================================================
    # STATS SECTIONS - REBOUNDING SUBSECTION
    # ========================================================================
    
    'oreb_pct': {
        'key': 'oreb_pct',
        'db_field': 'o_reb_pct_x1000',
        'display_name': 'OREB%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'divide_by_1000': True,
        'db_field_totals': 'off_rebounds',  # In totals mode, use raw count
    },
    
    'dreb_pct': {
        'key': 'dreb_pct',
        'db_field': 'd_reb_pct_x1000',
        'display_name': 'DREB%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'divide_by_1000': True,
        'db_field_totals': 'def_rebounds',  # In totals mode, use raw count
    },
    
    'cont_oreb_pct': {
        'key': 'cont_oreb_pct',
        'db_field': None,
        'display_name': 'Cont OREB%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'cont_o_rebs / o_rebounds',
    },
    
    'cont_dreb_pct': {
        'key': 'cont_dreb_pct',
        'db_field': None,
        'display_name': 'Cont DREB%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'cont_d_rebs / d_rebounds',
    },
    
    'putbacks_pct': {
        'key': 'putbacks_pct',
        'db_field': None,
        'display_name': 'Putbacks%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'putbacks / o_rebounds',
    },
    
    # ========================================================================
    # STATS SECTIONS - MOVEMENT SUBSECTION
    # ========================================================================
    
    'off_distance': {
        'key': 'off_distance',
        'db_field': 'off_distance_x10',
        'display_name': 'Off Dist',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'movement',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    'def_distance': {
        'key': 'def_distance',
        'db_field': 'def_distance_x10',
        'display_name': 'Def Dist',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'movement',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    # ========================================================================
    # STATS SECTIONS - DEFENSE SUBSECTION
    # ========================================================================
    
    'def_rim_2fga': {
        'key': 'def_rim_2fga',
        'db_field': 'def_rim_2fga',
        'display_name': 'Def <10ft 2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'def_rim_2fg_pct': {
        'key': 'def_rim_2fg_pct',
        'db_field': None,
        'display_name': 'Def <10ft 2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'd_close_2fgm / d_close_2fga',
    },
    
    'def_mid_2fga': {
        'key': 'def_mid_2fga',
        'db_field': None,
        'display_name': 'Def >10ft 2FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'd_2fga - d_close_2fga',
    },
    
    'def_mid_2fg_pct': {
        'key': 'def_mid_2fg_pct',
        'db_field': None,
        'display_name': 'Def >10ft 2FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': '(d_2fgm - d_close_2fgm) / (d_2fga - d_close_2fga)',
    },
    
    'def_3fga': {
        'key': 'def_3fga',
        'db_field': 'd_3fga',
        'display_name': 'Def 3FGA',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'def_3fg_pct': {
        'key': 'def_3fg_pct',
        'db_field': None,
        'display_name': 'Def 3FG%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'd_3fgm / d_3fga',
    },
    
    'real_def_pct': {
        'key': 'real_def_pct',
        'db_field': 'real_d_fg_pct_x1000',
        'display_name': 'Real Def%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': True,
        'decimal_places': 1,
        'divide_by_1000': True,
    },
    
    'block_pct': {
        'key': 'block_pct',
        'db_field': None,
        'display_name': 'Block%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': True,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'blocks / contests',
    },
    
    'contests': {
        'key': 'contests',
        'db_field': 'contests',
        'display_name': 'Contests',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'steals_plus_charges': {
        'key': 'steals_plus_charges',
        'db_field': None,
        'display_name': 'Steals+Charges',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'calculated': True,
        'calculation_formula': 'steals + charges',
    },
    
    'deflections': {
        'key': 'deflections',
        'db_field': 'deflections',
        'display_name': 'Deflections',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'advanced',  # Hidden on basic
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'charges': {
        'key': 'charges',
        'db_field': 'charges',
        'display_name': 'Charges',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    'fouls': {
        'key': 'fouls',
        'db_field': 'fouls',
        'display_name': 'Fouls',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    # ========================================================================
    # STATS SECTIONS - ON/OFF SUBSECTION
    # ========================================================================
    
    'off_rating': {
        'key': 'off_rating',
        'db_field': 'off_rating_x10',
        'display_name': 'ORtg',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    'def_rating': {
        'key': 'def_rating',
        'db_field': 'def_rating_x10',
        'display_name': 'DRtg',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,  # Lower is better
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    'off_onoff': {
        'key': 'off_onoff',
        'db_field': 'off_onoff_x10',
        'display_name': 'Off On/Off',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    'def_onoff': {
        'key': 'def_onoff',
        'db_field': 'def_onoff_x10',
        'display_name': 'Def On/Off',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 1,
        'divide_by_10': True,
    },
    
    'years': {
        'key': 'years',
        'db_field': 'years',  # Calculated field showing which years are included
        'display_name': 'Years',
        'section': ['historical_stats', 'postseason_stats'],
        'subsection': None,
        'applies_to_entities': ['player', 'team', 'opponent'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
        'calculated': True,
        'calculation_formula': 'format_years_range',  # Helper function to format year range
    },
    
    # ========================================================================
    # IDENTITY SECTION
    # ========================================================================
    
    'player_id': {
        'key': 'player_id',
        'db_field': 'player_id',
        'display_name': 'NBA ID',
        'section': ['identity'],
        'subsection': None,
        'applies_to_entities': ['player'],
        'view_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_display_columns_by_section(section):
    """Get all display columns for a specific section."""
    return {k: v for k, v in DISPLAY_COLUMNS.items() if v['section'] == section}


def get_display_columns_by_view(view_mode):
    """
    Get display columns for a specific view mode.
    Args:
        view_mode: 'basic', 'advanced', or 'both'
    """
    if view_mode == 'both':
        return DISPLAY_COLUMNS
    return {k: v for k, v in DISPLAY_COLUMNS.items() 
            if v['view_mode'] in [view_mode, 'both']}


def get_display_columns_by_entity(entity_type):
    """Get display columns applicable to an entity type."""
    return {k: v for k, v in DISPLAY_COLUMNS.items() 
            if entity_type in v['applies_to_entities']}


def get_display_columns_by_period(period):
    """Get display columns applicable to a period."""
    return {k: v for k, v in DISPLAY_COLUMNS.items() 
            if period in v['applies_to_periods']}


def get_editable_columns():
    """Get all editable columns (notes, wingspan)."""
    return {k: v for k, v in DISPLAY_COLUMNS.items() if v['editable']}


def generate_percentile_columns():
    """
    Auto-generate percentile column definitions for all columns with has_percentile=True.
    Percentiles are ALWAYS calculated for the current stats mode and separated by entity type.
    
    Returns a dictionary of percentile columns in the format:
    - {col_key}_pct: Percentile column (visible when show_percentiles toggle is on)
    
    Percentile columns inherit all properties from their base column except:
    - display_name gets "%" suffix
    - has_percentile = False (percentiles don't have percentiles)
    - is_generated_percentile = True (marks as auto-generated)
    """
    percentile_columns = {}
    
    for col_key, col_def in DISPLAY_COLUMNS.items():
        if not col_def.get('has_percentile'):
            continue
        
        # Generate one percentile column per base column
        # Entity type is handled during rendering (players vs teams vs opponents use different scales)
        pct_key = f"{col_key}_pct"
        
        percentile_columns[pct_key] = {
            'key': pct_key,
            'db_field': None,  # Percentiles are calculated, not from DB
            'display_name': f"{col_def['display_name']}%",
            'section': col_def['section'],  # Same sections as base column
            'subsection': col_def.get('subsection'),
            'applies_to_entities': col_def['applies_to_entities'],
            'view_mode': col_def['view_mode'],  # Same visibility rules
            'has_percentile': False,  # Percentiles don't have percentiles
            'is_stat': col_def.get('is_stat', False),
            'editable': False,  # Percentiles are never editable
            'reverse_percentile': col_def.get('reverse_percentile', False),
            'format_as_percentage': False,  # Percentile itself is already 0-100
            'decimal_places': 0,  # Percentiles shown as whole numbers
            'calculated': True,
            'calculation_formula': f"percentile({col_key})",
            'is_generated_percentile': True,
            'base_stat': col_key,
        }
    
    return percentile_columns


def get_all_columns_with_percentiles():
    """Get DISPLAY_COLUMNS plus all auto-generated percentile columns."""
    all_cols = dict(DISPLAY_COLUMNS)
    all_cols.update(generate_percentile_columns())
    return all_cols


# ============================================================================
# COLUMN FILTERING AND SELECTION HELPERS
# ============================================================================

def get_columns_by_filters(section=None, subsection=None, entity=None, view_mode=None, 
                           include_percentiles=False):
    """
    Get columns matching specified filters.
    
    Args:
        section: Filter by section (e.g., 'current_stats', 'player_info')
        subsection: Filter by subsection (e.g., 'scoring', 'defense')
        entity: Filter by entity type ('player', 'team', 'opponent')
        view_mode: Filter by view mode ('basic', 'advanced', 'both')
        include_percentiles: Whether to include auto-generated percentile columns
    
    Returns:
        Dictionary of matching columns
    """
    if include_percentiles:
        columns = get_all_columns_with_percentiles()
    else:
        columns = DISPLAY_COLUMNS
    
    filtered = {}
    for col_key, col_def in columns.items():
        # Check section filter (section is a list in column def)
        if section and section not in col_def.get('section', []):
            continue
        
        # Check subsection filter
        if subsection and col_def.get('subsection') != subsection:
            continue
        
        # Check entity filter
        if entity and entity not in col_def.get('applies_to_entities', []):
            continue
        
        # Check view mode filter
        if view_mode:
            col_view = col_def.get('view_mode', 'both')
            if col_view != 'both' and col_view != view_mode:
                continue
        
        filtered[col_key] = col_def
    
    return filtered


def get_columns_for_section_and_entity(section, entity, view_mode='both', include_percentiles=False):
    """
    Get all columns for a specific section and entity combination.
    This is the primary function used when building sheet columns.
    
    Args:
        section: Section name ('current_stats', 'historical_stats', etc.)
        entity: Entity type ('player', 'team', 'opponent')
        view_mode: View mode filter ('basic', 'advanced', 'both')
        include_percentiles: Whether to include percentile columns
    
    Returns:
        List of column definitions in display order
    """
    columns = get_columns_by_filters(
        section=section,
        entity=entity,
        view_mode=view_mode,
        include_percentiles=include_percentiles
    )
    
    # Sort by subsection order if in stats section
    section_config = SECTION_CONFIG.get(section, {})
    if section_config.get('is_stats_section'):
        # Group by subsection
        subsection_groups = {}
        for col_key, col_def in columns.items():
            subsec = col_def.get('subsection')
            if subsec not in subsection_groups:
                subsection_groups[subsec] = []
            subsection_groups[subsec].append((col_key, col_def))
        
        # Build ordered list following SUBSECTIONS order
        ordered = []
        for subsec in SUBSECTIONS:
            if subsec in subsection_groups:
                ordered.extend(subsection_groups[subsec])
        
        return ordered
    else:
        # Non-stats sections: return in definition order
        return [(k, v) for k, v in columns.items()]


def build_sheet_columns(entity='player', view_mode='both', show_percentiles=False):
    """
    Build complete column structure for a sheet.
    
    Args:
        entity: Entity type ('player', 'team', 'opponent')
        view_mode: View mode ('basic', 'advanced', 'both')
        show_percentiles: Whether percentile columns should be visible (vs value columns)
    
    Returns:
        List of tuples: (column_key, column_def, is_percentile)
    """
    all_columns = []
    
    for section in SECTIONS:
        section_cols = get_columns_for_section_and_entity(
            section=section,
            entity=entity,
            view_mode=view_mode,
            include_percentiles=True
        )
        
        for col_key, col_def in section_cols:
            is_percentile = col_def.get('is_generated_percentile', False)
            
            # If show_percentiles toggle is on, show percentile columns and hide value columns
            # If show_percentiles toggle is off, show value columns and hide percentile columns
            if is_percentile:
                visible = show_percentiles and col_def.get('has_percentile', False)
            else:
                visible = not show_percentiles or not col_def.get('has_percentile', False)
            
            all_columns.append((col_key, col_def, visible))
    
    return all_columns


def get_column_index(column_key, columns_list):
    """
    Get the 0-based index of a column in the columns list.
    
    Args:
        column_key: The key to search for
        columns_list: List of tuples from build_sheet_columns()
    
    Returns:
        Index of column, or None if not found
    """
    for idx, (col_key, col_def, visible) in enumerate(columns_list):
        if col_key == column_key:
            return idx
    return None


def build_headers(columns_list):
    """
    Build the 4-row header structure for Google Sheets.
    
    Row 1: Section headers (merged across section columns)
    Row 2: Subsection headers (merged across subsection columns) 
    Row 3: Column names
    Row 4: Empty (for filters)
    
    Args:
        columns_list: List of tuples from build_sheet_columns()
    
    Returns:
        Dict with:
            - row1: List of section header values
            - row2: List of subsection header values
            - row3: List of column names
            - row4: Empty list (for filters)
            - merges: List of merge ranges for rows 1 and 2
    """
    row1 = []
    row2 = []
    row3 = []
    merges = []
    
    current_section = None
    current_subsection = None
    section_start_col = 0
    subsection_start_col = 0
    
    for idx, (col_key, col_def, visible) in enumerate(columns_list):
        # Get section info
        sections = col_def.get('section', [])
        section = sections[0] if sections else 'unknown'
        subsection = col_def.get('subsection')
        
        # Row 1: Section headers
        if section != current_section:
            if current_section is not None and section_start_col < idx:
                # Create merge for previous section
                section_config = SECTION_CONFIG.get(current_section, {})
                display_name = section_config.get('display_name', current_section)
                merges.append({
                    'row': 0,
                    'start_col': section_start_col,
                    'end_col': idx,
                    'value': display_name
                })
            
            current_section = section
            section_start_col = idx
            row1.append(SECTION_CONFIG.get(section, {}).get('display_name', section))
        else:
            row1.append('')  # Will be merged
        
        # Row 2: Subsection headers (only for stats sections)
        section_config = SECTION_CONFIG.get(section, {})
        if section_config.get('is_stats_section') and subsection:
            if subsection != current_subsection:
                if current_subsection is not None and subsection_start_col < idx:
                    # Create merge for previous subsection
                    merges.append({
                        'row': 1,
                        'start_col': subsection_start_col,
                        'end_col': idx,
                        'value': current_subsection.title()
                    })
                
                current_subsection = subsection
                subsection_start_col = idx
                row2.append(subsection.title())
            else:
                row2.append('')  # Will be merged
        else:
            current_subsection = None
            row2.append('')
        
        # Row 3: Column names
        row3.append(col_def.get('display_name', col_key))
    
    # Close final section merge
    if current_section is not None:
        section_config = SECTION_CONFIG.get(current_section, {})
        display_name = section_config.get('display_name', current_section)
        merges.append({
            'row': 0,
            'start_col': section_start_col,
            'end_col': len(columns_list),
            'value': display_name
        })
    
    # Close final subsection merge
    if current_subsection is not None:
        merges.append({
            'row': 1,
            'start_col': subsection_start_col,
            'end_col': len(columns_list),
            'value': current_subsection.title()
        })
    
    return {
        'row1': row1,
        'row2': row2,
        'row3': row3,
        'row4': [''] * len(columns_list),  # Empty row for filters
        'merges': merges
    }


# ============================================================================
# STAT CALCULATION HELPERS
# ============================================================================

def calculate_stat_value(entity_data, col_def, stats_mode='per_100_poss'):
    """
    Calculate a single stat value based on column definition and stats mode.
    
    Args:
        entity_data: Dict with raw data from database
        col_def: Column definition from DISPLAY_COLUMNS
        stats_mode: 'totals', 'per_game', 'per_36', 'per_100_poss', etc.
    
    Returns:
        Calculated stat value
    """
    # Handle calculated fields
    if col_def.get('calculated'):
        formula = col_def.get('calculation_formula', '')
        
        # Parse and evaluate formula
        # Examples: 'steals + charges', '(2fgm * 2) + (3fgm * 3) + ftm'
        try:
            # Build local namespace with entity data
            local_vars = dict(entity_data)
            local_vars['STAT_CONSTANTS'] = STAT_CONSTANTS
            
            # Evaluate formula
            result = eval(formula, {"__builtins__": {}}, local_vars)
            return result
        except Exception as e:
            return 0
    
    # Handle totals mode override (e.g., OREB% becomes OREB count)
    if stats_mode == 'totals' and col_def.get('db_field_totals'):
        db_field = col_def['db_field_totals']
        return entity_data.get(db_field, 0)
    
    # Get raw value from database
    db_field = col_def.get('db_field')
    if not db_field:
        return 0
    
    raw_value = entity_data.get(db_field, 0)
    if raw_value is None:
        raw_value = 0
    
    # Apply scaling (divide_by_10, divide_by_1000)
    if col_def.get('divide_by_10'):
        raw_value = raw_value / 10.0
    elif col_def.get('divide_by_1000'):
        raw_value = raw_value / 1000.0
    
    # Apply stats mode scaling
    if stats_mode != 'totals' and col_def.get('is_stat'):
        minutes = entity_data.get('minutes_total', 0)
        possessions = entity_data.get('possessions', 0)
        games = entity_data.get('games_played', 1)
        
        if stats_mode == 'per_game':
            factor = 1.0 / max(games, 1)
        elif stats_mode == 'per_36':
            factor = STAT_CONSTANTS['default_per_minutes'] / max(minutes, 1)
        elif stats_mode == 'per_100_poss':
            factor = STAT_CONSTANTS['default_per_possessions'] / max(possessions, 1)
        else:
            factor = 1.0
        
        # Don't scale percentage fields
        if not col_def.get('format_as_percentage'):
            raw_value = raw_value * factor
    
    return raw_value


def format_stat_value(value, col_def, stats_mode='per_100_poss'):
    """
    Format a stat value for display according to column definition.
    
    Args:
        value: Raw calculated value
        col_def: Column definition
        stats_mode: Current stats mode
    
    Returns:
        Formatted string or number
    """
    if value is None or (isinstance(value, (int, float)) and value == 0):
        return 0
    
    # Handle percentages
    if col_def.get('format_as_percentage'):
        value = value * 100  # Convert 0.456 to 45.6
    
    # Round to specified decimal places
    decimals = col_def.get('decimal_places', 1)
    rounded = round(value, decimals)
    
    # Return int if whole number
    if rounded == int(rounded):
        return int(rounded)
    
    return rounded


# ============================================================================
# PERCENTILE CALCULATION HELPERS
# ============================================================================

def get_percentile_rank(value, all_values, reverse=False):
    """
    Calculate percentile rank of a value within a list of values.
    
    Args:
        value: The value to rank
        all_values: List of all values to compare against
        reverse: True if lower is better (e.g., turnovers, fouls)
    
    Returns:
        Percentile rank from 0-100
    """
    if not all_values or value is None:
        return 50  # Default to median
    
    # Filter out None values
    valid_values = [v for v in all_values if v is not None]
    if not valid_values:
        return 50
    
    # Sort values
    sorted_values = sorted(valid_values, reverse=reverse)
    
    # Find percentile
    try:
        rank = sorted_values.index(value)
        percentile = (1 - (rank / len(sorted_values))) * 100
        return percentile
    except ValueError:
        # Value not in list, find nearest
        sorted_values.append(value)
        sorted_values.sort(reverse=reverse)
        rank = sorted_values.index(value)
        percentile = (1 - (rank / len(sorted_values))) * 100
        return percentile


def get_color_for_percentile(percentile, reverse=False):
    """
    Get RGB color dict for a percentile value using gradient.
    
    Args:
        percentile: Value from 0-100
        reverse: True if lower is better (reverses color gradient)
    
    Returns:
        Dict with 'red', 'green', 'blue' keys (values 0-1)
    """
    if reverse:
        percentile = 100 - percentile
    
    # Clamp percentile to 0-100
    percentile = max(0, min(100, percentile))
    
    # Get threshold colors
    red = COLORS['red']
    yellow = COLORS['yellow']
    green = COLORS['green']
    
    # Calculate gradient
    if percentile < COLOR_THRESHOLDS['mid']:
        # Interpolate between red and yellow
        ratio = percentile / COLOR_THRESHOLDS['mid']
        return {
            'red': red['red'] + (yellow['red'] - red['red']) * ratio,
            'green': red['green'] + (yellow['green'] - red['green']) * ratio,
            'blue': red['blue'] + (yellow['blue'] - red['blue']) * ratio,
        }
    else:
        # Interpolate between yellow and green
        ratio = (percentile - COLOR_THRESHOLDS['mid']) / (COLOR_THRESHOLDS['high'] - COLOR_THRESHOLDS['mid'])
        return {
            'red': yellow['red'] + (green['red'] - yellow['red']) * ratio,
            'green': yellow['green'] + (green['green'] - yellow['green']) * ratio,
            'blue': yellow['blue'] + (green['blue'] - yellow['blue']) * ratio,
        }


# ============================================================================
# FORMATTING HELPERS (from formatting_utils.py)
# ============================================================================

def get_color_dict(color_name):
    """Get color dict from COLORS constant."""
    return COLORS.get(color_name, COLORS['white'])


def create_text_format(font_family=None, font_size=None, bold=False, foreground_color='white'):
    """Create a text format dict for Google Sheets API."""
    format_dict = {
        'foregroundColor': get_color_dict(foreground_color),
        'bold': bold
    }
    if font_family:
        format_dict['fontFamily'] = font_family
    if font_size:
        format_dict['fontSize'] = font_size
    return format_dict


def create_cell_format(background_color='white', text_format=None, h_align='CENTER', 
                       v_align='MIDDLE', wrap='CLIP'):
    """Create a complete cell format dict."""
    cell_format = {
        'backgroundColor': get_color_dict(background_color),
        'horizontalAlignment': h_align,
        'verticalAlignment': v_align,
        'wrapStrategy': wrap
    }
    if text_format:
        cell_format['textFormat'] = text_format
    return cell_format


def format_height(inches):
    """
    Format height in inches to feet-inches string.
    
    Args:
        inches: Height in inches (e.g., 80)
    
    Returns:
        String like "6'8\""
    """
    if not inches:
        return ''
    feet = int(inches // 12)
    remaining_inches = int(inches % 12)
    return f"{feet}'{remaining_inches}\""

