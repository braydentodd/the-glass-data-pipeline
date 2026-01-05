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
    'default_per_minutes': 36.0,        # Default minutes for per-minute stats
    'default_per_possessions': 100.0,   # Default possessions for per-possession stats
}

# ============================================================================
# SECTION AND SUBSECTION DEFINITIONS
# ============================================================================

# Section display configuration - defines how each section appears in sheets
SECTION_CONFIG = {
    'entities': {
        'display_name': 'Names',
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

# List of all sections (for backwards compatibility)
SECTIONS = list(SECTION_CONFIG.keys())

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

DISPLAY_COLUMNS = {

    'names': {
        'display_name': 'Names',
        'section': ['entities'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'name',
        'team_formula': 'Team',
        'opponents_formula': 'Opponents',
    },
    
    'team': {
        'display_name': 'Tm',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'nba',
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'team_abbr',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'jersey': {
        'display_name': '#',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'jersey_number',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'experience': {
        'display_name': 'Exp',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'years_experience',
        'team_formula': 'years_experience',
        'opponents_formula': None,
    },
    
    'age': {
        'display_name': 'Age',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'age',
        'team_formula': 'age',
        'opponents_formula': None,
    },
    
    'height': {
        'display_name': 'Ht',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'height',
        'decimal_places': 1,
        'player_formula': 'height_inches',
        'team_formula': 'height_inches',
        'opponents_formula': None,
    },
    
    'weight': {
        'display_name': 'Wt',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'weight_lbs',
        'team_formula': 'weight_lbs',
        'opponents_formula': None,
    },
    
    'wingspan': {
        'display_name': 'WS',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format': 'height',
        'decimal_places': 1,
        'player_formula': 'wingspan_inches',
        'team_formula': 'wingspan_inches',
        'opponents_formula': None,
    },
    
    'hand': {
        'display_name': '🖐️',
        'section': ['player_info'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'hand',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'notes': {
        'display_name': 'Notes',
        'section': ['analysis'],
        'subsection': None,
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': True,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'notes',
        'team_formula': 'notes',
        'opponents_formula': None,
    },

    'years': {
        'display_name': 'Yrs',
        'section': ['historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'sheets': 'both',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'year',
        'team_formula': 'year',
        'opponents_formula': None,
    },
        
    'games': {
        'display_name': 'GMS',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'games_played',
        'team_formula': 'games_played',
        'opponents_formula': None,
    },
    
    'minutes': {
        'display_name': 'Min',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'minutes_x10 / 10',
        'team_formula': 'minutes_x10 / 10',
        'opponents_formula': None,
    },
    
    'pace': {
        'display_name': 'Pac',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rates',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'possessions / minutes',
        'team_formula': 'possessions / minutes',
        'opponents_formula': None,
    },
    
    'points': {
        'display_name': 'PTS',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': '(2fgm * 2) + (3fgm * 3) + ftm',
        'team_formula': '(2fgm * 2) + (3fgm * 3) + ftm',
        'opponents_formula': '(opp_2fgm * 2) + (opp_3fgm * 3) + opp_ftm',
    },
    
    'true_points_per_shot_attempt': {
        'display_name': 'TPS',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': 'points / (2fga + 3fga + 0.44 * fta)',
        'team_formula': 'points / (2fga + 3fga + 0.44 * fta)',
        'opponents_formula': 'points / (opp_2fga + opp_3fga + 0.44 * opp_fta)',
    },
    
    '2fga': {
        'display_name': '2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'basic',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': '2fga',
        'team_formula': '2fga',
        'opponents_formula': 'opp_2fga',
    },
    
    'Points_Per_Two_attempt': {
        'display_name': 'P2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'basic',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2 * (2fgm / 2fga)',
        'team_formula': '2 * (2fgm / 2fga)',
        'opponents_formula': '2 * (opp_2fgm / opp_2fga)',
    },
    
    '3fga': {
        'display_name': '3A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'basic',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': '3fga',
        'team_formula': '3fga',
        'opponents_formula': 'opp_3fga',
    },
    
    'Points_Per_Three_attempt': {
        'display_name': 'P3',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'basic',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '3 * (3fgm / 3fga)',
        'team_formula': '3 * (3fgm / 3fga)',
        'opponents_formula': '3 * (opp_3fgm / opp_3fga)',
    },
    
    'cont_close_2fga': {
        'display_name': 'CC2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'cont_close_2fga',
        'team_formula': 'cont_close_2fga',
        'opponents_formula': None,
    },
    
    'Points_Per_cont_close_2fga': {
        'display_name': 'PCC2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2 * (cont_close_2fgm / cont_close_2fga)',
        'team_formula': '2 * (cont_close_2fgm / cont_close_2fga)',
        'opponents_formula': None,
    },
    
    'open_close_2fga': {
        'display_name': 'OC2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'open_close_2fga',
        'team_formula': 'open_close_2fga',
        'opponents_formula': None,
    },
    
    'Points_Per_open_close_2fga': {
        'display_name': 'POC2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2 * (open_close_2fgm / open_close_2fga)',
        'team_formula': '2 * (open_close_2fgm / open_close_2fga)',
        'opponents_formula': None,
    },
    
    'cont_long_2fga': {
        'display_name': 'CL2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'cont_2fga - cont_close_2fga',
        'team_formula': 'cont_2fga - cont_close_2fga',
        'opponents_formula': None,
    },
    
    'points_per_cont_long_2fga': {
        'display_name': 'PCL2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2 * ((cont_2fgm - cont_close_2fgm) / (cont_2fga - cont_close_2fga))',
        'team_formula': '2 * ((cont_2fgm - cont_close_2fgm) / (cont_2fga - cont_close_2fga))',
        'opponents_formula': None,
    },
    
    'open_long_2fga': {
        'display_name': 'OL2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'open_2fga - open_close_2fga',
        'team_formula': 'open_2fga - open_close_2fga',
        'opponents_formula': None,
    },
    
    'points_per_open_long_2fga': {
        'display_name': 'POL2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2* ((open_2fgm - open_close_2fgm) / (open_2fga - open_close_2fga))',
        'team_formula': '2* ((open_2fgm - open_close_2fgm) / (open_2fga - open_close_2fga))',
        'opponents_formula': None,
    },
    
    'cont_3fga': {
        'display_name': 'C3A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'cont_3fga',
        'team_formula': 'cont_3fga',
        'opponents_formula': None,
    },
    
    'points_per_cont_3fga': {
        'display_name': 'PC3',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '3 * (cont_3fgm / cont_3fga)',
        'team_formula': '3 * (cont_3fgm / cont_3fga)',
        'opponents_formula': None,
    },
    
    'open_3fga': {
        'display_name': 'O3A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'open_3fga',
        'team_formula': 'open_3fga',
        'opponents_formula': None,
    },
    
    'points_per_open_3fga': {
        'display_name': 'PO3',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '3 * (open_3fgm / open_3fga)',
        'team_formula': '3 * (open_3fgm / open_3fga)',
        'opponents_formula': None,
    },
    
    'free_throw_rate': {
        'display_name': 'FTR',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'percentage',
        'decimal_places': 1,
        'totals_config': {
            'display_name': 'FTA',
            'format': 'number',
            'decimal_places': 1,
            'player_formula': 'fta',
            'team_formula': 'fta',
            'opponents_formula': 'opp_fta',
        },
        'player_formula': 'fta / (2fga + 3fga)',
        'team_formula': 'fta / (2fga + 3fga)',
        'opponents_formula': 'opp_fta / (opp_2fga + opp_3fga)',
    },
    
    'points_per_fta': {
        'display_name': 'PFT',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'scoring',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': 'ftm / fta',
        'team_formula': 'ftm / fta',
        'opponents_formula': 'opp_ftm / opp_fta',
    },
    
    'assists': {
        'display_name': 'AST',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'assists',
        'team_formula': 'assists',
        'opponents_formula': 'opp_assists',
    },
    
    'potential_assists': {
        'display_name': 'PAST',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'pot_assists',
        'team_formula': 'pot_assists',
        'opponents_formula': None,
    },
    
    'secondary_assists': {
        'display_name': '2AST',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'sec_assists',
        'team_formula': 'sec_assists',
        'opponents_formula': None,
    },
    
    'passes': {
        'display_name': 'Pas',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'passes',
        'team_formula': 'passes',
        'opponents_formula': None,
    },
    
    'touches': {
        'display_name': 'Tou',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'touches',
        'team_formula': 'touches',
        'opponents_formula': None,
    },
    
    'time_on_ball': {
        'display_name': 'TOB',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'time_on_ball',
        'team_formula': 'time_on_ball',
        'opponents_formula': None,
    },
    
    'turnovers': {
        'display_name': 'TOV',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'distribution',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'turnovers',
        'team_formula': 'turnovers',
        'opponents_formula': 'opp_turnovers',
    },
    
    'oreb_pct': {
        'display_name': 'OR%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'percentage',
        'decimal_places': 1,
        'totals_config': {
            'display_name': 'OREB',
            'format': 'number',
            'decimal_places': 1,
            'player_formula': 'o_rebounds',
            'team_formula': 'o_rebounds',
            'opponents_formula': 'opp_o_rebounds',
        },
        'player_formula': 'o_rebound_pct_x1000 / 1000',
        'team_formula': 'o_rebound_pct_x1000 / 1000',
        'opponents_formula': 'opp_o_rebound_pct_x1000 / 1000',
    },
    
    'dreb_pct': {
        'display_name': 'DR%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'percentage',
        'totals_config': {
            'display_name': 'DRS',
            'format': 'number',
            'decimal_places': 1,
            'player_formula': 'd_rebounds',
            'team_formula': 'd_rebounds',
            'opponents_formula': 'opp_d_rebounds',
        },
        'player_formula': 'd_rebound_pct_x1000 / 1000',
        'team_formula': 'd_rebound_pct_x1000 / 1000',
        'opponents_formula': 'opp_d_rebound_pct_x1000 / 1000',
    },
    
    'cont_oreb_pct': {
        'display_name': 'COR%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'percentage',
        'decimal_places': 1,
        'totals_config': {
            'display_name': 'COR',
            'format': 'number',
            'decimal_places': 1,
            'player_formula': 'cont_o_rebs',
            'team_formula': 'cont_o_rebs',
            'opponents_formula': None,
        },
        'player_formula': 'cont_o_rebs / o_rebounds',
        'team_formula': 'cont_o_rebs / o_rebounds',
        'opponents_formula': None,
    },
    
    'cont_dreb_pct': {
        'display_name': 'Cont DREB%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'percentage',
        'decimal_places': 1,
        'totals_config': {
            'display_name': 'CDR',
            'format': 'number',
            'decimal_places': 1,
            'player_formula': 'cont_d_rebs',
            'team_formula': 'cont_d_rebs',
            'opponents_formula': None,
        },
        'player_formula': 'cont_d_rebs / d_rebounds',
        'team_formula': 'cont_d_rebs / d_rebounds',
        'opponents_formula': None,
    },
    
    'putbacks': {
        'display_name': 'Putbacks',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'rebounding',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'putbacks / o_rebounds',
        'team_formula': 'putbacks / o_rebounds',
        'opponents_formula': None,
    },
    
    'off_distance': {
        'display_name': 'Off Dist',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'movement',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'o_dist_x10 / 10',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'def_distance': {
        'display_name': 'Def Dist',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'movement',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'd_dist_x10 / 10',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'def_close_2fga': {
        'display_name': 'DC2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'd_close_2fga',
        'team_formula': 'd_close_2fga',
        'opponents_formula': None,
    },
    
    'points_per_def_close_2fga': {
        'display_name': 'PDC2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2 * (d_close_2fgm / d_close_2fga)',
        'team_formula': '2 * (d_close_2fgm / d_close_2fga)',
        'opponents_formula': None,
    },
    
    'def_long_2fga': {
        'display_name': 'DL2A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'd_2fga - d_close_2fga',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'points_per_def_long_2fga': {
        'display_name': 'PDL2',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '2 * ((d_2fgm - d_close_2fgm) / (d_2fga - d_close_2fga))',
        'team_formula': '2 * ((d_2fgm - d_close_2fgm) / (d_2fga - d_close_2fga))',
        'opponents_formula': None,
    },
    
    'def_3fga': {
        'display_name': 'D3A',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'd_3fga',
        'team_formula': 'd_3fga',
        'opponents_formula': None,
    },
    
    'points_per_def_3fga': {
        'display_name': 'PD3',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 2,
        'player_formula': '3 * (d_3fgm / d_3fga)',
        'team_formula': '3 * (d_3fgm / d_3fga)',
        'opponents_formula': None,
    },
    
    'real_def_pct': {
        'display_name': 'RD%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'percentage',
        'decimal_places': 1,
        'player_formula': 'real_d_fg_pct_x1000 / 1000',
        'team_formula': 'real_d_fg_pct_x1000 / 1000',
        'opponents_formula': None,
    },
    
    'block_pct': {
        'display_name': 'Blk%',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'percentage',
        'decimal_places': 1,
        'totals_config': {
            'display_name': 'BLK',
            'format': 'number',
            'decimal_places': 1,
            'player_formula': 'blocks',
            'team_formula': 'blocks',
            'opponents_formula': 'opp_blocks',
        },
        'player_formula': 'blocks / contests',
        'team_formula': 'blocks / (opp_2fga + opp_3fga)',
        'opponents_formula': 'opp_blocks / (2fga + 3fga)',
    },
    
    'contests': {
        'display_name': 'Contests',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'contests',
        'team_formula': 'contests',
        'opponents_formula': 'opp_contests',
    },
    
    'steals_plus_charges': {
        'display_name': 'TOVF',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'steals + charges',
        'team_formula': 'steals + charges',
        'opponents_formula': None,
    },
    
    'deflections': {
        'display_name': 'Deflections',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'advanced',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'deflections',
        'team_formula': 'deflections',
        'opponents_formula': 'opp_deflections',
    },
    
    'fouls': {
        'display_name': 'Fouls',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'defense',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'fouls',
        'team_formula': 'fouls',
        'opponents_formula': 'opp_fouls',
    },
    
    'off_rating': {
        'display_name': 'ORT',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'off_rating_x10 / 10',
        'team_formula': 'off_rating_x10 / 10',
        'opponents_formula': None,
    },
    
    'def_rating': {
        'display_name': 'DRT',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': True,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'def_rating_x10 / 10',
        'team_formula': 'def_rating_x10 / 10',
        'opponents_formula': None,
    },
    
    'off_onoff': {
        'display_name': 'OO/O',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'off_onoff_x10 / 10',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    'def_onoff': {
        'display_name': 'DO/O',
        'section': ['current_stats', 'historical_stats', 'postseason_stats'],
        'subsection': 'onoff',
        'stat_mode': 'both',
        'has_percentile': True,
        'is_stat': True,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 1,
        'player_formula': 'def_onoff_x10 / 10',
        'team_formula': None,
        'opponents_formula': None,
    },
    
    # ========================================================================
    # IDENTITY SECTION
    # ========================================================================
    
    'nba_id': {
        'display_name': 'NBA ID',
        'section': ['identity'],
        'subsection': None,
        'stat_mode': 'both',
        'has_percentile': False,
        'is_stat': False,
        'editable': False,
        'reverse_percentile': False,
        'format': 'number',
        'decimal_places': 0,
        'player_formula': 'player_id',
        'team_formula': 'team_id',
        'opponents_formula': None,
    },
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_display_columns_by_section(section):
    """Get all display columns for a specific section."""
    return {k: v for k, v in DISPLAY_COLUMNS.items() if v['section'] == section}


def get_display_columns_by_view(stat_mode):
    """
    Get display columns for a specific view mode.
    Args:
        stat_mode: 'basic', 'advanced', or 'both'
    """
    if stat_mode == 'both':
        return DISPLAY_COLUMNS
    return {k: v for k, v in DISPLAY_COLUMNS.items() 
            if v['stat_mode'] in [stat_mode, 'both']}


def get_display_columns_by_entity(entity_type):
    """Get display columns applicable to an entity type."""
    result = {}
    for k, v in DISPLAY_COLUMNS.items():
        # Check if the entity has a formula (not None)
        if entity_type == 'player' and v.get('player_formula') is not None:
            result[k] = v
        elif entity_type == 'team' and v.get('team_formula') is not None:
            result[k] = v
        elif entity_type == 'opponents' and v.get('opponents_formula') is not None:
            result[k] = v
    return result


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
            'display_name': f"{col_def['display_name']}%",
            'section': col_def['section'],  # Same sections as base column
            'subsection': col_def.get('subsection'),
            'stat_mode': col_def['stat_mode'],  # Same visibility rules
            'has_percentile': False,  # Percentiles don't have percentiles
            'is_stat': col_def.get('is_stat', False),
            'editable': False,  # Percentiles are never editable
            'reverse_percentile': col_def.get('reverse_percentile', False),
            'format': 'number',  # Percentile itself is already 0-100
            'decimal_places': 0,  # Percentiles shown as whole numbers
            'calculation_formula': f"percentile({col_key})",
            'is_generated_percentile': True,
            'base_stat': col_key,
            'player_formula': col_def.get('player_formula'),
            'team_formula': col_def.get('team_formula'),
            'opponents_formula': col_def.get('opponents_formula'),
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

def get_columns_by_filters(section=None, subsection=None, entity=None, stat_mode=None, 
                           include_percentiles=False):
    """
    Get columns matching specified filters.
    
    Args:
        section: Filter by section (e.g., 'current_stats', 'player_info')
        subsection: Filter by subsection (e.g., 'scoring', 'defense')
        entity: Filter by entity type ('player', 'team', 'opponents')
        stat_mode: Filter by view mode ('basic', 'advanced', 'both')
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
        
        # Check entity filter - check if entity has a formula (not None)
        if entity:
            formula_key = f'{entity}_formula'
            if col_def.get(formula_key) is None:
                continue
        
        # Check view mode filter
        if stat_mode:
            col_view = col_def.get('stat_mode', 'both')
            if col_view != 'both' and col_view != stat_mode:
                continue
        
        filtered[col_key] = col_def
    
    return filtered


def get_columns_for_section_and_entity(section, entity, stat_mode='both', include_percentiles=False):
    """
    Get all columns for a specific section and entity combination.
    This is the primary function used when building sheet columns.
    
    Args:
        section: Section name ('current_stats', 'historical_stats', etc.)
        entity: Entity type ('player', 'team', 'opponents')
        stat_mode: View mode filter ('basic', 'advanced', 'both')
        include_percentiles: Whether to include percentile columns
    
    Returns:
        List of column definitions in display order
    """
    columns = get_columns_by_filters(
        section=section,
        entity=entity,
        stat_mode=stat_mode,
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


def build_sheet_columns(entity='player', stat_mode='both', show_percentiles=False):
    """
    Build complete column structure for a sheet.
    
    Args:
        entity: Entity type ('player', 'team', 'opponents')
        stat_mode: View mode ('basic', 'advanced', 'both')
        show_percentiles: Whether percentile columns should be visible (vs value columns)
    
    Returns:
        List of tuples: (column_key, column_def, is_percentile)
    """
    all_columns = []
    
    for section in SECTIONS:
        section_cols = get_columns_for_section_and_entity(
            section=section,
            entity=entity,
            stat_mode=stat_mode,
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

def calculate_stat_value(entity_data, col_def, entity_type='player', stats_mode='per_100_poss'):
    """
    Calculate a single stat value based on column definition and stats mode.
    
    Args:
        entity_data: Dict with raw data from database
        col_def: Column definition from DISPLAY_COLUMNS
        entity_type: 'player', 'team', or 'opponents'
        stats_mode: 'totals', 'per_game', 'per_36', 'per_100_poss', etc.
    
    Returns:
        Calculated stat value
    """
    # Get the formula for this entity type
    formula_key = f'{entity_type}_formula'
    formula = col_def.get(formula_key)
    
    if formula is None:
        return 0
    
    # Handle totals mode override (e.g., OREB% becomes OREB count)
    if stats_mode == 'totals' and col_def.get('db_field_totals'):
        db_field = col_def['db_field_totals']
        return entity_data.get(db_field, 0)
    
    # If formula is a simple field name (no operators), just get the value
    if formula and not any(op in formula for op in ['+', '-', '*', '/', '(', ')']):
        raw_value = entity_data.get(formula, 0)
        if raw_value is None:
            raw_value = 0
    else:
        # Parse and evaluate formula
        try:
            # Build local namespace with entity data
            local_vars = dict(entity_data)
            local_vars['STAT_CONSTANTS'] = STAT_CONSTANTS
            
            # Evaluate formula
            raw_value = eval(formula, {"__builtins__": {}}, local_vars)
        except Exception as e:
            return 0
    
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
        if col_def.get('format') != 'percentage':
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
    if col_def.get('format') == 'percentage':
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

