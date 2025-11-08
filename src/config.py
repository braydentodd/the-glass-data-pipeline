"""
Centralized configuration for The Glass data pipeline.
All settings are loaded from environment variables with sensible defaults.
"""

import os
from datetime import datetime

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '150.136.255.23'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'the_glass_db'),
    'user': os.getenv('DB_USER', 'the_glass_user'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# ============================================================================
# GOOGLE SHEETS CONFIGURATION
# ============================================================================

GOOGLE_SHEETS_CONFIG = {
    'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE', 'google-credentials.json'),
    'spreadsheet_id': os.getenv('GOOGLE_SPREADSHEET_ID', '1kqVNHu8cs4lFAEAflI4Ow77oEZEusX7_VpQ6xt8CgB4'),
    'spreadsheet_name': os.getenv('GOOGLE_SPREADSHEET_NAME', 'The Glass'),
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
    'supported_modes': [
        'totals',
        'per_game', 
        'per_100',
        'per_36',
        'per_minutes',
        'per_possessions'
    ],
}

# ============================================================================
# NBA API CONFIGURATION
# ============================================================================

def get_current_season_year():
    """Calculate current NBA season year (e.g., 2025-26 season = 2026)"""
    now = datetime.now()
    return now.year + 1 if now.month >= 10 else now.year

NBA_CONFIG = {
    'current_season_year': get_current_season_year(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),  # 1 = Regular Season, 2 = Playoffs
    'api_rate_limit_delay': float(os.getenv('API_RATE_LIMIT_DELAY', '3.0')),  # seconds between API calls
}

# ============================================================================
# TEAMS CONFIGURATION
# ============================================================================

NBA_TEAMS = [
    ('ATL', 'Atlanta Hawks'),
    ('BOS', 'Boston Celtics'),
    ('BKN', 'Brooklyn Nets'),
    ('CHA', 'Charlotte Hornets'),
    ('CHI', 'Chicago Bulls'),
    ('CLE', 'Cleveland Cavaliers'),
    ('DAL', 'Dallas Mavericks'),
    ('DEN', 'Denver Nuggets'),
    ('DET', 'Detroit Pistons'),
    ('GSW', 'Golden State Warriors'),
    ('HOU', 'Houston Rockets'),
    ('IND', 'Indiana Pacers'),
    ('LAC', 'LA Clippers'),
    ('LAL', 'Los Angeles Lakers'),
    ('MEM', 'Memphis Grizzlies'),
    ('MIA', 'Miami Heat'),
    ('MIL', 'Milwaukee Bucks'),
    ('MIN', 'Minnesota Timberwolves'),
    ('NOP', 'New Orleans Pelicans'),
    ('NYK', 'New York Knicks'),
    ('OKC', 'Oklahoma City Thunder'),
    ('ORL', 'Orlando Magic'),
    ('PHI', 'Philadelphia 76ers'),
    ('PHX', 'Phoenix Suns'),
    ('POR', 'Portland Trail Blazers'),
    ('SAC', 'Sacramento Kings'),
    ('SAS', 'San Antonio Spurs'),
    ('TOR', 'Toronto Raptors'),
    ('UTA', 'Utah Jazz'),
    ('WAS', 'Washington Wizards'),
]

# Team ID to Team Name mapping (for API)
NBA_TEAMS_BY_ID = {
    1610612737: 'Atlanta Hawks',
    1610612738: 'Boston Celtics',
    1610612751: 'Brooklyn Nets',
    1610612766: 'Charlotte Hornets',
    1610612741: 'Chicago Bulls',
    1610612739: 'Cleveland Cavaliers',
    1610612742: 'Dallas Mavericks',
    1610612743: 'Denver Nuggets',
    1610612765: 'Detroit Pistons',
    1610612744: 'Golden State Warriors',
    1610612745: 'Houston Rockets',
    1610612754: 'Indiana Pacers',
    1610612746: 'LA Clippers',
    1610612747: 'Los Angeles Lakers',
    1610612763: 'Memphis Grizzlies',
    1610612748: 'Miami Heat',
    1610612749: 'Milwaukee Bucks',
    1610612750: 'Minnesota Timberwolves',
    1610612740: 'New Orleans Pelicans',
    1610612752: 'New York Knicks',
    1610612760: 'Oklahoma City Thunder',
    1610612753: 'Orlando Magic',
    1610612755: 'Philadelphia 76ers',
    1610612756: 'Phoenix Suns',
    1610612757: 'Portland Trail Blazers',
    1610612758: 'Sacramento Kings',
    1610612759: 'San Antonio Spurs',
    1610612761: 'Toronto Raptors',
    1610612762: 'Utah Jazz',
    1610612764: 'Washington Wizards',
}

# ============================================================================
# STATS CONFIGURATION
# ============================================================================

# Column indices for stats (0-indexed for Google Sheets API)
STAT_COLUMNS = {
    'games': 8,        # Column I (GM)
    'minutes': 9,      # Column J (Min)
    'points': 10,      # Column K (Pts)
    'ts_pct': 11,      # Column L (TS%)
    'fg2a': 12,        # Column M (2PA)
    'fg2_pct': 13,     # Column N (2P%)
    'fg3a': 14,        # Column O (3PA)
    'fg3_pct': 15,     # Column P (3P%)
    'fta': 16,         # Column Q (FTA)
    'ft_pct': 17,      # Column R (FT%)
    'assists': 18,     # Column S (Ast)
    'turnovers': 19,   # Column T (Tov) - reversed (lower is better)
    'oreb_pct': 20,    # Column U (OR%)
    'dreb_pct': 21,    # Column V (DR%)
    'steals': 22,      # Column W (Stl)
    'blocks': 23,      # Column X (Blk)
    'fouls': 24,       # Column Y (Fls) - reversed (lower is better)
}

# Stats where lower values are better (will use reversed color scale)
REVERSE_STATS = {'turnovers', 'fouls'}

# Percentile calculation settings
PERCENTILE_CONFIG = {
    'minutes_weight_factor': 10,  # 1 sample per X minutes played
    'min_percentile': 0,
    'max_percentile': 100,
}

# ============================================================================
# COLOR SCHEME CONFIGURATION
# ============================================================================

# Custom color scale for percentile visualization
# Colors in RGB format (0.0 to 1.0)
COLORS = {
    'red': {
        'hex': '#EE4B2B',
        'rgb': {'red': 0.933, 'green': 0.294, 'blue': 0.169}
    },
    'yellow': {
        'hex': '#FCF55F',
        'rgb': {'red': 0.988, 'green': 0.961, 'blue': 0.373}
    },
    'green': {
        'hex': '#4CBB17',
        'rgb': {'red': 0.298, 'green': 0.733, 'blue': 0.090}
    },
    'black': {
        'rgb': {'red': 0, 'green': 0, 'blue': 0}
    },
    'white': {
        'rgb': {'red': 1, 'green': 1, 'blue': 1}
    },
    'light_gray': {
        'rgb': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
    },
}

# Percentile color thresholds
COLOR_THRESHOLDS = {
    'low': 33,   # 0-33%: red to yellow gradient
    'mid': 66,   # 33-66%: yellow
    'high': 100, # 66-100%: yellow to green gradient
}

# ============================================================================
# SHEET FORMATTING CONFIGURATION
# ============================================================================

SHEET_FORMAT = {
    'fonts': {
        'header_large': {'family': 'Staatliches', 'size': 15, 'bold': True},
        'header_medium': {'family': 'Staatliches', 'size': 12, 'bold': True},
        'header_small': {'family': 'Staatliches', 'size': 10, 'bold': True},
        'data': {'family': 'Sofia Sans', 'size': 10, 'bold': False},
    },
    'column_widths': {
        'jersey_number': 22,   # Column B (J#)
        'games': 25,    # Column I (GM)
    },
    'frozen': {
        'rows': 3,      # Freeze first 3 rows (headers + filter)
        'columns': 1,   # Freeze first column (Name)
    },
    'total_columns': 25,  # A through Y
}

# Column headers
HEADERS = {
    'row_1': [
        '{team_name}', 'Player Info', '', '', '', '', '', 'Notes',
        '24-25 Stats per 100 poss', '', '', '', '', '', '', '', '', '',
        '', '', '', '', '', '', ''
    ],
    'row_2': [
        'Name', 'J#', 'Exp', 'Age', 'Ht', 'W/S', 'Wt',
        '*Double click cells to view more detailed analysis*',
        'GM', 'Min', 'Pts', 'TS%',
        '2PA', '2P%', '3PA', '3P%', 'FTA', 'FT%',
        'Ast', 'Tov', 'OR%', 'DR%', 'Stl', 'Blk', 'Fls'
    ],
}

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

LOGGING_CONFIG = {
    'format': '[%(asctime)s] %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'level': os.getenv('LOG_LEVEL', 'INFO'),
}
