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

# Database schema configuration
DB_SCHEMA = {
    # Table names
    'tables': {
        'teams': 'teams',
        'players': 'players',
        'player_season_stats': 'player_season_stats',
        'team_season_stats': 'team_season_stats',
    },
    
    # Player table columns
    'player_columns': {
        'player_id': 'player_id',
        'name': 'name',
        'team_id': 'team_id',
        'team_abbreviation': 'team_abbreviation',
        'jersey_number': 'jersey_number',
        'position': 'position',
        'height_inches': 'height_inches',
        'weight_pounds': 'weight_pounds',
        'wingspan_inches': 'wingspan_inches',
        'birthdate': 'birthdate',
        'country': 'country',
        'draft_year': 'draft_year',
        'draft_round': 'draft_round',
        'draft_number': 'draft_number',
        'school': 'school',
        'notes': 'notes',
        'created_at': 'created_at',
        'updated_at': 'updated_at',
    },
    
    # Player season stats columns
    'player_stats_columns': {
        'id': 'id',
        'player_id': 'player_id',
        'year': 'year',
        'team_id': 'team_id',
        'season_type': 'season_type',
        'games_played': 'games_played',
        'minutes_x10': 'minutes_x10',
        'possessions': 'possessions',
        'fg2m': 'fg2m',
        'fg2a': 'fg2a',
        'fg3m': 'fg3m',
        'fg3a': 'fg3a',
        'ftm': 'ftm',
        'fta': 'fta',
        'off_reb_pct_x1000': 'off_reb_pct_x1000',
        'def_reb_pct_x1000': 'def_reb_pct_x1000',
        'assists': 'assists',
        'turnovers': 'turnovers',
        'steals': 'steals',
        'blocks': 'blocks',
        'fouls': 'fouls',
        'off_rating_x10': 'off_rating_x10',
        'def_rating_x10': 'def_rating_x10',
        'created_at': 'created_at',
        'updated_at': 'updated_at',
    },
    
    # Editable player fields (for API updates)
    'editable_fields': ['wingspan_inches', 'notes'],
    
    # Database schema DDL (for auto-creation)
    'create_schema_sql': """
    -- Teams table
    CREATE TABLE IF NOT EXISTS teams (
        team_id INTEGER PRIMARY KEY,
        full_name VARCHAR(100),
        abbreviation VARCHAR(10),
        city VARCHAR(100),
        state VARCHAR(50),
        year_founded INTEGER,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Players table
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        team_id INTEGER REFERENCES teams(team_id),
        team_abbreviation VARCHAR(10),
        jersey_number VARCHAR(10),
        position VARCHAR(10),
        height_inches INTEGER,
        weight_pounds INTEGER,
        wingspan_inches INTEGER,
        birthdate DATE,
        country VARCHAR(100),
        draft_year INTEGER,
        draft_round INTEGER,
        draft_number INTEGER,
        school VARCHAR(255),
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Player season stats table
    CREATE TABLE IF NOT EXISTS player_season_stats (
        id SERIAL PRIMARY KEY,
        player_id INTEGER REFERENCES players(player_id),
        team_id INTEGER REFERENCES teams(team_id),
        year INTEGER NOT NULL,
        season_type INTEGER NOT NULL DEFAULT 1,
        games_played INTEGER DEFAULT 0,
        minutes_x10 INTEGER DEFAULT 0,
        possessions INTEGER DEFAULT 0,
        fg2m INTEGER DEFAULT 0,
        fg2a INTEGER DEFAULT 0,
        fg3m INTEGER DEFAULT 0,
        fg3a INTEGER DEFAULT 0,
        ftm INTEGER DEFAULT 0,
        fta INTEGER DEFAULT 0,
        off_reb_pct_x1000 INTEGER,
        def_reb_pct_x1000 INTEGER,
        assists INTEGER DEFAULT 0,
        turnovers INTEGER DEFAULT 0,
        steals INTEGER DEFAULT 0,
        blocks INTEGER DEFAULT 0,
        fouls INTEGER DEFAULT 0,
        off_rating_x10 INTEGER,
        def_rating_x10 INTEGER,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(player_id, year, season_type)
    );
    
    -- Team season stats table
    CREATE TABLE IF NOT EXISTS team_season_stats (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(team_id),
        year INTEGER NOT NULL,
        season_type INTEGER NOT NULL DEFAULT 1,
        games_played INTEGER DEFAULT 0,
        minutes_x10 INTEGER DEFAULT 0,
        possessions INTEGER DEFAULT 0,
        fg2m INTEGER DEFAULT 0,
        fg2a INTEGER DEFAULT 0,
        fg3m INTEGER DEFAULT 0,
        fg3a INTEGER DEFAULT 0,
        ftm INTEGER DEFAULT 0,
        fta INTEGER DEFAULT 0,
        off_reb_pct_x1000 INTEGER,
        def_reb_pct_x1000 INTEGER,
        assists INTEGER DEFAULT 0,
        turnovers INTEGER DEFAULT 0,
        steals INTEGER DEFAULT 0,
        blocks INTEGER DEFAULT 0,
        fouls INTEGER DEFAULT 0,
        off_rating_x10 INTEGER,
        def_rating_x10 INTEGER,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(team_id, year, season_type)
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_player_stats_year ON player_season_stats(year);
    CREATE INDEX IF NOT EXISTS idx_player_stats_player ON player_season_stats(player_id);
    CREATE INDEX IF NOT EXISTS idx_team_stats_year ON team_season_stats(year);
    CREATE INDEX IF NOT EXISTS idx_team_stats_team ON team_season_stats(team_id);
    """,
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

# Server/deployment configuration
SERVER_CONFIG = {
    'production_host': '150.136.255.23',
    'production_port': 5001,
    'ssh_user': 'ubuntu',
    'remote_dir': '/home/ubuntu/the-glass-data-pipeline',
    'systemd_service': 'flask-api',
}

# ============================================================================
# NBA API CONFIGURATION
# ============================================================================

def get_current_season_year():
    """Calculate current NBA season year (e.g., 2025-26 season = 2026)
    Season flips in September (month 9)"""
    now = datetime.now()
    return now.year + 1 if now.month >= 9 else now.year

def get_current_season():
    """Get current season in YYYY-YY format (e.g., 2025-26)"""
    year = get_current_season_year()
    return f"{year-1}-{str(year)[2:]}"

NBA_CONFIG = {
    'current_season_year': get_current_season_year(),
    'current_season': get_current_season(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),  # 1 = Regular Season, 2 = Playoffs
    'api_rate_limit_delay': float(os.getenv('API_RATE_LIMIT_DELAY', '3.0')),  # seconds between API calls
}

# Team IDs for API calls
TEAM_IDS = [
    1610612737,  # ATL - Atlanta Hawks
    1610612738,  # BOS - Boston Celtics
    1610612751,  # BKN - Brooklyn Nets
    1610612766,  # CHA - Charlotte Hornets
    1610612741,  # CHI - Chicago Bulls
    1610612739,  # CLE - Cleveland Cavaliers
    1610612742,  # DAL - Dallas Mavericks
    1610612743,  # DEN - Denver Nuggets
    1610612765,  # DET - Detroit Pistons
    1610612744,  # GSW - Golden State Warriors
    1610612745,  # HOU - Houston Rockets
    1610612754,  # IND - Indiana Pacers
    1610612746,  # LAC - LA Clippers
    1610612747,  # LAL - Los Angeles Lakers
    1610612763,  # MEM - Memphis Grizzlies
    1610612748,  # MIA - Miami Heat
    1610612749,  # MIL - Milwaukee Bucks
    1610612750,  # MIN - Minnesota Timberwolves
    1610612740,  # NOP - New Orleans Pelicans
    1610612752,  # NYK - New York Knicks
    1610612760,  # OKC - Oklahoma City Thunder
    1610612753,  # ORL - Orlando Magic
    1610612755,  # PHI - Philadelphia 76ers
    1610612756,  # PHX - Phoenix Suns
    1610612757,  # POR - Portland Trail Blazers
    1610612758,  # SAC - Sacramento Kings
    1610612759,  # SAS - San Antonio Spurs
    1610612761,  # TOR - Toronto Raptors
    1610612762,  # UTA - Utah Jazz
    1610612764   # WAS - Washington Wizards
]

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
    'games': 8,        # Column I (GMS)
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

# Historical stats section - starts after current stats
HISTORICAL_STAT_COLUMNS = {
    'years': 25,       # Column Z (YRS) - count of seasons played
    'games': 26,       # Column AA (GMS)
    'minutes': 27,     # Column AB (Min)
    'points': 28,      # Column AC (Pts)
    'ts_pct': 29,      # Column AD (TS%)
    'fg2a': 30,        # Column AE (2PA)
    'fg2_pct': 31,     # Column AF (2P%)
    'fg3a': 32,        # Column AG (3PA)
    'fg3_pct': 33,     # Column AH (3P%)
    'fta': 34,         # Column AI (FTA)
    'ft_pct': 35,      # Column AJ (FT%)
    'assists': 36,     # Column AK (Ast)
    'turnovers': 37,   # Column AL (Tov) - reversed (lower is better)
    'oreb_pct': 38,    # Column AM (OR%)
    'dreb_pct': 39,    # Column AN (DR%)
    'steals': 40,      # Column AO (Stl)
    'blocks': 41,      # Column AP (Blk)
    'fouls': 42,       # Column AQ (Fls) - reversed (lower is better)
}

# Player ID column - hidden at end after all stats
PLAYER_ID_COLUMN = 43  # Column AR - hidden player_id for onEdit lookups

# Stats where lower values are better (will use reversed color scale)
REVERSE_STATS = {'turnovers', 'fouls'}

# For totals mode, use raw counts instead of percentages
TOTALS_MODE_REPLACEMENTS = {
    'oreb_pct': 'ORS',  # Offensive rebounds (raw count)
    'dreb_pct': 'DRS',  # Defensive rebounds (raw count)
}

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
        'header_primary': {'family': 'Staatliches', 'size': 12, 'bold': True},
        'header_secondary': {'family': 'Staatliches', 'size': 10, 'bold': True},
        'team_name': {'family': 'Staatliches', 'size': 15, 'bold': True},
        'header_large': {'family': 'Staatliches', 'size': 15, 'bold': True},
        'header_medium': {'family': 'Staatliches', 'size': 12, 'bold': True},
        'header_small': {'family': 'Staatliches', 'size': 10, 'bold': True},
        'data': {'family': 'Sofia Sans', 'size': 10, 'bold': False},
    },
    'column_widths': {
        'jersey_number': 22,   # Column B (J#)
        'games': 25,    # Column I (GMS)
        'years': 25,    # Column AA (YRS) for historical section
    },
    'frozen': {
        'rows': 3,      # Freeze first 3 rows (headers + filter)
        'columns': 1,   # Freeze first column (Name)
    },
    'total_columns': 44,  # A through AR (AR is hidden player_id)
}

# Default settings for historical stats
HISTORICAL_STATS_CONFIG = {
    'default_past_years': 3,  # Default number of past seasons to show
    'display_mode': 'values',  # 'values' or 'percentiles'
}

# Column headers
HEADERS = {
    'row_1': [
        '{team_name}', 'Player Info', '', '', '', '', '', 'Notes',
        '{season}', '', '', '', '', '', '', '', '',
        '', '', '', '', '', '', '', '',
        '{past_years}', '', '', '', '', '', '', '', '',
        '', '', '', '', '', '', '', '', '', ''
    ],
    'row_2': [
        'Name', 'J#', 'Exp', 'Age', 'Ht', 'W/S', 'Wt',
        '*Double click cells to expand*',
        'GMS', 'Min', 'Pts', 'TS%',
        '2PA', '2P%', '3PA', '3P%', 'FTA', 'FT%',
        'Ast', 'Tov', 'OR%', 'DR%', 'Stl', 'Blk', 'Fls',
        'YRS', 'GMS', 'Min', 'Pts', 'TS%',
        '2PA', '2P%', '3PA', '3P%', 'FTA', 'FT%',
        'Ast', 'Tov', 'OR%', 'DR%', 'Stl', 'Blk', 'Fls', 'Player ID'
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
