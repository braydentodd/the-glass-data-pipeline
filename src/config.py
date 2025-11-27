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
        name VARCHAR(50),
        team_id INTEGER REFERENCES teams(team_id),
        jersey_number VARCHAR(3),
        height_inches INTEGER,
        weight_lbs INTEGER,
        wingspan_inches INTEGER,
        years_experience INTEGER,
        pre_nba_team VARCHAR(100),
        contract_summary TEXT,
        birthdate DATE,
        skin_color VARCHAR(15),
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        off_rebounds INTEGER DEFAULT 0,
        def_rebounds INTEGER DEFAULT 0,
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
    
    -- Add rebound columns if they don't exist (migration)
    DO $$ 
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='player_season_stats' AND column_name='off_rebounds') THEN
            ALTER TABLE player_season_stats ADD COLUMN off_rebounds INTEGER DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='player_season_stats' AND column_name='def_rebounds') THEN
            ALTER TABLE player_season_stats ADD COLUMN def_rebounds INTEGER DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='team_season_stats' AND column_name='off_rebounds') THEN
            ALTER TABLE team_season_stats ADD COLUMN off_rebounds INTEGER DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name='team_season_stats' AND column_name='def_rebounds') THEN
            ALTER TABLE team_season_stats ADD COLUMN def_rebounds INTEGER DEFAULT 0;
        END IF;
    END $$;
    
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

def get_season_type_from_env():
    """Parse SEASON_TYPE from environment - handles comma-separated values for postseason (2,3)"""
    season_type_str = os.getenv('SEASON_TYPE', '1')
    # If comma-separated (e.g., '2,3'), return the first value as int
    # Otherwise just return the single value as int
    return int(season_type_str.split(',')[0].strip())

NBA_CONFIG = {
    'current_season_year': get_current_season_year(),
    'current_season': get_current_season(),
    'season_type': get_season_type_from_env(),  # 1 = Regular Season, 2 = Playoffs
    'api_rate_limit_delay': float(os.getenv('API_RATE_LIMIT_DELAY', '0.6')),  # seconds between API calls
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

# Postseason stat columns (same structure as historical, different column range)
# Includes both playoffs (season_type=2) and play-in games (season_type=3)
POSTSEASON_STAT_COLUMNS = {
    'years': 43,       # Column AR (YRS) - count of postseason seasons played
    'games': 44,       # Column AS (GMS)
    'minutes': 45,     # Column AT (Min)
    'points': 46,      # Column AU (Pts)
    'ts_pct': 47,      # Column AV (TS%)
    'fg2a': 48,        # Column AW (2PA)
    'fg2_pct': 49,     # Column AX (2P%)
    'fg3a': 50,        # Column AY (3PA)
    'fg3_pct': 51,     # Column AZ (3P%)
    'fta': 52,         # Column BA (FTA)
    'ft_pct': 53,      # Column BB (FT%)
    'assists': 54,     # Column BC (Ast)
    'turnovers': 55,   # Column BD (Tov) - reversed (lower is better)
    'oreb_pct': 56,    # Column BE (OR%)
    'dreb_pct': 57,    # Column BF (DR%)
    'steals': 58,      # Column BG (Stl)
    'blocks': 59,      # Column BH (Blk)
    'fouls': 60,       # Column BI (Fls) - reversed (lower is better)
}

# Player ID column - hidden at end after all stats
PLAYER_ID_COLUMN = 61  # Column BJ - hidden player_id for onEdit lookups (moved to end after postseason section)

# Stats where lower values are better (will use reversed color scale)
REVERSE_STATS = {'turnovers', 'fouls'}

# ============================================================================
# SECTION-BASED CONFIGURATION (CENTRALIZED)
# ============================================================================
# This is the single source of truth for all stat sections in the sheets.
# All column ranges, headers, and behaviors are defined here.

SECTIONS = {
    'player_info': {
        'name': 'Player Info',
        'columns': {
            'start': 0,  # Column A
            'end': 8,    # Column H (inclusive)
            'count': 8
        },
        'fields': ['name', 'jersey_number', 'experience', 'age', 'height', 'wingspan', 'weight', 'notes'],
        'merge_header': True,  # Merge B-G for "Player Info"
        'merge_range': {'start': 1, 'end': 7},  # Columns B-G (0-indexed: 1-6)
        'notes_header': True,  # Column H has separate "Notes" header
        'header_placeholder': '{team_name}',  # Row 1 header (Column A)
        'player_info_text': 'Player Info',  # Text for merged B-G header
        'notes_text': 'Notes',  # Text for Column H header
        'resize_rules': {
            'name': {'width': 187, 'fixed': True},  # Column A always 187px
            'jersey_number': {'width': 22, 'fixed': True},  # Column B always 22px
        },
        'auto_resize': True,  # Auto-resize other player info columns
        'auto_resize_start': 2,  # Start from column C (experience)
        'auto_resize_end': 8,    # End at column H (notes) - inclusive
    },
    
    'current': {
        'name': 'Current Season Stats',
        'columns': {
            'start': 8,   # Column I
            'end': 24,    # Column Y (index 24, last current stat column)
            'count': 17
        },
        'season_type': 1,  # Regular season
        'include_current': True,
        'stats': ['games', 'minutes', 'points', 'ts_pct', 'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct', 
                  'fta', 'ft_pct', 'assists', 'turnovers', 'oreb_pct', 'dreb_pct', 'steals', 'blocks', 'fouls'],
        'merge_header': True,
        'header_placeholder': '{season}',  # Replaced with "2025-26 Stats Per 36 Mins" by default
        'default_visible': True,
        'default_stats_mode': 'per_36',  # Default to per 36 mins
        'has_percentiles': True,
        'has_border': True,  # Section has borders on first and last columns
        'border_config': {
            'first_column_left': True,   # Left border on first column only
            'last_column_right': True,   # Right border on last column only
            'weight': 2,
            'header_color': 'white',  # Top 2 rows
            'data_color': 'black',    # Data rows
        },
        'resize_rules': {
            'games': {'width': 25, 'fixed': True},  # Column I (first stat column) - fixed 25px
        },
        'auto_resize': True,  # Auto-resize all other columns in this section
        'auto_resize_start': 9,  # Start auto-resize from column J (after games)
        'auto_resize_end': 25,   # End at column Y (last current stat)
    },
    
    'historical': {
        'name': 'Historical Stats',
        'columns': {
            'start': 25,  # Column Z
            'end': 42,    # Column AQ (index 42, last historical stat column)
            'count': 18   # Includes YRS column
        },
        'season_type': 1,  # Regular season
        'include_current': True,  # Include current season by default
        'stats': ['years', 'games', 'minutes', 'points', 'ts_pct', 'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct',
                  'fta', 'ft_pct', 'assists', 'turnovers', 'oreb_pct', 'dreb_pct', 'steals', 'blocks', 'fouls'],
        'merge_header': True,
        'header_placeholder': '{historical_years}',  # Replaced with "Career Stats Per 36 Mins" by default
        'default_visible': True,
        'default_mode': 'career',  # 'years', 'seasons', or 'career'
        'default_years': 25,  # For career mode
        'default_include_current': True,  # Include current season by default
        'default_stats_mode': 'per_36',  # Default to per 36 mins
        'has_percentiles': True,
        'has_border': True,  # Section has borders on first and last columns
        'border_config': {
            'first_column_left': True,   # Left border on first column only
            'last_column_right': True,   # Right border on last column only
            'weight': 2,
            'header_color': 'white',  # Top 2 rows
            'data_color': 'black',    # Data rows
        },
        'resize_rules': {
            'years': {'width': 25, 'fixed': True},  # First column - fixed 25px
            'games': {'width': 60, 'fixed': False},  # Auto-fit
            'minutes': {'width': 60, 'fixed': False},
            'points': {'width': 60, 'fixed': False},
            'ts_pct': {'width': 60, 'fixed': False},
            'fg2a': {'width': 60, 'fixed': False},
            'fg2_pct': {'width': 60, 'fixed': False},
            'fg3a': {'width': 60, 'fixed': False},
            'fg3_pct': {'width': 60, 'fixed': False},
            'fta': {'width': 60, 'fixed': False},
            'ft_pct': {'width': 60, 'fixed': False},
            'assists': {'width': 60, 'fixed': False},
            'turnovers': {'width': 60, 'fixed': False},
            'oreb_pct': {'width': 60, 'fixed': False},
            'dreb_pct': {'width': 60, 'fixed': False},
            'steals': {'width': 60, 'fixed': False},
            'blocks': {'width': 60, 'fixed': False},
            'fouls': {'width': 60, 'fixed': False},
        },
        'auto_resize': True,  # Auto-resize all columns except YRS
        'auto_resize_start': 26,  # Start from column AA (first stat after YRS)
        'auto_resize_end': 43,    # End at column AQ (last historical stat) - inclusive
    },
    
    'postseason': {
        'name': 'Postseason Stats',
        'columns': {
            'start': 43,  # Column AR
            'end': 60,    # Column BI (index 60, last postseason stat column)
            'count': 18   # Includes YRS column
        },
        'season_type': [2, 3],  # Playoffs (2) + Play-in (3)
        'include_current': True,  # Include current season postseason by default
        'stats': ['years', 'games', 'minutes', 'points', 'ts_pct', 'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct',
                  'fta', 'ft_pct', 'assists', 'turnovers', 'oreb_pct', 'dreb_pct', 'steals', 'blocks', 'fouls'],
        'merge_header': True,
        'header_placeholder': '{postseason_years}',  # Replaced with "Postseason Stats Per 36 Mins" by default
        'default_visible': True,
        'default_mode': 'career',  # 'years', 'seasons', or 'career'
        'default_years': 25,  # For career mode
        'default_include_current': True,  # Include current season by default
        'default_stats_mode': 'per_36',  # Default to per 36 mins
        'has_percentiles': True,
        'has_border': True,  # Section has borders on first and last columns
        'border_config': {
            'first_column_left': True,   # Left border on first column only
            'last_column_right': True,   # Right border on last column only
            'weight': 2,
            'header_color': 'white',  # Top 2 rows
            'data_color': 'black',    # Data rows
        },
        'resize_rules': {
            'years': {'width': 25, 'fixed': True},  # Column AR (first column of postseason section, NOT HIDDEN)
            'games': {'width': 60, 'fixed': False},  # Auto-fit all postseason columns
            'minutes': {'width': 60, 'fixed': False},
            'points': {'width': 60, 'fixed': False},
            'ts_pct': {'width': 60, 'fixed': False},
            'fg2a': {'width': 60, 'fixed': False},
            'fg2_pct': {'width': 60, 'fixed': False},
            'fg3a': {'width': 60, 'fixed': False},
            'fg3_pct': {'width': 60, 'fixed': False},
            'fta': {'width': 60, 'fixed': False},
            'ft_pct': {'width': 60, 'fixed': False},
            'assists': {'width': 60, 'fixed': False},
            'turnovers': {'width': 60, 'fixed': False},
            'oreb_pct': {'width': 60, 'fixed': False},
            'dreb_pct': {'width': 60, 'fixed': False},
            'steals': {'width': 60, 'fixed': False},
            'blocks': {'width': 60, 'fixed': False},
            'fouls': {'width': 60, 'fixed': False},
        },
        'auto_resize': True,  # Auto-resize all columns except YRS
        'auto_resize_start': 44,  # Start from column AS (first stat after YRS)
        'auto_resize_end': 61,    # End at column BI (last postseason stat) - inclusive
    },
    
    'hidden': {
        'name': 'Hidden Fields',
        'columns': {
            'start': 61,  # Column BJ
            'end': 62,    # Column BJ (inclusive)
            'count': 1
        },
        'fields': ['player_id'],
        'merge_header': False,
        'default_visible': False,  # Always hidden
    }
}

# Helper function to get column letter from index
def get_column_letter(index):
    """Convert 0-based column index to Excel-style letter (0=A, 25=Z, 26=AA, etc.)"""
    letter = ''
    index += 1  # Convert to 1-based
    while index > 0:
        index -= 1
        letter = chr(65 + (index % 26)) + letter
        index //= 26
    return letter

# Generate column ranges for easy reference
for section_name, section in SECTIONS.items():
    if 'columns' in section:
        cols = section['columns']
        cols['start_letter'] = get_column_letter(cols['start'])
        cols['end_letter'] = get_column_letter(cols['end'] - 1)  # end is exclusive
        cols['range'] = f"{cols['start_letter']}-{cols['end_letter']}"

# Player ID column - hidden at end after all stats
# Legacy column mappings for backward compatibility
PLAYOFF_STAT_COLUMNS = POSTSEASON_STAT_COLUMNS  # Alias for backward compatibility
PLAYER_ID_COLUMN_OLD = 43  # OLD location - keep for migration reference

# For totals mode, use raw rebound counts instead of percentages
# Maps the percentage key to the raw count key
TOTALS_MODE_REPLACEMENTS = {
    'oreb_pct': 'off_rebounds',  # Use raw offensive rebound count
    'dreb_pct': 'def_rebounds',  # Use raw defensive rebound count  
}

# Column headers for totals mode
TOTALS_MODE_HEADERS = {
    'off_rebounds': 'ORS',  # Offensive Rebounds
    'def_rebounds': 'DRS',  # Defensive Rebounds
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

# Default stats mode for all sections
DEFAULT_STATS_MODE = 'per_36'

# Stats mode display names
STATS_MODE_DISPLAY = {
    'totals': 'Totals',
    'per_game': 'Per Game',
    'per_36': 'Per 36 Mins',
    'per_100_poss': 'Per 100 Poss',
    'per_minutes': 'Per {value} Mins',  # {value} replaced with custom_value
    'per_possessions': 'Per {value} Poss',  # {value} replaced with custom_value
}

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
        'jersey_number': 22,  # Column B - 22px
        'games': 25,          # Column I (GMS) - first stat column 25px
        'years': 25,          # Column Z (YRS) and Column AR (playoff YRS) - 25px
    },
    'frozen': {
        'rows': 3,      # Freeze first 3 rows (headers + filter)
        'columns': 1,   # Freeze first column (Name)
    },
    'header_rows': 2,  # Number of header rows (for border config)
    'total_columns': sum(s['columns']['count'] for s in SECTIONS.values()),  # Calculated from sections
}

# Default settings for historical stats
HISTORICAL_STATS_CONFIG = {
    'default_past_years': 3,  # Default number of past seasons to show
    'display_mode': 'values',  # 'values' or 'percentiles'
}

# Column headers - Generated dynamically from SECTIONS
def generate_headers():
    """Generate header rows from SECTIONS configuration"""
    total_cols = SHEET_FORMAT['total_columns']
    row_1 = [''] * total_cols
    row_2 = [''] * total_cols
    
    for section_name, section in SECTIONS.items():
        cols = section['columns']
        start = cols['start']
        
        if section_name == 'player_info':
            # Column A (0): Team name placeholder
            row_1[start] = section.get('header_placeholder', '')
            # Columns B-G (1-6): "Player Info" (will be merged)
            row_1[start + 1] = section.get('player_info_text', 'Player Info')
            # Column H (7): "Notes"
            row_1[start + 7] = section.get('notes_text', 'Notes')
            # Player info sub-headers in row 2
            row_2[start:start+8] = ['Name', 'J#', 'Exp', 'Age', 'Ht', 'W/S', 'Wt', '*Double click cells to expand*']
            
        elif section_name in ['current', 'historical', 'postseason']:
            # Merged header cell placeholder
            row_1[start] = section.get('header_placeholder', '')
            
            # Stat column headers
            stat_headers = []
            for stat in section['stats']:
                if stat == 'years':
                    stat_headers.append('YRS')
                elif stat == 'games':
                    stat_headers.append('GMS')
                elif stat == 'minutes':
                    stat_headers.append('Min')
                elif stat == 'points':
                    stat_headers.append('Pts')
                elif stat == 'ts_pct':
                    stat_headers.append('TS%')
                elif stat == 'fg2a':
                    stat_headers.append('2PA')
                elif stat == 'fg2_pct':
                    stat_headers.append('2P%')
                elif stat == 'fg3a':
                    stat_headers.append('3PA')
                elif stat == 'fg3_pct':
                    stat_headers.append('3P%')
                elif stat == 'fta':
                    stat_headers.append('FTA')
                elif stat == 'ft_pct':
                    stat_headers.append('FT%')
                elif stat == 'assists':
                    stat_headers.append('Ast')
                elif stat == 'turnovers':
                    stat_headers.append('Tov')
                elif stat == 'oreb_pct':
                    stat_headers.append('OR%')
                elif stat == 'dreb_pct':
                    stat_headers.append('DR%')
                elif stat == 'steals':
                    stat_headers.append('Stl')
                elif stat == 'blocks':
                    stat_headers.append('Blk')
                elif stat == 'fouls':
                    stat_headers.append('Fls')
            row_2[start:start+len(stat_headers)] = stat_headers
            
        elif section_name == 'hidden':
            row_2[start] = 'Player ID'
    
    return {'row_1': row_1, 'row_2': row_2}

HEADERS = generate_headers()

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

LOGGING_CONFIG = {
    'format': '[%(asctime)s] %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'level': os.getenv('LOG_LEVEL', 'INFO'),
}

# ============================================================================
# CONFIG EXPORT FOR APPS SCRIPT
# ============================================================================

def get_config_for_apps_script():
    """
    Export configuration in format suitable for Apps Script consumption.
    This ensures Apps Script and Python stay in sync.
    """
    # Convert NBA_TEAMS list of tuples to dictionary for Apps Script
    nba_teams_dict = {abbr: team_id for abbr, team_id in [(abbr, NBA_TEAMS_BY_ID[[k for k, v in NBA_TEAMS_BY_ID.items() if v == name][0]]) for abbr, name in NBA_TEAMS]}
    # Simpler: just create abbr->id mapping directly
    nba_teams_dict = {abbr: [tid for tid, tname in NBA_TEAMS_BY_ID.items() if tname == name][0] for abbr, name in NBA_TEAMS}
    
    return {
        'api_base_url': f"http://{SERVER_CONFIG['production_host']}:{SERVER_CONFIG['production_port']}",
        'sheet_id': GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
        'nba_teams': nba_teams_dict,
        'stat_columns': list(STAT_COLUMNS.keys()),
        'reverse_stats': list(REVERSE_STATS),
        'stats_mode_display': STATS_MODE_DISPLAY,
        'sections': {
            name: {
                'name': section['name'],
                'columns': section['columns'],
                'default_visible': section.get('default_visible', True),
                'has_percentiles': section.get('has_percentiles', False),
                'stats': section.get('stats', []),
            }
            for name, section in SECTIONS.items()
        },
        'column_indices': {
            'wingspan': SECTIONS['player_info']['columns']['start'] + 5,  # Column F (W/S)
            'notes': SECTIONS['player_info']['columns']['start'] + 7,      # Column H (Notes)
            'player_id': SECTIONS['hidden']['columns']['start'],            # Column BJ (Player ID)
            'stats_start': SECTIONS['current']['columns']['start'],         # Column I (first stat column)
        },
        'colors': {
            'red': {'r': COLORS['red']['rgb']['red'], 'g': COLORS['red']['rgb']['green'], 'b': COLORS['red']['rgb']['blue']},
            'yellow': {'r': COLORS['yellow']['rgb']['red'], 'g': COLORS['yellow']['rgb']['green'], 'b': COLORS['yellow']['rgb']['blue']},
            'green': {'r': COLORS['green']['rgb']['red'], 'g': COLORS['green']['rgb']['green'], 'b': COLORS['green']['rgb']['blue']},
        },
    }
