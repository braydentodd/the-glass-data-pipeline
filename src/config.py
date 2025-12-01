"""
Centralized configuration for The Glass data pipeline.
SINGLE SOURCE OF TRUTH - All column definitions, mappings, and formatting rules.
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

# Database schema for auto-creation
DB_SCHEMA = {
    'editable_fields': ['wingspan_inches', 'notes'],
    
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
}

SERVER_CONFIG = {
    'production_host': '150.136.255.23',
    'production_port': 5001,
    'ssh_user': 'ubuntu',
    'remote_dir': '/home/ubuntu/the-glass-data-pipeline',
    'systemd_service': 'flask-api',
}

# ============================================================================
# NBA CONFIGURATION
# ============================================================================

def get_current_season_year():
    """Get current NBA season year (e.g., 2025 for 2025-26 season)"""
    now = datetime.now()
    return now.year if now.month > 8 else now.year - 1

def get_current_season():
    """Get current NBA season string (e.g., '2025-26')"""
    year = get_current_season_year()
    return f"{year}-{str(year + 1)[-2:]}"

NBA_CONFIG = {
    'current_season_year': get_current_season_year(),
    'current_season': get_current_season(),
    'season_type': int(os.getenv('SEASON_TYPE', '1')),  # 1=regular, 2=playoffs, 3=play-in
    'api_rate_limit_delay': float(os.getenv('API_RATE_LIMIT_DELAY', '0.6')),
}

# ============================================================================
# NBA TEAMS
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

# Build team ID mappings (NBA team IDs start at 1610612737)
TEAM_IDS = {abbr: 1610612737 + idx for idx, (abbr, name) in enumerate(NBA_TEAMS)}
NBA_TEAMS_BY_ID = {team_id: name for (abbr, name), team_id in zip(NBA_TEAMS, TEAM_IDS.values())}
NBA_TEAMS_BY_ABBR = {abbr: name for abbr, name in NBA_TEAMS}

# ============================================================================
# COLUMN DEFINITIONS - SINGLE SOURCE OF TRUTH
# ============================================================================
# This is THE definitive source for all column information.
# Everything else is derived from this dictionary.

COLUMN_DEFINITIONS = {
    # PLAYER INFO COLUMNS
    'name': {
        'display_name': 'NAME',
        'db_field': 'player_name',
        'width': 187,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': False,
    },
    'team': {
        'display_name': 'TEAM',
        'db_field': 'team_abbr',
        'width': 40,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': True,  # Only appears in NBA sheet
        'is_stat': False,
        'editable': False,
        'hidden': False,
    },
    'jersey': {
        'display_name': 'J#',
        'db_field': 'jersey_number',
        'width': 22,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': False,
    },
    'experience': {
        'display_name': 'EXP',
        'db_field': 'years_experience',
        'width': None,  # Auto-resize
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': False,
    },
    'age': {
        'display_name': 'AGE',
        'db_field': 'age',
        'width': None,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': False,
        'decimal_places': 1,
    },
    'height': {
        'display_name': 'HT',
        'db_field': 'height_inches',
        'width': None,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': False,
    },
    'wingspan': {
        'display_name': 'WS',
        'db_field': 'wingspan_inches',
        'width': None,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': True,  # Can be edited by user
        'hidden': False,
    },
    'weight': {
        'display_name': 'WT',
        'db_field': 'weight_lbs',
        'width': None,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': False,
    },
    'notes': {
        'display_name': 'NOTES',
        'db_field': 'notes',
        'width': None,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': True,  # Can be edited by user
        'hidden': False,
    },
    
    # STAT COLUMNS (ordered left-to-right as they appear)
    'years': {
        'display_name': 'YRS',
        'display_name_totals': 'YRS',
        'db_field': 'seasons_played',
        'width': 25,
        'in_current': False,  # NOT in current season stats
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    'games': {
        'display_name': 'GMS',
        'display_name_totals': 'GMS',
        'db_field': 'games_played',
        'width': 25,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 0,
    },
    'minutes': {
        'display_name': 'MIN',
        'display_name_totals': 'MIN',
        'db_field': 'minutes_total',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'possessions': {
        'display_name': 'POS',
        'display_name_totals': 'POS',
        'db_field': 'possessions',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'points': {
        'display_name': 'PTS',
        'display_name_totals': 'PTS',
        'db_field': None,  # Calculated from fg2m, fg3m, ftm
        'calculated': True,
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'ts_pct': {
        'display_name': 'TS%',
        'display_name_totals': 'TS%',
        'db_field': None,  # Calculated
        'calculated': True,
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': True,
        'decimal_places': 1,
    },
    'fg2a': {
        'display_name': '2PA',
        'display_name_totals': '2PA',
        'db_field': 'fg2a',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'fg2_pct': {
        'display_name': '2P%',
        'display_name_totals': '2P%',
        'db_field': None,  # Calculated from fg2m/fg2a
        'calculated': True,
        'calculated_from': ['fg2m', 'fg2a'],
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': True,
        'decimal_places': 1,
    },
    'fg3a': {
        'display_name': '3PA',
        'display_name_totals': '3PA',
        'db_field': 'fg3a',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'fg3_pct': {
        'display_name': '3P%',
        'display_name_totals': '3P%',
        'db_field': None,  # Calculated from fg3m/fg3a
        'calculated': True,
        'calculated_from': ['fg3m', 'fg3a'],
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': True,
        'decimal_places': 1,
    },
    'fta': {
        'display_name': 'FTA',
        'display_name_totals': 'FTA',
        'db_field': 'fta',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'ft_pct': {
        'display_name': 'FT%',
        'display_name_totals': 'FT%',
        'db_field': None,  # Calculated from ftm/fta
        'calculated': True,
        'calculated_from': ['ftm', 'fta'],
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': True,
        'decimal_places': 1,
    },
    'assists': {
        'display_name': 'AST',
        'display_name_totals': 'AST',
        'db_field': 'assists',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'turnovers': {
        'display_name': 'TOV',
        'display_name_totals': 'TOV',
        'db_field': 'turnovers',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': True,  # Lower is better
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'oreb_pct': {
        'display_name': 'OR%',
        'display_name_totals': 'ORS',  # Changes for totals mode
        'db_field': 'oreb_pct',
        'db_field_totals': 'off_rebounds',  # Different field for totals
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': True,
        'format_as_percentage_totals': False,  # Not percentage in totals mode
        'decimal_places': 1,
    },
    'dreb_pct': {
        'display_name': 'DR%',
        'display_name_totals': 'DRS',  # Changes for totals mode
        'db_field': 'dreb_pct',
        'db_field_totals': 'def_rebounds',  # Different field for totals
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': True,
        'format_as_percentage_totals': False,  # Not percentage in totals mode
        'decimal_places': 1,
    },
    'steals': {
        'display_name': 'STL',
        'display_name_totals': 'STL',
        'db_field': 'steals',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'blocks': {
        'display_name': 'BLK',
        'display_name_totals': 'BLK',
        'db_field': 'blocks',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'fouls': {
        'display_name': 'FLS',
        'display_name_totals': 'FLS',
        'db_field': 'fouls',
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': True,  # Lower is better
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'off_rating': {
        'display_name': 'OR',
        'display_name_totals': 'OR',
        'db_field': 'off_rating_x10',
        'divide_by_10': True,  # DB stores as x10
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    'def_rating': {
        'display_name': 'DR',
        'display_name_totals': 'DR',
        'db_field': 'def_rating_x10',
        'divide_by_10': True,  # DB stores as x10
        'width': None,
        'in_current': True,
        'in_historical': True,
        'in_postseason': True,
        'nba_sheet_only': False,
        'is_stat': True,
        'editable': False,
        'hidden': False,
        'reverse_stat': False,
        'format_as_percentage': False,
        'decimal_places': 1,
    },
    
    # HIDDEN COLUMN
    'player_id': {
        'display_name': 'ID',
        'db_field': 'player_id',
        'width': 100,
        'in_current': False,
        'in_historical': False,
        'in_postseason': False,
        'nba_sheet_only': False,
        'is_stat': False,
        'editable': False,
        'hidden': True,  # Always hidden
    },
}

# Define the order of stats as they appear in sections (left to right)
STAT_ORDER = [
    'years', 'games', 'minutes', 'possessions', 'points', 'ts_pct',
    'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct', 'fta', 'ft_pct',
    'assists', 'turnovers', 'oreb_pct', 'dreb_pct',
    'steals', 'blocks', 'fouls', 'off_rating', 'def_rating'
]

# Helper functions to get columns for each section
def get_player_info_columns(for_nba_sheet=False):
    """Get player info columns in order: J#, Age, Exp, Ht, Wt, WS"""
    return ['jersey', 'age', 'experience', 'height', 'weight', 'wingspan']

def get_notes_columns():
    """Get notes column as separate section"""
    return ['notes']

def get_name_columns(for_nba_sheet=False):
    """Get name column(s) - includes team for NBA sheet"""
    if for_nba_sheet:
        return ['name', 'team']
    return ['name']

def get_current_stats():
    """Get stats that appear in current season section"""
    return [stat for stat in STAT_ORDER if COLUMN_DEFINITIONS[stat]['in_current']]

def get_historical_stats():
    """Get stats that appear in historical section"""
    return [stat for stat in STAT_ORDER if COLUMN_DEFINITIONS[stat]['in_historical']]

def get_postseason_stats():
    """Get stats that appear in postseason section"""
    return [stat for stat in STAT_ORDER if COLUMN_DEFINITIONS[stat]['in_postseason']]

def get_reverse_stats():
    """Get stats where lower is better"""
    return {col for col, defn in COLUMN_DEFINITIONS.items() if defn.get('reverse_stat', False)}

def get_editable_fields():
    """Get fields that can be edited by user"""
    return [col for col, defn in COLUMN_DEFINITIONS.items() if defn.get('editable', False)]

# ============================================================================
# SECTIONS CONFIGURATION (DYNAMICALLY GENERATED FROM COLUMN_DEFINITIONS)
# ============================================================================

def build_sections(for_nba_sheet=False):
    """Build sections configuration dynamically from COLUMN_DEFINITIONS"""
    
    name_cols = get_name_columns(for_nba_sheet)
    player_info_cols = get_player_info_columns(for_nba_sheet)
    notes_cols = get_notes_columns()
    current_stats = get_current_stats()
    historical_stats = get_historical_stats()
    postseason_stats = get_postseason_stats()
    
    # Calculate column ranges - NOTES now comes after player_info, before current
    name_start = 0
    name_count = len(name_cols)
    
    player_info_start = name_count
    player_info_count = len(player_info_cols)
    
    notes_start = player_info_start + player_info_count
    notes_count = len(notes_cols)
    
    current_start = notes_start + notes_count
    current_count = len(current_stats)
    
    historical_start = current_start + current_count
    historical_count = len(historical_stats)
    
    postseason_start = historical_start + historical_count
    postseason_count = len(postseason_stats)
    
    hidden_start = postseason_start + postseason_count
    
    sections = {
        'name': {
            'name': 'Name',
            'start_col': name_start,
            'end_col': name_count,
            'column_count': name_count,
            'columns': name_cols,
            'is_stat_section': False,
        },
        'player_info': {
            'name': 'Player Info',
            'start_col': player_info_start,
            'end_col': player_info_start + player_info_count,
            'column_count': player_info_count,
            'columns': player_info_cols,
            'is_stat_section': False,
        },
        'notes': {
            'name': 'Notes',
            'start_col': notes_start,
            'end_col': notes_start + notes_count,
            'column_count': notes_count,
            'columns': notes_cols,
            'is_stat_section': False,
            'header_row_1': 'My Analysis',
            'header_row_2': 'Notes',
        },
        'current': {
            'name': 'Current Season Stats',
            'start_col': current_start,
            'end_col': current_start + current_count,
            'column_count': current_count,
            'columns': current_stats,
            'is_stat_section': True,
            'season_type': 1,
            'has_borders': True,
            'default_visible': True,
            'first_column_width': 25,  # Override width for first column in section
        },
        'historical': {
            'name': 'Historical Stats',
            'start_col': historical_start,
            'end_col': historical_start + historical_count,
            'column_count': historical_count,
            'columns': historical_stats,
            'is_stat_section': True,
            'season_type': 1,
            'has_borders': True,
            'default_visible': True,
            'first_column_width': 25,  # Override width for first column in section
        },
        'postseason': {
            'name': 'Postseason Stats',
            'start_col': postseason_start,
            'end_col': postseason_start + postseason_count,
            'column_count': postseason_count,
            'columns': postseason_stats,
            'is_stat_section': True,
            'season_type': [2, 3],  # Playoffs + Play-in
            'has_borders': True,
            'default_visible': True,
            'first_column_width': 25,  # Override width for first column in section
        },
        'hidden': {
            'name': 'Hidden',
            'start_col': hidden_start,
            'end_col': hidden_start + 1,
            'column_count': 1,
            'columns': ['player_id'],
            'is_stat_section': False,
            'default_visible': False,
        }
    }
    
    return sections

# Build sections for team sheets and NBA sheet
SECTIONS = build_sections(for_nba_sheet=False)
SECTIONS_NBA = build_sections(for_nba_sheet=True)

# ============================================================================
# HELPER FUNCTIONS FOR COLUMN LOOKUPS
# ============================================================================

def get_column_index(column_name, section=None, for_nba_sheet=False):
    """Get the 0-indexed column position for a given column name"""
    sections = SECTIONS_NBA if for_nba_sheet else SECTIONS
    
    if section:
        # Search within a specific section
        sect = sections[section]
        if column_name in sect['columns']:
            offset = sect['columns'].index(column_name)
            return sect['start_col'] + offset
    else:
        # Search all sections
        for sect in sections.values():
            if column_name in sect['columns']:
                offset = sect['columns'].index(column_name)
                return sect['start_col'] + offset
    
    return None

def get_column_letter(column_index):
    """Convert 0-indexed column number to letter (0='A', 25='Z', 26='AA', etc.)"""
    letter = ''
    while column_index >= 0:
        letter = chr(column_index % 26 + ord('A')) + letter
        column_index = column_index // 26 - 1
    return letter

# ============================================================================
# COLORS & PERCENTILE CONFIGURATION
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
    'low': 33,   # 0-33% red to yellow gradient
    'mid': 66,   # 33-66% yellow plateau
    'high': 100, # 66-100% yellow to green gradient
}

PERCENTILE_CONFIG = {
    'min_games': 5,
    'min_minutes': 50,
    'minutes_weight_factor': 10,  # Weight factor for percentile calculation (1 sample per N minutes)
}

# ============================================================================
# LEGACY COMPATIBILITY & HELPER FUNCTIONS
# ============================================================================

# Build stat column lists for backward compatibility
STAT_COLUMNS = [col for col in STAT_ORDER if COLUMN_DEFINITIONS[col]['in_current']]
HISTORICAL_STAT_COLUMNS = [col for col in STAT_ORDER if COLUMN_DEFINITIONS[col]['in_historical']]
PLAYOFF_STAT_COLUMNS = [col for col in STAT_ORDER if COLUMN_DEFINITIONS[col]['in_postseason']]
PLAYER_ID_COLUMN = 'player_id'
REVERSE_STATS = get_reverse_stats()  # Set where lower is better

# Build sheet format dicts from sections
SHEET_FORMAT = {
    'total_columns': sum(sect['column_count'] for sect in SECTIONS.values()),
    'player_info_columns': SECTIONS['player_info']['column_count'],
    'current_stats_columns': SECTIONS['current']['column_count'],
    'historical_stats_columns': SECTIONS['historical']['column_count'],
    'playoff_stats_columns': SECTIONS['postseason']['column_count'],
    'fonts': {
        'header_primary': {'family': 'Staatliches', 'size': 11},
        'header_secondary': {'family': 'Staatliches', 'size': 10},
        'team_name': {'family': 'Staatliches', 'size': 15},
        'player_names': {'family': 'Sofia Sans', 'size': 10},
        'data': {'family': 'Sofia Sans', 'size': 9},
    },
    'frozen': {
        'rows': 3,
        'columns': 1,  # Freeze only column A (team name)
    },
    'header_rows': 3,
}

SHEET_FORMAT_NBA = {
    'total_columns': sum(sect['column_count'] for sect in SECTIONS_NBA.values()),
    'player_info_columns': SECTIONS_NBA['player_info']['column_count'],
    'current_stats_columns': SECTIONS_NBA['current']['column_count'],
    'historical_stats_columns': SECTIONS_NBA['historical']['column_count'],
    'playoff_stats_columns': SECTIONS_NBA['postseason']['column_count'],
    'frozen': {
        'rows': 3,
        'columns': 1,  # Freeze only column A
    },
    'header_rows': 3,
}

def build_headers(for_nba_sheet=False, stats_mode='per_36'):
    """Build header rows dynamically from SECTIONS configuration.
    
    Row 1: Section titles (merged cells) - Team name in A1, then sections
    Row 2: Individual column headers
    Row 3: Unused (for future expansion)
    
    This function iterates through SECTIONS dict to ensure headers match section order.
    """
    sections = SECTIONS_NBA if for_nba_sheet else SECTIONS
    
    row_1 = []
    row_2 = []
    
    # Section name to row 1 header mapping
    section_headers = {
        'name': '{team_name}',  # Placeholder for team name (filled at runtime)
        'player_info': 'PLAYER INFO',
        'notes': 'My Analysis',  # Custom header for notes section
        'current': '{season}',  # Placeholder for "2025-26 Stats Per 36 Mins"
        'historical': '{historical_years}',  # Placeholder for "Career Stats Per 36 Mins"
        'postseason': '{postseason_years}',  # Placeholder for "Career Postseason Stats Per 36 Mins"
        'hidden': ''
    }
    
    # Iterate through sections in the order they're defined
    for section_name, section_info in sections.items():
        col_count = section_info['column_count']
        
        # Row 1: Section header (use custom header from section if available)
        if 'header_row_1' in section_info:
            row_1.append(section_info['header_row_1'])
        else:
            row_1.append(section_headers.get(section_name, ''))
        for _ in range(col_count - 1):
            row_1.append('')
        
        # Row 2: Individual column headers (use custom header from section if available)
        if 'header_row_2' in section_info:
            # For sections with custom row 2 header, use it for all columns
            for i in range(col_count):
                if i == 0:
                    row_2.append(section_info['header_row_2'])
                else:
                    row_2.append('')
        else:
            # Standard: individual column headers
            for col_name in section_info['columns']:
                col_def = COLUMN_DEFINITIONS[col_name]
                # Use totals display name if in totals mode and available
                if stats_mode == 'totals' and 'display_name_totals' in col_def:
                    row_2.append(col_def['display_name_totals'])
                else:
                    row_2.append(col_def['display_name'])
    
    # Row 3: Unused (for future expansion)
    row_3 = [''] * len(row_2)
    
    return {
        'row_1': row_1,
        'row_2': row_2,
        'row_3': row_3,
    }

# Build default headers
HEADERS = build_headers(for_nba_sheet=False)
HEADERS_NBA = build_headers(for_nba_sheet=True)

# ============================================================================
# EXPORT CONFIGURATION FOR API AND APPS SCRIPT
# ============================================================================

def get_config_for_export():
    """Export configuration in a format suitable for API/Apps Script"""
    return {
        'column_definitions': COLUMN_DEFINITIONS,
        'stat_order': STAT_ORDER,
        'sections': SECTIONS,
        'sections_nba': SECTIONS_NBA,
        'nba_teams': dict(NBA_TEAMS),
        'reverse_stats': list(get_reverse_stats()),
        'editable_fields': get_editable_fields(),
        'colors': COLORS,
        'color_thresholds': COLOR_THRESHOLDS,
        # Add column ranges for Apps Script visibility toggles
        'column_ranges': {
            'team_sheet': {
                'current': {
                    'start': SECTIONS['current']['start_col'] + 1,  # +1 for 1-indexed
                    'count': SECTIONS['current']['column_count']
                },
                'historical': {
                    'start': SECTIONS['historical']['start_col'] + 1,
                    'count': SECTIONS['historical']['column_count']
                },
                'postseason': {
                    'start': SECTIONS['postseason']['start_col'] + 1,
                    'count': SECTIONS['postseason']['column_count']
                }
            },
            'nba_sheet': {
                'current': {
                    'start': SECTIONS_NBA['current']['start_col'] + 1,
                    'count': SECTIONS_NBA['current']['column_count']
                },
                'historical': {
                    'start': SECTIONS_NBA['historical']['start_col'] + 1,
                    'count': SECTIONS_NBA['historical']['column_count']
                },
                'postseason': {
                    'start': SECTIONS_NBA['postseason']['start_col'] + 1,
                    'count': SECTIONS_NBA['postseason']['column_count']
                }
            }
        }
    }
