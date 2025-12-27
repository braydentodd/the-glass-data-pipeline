"""
Sync all 30 NBA teams to Google Sheets with percentile-based color coding.
"""

import os
import sys
import psycopg2
import gspread
import numpy as np
import time
from google.oauth2.service_account import Credentials
from psycopg2.extras import RealDictCursor

# Import centralized configuration
from config.database import DB_CONFIG, NBA_CONFIG
from config.sheets import (
    GOOGLE_SHEETS_CONFIG,
    NBA_TEAMS,
    STAT_COLUMNS,
    HISTORICAL_STAT_COLUMNS,
    PLAYOFF_STAT_COLUMNS,
    PLAYER_ID_COLUMN,
    REVERSE_STATS,
    COLORS,
    COLOR_THRESHOLDS,
    SHEET_FORMAT,
    SHEET_FORMAT_NBA,
    SECTIONS,
    SECTIONS_NBA,
    COLUMN_DEFINITIONS,
    STAT_CONSTANTS,
    build_headers,
    get_column_index,
)

# Import formatting utilities (currently unused - may be needed for future refactoring)
# from src.formatting_utils import (
#     create_header_format_requests,
#     create_data_format_requests,
#     create_section_border_requests,
#     create_section_merge_requests,
#     create_column_resize_requests,
#     get_color_dict,
# )

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

def log(message):
    import sys
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(log_msg, file=sys.stderr, flush=True)

def get_max_years_in_database(conn):
    """
    Get the maximum number of years available in the database for postseason data.
    This is calculated as: current_year - earliest_year_in_database
    """
    current_year = NBA_CONFIG['current_season_year']
    
    with conn.cursor() as cur:
        # Get earliest year with postseason data (season_type IN (2, 3))
        cur.execute("""
            SELECT MIN(year) 
            FROM team_season_stats 
            WHERE season_type IN (2, 3)
        """)
        earliest_year = cur.fetchone()[0]
        
        if earliest_year is None:
            # No postseason data yet, default to 25
            return 25
        
        max_years = current_year - earliest_year
        log(f"📊 Database has postseason data from {earliest_year} to {current_year} ({max_years} years)")
        return max_years

def get_current_season():
    """
    Get current NBA season based on date.
    If after August, we're in the next season (e.g., Nov 2025 -> 25-26)
    """
    from datetime import datetime
    now = datetime.now()
    year = now.year
    month = now.month
    
    # If after August (month 8), we're in the next season
    if month > 8:
        return f"{year}-{str(year + 1)[-2:]}"
    else:
        return f"{year - 1}-{str(year)[-2:]}"

def get_google_sheets_client():
    """Initialize Google Sheets API client"""
    credentials = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['credentials_file'], 
        scopes=GOOGLE_SHEETS_CONFIG['scopes']
    )
    client = gspread.authorize(credentials)
    return client

def fetch_all_players_data(conn):
    """Fetch all players (with or without stats) for percentile calculation - CURRENT SEASON ONLY"""
    from src.stat_engine import build_select_fields
    
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    # Build SELECT fields dynamically from config
    base_fields = build_select_fields(entity_type='player')
    
    # Add special fields for current season query
    additional_fields = [
        't.team_abbr',
        '(SELECT COUNT(DISTINCT year) FROM player_season_stats WHERE player_id = p.player_id AND season_type = 1 AND minutes_x10 > 0) AS years_experience',
        'EXTRACT(YEAR FROM AGE(p.birthdate)) + (EXTRACT(MONTH FROM AGE(p.birthdate)) / 12.0) + (EXTRACT(DAY FROM AGE(p.birthdate)) / 365.25) AS age',
        'p.notes',
    ]
    
    # Combine all fields
    select_fields = base_fields + additional_fields
    
    query = f"""
    SELECT {', '.join(select_fields)}
    FROM teams t
    INNER JOIN players p ON p.team_id = t.team_id
    LEFT JOIN player_season_stats s 
        ON s.player_id = p.player_id 
        AND s.year = %s
        AND s.season_type = %s
    WHERE p.team_id IS NOT NULL
    ORDER BY t.team_abbr, COALESCE(s.minutes_x10, 0) DESC, p.name
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (current_year, season_type))
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def fetch_all_nba_players_data(conn):
    """Fetch ALL players in the database including those without teams (marked as FA) - CURRENT SEASON ONLY"""
    from src.stat_engine import build_select_fields
    
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    # Build SELECT fields dynamically from config
    base_fields = build_select_fields(entity_type='player', context='current')
    
    # Add special fields for NBA sheet query
    additional_fields = [
        "COALESCE(t.team_abbr, 'FA') AS team_abbr",
        '(SELECT COUNT(DISTINCT year) FROM player_season_stats WHERE player_id = p.player_id AND season_type = 1 AND minutes_x10 > 0) AS years_experience',
        'EXTRACT(YEAR FROM AGE(p.birthdate)) + (EXTRACT(MONTH FROM AGE(p.birthdate)) / 12.0) + (EXTRACT(DAY FROM AGE(p.birthdate)) / 365.25) AS age',
        'p.notes',
    ]
    
    # Combine all fields
    select_fields = base_fields + additional_fields
    
    query = f"""
    SELECT 
        {', '.join(select_fields)}
    FROM players p
    LEFT JOIN teams t ON p.team_id = t.team_id
    INNER JOIN player_season_stats s 
        ON s.player_id = p.player_id 
        AND s.year = %s
        AND s.season_type = %s
    ORDER BY COALESCE(t.team_abbr, 'FA'), COALESCE(s.minutes_x10, 0) DESC, p.name
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (current_year, season_type))
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def fetch_historical_players_data(conn, past_years=3, include_current=False, specific_seasons=None):
    """
    Fetch aggregated historical stats for past N years or specific seasons
    
    Args:
        conn: Database connection
        past_years: Number of years to look back (default 3)
        include_current: Whether to include current season in aggregates
        specific_seasons: List of specific season years to include (e.g., [2023, 2024])
    
    Returns player stats aggregated across specified seasons
    """
    current_year = NBA_CONFIG['current_season_year']
    log(f"🗓️ Current season year: {current_year}, past_years: {past_years}, include_current: {include_current}")
    
    if specific_seasons:
        # Use specific seasons provided
        season_filter = "AND s.year IN %s"
        season_params = (tuple(specific_seasons),)
        print(f"[HISTORICAL] Using specific seasons: {specific_seasons}")
    else:
        # Use year range
        if include_current:
            start_year = current_year - past_years + 1
            end_year = current_year + 1  # Include current
            print(f"[HISTORICAL] Including current year: {start_year} to {current_year} (current={current_year})")
        else:
            start_year = current_year - past_years
            end_year = current_year  # Exclude current
            print(f"[HISTORICAL] Excluding current year: {start_year} to {current_year-1} (current={current_year})")
        
        season_filter = "AND s.year >= %s AND s.year < %s"
        season_params = (start_year, end_year)
    
    # Build aggregated SELECT fields dynamically from config
    from src.stat_engine import build_aggregated_select_fields
    
    aggregated_fields = build_aggregated_select_fields(entity_type='player')
    additional_fields = [
        'p.player_id',
        'p.name AS player_name',
        'p.team_id',
        't.team_abbr'
    ]
    
    select_fields = additional_fields + aggregated_fields
    
    query = f"""
    SELECT 
        {', '.join(select_fields)}
    FROM teams t
    INNER JOIN players p ON p.team_id = t.team_id
    LEFT JOIN player_season_stats s 
        ON s.player_id = p.player_id 
        {season_filter}
        AND s.season_type = %s
    WHERE p.team_id IS NOT NULL
    GROUP BY p.player_id, p.name, p.team_id, t.team_abbr
    ORDER BY t.team_abbr, SUM(COALESCE(s.minutes_x10, 0)) DESC, p.name
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        params = season_params + (NBA_CONFIG['season_type'],)
        cur.execute(query, params)
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def fetch_postseason_players_data(conn, past_years=None, specific_seasons=None):
    """
    Fetch aggregated postseason stats (season_type IN (2, 3) - playoffs + play-in).
    Similar to historical stats but specifically for postseason games.
    
    Args:
        conn: Database connection
        past_years: Number of years to look back (default None = all years in database)
        specific_seasons: List of specific season years to include (e.g., [2023, 2024])
    
    Returns player postseason stats aggregated across specified seasons
    """
    current_year = NBA_CONFIG['current_season_year']
    
    # If past_years not specified, get all available years from database
    if past_years is None:
        past_years = get_max_years_in_database(conn)
    
    if specific_seasons:
        # Use specific seasons provided
        season_filter = "AND s.year IN %s"
        season_params = (tuple(specific_seasons),)
        print(f"[POSTSEASON] Using specific seasons: {specific_seasons}")
    else:
        # Use year range (postseason never includes current year since postseason hasn't happened yet)
        start_year = current_year - past_years
        end_year = current_year  # Exclude current year
        print(f"[POSTSEASON] Year range: {start_year} to {end_year-1} (current={current_year}, excluded)")
        
        season_filter = "AND s.year >= %s AND s.year < %s"
        season_params = (start_year, end_year)
    
    # Build aggregated SELECT fields dynamically from config
    from src.stat_engine import build_aggregated_select_fields
    
    aggregated_fields = build_aggregated_select_fields(entity_type='player')
    additional_fields = [
        'p.player_id',
        'p.name AS player_name',
        'p.team_id',
        't.team_abbr'
    ]
    
    select_fields = additional_fields + aggregated_fields
    
    query = f"""
    SELECT 
        {', '.join(select_fields)}
    FROM teams t
    INNER JOIN players p ON p.team_id = t.team_id
    LEFT JOIN player_season_stats s 
        ON s.player_id = p.player_id 
        {season_filter}
        AND s.season_type IN (2, 3)
    WHERE p.team_id IS NOT NULL
    GROUP BY p.player_id, p.name, p.team_id, t.team_abbr
    ORDER BY t.team_abbr, SUM(COALESCE(s.minutes_x10, 0)) DESC, p.name
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        params = season_params
        cur.execute(query, params)
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

# Backward compatibility alias
fetch_playoff_players_data = fetch_postseason_players_data

def fetch_team_stats(conn, team_id=None):
    """Fetch current season team stats from team_season_stats table"""
    from src.stat_engine import build_select_fields
    
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    if team_id:
        team_filter = "AND t.team_id = %s"
        params = (current_year, season_type, team_id)
    else:
        team_filter = ""
        params = (current_year, season_type)
    
    # Build SELECT fields dynamically from config
    base_fields = build_select_fields(entity_type='team', include_opponent=True, context='current')
    additional_fields = [
        't.team_id',
        't.team_abbr',
        't.team_name',
        't.notes',
    ]
    
    select_fields = additional_fields + base_fields
    
    query = f"""
    SELECT 
        {', '.join(select_fields)}
    FROM teams t
    LEFT JOIN team_season_stats s 
        ON s.team_id = t.team_id 
        AND s.year = %s
        AND s.season_type = %s
    WHERE t.team_id IS NOT NULL
    {team_filter}
    ORDER BY t.team_abbr
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def fetch_historical_team_stats(conn, past_years=3, include_current=False, specific_seasons=None, team_id=None):
    """Fetch aggregated historical team stats"""
    from src.stat_engine import build_aggregated_select_fields
    
    current_year = NBA_CONFIG['current_season_year']
    
    if specific_seasons:
        season_filter = "AND s.year IN %s"
        params = [tuple(specific_seasons), 1]
    else:
        if include_current:
            season_filter = f"AND s.year >= {current_year - past_years + 1} AND s.year <= {current_year}"
        else:
            season_filter = f"AND s.year >= {current_year - past_years} AND s.year < {current_year}"
        params = [1]
    
    if team_id:
        team_filter = "AND t.team_id = %s"
        params.append(team_id)
    else:
        team_filter = ""
    
    # Build aggregated SELECT fields dynamically from config
    aggregated_fields = build_aggregated_select_fields(entity_type='team', include_opponent=True)
    additional_fields = [
        't.team_id',
        't.team_abbr',
        't.team_name'
    ]
    
    select_fields = additional_fields + aggregated_fields
    
    query = f"""
    SELECT 
        {', '.join(select_fields)}
    FROM teams t
    LEFT JOIN team_season_stats s 
        ON s.team_id = t.team_id 
        {season_filter}
        AND s.season_type = %s
    WHERE t.team_id IS NOT NULL
    {team_filter}
    GROUP BY t.team_id, t.team_abbr, t.team_name
    ORDER BY t.team_abbr
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def fetch_postseason_team_stats(conn, past_years=None, specific_seasons=None, team_id=None):
    """Fetch aggregated postseason team stats (playoffs + play-in)"""
    current_year = NBA_CONFIG['current_season_year']
    
    # If past_years not specified, get all available years from database
    if past_years is None:
        past_years = get_max_years_in_database(conn)
    
    if specific_seasons:
        season_filter = "AND s.year IN %s"
        season_params = (tuple(specific_seasons),)
        print(f"[POSTSEASON TEAMS] Using specific seasons: {specific_seasons}")
    else:
        # Use year range (postseason always excludes current year since postseason hasn't happened yet)
        start_year = current_year - past_years
        end_year = current_year  # Exclude current year
        print(f"[POSTSEASON TEAMS] Year range: {start_year} to {end_year-1} (current={current_year}, excluded)")
        
        season_filter = "AND s.year >= %s AND s.year < %s"
        season_params = (start_year, end_year)
    
    if team_id:
        team_filter = "AND t.team_id = %s"
    else:
        team_filter = ""
    
    # Build aggregated SELECT fields dynamically from config
    from src.stat_engine import build_aggregated_select_fields
    
    aggregated_fields = build_aggregated_select_fields(entity_type='team', include_opponent=True)
    additional_fields = [
        't.team_id',
        't.team_abbr',
        't.team_name'
    ]
    
    select_fields = additional_fields + aggregated_fields
    
    # Season types: 2 = Playoffs, 3 = PlayIn
    query = f"""
    SELECT 
        {', '.join(select_fields)}
    FROM teams t
    LEFT JOIN team_season_stats s 
        ON s.team_id = t.team_id 
        {season_filter}
        AND s.season_type IN (2, 3)
    WHERE t.team_id IS NOT NULL
    {team_filter}
    GROUP BY t.team_id, t.team_abbr, t.team_name
    ORDER BY t.team_abbr
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if team_id:
            params = season_params + (team_id,)
        else:
            params = season_params
        cur.execute(query, params)
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

# calculate_per_100_poss_stats() - DELETED: Replaced by generic engine calculate_entity_stats()

def calculate_per_36_stats(player):
    """Calculate per-36 minute stats (default view)"""
    minutes_total = player.get('minutes_total', 0)
    if not minutes_total or minutes_total == 0:
        return {}
    
    factor = STAT_CONSTANTS['default_per_minutes'] / minutes_total
    
    points = ((player.get('2fgm', 0) or 0) * 2 + 
              (player.get('3fgm', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('2fga', 0) or 0) + (player.get('3fga', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + STAT_CONSTANTS['ts_fta_multiplier'] * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    2fg_pct = ((player.get('2fgm', 0) or 0) / (player.get('2fga', 0) or 1)) if player.get('2fga', 0) else 0
    3fg_pct = ((player.get('3fgm', 0) or 0) / (player.get('3fga', 0) or 1)) if player.get('3fga', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'possessions': (player.get('possessions', 0) or 0) * factor,
        'points': points * factor,
        'ts_pct': ts_pct,
        '2fga': (player.get('2fga', 0) or 0) * factor,
        '2fg_pct': 2fg_pct,
        '3fga': (player.get('3fga', 0) or 0) * factor,
        '3fg_pct': 3fg_pct,
        'fta': fta * factor,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0) * factor,
        'turnovers': (player.get('turnovers', 0) or 0) * factor,
        'oreb_pct': (player.get('oreb_pct', 0) or 0),
        'dreb_pct': (player.get('dreb_pct', 0) or 0),
        'steals': (player.get('steals', 0) or 0) * factor,
        'blocks': (player.get('blocks', 0) or 0) * factor,
        'fouls': (player.get('fouls', 0) or 0) * factor,
        'off_rating': (player.get('off_rating_x10', 0) or 0) / 10.0,
        'def_rating': (player.get('def_rating_x10', 0) or 0) / 10.0,
    }

def calculate_totals_stats(player):
    """Calculate total stats (raw totals)"""
    if not player.get('games_played', 0):
        return {}
    
    points = ((player.get('2fgm', 0) or 0) * 2 + 
              (player.get('3fgm', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('2fga', 0) or 0) + (player.get('3fga', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + STAT_CONSTANTS['ts_fta_multiplier'] * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    2fg_pct = ((player.get('2fgm', 0) or 0) / (player.get('2fga', 0) or 1)) if player.get('2fga', 0) else 0
    3fg_pct = ((player.get('3fgm', 0) or 0) / (player.get('3fga', 0) or 1)) if player.get('3fga', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    # For totals, OR% and DR% become ORS and DRS (actual rebound counts from database)
    ors = player.get('off_rebounds', 0) or 0
    drs = player.get('def_rebounds', 0) or 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0),
        'possessions': player.get('possessions', 0) or 0,
        'points': points,
        'ts_pct': ts_pct,
        '2fga': (player.get('2fga', 0) or 0),
        '2fg_pct': 2fg_pct,
        '3fga': (player.get('3fga', 0) or 0),
        '3fg_pct': 3fg_pct,
        'fta': fta,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0),
        'turnovers': (player.get('turnovers', 0) or 0),
        'oreb_pct': ors,  # ORS instead of OR%
        'dreb_pct': drs,  # DRS instead of DR%
        'steals': (player.get('steals', 0) or 0),
        'blocks': (player.get('blocks', 0) or 0),
        'fouls': (player.get('fouls', 0) or 0),
        'off_rating': (player.get('off_rating_x10', 0) or 0) / 10.0,
        'def_rating': (player.get('def_rating_x10', 0) or 0) / 10.0,
    }

def calculate_per_game_stats(player):
    """Calculate per-game stats"""
    games = player.get('games_played', 0)
    if not games or games == 0:
        return {}
    
    factor = 1.0 / games
    
    points = ((player.get('2fgm', 0) or 0) * 2 + 
              (player.get('3fgm', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('2fga', 0) or 0) + (player.get('3fga', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + STAT_CONSTANTS['ts_fta_multiplier'] * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    2fg_pct = ((player.get('2fgm', 0) or 0) / (player.get('2fga', 0) or 1)) if player.get('2fga', 0) else 0
    3fg_pct = ((player.get('3fgm', 0) or 0) / (player.get('3fga', 0) or 1)) if player.get('3fga', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': games,
        'minutes': player.get('minutes_total', 0) / games,
        'possessions': (player.get('possessions', 0) or 0) * factor,
        'points': points * factor,
        'ts_pct': ts_pct,
        '2fga': (player.get('2fga', 0) or 0) * factor,
        '2fg_pct': 2fg_pct,
        '3fga': (player.get('3fga', 0) or 0) * factor,
        '3fg_pct': 3fg_pct,
        'fta': fta * factor,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0) * factor,
        'turnovers': (player.get('turnovers', 0) or 0) * factor,
        'oreb_pct': (player.get('oreb_pct', 0) or 0),
        'dreb_pct': (player.get('dreb_pct', 0) or 0),
        'steals': (player.get('steals', 0) or 0) * factor,
        'blocks': (player.get('blocks', 0) or 0) * factor,
        'fouls': (player.get('fouls', 0) or 0) * factor,
        'off_rating': (player.get('off_rating_x10', 0) or 0) / 10.0,
        'def_rating': (player.get('def_rating_x10', 0) or 0) / 10.0,
    }

def calculate_per_minutes_stats(player, minutes=None):
    """Calculate per-X minute stats"""
    if minutes is None:
        minutes = STAT_CONSTANTS['default_per_minutes']
    
    minutes_total = player.get('minutes_total', 0)
    if not minutes_total or minutes_total == 0:
        return {}
    
    factor = minutes / minutes_total
    
    points = ((player.get('2fgm', 0) or 0) * 2 + 
              (player.get('3fgm', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('2fga', 0) or 0) + (player.get('3fga', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + STAT_CONSTANTS['ts_fta_multiplier'] * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    2fg_pct = ((player.get('2fgm', 0) or 0) / (player.get('2fga', 0) or 1)) if player.get('2fga', 0) else 0
    3fg_pct = ((player.get('3fgm', 0) or 0) / (player.get('3fga', 0) or 1)) if player.get('3fga', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'possessions': (player.get('possessions', 0) or 0) * factor,
        'points': points * factor,
        'ts_pct': ts_pct,
        '2fga': (player.get('2fga', 0) or 0) * factor,
        '2fg_pct': 2fg_pct,
        '3fga': (player.get('3fga', 0) or 0) * factor,
        '3fg_pct': 3fg_pct,
        'fta': fta * factor,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0) * factor,
        'turnovers': (player.get('turnovers', 0) or 0) * factor,
        'oreb_pct': (player.get('oreb_pct', 0) or 0),
        'dreb_pct': (player.get('dreb_pct', 0) or 0),
        'steals': (player.get('steals', 0) or 0) * factor,
        'blocks': (player.get('blocks', 0) or 0) * factor,
        'fouls': (player.get('fouls', 0) or 0) * factor,
        'off_rating': (player.get('off_rating_x10', 0) or 0) / 10.0,
        'def_rating': (player.get('def_rating_x10', 0) or 0) / 10.0,
    }

def calculate_stats_by_mode(player, mode='per_36', custom_value=None):
    """Calculate stats based on the specified mode - now uses generic engine"""
    from src.stat_engine import calculate_entity_stats
    return calculate_entity_stats(player, STAT_COLUMNS, mode, custom_value)

def calculate_percentiles(all_players_data, mode='per_36', custom_value=None):
    """Calculate weighted percentiles for each stat across all players - now uses generic engine"""
    from src.stat_engine import calculate_percentiles_generic
    
    percentiles, players_with_stats = calculate_percentiles_generic(
        all_players_data,
        STAT_COLUMNS,
        mode,
        custom_value,
        entity_type='player',
        use_minutes_weighting=True
    )
    
    return percentiles, players_with_stats

def calculate_historical_percentiles(historical_players_data, mode='per_36', custom_value=None):
    """Calculate weighted percentiles for historical data - now uses generic engine"""
    from src.stat_engine import calculate_percentiles_generic
    
    # Filter out 'years' column as it has custom logic
    stat_columns = [col for col in HISTORICAL_STAT_COLUMNS if col != 'years']
    
    percentiles, players_with_stats = calculate_percentiles_generic(
        historical_players_data,
        stat_columns,
        mode,
        custom_value,
        entity_type='player',
        use_minutes_weighting=True
    )
    
    return percentiles, players_with_stats

def calculate_postseason_percentiles(postseason_players_data, mode='per_36', custom_value=None):
    """Calculate weighted percentiles for postseason data - now uses generic engine"""
    from src.stat_engine import calculate_percentiles_generic
    
    # Filter out 'years' column as it has custom logic
    stat_columns = [col for col in PLAYOFF_STAT_COLUMNS if col != 'years']
    
    percentiles, players_with_stats = calculate_percentiles_generic(
        postseason_players_data,
        stat_columns,
        mode,
        custom_value,
        entity_type='player',
        use_minutes_weighting=True
    )
    
    return percentiles, players_with_stats

# Legacy alias removed - use calculate_postseason_percentiles directly

def calculate_physical_attribute_percentiles(all_players_data):
    """Calculate percentiles for physical attributes (age, height, weight, wingspan) across all players"""
    from src.stat_engine import get_physical_attribute_columns
    
    percentiles = {}
    
    # Mapping from config keys to database field names
    db_field_mapping = {
        'age': 'age',
        'height': 'height_inches',
        'weight': 'weight_lbs',
        'wingspan': 'wingspan_inches'
    }
    
    # Get physical attribute columns from config
    physical_attrs = get_physical_attribute_columns()
    
    # Calculate percentiles for each physical attribute
    for attr_key in physical_attrs:
        db_field = db_field_mapping.get(attr_key)
        if not db_field:
            continue
            
        values = []
        for player in all_players_data:
            value = player.get(db_field, 0)
            if value and value > 0:  # Only include valid values
                values.append(float(value))
        
        if values:
            percentiles[attr_key] = np.percentile(values, range(101))
        else:
            percentiles[attr_key] = None
    
    return percentiles

def calculate_team_average_percentiles(all_players_by_team):
    """Calculate percentiles for team averages of physical attributes (age, height, weight, wingspan)"""
    team_averages = {}
    
    # Calculate averages for each team
    for team_abbr, players in all_players_by_team.items():
        players_with_minutes = [p for p in players if (p.get('minutes_total', 0) or 0) > 0]
        if not players_with_minutes:
            continue
            
        player_count = len(players_with_minutes)
        team_averages[team_abbr] = {
            'age': float(sum(p.get('age', 0) or 0 for p in players_with_minutes)) / player_count,
            'height_inches': float(sum(p.get('height_inches', 0) or 0 for p in players_with_minutes)) / player_count,
            'weight_lbs': float(sum(p.get('weight_lbs', 0) or 0 for p in players_with_minutes)) / player_count,
        }
        
        # Wingspan - only include players who have it
        wingspans = [float(p.get('wingspan_inches', 0)) for p in players_with_minutes if p.get('wingspan_inches')]
        if wingspans:
            team_averages[team_abbr]['wingspan_inches'] = sum(wingspans) / len(wingspans)
        else:
            team_averages[team_abbr]['wingspan_inches'] = 0
    
    # Calculate percentiles across all teams
    percentiles = {}
    for attr in ['age', 'height_inches', 'weight_lbs', 'wingspan_inches']:
        values = [team[attr] for team in team_averages.values() if team.get(attr, 0) > 0]
        if values:
            percentiles[attr] = np.percentile(values, range(101))
        else:
            percentiles[attr] = None
    
    return percentiles

def calculate_team_percentiles(all_teams_data, mode='per_36', custom_value=None):
    """Calculate percentiles for teams (against other teams) - now uses generic engine"""
    from src.stat_engine import calculate_percentiles_generic
    
    return calculate_percentiles_generic(
        all_teams_data,
        STAT_COLUMNS,
        mode,
        custom_value,
        entity_type='team',
        use_minutes_weighting=False  # Teams compared directly, no weighting
    )

def calculate_historical_team_percentiles(historical_teams_data, mode='per_36', custom_value=None):
    """Calculate percentiles for historical team stats - now uses generic engine"""
    from src.stat_engine import calculate_percentiles_generic
    
    # Filter out 'years' column
    stat_columns = [col for col in HISTORICAL_STAT_COLUMNS if col != 'years']
    
    return calculate_percentiles_generic(
        historical_teams_data,
        stat_columns,
        mode,
        custom_value,
        entity_type='team',
        use_minutes_weighting=False
    )

def calculate_postseason_team_percentiles(postseason_teams_data, mode='per_36', custom_value=None):
    """Calculate percentiles for postseason team stats - now uses generic engine"""
    from src.stat_engine import calculate_percentiles_generic
    
    # Filter out 'years' column
    stat_columns = [col for col in PLAYOFF_STAT_COLUMNS if col != 'years']
    
    return calculate_percentiles_generic(
        postseason_teams_data,
        stat_columns,
        mode,
        custom_value,
        entity_type='team',
        use_minutes_weighting=False
    )

def get_percentile_rank(value, percentiles_array, reverse=False):
    """Get the percentile rank for a value"""
    if percentiles_array is None or value == '':
        return None
    
    # Allow zero values to be ranked (don't exclude them)
    if value == 0:
        value = 0.0
    
    # Find which percentile this value falls into
    rank = np.searchsorted(percentiles_array, value)
    
    if reverse:
        rank = 100 - rank
    
    return min(max(rank, 0), 100)

def get_color_for_percentile(percentile):
    """
    Get RGB color for a percentile value using custom color scale.
    Uses colors from config: red, yellow, green with gradient transitions.
    """
    if percentile is None:
        return None
    
    red_rgb = COLORS['red']
    yellow_rgb = COLORS['yellow']
    green_rgb = COLORS['green']
    
    low_threshold = COLOR_THRESHOLDS['low']
    mid_threshold = COLOR_THRESHOLDS['mid']
    
    if percentile <= low_threshold:
        # Red to Yellow gradient (0-33%)
        ratio = percentile / low_threshold
        return {
            'red': red_rgb['red'] + (yellow_rgb['red'] - red_rgb['red']) * ratio,
            'green': red_rgb['green'] + (yellow_rgb['green'] - red_rgb['green']) * ratio,
            'blue': red_rgb['blue'] + (yellow_rgb['blue'] - red_rgb['blue']) * ratio
        }
    elif percentile <= mid_threshold:
        # Yellow plateau (33-66%)
        return yellow_rgb.copy()
    else:
        # Yellow to Green gradient (66-100%)
        ratio = (percentile - mid_threshold) / (COLOR_THRESHOLDS['high'] - mid_threshold)
        return {
            'red': yellow_rgb['red'] + (green_rgb['red'] - yellow_rgb['red']) * ratio,
            'green': yellow_rgb['green'] + (green_rgb['green'] - yellow_rgb['green']) * ratio,
            'blue': yellow_rgb['blue'] + (green_rgb['blue'] - yellow_rgb['blue']) * ratio
        }


def format_height(inches):
    """Convert inches to feet-inches format"""
    if not inches or inches is None:
        return ""
    try:
        inches_int = int(inches)
        feet = inches_int // 12
        remaining_inches = inches_int % 12
        return f'{feet}\'{remaining_inches}"'
    except (ValueError, TypeError):
        return ""

def get_reverse_stats_for_mode(stats_mode):
    """
    Get reverse stats (lower is better) based on stats mode.
    In per possessions mode, minutes represents efficiency (lower is better).
    In other modes, minutes is total/average (higher is better).
    """
    reverse_stats = set(REVERSE_STATS)
    
    if stats_mode in ['per_100_poss', 'per_possessions']:
        # In per possessions mode, minutes is per game (efficiency - lower is better)
        reverse_stats.add('minutes')
    else:
        # In other modes, minutes is total or per-game average (higher is better)
        reverse_stats.discard('minutes')
    
    return reverse_stats

def parse_sheet_config(worksheet):
    """
    Parse existing sheet configuration from header row to preserve user settings.
    Returns: (stats_mode, custom_value, historical_config, show_percentiles)
    
    historical_config contains: (past_years, include_current, specific_seasons)
    show_percentiles: boolean indicating if sheet is in percentiles mode
    """
    try:
        # Get first row to check headers
        header_row = worksheet.row_values(1)
        if len(header_row) < 26:  # Not enough columns
            log(f"⚠️  Parse config: header row too short ({len(header_row)} columns)")
            return None, None, None, False
        
        log(f"🔍 Parsing sheet config from header row (length: {len(header_row)} columns)")
        
        # Parse stats mode from current stats header (column I, index 8)
        # Get current stats header dynamically from first column of current section
        current_section_start = SECTIONS['current']['start_col']
        current_stats_header = header_row[current_section_start] if len(header_row) > current_section_start else ""
        
        log(f"🔍 Current section starts at column {current_section_start}, header: '{current_stats_header}'")
        
        stats_mode = 'per_36'  # default
        custom_value = None
        
        if 'Totals' in current_stats_header:
            stats_mode = 'totals'
        elif 'Per Game' in current_stats_header:
            stats_mode = 'per_game'
        elif 'Per' in current_stats_header and 'Mins' in current_stats_header:
            # Extract number from "Per XX Mins"
            import re
            match = re.search(r'Per (\d+) Mins', current_stats_header)
            if match:
                custom_value = match.group(1)
                if custom_value == '36':
                    stats_mode = 'per_36'
                else:
                    stats_mode = 'per_minutes'
        
        # Parse historical config from historical stats header
        # Use dynamic column index from SECTIONS configuration
        historical_section_start = SECTIONS['historical']['start_col']
        
        # Check the expected column and adjacent columns for the historical header
        # (there may be an off-by-one due to merged cells or formatting)
        historical_header = ""
        for offset in range(3):  # Check columns 28, 29, 30
            col_idx = historical_section_start + offset
            if col_idx < len(header_row) and header_row[col_idx]:
                historical_header = header_row[col_idx]
                log(f"🔍 Found historical header at column {col_idx}: '{historical_header}'")
                break
        
        if not historical_header:
            log(f"⚠️  Historical header not found near column {historical_section_start}")
        
        log("🔍 DEBUG - Columns around historical start:")
        for offset in range(-2, 3):
            col_idx = historical_section_start + offset
            if 0 <= col_idx < len(header_row):
                log(f"   Column {col_idx}: '{header_row[col_idx]}'")
        
        # Parse percentile mode from header (look for "Percentiles" suffix)
        show_percentiles = 'Percentiles' in historical_header
        
        log(f"🔍 Checking for 'Percentiles' in historical header: {'FOUND' if show_percentiles else 'NOT FOUND'}")
        
        past_years = 3  # default
        include_current = False
        specific_seasons = None
        
        if 'Career' in historical_header:
            past_years = 25
            include_current = 'prev' not in historical_header
        elif 'prev' in historical_header.lower():
            # Try to extract number of years (e.g., "prev 3 season stats")
            import re
            match = re.search(r'prev (\d+) season', historical_header, re.IGNORECASE)
            if match:
                past_years = int(match.group(1))
            include_current = False
        elif 'last' in historical_header.lower():
            # e.g., "last 5 season stats"
            import re
            match = re.search(r'last (\d+) season', historical_header, re.IGNORECASE)
            if match:
                past_years = int(match.group(1))
            include_current = True
        elif 'since' in historical_header:
            # Specific season format - e.g., "prev season stats per 36 mins since 22-23"
            import re
            # Match season format like "22-23" or "2022-23"
            seasons_match = re.findall(r'(\d{2,4})-(\d{2})', historical_header)
            if seasons_match:
                specific_seasons = []
                for start, end in seasons_match:
                    # Convert to 4-digit year
                    if len(start) == 2:
                        year = 2000 + int(end)
                    else:
                        year = int(start) + 1
                    specific_seasons.append(year)
                include_current = 'prev' not in historical_header
        
        log(f"Parsed sheet config: mode={stats_mode}, custom={custom_value}, years={past_years}, include_current={include_current}, seasons={specific_seasons}, percentiles={show_percentiles}")
        return stats_mode, custom_value, (past_years, include_current, specific_seasons), show_percentiles
        
    except Exception as e:
        log(f"Could not parse sheet config: {e}")
        return None, None, None, False

def create_team_sheet(worksheet, team_abbr, team_name, team_players, percentiles, historical_percentiles, 
                      past_years=3, stats_mode='per_36', stats_custom_value=None, specific_seasons=None, 
                      include_current=False, sync_section=None, show_percentiles=False, playoff_percentiles=None,
                      for_nba_sheet=False, team_data=None, team_percentiles=None, historical_team_percentiles=None,
                      playoff_team_percentiles=None):
    """Create/update a team sheet with formatting and color coding (including historical/playoff stats)
    
    Args:
        sync_section: 'historical' or 'playoff' - determines which columns to write to
        show_percentiles: If True, display percentile values instead of stat values
        playoff_percentiles: Percentiles for playoff stats (when sync_section='playoff')
        for_nba_sheet: If True, include team column and use NBA sheet configuration
        team_data: Dict with team_stats, historical_team_stats, playoff_team_stats
        team_percentiles: Percentiles for team vs other teams (current)
        historical_team_percentiles: Percentiles for team vs other teams (historical)
        playoff_team_percentiles: Percentiles for team vs other teams (playoff)
    """
    log(f"Creating {team_name} sheet with stats mode: {stats_mode}, section: {sync_section}, show_percentiles: {show_percentiles}, for_nba_sheet: {for_nba_sheet}...")
    
    # Get reverse stats for current mode (minutes is reverse only in per possessions mode)
    reverse_stats_set = get_reverse_stats_for_mode(stats_mode)
    
    # Get current season dynamically
    current_season = get_current_season()
    
    # Select appropriate configuration based on sheet type
    SECTIONS_CONFIG = SECTIONS_NBA if for_nba_sheet else SECTIONS
    SHEET_FORMAT_CONFIG = SHEET_FORMAT_NBA if for_nba_sheet else SHEET_FORMAT
    
    # Build headers dynamically based on stats_mode and sheet type
    HEADERS = build_headers(for_nba_sheet=for_nba_sheet, stats_mode=stats_mode)
    
    # Header row 1 - replace placeholders
    header_row_1 = []
    mode_display = {
        'totals': 'Totals',
        'per_game': 'Per Game',
        'per_36': 'Per 36 Mins',
        'per_100_poss': f'Per {stats_custom_value} Poss' if stats_custom_value else 'Per 100 Poss',
        'per_minutes': f'Per {stats_custom_value} Mins' if stats_custom_value else 'Per Minute'
    }
    mode_text = mode_display.get(stats_mode, 'Per 36 Mins')
    
    # Add "Percentiles" suffix if showing percentiles
    percentile_suffix = '\tPercentiles' if show_percentiles else ''
    
    for i, h in enumerate(HEADERS['row_1']):
        # Handle historical_years placeholder
        if '{historical_years}' in h:
            # Build header text for historical section
            if specific_seasons:
                # Specific season(s) - show start season only
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                if include_current:
                    historical_text = f'Stats since {start_season_text} {mode_text}{percentile_suffix}'
                else:
                    historical_text = f'Prev stats since {start_season_text} {mode_text}{percentile_suffix}'
            else:
                # Number of years mode
                if include_current:
                    historical_text = f'Last {past_years} Seasons {mode_text}{percentile_suffix}'
                else:
                    historical_text = f'Prev {past_years} Seasons {mode_text}{percentile_suffix}'
            header_row_1.append(h.replace('{historical_years}', historical_text))
            
        # Handle postseason_years placeholder
        elif '{postseason_years}' in h:
            # Build header text for postseason section - always show mode
            if specific_seasons:
                # Specific season(s) - show start season only
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                if include_current:
                    postseason_text = f'Postseason Stats since {start_season_text} {mode_text}{percentile_suffix}'
                else:
                    postseason_text = f'Prev Postseason Stats since {start_season_text} {mode_text}{percentile_suffix}'
            else:
                # Number of years mode
                if include_current:
                    postseason_text = f'Last {past_years} Postseason Seasons {mode_text}{percentile_suffix}'
                else:
                    postseason_text = f'Prev {past_years} Postseason Seasons {mode_text}{percentile_suffix}'
            header_row_1.append(h.replace('{postseason_years}', postseason_text))
            
        # Handle team_name placeholder
        elif '{team_name}' in h:
            # Row 1: Replace with actual team name
            header_row_1.append(team_name.upper())
            
        # Handle season placeholder
        elif '{season}' in h:
            # Update header to reflect the stats mode
            header_row_1.append(h.replace('{season}', f'{current_season} Stats {mode_text}{percentile_suffix}'))
        else:
            header_row_1.append(h)
    
    # Header row 2 - subsection headers (Rates, Scoring, Distribution, Rebounding, Defense, On/Off)
    header_row_2 = list(HEADERS['row_2'])  # Subsection headers for ADVANCED view
    
    # Header row 3 - column headers (GMS, MIN, PTS, etc.)
    header_row_3 = list(HEADERS['row_3'])  # Headers already have OR%/DR% or ORS/DRS based on mode
    
    # Override row 3 cell A3 to say "PLAYERS" instead of "NAME"
    header_row_3[0] = 'PLAYERS'
    
    # For NBA sheet: Move "PLAYER INFO" from C1 to B1 so it appears in the merged B1-H1 cell
    if for_nba_sheet:
        # Find where PLAYER INFO is in row 1 (should be at player_info section start)
        player_info_start = SECTIONS_CONFIG['player_info']['start_col']
        if player_info_start < len(header_row_1) and header_row_1[player_info_start] == 'PLAYER INFO':
            # Move it to B1 (index 1)
            header_row_1[1] = header_row_1[player_info_start]
            header_row_1[player_info_start] = ''
    
    # Filter row (row 4)
    filter_row = [""] * SHEET_FORMAT_CONFIG['total_columns']
    
    # Prepare data rows with percentile tracking
    data_rows = []
    percentile_data = []  # Track percentiles for color coding
    
    for player in team_players:
        # Get calculated stats if available, otherwise empty dict for players without stats
        calculated_stats = player.get('calculated_stats', {})
        
        exp = player.get('years_experience')
        exp_display = 0 if exp == 0 else (exp if exp else '')
        
        # Helper function to format numbers without .0 for whole numbers
        def format_stat(value, decimals=1):
            if value is None or value == 0:
                return 0
            rounded = round(value, decimals)
            # If it's a whole number, return as int to avoid .0
            if rounded == int(rounded):
                return int(rounded)
            return rounded
        
        # Helper function for percentage stats (multiply by 100)
        def format_pct(value, decimals=1, allow_zero=False):
            if value is None:
                return ''
            if value == 0:
                return 0 if allow_zero else ''  # Show 0 for rebounding %, empty for shooting %
            result = value * 100
            rounded = round(result, decimals)
            # If it's a whole number, return as int to avoid .0
            if rounded == int(rounded):
                return int(rounded)
            return rounded
        
        # Helper function to get percentile value or stat value based on mode
        def get_display_value(stat_value, percentile_value, col_idx, is_pct=False, allow_zero=False):
            """Return percentile if show_percentiles=True, otherwise return stat value"""
            if show_percentiles and percentile_value is not None:
                # Display percentile as whole number (0-100)
                return int(round(percentile_value))
            elif is_pct:
                return format_pct(stat_value, allow_zero=allow_zero)
            else:
                return format_stat(stat_value)
        
        # Check if there are attempts for shooting percentages
        2fga = calculated_stats.get('2fga', 0)
        3fga = calculated_stats.get('3fga', 0)
        fta = calculated_stats.get('fta', 0)
        
        # Check if player has minutes - if not, leave all stats empty
        minutes = calculated_stats.get('minutes', 0)
        has_minutes = minutes and minutes > 0
        
        # Get historical stats
        historical_calculated_stats = player.get('historical_calculated_stats', {})
        historical_minutes = historical_calculated_stats.get('minutes', 0)
        has_historical_minutes = historical_minutes and historical_minutes > 0
        seasons_played = player.get('seasons_played', 0) if has_historical_minutes else ''
        
        # Historical shooting attempts
        hist_2fga = historical_calculated_stats.get('2fga', 0)
        hist_3fga = historical_calculated_stats.get('3fga', 0)
        hist_fta = historical_calculated_stats.get('fta', 0)
        
        # Get playoff stats
        playoff_calculated_stats = player.get('playoff_calculated_stats', {})
        playoff_minutes = playoff_calculated_stats.get('minutes', 0)
        has_playoff_minutes = playoff_minutes and playoff_minutes > 0
        playoff_seasons_played = player.get('playoff_seasons_played', 0) if has_playoff_minutes else ''
        
        # Playoff shooting attempts
        playoff_2fga = playoff_calculated_stats.get('2fga', 0)
        playoff_3fga = playoff_calculated_stats.get('3fga', 0)
        playoff_fta = playoff_calculated_stats.get('fta', 0)
        
        # PRE-CALCULATE PERCENTILES (needed for show_percentiles mode)
        player_percentiles = {}
        
        # Player info percentiles (physical attributes)
        for col_name in SECTIONS_CONFIG['player_info']['columns']:
            if col_name in ['age', 'height', 'weight', 'wingspan']:
                col_idx = get_column_index(col_name, section='player_info', for_nba_sheet=for_nba_sheet)
                if col_name == 'age':
                    value = player.get('age', 0)
                elif col_name == 'height':
                    value = player.get('height_inches', 0)
                elif col_name == 'weight':
                    value = player.get('weight_lbs', 0)
                elif col_name == 'wingspan':
                    value = player.get('wingspan_inches', 0)
                
                if value and value > 0 and percentiles.get(col_name) is not None:
                    # Physical attributes: higher is generally better (not reversed)
                    pct = get_percentile_rank(value, percentiles.get(col_name), reverse=False)
                    player_percentiles[col_idx] = pct
                else:
                    player_percentiles[col_idx] = None
        
        # Current season percentiles
        for stat_name in STAT_COLUMNS:
            col_idx = get_column_index(stat_name, section='current', for_nba_sheet=for_nba_sheet)
            value = calculated_stats.get(stat_name, 0)
            if not has_minutes:
                player_percentiles[col_idx] = None
            elif stat_name == 'ts_pct' and value == 0:
                player_percentiles[col_idx] = None
            elif stat_name == '2fg_pct' and 2fga == 0:
                player_percentiles[col_idx] = None
            elif stat_name == '3fg_pct' and 3fga == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and fta == 0:
                player_percentiles[col_idx] = None
            elif value is not None and value != '':
                reverse = stat_name in reverse_stats_set
                pct = get_percentile_rank(value, percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # Physical attributes percentiles (player_info section)
        physical_attributes = ['age', 'height', 'weight', 'wingspan']
        for attr_name in physical_attributes:
            col_idx = get_column_index(attr_name, section='player_info', for_nba_sheet=for_nba_sheet)
            if attr_name == 'age':
                value = player.get('age', 0)
            elif attr_name == 'height':
                value = player.get('height_inches', 0)
            elif attr_name == 'weight':
                value = player.get('weight_lbs', 0)
            elif attr_name == 'wingspan':
                value = player.get('wingspan_inches', 0)
            else:
                value = 0
            
            if value and value > 0:
                reverse = attr_name in reverse_stats_set  # Age is reverse (lower is better)
                pct = get_percentile_rank(value, percentiles.get(attr_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # Historical percentiles
        for stat_name in HISTORICAL_STAT_COLUMNS:
            col_idx = get_column_index(stat_name, section='historical', for_nba_sheet=for_nba_sheet)
            if stat_name == 'years':
                if seasons_played and seasons_played > 0:
                    if seasons_played >= 3:
                        player_percentiles[col_idx] = 100
                    elif seasons_played == 2:
                        player_percentiles[col_idx] = 60
                    else:
                        player_percentiles[col_idx] = 20
                else:
                    player_percentiles[col_idx] = None
                continue
            
            value = historical_calculated_stats.get(stat_name, 0)
            if not has_historical_minutes:
                player_percentiles[col_idx] = None
            elif stat_name == 'ts_pct' and value == 0:
                player_percentiles[col_idx] = None
            elif stat_name == '2fg_pct' and hist_2fga == 0:
                player_percentiles[col_idx] = None
            elif stat_name == '3fg_pct' and hist_3fga == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and hist_fta == 0:
                player_percentiles[col_idx] = None
            elif value is not None and value != '':
                reverse = stat_name in reverse_stats_set
                pct = get_percentile_rank(value, historical_percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # Playoff percentiles
        for stat_name in PLAYOFF_STAT_COLUMNS:
            col_idx = get_column_index(stat_name, section='postseason', for_nba_sheet=for_nba_sheet)
            if stat_name == 'years':
                # Use playoff_seasons_played (from playoff data) not seasons_played (career)
                if playoff_seasons_played and playoff_seasons_played > 0:
                    if playoff_seasons_played >= 3:
                        player_percentiles[col_idx] = 100
                    elif playoff_seasons_played == 2:
                        player_percentiles[col_idx] = 60
                    else:
                        player_percentiles[col_idx] = 20
                else:
                    player_percentiles[col_idx] = None
                continue
            
            value = playoff_calculated_stats.get(stat_name, 0)
            if not has_playoff_minutes:
                player_percentiles[col_idx] = None
            elif stat_name == 'ts_pct' and value == 0:
                player_percentiles[col_idx] = None
            elif stat_name == '2fg_pct' and playoff_2fga == 0:
                player_percentiles[col_idx] = None
            elif stat_name == '3fg_pct' and playoff_3fga == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and playoff_fta == 0:
                player_percentiles[col_idx] = None
            elif value is not None and value != '':
                reverse = stat_name in reverse_stats_set
                pct = get_percentile_rank(value, playoff_percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # BUILD ROW DYNAMICALLY based on SECTIONS configuration
        row = []
        
        # Helper to get stat value with proper formatting
        def get_stat_value(col_name, stats_dict, has_data, season_type='current'):
            """Get formatted stat value for a column"""
            if not has_data:
                return ''
            
            col_def = COLUMN_DEFINITIONS[col_name]
            value = stats_dict.get(col_name, 0)
            
            # Get column index for percentile lookup
            if season_type == 'current':
                col_idx = get_column_index(col_name, section='current', for_nba_sheet=for_nba_sheet)
            elif season_type == 'historical':
                col_idx = get_column_index(col_name, section='historical', for_nba_sheet=for_nba_sheet)
            else:  # postseason
                col_idx = get_column_index(col_name, section='postseason', for_nba_sheet=for_nba_sheet)
            
            percentile_val = player_percentiles.get(col_idx)
            
            # Handle percentage stats
            is_pct = col_def.get('format_as_percentage', False)
            
            # In totals mode, oreb_pct and dreb_pct are actually counts (ORS/DRS), not percentages
            if stats_mode == 'totals' and col_name in ['oreb_pct', 'dreb_pct']:
                is_pct = False
            
            # Special handling for shooting percentages (allow_zero)
            allow_zero = False
            if col_name == '2fg_pct' and (2fga if season_type == 'current' else (hist_2fga if season_type == 'historical' else playoff_2fga)) > 0:
                allow_zero = True
            elif col_name == '3fg_pct' and (3fga if season_type == 'current' else (hist_3fga if season_type == 'historical' else playoff_3fga)) > 0:
                allow_zero = True
            elif col_name == 'ft_pct' and (fta if season_type == 'current' else (hist_fta if season_type == 'historical' else playoff_fta)) > 0:
                allow_zero = True
            elif col_name in ['oreb_pct', 'dreb_pct']:
                allow_zero = True
            
            return get_display_value(value, percentile_val, col_idx, is_pct=is_pct, allow_zero=allow_zero)
        
        # Build name section
        for col_name in SECTIONS_CONFIG['name']['columns']:
            if col_name == 'name':
                row.append(player['player_name'])
            elif col_name == 'team':
                row.append(player.get('team_abbr', ''))
        
        # Build player_info section
        for col_name in SECTIONS_CONFIG['player_info']['columns']:
            if col_name == 'jersey':
                # Store as number for proper sorting
                jersey = player.get('jersey_number', '')
                if jersey and str(jersey).isdigit():
                    row.append(int(jersey))
                else:
                    row.append(jersey if jersey else '')
            elif col_name == 'age':
                col_idx = get_column_index(col_name, section='player_info', for_nba_sheet=for_nba_sheet)
                age_val = player.get('age', 0)
                if show_percentiles and player_percentiles.get(col_idx) is not None:
                    row.append(int(round(player_percentiles[col_idx])))
                else:
                    row.append(round(float(age_val), 1) if age_val else '')
            elif col_name == 'experience':
                row.append(exp_display)
            elif col_name == 'height':
                col_idx = get_column_index(col_name, section='player_info', for_nba_sheet=for_nba_sheet)
                height_inches = player.get('height_inches')
                if show_percentiles and player_percentiles.get(col_idx) is not None:
                    row.append(int(round(player_percentiles[col_idx])))
                elif height_inches:
                    feet = height_inches // 12
                    inches = height_inches % 12
                    row.append(f"{feet}'{inches}\"")
                else:
                    row.append('')
            elif col_name == 'weight':
                col_idx = get_column_index(col_name, section='player_info', for_nba_sheet=for_nba_sheet)
                weight_lbs = player.get('weight_lbs', 0)
                if show_percentiles and player_percentiles.get(col_idx) is not None:
                    row.append(int(round(player_percentiles[col_idx])))
                else:
                    row.append(weight_lbs if weight_lbs else '')
            elif col_name == 'wingspan':
                col_idx = get_column_index(col_name, section='player_info', for_nba_sheet=for_nba_sheet)
                wingspan_inches = player.get('wingspan_inches')
                if show_percentiles and player_percentiles.get(col_idx) is not None:
                    row.append(int(round(player_percentiles[col_idx])))
                elif wingspan_inches:
                    feet = wingspan_inches // 12
                    inches = wingspan_inches % 12
                    row.append(f"{feet}'{inches}\"")
                else:
                    row.append('')
        
        # Build notes section (AFTER player_info, BEFORE current stats)
        for col_name in SECTIONS_CONFIG['notes']['columns']:
            if col_name == 'notes':
                row.append(player.get('notes', ''))
        
        # Build current stats section
        for col_name in SECTIONS_CONFIG['current']['columns']:
            if col_name == 'games':
                row.append(calculated_stats.get('games', 0) if calculated_stats.get('games', 0) and has_minutes else '')
            else:
                row.append(get_stat_value(col_name, calculated_stats, has_minutes, 'current'))
        
        # Build historical stats section
        for col_name in SECTIONS_CONFIG['historical']['columns']:
            if col_name == 'years':
                row.append(seasons_played)
            elif col_name == 'games':
                # For non-totals modes, show games per season; for totals show total games
                games_val = historical_calculated_stats.get('games', 0) / seasons_played if seasons_played and seasons_played > 0 and stats_mode != 'totals' else historical_calculated_stats.get('games', 0)
                col_idx = get_column_index(col_name, section='historical', for_nba_sheet=for_nba_sheet)
                row.append(get_display_value(games_val, player_percentiles.get(col_idx), col_idx) if has_historical_minutes else '')
            else:
                row.append(get_stat_value(col_name, historical_calculated_stats, has_historical_minutes, 'historical'))
        
        # Build postseason stats section
        for col_name in SECTIONS_CONFIG['postseason']['columns']:
            if col_name == 'years':
                # Use playoff_seasons_played (from playoff data) not career seasons
                row.append(playoff_seasons_played if playoff_seasons_played else '')
            elif col_name == 'games':
                # For non-totals modes, show games per season; for totals show total games
                games_val = playoff_calculated_stats.get('games', 0) / playoff_seasons_played if playoff_seasons_played and playoff_seasons_played > 0 and stats_mode != 'totals' else playoff_calculated_stats.get('games', 0)
                col_idx = get_column_index(col_name, section='postseason', for_nba_sheet=for_nba_sheet)
                row.append(get_display_value(games_val, player_percentiles.get(col_idx), col_idx) if has_playoff_minutes else '')
            else:
                row.append(get_stat_value(col_name, playoff_calculated_stats, has_playoff_minutes, 'postseason'))
        
        # Add hidden player_id
        row.append(str(player['player_id']))
        
        data_rows.append(row)
        percentile_data.append(player_percentiles)
    
    # ============================================================================
    # ADD TEAM AND OPPONENTS ROWS
    # ============================================================================
    if team_data:
        team_stats = team_data.get('team_stats', {})
        historical_team_stats = team_data.get('historical_team_stats', {})
        playoff_team_stats = team_data.get('playoff_team_stats', {})
        
        # Calculate team stats using the SAME calculate_stats_by_mode function as players
        team_calculated_stats = calculate_stats_by_mode(team_stats, stats_mode, stats_custom_value) if team_stats else {}
        historical_team_calculated_stats = calculate_stats_by_mode(historical_team_stats, stats_mode, stats_custom_value) if historical_team_stats else {}
        playoff_team_calculated_stats = calculate_stats_by_mode(playoff_team_stats, stats_mode, stats_custom_value) if playoff_team_stats else {}
        
        # Calculate team averages from players (for player info section only)
        players_with_minutes = [p for p in team_players if (p.get('minutes_total', 0) or 0) > 0]
        player_count = len(players_with_minutes)
        
        # Create a "fake player" dict with team averages for player info
        team_as_player = {
            'player_id': team_stats.get('team_id', ''),
            'player_name': 'Team',
            'team_abbr': team_abbr,
        }
        
        if player_count > 0:
            team_as_player['years_experience'] = round(sum(p.get('years_experience', 0) or 0 for p in players_with_minutes) / player_count, 1)
            team_as_player['age'] = round(sum(p.get('age', 0) or 0 for p in players_with_minutes) / player_count, 1)
            team_as_player['height_inches'] = round(sum(p.get('height_inches', 0) or 0 for p in players_with_minutes) / player_count, 1)
            team_as_player['weight_lbs'] = round(sum(p.get('weight_lbs', 0) or 0 for p in players_with_minutes) / player_count, 1)
            
            wingspans = [p.get('wingspan_inches', 0) for p in players_with_minutes if p.get('wingspan_inches')]
            team_as_player['wingspan_inches'] = round(sum(wingspans) / len(wingspans), 1) if wingspans else 0
        else:
            team_as_player['years_experience'] = 0
            team_as_player['age'] = 0
            team_as_player['height_inches'] = 0
            team_as_player['weight_lbs'] = 0
            team_as_player['wingspan_inches'] = 0
        
        # Add calculated stats and metadata to team_as_player (just like players)
        team_as_player['calculated_stats'] = team_calculated_stats
        team_as_player['historical_calculated_stats'] = historical_team_calculated_stats
        team_as_player['playoff_calculated_stats'] = playoff_team_calculated_stats
        team_as_player['minutes_total'] = team_stats.get('minutes_total', 0)
        team_as_player['seasons_played'] = historical_team_stats.get('seasons_played', 0) if historical_team_stats else 0
        team_as_player['playoff_seasons_played'] = playoff_team_stats.get('seasons_played', 0) if playoff_team_stats else 0
        
        # Calculate percentiles for team stats (against OTHER TEAMS)
        team_row_percentiles = {}
        
        # Player info percentiles (age, height, weight, wingspan - vs OTHER TEAM AVERAGES)
        # Note: team_data should include 'team_avg_percentiles' calculated in main()
        team_avg_percentiles = team_data.get('team_avg_percentiles', {}) if team_data else {}
        
        for attr_name in ['age', 'height', 'weight', 'wingspan']:
            col_idx = get_column_index(attr_name, section='player_info', for_nba_sheet=for_nba_sheet)
            if attr_name == 'age':
                value = team_as_player.get('age', 0)
                percentile_key = 'age'
            elif attr_name == 'height':
                value = team_as_player.get('height_inches', 0)
                percentile_key = 'height_inches'
            elif attr_name == 'weight':
                value = team_as_player.get('weight_lbs', 0)
                percentile_key = 'weight_lbs'
            elif attr_name == 'wingspan':
                value = team_as_player.get('wingspan_inches', 0)
                percentile_key = 'wingspan_inches'
            
            if value and value > 0 and team_avg_percentiles.get(percentile_key) is not None:
                reverse = attr_name in reverse_stats_set  # Age is reverse
                pct = get_percentile_rank(value, team_avg_percentiles.get(percentile_key), reverse=reverse)
                team_row_percentiles[col_idx] = pct
            else:
                team_row_percentiles[col_idx] = None
        
        # Current stats percentiles (vs other teams)
        for stat_name in STAT_COLUMNS:
            col_idx = get_column_index(stat_name, section='current', for_nba_sheet=for_nba_sheet)
            value = team_calculated_stats.get(stat_name, 0)
            minutes = team_stats.get('minutes_total', 0) or 0
            
            # Games and minutes always get percentiles (they're always valid)
            if stat_name in ['games', 'minutes']:
                if value is not None and value != '':
                    reverse = stat_name in reverse_stats_set
                    pct = get_percentile_rank(value, team_percentiles.get(stat_name), reverse=reverse) if team_percentiles and stat_name in team_percentiles else None
                    team_row_percentiles[col_idx] = pct
                else:
                    team_row_percentiles[col_idx] = None
            # Other stats require minutes > 0
            elif minutes > 0 and value is not None and value != '':
                reverse = stat_name in reverse_stats_set
                pct = get_percentile_rank(value, team_percentiles.get(stat_name), reverse=reverse) if team_percentiles and stat_name in team_percentiles else None
                team_row_percentiles[col_idx] = pct
            else:
                team_row_percentiles[col_idx] = None
        
        # Historical stats percentiles (vs other teams)
        for stat_name in HISTORICAL_STAT_COLUMNS:
            col_idx = get_column_index(stat_name, section='historical', for_nba_sheet=for_nba_sheet)
            if stat_name == 'years':
                # Handle years column specially
                seasons_played_team = team_as_player.get('seasons_played', 0)
                if seasons_played_team >= 3:
                    team_row_percentiles[col_idx] = 100
                elif seasons_played_team == 2:
                    team_row_percentiles[col_idx] = 60
                elif seasons_played_team == 1:
                    team_row_percentiles[col_idx] = 20
                else:
                    team_row_percentiles[col_idx] = None
                continue
            
            value = historical_team_calculated_stats.get(stat_name, 0)
            hist_minutes = historical_team_stats.get('minutes_total', 0) if historical_team_stats else 0
            hist_minutes = hist_minutes or 0
            
            # Games and minutes always get percentiles (they're always valid)
            if stat_name in ['games', 'minutes']:
                if historical_team_stats and value is not None and value != '':
                    reverse = stat_name in reverse_stats_set
                    pct = get_percentile_rank(value, historical_team_percentiles.get(stat_name), reverse=reverse) if historical_team_percentiles and stat_name in historical_team_percentiles else None
                    team_row_percentiles[col_idx] = pct
                else:
                    team_row_percentiles[col_idx] = None
            # Other stats require minutes > 0
            elif historical_team_stats and hist_minutes > 0 and value is not None and value != '':
                reverse = stat_name in reverse_stats_set
                pct = get_percentile_rank(value, historical_team_percentiles.get(stat_name), reverse=reverse) if historical_team_percentiles and stat_name in historical_team_percentiles else None
                team_row_percentiles[col_idx] = pct
            else:
                team_row_percentiles[col_idx] = None
        
        # Postseason stats percentiles (vs other teams)
        for stat_name in PLAYOFF_STAT_COLUMNS:
            col_idx = get_column_index(stat_name, section='postseason', for_nba_sheet=for_nba_sheet)
            if stat_name == 'years':
                playoff_seasons_played_team = team_as_player.get('playoff_seasons_played', 0)
                if playoff_seasons_played_team >= 3:
                    team_row_percentiles[col_idx] = 100
                elif playoff_seasons_played_team == 2:
                    team_row_percentiles[col_idx] = 60
                elif playoff_seasons_played_team == 1:
                    team_row_percentiles[col_idx] = 20
                else:
                    team_row_percentiles[col_idx] = None
                continue
            
            value = playoff_team_calculated_stats.get(stat_name, 0)
            playoff_minutes = playoff_team_stats.get('minutes_total', 0) if playoff_team_stats else 0
            
            # Games and minutes always get percentiles (they're always valid)
            if stat_name in ['games', 'minutes']:
                if playoff_team_stats and value is not None and value != '':
                    reverse = stat_name in reverse_stats_set
                    pct = get_percentile_rank(value, playoff_team_percentiles.get(stat_name), reverse=reverse) if playoff_team_percentiles and stat_name in playoff_team_percentiles else None
                    team_row_percentiles[col_idx] = pct
                else:
                    team_row_percentiles[col_idx] = None
            # Other stats require minutes > 0
            elif playoff_team_stats and playoff_minutes and playoff_minutes > 0 and value is not None and value != '':
                reverse = stat_name in reverse_stats_set
                pct = get_percentile_rank(value, playoff_team_percentiles.get(stat_name), reverse=reverse) if playoff_team_percentiles and stat_name in playoff_team_percentiles else None
                team_row_percentiles[col_idx] = pct
            else:
                team_row_percentiles[col_idx] = None
        
        # Now build the team row using the SAME logic as player rows
        team_row = []
        
        # Name column (Team goes in column A, not B)
        team_row.append('Team')
        
        # Player Info section - use SECTIONS_CONFIG just like players
        for col_name in SECTIONS_CONFIG['player_info']['columns']:
            col_idx = get_column_index(col_name, section='player_info', for_nba_sheet=for_nba_sheet)
            
            if col_name == 'team' and for_nba_sheet:
                team_row.append(team_abbr)
            elif col_name == 'jersey':
                team_row.append('')
            elif col_name == 'experience':
                # Experience is average without percentiles
                exp_val = team_as_player.get('years_experience', 0)
                if exp_val > 0:
                    team_row.append(round(exp_val, 1) if exp_val != int(exp_val) else int(exp_val))
                else:
                    team_row.append('')
            elif col_name == 'age':
                age_val = team_as_player.get('age', 0)
                team_row.append(get_display_value(age_val, team_row_percentiles.get(col_idx), col_idx))
            elif col_name == 'height':
                ht_val = team_as_player.get('height_inches', 0)
                if ht_val > 0:
                    if show_percentiles and team_row_percentiles.get(col_idx) is not None:
                        team_row.append(int(round(team_row_percentiles[col_idx])))
                    else:
                        # Format as X'Y.Y" with 1 decimal
                        feet = int(ht_val) // 12
                        inches = ht_val - (feet * 12)
                        team_row.append(f"{feet}'{inches:.1f}\"")
                else:
                    team_row.append('')
            elif col_name == 'weight':
                wt_val = team_as_player.get('weight_lbs', 0)
                team_row.append(get_display_value(wt_val, team_row_percentiles.get(col_idx), col_idx) if wt_val > 0 else '')
            elif col_name == 'wingspan':
                ws_val = team_as_player.get('wingspan_inches', 0)
                if ws_val > 0:
                    if show_percentiles and team_row_percentiles.get(col_idx) is not None:
                        team_row.append(int(round(team_row_percentiles[col_idx])))
                    else:
                        # Format as X'Y.Y" with 1 decimal
                        feet = int(ws_val) // 12
                        inches = ws_val - (feet * 12)
                        team_row.append(f"{feet}'{inches:.1f}\"")
                else:
                    team_row.append('')
        
        # Notes column - get from team_stats
        team_notes = team_stats.get('notes', '') or ''
        team_row.append(team_notes)
        
        # Helper function to get team stat value with TEAM percentiles
        def get_team_stat_value(col_name, stats_dict, has_data, season_type='current'):
            """Get formatted stat value for team row using TEAM percentiles"""
            if not has_data:
                return ''
            
            col_def = COLUMN_DEFINITIONS[col_name]
            value = stats_dict.get(col_name, 0)
            
            # Get column index for percentile lookup
            if season_type == 'current':
                col_idx = get_column_index(col_name, section='current', for_nba_sheet=for_nba_sheet)
                # Use TEAM percentiles from team_percentiles dict
                percentile_val = None
                if team_percentiles and col_name in STAT_COLUMNS:
                    team_stat_value = team_calculated_stats.get(col_name, 0)
                    if team_stat_value is not None and team_stat_value != '':
                        reverse = col_name in reverse_stats_set
                        percentile_val = get_percentile_rank(team_stat_value, team_percentiles.get(col_name), reverse=reverse)
            elif season_type == 'historical':
                col_idx = get_column_index(col_name, section='historical', for_nba_sheet=for_nba_sheet)
                # Use historical TEAM percentiles
                percentile_val = None
                if historical_team_percentiles and col_name in STAT_COLUMNS:
                    team_stat_value = stats_dict.get(col_name, 0)
                    if team_stat_value is not None and team_stat_value != '':
                        reverse = col_name in reverse_stats_set
                        percentile_val = get_percentile_rank(team_stat_value, historical_team_percentiles.get(col_name), reverse=reverse)
            else:  # postseason
                col_idx = get_column_index(col_name, section='postseason', for_nba_sheet=for_nba_sheet)
                # Use playoff TEAM percentiles
                percentile_val = None
                if playoff_team_percentiles and col_name in STAT_COLUMNS:
                    team_stat_value = stats_dict.get(col_name, 0)
                    if team_stat_value is not None and team_stat_value != '':
                        reverse = col_name in reverse_stats_set
                        percentile_val = get_percentile_rank(team_stat_value, playoff_team_percentiles.get(col_name), reverse=reverse)
            
            # Handle percentage stats
            is_pct = col_def.get('format_as_percentage', False)
            
            # In totals mode, oreb_pct and dreb_pct are actually counts (ORS/DRS), not percentages
            if stats_mode == 'totals' and col_name in ['oreb_pct', 'dreb_pct']:
                is_pct = False
            
            # For team row, always allow_zero for rebounding percentages
            allow_zero = col_name in ['oreb_pct', 'dreb_pct']
            
            return get_display_value(value, percentile_val, col_idx, is_pct=is_pct, allow_zero=allow_zero)
        
        # Current stats section
        for col_name in SECTIONS_CONFIG['current']['columns']:
            team_row.append(get_team_stat_value(col_name, team_calculated_stats, (team_stats.get('minutes_total', 0) or 0) > 0, 'current'))
        
        # Historical stats section
        for col_name in SECTIONS_CONFIG['historical']['columns']:
            if col_name == 'years':
                team_row.append(team_as_player.get('seasons_played', '') or '')
            elif col_name == 'games':
                seasons = team_as_player.get('seasons_played', 0)
                games_val = historical_team_calculated_stats.get('games', 0) / seasons if seasons and seasons > 0 and stats_mode != 'totals' else historical_team_calculated_stats.get('games', 0)
                col_idx = get_column_index(col_name, section='historical', for_nba_sheet=for_nba_sheet)
                has_hist = historical_team_stats and (historical_team_stats.get('minutes_total', 0) or 0) > 0
                # For games, use team_row_percentiles (not team percentiles)
                team_row.append(get_display_value(games_val, team_row_percentiles.get(col_idx), col_idx) if has_hist else '')
            else:
                team_row.append(get_team_stat_value(col_name, historical_team_calculated_stats, historical_team_stats and (historical_team_stats.get('minutes_total', 0) or 0) > 0, 'historical'))
        
        # Postseason stats section
        for col_name in SECTIONS_CONFIG['postseason']['columns']:
            if col_name == 'years':
                team_row.append(team_as_player.get('playoff_seasons_played', '') or '')
            elif col_name == 'games':
                seasons = team_as_player.get('playoff_seasons_played', 0)
                games_val = playoff_team_calculated_stats.get('games', 0) / seasons if seasons and seasons > 0 and stats_mode != 'totals' else playoff_team_calculated_stats.get('games', 0)
                col_idx = get_column_index(col_name, section='postseason', for_nba_sheet=for_nba_sheet)
                has_playoff = playoff_team_stats and (playoff_team_stats.get('minutes_total', 0) or 0) > 0
                # For games, use team_row_percentiles (not team percentiles)
                team_row.append(get_display_value(games_val, team_row_percentiles.get(col_idx), col_idx) if has_playoff else '')
            else:
                team_row.append(get_team_stat_value(col_name, playoff_team_calculated_stats, playoff_team_stats and (playoff_team_stats.get('minutes_total', 0) or 0) > 0, 'postseason'))
        
        # Hidden ID column
        team_row.append(str(team_stats.get('team_id', '')))
        
        data_rows.append(team_row)
        percentile_data.append(team_row_percentiles)
        
        # ============================================================================
        # BUILD OPPONENT ROW - Shows what opponents did AGAINST this team
        # ============================================================================
        from src.stat_engine import calculate_entity_stats, calculate_percentiles_generic, get_opponent_stat_name
        from config.sheets import OPPONENT_STAT_ORDER
        
        # Calculate opponent stats in current mode using generic engine
        opponent_stats_current = calculate_entity_stats(team_stats, OPPONENT_STAT_ORDER, stats_mode, stats_custom_value) if team_stats else {}
        opponent_stats_historical = calculate_entity_stats(historical_team_stats, OPPONENT_STAT_ORDER, stats_mode, stats_custom_value) if historical_team_stats else {}
        opponent_stats_postseason = calculate_entity_stats(playoff_team_stats, OPPONENT_STAT_ORDER, stats_mode, stats_custom_value) if playoff_team_stats else {}
        
        # Calculate opponent percentiles (compare this team's opponent stats vs all other teams)
        opponent_percentiles_current, _ = calculate_percentiles_generic(
            team_data.get('all_teams_current', []),
            OPPONENT_STAT_ORDER,
            stats_mode,
            stats_custom_value,
            entity_type='team',
            use_minutes_weighting=False
        ) if team_data.get('all_teams_current') else ({}, [])
        
        opponent_percentiles_historical, _ = calculate_percentiles_generic(
            team_data.get('all_teams_historical', []),
            OPPONENT_STAT_ORDER,
            stats_mode,
            stats_custom_value,
            entity_type='team',
            use_minutes_weighting=False
        ) if team_data.get('all_teams_historical') else ({}, [])
        
        opponent_percentiles_postseason, _ = calculate_percentiles_generic(
            team_data.get('all_teams_playoff', []),
            OPPONENT_STAT_ORDER,
            stats_mode,
            stats_custom_value,
            entity_type='team',
            use_minutes_weighting=False
        ) if team_data.get('all_teams_playoff') else ({}, [])
        
        # Build opponent row
        opponent_row = []
        opponent_row_percentiles = {}
        
        # Name column
        opponent_row.append('Opponent')
        
        # Player Info section - mostly empty for opponent
        for col_name in SECTIONS_CONFIG['player_info']['columns']:
            if col_name == 'team' and for_nba_sheet:
                opponent_row.append('')  # No team for opponent
            else:
                opponent_row.append('')  # No player info for opponent
        
        # Notes column
        opponent_row.append('')
        
        # Current season opponent stats
        for col_name in SECTIONS_CONFIG['current']['columns']:
            col_idx = get_column_index(col_name, section='current', for_nba_sheet=for_nba_sheet)
            
            # Map regular stat name to opponent stat name using centralized helper
            opp_stat_name = get_opponent_stat_name(col_name)
            
            if opp_stat_name and opp_stat_name in OPPONENT_STAT_ORDER:
                # This is an opponent stat - display it
                value = opponent_stats_current.get(opp_stat_name, 0)
                col_def = COLUMN_DEFINITIONS[opp_stat_name]
                
                # Calculate percentile
                if opponent_percentiles_current and opp_stat_name in opponent_percentiles_current:
                    reverse = col_def.get('reverse_stat', False)
                    pct = get_percentile_rank(value, opponent_percentiles_current.get(opp_stat_name), reverse=reverse)
                    opponent_row_percentiles[col_idx] = pct
                    
                    # Display value or percentile
                    if show_percentiles and pct is not None:
                        opponent_row.append(int(round(pct)))
                    elif col_def.get('format_as_percentage'):
                        opponent_row.append(format_pct(value))
                    else:
                        opponent_row.append(format_stat(value))
                else:
                    opponent_row_percentiles[col_idx] = None
                    if col_def.get('format_as_percentage'):
                        opponent_row.append(format_pct(value))
                    else:
                        opponent_row.append(format_stat(value))
            else:
                # Not an opponent stat - leave empty (including games and minutes)
                opponent_row.append('')
                opponent_row_percentiles[col_idx] = None
        
        # Historical opponent stats
        for col_name in SECTIONS_CONFIG['historical']['columns']:
            col_idx = get_column_index(col_name, section='historical', for_nba_sheet=for_nba_sheet)
            
            # Map regular stat name to opponent stat name using centralized helper
            opp_stat_name = get_opponent_stat_name(col_name)
            
            if opp_stat_name and opp_stat_name in OPPONENT_STAT_ORDER:
                value = opponent_stats_historical.get(opp_stat_name, 0)
                col_def = COLUMN_DEFINITIONS[opp_stat_name]
                
                if opponent_percentiles_historical and opp_stat_name in opponent_percentiles_historical:
                    reverse = col_def.get('reverse_stat', False)
                    pct = get_percentile_rank(value, opponent_percentiles_historical.get(opp_stat_name), reverse=reverse)
                    opponent_row_percentiles[col_idx] = pct
                    
                    if show_percentiles and pct is not None:
                        opponent_row.append(int(round(pct)))
                    elif col_def.get('format_as_percentage'):
                        opponent_row.append(format_pct(value))
                    else:
                        opponent_row.append(format_stat(value))
                else:
                    opponent_row_percentiles[col_idx] = None
                    if col_def.get('format_as_percentage'):
                        opponent_row.append(format_pct(value))
                    else:
                        opponent_row.append(format_stat(value))
            else:
                # Not an opponent stat - leave empty (including years, games, minutes)
                opponent_row.append('')
                opponent_row_percentiles[col_idx] = None
        
        # Postseason opponent stats
        for col_name in SECTIONS_CONFIG['postseason']['columns']:
            col_idx = get_column_index(col_name, section='postseason', for_nba_sheet=for_nba_sheet)
            
            # Map regular stat name to opponent stat name using centralized helper
            opp_stat_name = get_opponent_stat_name(col_name)
            
            if opp_stat_name and opp_stat_name in OPPONENT_STAT_ORDER:
                value = opponent_stats_postseason.get(opp_stat_name, 0)
                col_def = COLUMN_DEFINITIONS[opp_stat_name]
                
                if opponent_percentiles_postseason and opp_stat_name in opponent_percentiles_postseason:
                    reverse = col_def.get('reverse_stat', False)
                    pct = get_percentile_rank(value, opponent_percentiles_postseason.get(opp_stat_name), reverse=reverse)
                    opponent_row_percentiles[col_idx] = pct
                    
                    if show_percentiles and pct is not None:
                        opponent_row.append(int(round(pct)))
                    elif col_def.get('format_as_percentage'):
                        opponent_row.append(format_pct(value))
                    else:
                        opponent_row.append(format_stat(value))
                else:
                    opponent_row_percentiles[col_idx] = None
                    if col_def.get('format_as_percentage'):
                        opponent_row.append(format_pct(value))
                    else:
                        opponent_row.append(format_stat(value))
            else:
                # Not an opponent stat - leave empty (including years, games, minutes)
                opponent_row.append('')
                opponent_row_percentiles[col_idx] = None
        
        # Hidden ID column - empty for opponent
        opponent_row.append('')
        
        data_rows.append(opponent_row)
        percentile_data.append(opponent_row_percentiles)
    
    # Combine all data (Row 1: main sections, Row 2: subsections, Row 3: column headers, Row 4: filters)
    all_data = [header_row_1, header_row_2, header_row_3, filter_row] + data_rows
    
    spreadsheet = worksheet.spreadsheet
    total_rows = 4 + len(data_rows)  # 4 header rows + data rows
    total_cols = SHEET_FORMAT_CONFIG['total_columns']
    
    # Build ONE mega batch request with ALL operations
    # This reduces from 6+ API calls per sheet to just 1!
    requests = []
    
    # 1. First, get current sheet metadata (we need this for sheet dimensions)
    try:
        sheet_metadata = spreadsheet.fetch_sheet_metadata({'includeGridData': False})
        current_row_count = 1000
        current_col_count = 26
        sheet_id = worksheet.id
        
        for sheet in sheet_metadata.get('sheets', []):
            if sheet['properties']['sheetId'] == sheet_id:
                current_row_count = sheet['properties']['gridProperties'].get('rowCount', 1000)
                current_col_count = sheet['properties']['gridProperties'].get('columnCount', 26)
                
                # Add delete banding requests if needed
                banded_ranges = sheet.get('bandedRanges', [])
                if banded_ranges:
                    for br in banded_ranges:
                        requests.append({'deleteBanding': {'bandedRangeId': br['bandedRangeId']}})
                
                # Unmerge any existing merged cells in row 1 (headers) to avoid conflicts
                merges = sheet.get('merges', [])
                if merges:
                    for merge in merges:
                        if merge.get('startRowIndex', 0) == 0 and merge.get('endRowIndex', 1) == 1:
                            # This is a header row merge, unmerge it
                            requests.append({
                                'unmergeCells': {
                                    'range': merge
                                }
                            })
                break
    except Exception as e:
        log(f"⚠️  Warning: Could not fetch sheet metadata for {team_abbr}: {e}")
        sheet_id = worksheet.id
        current_row_count = 1000
        current_col_count = 26
    
    # 2. Adjust columns if needed (BEFORE updateCells!)
    # ONLY adjust dimensions when doing a full sync (not partial section updates)
    if sync_section in [None, 'all', 'current']:
        if current_col_count > total_cols:
            requests.append({
                'deleteDimension': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': total_cols,
                        'endIndex': current_col_count
                    }
                }
            })
        elif current_col_count < total_cols:
            # Add columns if we need more (for playoff section)
            requests.append({
                'appendDimension': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'length': total_cols - current_col_count
                }
            })
    
    # 3. Adjust rows if needed
    # ONLY adjust dimensions when doing a full sync (not partial section updates)
    if sync_section in [None, 'all', 'current']:
        if current_row_count > total_rows:
            requests.append({
                'deleteDimension': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': total_rows,
                        'endIndex': current_row_count
                    }
                }
            })
        elif current_row_count < total_rows:
            # Add rows if we need more
            requests.append({
                'appendDimension': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'length': total_rows - current_row_count
                }
            })
    
    # 4. Update cell values (via updateCells instead of separate update call)
    # For partial syncs, only update the relevant column range
    rows_data = []
    start_col_idx = 0
    
    if sync_section == 'historical':
        start_col_idx = SECTIONS_CONFIG['historical']['columns']['start']
        end_col_idx = SECTIONS_CONFIG['historical']['columns']['end'] + 1  # +1 for exclusive end
    elif sync_section == 'postseason':
        start_col_idx = SECTIONS_CONFIG['postseason']['columns']['start']
        end_col_idx = SECTIONS_CONFIG['postseason']['columns']['end'] + 1  # +1 for exclusive end
    else:
        # Full sync - write all columns
        end_col_idx = total_cols
    
    try:
        for row_idx, row_data in enumerate(all_data):
            row_values = []
            # Only process columns in the target range
            for col_idx in range(start_col_idx, end_col_idx):
                cell_value = row_data[col_idx] if col_idx < len(row_data) else ''
                
                # Handle empty cells
                if cell_value is None or cell_value == '':
                    row_values.append({
                        'userEnteredValue': {
                            'stringValue': ''
                        }
                    })
                # Try to store as number if it's numeric (for proper sorting)
                elif isinstance(cell_value, (int, float)):
                    row_values.append({
                        'userEnteredValue': {
                            'numberValue': float(cell_value)
                        }
                    })
                # Store as string for text values
                else:
                    try:
                        str_value = str(cell_value)
                        row_values.append({
                            'userEnteredValue': {
                                'stringValue': str_value
                            }
                        })
                    except Exception as e:
                        log(f"Error converting cell at row {row_idx}, col {col_idx}: value={cell_value}, type={type(cell_value)}, error={e}")
                        raise
            rows_data.append({'values': row_values})
    except Exception as e:
        log(f"Error building rows_data: {e}")
        log(f"First row data types: {[type(v) for v in all_data[0]] if all_data else 'empty'}")
        raise
    
    requests.append({
        'updateCells': {
            'rows': rows_data,
            'fields': 'userEnteredValue',
            'start': {'sheetId': sheet_id, 'rowIndex': 0, 'columnIndex': start_col_idx}
        }
    })
    
    # Only apply formatting, merges, and column widths during full syncs
    if sync_section in [None, 'all', 'current']:
        # 2.4. Apply custom number formatting for height and wingspan columns (feet-inches display)
        # Format: 0"-"0 displays 80 as 6-8 (integer division by 12 gives feet, modulo gives inches)
        # BUT: When showing percentiles, clear this formatting so numbers display normally
        height_col_idx = get_column_index('height', section='player_info', for_nba_sheet=for_nba_sheet)
        wingspan_col_idx = get_column_index('wingspan', section='player_info', for_nba_sheet=for_nba_sheet)
        
        for col_idx in [height_col_idx, wingspan_col_idx]:
            if show_percentiles:
                # Clear custom formatting - use default number format
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 3,  # Start after headers (rows 1-3)
                            'endRowIndex': total_rows,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '0'  # Simple integer format
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
            else:
                # Apply feet-inches formatting
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 3,  # Start after headers (rows 1-3)
                            'endRowIndex': total_rows,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '0"-"0'  # Custom format: displays 80 as 6-8
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })
        
        # 2.5. Set column A (NAME) to width from config
        name_width = COLUMN_DEFINITIONS['name'].get('width', 187)
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': 1
                },
                'properties': {
                    'pixelSize': name_width
                },
                'fields': 'pixelSize'
            }
        })
        
        # 2.6. Set column A text wrapping to CLIP (don't wrap or truncate visibly)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'startColumnIndex': 0,
                    'endColumnIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'wrapStrategy': 'CLIP'
                    }
                },
                'fields': 'userEnteredFormat.wrapStrategy'
            }
        })
        
        # 5. Merge cells for section headers (row 1)
        # For NBA sheet: Team column (B1) merges with Player Info (C-H)
        # For team sheets: Only merge Player Info if multiple columns (B-G)
        if for_nba_sheet:
            # NBA sheet: merge from team column (B=index 1) through player info end (H)
            # Name section has 2 columns (A=name, B=team), so team starts at index 1
            team_col_idx = 1  # Column B (team column)
            player_info_end = SECTIONS_CONFIG['player_info']['end_col']
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': team_col_idx,  # Start at column B (team)
                        'endColumnIndex': player_info_end,  # End at player info end (H)
                    },
                    'mergeType': 'MERGE_ALL'
                }
            })
        else:
            # Team sheet: only merge player info if multiple columns
            player_info_cols = SECTIONS_CONFIG['player_info']['column_count']
            if player_info_cols > 1:
                requests.append({
                    'mergeCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': SECTIONS_CONFIG['player_info']['start_col'],
                            'endColumnIndex': SECTIONS_CONFIG['player_info']['end_col'],
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                })
        
        # Current season stats header
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': SECTIONS_CONFIG['current']['start_col'],
                    'endColumnIndex': SECTIONS_CONFIG['current']['end_col'],
                },
                'mergeType': 'MERGE_ALL'
            }
        })
    
        # Historical stats header
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': SECTIONS_CONFIG['historical']['start_col'],
                    'endColumnIndex': SECTIONS_CONFIG['historical']['end_col'],
                },
                'mergeType': 'MERGE_ALL'
            }
        })
        
        # Postseason stats header
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': SECTIONS_CONFIG['postseason']['start_col'],
                    'endColumnIndex': SECTIONS_CONFIG['postseason']['end_col'],
                },
                'mergeType': 'MERGE_ALL'
            }
        })
        
        # Notes section - merge header
        notes_cols = SECTIONS_CONFIG['notes']['column_count']
        if notes_cols > 1:
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': SECTIONS_CONFIG['notes']['start_col'],
                        'endColumnIndex': SECTIONS_CONFIG['notes']['end_col'],
                    },
                    'mergeType': 'MERGE_ALL'
                }
            })
    
    # Row 2: Merge subsection headers (Rates, Scoring, Distribution, Rebounding, Defense, On/Off)
    # Find consecutive cells with the same subsection name and merge them
    subsection_merges = []
    current_subsection = None
    start_col = None
    
    for col_idx, subsection in enumerate(header_row_2):
        if subsection and subsection != '':
            if subsection == current_subsection:
                # Continue current subsection
                continue
            else:
                # Save previous subsection merge if exists
                if current_subsection and start_col is not None and col_idx > start_col + 1:
                    subsection_merges.append({
                        'start': start_col,
                        'end': col_idx,
                        'name': current_subsection
                    })
                # Start new subsection
                current_subsection = subsection
                start_col = col_idx
        else:
            # Empty cell - save previous subsection if exists
            if current_subsection and start_col is not None and col_idx > start_col + 1:
                subsection_merges.append({
                    'start': start_col,
                    'end': col_idx,
                    'name': current_subsection
                })
            current_subsection = None
            start_col = None
    
    # Handle last subsection if exists
    if current_subsection and start_col is not None and len(header_row_2) > start_col + 1:
        subsection_merges.append({
            'start': start_col,
            'end': len(header_row_2),
            'name': current_subsection
        })
    
    # Apply subsection merges
    for merge in subsection_merges:
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,  # Row 2 (0-indexed)
                    'endRowIndex': 2,
                    'startColumnIndex': merge['start'],
                    'endColumnIndex': merge['end'],
                },
                'mergeType': 'MERGE_ALL'
            }
        })
    
    # Hide Row 2 by default (subsection headers shown only in ADVANCED view)
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'ROWS',
                'startIndex': 1,  # Row 2 (0-indexed)
                'endIndex': 2
            },
            'properties': {
                'hiddenByUser': True
            },
            'fields': 'hiddenByUser'
        }
    })
    
    # Format row 1
    black = COLORS['black']
    white = COLORS['white']
    light_gray = COLORS['light_gray']
    
    # Format row 1 - PRIMARY HEADER (font 12)
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 1,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT_CONFIG['fonts']['header_primary']['family'],
                        'fontSize': 12,  # Row 1 is always 12
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format row 2 - SUBSECTION HEADERS (font 9, hidden by default)
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 1,
                'endRowIndex': 2,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT_CONFIG['fonts']['header_secondary']['family'],
                        'fontSize': 9,  # Row 2 subsection headers
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format row 3 - COLUMN HEADERS (font 10)
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 2,
                'endRowIndex': 3,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT_CONFIG['fonts']['header_secondary']['family'],
                        'fontSize': 10,  # Row 3 column headers
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format A1 - TEAM NAME (font 12)
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 0,
                'endColumnIndex': 1
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT_CONFIG['fonts']['team_name']['family'],
                        'fontSize': 15,  # A1 is 15 (team name)
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format filter row (row 4) - (font 10)
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 3,
                'endRowIndex': 4,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT_CONFIG['fonts']['header_primary']['family'],
                        'fontSize': 10,  # Row 4 is filter row
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format data rows (centered by default)
    # IMPORTANT: Use specific fields to avoid overwriting background colors set by percentile logic
    if len(data_rows) > 0:
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 4,  # Data starts at row 5 (0-indexed = 4)
                    'endRowIndex': 4 + len(data_rows),
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontFamily': SHEET_FORMAT_CONFIG['fonts']['data']['family'],
                            'fontSize': 10  # Changed from SHEET_FORMAT size (9) to 10
                        },
                        'wrapStrategy': 'CLIP',
                        'verticalAlignment': 'TOP',
                        'horizontalAlignment': 'CENTER'
                    }
                },
                'fields': 'userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment,userEnteredFormat.horizontalAlignment'
            }
        })
        
        # Left-align column A
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'LEFT'
                    }
                },
                'fields': 'userEnteredFormat.horizontalAlignment'
            }
        })
        
        # Left-align Notes column and set to clip overflow text
        notes_col_idx = get_column_index('notes')
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': notes_col_idx,
                    'endColumnIndex': notes_col_idx + 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'LEFT',
                        'wrapStrategy': 'CLIP'
                    }
                },
                'fields': 'userEnteredFormat.horizontalAlignment,userEnteredFormat.wrapStrategy'
            }
        })
    
    # Bold column A (with Sofia Sans font, size 12)
    if len(data_rows) > 0:
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': 1,
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'bold': True,
                            'fontFamily': SHEET_FORMAT_CONFIG['fonts']['player_names']['family'],  # Sofia Sans
                            'fontSize': 10  # Column A font size 10 (player names)
                        }
                    }
                },
                'fields': 'userEnteredFormat.textFormat'
            }
        })
        
        # Apply banding to ALL data columns FIRST (so empty cells get alternating colors)
        if len(data_rows) > 0:
            requests.append({
                'addBanding': {
                    'bandedRange': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 3,
                            'endRowIndex': 3 + len(data_rows),
                            'startColumnIndex': 0,
                            'endColumnIndex': SHEET_FORMAT_CONFIG['total_columns']
                        },
                        'rowProperties': {
                            'firstBandColor': white,
                            'secondBandColor': light_gray
                        }
                    }
                }
            })
            
    # Apply percentile colors to stat cells (for both full and partial syncs)
    # For partial syncs, only color the columns in the target section
    if len(data_rows) > 0:
        for row_idx, player_percentiles in enumerate(percentile_data):
            for col_idx, percentile in player_percentiles.items():
                # Skip if this column is not in the target section for partial syncs
                if sync_section == 'historical':
                    if not (start_col_idx <= col_idx < end_col_idx):
                        continue
                elif sync_section == 'postseason':
                    if not (start_col_idx <= col_idx < end_col_idx):
                        continue
                
                if percentile is not None:
                    color = get_color_for_percentile(percentile)
                    if color:
                        requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': 3 + row_idx,
                                    'endRowIndex': 3 + row_idx + 1,
                                    'startColumnIndex': col_idx,
                                    'endColumnIndex': col_idx + 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'backgroundColor': color
                                    }
                                },
                                'fields': 'userEnteredFormat.backgroundColor'
                            }
                        })
    
    # End of full-sync-only formatting section
    
    
    # Row heights - Set row 3 (filter row) to 15 pixels
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'ROWS',
                'startIndex': 2,  # Row 3 (0-indexed)
                'endIndex': 3
            },
            'properties': {
                'pixelSize': 15
            },
            'fields': 'pixelSize'
        }
    })
    
    # Set all data rows to fixed height (21 pixels) to prevent expansion
    if len(data_rows) > 0:
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'startIndex': 3,  # Start after header rows
                    'endIndex': 3 + len(data_rows)
                },
                'properties': {
                    'pixelSize': 21
                },
                'fields': 'pixelSize'
            }
        })
    
    # Define header_rows for use in both full and partial syncs
    header_rows = SHEET_FORMAT.get('header_rows', 2)
    
    # CONFIG-DRIVEN BORDERS for stat sections (only for full syncs)
    if sync_section in [None, 'all', 'current']:
        # Add borders based on SECTIONS config
        
        # Iterate through sections that have borders configured
        for section_key in ['current', 'historical', 'postseason']:
            section = SECTIONS.get(section_key)
            if not section or not section.get('has_border'):
                continue
                
            border_cfg = section.get('border_config', {})
            start_col = section['columns']['start']
            end_col = section['columns']['end']
            weight = border_cfg.get('weight', 2)
            header_color = COLORS[border_cfg.get('header_color', 'white')]
            data_color = COLORS[border_cfg.get('data_color', 'black')]
            
            # Left border on FIRST column only (if configured)
            if border_cfg.get('first_column_left'):
                # Header rows (white)
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': header_rows + 1,
                            'startColumnIndex': start_col,
                            'endColumnIndex': start_col + 1,
                        },
                        'left': {
                            'style': 'SOLID',
                            'width': weight,
                            'color': header_color
                        }
                    }
                })
            
                # Data rows (black)
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': header_rows + 1,
                            'endRowIndex': header_rows + 1 + len(data_rows),
                            'startColumnIndex': start_col,
                            'endColumnIndex': start_col + 1,
                        },
                        'left': {
                            'style': 'SOLID',
                            'width': weight,
                            'color': data_color
                        }
                    }
                })
        
            # Right border on LAST column only (if configured)
            if border_cfg.get('last_column_right'):
                # Row 1 only (white, weight 2)
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': end_col,
                            'endColumnIndex': end_col + 1,
                        },
                        'right': {
                            'style': 'SOLID',
                            'width': weight,
                            'color': header_color
                        }
                    }
                })
                
                # Rows 2-3 (header rows, white)
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': header_rows + 1,
                            'startColumnIndex': end_col,
                            'endColumnIndex': end_col + 1,
                        },
                        'right': {
                            'style': 'SOLID',
                            'width': weight,
                            'color': header_color
                        }
                    }
                })
                
                # Data rows (black)
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': header_rows + 1,
                            'endRowIndex': header_rows + 1 + len(data_rows),
                            'startColumnIndex': end_col,
                            'endColumnIndex': end_col + 1,
                        },
                        'right': {
                            'style': 'SOLID',
                            'width': weight,
                            'color': data_color
                        }
                    }
                })
    
    # Top border for row 2 (white, across all columns)
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 1,
                'endRowIndex': 2,
                'startColumnIndex': 0,
                'endColumnIndex': SHEET_FORMAT_CONFIG['total_columns']
            },
            'top': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    # WHITE borders between all columns in header rows (rows 2-3)
    # Skip column 0 (name section) - start from column 1 onwards
    name_end_col = SECTIONS_CONFIG['name']['end_col']
    for col_idx in range(name_end_col, SHEET_FORMAT_CONFIG['total_columns'] - 1):
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': 3,  # Rows 2-3 only
                    'startColumnIndex': col_idx,
                    'endColumnIndex': col_idx + 1,
                },
                'right': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': white
                }
            }
        })
    
    # Add white left border to J# column (jersey) in rows 2-3 for all sheets
    jersey_col = get_column_index('jersey', for_nba_sheet=for_nba_sheet)
    if jersey_col is not None:
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,  # Row 2
                    'endRowIndex': 3,    # Through row 3
                    'startColumnIndex': jersey_col,
                    'endColumnIndex': jersey_col + 1,
                },
                'left': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': white
                }
            }
        })
    
    # SECTION BOUNDARIES - Add borders at start of each section (except first)
    # WHITE borders in header rows 2-3, BLACK borders in data rows 4+
    # Also add RIGHT borders for stat sections
    black = {'red': 0, 'green': 0, 'blue': 0}
    for section_name, section_info in SECTIONS_CONFIG.items():
        if section_name in ['name', 'player_info', 'hidden']:  # Skip name, player_info, and hidden sections
            continue
        
        section_start_col = section_info['start_col']
        section_end_col = section_info['end_col']
        
        # LEFT border - White border in row 1
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': section_start_col,
                    'endColumnIndex': section_start_col + 1,
                },
                'left': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': white
                }
            }
        })
        
        # LEFT border - Black border from row 4 down (data rows), skip column A
        if len(data_rows) > 0 and section_start_col > 0:  # Don't add left border to column A
            requests.append({
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 3,  # Row 4 (data starts)
                        'endRowIndex': 3 + len(data_rows),
                        'startColumnIndex': section_start_col,
                        'endColumnIndex': section_start_col + 1,
                    },
                    'left': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': black
                    }
                }
            })
        
        # RIGHT border for stat sections (notes, current, historical, postseason)
        if section_name in ['notes', 'current', 'historical', 'postseason']:
            # White border in header rows 0-3 (all header rows including row 1)
            requests.append({
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 3,
                        'startColumnIndex': section_end_col - 1,
                        'endColumnIndex': section_end_col,
                    },
                    'right': {
                        'style': 'SOLID',
                        'width': 2,
                        'color': white
                    }
                }
            })
            
            # Black border from row 4 down (data rows)
            if len(data_rows) > 0:
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 3,
                            'endRowIndex': 3 + len(data_rows),
                            'startColumnIndex': section_end_col - 1,
                            'endColumnIndex': section_end_col,
                        },
                        'right': {
                            'style': 'SOLID',
                            'width': 2,
                            'color': black
                        }
                    }
                })
    
    # Add LEFT border to hidden column (creates right border for postseason section)
    hidden_col = SECTIONS_CONFIG['hidden']['start_col']
    # White border in header rows 0-3
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': hidden_col,
                'endColumnIndex': hidden_col + 1,
            },
            'left': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    # Black border in data rows
    if len(data_rows) > 0:
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': hidden_col,
                    'endColumnIndex': hidden_col + 1,
                },
                'left': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': black
                }
            }
        })
    
    # Add black border ABOVE the Team Row for visual separation
    # Team row is after all player rows: header(3) + players
    # data_rows includes players + team + opponents, so team is at len(players)
    if team_data and len(data_rows) >= 2:  # Need at least team + opponents rows
        team_row_index = 3 + (len(data_rows) - 2)  # -2 to exclude team and opponents from player count
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': team_row_index,
                    'endRowIndex': team_row_index + 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': SHEET_FORMAT_CONFIG['total_columns']
                },
                'top': {
                    'style': 'SOLID_THICK',
                    'width': 3,
                    'color': black
                }
            }
        })
    
    # Freeze panes
    # Both NBA sheet and team sheets freeze rows and column A
    freeze_props = {
        'sheetId': sheet_id,
        'gridProperties': {
            'frozenRowCount': SHEET_FORMAT_CONFIG['frozen']['rows'],
            'frozenColumnCount': SHEET_FORMAT_CONFIG['frozen']['columns']
        }
    }
    
    requests.append({
        'updateSheetProperties': {
            'properties': freeze_props,
            'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'
        }
    })
    
    # Hide gridlines
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {
                    'hideGridlines': True
                }
            },
            'fields': 'gridProperties.hideGridlines'
        }
    })
    
    # Hide player_id column
    player_id_col_idx = get_column_index(PLAYER_ID_COLUMN, for_nba_sheet=for_nba_sheet)
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': player_id_col_idx,
                'endIndex': player_id_col_idx + 1
            },
            'properties': {
                'hiddenByUser': True
            },
            'fields': 'hiddenByUser'
        }
    })
    
    # Add filter
    requests.append({
        'setBasicFilter': {
            'filter': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': SHEET_FORMAT_CONFIG['total_columns']
                }
            }
        }
    })
    # End of full-sync-only operations
    
    # SET COLUMN WIDTHS - Auto-resize or fixed based on config
    for col_idx in range(SHEET_FORMAT_CONFIG['total_columns']):
        # Determine which column this is
        col_name = None
        col_width = None
        
        # Check all sections using SECTIONS_CONFIG (not hardcoded SECTIONS)
        for section_name, section_info in SECTIONS_CONFIG.items():
            section_start = section_info['start_col']
            section_cols = section_info['columns']
            if section_start <= col_idx < section_start + len(section_cols):
                col_name = section_cols[col_idx - section_start]
                if col_name in COLUMN_DEFINITIONS:
                    col_width = COLUMN_DEFINITIONS[col_name].get('width')
                break
        
        # Apply width: fixed if specified, auto-resize otherwise
        if col_width is not None:
            # Fixed width
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': col_idx,
                        'endIndex': col_idx + 1
                    },
                    'properties': {
                        'pixelSize': col_width
                    },
                    'fields': 'pixelSize'
                }
            })
        else:
            # Auto-resize
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': col_idx,
                        'endIndex': col_idx + 1
                    }
                }
            })
    
    # OVERRIDE first column width for stat sections (after width setting)
    for section_name, section_info in SECTIONS_CONFIG.items():
        if 'first_column_width' in section_info:
            first_col_idx = section_info['start_col']
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': first_col_idx,
                        'endIndex': first_col_idx + 1
                    },
                    'properties': {
                        'pixelSize': section_info['first_column_width']
                    },
                    'fields': 'pixelSize'
                }
            })
    
    # Set up basic filter (row 3 onwards, excluding Team and Opponent rows)
    # Team and Opponent rows are the last 2 rows in data_rows
    if team_data and len(data_rows) >= 2:
        # Filter should only include player rows (exclude last 2 rows which are Team and Opponent)
        filter_end_row = 3 + len(data_rows) - 2  # Exclude Team and Opponent
    else:
        # No team/opponent rows, filter all data rows
        filter_end_row = 3 + len(data_rows)
    
    if len(data_rows) > 0:
        requests.append({
            'setBasicFilter': {
                'filter': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 2,  # Row 3 (filter row)
                        'endRowIndex': filter_end_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': total_cols
                    }
                }
            }
        })
    
    # Execute all requests in ONE batch call with retry logic
    try:
        log(f"Executing batch update with {len(requests)} requests for {team_abbr}")
        spreadsheet.batch_update({'requests': requests})
    except Exception as e:
        log(f"⚠️  Error in batch update for {team_abbr}: {type(e).__name__}: {str(e)}")
        log(f"Error details - Total requests: {len(requests)}")
        # Log the type of error more clearly
        import traceback
        log(f"Full traceback:\n{traceback.format_exc()}")
        
        log("Retrying after delay...")
        time.sleep(3)
        try:
            spreadsheet.batch_update({'requests': requests})
            log(f"✅ Retry successful for {team_abbr}")
        except Exception as e2:
            log(f"❌ Failed batch update for {team_abbr} after retry: {e2}")
            raise
    
    log(f"✅ {team_name} sheet created with {len(data_rows)} players")

def create_nba_sheet(worksheet, nba_players, percentiles, historical_percentiles,
                     past_years=3, stats_mode='per_36', stats_custom_value=None, specific_seasons=None,
                     include_current=False, sync_section=None, show_percentiles=False, playoff_percentiles=None):
    """Create/update the NBA sheet with all players - wrapper around create_team_sheet with for_nba_sheet=True
    
    This uses the same logic as team sheets but with:
    - Team column added after name column  
    - Uses SECTIONS_NBA, HEADERS_NBA, and SHEET_FORMAT_NBA configurations
    
    Note: NBA sheet does NOT include Team/Opponents rows (those are only for individual team sheets)
    """
    # Simply call create_team_sheet with for_nba_sheet=True (no team_data for NBA sheet)
    return create_team_sheet(
        worksheet=worksheet,
        team_abbr='NBA',
        team_name='NBA',
        team_players=nba_players,
        percentiles=percentiles,
        historical_percentiles=historical_percentiles,
        past_years=past_years,
        stats_mode=stats_mode,
        stats_custom_value=stats_custom_value,
        specific_seasons=specific_seasons,
        include_current=include_current,
        sync_section=sync_section,
        show_percentiles=show_percentiles,
        playoff_percentiles=playoff_percentiles,
        for_nba_sheet=True
    )

def main(priority_team=None):
    log("=" * 60)
    log("SYNCING ALL 30 NBA TEAMS TO GOOGLE SHEETS")
    if priority_team:
        log(f"Priority team: {priority_team} (will be updated first)")
    log("=" * 60)
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        log("✅ Database connection established")
    except Exception as e:
        log(f"❌ Database connection error: {e}")
        return False
    
    # Fetch all players data
    log("Fetching all players data for percentile calculation...")
    all_players = fetch_all_players_data(conn)
    log(f"✅ Fetched {len(all_players)} total players")
    
    # Read stats mode from environment (Issue #5: Fix defaults)
    stats_mode = os.environ.get('STATS_MODE', 'per_100_poss')  # DEFAULT: per 100 possessions
    stats_custom_value = os.environ.get('STATS_CUSTOM_VALUE')
    sync_section = os.environ.get('SYNC_SECTION')  # None = full sync, 'historical' or 'postseason' for partial
    toggle_percentiles = os.environ.get('TOGGLE_PERCENTILES', 'false').lower() == 'true'
    # Note: show_percentiles is parsed from sheet header, not from environment
    log(f"Using stats mode: {stats_mode}" + (f" (custom value: {stats_custom_value})" if stats_custom_value else ""))
    log(f"Sync section: {sync_section}")
    if toggle_percentiles:
        log("Toggle percentiles: ON (will flip current state)")
    
    # Parse historical stats configuration from environment variables (Issue #5: Fix defaults)
    past_years = 3  # Default to 3 years
    include_current = False  # DEFAULT: exclude current season from historical stats
    specific_seasons = None
    
    historical_mode = os.environ.get('HISTORICAL_MODE', 'years')  # Default to years mode (3 years)
    include_current_env = os.environ.get('INCLUDE_CURRENT_YEAR', 'false')  # DEFAULT: 'false'
    include_current = (include_current_env.lower() == 'true')
    
    log(f"Historical mode: {historical_mode}, include_current={include_current}")
    
    if historical_mode == 'seasons':
        # Parse specific seasons
        # When user enters a single season like "2010-11", it means "since 2010-11" (all seasons from then to now)
        # When user enters multiple seasons like "2010-11, 2015-16", it means only those specific seasons
        seasons_str = os.environ.get('HISTORICAL_SEASONS', '')
        if seasons_str:
            season_list = [s.strip() for s in seasons_str.split(',')]
            
            if len(season_list) == 1:
                # Single season = "since" that season (expand to full range)
                season = season_list[0]
                if '-' in season or '/' in season:
                    start_year_str = season.split('-')[0] if '-' in season else season.split('/')[0]
                    # Convert to 4-digit year (e.g., "2021-22" -> 2021, "10-11" -> 2010)
                    # The database stores the starting year of the season
                    if len(start_year_str) == 4:
                        start_year = int(start_year_str)
                    elif len(start_year_str) == 2:
                        # Handle 2-digit years (e.g., "10" in "10-11")
                        yr = int(start_year_str)
                        start_year = 2000 + yr if yr >= 0 else 1900 + yr
                    else:
                        start_year = int(start_year_str)
                    
                    # Generate all years from start_year through current
                    current_year = NBA_CONFIG['current_season_year']
                    specific_seasons = list(range(start_year, current_year + 1))
                    log(f"Expanding single season '{season}' to range since that year: {start_year} to {current_year} = {len(specific_seasons)} seasons")
            else:
                # Multiple seasons = use exactly those seasons
                specific_seasons = []
                for season in season_list:
                    if '-' in season or '/' in season:
                        start_year_str = season.split('-')[0] if '-' in season else season.split('/')[0]
                        # Convert to 4-digit year (database uses starting year of season)
                        if len(start_year_str) == 4:
                            year = int(start_year_str)
                        elif len(start_year_str) == 2:
                            yr = int(start_year_str)
                            year = 2000 + yr if yr >= 0 else 1900 + yr
                        else:
                            year = int(start_year_str)
                        specific_seasons.append(year)
                log(f"Using specific seasons only: {specific_seasons}")
    elif historical_mode == 'career':
        past_years = 25  # Use max years for career stats
        log("Using career mode (all available seasons)")
    else:
        # Use number of years
        past_years_env = os.environ.get('HISTORICAL_YEARS')
        if past_years_env:
            try:
                past_years = int(past_years_env)
                past_years = max(1, min(25, past_years))  # Clamp between 1-25
            except ValueError:
                log(f"⚠️  Invalid HISTORICAL_YEARS value '{past_years_env}', using default: {past_years}")
    
    # Fetch historical data
    if specific_seasons:
        # If include_current is true and current season not in list, add it
        seasons_to_fetch = specific_seasons.copy() if specific_seasons else []
        if include_current:
            current_season_year = NBA_CONFIG['current_season_year']
            if current_season_year not in seasons_to_fetch:
                seasons_to_fetch.append(current_season_year)
                log(f"Including current season ({current_season_year}) in specific seasons")
        log(f"Fetching historical data for specific seasons: {seasons_to_fetch}...")
        historical_players = fetch_historical_players_data(conn, specific_seasons=seasons_to_fetch)
    else:
        log(f"Fetching historical data for past {past_years} seasons (include_current={include_current})...")
        historical_players = fetch_historical_players_data(conn, past_years, include_current)
    
    log(f"✅ Fetched historical data for {len(historical_players)} players")
    
    # Fetch playoff data (respect user's year range selection)
    log(f"Fetching playoff data for past {past_years} seasons...")
    if specific_seasons:
        playoff_players = fetch_playoff_players_data(conn, specific_seasons=specific_seasons)
    else:
        playoff_players = fetch_playoff_players_data(conn, past_years=past_years)
    log(f"✅ Fetched playoff data for {len(playoff_players)} players")
    
    # Don't close connection yet - we need it for fetching NBA players data
    
    # Connect to Google Sheets
    spreadsheet_name = GOOGLE_SHEETS_CONFIG['spreadsheet_name']
    try:
        gc = get_google_sheets_client()
        spreadsheet = gc.open(spreadsheet_name)
        log(f"✅ Opened spreadsheet: {spreadsheet_name}")
            
    except gspread.SpreadsheetNotFound:
        log(f"❌ Spreadsheet '{spreadsheet_name}' not found")
        return False
    except Exception as e:
        log(f"❌ Error connecting to Google Sheets: {e}")
        return False
    
    # Read configuration from the first sheet to check if it differs
    # This ensures all sheets use the same configuration
    first_sheet = spreadsheet.get_worksheet(0)
    existing_mode, existing_custom, existing_historical, existing_show_percentiles = parse_sheet_config(first_sheet)
    
    # ALWAYS use the provided configuration (from environment/API) - don't preserve old config
    # The API/environment variables represent the USER'S CURRENT REQUEST
    final_stats_mode = stats_mode
    final_custom_value = stats_custom_value
    final_past_years = past_years
    final_include_current = include_current
    final_specific_seasons = specific_seasons
    # Use existing percentile setting from sheet (always preserve, unless toggling)
    final_show_percentiles = existing_show_percentiles if existing_show_percentiles is not None else False
    
    # Apply toggle if requested
    if toggle_percentiles:
        final_show_percentiles = not final_show_percentiles
        log(f"🔄 Toggling percentiles: {not final_show_percentiles} → {final_show_percentiles}")
    
    log(f"Using NEW configuration for all sheets: mode={final_stats_mode}, years={final_past_years}, include_current={final_include_current}")
    
    # Check if we need to re-fetch historical data (compare new config to existing)
    need_refetch = False
    if existing_historical:
        # Config came from sheet - check if it differs from what we're trying to set
        existing_years, existing_include, existing_seasons = existing_historical
        if final_specific_seasons != existing_seasons:
            need_refetch = True
            log("Historical config changed (seasons differ), re-fetching data...")
        elif final_past_years != existing_years or final_include_current != existing_include:
            need_refetch = True
            log(f"Historical config changed (years: {existing_years}->{final_past_years}, include_current: {existing_include}->{final_include_current}), re-fetching data...")
        else:
            log("ℹ️  Historical config unchanged from existing sheet")
    else:
        # No existing config - this is a new configuration
        need_refetch = True
        log("No existing historical config found, fetching data...")
    
    # Re-fetch historical data only if config changed
    if need_refetch:
        # Reopen connection if it was closed
        if conn.closed:
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
        
        if final_specific_seasons:
            historical_players = fetch_historical_players_data(conn, specific_seasons=final_specific_seasons)
        else:
            historical_players = fetch_historical_players_data(conn, final_past_years, final_include_current)
        
        # Don't close connection yet - we still need it for team stats and NBA players
        log(f"✅ Re-fetched historical data for {len(historical_players)} players")
    else:
        log("ℹ️  Using already-fetched historical data (config unchanged)")
    
    # Fetch team stats for all teams (reopen connection if needed)
    log("Fetching team stats for all teams...")
    if conn.closed:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
    
    all_teams_current = fetch_team_stats(conn)
    if final_specific_seasons:
        all_teams_historical = fetch_historical_team_stats(conn, specific_seasons=final_specific_seasons)
        all_teams_playoff = fetch_postseason_team_stats(conn, specific_seasons=final_specific_seasons)
    else:
        all_teams_historical = fetch_historical_team_stats(conn, past_years=final_past_years, include_current=final_include_current)
        all_teams_playoff = fetch_postseason_team_stats(conn, past_years=final_past_years)
    
    # Don't close connection yet - still need it for NBA players
    log(f"✅ Fetched team stats: {len(all_teams_current)} current, {len(all_teams_historical)} historical, {len(all_teams_playoff)} playoff")
    
    # Calculate percentiles once for all sheets using the same configuration
    log(f"Calculating player percentiles using mode: {final_stats_mode}...")
    percentiles, players_with_stats = calculate_percentiles(all_players, final_stats_mode, final_custom_value)
    historical_percentiles, historical_players_with_stats = calculate_historical_percentiles(historical_players, final_stats_mode, final_custom_value)
    playoff_percentiles, playoff_players_with_stats = calculate_postseason_percentiles(playoff_players, final_stats_mode, final_custom_value)
    
    # Calculate physical attribute percentiles and merge into percentiles dict
    log("Calculating physical attribute percentiles...")
    physical_percentiles = calculate_physical_attribute_percentiles(all_players)
    percentiles.update(physical_percentiles)
    log("✅ Player percentiles calculated")
    
    # Calculate team percentiles (teams vs teams)
    log(f"Calculating team percentiles using mode: {final_stats_mode}...")
    team_percentiles, _ = calculate_team_percentiles(all_teams_current, final_stats_mode, final_custom_value)
    historical_team_percentiles, _ = calculate_historical_team_percentiles(all_teams_historical, final_stats_mode, final_custom_value)
    playoff_team_percentiles, _ = calculate_postseason_team_percentiles(all_teams_playoff, final_stats_mode, final_custom_value)
    log("✅ Team percentiles calculated")
    
    # Calculate team average percentiles (for physical attributes)
    log("Calculating team average percentiles for physical attributes...")
    # Group all_players by team first
    all_players_by_team = {}
    for player in all_players:
        team_abbr = player['team_abbr']
        if team_abbr not in all_players_by_team:
            all_players_by_team[team_abbr] = []
        all_players_by_team[team_abbr].append(player)
    
    team_avg_percentiles = calculate_team_average_percentiles(all_players_by_team)
    log("✅ Team average percentiles calculated")
    
    # Group players by team and add calculated stats
    # First, create lookup dictionaries for O(1) access instead of O(n) loops
    log("Building player stats lookups...")
    current_stats_by_id = {p['player_id']: p for p in players_with_stats}
    historical_stats_by_id = {p['player_id']: p for p in historical_players_with_stats}
    playoff_stats_by_id = {p['player_id']: p for p in playoff_players_with_stats}
    
    teams_data = {}
    for player in all_players:
        team_abbr = player['team_abbr']
        if team_abbr not in teams_data:
            teams_data[team_abbr] = []
        
        # Add current season calculated stats - O(1) lookup instead of O(n) loop
        player_id = player['player_id']
        if player_id in current_stats_by_id:
            p_with_stats = current_stats_by_id[player_id]
            player['calculated_stats'] = p_with_stats.get('calculated_stats', {})
            player['per100'] = p_with_stats.get('per100', {})
        
        # Add historical stats - O(1) lookup instead of O(n) loop
        if player_id in historical_stats_by_id:
            hist_player = historical_stats_by_id[player_id]
            player['historical_calculated_stats'] = hist_player.get('calculated_stats', {})
            player['historical_per100'] = hist_player.get('per100', {})
            player['seasons_played'] = hist_player.get('seasons_played', 0)
        
        # Add playoff stats - O(1) lookup instead of O(n) loop
        if player_id in playoff_stats_by_id:
            playoff_player = playoff_stats_by_id[player_id]
            player['playoff_calculated_stats'] = playoff_player.get('calculated_stats', {})
            player['playoff_per100'] = playoff_player.get('per100', {})
            player['playoff_seasons_played'] = playoff_player.get('seasons_played', 0)
        
        teams_data[team_abbr].append(player)
    
    log("✅ Player data grouped by team")
    
    # Build team stats lookup by team_abbr
    log("Building team stats lookups...")
    teams_stats_by_abbr = {}
    for team in all_teams_current:
        team_abbr = team.get('team_abbr')
        if team_abbr:
            teams_stats_by_abbr[team_abbr] = {
                'team_stats': team,
                'historical_team_stats': None,
                'playoff_team_stats': None,
                'team_avg_percentiles': team_avg_percentiles,  # Add team average percentiles for all teams
                'all_teams_current': all_teams_current,  # For opponent percentile comparisons
                'all_teams_historical': all_teams_historical,  # For opponent percentile comparisons
                'all_teams_playoff': all_teams_playoff  # For opponent percentile comparisons
            }
    
    for team in all_teams_historical:
        team_abbr = team.get('team_abbr')
        if team_abbr and team_abbr in teams_stats_by_abbr:
            teams_stats_by_abbr[team_abbr]['historical_team_stats'] = team
    
    for team in all_teams_playoff:
        team_abbr = team.get('team_abbr')
        if team_abbr and team_abbr in teams_stats_by_abbr:
            teams_stats_by_abbr[team_abbr]['playoff_team_stats'] = team
    
    log("✅ Team stats grouped by team abbreviation")
    
    # Fetch all worksheet metadata once to avoid repeated API calls
    log("Fetching worksheet metadata to avoid API rate limits...")
    all_worksheets = {ws.title: ws for ws in spreadsheet.worksheets()}
    log(f"✅ Found {len(all_worksheets)} existing worksheets")
    
    # Create NBA sheet FIRST with all players including FA
    log("=" * 60)
    log("Creating NBA sheet with all players...")
    log("=" * 60)
    
    # Fetch all NBA players including those without teams
    nba_players = fetch_all_nba_players_data(conn)
    log(f"✅ Fetched {len(nba_players)} total players for NBA sheet")
    
    # Close database connection now that we're done with all queries
    conn.close()
    log("✅ Database connection closed")
    
    # Add calculated stats to NBA players
    for player in nba_players:
        player_id = player['player_id']
        if player_id in current_stats_by_id:
            p_with_stats = current_stats_by_id[player_id]
            player['calculated_stats'] = p_with_stats.get('calculated_stats', {})
            player['per100'] = p_with_stats.get('per100', {})
        
        if player_id in historical_stats_by_id:
            hist_player = historical_stats_by_id[player_id]
            player['historical_calculated_stats'] = hist_player.get('calculated_stats', {})
            player['historical_per100'] = hist_player.get('per100', {})
            player['seasons_played'] = hist_player.get('seasons_played', 0)
        
        if player_id in playoff_stats_by_id:
            playoff_player = playoff_stats_by_id[player_id]
            player['playoff_calculated_stats'] = playoff_player.get('calculated_stats', {})
            player['playoff_per100'] = playoff_player.get('per100', {})
            player['playoff_seasons_played'] = playoff_player.get('seasons_played', 0)
    
    # Check if there's a priority team to process first (from parameter or env var)
    priority_team_param = priority_team or os.environ.get('PRIORITY_TEAM_ABBR')
    
    # Determine processing order: priority team first, then NBA sheet, then rest
    # Exception: If priority team IS NBA, process NBA first
    process_priority_first = False
    if priority_team_param:
        priority_team_upper = priority_team_param.upper()
        if priority_team_upper != 'NBA':
            process_priority_first = True
            log(f"📌 Priority team: {priority_team_upper} will be processed first (before NBA sheet)")
        else:
            log("📌 Priority team is NBA sheet, will be processed first")
    
    # If we have a priority team (and it's not NBA), process it first
    if process_priority_first:
        for team_abbr, team_name in NBA_TEAMS:
            if team_abbr == priority_team_upper:
                team_players = teams_data.get(team_abbr, [])
                if team_players:
                    # Use cached worksheet to avoid API rate limits
                    if team_abbr in all_worksheets:
                        worksheet = all_worksheets[team_abbr]
                    else:
                        worksheet = spreadsheet.add_worksheet(title=team_abbr, rows=100, cols=30)
                        all_worksheets[team_abbr] = worksheet
                    
                    log(f"Updating priority team: {team_name} ({team_abbr})...")
                    
                    create_team_sheet(
                        worksheet, team_abbr, team_name, team_players, 
                        percentiles, historical_percentiles,
                        past_years=final_past_years,
                        stats_mode=final_stats_mode,
                        stats_custom_value=final_custom_value,
                        specific_seasons=final_specific_seasons,
                        include_current=final_include_current,
                        sync_section=sync_section,
                        playoff_percentiles=playoff_percentiles,
                        show_percentiles=final_show_percentiles,
                        team_data=teams_stats_by_abbr.get(team_abbr),
                        team_percentiles=team_percentiles,
                        historical_team_percentiles=historical_team_percentiles,
                        playoff_team_percentiles=playoff_team_percentiles
                    )
                    log(f"✅ {team_name} (priority) complete")
                break
    
    # Get or create NBA worksheet
    # If NBA sheet already exists, just update it in place (don't delete/recreate)
    if 'NBA' in all_worksheets:
        log("NBA sheet exists - will update in place...")
        nba_worksheet = all_worksheets['NBA']
        
        # Move NBA sheet to position 0 if not already there
        try:
            sheet_id = nba_worksheet.id
            log("Ensuring NBA sheet is in first position...")
            spreadsheet.batch_update({
                'requests': [{
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'index': 0
                        },
                        'fields': 'index'
                    }
                }]
            })
            log("✅ NBA sheet position confirmed")
        except Exception as e:
            log(f"⚠️  Could not move NBA sheet: {e}")
    else:
        # Create new NBA worksheet (will appear at end, but we'll move it to position 0)
        log("Creating new NBA worksheet...")
        # Calculate needed rows: 3 header rows + all players + buffer
        needed_rows = 3 + len(nba_players) + 10
        nba_worksheet = spreadsheet.add_worksheet(title='NBA', rows=needed_rows, cols=64)
        all_worksheets['NBA'] = nba_worksheet
        
        # Move NBA sheet to position 0 (first position)
        try:
            log("Moving NBA sheet to first position...")
            # Get the sheet ID
            sheet_id = nba_worksheet.id
            # Create request to move sheet to index 0
            spreadsheet.batch_update({
                'requests': [{
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'index': 0
                        },
                        'fields': 'index'
                    }
                }]
            })
            log("✅ NBA sheet moved to first position")
        except Exception as e:
            log(f"⚠️  Could not move NBA sheet to first position: {e}")
            log("NBA sheet will appear at the end of sheet list")
    
    log("Updating NBA sheet...")
    # No delay needed for first sheet
    
    create_nba_sheet(
        nba_worksheet, nba_players,
        percentiles, historical_percentiles,
        past_years=final_past_years,
        stats_mode=final_stats_mode,
        stats_custom_value=final_custom_value,
        specific_seasons=final_specific_seasons,
        include_current=final_include_current,
        sync_section=sync_section,
        playoff_percentiles=playoff_percentiles,
        show_percentiles=final_show_percentiles
    )
    log("✅ NBA sheet complete")
    
    # Create/update sheets for remaining teams
    log("=" * 60)
    log("Creating remaining team sheets...")
    log("=" * 60)
    
    # Reorder teams to skip priority team (already processed)
    teams_to_process = list(NBA_TEAMS)
    if process_priority_first:
        # Remove priority team from the list since we already processed it
        teams_to_process = [(abbr, name) for abbr, name in teams_to_process if abbr != priority_team_upper]
    
    for idx, (team_abbr, team_name) in enumerate(teams_to_process):
        team_players = teams_data.get(team_abbr, [])
        if not team_players:
            log(f"⚠️  No data found for {team_name}, skipping...")
            continue
        
        # Use cached worksheet to avoid API rate limits
        if team_abbr in all_worksheets:
            worksheet = all_worksheets[team_abbr]
        else:
            worksheet = spreadsheet.add_worksheet(title=team_abbr, rows=100, cols=30)
            all_worksheets[team_abbr] = worksheet
        
        log(f"Updating {team_name} ({team_abbr})...")
        
        # Add small delay before updating each sheet to avoid API rate limits
        if idx > 0:
            time.sleep(0.5)  # 500ms between sheets
        
        create_team_sheet(
            worksheet, team_abbr, team_name, team_players, 
            percentiles, historical_percentiles,
            past_years=final_past_years,
            stats_mode=final_stats_mode,
            stats_custom_value=final_custom_value,
            specific_seasons=final_specific_seasons,
            include_current=final_include_current,
            sync_section=sync_section,
            playoff_percentiles=playoff_percentiles,
            show_percentiles=final_show_percentiles,
            team_data=teams_stats_by_abbr.get(team_abbr),
            team_percentiles=team_percentiles,
            historical_team_percentiles=historical_team_percentiles,
            playoff_team_percentiles=playoff_team_percentiles
        )
        log(f"✅ {team_name} complete")
    
    log("=" * 60)
    log("✅ SUCCESS! All teams and NBA sheet synced to Google Sheets")
    log(f"   View it here: {spreadsheet.url}")
    log("=" * 60)
    
    return True

if __name__ == "__main__":
    # Check for priority team argument
    priority = None
    if len(sys.argv) > 1:
        priority = sys.argv[1]
    success = main(priority_team=priority)
    sys.exit(0 if success else 1)
