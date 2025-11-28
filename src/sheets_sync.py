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
from src.config import (
    DB_CONFIG,
    GOOGLE_SHEETS_CONFIG,
    NBA_CONFIG,
    NBA_TEAMS,
    STAT_COLUMNS,
    HISTORICAL_STAT_COLUMNS,
    PLAYOFF_STAT_COLUMNS,
    PLAYER_ID_COLUMN,
    REVERSE_STATS,
    PERCENTILE_CONFIG,
    COLORS,
    COLOR_THRESHOLDS,
    SHEET_FORMAT,
    SHEET_FORMAT_NBA,
    HEADERS,
    HEADERS_NBA,
    SECTIONS,
    SECTIONS_NBA,
)

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

def log(message):
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

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
    """Fetch all players (with or without stats) for percentile calculation"""
    query = """
    SELECT 
        p.player_id,
        p.name AS player_name,
        p.team_id,
        t.team_abbr,
        p.jersey_number,
        p.years_experience,
        EXTRACT(YEAR FROM AGE(p.birthdate)) + 
            (EXTRACT(MONTH FROM AGE(p.birthdate)) / 12.0) + 
            (EXTRACT(DAY FROM AGE(p.birthdate)) / 365.25) AS age,
        p.height_inches,
        p.weight_lbs,
        p.wingspan_inches,
        p.notes,
        s.games_played,
        s.minutes_x10::float / 10 AS minutes_total,
        s.possessions,
        s.fg2m, s.fg2a,
        s.fg3m, s.fg3a,
        s.ftm, s.fta,
        s.off_rebounds,
        s.def_rebounds,
        s.off_reb_pct_x1000::float / 1000 AS oreb_pct,
        s.def_reb_pct_x1000::float / 1000 AS dreb_pct,
        s.assists,
        s.turnovers,
        s.steals,
        s.blocks,
        s.fouls,
        s.off_rating_x10,
        s.def_rating_x10
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
        cur.execute(query, (NBA_CONFIG['current_season_year'], NBA_CONFIG['season_type']))
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def fetch_all_nba_players_data(conn):
    """Fetch ALL players in the database including those without teams (marked as FA)"""
    query = """
    SELECT 
        p.player_id,
        p.name AS player_name,
        p.team_id,
        COALESCE(t.team_abbr, 'FA') AS team_abbr,
        p.jersey_number,
        p.years_experience,
        EXTRACT(YEAR FROM AGE(p.birthdate)) + 
            (EXTRACT(MONTH FROM AGE(p.birthdate)) / 12.0) + 
            (EXTRACT(DAY FROM AGE(p.birthdate)) / 365.25) AS age,
        p.height_inches,
        p.weight_lbs,
        p.wingspan_inches,
        p.notes,
        s.games_played,
        s.minutes_x10::float / 10 AS minutes_total,
        s.possessions,
        s.fg2m, s.fg2a,
        s.fg3m, s.fg3a,
        s.ftm, s.fta,
        s.off_rebounds,
        s.def_rebounds,
        s.off_reb_pct_x1000::float / 1000 AS oreb_pct,
        s.def_reb_pct_x1000::float / 1000 AS dreb_pct,
        s.assists,
        s.turnovers,
        s.steals,
        s.blocks,
        s.fouls,
        s.off_rating_x10,
        s.def_rating_x10
    FROM players p
    LEFT JOIN teams t ON p.team_id = t.team_id
    LEFT JOIN player_season_stats s 
        ON s.player_id = p.player_id 
        AND s.year = %s 
        AND s.season_type = %s
    ORDER BY COALESCE(t.team_abbr, 'FA'), COALESCE(s.minutes_x10, 0) DESC, p.name
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (NBA_CONFIG['current_season_year'], NBA_CONFIG['season_type']))
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
            print(f"[HISTORICAL] Including current year: {start_year} to {end_year} (current={current_year})")
        else:
            start_year = current_year - past_years
            end_year = current_year  # Exclude current
            print(f"[HISTORICAL] Excluding current year: {start_year} to {end_year-1} (current={current_year})")
        
        season_filter = "AND s.year >= %s AND s.year < %s"
        season_params = (start_year, end_year)
    
    query = f"""
    SELECT 
        p.player_id,
        p.name AS player_name,
        p.team_id,
        t.team_abbr,
        COUNT(DISTINCT s.year) AS seasons_played,
        SUM(s.games_played) AS games_played,
        SUM(s.minutes_x10::float) / 10 AS minutes_total,
        SUM(s.possessions) AS possessions,
        SUM(s.fg2m) AS fg2m, 
        SUM(s.fg2a) AS fg2a,
        SUM(s.fg3m) AS fg3m, 
        SUM(s.fg3a) AS fg3a,
        SUM(s.ftm) AS ftm, 
        SUM(s.fta) AS fta,
        SUM(s.off_rebounds) AS off_rebounds,
        SUM(s.def_rebounds) AS def_rebounds,
        -- Weighted average for rebounding percentages
        SUM(s.off_reb_pct_x1000 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) / 1000 AS oreb_pct,
        SUM(s.def_reb_pct_x1000 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) / 1000 AS dreb_pct,
        -- Weighted average for ratings
        SUM(s.off_rating_x10 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) AS off_rating_x10,
        SUM(s.def_rating_x10 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) AS def_rating_x10,
        SUM(s.assists) AS assists,
        SUM(s.turnovers) AS turnovers,
        SUM(s.steals) AS steals,
        SUM(s.blocks) AS blocks,
        SUM(s.fouls) AS fouls
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

def fetch_postseason_players_data(conn, past_years=25, specific_seasons=None):
    """
    Fetch aggregated postseason stats (season_type IN (2, 3) - playoffs + play-in).
    Similar to historical stats but specifically for postseason games.
    
    Args:
        conn: Database connection
        past_years: Number of years to look back (default 25 for career)
        specific_seasons: List of specific season years to include (e.g., [2023, 2024])
    
    Returns player postseason stats aggregated across specified seasons
    """
    current_year = NBA_CONFIG['current_season_year']
    
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
    
    query = f"""
    SELECT 
        p.player_id,
        p.name AS player_name,
        p.team_id,
        t.team_abbr,
        COUNT(DISTINCT s.year) AS seasons_played,
        SUM(s.games_played) AS games_played,
        SUM(s.minutes_x10::float) / 10 AS minutes_total,
        SUM(s.possessions) AS possessions,
        SUM(s.fg2m) AS fg2m, 
        SUM(s.fg2a) AS fg2a,
        SUM(s.fg3m) AS fg3m, 
        SUM(s.fg3a) AS fg3a,
        SUM(s.ftm) AS ftm, 
        SUM(s.fta) AS fta,
        SUM(s.off_rebounds) AS off_rebounds,
        SUM(s.def_rebounds) AS def_rebounds,
        -- Weighted average for rebounding percentages
        SUM(s.off_reb_pct_x1000 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) / 1000 AS oreb_pct,
        SUM(s.def_reb_pct_x1000 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) / 1000 AS dreb_pct,
        -- Weighted average for ratings
        SUM(s.off_rating_x10 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) AS off_rating_x10,
        SUM(s.def_rating_x10 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) AS def_rating_x10,
        SUM(s.assists) AS assists,
        SUM(s.turnovers) AS turnovers,
        SUM(s.steals) AS steals,
        SUM(s.blocks) AS blocks,
        SUM(s.fouls) AS fouls
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

def calculate_per_100_poss_stats(player):
    """Calculate per-100 possession stats"""
    possessions = player.get('possessions', 0)
    if not possessions or possessions == 0:
        return {}
    
    factor = 100.0 / possessions
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1)) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1)) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'points': points * factor,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0) * factor,
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0) * factor,
        'fg3_pct': fg3_pct,
        'fta': fta * factor,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0) * factor,
        'turnovers': (player.get('turnovers', 0) or 0) * factor,
        'oreb_pct': (player.get('oreb_pct', 0) or 0),
        'dreb_pct': (player.get('dreb_pct', 0) or 0),
        'steals': (player.get('steals', 0) or 0) * factor,
        'blocks': (player.get('blocks', 0) or 0) * factor,
        'fouls': (player.get('fouls', 0) or 0) * factor,
    }

def calculate_per_36_stats(player):
    """Calculate per-36 minute stats (default view)"""
    minutes_total = player.get('minutes_total', 0)
    if not minutes_total or minutes_total == 0:
        return {}
    
    factor = 36.0 / minutes_total
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1)) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1)) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'possessions': (player.get('possessions', 0) or 0) * factor,
        'points': points * factor,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0) * factor,
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0) * factor,
        'fg3_pct': fg3_pct,
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
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1)) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1)) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    # For totals, OR% and DR% become ORS and DRS (actual rebound counts)
    # These are stored as percentages (0-1), so multiply by 100 to get raw counts
    ors = (player.get('oreb_pct', 0) or 0) * 100
    drs = (player.get('dreb_pct', 0) or 0) * 100
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0),
        'possessions': player.get('possessions', 0) or 0,
        'points': points,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0),
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0),
        'fg3_pct': fg3_pct,
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
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1)) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1)) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': games,
        'minutes': player.get('minutes_total', 0) / games,
        'possessions': (player.get('possessions', 0) or 0) * factor,
        'points': points * factor,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0) * factor,
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0) * factor,
        'fg3_pct': fg3_pct,
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

def calculate_per_minutes_stats(player, minutes=36.0):
    """Calculate per-X minute stats"""
    minutes_total = player.get('minutes_total', 0)
    if not minutes_total or minutes_total == 0:
        return {}
    
    factor = minutes / minutes_total
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1)) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1)) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        # Keep minutes as per-game average, not the scaled target value
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'possessions': (player.get('possessions', 0) or 0) * factor,
        'points': points * factor,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0) * factor,
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0) * factor,
        'fg3_pct': fg3_pct,
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
    """Calculate stats based on the specified mode"""
    if mode == 'totals':
        return calculate_totals_stats(player)
    elif mode == 'per_game':
        return calculate_per_game_stats(player)
    elif mode == 'per_minutes':
        minutes = float(custom_value) if custom_value else 36.0
        return calculate_per_minutes_stats(player, minutes)
    else:  # per_36 or default
        return calculate_per_36_stats(player)

def calculate_per_36_stats_old(player):
    """Calculate per-36 minute stats (default view)"""
    minutes_total = player.get('minutes_total', 0)
    if not minutes_total or minutes_total == 0:
        return {}
    
    factor = 36.0 / minutes_total
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1)) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1)) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1)) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'points': points * factor,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0) * factor,
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0) * factor,
        'fg3_pct': fg3_pct,
        'fta': fta * factor,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0) * factor,
        'turnovers': (player.get('turnovers', 0) or 0) * factor,
        'oreb_pct': (player.get('oreb_pct', 0) or 0),
        'dreb_pct': (player.get('dreb_pct', 0) or 0),
        'steals': (player.get('steals', 0) or 0) * factor,
        'blocks': (player.get('blocks', 0) or 0) * factor,
        'fouls': (player.get('fouls', 0) or 0) * factor,
    }

def calculate_percentiles(all_players_data, mode='per_36', custom_value=None):
    """Calculate weighted percentiles for each stat across all players (weighted by total minutes)"""
    # Calculate stats for all players in the specified mode
    players_with_stats = []
    for player in all_players_data:
        stats = calculate_stats_by_mode(player, mode, custom_value)
        per100 = calculate_per_100_poss_stats(player)
        if stats:
            player['calculated_stats'] = stats
            player['per100'] = per100
            players_with_stats.append(player)
    
    # Calculate weighted percentiles for each stat
    percentiles = {}
    for stat_name in STAT_COLUMNS.keys():
        # Create weighted samples where each player's stat appears proportional to their minutes
        weighted_values = []
        for p in players_with_stats:
            stat_value = p['calculated_stats'].get(stat_name, 0)
            if stat_value and stat_value != 0:
                # Weight by total minutes played
                minutes_weight = p.get('minutes_total', 0)
                if minutes_weight > 0:
                    # Add the value multiple times based on minutes (rounded to avoid too many samples)
                    # Scale minutes to reasonable sample size (e.g., 1 sample per 10 minutes)
                    weight_count = max(1, int(round(minutes_weight / PERCENTILE_CONFIG['minutes_weight_factor'])))
                    weighted_values.extend([stat_value] * weight_count)
        
        if weighted_values:
            percentiles[stat_name] = np.percentile(weighted_values, range(101))
        else:
            percentiles[stat_name] = None
    
    return percentiles, players_with_stats

def calculate_historical_percentiles(historical_players_data, mode='per_36', custom_value=None):
    """
    Calculate weighted percentiles for historical data (past seasons only)
    Returns percentiles and players with calculated historical stats
    """
    # Calculate stats for all players with historical data in the specified mode
    players_with_stats = []
    for player in historical_players_data:
        stats = calculate_stats_by_mode(player, mode, custom_value)
        per100 = calculate_per_100_poss_stats(player)
        if stats:
            player['calculated_stats'] = stats
            player['per100'] = per100
            players_with_stats.append(player)
    
    # Calculate weighted percentiles for each stat
    # Use HISTORICAL_STAT_COLUMNS keys (excluding 'years') for historical data
    percentiles = {}
    for stat_name in HISTORICAL_STAT_COLUMNS.keys():
        if stat_name == 'years':  # Skip years column, it has custom percentile logic
            continue
            
        # Create weighted samples
        weighted_values = []
        for p in players_with_stats:
            stat_value = p['calculated_stats'].get(stat_name, 0)
            if stat_value and stat_value != 0:
                minutes_weight = p.get('minutes_total', 0)
                if minutes_weight > 0:
                    weight_count = max(1, int(round(minutes_weight / PERCENTILE_CONFIG['minutes_weight_factor'])))
                    weighted_values.extend([stat_value] * weight_count)
        
        if weighted_values:
            percentiles[stat_name] = np.percentile(weighted_values, range(101))
        else:
            percentiles[stat_name] = None
    
    return percentiles, players_with_stats

def calculate_postseason_percentiles(postseason_players_data, mode='per_36', custom_value=None):
    """
    Calculate weighted percentiles for postseason data (season_type IN (2, 3) - playoffs + play-in).
    Returns percentiles and players with calculated postseason stats.
    """
    # Calculate stats for all players with postseason data in the specified mode
    players_with_stats = []
    for player in postseason_players_data:
        stats = calculate_stats_by_mode(player, mode, custom_value)
        per100 = calculate_per_100_poss_stats(player)
        if stats:
            player['calculated_stats'] = stats
            player['per100'] = per100
            players_with_stats.append(player)
    
    # Calculate weighted percentiles for each stat
    # Use HISTORICAL_STAT_COLUMNS keys (excluding 'years') for playoff data (same structure)
    percentiles = {}
    for stat_name in HISTORICAL_STAT_COLUMNS.keys():
        if stat_name == 'years':  # Skip years column, it has custom percentile logic
            continue
            
        # Create weighted samples
        weighted_values = []
        for p in players_with_stats:
            stat_value = p['calculated_stats'].get(stat_name, 0)
            if stat_value and stat_value != 0:
                minutes_weight = p.get('minutes_total', 0)
                if minutes_weight > 0:
                    weight_count = max(1, int(round(minutes_weight / PERCENTILE_CONFIG['minutes_weight_factor'])))
                    weighted_values.extend([stat_value] * weight_count)
        
        if weighted_values:
            percentiles[stat_name] = np.percentile(weighted_values, range(101))
        else:
            percentiles[stat_name] = None
    
    return percentiles, players_with_stats

# Backward compatibility alias
calculate_playoff_percentiles = calculate_postseason_percentiles

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
    Uses colors from config: red, yellow, green.
    """
    if percentile is None:
        return None
    
    red_rgb = COLORS['red']['rgb']
    yellow_rgb = COLORS['yellow']['rgb']
    green_rgb = COLORS['green']['rgb']
    
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
    if not inches:
        return ""
    feet = inches // 12
    remaining_inches = inches % 12
    return f'{feet}\'{remaining_inches}"'

def parse_sheet_config(worksheet):
    """
    Parse existing sheet configuration from header row to preserve user settings.
    Returns: (stats_mode, custom_value, historical_config)
    
    historical_config contains: (past_years, include_current, specific_seasons)
    """
    try:
        # Get first row to check headers
        header_row = worksheet.row_values(1)
        if len(header_row) < 26:  # Not enough columns
            return None, None, None
        
        # Parse stats mode from current stats header (column I, index 8)
        current_stats_header = header_row[8] if len(header_row) > 8 else ""
        
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
        
        # Parse historical config from historical stats header (column Z, index 25)
        historical_header = header_row[25] if len(header_row) > 25 else ""
        
        past_years = 3  # default
        include_current = False
        specific_seasons = None
        
        if 'Career' in historical_header:
            past_years = 25
            include_current = 'prev' not in historical_header
        elif 'prev' in historical_header:
            # Try to extract number of years (e.g., "prev 3 season stats")
            import re
            match = re.search(r'prev (\d+) season', historical_header)
            if match:
                past_years = int(match.group(1))
            include_current = False
        elif 'last' in historical_header:
            # e.g., "last 5 season stats"
            import re
            match = re.search(r'last (\d+) season', historical_header)
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
        
        log(f"Parsed sheet config: mode={stats_mode}, custom={custom_value}, years={past_years}, include_current={include_current}, seasons={specific_seasons}")
        return stats_mode, custom_value, (past_years, include_current, specific_seasons)
        
    except Exception as e:
        log(f"Could not parse sheet config: {e}")
        return None, None, None

def create_team_sheet(worksheet, team_abbr, team_name, team_players, percentiles, historical_percentiles, 
                      past_years=3, stats_mode='per_36', stats_custom_value=None, specific_seasons=None, 
                      include_current=False, sync_section=None, show_percentiles=False, playoff_percentiles=None):
    """Create/update a team sheet with formatting and color coding (including historical/playoff stats)
    
    Args:
        sync_section: 'historical' or 'playoff' - determines which columns to write to
        show_percentiles: If True, display percentile values instead of stat values
        playoff_percentiles: Percentiles for playoff stats (when sync_section='playoff')
    """
    log(f"Creating {team_name} sheet with stats mode: {stats_mode}, section: {sync_section}, show_percentiles: {show_percentiles}...")
    
    # Get current season dynamically
    current_season = get_current_season()
    
    # Header row 1 - replace placeholders
    header_row_1 = []
    mode_display = {
        'totals': 'Totals',
        'per_game': 'Per Game',
        'per_36': 'Per 36 Mins',
        'per_100_poss': 'Per 100 Poss',
        'per_minutes': f'Per {stats_custom_value} Mins' if stats_custom_value else 'Per Minute'
    }
    mode_text = mode_display.get(stats_mode, 'Per 36 Mins')
    
    for i, h in enumerate(HEADERS['row_1']):
        # Handle historical_years placeholder
        if '{historical_years}' in h:
            # Build header text for historical section
            if specific_seasons:
                # Specific season(s) - show start season only
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                if include_current:
                    historical_text = f'Stats since {start_season_text} {mode_text}'
                else:
                    historical_text = f'Prev stats since {start_season_text} {mode_text}'
            elif past_years >= 25:
                # Career mode (default)
                if include_current:
                    historical_text = f'Career Stats {mode_text}'
                else:
                    historical_text = f'Career Prev Season Stats {mode_text}'
            else:
                # Number of years mode
                if include_current:
                    historical_text = f'Last {past_years} Seasons {mode_text}'
                else:
                    historical_text = f'Prev {past_years} Seasons {mode_text}'
            header_row_1.append(h.replace('{historical_years}', historical_text))
            
        # Handle postseason_years placeholder
        elif '{postseason_years}' in h:
            # Build header text for postseason section - always show mode
            if specific_seasons:
                # Specific season(s) - show start season only
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                if include_current:
                    postseason_text = f'Postseason Stats since {start_season_text} {mode_text}'
                else:
                    postseason_text = f'Prev Postseason Stats since {start_season_text} {mode_text}'
            elif past_years >= 25:
                # Career mode (default)
                if include_current:
                    postseason_text = f'Career Postseason Stats {mode_text}'
                else:
                    postseason_text = f'Career Prev Season Postseason Stats {mode_text}'
            else:
                # Number of years mode
                if include_current:
                    postseason_text = f'Last {past_years} Postseason Seasons {mode_text}'
                else:
                    postseason_text = f'Prev {past_years} Postseason Seasons {mode_text}'
            header_row_1.append(h.replace('{postseason_years}', postseason_text))
            
        # Handle team_name placeholder
        elif '{team_name}' in h:
            header_row_1.append(h.replace('{team_name}', team_name.upper()))
            
        # Handle season placeholder
        elif '{season}' in h:
            # Update header to reflect the stats mode
            header_row_1.append(h.replace('{season}', f'{current_season} Stats {mode_text}'))
        else:
            header_row_1.append(h)
    
    # Header row 2 - replace OR%/DR% with ORS/DRS for totals mode
    header_row_2 = list(HEADERS['row_2'])  # Make a copy
    if stats_mode == 'totals':
        # Replace OR% and DR% with ORS and DRS for both current and historical sections
        header_row_2 = [h.replace('OR%', 'ORS').replace('DR%', 'DRS') for h in header_row_2]
    else:
        # For non-totals modes, change the historical GMS column to GMS
        # Current GMS is at index 8, Historical GMS is at index 26
        header_row_2[26] = 'GMS'  # Historical games column
    
    # Filter row
    filter_row = [""] * SHEET_FORMAT['total_columns']
    
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
        fg2a = calculated_stats.get('fg2a', 0)
        fg3a = calculated_stats.get('fg3a', 0)
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
        hist_fg2a = historical_calculated_stats.get('fg2a', 0)
        hist_fg3a = historical_calculated_stats.get('fg3a', 0)
        hist_fta = historical_calculated_stats.get('fta', 0)
        
        # Get playoff stats
        playoff_calculated_stats = player.get('playoff_calculated_stats', {})
        playoff_minutes = playoff_calculated_stats.get('minutes', 0)
        has_playoff_minutes = playoff_minutes and playoff_minutes > 0
        playoff_seasons_played = player.get('playoff_seasons_played', 0) if has_playoff_minutes else ''
        
        # Playoff shooting attempts
        playoff_fg2a = playoff_calculated_stats.get('fg2a', 0)
        playoff_fg3a = playoff_calculated_stats.get('fg3a', 0)
        playoff_fta = playoff_calculated_stats.get('fta', 0)
        
        # PRE-CALCULATE PERCENTILES (needed for show_percentiles mode)
        player_percentiles = {}
        
        # Current season percentiles
        for stat_name, col_idx in STAT_COLUMNS.items():
            value = calculated_stats.get(stat_name, 0)
            if not has_minutes:
                player_percentiles[col_idx] = None
            elif stat_name == 'ts_pct' and value == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg2_pct' and fg2a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg3_pct' and fg3a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and fta == 0:
                player_percentiles[col_idx] = None
            elif value is not None and value != '':
                reverse = stat_name in REVERSE_STATS
                pct = get_percentile_rank(value, percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # Historical percentiles
        for stat_name, col_idx in HISTORICAL_STAT_COLUMNS.items():
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
            elif stat_name == 'fg2_pct' and hist_fg2a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg3_pct' and hist_fg3a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and hist_fta == 0:
                player_percentiles[col_idx] = None
            elif value is not None and value != '':
                reverse = stat_name in REVERSE_STATS
                pct = get_percentile_rank(value, historical_percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # Playoff percentiles
        for stat_name, col_idx in PLAYOFF_STAT_COLUMNS.items():
            if stat_name == 'years':
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
            elif stat_name == 'fg2_pct' and playoff_fg2a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg3_pct' and playoff_fg3a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and playoff_fta == 0:
                player_percentiles[col_idx] = None
            elif value is not None and value != '':
                reverse = stat_name in REVERSE_STATS
                pct = get_percentile_rank(value, playoff_percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # BUILD ROW with percentile-aware display
        row = [
            player['player_name'],
            player.get('jersey_number', ''),
            exp_display,
            round(float(player.get('age', 0)), 1) if player.get('age') else '',
            format_height(player.get('height_inches')),
            format_height(player.get('wingspan_inches')),
            player.get('weight_lbs', ''),
            player.get('notes', ''),
            # Current season stats (columns 8-27, indexes 8-27)
            calculated_stats.get('games', 0) if calculated_stats.get('games', 0) and has_minutes else '',
            get_display_value(minutes, player_percentiles.get(9), 9) if has_minutes else '',
            get_display_value(calculated_stats.get('possessions', 0), player_percentiles.get(10), 10) if has_minutes else '',
            get_display_value(calculated_stats.get('points', 0), player_percentiles.get(11), 11) if has_minutes else '',
            get_display_value(calculated_stats.get('ts_pct'), player_percentiles.get(12), 12, is_pct=True) if has_minutes else '',
            get_display_value(fg2a, player_percentiles.get(13), 13) if has_minutes else '',
            get_display_value(calculated_stats.get('fg2_pct'), player_percentiles.get(14), 14, is_pct=True, allow_zero=(fg2a > 0)) if has_minutes else '',
            get_display_value(fg3a, player_percentiles.get(15), 15) if has_minutes else '',
            get_display_value(calculated_stats.get('fg3_pct'), player_percentiles.get(16), 16, is_pct=True, allow_zero=(fg3a > 0)) if has_minutes else '',
            get_display_value(fta, player_percentiles.get(17), 17) if has_minutes else '',
            get_display_value(calculated_stats.get('ft_pct'), player_percentiles.get(18), 18, is_pct=True, allow_zero=(fta > 0)) if has_minutes else '',
            get_display_value(calculated_stats.get('assists', 0), player_percentiles.get(19), 19) if has_minutes else '',
            get_display_value(calculated_stats.get('turnovers', 0), player_percentiles.get(20), 20) if has_minutes else '',
            get_display_value(calculated_stats.get('oreb_pct'), player_percentiles.get(21), 21, is_pct=True, allow_zero=True) if has_minutes else '',
            get_display_value(calculated_stats.get('dreb_pct'), player_percentiles.get(22), 22, is_pct=True, allow_zero=True) if has_minutes else '',
            get_display_value(calculated_stats.get('steals', 0), player_percentiles.get(23), 23) if has_minutes else '',
            get_display_value(calculated_stats.get('blocks', 0), player_percentiles.get(24), 24) if has_minutes else '',
            get_display_value(calculated_stats.get('fouls', 0), player_percentiles.get(25), 25) if has_minutes else '',
            get_display_value(calculated_stats.get('off_rating', 0), player_percentiles.get(26), 26) if has_minutes else '',
            get_display_value(calculated_stats.get('def_rating', 0), player_percentiles.get(27), 27) if has_minutes else '',
            # Historical stats section (columns 28-48, indexes 28-48)
            seasons_played,  # YRS column - no percentile display
            # For non-totals modes, show games per season; for totals show total games
            get_display_value(historical_calculated_stats.get('games', 0) / seasons_played if seasons_played and seasons_played > 0 and stats_mode != 'totals' else historical_calculated_stats.get('games', 0), player_percentiles.get(29), 29) if has_historical_minutes else '',
            get_display_value(historical_minutes, player_percentiles.get(30), 30) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('possessions', 0), player_percentiles.get(31), 31) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('points', 0), player_percentiles.get(32), 32) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('ts_pct'), player_percentiles.get(33), 33, is_pct=True) if has_historical_minutes else '',
            get_display_value(hist_fg2a, player_percentiles.get(34), 34) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('fg2_pct'), player_percentiles.get(35), 35, is_pct=True, allow_zero=(hist_fg2a > 0)) if has_historical_minutes else '',
            get_display_value(hist_fg3a, player_percentiles.get(36), 36) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('fg3_pct'), player_percentiles.get(37), 37, is_pct=True, allow_zero=(hist_fg3a > 0)) if has_historical_minutes else '',
            get_display_value(hist_fta, player_percentiles.get(38), 38) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('ft_pct'), player_percentiles.get(39), 39, is_pct=True, allow_zero=(hist_fta > 0)) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('assists', 0), player_percentiles.get(40), 40) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('turnovers', 0), player_percentiles.get(41), 41) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('oreb_pct'), player_percentiles.get(42), 42, is_pct=True, allow_zero=True) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('dreb_pct'), player_percentiles.get(43), 43, is_pct=True, allow_zero=True) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('steals', 0), player_percentiles.get(44), 44) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('blocks', 0), player_percentiles.get(45), 45) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('fouls', 0), player_percentiles.get(46), 46) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('off_rating', 0), player_percentiles.get(47), 47) if has_historical_minutes else '',
            get_display_value(historical_calculated_stats.get('def_rating', 0), player_percentiles.get(48), 48) if has_historical_minutes else '',
            # Playoff stats section (columns 49-69, indexes 49-69)
            playoff_seasons_played,  # YRS column - no percentile display
            # For non-totals modes, show games per season; for totals show total games
            get_display_value(playoff_calculated_stats.get('games', 0) / playoff_seasons_played if playoff_seasons_played and playoff_seasons_played > 0 and stats_mode != 'totals' else playoff_calculated_stats.get('games', 0), player_percentiles.get(50), 50) if has_playoff_minutes else '',
            get_display_value(playoff_minutes, player_percentiles.get(51), 51) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('possessions', 0), player_percentiles.get(52), 52) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('points', 0), player_percentiles.get(53), 53) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('ts_pct'), player_percentiles.get(54), 54, is_pct=True) if has_playoff_minutes else '',
            get_display_value(playoff_fg2a, player_percentiles.get(55), 55) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('fg2_pct'), player_percentiles.get(56), 56, is_pct=True, allow_zero=(playoff_fg2a > 0)) if has_playoff_minutes else '',
            get_display_value(playoff_fg3a, player_percentiles.get(57), 57) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('fg3_pct'), player_percentiles.get(58), 58, is_pct=True, allow_zero=(playoff_fg3a > 0)) if has_playoff_minutes else '',
            get_display_value(playoff_fta, player_percentiles.get(59), 59) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('ft_pct'), player_percentiles.get(60), 60, is_pct=True, allow_zero=(playoff_fta > 0)) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('assists', 0), player_percentiles.get(61), 61) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('turnovers', 0), player_percentiles.get(62), 62) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('oreb_pct'), player_percentiles.get(63), 63, is_pct=True, allow_zero=True) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('dreb_pct'), player_percentiles.get(64), 64, is_pct=True, allow_zero=True) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('steals', 0), player_percentiles.get(65), 65) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('blocks', 0), player_percentiles.get(66), 66) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('fouls', 0), player_percentiles.get(67), 67) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('off_rating', 0), player_percentiles.get(68), 68) if has_playoff_minutes else '',
            get_display_value(playoff_calculated_stats.get('def_rating', 0), player_percentiles.get(69), 69) if has_playoff_minutes else '',
            # Player ID at the end (hidden)
            str(player['player_id']),  # Column BS (index 70) - hidden player_id for onEdit lookups
        ]
        
        data_rows.append(row)
        percentile_data.append(player_percentiles)
    
    # Combine all data
    all_data = [header_row_1, header_row_2, filter_row] + data_rows
    
    spreadsheet = worksheet.spreadsheet
    total_rows = 3 + len(data_rows)
    total_cols = SHEET_FORMAT['total_columns']
    
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
    if sync_section in [None, 'all', 'current'] and current_row_count > total_rows:
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
    
    # 4. Update cell values (via updateCells instead of separate update call)
    # For partial syncs, only update the relevant column range
    rows_data = []
    start_col_idx = 0
    
    if sync_section == 'historical':
        start_col_idx = SECTIONS['historical']['columns']['start']
        end_col_idx = SECTIONS['historical']['columns']['end'] + 1  # +1 for exclusive end
    elif sync_section == 'postseason':
        start_col_idx = SECTIONS['postseason']['columns']['start']
        end_col_idx = SECTIONS['postseason']['columns']['end'] + 1  # +1 for exclusive end
    else:
        # Full sync - write all columns
        end_col_idx = total_cols
    
    try:
        for row_idx, row_data in enumerate(all_data):
            row_values = []
            # Only process columns in the target range
            for col_idx in range(start_col_idx, end_col_idx):
                cell_value = row_data[col_idx] if col_idx < len(row_data) else ''
                
                # Convert all values to strings, handling None and empty strings
                if cell_value is None or cell_value == '':
                    str_value = ''
                else:
                    try:
                        str_value = str(cell_value)
                    except Exception as e:
                        log(f"Error converting cell at row {row_idx}, col {col_idx}: value={cell_value}, type={type(cell_value)}, error={e}")
                        raise
                
                row_values.append({
                    'userEnteredValue': {
                        'stringValue': str_value
                    }
                })
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
        # 2.5. Set column A to fixed 187px width
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': 1
                },
                'properties': {
                    'pixelSize': 187
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
        
        # 5. Merge cells
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 1,
                    'endColumnIndex': 7,
                },
                'mergeType': 'MERGE_ALL'
            }
        })
        
        # Current season stats header (I to Y, excluding hidden Z)
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 8,   # I
                    'endColumnIndex': 25,    # Y
                },
                'mergeType': 'MERGE_ALL'
            }
        })
    
    # Historical stats header (Z to AQ - includes YRS column)
    requests.append({
        'mergeCells': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 25,  # Z (YRS column)
                'endColumnIndex': 43,    # AQ + 1 (endColumnIndex is exclusive)
            },
            'mergeType': 'MERGE_ALL'
        }
    })
    
    # Playoff stats header (AR to BI - includes YRS column)
    requests.append({
        'mergeCells': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 43,  # AR (YRS column for playoff)
                'endColumnIndex': 61,    # BI + 1 (endColumnIndex is exclusive)
            },
            'mergeType': 'MERGE_ALL'
        }
    })
    
    # Format row 1
    black = COLORS['black']['rgb']
    white = COLORS['white']['rgb']
    light_gray = COLORS['light_gray']['rgb']
    
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
                        'fontFamily': SHEET_FORMAT['fonts']['header_primary']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['header_primary']['size'],
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
    
    # Format row 2
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
                        'fontFamily': SHEET_FORMAT['fonts']['header_secondary']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['header_secondary']['size'],
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
    
    # Format A1
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
                        'fontFamily': SHEET_FORMAT['fonts']['team_name']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['team_name']['size'],
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
    
    # Format filter row
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
                        'fontFamily': SHEET_FORMAT['fonts']['header_primary']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['header_primary']['size'],
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
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontFamily': SHEET_FORMAT['fonts']['data']['family'],
                            'fontSize': SHEET_FORMAT['fonts']['data']['size']
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
        
        # Left-align column H (Notes) and set to clip overflow text
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 7,
                    'endColumnIndex': 8
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
    
    # Bold column A
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
                            'bold': True
                        }
                    }
                },
                'fields': 'userEnteredFormat.textFormat.bold'
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
                            'endColumnIndex': SHEET_FORMAT['total_columns']
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
    
    # CONFIG-DRIVEN COLUMN WIDTHS AND AUTO-RESIZE (only for full syncs)
    if sync_section in [None, 'all', 'current']:
        # Process all sections for fixed widths and auto-resize
        for section_key in ['player_info', 'current', 'historical', 'postseason']:
            section = SECTIONS.get(section_key)
            if not section:
                continue
            
            # Apply fixed width columns from resize_rules
            resize_rules = section.get('resize_rules', {})
            for field_name, rule in resize_rules.items():
                if rule.get('fixed'):
                    # Map field name to column index based on section
                    if section_key == 'player_info':
                        field_map = {'name': 0, 'jersey_number': 1}
                    elif section_key == 'current':
                        field_map = {'games': 8}
                    elif section_key == 'historical':
                        field_map = {'years': 25}
                    elif section_key == 'postseason':
                        field_map = {'years': 43}
                    else:
                        field_map = {}
                
                col_idx = field_map.get(field_name)
                if col_idx is not None:
                    requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': sheet_id,
                                'dimension': 'COLUMNS',
                                'startIndex': col_idx,
                                'endIndex': col_idx + 1
                            },
                            'properties': {
                                'pixelSize': rule['width']
                            },
                            'fields': 'pixelSize'
                        }
                    })
        
        # Apply auto-resize if configured
        if section.get('auto_resize'):
            start_idx = section.get('auto_resize_start')
            end_idx = section.get('auto_resize_end')
            if start_idx is not None and end_idx is not None:
                requests.append({
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': start_idx,
                            'endIndex': end_idx
                        }
                    }
                })
    
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
            header_color = COLORS[border_cfg.get('header_color', 'white')]['rgb']
            data_color = COLORS[border_cfg.get('data_color', 'black')]['rgb']
            
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
                'endColumnIndex': SHEET_FORMAT['total_columns']
            },
            'top': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    # WHITE borders between all columns in header rows (rows 2-3)
    for col_idx in range(1, SHEET_FORMAT['total_columns'] - 1):
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': header_rows + 1,
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
    
    # WHITE borders between columns in row 1 (only certain columns)
    # Add border on left of column H (Notes) to separate from Player Info
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 7,  # Column H (Notes)
                'endColumnIndex': 8,
            },
            'left': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    # BLACK border on right of column G (last player info column before Notes) from row 4 down
    black = {'red': 0, 'green': 0, 'blue': 0}
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 3,  # Row 4 (data starts)
                'endRowIndex': 3 + len(data_rows),
                'startColumnIndex': 6,  # Column G (weight/last player stat)
                'endColumnIndex': 7,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': black
            }
        }
    })
    
    # Freeze panes
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {
                    'frozenRowCount': SHEET_FORMAT['frozen']['rows'],
                    'frozenColumnCount': SHEET_FORMAT['frozen']['columns']
                }
            },
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
    
    # Hide player_id column (column BJ at index 61, NOT column AR which is playoff YRS)
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': PLAYER_ID_COLUMN,
                'endIndex': PLAYER_ID_COLUMN + 1
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
                    'endColumnIndex': SHEET_FORMAT['total_columns']
                }
            }
        }
    })
    # End of full-sync-only operations
    
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
    """Create/update the NBA sheet with all players including FA with Team column
    
    This is similar to create_team_sheet but with:
    - Team column added as column B
    - All other columns shifted right by 1
    - Uses SECTIONS_NBA, HEADERS_NBA, and SHEET_FORMAT_NBA configurations
    """
    log(f"Creating NBA sheet with {len(nba_players)} total players (stats mode: {stats_mode}, show_percentiles: {show_percentiles})...")
    
    # Get current season dynamically
    current_season = get_current_season()
    
    # Header row 1 - replace placeholders
    header_row_1 = []
    mode_display = {
        'totals': 'Totals',
        'per_game': 'Per Game',
        'per_36': 'Per 36 Mins',
        'per_100_poss': 'Per 100 Poss',
        'per_minutes': f'Per {stats_custom_value} Mins' if stats_custom_value else 'Per Minute'
    }
    mode_text = mode_display.get(stats_mode, 'Per 36 Mins')
    
    for i, h in enumerate(HEADERS_NBA['row_1']):
        # Handle historical_years placeholder
        if '{historical_years}' in h:
            if specific_seasons:
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                if include_current:
                    historical_text = f'Stats since {start_season_text} {mode_text}'
                else:
                    historical_text = f'Prev stats since {start_season_text} {mode_text}'
            elif past_years >= 25:
                if include_current:
                    historical_text = f'Career Stats {mode_text}'
                else:
                    historical_text = f'Career Prev Season Stats {mode_text}'
            else:
                if include_current:
                    historical_text = f'Last {past_years} Seasons {mode_text}'
                else:
                    historical_text = f'Prev {past_years} Seasons {mode_text}'
            header_row_1.append(h.replace('{historical_years}', historical_text))
            
        # Handle postseason_years placeholder
        elif '{postseason_years}' in h:
            if specific_seasons:
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                if include_current:
                    postseason_text = f'Postseason Stats since {start_season_text} {mode_text}'
                else:
                    postseason_text = f'Prev Postseason Stats since {start_season_text} {mode_text}'
            elif past_years >= 25:
                if include_current:
                    postseason_text = f'Career Postseason Stats {mode_text}'
                else:
                    postseason_text = f'Career Prev Season Postseason Stats {mode_text}'
            else:
                if include_current:
                    postseason_text = f'Last {past_years} Postseason Seasons {mode_text}'
                else:
                    postseason_text = f'Prev {past_years} Postseason Seasons {mode_text}'
            header_row_1.append(h.replace('{postseason_years}', postseason_text))
            
        # Handle season placeholder
        elif '{season}' in h:
            header_row_1.append(h.replace('{season}', f'{current_season} Stats {mode_text}'))
        else:
            header_row_1.append(h)
    
    # Header row 2 - replace OR%/DR% with ORS/DRS for totals mode
    header_row_2 = list(HEADERS_NBA['row_2'])
    if stats_mode == 'totals':
        header_row_2 = [h.replace('OR%', 'ORS').replace('DR%', 'DRS') for h in header_row_2]
    else:
        # For non-totals modes, adjust GMS headers for historical (index 27 for NBA sheet)
        header_row_2[27] = 'GMS'  # Historical games column (shifted right by 1)
    
    # Filter row
    filter_row = [""] * SHEET_FORMAT_NBA['total_columns']
    
    # NOTE: The rest of the logic is identical to create_team_sheet,
    # but we need to insert team abbreviation in column B (index 1)
    # For simplicity, I'll reuse create_team_sheet's data row logic inline here
    
    # Prepare data rows with percentile tracking
    data_rows = []
    percentile_data = []
    
    for player in nba_players:
        calculated_stats = player.get('calculated_stats', {})
        
        exp = player.get('years_experience')
        exp_display = 0 if exp == 0 else (exp if exp else '')
        
        # Format functions (same as in create_team_sheet)
        def format_stat(value, decimals=1):
            if value is None or value == 0:
                return 0
            rounded = round(value, decimals)
            if rounded == int(rounded):
                return int(rounded)
            return rounded
        
        def format_pct(value, decimals=1, allow_zero=False):
            if value is None:
                return ''
            if value == 0:
                return 0 if allow_zero else ''
            result = value * 100
            rounded = round(result, decimals)
            if rounded == int(rounded):
                return int(rounded)
            return rounded
        
        def get_display_value(stat_value, percentile_value, col_idx, is_pct=False, allow_zero=False):
            if show_percentiles and percentile_value is not None:
                return int(round(percentile_value))
            elif is_pct:
                return format_pct(stat_value, allow_zero=allow_zero)
            else:
                return format_stat(stat_value)
        
        # Check for shooting attempts
        fg2a = calculated_stats.get('fg2a', 0)
        fg3a = calculated_stats.get('fg3a', 0)
        fta = calculated_stats.get('fta', 0)
        
        # Skip if no minutes
        games_played = calculated_stats.get('games', 0)
        minutes_played = calculated_stats.get('minutes', 0)
        has_stats = games_played and minutes_played
        
        # Get percentiles for this player
        player_id = player['player_id']
        player_percentiles = percentiles.get(player_id, {})
        
        # NBA Sheet Row Structure (shifted right by 1, with Team in column B):
        # A: Name, B: Team, C: J#, D: Exp, E: Age, F: Ht, G: W/S, H: Wt, I: Notes,
        # J-Z: Current stats (17 cols), AA-AR: Historical stats (18 cols), AS-BJ: Postseason stats (18 cols), BK: Player ID
        
        row = [
            player.get('player_name', ''),  # A: Name
            player.get('team_abbr', 'FA'),  # B: Team (NEW COLUMN!)
            player.get('jersey_number', ''),  # C: Jersey
            exp_display,  # D: Experience
            round(float(player.get('age', 0)), 1) if player.get('age') else '',  # E: Age (convert Decimal to float)
            format_height(player.get('height_inches')),  # F: Height
            format_height(player.get('wingspan_inches')),  # G: Wingspan (convert Decimal)
            int(player.get('weight_lbs', 0)) if player.get('weight_lbs') else '',  # H: Weight (convert Decimal to int)
            player.get('notes', ''),  # I: Notes
        ]
        
        # Current season stats (columns J-AC, 20 columns) - NBA sheet includes Team so +1 shift
        if has_stats and not sync_section:  # Only write if full sync or current section
            current_stats = [
                get_display_value(calculated_stats.get('games', 0), None, 9),
                get_display_value(calculated_stats.get('minutes', 0), None, 10),
                get_display_value(calculated_stats.get('possessions', 0), None, 11),  # POS after MIN
                get_display_value(calculated_stats.get('points', 0), player_percentiles.get('points'), 12),
                get_display_value(calculated_stats.get('ts_pct', 0), player_percentiles.get('ts_pct'), 13, is_pct=True),
                get_display_value(fg2a, player_percentiles.get('fg2a'), 14),
                get_display_value(calculated_stats.get('fg2_pct', 0) if fg2a else 0, player_percentiles.get('fg2_pct'), 15, is_pct=True),
                get_display_value(fg3a, player_percentiles.get('fg3a'), 16),
                get_display_value(calculated_stats.get('fg3_pct', 0) if fg3a else 0, player_percentiles.get('fg3_pct'), 17, is_pct=True),
                get_display_value(fta, player_percentiles.get('fta'), 18),
                get_display_value(calculated_stats.get('ft_pct', 0) if fta else 0, player_percentiles.get('ft_pct'), 19, is_pct=True),
                get_display_value(calculated_stats.get('assists', 0), player_percentiles.get('assists'), 20),
                get_display_value(calculated_stats.get('turnovers', 0), player_percentiles.get('turnovers'), 21),
                get_display_value(calculated_stats.get('oreb_pct', 0), player_percentiles.get('oreb_pct'), 22, is_pct=(stats_mode != 'totals'), allow_zero=True),
                get_display_value(calculated_stats.get('dreb_pct', 0), player_percentiles.get('dreb_pct'), 23, is_pct=(stats_mode != 'totals'), allow_zero=True),
                get_display_value(calculated_stats.get('steals', 0), player_percentiles.get('steals'), 24),
                get_display_value(calculated_stats.get('blocks', 0), player_percentiles.get('blocks'), 25),
                get_display_value(calculated_stats.get('fouls', 0), player_percentiles.get('fouls'), 26),
                get_display_value(calculated_stats.get('off_rating', 0), player_percentiles.get('off_rating'), 27),  # OR after FLS
                get_display_value(calculated_stats.get('def_rating', 0), player_percentiles.get('def_rating'), 28),  # DR after OR
            ]
            row.extend(current_stats)
        else:
            row.extend([''] * 20)  # Empty current stats
        
        # Historical stats placeholder (we'll fill these if historical data exists)
        # Columns AD-AX (29-49 in 0-indexed, 21 columns) - NBA sheet
        row.extend([''] * 21)
        
        # Postseason stats placeholder
        # Columns AY-BS (50-70 in 0-indexed, 21 columns) - NBA sheet
        row.extend([''] * 21)
        
        # Hidden player ID column (BT, index 71) - NBA sheet
        row.append(player_id)
        
        data_rows.append(row)
        
        # Track percentiles for color coding (only if showing values, not percentiles)
        if not show_percentiles:
            percentile_data.append(player_percentiles)
    
    # Write data to sheet with full formatting (similar to create_team_sheet)
    try:
        all_data = [header_row_1, header_row_2, filter_row] + data_rows
        total_rows = len(all_data)
        total_cols = SHEET_FORMAT_NBA['total_columns']
        
        # Get spreadsheet object for batch updates
        spreadsheet = worksheet.spreadsheet
        
        # Build ONE mega batch request with ALL operations
        requests = []
        
        # 1. Get current sheet metadata
        try:
            sheet_metadata = spreadsheet.fetch_sheet_metadata({'includeGridData': False})
            current_row_count = 1000
            current_col_count = 27  # NBA sheet has +1 columns vs team sheets
            sheet_id = worksheet.id
            
            for sheet in sheet_metadata.get('sheets', []):
                if sheet['properties']['sheetId'] == sheet_id:
                    current_row_count = sheet['properties']['gridProperties'].get('rowCount', 1000)
                    current_col_count = sheet['properties']['gridProperties'].get('columnCount', 27)
                    
                    # Delete any existing banding
                    banded_ranges = sheet.get('bandedRanges', [])
                    if banded_ranges:
                        for br in banded_ranges:
                            requests.append({'deleteBanding': {'bandedRangeId': br['bandedRangeId']}})
                    break
        except Exception as e:
            log(f"⚠️  Warning: Could not fetch sheet metadata for NBA: {e}")
            sheet_id = worksheet.id
            current_row_count = 1000
            current_col_count = 27
        
        # 2. Adjust columns if needed
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
                requests.append({
                    'appendDimension': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'length': total_cols - current_col_count
                    }
                })
        
        # 3. Adjust rows if needed
        if sync_section in [None, 'all', 'current'] and current_row_count > total_rows:
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
        
        # 4. Update cell values
        rows_data = []
        start_col_idx = 0
        
        if sync_section == 'historical':
            start_col_idx = SECTIONS_NBA['historical']['columns']['start']
            end_col_idx = SECTIONS_NBA['historical']['columns']['end'] + 1
        elif sync_section == 'postseason':
            start_col_idx = SECTIONS_NBA['postseason']['columns']['start']
            end_col_idx = SECTIONS_NBA['postseason']['columns']['end'] + 1
        else:
            # Full sync - write all columns
            end_col_idx = total_cols
        
        for row_idx, row_data in enumerate(all_data):
            row_values = []
            for col_idx in range(start_col_idx, end_col_idx):
                cell_value = row_data[col_idx] if col_idx < len(row_data) else ''
                
                if cell_value is None or cell_value == '':
                    str_value = ''
                else:
                    str_value = str(cell_value)
                
                row_values.append({
                    'userEnteredValue': {
                        'stringValue': str_value
                    }
                })
            rows_data.append({'values': row_values})
        
        requests.append({
            'updateCells': {
                'rows': rows_data,
                'fields': 'userEnteredValue',
                'start': {'sheetId': sheet_id, 'rowIndex': 0, 'columnIndex': start_col_idx}
            }
        })
        
        # Only apply formatting for full syncs
        if sync_section in [None, 'all', 'current']:
            # Set column A (Name) to fixed 187px width
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 1
                    },
                    'properties': {
                        'pixelSize': 187
                    },
                    'fields': 'pixelSize'
                }
            })
            
            # Set column A text wrapping to CLIP
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
            
            # 5. Merge cells
            # Player Info header (B-G, columns 1-7) - includes Team column
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 1,
                        'endColumnIndex': 8,  # +1 for Team column
                    },
                    'mergeType': 'MERGE_ALL'
                }
            })
            
            # Current season stats header (I to Z) - shifted by 1
            requests.append({
                'mergeCells': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 9,   # J (+1 from team sheets)
                        'endColumnIndex': 26,    # Z (+1 from team sheets)
                    },
                    'mergeType': 'MERGE_ALL'
                }
            })
        
        # Historical stats header (AA to AR - includes YRS column) - shifted by 1
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 26,  # AA (+1 from Z on team sheets)
                    'endColumnIndex': 44,    # AR + 1 (+1 from team sheets)
                },
                'mergeType': 'MERGE_ALL'
            }
        })
        
        # Playoff stats header (AS to BJ - includes YRS column) - shifted by 1
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 44,  # AS (+1 from team sheets)
                    'endColumnIndex': 62,    # BJ + 1 (+1 from team sheets)
                },
                'mergeType': 'MERGE_ALL'
            }
        })
        
        # Format row 1
        black = COLORS['black']['rgb']
        white = COLORS['white']['rgb']
        light_gray = COLORS['light_gray']['rgb']
        
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
                            'fontFamily': SHEET_FORMAT_NBA['fonts']['header_primary']['family'],
                            'fontSize': SHEET_FORMAT_NBA['fonts']['header_primary']['size'],
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
        
        # Format row 2
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
                            'fontFamily': SHEET_FORMAT_NBA['fonts']['header_secondary']['family'],
                            'fontSize': SHEET_FORMAT_NBA['fonts']['header_secondary']['size'],
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
        
        # Format A1 (NBA text)
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
                            'fontFamily': SHEET_FORMAT_NBA['fonts']['team_name']['family'],
                            'fontSize': SHEET_FORMAT_NBA['fonts']['team_name']['size'],
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
        
        # Format filter row
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
                            'fontFamily': SHEET_FORMAT_NBA['fonts']['header_primary']['family'],
                            'fontSize': SHEET_FORMAT_NBA['fonts']['header_primary']['size'],
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
        
        # Format data rows
        if len(data_rows) > 0:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 3,
                        'endRowIndex': 3 + len(data_rows),
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'fontFamily': SHEET_FORMAT_NBA['fonts']['data']['family'],
                                'fontSize': SHEET_FORMAT_NBA['fonts']['data']['size']
                            },
                            'wrapStrategy': 'CLIP',
                            'verticalAlignment': 'TOP',
                            'horizontalAlignment': 'CENTER'
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment,userEnteredFormat.horizontalAlignment'
                }
            })
            
            # Left-align column A (Name)
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
            
            # Left-align column I (Notes) - shifted by 1 from team sheets
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 3,
                        'endRowIndex': 3 + len(data_rows),
                        'startColumnIndex': 8,  # Column I (+1 from team sheets)
                        'endColumnIndex': 9
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
        
        # Bold column A
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
                                'bold': True
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.textFormat.bold'
                }
            })
            
            # Apply banding
            requests.append({
                'addBanding': {
                    'bandedRange': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 3,
                            'endRowIndex': 3 + len(data_rows),
                            'startColumnIndex': 0,
                            'endColumnIndex': SHEET_FORMAT_NBA['total_columns']
                        },
                        'rowProperties': {
                            'firstBandColor': white,
                            'secondBandColor': light_gray
                        }
                    }
                }
            })
            
        # Apply percentile colors (if not showing percentiles)
        if len(data_rows) > 0 and not show_percentiles:
            for row_idx, player_percentiles in enumerate(percentile_data):
                for col_idx, percentile in player_percentiles.items():
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
        
        # CONFIG-DRIVEN COLUMN WIDTHS AND AUTO-RESIZE (only for full syncs)
        if sync_section in [None, 'all', 'current']:
            # Set column B (Team) to 40px fixed width
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 1,
                        'endIndex': 2
                    },
                    'properties': {
                        'pixelSize': 40
                    },
                    'fields': 'pixelSize'
                }
            })
            
            # Set column C (Jersey) to 22px
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 2,
                        'endIndex': 3
                    },
                    'properties': {
                        'pixelSize': 22
                    },
                    'fields': 'pixelSize'
                }
            })
            
            # Set column J (GMS) to 25px - shifted by 1
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 9,  # +1 from team sheets
                        'endIndex': 10
                    },
                    'properties': {
                        'pixelSize': 25
                    },
                    'fields': 'pixelSize'
                }
            })
            
            # Set column AA (Historical YRS) to 25px - shifted by 1
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 26,  # +1 from team sheets
                        'endIndex': 27
                    },
                    'properties': {
                        'pixelSize': 25
                    },
                    'fields': 'pixelSize'
                }
            })
            
            # Set column AS (Playoff YRS) to 25px - shifted by 1
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 44,  # +1 from team sheets
                        'endIndex': 45
                    },
                    'properties': {
                        'pixelSize': 25
                    },
                    'fields': 'pixelSize'
                }
            })
            
            # Auto-resize columns D-I (player info)
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 3,  # Column D (Exp)
                        'endIndex': 9     # Column I (Notes)
                    }
                }
            })
            
            # Auto-resize stat columns (J-AC, AA-AR, AS-BK) - shifted by 1
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 10,  # After GMS
                        'endIndex': 26     # Before historical YRS
                    }
                }
            })
            
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 27,  # After historical YRS
                        'endIndex': 44     # Before playoff YRS
                    }
                }
            })
            
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 45,  # After playoff YRS
                        'endIndex': 62     # Before hidden player ID
                    }
                }
            })
        
        # Row heights
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'startIndex': 2,
                    'endIndex': 3
                },
                'properties': {
                    'pixelSize': 15
                },
                'fields': 'pixelSize'
            }
        })
        
        if len(data_rows) > 0:
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'ROWS',
                        'startIndex': 3,
                        'endIndex': 3 + len(data_rows)
                    },
                    'properties': {
                        'pixelSize': 21
                    },
                    'fields': 'pixelSize'
                }
            })
        
        # CONFIG-DRIVEN BORDERS for stat sections (only for full syncs)
        if sync_section in [None, 'all', 'current']:
            header_rows = SHEET_FORMAT_NBA.get('header_rows', 2)
            
            # Add borders based on SECTIONS_NBA config
            for section_key in ['current', 'historical', 'postseason']:
                section = SECTIONS_NBA.get(section_key)
                if not section or not section.get('has_border'):
                    continue
                    
                border_cfg = section.get('border_config', {})
                start_col = section['columns']['start']
                end_col = section['columns']['end']
                weight = border_cfg.get('weight', 2)
                header_color = COLORS[border_cfg.get('header_color', 'white')]['rgb']
                data_color = COLORS[border_cfg.get('data_color', 'black')]['rgb']
                
                # Left border on FIRST column (if configured)
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
                
                # Right border on LAST column (if configured)
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
                    'endColumnIndex': SHEET_FORMAT_NBA['total_columns']
                },
                'top': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': white
                }
            }
        })
        
        # WHITE borders between all columns in header rows (rows 2-3)
        for col_idx in range(1, SHEET_FORMAT_NBA['total_columns'] - 1):
            requests.append({
                'updateBorders': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 1,
                        'endRowIndex': 3,
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
        
        # WHITE border on left of column I (Notes) in row 1 - shifted by 1
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 8,  # Column I (+1 from team sheets)
                    'endColumnIndex': 9,
                },
                'left': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': white
                }
            }
        })
        
        # BLACK border on right of column H (last player info before Notes) - shifted by 1
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 7,  # Column H (+1 from team sheets)
                    'endColumnIndex': 8,
                },
                'right': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': black
                }
            }
        })
        
        # Freeze panes
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': SHEET_FORMAT_NBA['frozen']['rows'],
                        'frozenColumnCount': SHEET_FORMAT_NBA['frozen']['columns']
                    }
                },
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
        
        # Hide player_id column (column BK at index 62 for NBA sheet, +1 from team sheets)
        nba_player_id_column = 62  # BK column for NBA sheet (+1 from team sheets' BJ)
        requests.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': nba_player_id_column,
                    'endIndex': nba_player_id_column + 1
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
                        'endColumnIndex': SHEET_FORMAT_NBA['total_columns']
                    }
                }
            }
        })
        
        # Execute all requests in ONE batch call with retry logic
        log(f"Executing batch update with {len(requests)} requests for NBA sheet")
        try:
            spreadsheet.batch_update({'requests': requests})
        except Exception as e:
            log(f"⚠️  Error in batch update for NBA: {type(e).__name__}: {str(e)}")
            log(f"Error details - Total requests: {len(requests)}")
            import traceback
            log(f"Full traceback:\n{traceback.format_exc()}")
            
            log("Retrying after delay...")
            time.sleep(3)
            try:
                spreadsheet.batch_update({'requests': requests})
                log("✅ Retry successful for NBA")
            except Exception as e2:
                log(f"❌ Failed batch update for NBA after retry: {e2}")
                raise
        
        log(f"✅ NBA sheet created with {len(data_rows)} players")
    except Exception as e:
        log(f"❌ Failed to create NBA sheet: {e}")
        raise

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
    
    # Read stats mode from environment
    stats_mode = os.environ.get('STATS_MODE', 'per_36')  # Default to per_36
    stats_custom_value = os.environ.get('STATS_CUSTOM_VALUE')
    sync_section = os.environ.get('SYNC_SECTION')  # None = full sync, 'historical' or 'postseason' for partial
    show_percentiles = os.environ.get('SHOW_PERCENTILES', 'false').lower() == 'true'
    log(f"Using stats mode: {stats_mode}" + (f" ({stats_custom_value} minutes)" if stats_custom_value else ""))
    log(f"Sync section: {sync_section}")
    log(f"Show percentiles: {show_percentiles}")
    
    # Parse historical stats configuration from environment variables
    past_years = 25  # Default to career (25 years)
    include_current = True  # Default to include current season
    specific_seasons = None
    
    historical_mode = os.environ.get('HISTORICAL_MODE', 'career')  # Default to career mode
    include_current_env = os.environ.get('INCLUDE_CURRENT_YEAR', 'true')  # Default to true
    include_current = (include_current_env.lower() == 'true')
    
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
                    # Convert to 4-digit year (e.g., "2010-11" -> 2011, "10-11" -> 2011)
                    if len(start_year_str) == 4:
                        start_year = int(start_year_str) + 1
                    elif len(start_year_str) == 2:
                        # Handle 2-digit years (e.g., "10" in "10-11")
                        yr = int(start_year_str)
                        start_year = (2000 + yr if yr >= 0 else 1900 + yr) + 1
                    else:
                        start_year = int(start_year_str) + 1
                    
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
                        # Convert to 4-digit year
                        if len(start_year_str) == 4:
                            year = int(start_year_str) + 1
                        else:
                            year = int(start_year_str) + 1
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
    
    # Fetch playoff data (always use career mode for playoffs)
    log("Fetching playoff data (career stats)...")
    if specific_seasons:
        playoff_players = fetch_playoff_players_data(conn, specific_seasons=specific_seasons)
    else:
        playoff_players = fetch_playoff_players_data(conn, past_years=25)  # Career = 25 years
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
    existing_mode, existing_custom, existing_historical = parse_sheet_config(first_sheet)
    
    # ALWAYS use the provided configuration (from environment/API) - don't preserve old config
    # The API/environment variables represent the USER'S CURRENT REQUEST
    final_stats_mode = stats_mode
    final_custom_value = stats_custom_value
    final_past_years = past_years
    final_include_current = include_current
    final_specific_seasons = specific_seasons
    
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
        
        # Don't close connection yet - we still need it for NBA players
        log(f"✅ Re-fetched historical data for {len(historical_players)} players")
    else:
        log("ℹ️  Using already-fetched historical data (config unchanged)")
    
    # Calculate percentiles once for all sheets using the same configuration
    log(f"Calculating percentiles using mode: {final_stats_mode}...")
    percentiles, players_with_stats = calculate_percentiles(all_players, final_stats_mode, final_custom_value)
    historical_percentiles, historical_players_with_stats = calculate_historical_percentiles(historical_players, final_stats_mode, final_custom_value)
    playoff_percentiles, playoff_players_with_stats = calculate_playoff_percentiles(playoff_players, final_stats_mode, final_custom_value)
    log("✅ Percentiles calculated")
    
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
    
    # Get or create NBA worksheet
    # If NBA sheet already exists, DELETE it and recreate to ensure it appears first
    if 'NBA' in all_worksheets:
        log("NBA sheet exists - deleting to recreate in first position...")
        try:
            old_nba_sheet = all_worksheets['NBA']
            spreadsheet.del_worksheet(old_nba_sheet)
            log("✅ Old NBA sheet deleted")
            # Remove from cache
            del all_worksheets['NBA']
        except Exception as e:
            log(f"⚠️  Could not delete existing NBA sheet: {e}")
    
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
        show_percentiles=show_percentiles
    )
    log("✅ NBA sheet complete")
    
    # Create/update sheets for each team
    log("=" * 60)
    log("Creating team sheets...")
    log("=" * 60)
    
    # Check if there's a priority team to process first (from parameter or env var)
    priority_team_param = priority_team or os.environ.get('PRIORITY_TEAM_ABBR')
    
    # Reorder teams to process priority team first
    teams_to_process = list(NBA_TEAMS)
    if priority_team_param:
        priority_team_upper = priority_team_param.upper()
        # Find the priority team in the list
        for i, (team_abbr, team_name) in enumerate(teams_to_process):
            if team_abbr == priority_team_upper:
                # Move this team to the front
                priority_entry = teams_to_process.pop(i)
                teams_to_process.insert(0, priority_entry)
                log(f"📌 Priority team: {priority_team_upper} will be processed first")
                break
    
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
            show_percentiles=show_percentiles
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
