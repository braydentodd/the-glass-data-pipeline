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
    PLAYER_ID_COLUMN,
    REVERSE_STATS,
    TOTALS_MODE_REPLACEMENTS,
    PERCENTILE_CONFIG,
    COLORS,
    COLOR_THRESHOLDS,
    SHEET_FORMAT,
    HEADERS,
    HISTORICAL_STATS_CONFIG,
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
        s.off_reb_pct_x1000::float / 1000 AS oreb_pct,
        s.def_reb_pct_x1000::float / 1000 AS dreb_pct,
        s.assists,
        s.turnovers,
        s.steals,
        s.blocks,
        s.fouls
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
    else:
        # Use year range
        if include_current:
            start_year = current_year - past_years + 1
            end_year = current_year + 1  # Include current
        else:
            start_year = current_year - past_years
            end_year = current_year  # Exclude current
        
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
        -- Weighted average for rebounding percentages
        SUM(s.off_reb_pct_x1000 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) / 1000 AS oreb_pct,
        SUM(s.def_reb_pct_x1000 * s.possessions)::float / NULLIF(SUM(s.possessions), 0) / 1000 AS dreb_pct,
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
    
    # For totals, OR% and DR% become ORS and DRS (rating shares)
    minutes_total = player.get('minutes_total', 0) or 1
    ors = (player.get('oreb_pct', 0) or 0) * minutes_total
    drs = (player.get('dreb_pct', 0) or 0) * minutes_total
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0),
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
    percentiles = {}
    for stat_name in STAT_COLUMNS.keys():
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

def create_team_sheet(worksheet, team_abbr, team_name, team_players, percentiles, historical_percentiles, past_years=3, stats_mode='per_36', stats_custom_value=None):
    """Create/update a team sheet with formatting and color coding (including historical stats)"""
    log(f"Creating {team_name} sheet with stats mode: {stats_mode}...")
    
    # Get current season dynamically
    current_season = get_current_season()
    
    # Header row 1 - replace placeholders
    header_row_1 = []
    mode_display = {
        'totals': 'Totals',
        'per_game': 'Per Game',
        'per_36': 'Per 36 Mins',
        'per_minutes': f'Per {stats_custom_value} Mins' if stats_custom_value else 'Per Minute'
    }
    mode_text = mode_display.get(stats_mode, 'Per 36 Mins')
    
    for h in HEADERS['row_1']:
        if '{team_name}' in h:
            header_row_1.append(h.replace('{team_name}', team_name.upper()))
        elif '{season}' in h:
            # Update header to reflect the stats mode
            header_row_1.append(h.replace('{season}', f'{current_season} Stats {mode_text}'))
        elif '{past_years}' in h:
            # Show "Career stats" for career mode (25 years), otherwise show "prev X years stats"
            if past_years >= 25:
                historical_text = f'Career stats {mode_text.lower()}'
            else:
                historical_text = f'prev {past_years} years stats {mode_text.lower()}'
            header_row_1.append(h.replace('{past_years}', historical_text))
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
        
        row = [
            player['player_name'],
            player.get('jersey_number', ''),
            exp_display,
            round(float(player.get('age', 0)), 1) if player.get('age') else '',
            format_height(player.get('height_inches')),
            format_height(player.get('wingspan_inches')),
            player.get('weight_lbs', ''),
            player.get('notes', ''),
            # Current season stats
            calculated_stats.get('games', 0) if calculated_stats.get('games', 0) and has_minutes else '',
            format_stat(minutes) if has_minutes else '',
            format_stat(calculated_stats.get('points', 0)) if has_minutes else '',
            format_pct(calculated_stats.get('ts_pct')) if has_minutes else '',  # TS% - empty when 0 (no formula for attempts)
            format_stat(fg2a) if has_minutes else '',
            format_pct(calculated_stats.get('fg2_pct'), allow_zero=(fg2a > 0)) if has_minutes else '',  # 2P% - show 0 only if 2PA > 0
            format_stat(fg3a) if has_minutes else '',
            format_pct(calculated_stats.get('fg3_pct'), allow_zero=(fg3a > 0)) if has_minutes else '',  # 3P% - show 0 only if 3PA > 0
            format_stat(fta) if has_minutes else '',
            format_pct(calculated_stats.get('ft_pct'), allow_zero=(fta > 0)) if has_minutes else '',  # FT% - show 0 only if FTA > 0
            format_stat(calculated_stats.get('assists', 0)) if has_minutes else '',
            format_stat(calculated_stats.get('turnovers', 0)) if has_minutes else '',
            format_pct(calculated_stats.get('oreb_pct'), allow_zero=True) if has_minutes else '',  # Rebounding % - show 0
            format_pct(calculated_stats.get('dreb_pct'), allow_zero=True) if has_minutes else '',  # Rebounding % - show 0
            format_stat(calculated_stats.get('steals', 0)) if has_minutes else '',
            format_stat(calculated_stats.get('blocks', 0)) if has_minutes else '',
            format_stat(calculated_stats.get('fouls', 0)) if has_minutes else '',
            # Historical stats section
            seasons_played,  # YRS column
            # For non-totals modes, show games per season; for totals show total games
            format_stat(historical_calculated_stats.get('games', 0) / seasons_played if seasons_played and seasons_played > 0 and stats_mode != 'totals' else historical_calculated_stats.get('games', 0)) if has_historical_minutes else '',
            format_stat(historical_minutes) if has_historical_minutes else '',
            format_stat(historical_calculated_stats.get('points', 0)) if has_historical_minutes else '',
            format_pct(historical_calculated_stats.get('ts_pct')) if has_historical_minutes else '',
            format_stat(hist_fg2a) if has_historical_minutes else '',
            format_pct(historical_calculated_stats.get('fg2_pct'), allow_zero=(hist_fg2a > 0)) if has_historical_minutes else '',
            format_stat(hist_fg3a) if has_historical_minutes else '',
            format_pct(historical_calculated_stats.get('fg3_pct'), allow_zero=(hist_fg3a > 0)) if has_historical_minutes else '',
            format_stat(hist_fta) if has_historical_minutes else '',
            format_pct(historical_calculated_stats.get('ft_pct'), allow_zero=(hist_fta > 0)) if has_historical_minutes else '',
            format_stat(historical_calculated_stats.get('assists', 0)) if has_historical_minutes else '',
            format_stat(historical_calculated_stats.get('turnovers', 0)) if has_historical_minutes else '',
            format_pct(historical_calculated_stats.get('oreb_pct'), allow_zero=True) if has_historical_minutes else '',
            format_pct(historical_calculated_stats.get('dreb_pct'), allow_zero=True) if has_historical_minutes else '',
            format_stat(historical_calculated_stats.get('steals', 0)) if has_historical_minutes else '',
            format_stat(historical_calculated_stats.get('blocks', 0)) if has_historical_minutes else '',
            format_stat(historical_calculated_stats.get('fouls', 0)) if has_historical_minutes else '',
            # Player ID at the end (hidden)
            player['player_id'],  # Column AR - hidden player_id for onEdit lookups
        ]
        
        # Calculate percentiles for this player (current season)
        player_percentiles = {}
        for stat_name, col_idx in STAT_COLUMNS.items():
            value = calculated_stats.get(stat_name, 0)
            # Skip percentile calculation if player has no minutes
            if not has_minutes:
                player_percentiles[col_idx] = None
            # For shooting percentages, skip percentile calculation if no attempts (empty cell)
            elif stat_name == 'ts_pct' and value == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg2_pct' and fg2a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg3_pct' and fg3a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and fta == 0:
                player_percentiles[col_idx] = None
            # Include zero values in percentile calculation for other stats
            elif value is not None and value != '':
                reverse = stat_name in REVERSE_STATS
                pct = get_percentile_rank(value, percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        # Calculate percentiles for historical stats
        for stat_name, col_idx in HISTORICAL_STAT_COLUMNS.items():
            if stat_name == 'years':  # YRS column gets percentile coloring too
                # Color code based on number of seasons played
                if seasons_played and seasons_played > 0:
                    # Create a simple percentile based on years (more years = better/greener)
                    # Use wider spread for more distinctive colors: 1 year = 20%, 2 years = 60%, 3+ years = 100%
                    if seasons_played >= 3:
                        player_percentiles[col_idx] = 100
                    elif seasons_played == 2:
                        player_percentiles[col_idx] = 60
                    else:  # 1 year
                        player_percentiles[col_idx] = 20
                else:
                    player_percentiles[col_idx] = None
                continue
                
            value = historical_calculated_stats.get(stat_name, 0)
            # Skip percentile calculation if player has no historical minutes
            if not has_historical_minutes:
                player_percentiles[col_idx] = None
            # For shooting percentages, skip if no attempts
            elif stat_name == 'ts_pct' and value == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg2_pct' and hist_fg2a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'fg3_pct' and hist_fg3a == 0:
                player_percentiles[col_idx] = None
            elif stat_name == 'ft_pct' and hist_fta == 0:
                player_percentiles[col_idx] = None
            # Include zero values in percentile calculation for other stats
            elif value is not None and value != '':
                reverse = stat_name in REVERSE_STATS
                pct = get_percentile_rank(value, historical_percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        data_rows.append(row)
        percentile_data.append(player_percentiles)
    
    # Combine all data
    all_data = [header_row_1, header_row_2, filter_row] + data_rows
    
    # Clear and update
    worksheet.clear()
    
    spreadsheet = worksheet.spreadsheet
    
    # Remove existing banding
    sheet_metadata = spreadsheet.fetch_sheet_metadata({'includeGridData': False})
    for sheet in sheet_metadata.get('sheets', []):
        if sheet['properties']['sheetId'] == worksheet.id:
            banded_ranges = sheet.get('bandedRanges', [])
            if banded_ranges:
                delete_requests = [
                    {'deleteBanding': {'bandedRangeId': br['bandedRangeId']}}
                    for br in banded_ranges
                ]
                spreadsheet.batch_update({'requests': delete_requests})
                break
    
    worksheet.update(values=all_data, range_name='A1')
    
    total_rows = 3 + len(data_rows)
    total_cols = SHEET_FORMAT['total_columns']
    
    requests = []
    
    # Delete extra rows/columns
    sheet_properties = spreadsheet.fetch_sheet_metadata({'includeGridData': False})
    current_row_count = 1000
    current_col_count = 26
    for sheet in sheet_properties.get('sheets', []):
        if sheet['properties']['sheetId'] == worksheet.id:
            current_row_count = sheet['properties']['gridProperties'].get('rowCount', 1000)
            current_col_count = sheet['properties']['gridProperties'].get('columnCount', 26)
            break
    
    if current_row_count > total_rows:
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'ROWS',
                    'startIndex': total_rows,
                    'endIndex': current_row_count
                }
            }
        })
    
    if current_col_count > total_cols:
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'COLUMNS',
                    'startIndex': total_cols,
                    'endIndex': current_col_count
                }
            }
        })
    
    # Merge cells
    requests.append({
        'mergeCells': {
            'range': {
                'sheetId': worksheet.id,
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
                'sheetId': worksheet.id,
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
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 25,  # Z (YRS column)
                'endColumnIndex': 42,    # AQ (last historical stat)
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
                'sheetId': worksheet.id,
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
                'sheetId': worksheet.id,
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
                'sheetId': worksheet.id,
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
                'sheetId': worksheet.id,
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
                    'sheetId': worksheet.id,
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
                    'sheetId': worksheet.id,
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
                    'sheetId': worksheet.id,
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
                    'sheetId': worksheet.id,
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
                        'sheetId': worksheet.id,
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
        
        # Apply percentile colors to stat cells AFTER banding
        # This way, empty cells keep the alternating row colors, but cells with data get percentile colors
        for row_idx, player_percentiles in enumerate(percentile_data):
            for col_idx, percentile in player_percentiles.items():
                if percentile is not None:
                    color = get_color_for_percentile(percentile)
                    if color:
                        requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': worksheet.id,
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
    
    # Column widths
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
                'dimension': 'COLUMNS',
                'startIndex': 1,
                'endIndex': 2
            },
            'properties': {
                'pixelSize': SHEET_FORMAT['column_widths']['jersey_number']
            },
            'fields': 'pixelSize'
        }
    })
    
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
                'dimension': 'COLUMNS',
                'startIndex': 8,
                'endIndex': 9
            },
            'properties': {
                'pixelSize': SHEET_FORMAT['column_widths']['games']
            },
            'fields': 'pixelSize'
        }
    })
    
    # YRS column width for historical section
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
                'dimension': 'COLUMNS',
                'startIndex': 25,  # Column Z (YRS)
                'endIndex': 26
            },
            'properties': {
                'pixelSize': SHEET_FORMAT['column_widths']['years']
            },
            'fields': 'pixelSize'
        }
    })
    
    # Row heights - Set row 3 (filter row) to 15 pixels
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
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
                    'sheetId': worksheet.id,
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
    
    # Borders
    # Black border after weight column (between player info and current stats)
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 3,
                'endRowIndex': 3 + len(data_rows),
                'startColumnIndex': 6,
                'endColumnIndex': 7,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': black
            }
        }
    })
    
    # Black border after notes column (between player info and current stats)
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 3,
                'endRowIndex': 3 + len(data_rows),
                'startColumnIndex': 7,
                'endColumnIndex': 8,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': black
            }
        }
    })
    
    # BLACK border between current stats and historical stats (left of YRS column)
    # Column 24 (Y) is the last current stat column (Fls)
    # Column 25 (Z) is the first historical stat column (YRS)
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 3,
                'endRowIndex': 3 + len(data_rows),
                'startColumnIndex': 24,
                'endColumnIndex': 25,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': black
            }
        }
    })
    
    # White borders in header rows
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 6,
                'endColumnIndex': 7,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 7,
                'endColumnIndex': 8,
            },
            'left': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 7,
                'endColumnIndex': 8,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    # Border before historical section (after hidden column Z, before column AA)
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 25,  # Column Z (hidden player_id)
                'endColumnIndex': 26,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 25,  # Column Z (YRS - start of historical)
                'endColumnIndex': 26,
            },
            'left': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
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
    
    # WHITE border to the right of each column B-X in rows 2-3 (exclude Y, column 24)
    for col_idx in range(1, SHEET_FORMAT['total_columns'] - 1):
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': worksheet.id,
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
    
    # Freeze panes
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': worksheet.id,
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
                'sheetId': worksheet.id,
                'gridProperties': {
                    'hideGridlines': True
                }
            },
            'fields': 'gridProperties.hideGridlines'
        }
    })
    
    # Auto-resize columns (do this BEFORE hiding column Z so it sizes correctly)
    for col_idx in range(SHEET_FORMAT['total_columns']):
        # Skip columns we already set specific widths for
        if col_idx not in [1, 8, 25]:  # B (jersey), I (games), Z (years)
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': col_idx,
                        'endIndex': col_idx + 1
                    }
                }
            })
    
    # Hide player_id column (column AR) - do this AFTER auto-resize
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
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
                    'sheetId': worksheet.id,
                    'startRowIndex': 2,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': SHEET_FORMAT['total_columns']
                }
            }
        }
    })
    
    # Execute all requests
    spreadsheet.batch_update({'requests': requests})
    
    log(f"✅ {team_name} sheet created with {len(data_rows)} players")

def main():
    log("=" * 60)
    log("SYNCING ALL 30 NBA TEAMS TO GOOGLE SHEETS")
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
    log(f"Using stats mode: {stats_mode}" + (f" ({stats_custom_value} minutes)" if stats_custom_value else ""))
    
    # Parse historical stats configuration from environment variables
    past_years = 3  # Default
    include_current = False  # Default
    specific_seasons = None
    
    historical_mode = os.environ.get('HISTORICAL_MODE', 'years')
    include_current_env = os.environ.get('INCLUDE_CURRENT_YEAR', 'false')
    include_current = (include_current_env.lower() == 'true')
    
    if historical_mode == 'seasons':
        # Parse specific seasons (e.g., "2023-24, 2012-13, 1999-00")
        seasons_str = os.environ.get('HISTORICAL_SEASONS', '')
        if seasons_str:
            specific_seasons = []
            for season in seasons_str.split(','):
                season = season.strip()
                # Parse season format like "2023-24" or "1999-00"
                if '-' in season:
                    start_year = season.split('-')[0]
                    # Convert to 4-digit year (e.g., "2023-24" -> 2024, "1999-00" -> 2000)
                    if len(start_year) == 4:
                        year = int(start_year) + 1
                    else:
                        year = int(start_year)
                    specific_seasons.append(year)
            log(f"Using specific seasons: {specific_seasons}")
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
        log(f"Fetching historical data for specific seasons: {specific_seasons}...")
        historical_players = fetch_historical_players_data(conn, specific_seasons=specific_seasons)
    else:
        log(f"Fetching historical data for past {past_years} seasons (include_current={include_current})...")
        historical_players = fetch_historical_players_data(conn, past_years, include_current)
    
    log(f"✅ Fetched historical data for {len(historical_players)} players")
    
    # Calculate percentiles for current season
    log(f"Calculating percentiles across all players (current season) using mode: {stats_mode}...")
    percentiles, players_with_stats = calculate_percentiles(all_players, stats_mode, stats_custom_value)
    log("✅ Current season percentiles calculated")
    
    # Calculate percentiles for historical data
    log(f"Calculating percentiles for historical data using mode: {stats_mode}...")
    historical_percentiles, historical_players_with_stats = calculate_historical_percentiles(historical_players, stats_mode, stats_custom_value)
    log("✅ Historical percentiles calculated")
    
    conn.close()
    
    # Group ALL players by team (including those without stats)
    teams_data = {}
    for player in all_players:
        team_abbr = player['team_abbr']
        if team_abbr not in teams_data:
            teams_data[team_abbr] = []
        
        # Add calculated stats to player if they have stats
        for p_with_stats in players_with_stats:
            if p_with_stats['player_id'] == player['player_id']:
                player['calculated_stats'] = p_with_stats.get('calculated_stats', {})
                player['per100'] = p_with_stats.get('per100', {})
                break
        
        # Add historical stats to player if they exist
        for hist_player in historical_players_with_stats:
            if hist_player['player_id'] == player['player_id']:
                player['historical_calculated_stats'] = hist_player.get('calculated_stats', {})
                player['historical_per100'] = hist_player.get('per100', {})
                player['seasons_played'] = hist_player.get('seasons_played', 0)
                break
        
        teams_data[team_abbr].append(player)
    
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
    
    # Create/update sheets for each team
    # Process each team completely (current + historical) before moving to next
    # Check if there's a priority team to process first
    priority_team = os.environ.get('PRIORITY_TEAM_ABBR')
    
    # Reorder teams to process priority team first
    teams_to_process = list(NBA_TEAMS)
    if priority_team:
        priority_team = priority_team.upper()
        # Find the priority team in the list
        for i, (team_abbr, team_name) in enumerate(teams_to_process):
            if team_abbr == priority_team:
                # Move this team to the front
                priority_entry = teams_to_process.pop(i)
                teams_to_process.insert(0, priority_entry)
                log(f"📌 Priority team: {priority_team} will be processed first")
                break
    
    for idx, (team_abbr, team_name) in enumerate(teams_to_process):
        team_players = teams_data.get(team_abbr, [])
        if not team_players:
            log(f"⚠️  No data found for {team_name}, skipping...")
            continue
        
        try:
            worksheet = spreadsheet.worksheet(team_abbr)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=team_abbr, rows=100, cols=30)
        
        log(f"Updating {team_name} ({team_abbr}) - both current and historical stats...")
        create_team_sheet(worksheet, team_abbr, team_name, team_players, percentiles, historical_percentiles, past_years, stats_mode, stats_custom_value)
        log(f"✅ {team_name} complete")
        
        # Add a 5-second delay after each team to avoid rate limits
        if idx < len(teams_to_process) - 1:
            time.sleep(5)
    
    log("=" * 60)
    log("✅ SUCCESS! All teams synced to Google Sheets")
    log(f"   View it here: {spreadsheet.url}")
    log("=" * 60)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
