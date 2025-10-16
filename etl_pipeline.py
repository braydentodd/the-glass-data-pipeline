"""
THE GLASS - ETL Pipeline
Extracts NBA data and loads it into PostgreSQL

COMPLETE PIPELINE - Updates ALL tables:
1. games - Game records with scores and status (Scheduled → Final)
2. player_game_stats - Individual player stats per game
3. team_game_stats - Team stats per game (includes opponent stats)
4. player_season_stats - Aggregated player season averages
5. team_season_stats - Aggregated team season averages (includes opponent stats)

Features:
- Fetches and pre-populates upcoming games (game_status = 'Scheduled')
- Updates completed games with final scores (game_status = 'Final')
- Processes all game types (Regular Season, Playoffs, PlayIn, Pre Season, Summer League)
- Aggregates season statistics after each daily run
- Includes advanced stats: shot charts, matchup data, hustle stats, scoring stats
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv2,
    boxscoreadvancedv2,
    boxscorehustlev2,
    boxscorescoringv2,
    boxscorematchupsv3,
    shotchartdetail,
)

# ============================================
# CONFIGURATION
# ============================================

class Config:
    # PostgreSQL connection (set via environment variables in production)
    DB_HOST = os.getenv('DB_HOST', '150.136.255.23')
    DB_NAME = os.getenv('DB_NAME', 'the_glass_db')
    DB_USER = os.getenv('DB_USER', 'the_glass_user')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')  # Must be set via env var
    
    # Season configuration (auto-detected based on current date)
    @staticmethod
    def get_current_season():
        """
        Automatically determine the current NBA season based on the date.
        Season changes on July 1st each year.
        - Before July 1: Previous year's season (e.g., 2024-25 if it's May 2025)
        - On/after July 1: Next season (e.g., 2025-26 if it's July 2025)
        """
        now = datetime.now()
        if now.month >= 7:  # July 1 or later
            start_year = now.year
        else:  # Before July 1
            start_year = now.year - 1
        
        end_year = start_year + 1
        return f"{start_year}-{str(end_year)[2:]}"
    
    CURRENT_SEASON = get_current_season.__func__()  # Call the static method
    
    # All season types to fetch (includes Summer League!)
    SEASON_TYPES = ["Regular Season", "Playoffs", "PlayIn", "Pre Season", "Summer League"]
    
    # Date range for backfill (from env vars or None = today only)
    START_DATE = os.getenv('START_DATE')  # e.g., "2024-10-22" for backfill
    END_DATE = os.getenv('END_DATE')      # e.g., "2024-10-24" for backfill
    
    # Rate limiting and retry
    RATE_LIMIT_DELAY = 0.6  # 600ms between requests
    MAX_RETRIES = 3         # Number of retries for API calls
    RETRY_DELAY = 2         # Seconds to wait between retries
    
    # Logging
    LOG_FILE = "etl_pipeline.log"
    
    # In-Season Tournament detection
    IST_GROUP_STAGE_START = (11, 3)  # (month, day) - Nov 3
    IST_GROUP_STAGE_END = (12, 3)    # Dec 3
    IST_KNOCKOUT_START = (12, 4)     # Dec 4
    IST_CHAMPIONSHIP_DATES = [
        "2023-12-09",  # 2023-24 season
        "2024-12-17",  # 2024-25 season
        "2025-12-16",  # 2025-26 season (estimated)
    ]

# ============================================
# DATABASE CONNECTION
# ============================================

def get_db_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
        return conn
    except Exception as e:
        log_error(f"Database connection failed: {e}")
        raise

# Teams table population removed - use populate_teams.py script instead

# ============================================
# LOGGING
# ============================================

def log_message(message: str, level: str = "INFO"):
    """Log message to console and file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {level}: {message}"
    print(log_line)
    
    with open(Config.LOG_FILE, 'a') as f:
        f.write(log_line + "\n")

def log_info(message: str):
    log_message(message, "INFO")

def log_error(message: str):
    log_message(message, "ERROR")

def log_success(message: str):
    log_message(message, "SUCCESS")

# ============================================
# UTILITY FUNCTIONS
# ============================================

def rate_limit():
    """Sleep to avoid API rate limits"""
    time.sleep(Config.RATE_LIMIT_DELAY)

def retry_api_call(func, *args, **kwargs):
    """Retry an API call with exponential backoff"""
    for attempt in range(Config.MAX_RETRIES):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt < Config.MAX_RETRIES - 1:
                wait_time = Config.RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                log_info(f"  Retry {attempt + 1}/{Config.MAX_RETRIES} after {wait_time}s due to: {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                raise  # Re-raise on final attempt

def safe_float(value, default=None):
    """Safely convert to float"""
    if pd.isna(value) or value is None or value == '':
        return default
    try:
        return float(value)
    except:
        return default

def safe_int(value, default=None):
    """Safely convert to int"""
    if pd.isna(value) or value is None or value == '':
        return default
    try:
        return int(value)
    except:
        return default

def safe_str(value):
    """Safely convert value to str, handling None and numpy types"""
    if value is None or value == '' or (hasattr(value, '__len__') and len(value) == 0):
        return None
    if pd.isna(value):
        return None
    return str(value)

def parse_minutes(min_str):
    """Convert MIN string (e.g., '33:45') to decimal minutes"""
    if pd.isna(min_str) or min_str is None or min_str == '':
        return None
    try:
        if isinstance(min_str, (int, float)):
            return float(min_str)
        parts = str(min_str).split(':')
        return float(parts[0]) + float(parts[1]) / 60
    except:
        return None

# ============================================
# SCHEMA OPTIMIZATION HELPERS
# ============================================

def scale_minutes(minutes):
    """Convert minutes to smallint (tenths of minutes, x10)"""
    if minutes is None:
        return None
    return round(minutes * 10)

def scale_percentage(pct):
    """Convert percentage (0.0-1.0) to smallint (x1000)"""
    if pct is None:
        return None
    return round(pct * 1000)

def scale_rating(rating):
    """Convert rating to smallint (x100)"""
    if rating is None:
        return None
    return round(rating * 100)

def to_smallint(value):
    """Safely convert integer to smallint (ensure within range -32768 to 32767)"""
    if value is None:
        return None
    val = int(value)
    if val < -32768 or val > 32767:
        log_error(f"Value {val} out of smallint range, capping")
        return max(-32768, min(32767, val))
    return val

# ============================================
# DATA EXTRACTION
# ============================================

def detect_ist_game(game_date: str, game_count: int = 1) -> str:
    """
    Detect if a game is part of the In-Season Tournament
    Returns: 'IST Championship', 'IST', or None
    """
    # Check if it's the championship date
    if game_date in Config.IST_CHAMPIONSHIP_DATES:
        return 'IST Championship'
    
    # Check if date falls in IST window (Nov 3 - Dec 15)
    try:
        date_obj = datetime.strptime(game_date, "%Y-%m-%d")
        month = date_obj.month
        day = date_obj.day
        
        # IST runs from early November through mid-December
        if month == 11 and day >= Config.IST_GROUP_STAGE_START[1]:
            return 'IST'  # Group stage
        elif month == 12 and day >= Config.IST_KNOCKOUT_START[1] and day <= 15:
            return 'IST'  # Knockout rounds (except championship)
    except:
        pass
    
    return None

def fetch_upcoming_games(season: str = None, season_type: str = 'Regular Season') -> List[Dict]:
    """
    Fetch all scheduled (upcoming) games for the current season
    Returns: List of game dicts with basic info (no stats yet)
    """
    if season is None:
        season = Config.CURRENT_SEASON
    
    log_info(f"Fetching upcoming games for {season} {season_type}")
    
    try:
        def fetch_games():
            finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type,
                league_id_nullable='00'  # NBA
            )
            return finder.get_data_frames()[0]
        
        games_df = retry_api_call(fetch_games)
        
        if games_df.empty:
            log_info("No games found")
            return []
        
        # Group by GAME_ID to get unique games (API returns one row per team)
        unique_games = games_df.drop_duplicates(subset=['GAME_ID'])
        
        upcoming_games = []
        for _, game in unique_games.iterrows():
            game_id = game['GAME_ID']
            game_date = game['GAME_DATE']
            
            # Parse game date
            if isinstance(game_date, str):
                game_date = datetime.strptime(game_date, '%Y-%m-%d').date()
            
            # Get both teams for this game
            game_teams = games_df[games_df['GAME_ID'] == game_id]
            if len(game_teams) == 2:
                # First row is visitor (away), second is home
                away_team = game_teams.iloc[0]
                home_team = game_teams.iloc[1]
                
                upcoming_games.append({
                    'game_id': game_id,
                    'game_date': game_date,
                    'season': season,
                    'season_type': season_type,
                    'home_team_id': safe_int(home_team['TEAM_ID']),
                    'away_team_id': safe_int(away_team['TEAM_ID']),
                    'home_score': None,  # Not played yet
                    'away_score': None,
                    'game_status': 'Scheduled'
                })
        
        log_info(f"Found {len(upcoming_games)} upcoming games")
        return upcoming_games
        
    except Exception as e:
        log_error(f"Failed to fetch upcoming games: {e}")
        return []

def fetch_games_for_date(game_date: str) -> List[Tuple[str, str]]:
    """
    Fetch game IDs for a specific date across all season types
    Returns: List of (game_id, season_type) tuples
    """
    log_info(f"Fetching games for {game_date}")
    
    all_games = []
    
    # Try each season type
    for season_type in Config.SEASON_TYPES:
        try:
            # Use retry logic for API call
            def fetch_games():
                gf = leaguegamefinder.LeagueGameFinder(
                    season_nullable=Config.CURRENT_SEASON,
                    season_type_nullable=season_type,
                    date_from_nullable=game_date,
                    date_to_nullable=game_date
                )
                return gf.get_data_frames()[0]
            
            games_df = retry_api_call(fetch_games)
            
            if not games_df.empty:
                # Get unique game IDs
                unique_games = games_df.drop_duplicates(subset=['GAME_ID'])
                
                for game_id in unique_games['GAME_ID'].tolist():
                    # Check if this is an IST game
                    ist_type = detect_ist_game(game_date, len(unique_games))
                    
                    if ist_type and season_type == "Regular Season":
                        # Mark IST games specially
                        all_games.append((game_id, ist_type))
                        log_info(f"  Found {ist_type} game: {game_id}")
                    else:
                        # Regular game
                        all_games.append((game_id, season_type))
                
                log_info(f"  {season_type}: {len(unique_games)} games")
            
            rate_limit()
            
        except Exception as e:
            if "resultSet" not in str(e):  # Ignore invalid season type errors
                log_error(f"Error fetching {season_type} games: {e}")
            rate_limit()
    
    if not all_games:
        log_info(f"No games found for {game_date}")
    else:
        log_info(f"Found {len(all_games)} total games")
    
    return all_games

def fetch_box_scores(game_id: str) -> Dict[str, pd.DataFrame]:
    """Fetch all box score data for a game"""
    log_info(f"Fetching box scores for game {game_id}")
    
    box_scores = {}
    
    # Traditional box score
    try:
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        box_scores['traditional_player'] = box.get_data_frames()[0]
        box_scores['traditional_team'] = box.get_data_frames()[1]
        log_info(f"  ✓ Traditional: {len(box_scores['traditional_player'])} players")
    except Exception as e:
        log_error(f"  ✗ Traditional failed: {e}")
        box_scores['traditional_player'] = pd.DataFrame()
        box_scores['traditional_team'] = pd.DataFrame()
    finally:
        rate_limit()
    
    # Advanced box score
    try:
        box = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
        box_scores['advanced_player'] = box.get_data_frames()[0]
        box_scores['advanced_team'] = box.get_data_frames()[1]
        log_info(f"  ✓ Advanced: {len(box_scores['advanced_player'])} players")
    except Exception as e:
        log_error(f"  ✗ Advanced failed: {e}")
        box_scores['advanced_player'] = pd.DataFrame()
        box_scores['advanced_team'] = pd.DataFrame()
    finally:
        rate_limit()
    
    # Hustle stats
    try:
        box = boxscorehustlev2.BoxScoreHustleV2(game_id=game_id)
        box_scores['hustle_player'] = box.get_data_frames()[0]
        box_scores['hustle_team'] = box.get_data_frames()[1]
        log_info(f"  ✓ Hustle: {len(box_scores['hustle_player'])} players")
    except Exception as e:
        log_info(f"  ⚠ Hustle not available (expected for some games)")
        box_scores['hustle_player'] = pd.DataFrame()
        box_scores['hustle_team'] = pd.DataFrame()
    finally:
        rate_limit()
    
    # Scoring stats
    try:
        box = boxscorescoringv2.BoxScoreScoringV2(game_id=game_id)
        box_scores['scoring_player'] = box.get_data_frames()[0]
        box_scores['scoring_team'] = box.get_data_frames()[1]
        log_info(f"  ✓ Scoring: {len(box_scores['scoring_player'])} players")
    except Exception as e:
        log_error(f"  ✗ Scoring failed: {e}")
        box_scores['scoring_player'] = pd.DataFrame()
        box_scores['scoring_team'] = pd.DataFrame()
    finally:
        rate_limit()
    
    return box_scores

def fetch_shot_chart_data(game_id: str, player_ids: List[int], season_type: str = 'Regular Season') -> Dict[int, pd.DataFrame]:
    """Fetch shot chart data for all players in a game"""
    log_info(f"Fetching shot chart data for {len(player_ids)} players")
    
    shot_charts = {}
    
    # Map our season types to NBA API season_type_all_star values
    season_type_map = {
        'Regular Season': 'Regular Season',
        'Playoffs': 'Playoffs',
        'PlayIn': 'Playoffs',  # API treats PlayIn as Playoffs
        'Pre Season': 'Pre Season',
        'Summer League': 'Regular Season',  # Summer League uses Regular Season endpoint
        'IST': 'Regular Season',
        'IST Championship': 'Regular Season'
    }
    
    api_season_type = season_type_map.get(season_type, 'Regular Season')
    
    for player_id in player_ids:
        try:
            shots = shotchartdetail.ShotChartDetail(
                team_id=0,
                player_id=player_id,
                game_id_nullable=game_id,
                context_measure_simple='FGA',
                season_nullable=Config.CURRENT_SEASON,
                season_type_all_star=api_season_type
            )
            
            shot_df = shots.get_data_frames()[0]
            if not shot_df.empty:
                shot_charts[player_id] = shot_df
            
            rate_limit()
            
        except Exception as e:
            log_error(f"  ✗ Shot chart failed for player {player_id}: {e}")
            continue
    
    log_info(f"  ✓ Retrieved shot charts for {len(shot_charts)} players")
    return shot_charts

def fetch_matchup_data(game_id: str) -> Dict:
    """Fetch defensive matchup data for a game"""
    log_info(f"Fetching matchup data for game {game_id}")
    
    try:
        box = boxscorematchupsv3.BoxScoreMatchupsV3(game_id=game_id)
        matchup_data = box.get_dict()
        log_info(f"  ✓ Retrieved matchup data")
        return matchup_data
    except Exception as e:
        log_info(f"  ⚠ Matchup data not available: {e}")
        return {}
    finally:
        rate_limit()

# ============================================
# DATA TRANSFORMATION
# ============================================

def aggregate_shot_chart_stats(shot_df: pd.DataFrame) -> Dict:
    """
    Aggregate shot chart data into location-based stats.
    
    NOTE: Only uses RELIABLE shot zone data from NBA API.
    Does NOT approximate unassisted/open shots via string matching due to fragility.
    """
    
    stats = {
        'rim_fga': None,
        'rim_fg_pct': None,
        'uast_rim_fga': None,  # Not available - set to None
        'mr_fga': None,
        'mr_fg_pct': None,
        'uast_mr_fga': None,  # Not available - set to None
        'open_3pa': None,  # Not available - set to None
        'open_3p_pct': None,  # Not available - set to None
    }
    
    if shot_df.empty:
        return stats
    
    # Rim shots (Restricted Area) - RELIABLE from SHOT_ZONE_BASIC
    rim_shots = shot_df[shot_df['SHOT_ZONE_BASIC'] == 'Restricted Area']
    if not rim_shots.empty:
        stats['rim_fga'] = len(rim_shots)
        rim_made = rim_shots[rim_shots['SHOT_MADE_FLAG'] == 1]
        stats['rim_fg_pct'] = len(rim_made) / len(rim_shots) if len(rim_shots) > 0 else None
        # NOTE: uast_rim_fga left as None - no reliable data for unassisted shots
    
    # Mid-range shots (8-16 ft and 16-24 ft) - RELIABLE from SHOT_ZONE_RANGE
    mr_shots = shot_df[
        shot_df['SHOT_ZONE_RANGE'].isin(['8-16 ft.', '16-24 ft.'])
    ]
    if not mr_shots.empty:
        stats['mr_fga'] = len(mr_shots)
        mr_made = mr_shots[mr_shots['SHOT_MADE_FLAG'] == 1]
        stats['mr_fg_pct'] = len(mr_made) / len(mr_shots) if len(mr_shots) > 0 else None
        # NOTE: uast_mr_fga left as None - no reliable data for unassisted shots
    
    # NOTE: open_3pa and open_3p_pct left as None
    # NBA API shot chart does not include reliable defender distance or contest data
    # Approximating via ACTION_TYPE string matching is too fragile and unreliable
    
    return stats

def aggregate_matchup_stats(matchup_data: Dict, player_id: int) -> Dict:
    """Aggregate matchup data for a specific player"""
    
    stats = {
        'cont_3pa': None,
        'cont_3p_pct': None,
        'def_efg_pct': None,
    }
    
    if not matchup_data or 'boxScoreMatchups' not in matchup_data:
        return stats
    
    try:
        matchups = matchup_data['boxScoreMatchups']
        
        # Find this player's defensive matchups
        player_matchups = [m for m in matchups if m.get('personId') == player_id]
        
        if not player_matchups:
            return stats
        
        # Aggregate across all matchups
        total_cont_3pa = 0
        total_cont_3pm = 0
        total_def_fga = 0
        total_def_fgm = 0
        total_def_3pm = 0
        
        for matchup in player_matchups:
            # Contested 3-pointers
            cont_3pa = matchup.get('contestedShots3pt', 0)
            cont_3pm = matchup.get('contestedShots3ptMade', 0)
            total_cont_3pa += cont_3pa if cont_3pa else 0
            total_cont_3pm += cont_3pm if cont_3pm else 0
            
            # Defensive eFG% components
            def_fga = matchup.get('defensiveFga', 0)
            def_fgm = matchup.get('defensiveFgm', 0)
            def_3pm = matchup.get('defensive3pm', 0)
            total_def_fga += def_fga if def_fga else 0
            total_def_fgm += def_fgm if def_fgm else 0
            total_def_3pm += def_3pm if def_3pm else 0
        
        # Calculate stats
        if total_cont_3pa > 0:
            stats['cont_3pa'] = total_cont_3pa
            stats['cont_3p_pct'] = total_cont_3pm / total_cont_3pa
        
        if total_def_fga > 0:
            # Defensive eFG% = (FGM + 0.5 * 3PM) / FGA
            # Accounts for 3-pointers being worth 1.5x as much
            stats['def_efg_pct'] = (total_def_fgm + 0.5 * total_def_3pm) / total_def_fga
    
    except Exception as e:
        log_error(f"Error aggregating matchup stats for player {player_id}: {e}")
    
    return stats

def transform_player_game_stats(
    game_id: str, 
    game_date: str, 
    box_scores: Dict[str, pd.DataFrame],
    shot_charts: Dict[int, pd.DataFrame] = None,
    matchup_data: Dict = None
) -> List[Dict]:
    """Transform box score data into player_game_stats records"""
    
    trad = box_scores['traditional_player']
    adv = box_scores['advanced_player']
    hustle = box_scores['hustle_player']
    scoring = box_scores['scoring_player']
    
    if trad.empty:
        log_error("No traditional stats available")
        return []
    
    # Get team IDs to determine home/away and opponent
    # NBA API returns home team first, then away team
    team_ids = trad['TEAM_ID'].unique().tolist()
    if len(team_ids) != 2:
        log_error(f"Expected 2 teams, found {len(team_ids)}")
        return []
    
    home_team_id = team_ids[0]
    away_team_id = team_ids[1]
    
    # Merge all dataframes on PLAYER_ID
    merged = trad.copy()
    
    if not adv.empty:
        merged = merged.merge(adv[['PLAYER_ID', 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 
                                     'POSS', 'TS_PCT', 'OREB_PCT', 'DREB_PCT']], 
                              on='PLAYER_ID', how='left', suffixes=('', '_adv'))
    
    if not hustle.empty:
        # Hustle uses 'personId' instead of 'PLAYER_ID'
        hustle_renamed = hustle.rename(columns={'personId': 'PLAYER_ID'})
        merged = merged.merge(hustle_renamed[['PLAYER_ID', 'chargesDrawn', 'deflections', 'contestedShots']], 
                              on='PLAYER_ID', how='left', suffixes=('', '_hustle'))
    
    if not scoring.empty:
        merged = merged.merge(scoring[['PLAYER_ID', 'PCT_UAST_3PM', 'PCT_UAST_FGM']], 
                              on='PLAYER_ID', how='left', suffixes=('', '_scoring'))
    
    # Transform to records
    records = []
    
    for _, row in merged.iterrows():
        player_id = safe_int(row.get('PLAYER_ID'))
        
        # Calculate fg2a and fg2_pct
        fga = safe_int(row.get('FGA'))
        fg3a = safe_int(row.get('FG3A'))
        fgm = safe_int(row.get('FGM'))
        fg3m = safe_int(row.get('FG3M'))
        
        fg2a = None
        fg2_pct = None
        if fga is not None and fg3a is not None:
            fg2a = fga - fg3a
            if fg2a > 0 and fgm is not None and fg3m is not None:
                fg2m = fgm - fg3m
                fg2_pct = fg2m / fg2a
        
        # Calculate unassisted 3PA (approximate from scoring data)
        uast_3fga = None
        pct_uast_3pm = safe_float(row.get('PCT_UAST_3PM'))
        if pct_uast_3pm is not None and fg3a is not None:
            uast_3fga = int(pct_uast_3pm * fg3a)
        
        # Get shot chart stats for this player
        shot_stats = {}
        if shot_charts and player_id in shot_charts:
            shot_stats = aggregate_shot_chart_stats(shot_charts[player_id])
        
        # Get matchup stats for this player
        matchup_stats = {}
        if matchup_data:
            matchup_stats = aggregate_matchup_stats(matchup_data, player_id)
        
        # Determine opponent and home/away
        team_id = safe_int(row.get('TEAM_ID'))
        is_home = team_id == home_team_id
        opponent_team_id = away_team_id if is_home else home_team_id
        
        record = {
            'game_id': game_id,
            'player_id': player_id,
            'team_id': team_id,
            'opponent_team_id': opponent_team_id,
            'is_home': is_home,
            'game_date': game_date,
            
            # Basic - SCALED
            'minutes': scale_minutes(parse_minutes(row.get('MIN'))),  # x10 -> smallint
            'points': safe_int(row.get('PTS')),
            
            # Shooting - SCALED PERCENTAGES
            'fg2a': to_smallint(fg2a),
            'fg2_pct': scale_percentage(fg2_pct),  # x1000 -> smallint
            'fg3a': to_smallint(fg3a),
            'fg3_pct': scale_percentage(safe_float(row.get('FG3_PCT'))),  # x1000 -> smallint
            'fta': to_smallint(safe_int(row.get('FTA'))),
            'ft_pct': scale_percentage(safe_float(row.get('FT_PCT'))),  # x1000 -> smallint
            
            # Rebounding - SCALED
            'off_rebs': to_smallint(safe_int(row.get('OREB'))),
            'def_rebs': to_smallint(safe_int(row.get('DREB'))),
            
            # Playmaking - SCALED
            'assists': to_smallint(safe_int(row.get('AST'))),
            'turnovers': to_smallint(safe_int(row.get('TO'))),
            
            # Defense - SCALED
            'steals': to_smallint(safe_int(row.get('STL'))),
            'blocks': to_smallint(safe_int(row.get('BLK'))),
            
            # Impact - SCALED
            'plus_minus': to_smallint(safe_int(row.get('PLUS_MINUS'))),
            'on_off': scale_rating(safe_float(row.get('NET_RATING'))),  # x100 -> smallint
            'off_rtg': scale_rating(safe_float(row.get('OFF_RATING'))),  # x100 -> smallint
            'def_rtg': scale_rating(safe_float(row.get('DEF_RATING'))),  # x100 -> smallint
            'possessions': safe_int(row.get('POSS')),
            
            # Advanced Shooting - SCALED PERCENTAGES
            'ts_pct': scale_percentage(safe_float(row.get('TS_PCT'))),  # x1000 -> smallint
            
            # Shot location (from shot chart aggregation) - SCALED
            'rim_fga': to_smallint(shot_stats.get('rim_fga')),
            'rim_fg_pct': scale_percentage(shot_stats.get('rim_fg_pct')),  # x1000 -> smallint
            'uast_rim_fga': to_smallint(shot_stats.get('uast_rim_fga')),
            'mr_fga': to_smallint(shot_stats.get('mr_fga')),
            'mr_fg_pct': scale_percentage(shot_stats.get('mr_fg_pct')),  # x1000 -> smallint

            'uast_mr_fga': to_smallint(shot_stats.get('uast_mr_fga')),
            'uast_3fga': to_smallint(uast_3fga),
            'cont_3pa': to_smallint(matchup_stats.get('cont_3pa')),
            'cont_3p_pct': scale_percentage(matchup_stats.get('cont_3p_pct')),  # x1000 -> smallint
            'open_3pa': to_smallint(shot_stats.get('open_3pa')),
            'open_3p_pct': scale_percentage(shot_stats.get('open_3p_pct')),  # x1000 -> smallint
            
            # Playmaking Advanced (tracking not available - dropped)
            'pot_assists': None,
            'on_ball_pct': scale_percentage(None),  # x1000 -> smallint (NULL)
            'avg_sec_touch': None,
            
            # Rebounding Advanced - SCALED PERCENTAGES
            'oreb_pct': scale_percentage(safe_float(row.get('OREB_PCT'))),  # x1000 -> smallint
            'dreb_pct': scale_percentage(safe_float(row.get('DREB_PCT'))),  # x1000 -> smallint
            
            # Hustle - SCALED
            'off_distance': None,  # Tracking not available - dropped
            'charges_drawn': to_smallint(safe_int(row.get('chargesDrawn'))),
            'deflections': to_smallint(safe_int(row.get('deflections'))),
            'contests': to_smallint(safe_int(row.get('contestedShots'))),
            'def_efg_pct': scale_percentage(matchup_stats.get('def_efg_pct')),  # x1000 -> smallint
            'def_distance': None,  # Tracking not available - dropped
        }
        
        records.append(record)
    
    log_info(f"Transformed {len(records)} player records")
    return records

def transform_team_game_stats(
    game_id: str,
    game_date: str,
    box_scores: Dict[str, pd.DataFrame],
    shot_charts: Dict[int, pd.DataFrame] = None,
    matchup_data: Dict = None
) -> List[Dict]:
    """Transform box score data into team_game_stats records (includes opponent stats)"""
    
    trad_team = box_scores['traditional_team']
    adv_team = box_scores['advanced_team']
    hustle_team = box_scores['hustle_team']
    scoring_team = box_scores['scoring_team']
    
    if trad_team.empty:
        log_error("No team traditional stats available")
        return []
    
    # Get team stats
    merged_team = trad_team.copy()
    
    if not adv_team.empty:
        merged_team = merged_team.merge(
            adv_team[['TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'TS_PCT', 'OREB_PCT', 'DREB_PCT']],
            on='TEAM_ID', how='left', suffixes=('', '_adv')
        )
    
    if not hustle_team.empty:
        hustle_team_renamed = hustle_team.rename(columns={'teamId': 'TEAM_ID'})
        merged_team = merged_team.merge(
            hustle_team_renamed[['TEAM_ID', 'chargesDrawn', 'deflections', 'contestedShots']],
            on='TEAM_ID', how='left', suffixes=('', '_hustle')
        )
    
    if not scoring_team.empty:
        merged_team = merged_team.merge(
            scoring_team[['TEAM_ID', 'PCT_UAST_3PM', 'PCT_UAST_FGM']],
            on='TEAM_ID', how='left', suffixes=('', '_scoring')
        )
    
    # Aggregate shot chart stats by team
    team_shot_stats = {}
    if shot_charts:
        for player_id, shot_df in shot_charts.items():
            if shot_df.empty:
                continue
            team_id = shot_df['TEAM_ID'].iloc[0] if 'TEAM_ID' in shot_df.columns else None
            if team_id:
                if team_id not in team_shot_stats:
                    team_shot_stats[team_id] = {
                        'rim_fga': 0, 'rim_fgm': 0,
                        'mr_fga': 0, 'mr_fgm': 0,
                        'open_3pa': 0, 'open_3pm': 0,
                        'uast_rim_fga': 0, 'uast_mr_fga': 0
                    }
                
                stats = aggregate_shot_chart_stats(shot_df)
                if stats.get('rim_fga'):
                    team_shot_stats[team_id]['rim_fga'] += stats['rim_fga']
                    team_shot_stats[team_id]['rim_fgm'] += int(stats['rim_fga'] * stats['rim_fg_pct']) if stats.get('rim_fg_pct') else 0
                if stats.get('uast_rim_fga'):
                    team_shot_stats[team_id]['uast_rim_fga'] += stats['uast_rim_fga']
                if stats.get('mr_fga'):
                    team_shot_stats[team_id]['mr_fga'] += stats['mr_fga']
                    team_shot_stats[team_id]['mr_fgm'] += int(stats['mr_fga'] * stats['mr_fg_pct']) if stats.get('mr_fg_pct') else 0
                if stats.get('uast_mr_fga'):
                    team_shot_stats[team_id]['uast_mr_fga'] += stats['uast_mr_fga']
                if stats.get('open_3pa'):
                    team_shot_stats[team_id]['open_3pa'] += stats['open_3pa']
                    team_shot_stats[team_id]['open_3pm'] += int(stats['open_3pa'] * stats['open_3p_pct']) if stats.get('open_3p_pct') else 0
    
    # Calculate percentages for aggregated shot stats
    for team_id, stats in team_shot_stats.items():
        stats['rim_fg_pct'] = stats['rim_fgm'] / stats['rim_fga'] if stats['rim_fga'] > 0 else None
        stats['mr_fg_pct'] = stats['mr_fgm'] / stats['mr_fga'] if stats['mr_fga'] > 0 else None
        stats['open_3p_pct'] = stats['open_3pm'] / stats['open_3pa'] if stats['open_3pa'] > 0 else None
    
    # Aggregate matchup stats by team
    team_matchup_stats = {}
    if matchup_data and 'boxScoreMatchups' in matchup_data:
        for matchup in matchup_data['boxScoreMatchups']:
            team_id = matchup.get('teamId')
            if team_id:
                if team_id not in team_matchup_stats:
                    team_matchup_stats[team_id] = {
                        'cont_3pa': 0, 'cont_3pm': 0,
                        'def_fga': 0, 'def_fgm': 0
                    }
                
                cont_3pa = matchup.get('contestedShots3pt', 0) or 0
                cont_3pm = matchup.get('contestedShots3ptMade', 0) or 0
                def_fga = matchup.get('defendingFga', 0) or 0
                def_fgm = matchup.get('defendingFgm', 0) or 0
                
                team_matchup_stats[team_id]['cont_3pa'] += cont_3pa
                team_matchup_stats[team_id]['cont_3pm'] += cont_3pm
                team_matchup_stats[team_id]['def_fga'] += def_fga
                team_matchup_stats[team_id]['def_fgm'] += def_fgm
    
    # Calculate percentages for aggregated matchup stats
    for team_id, stats in team_matchup_stats.items():
        stats['cont_3p_pct'] = stats['cont_3pm'] / stats['cont_3pa'] if stats['cont_3pa'] > 0 else None
        stats['def_efg_pct'] = stats['def_fgm'] / stats['def_fga'] if stats['def_fga'] > 0 else None
    
    records = []
    
    # Process both teams (should be exactly 2 rows)
    if len(merged_team) != 2:
        log_error(f"Expected 2 teams, got {len(merged_team)}")
        return []
    
    for idx, row in merged_team.iterrows():
        team_id = safe_int(row.get('TEAM_ID'))
        
        # Determine opponent
        opponent_row = merged_team[merged_team['TEAM_ID'] != team_id].iloc[0]
        opponent_team_id = safe_int(opponent_row.get('TEAM_ID'))
        
        # Determine home/away
        # The first team in the dataframe is typically the home team
        is_home = idx == 0
        
        # Calculate fg2a and fg2_pct
        fga = safe_int(row.get('FGA'))
        fg3a = safe_int(row.get('FG3A'))
        fgm = safe_int(row.get('FGM'))
        fg3m = safe_int(row.get('FG3M'))
        
        fg2a = None
        fg2_pct = None
        if fga is not None and fg3a is not None:
            fg2a = fga - fg3a
            fg2m = (fgm - fg3m) if (fgm is not None and fg3m is not None) else None
            fg2_pct = fg2m / fg2a if (fg2a > 0 and fg2m is not None) else None
        
        # Calculate unassisted 3PA
        uast_3fga = None
        pct_uast_3pm = safe_float(row.get('PCT_UAST_3PM'))
        if pct_uast_3pm is not None and fg3a is not None:
            uast_3fga = int(fg3a * pct_uast_3pm)
        
        # Get aggregated shot stats
        shot_stats = team_shot_stats.get(team_id, {})
        
        # Get aggregated matchup stats
        matchup_stats = team_matchup_stats.get(team_id, {})
        
        # Opponent stats (mirror of opponent row)
        opp_fga = safe_int(opponent_row.get('FGA'))
        opp_fg3a = safe_int(opponent_row.get('FG3A'))
        opp_fgm = safe_int(opponent_row.get('FGM'))
        opp_fg3m = safe_int(opponent_row.get('FG3M'))
        
        opp_fg2a = None
        opp_fg2_pct = None
        if opp_fga is not None and opp_fg3a is not None:
            opp_fg2a = opp_fga - opp_fg3a
            opp_fg2m = (opp_fgm - opp_fg3m) if (opp_fgm is not None and opp_fg3m is not None) else None
            opp_fg2_pct = opp_fg2m / opp_fg2a if (opp_fg2a > 0 and opp_fg2m is not None) else None
        
        opp_uast_3fga = None
        opp_pct_uast_3pm = safe_float(opponent_row.get('PCT_UAST_3PM'))
        if opp_pct_uast_3pm is not None and opp_fg3a is not None:
            opp_uast_3fga = int(opp_fg3a * opp_pct_uast_3pm)
        
        opp_shot_stats = team_shot_stats.get(opponent_team_id, {})
        opp_matchup_stats = team_matchup_stats.get(opponent_team_id, {})
        
        record = {
            'game_id': game_id,
            'team_id': team_id,
            'opponent_team_id': opponent_team_id,
            'is_home': is_home,
            'minutes': scale_minutes(parse_minutes(row.get('MIN'))),  # x10 -> smallint
            'points': safe_int(row.get('PTS')),
            'fg2a': to_smallint(fg2a),
            'fg2_pct': scale_percentage(fg2_pct),  # x1000 -> smallint
            'fg3a': to_smallint(fg3a),
            'fg3_pct': scale_percentage(safe_float(row.get('FG3_PCT'))),  # x1000 -> smallint
            'fta': to_smallint(safe_int(row.get('FTA'))),
            'ft_pct': scale_percentage(safe_float(row.get('FT_PCT'))),  # x1000 -> smallint
            'off_rebs': to_smallint(safe_int(row.get('OREB'))),
            'def_rebs': to_smallint(safe_int(row.get('DREB'))),
            'assists': to_smallint(safe_int(row.get('AST'))),
            'turnovers': to_smallint(safe_int(row.get('TO'))),
            'steals': to_smallint(safe_int(row.get('STL'))),
            'blocks': to_smallint(safe_int(row.get('BLK'))),
            'plus_minus': to_smallint(safe_int(row.get('PLUS_MINUS'))),
            'off_rtg': scale_rating(safe_float(row.get('OFF_RATING'))),  # x100 -> smallint
            'def_rtg': scale_rating(safe_float(row.get('DEF_RATING'))),  # x100 -> smallint
            'possessions': safe_int(row.get('POSS')),
            'ts_pct': scale_percentage(safe_float(row.get('TS_PCT'))),  # x1000 -> smallint
            'rim_fga': to_smallint(shot_stats.get('rim_fga')),
            'rim_fg_pct': scale_percentage(shot_stats.get('rim_fg_pct')),  # x1000 -> smallint
            'uast_rim_fga': to_smallint(shot_stats.get('uast_rim_fga')),
            'mr_fga': to_smallint(shot_stats.get('mr_fga')),
            'mr_fg_pct': scale_percentage(shot_stats.get('mr_fg_pct')),  # x1000 -> smallint
            'uast_mr_fga': to_smallint(shot_stats.get('uast_mr_fga')),
            'uast_3fga': to_smallint(uast_3fga),
            'cont_3pa': to_smallint(matchup_stats.get('cont_3pa')),
            'cont_3p_pct': scale_percentage(matchup_stats.get('cont_3p_pct')),  # x1000 -> smallint
            'open_3pa': to_smallint(shot_stats.get('open_3pa')),
            'open_3p_pct': scale_percentage(shot_stats.get('open_3p_pct')),  # x1000 -> smallint
            'pot_assists': None,
            'avg_sec_touch': None,
            'oreb_pct': scale_percentage(safe_float(row.get('OREB_PCT'))),  # x1000 -> smallint
            'off_distance': None,
            'charges_drawn': to_smallint(safe_int(row.get('chargesDrawn'))),
            'deflections': to_smallint(safe_int(row.get('deflections'))),
            'contests': to_smallint(safe_int(row.get('contestedShots'))),
            'def_efg_pct': scale_percentage(matchup_stats.get('def_efg_pct')),  # x1000 -> smallint
            'dreb_pct': scale_percentage(safe_float(row.get('DREB_PCT'))),  # x1000 -> smallint
            'def_distance': None,
            # Opponent stats - ALL SCALED
            'opp_points': safe_int(opponent_row.get('PTS')),
            'opp_fg2a': to_smallint(opp_fg2a),
            'opp_fg2_pct': scale_percentage(opp_fg2_pct),  # x1000 -> smallint
            'opp_fg3a': to_smallint(opp_fg3a),
            'opp_fg3_pct': scale_percentage(safe_float(opponent_row.get('FG3_PCT'))),  # x1000 -> smallint
            'opp_fta': to_smallint(safe_int(opponent_row.get('FTA'))),
            'opp_ft_pct': scale_percentage(safe_float(opponent_row.get('FT_PCT'))),  # x1000 -> smallint
            'opp_off_rebs': to_smallint(safe_int(opponent_row.get('OREB'))),
            'opp_def_rebs': to_smallint(safe_int(opponent_row.get('DREB'))),
            'opp_assists': to_smallint(safe_int(opponent_row.get('AST'))),
            'opp_turnovers': to_smallint(safe_int(opponent_row.get('TO'))),
            'opp_steals': to_smallint(safe_int(opponent_row.get('STL'))),
            'opp_blocks': to_smallint(safe_int(opponent_row.get('BLK'))),
            'opp_off_rtg': scale_rating(safe_float(opponent_row.get('OFF_RATING'))),  # x100 -> smallint
            'opp_def_rtg': scale_rating(safe_float(opponent_row.get('DEF_RATING'))),  # x100 -> smallint
            'opp_possessions': safe_int(opponent_row.get('POSS')),
            'opp_ts_pct': scale_percentage(safe_float(opponent_row.get('TS_PCT'))),  # x1000 -> smallint
            'opp_rim_fga': to_smallint(opp_shot_stats.get('rim_fga')),
            'opp_rim_fg_pct': scale_percentage(opp_shot_stats.get('rim_fg_pct')),  # x1000 -> smallint
            'opp_uast_rim_fga': to_smallint(opp_shot_stats.get('uast_rim_fga')),
            'opp_mr_fga': to_smallint(opp_shot_stats.get('mr_fga')),
            'opp_mr_fg_pct': scale_percentage(opp_shot_stats.get('mr_fg_pct')),  # x1000 -> smallint
            'opp_uast_mr_fga': to_smallint(opp_shot_stats.get('uast_mr_fga')),
            'opp_uast_3fga': to_smallint(opp_uast_3fga),
            'opp_cont_3pa': to_smallint(opp_matchup_stats.get('cont_3pa')),
            'opp_cont_3p_pct': scale_percentage(opp_matchup_stats.get('cont_3p_pct')),  # x1000 -> smallint
            'opp_open_3pa': to_smallint(opp_shot_stats.get('open_3pa')),
            'opp_open_3p_pct': scale_percentage(opp_shot_stats.get('open_3p_pct')),  # x1000 -> smallint
            'opp_pot_assists': None,
            'opp_avg_sec_touch': None,
            'opp_oreb_pct': scale_percentage(safe_float(opponent_row.get('OREB_PCT'))),  # x1000 -> smallint
            'opp_off_distance': None,
            'opp_charges_drawn': to_smallint(safe_int(opponent_row.get('chargesDrawn'))),
            'opp_deflections': to_smallint(safe_int(opponent_row.get('deflections'))),
            'opp_contests': to_smallint(safe_int(opponent_row.get('contestedShots'))),
            'opp_def_efg_pct': scale_percentage(opp_matchup_stats.get('def_efg_pct')),  # x1000 -> smallint
            'opp_dreb_pct': scale_percentage(safe_float(opponent_row.get('DREB_PCT'))),  # x1000 -> smallint
            'opp_def_distance': None,
        }
        
        records.append(record)
    
    log_info(f"Transformed {len(records)} team records")
    return records

# ============================================
# DATA LOADING
# ============================================

def calculate_age_decimal(birth_date_str):
    """Calculate age as decimal (e.g., 24.5 years old)"""
    if not birth_date_str:
        return None
    
    try:
        birth_date = datetime.strptime(birth_date_str, "%Y-%m-%dT%H:%M:%S")
        today = datetime.now()
        
        age_years = today.year - birth_date.year
        days_in_year = 366 if ((today.year % 4 == 0 and today.year % 100 != 0) or (today.year % 400 == 0)) else 365
        
        # Adjust if birthday hasn't occurred this year
        birth_this_year = birth_date.replace(year=today.year)
        if today < birth_this_year:
            age_years -= 1
            # Days from birthday last year to today
            birth_last_year = birth_date.replace(year=today.year - 1)
            days_diff = (today - birth_last_year).days
        else:
            # Days from birthday this year to today
            days_diff = (today - birth_this_year).days
        
        # Convert days to fraction of year
        age_decimal = age_years + (days_diff / days_in_year)
        return round(age_decimal, 1)
        
    except (ValueError, TypeError):
        return None

def ensure_players_exist(conn, player_ids: List[int]):
    """Ensure all players exist in the database, add minimal records if missing"""
    if not player_ids:
        return
    
    # Check which players are missing
    query = "SELECT player_id FROM players WHERE player_id = ANY(%s)"
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (player_ids,))
            existing_ids = set(row[0] for row in cur.fetchall())
        
        missing_ids = set(player_ids) - existing_ids
        
        if not missing_ids:
            return  # All players exist
        
        log_info(f"Found {len(missing_ids)} players not in database, fetching details...")
        
        # Fetch basic info for missing players
        from nba_api.stats.endpoints import commonplayerinfo
        
        players_to_insert = []
        for player_id in missing_ids:
            try:
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                info_df = player_info.get_data_frames()[0]
                
                if not info_df.empty:
                    row = info_df.iloc[0]
                    
                    # Parse height
                    height_inches = None
                    height_str = row.get('HEIGHT')
                    if height_str and '-' in str(height_str):
                        parts = str(height_str).split('-')
                        if len(parts) == 2:
                            feet = int(parts[0])
                            inches = int(parts[1])
                            height_inches = (feet * 12) + inches
                    
                    # Parse weight
                    weight_lbs = safe_int(row.get('WEIGHT'))
                    
                    # Get team_id
                    team_id = safe_int(row.get('TEAM_ID'))
                    if team_id == 0:
                        team_id = None
                    
                    # Calculate age from birthdate
                    birth_date = safe_str(row.get('BIRTHDATE'))
                    age_decimal = calculate_age_decimal(birth_date)
                    
                    players_to_insert.append((
                        player_id,
                        team_id,
                        safe_str(row.get('FIRST_NAME')),
                        safe_str(row.get('LAST_NAME')),
                        safe_str(row.get('DISPLAY_FIRST_LAST')),
                        height_inches,
                        weight_lbs,
                        age_decimal,
                        safe_int(row.get('SEASON_EXP')),
                        safe_str(row.get('JERSEY')),
                        safe_str(row.get('SCHOOL')),
                        safe_str(row.get('COUNTRY')),
                        safe_str(row.get('POSITION')),
                        player_id,
                        f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
                    ))
                    
                    log_info(f"  ✓ Fetched info for player {player_id}: {row.get('DISPLAY_FIRST_LAST')}")
                
                rate_limit()
                
            except Exception as e:
                log_error(f"  ✗ Failed to fetch player {player_id}: {e}")
                # Insert minimal record so we don't fail on foreign key
                players_to_insert.append((
                    player_id,
                    None, None, None, f"Unknown Player {player_id}",
                    None, None, None, None, None, None, None, None,
                    player_id,
                    f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
                ))
        
        if players_to_insert:
            insert_query = """
                INSERT INTO players (
                    player_id, team_id, first_name, last_name, full_name,
                    height_inches, weight_lbs, age_decimal, years_experience,
                    jersey_number, pre_nba_team, birthplace, position,
                    nba_api_id, headshot_url
                )
                VALUES %s
                ON CONFLICT (player_id) DO NOTHING
            """
            
            with conn.cursor() as cur:
                execute_values(cur, insert_query, players_to_insert)
            conn.commit()
            log_success(f"✓ Added {len(players_to_insert)} missing players to database")
    
    except Exception as e:
        log_error(f"Failed to ensure players exist: {e}")
        # Don't raise - let the foreign key constraint fail with better context

def upsert_player_game_stats(conn, records: List[Dict]):
    """Insert or update player game stats in PostgreSQL (only for players on NBA teams)"""
    if not records:
        return
    
    # Get valid NBA team IDs from database
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_id FROM teams")
            valid_team_ids = {row[0] for row in cur.fetchall()}
    except Exception as e:
        log_error(f"Failed to fetch valid team IDs: {e}")
        return
    
    # Filter to only players on NBA teams (skip players on non-NBA exhibition opponents)
    nba_records = [r for r in records if r.get('team_id') in valid_team_ids]
    
    if not nba_records:
        log_info("No NBA player stats to upsert (exhibition game)")
        return
    
    if len(nba_records) < len(records):
        log_info(f"Filtered to {len(nba_records)} NBA players (skipped {len(records) - len(nba_records)} non-NBA opponent players)")
    
    # Ensure all players exist in database first
    player_ids = [r['player_id'] for r in nba_records if r.get('player_id')]
    ensure_players_exist(conn, player_ids)
    
    log_info(f"Upserting {len(nba_records)} player game stats...")
    
    columns = [
        'game_id', 'player_id', 'team_id', 'opponent_team_id', 'is_home', 'game_date',
        'minutes', 'points',
        'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct', 'fta', 'ft_pct',
        'off_rebs', 'def_rebs',
        'assists', 'turnovers',
        'steals', 'blocks',
        'plus_minus', 'on_off', 'off_rtg', 'def_rtg', 'possessions',
        'ts_pct',
        'rim_fga', 'rim_fg_pct', 'uast_rim_fga',
        'mr_fga', 'mr_fg_pct', 'uast_mr_fga',
        'uast_3fga', 'cont_3pa', 'cont_3p_pct', 'open_3pa', 'open_3p_pct',
        'pot_assists', 'on_ball_pct', 'avg_sec_touch',
        'oreb_pct', 'dreb_pct',
        'off_distance', 'charges_drawn', 'deflections', 'contests', 'def_efg_pct', 'def_distance'
    ]
    
    # Prepare values (using nba_records instead of records)
    values = []
    for record in nba_records:
        values.append(tuple(record.get(col) for col in columns))
    
    # Build upsert query
    columns_str = ', '.join(columns)
    
    # Update clause (all columns except keys)
    update_cols = [col for col in columns if col not in ['game_id', 'player_id']]
    update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    
    query = f"""
        INSERT INTO player_game_stats ({columns_str})
        VALUES %s
        ON CONFLICT (game_id, player_id)
        DO UPDATE SET {update_str}, updated_at = CURRENT_TIMESTAMP
    """
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
        log_success(f"✓ Upserted {len(nba_records)} player game stats")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to upsert player game stats: {e}")
        raise

def upsert_team_game_stats(conn, records: List[Dict]):
    """Insert or update team game stats in PostgreSQL (only for NBA teams)"""
    if not records:
        return
    
    # Get valid NBA team IDs from database
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_id FROM teams")
            valid_team_ids = {row[0] for row in cur.fetchall()}
    except Exception as e:
        log_error(f"Failed to fetch valid team IDs: {e}")
        return
    
    # Filter to only NBA teams (skip non-NBA exhibition opponents)
    nba_records = [r for r in records if r.get('team_id') in valid_team_ids]
    
    if not nba_records:
        log_info("No NBA team stats to upsert (exhibition game)")
        return
    
    if len(nba_records) < len(records):
        log_info(f"Filtered to {len(nba_records)} NBA teams (skipped {len(records) - len(nba_records)} non-NBA opponents)")
    
    log_info(f"Upserting {len(nba_records)} team game stats...")
    
    columns = [
        'game_id', 'team_id', 'opponent_team_id', 'is_home',
        'minutes', 'points',
        'fg2a', 'fg2_pct', 'fg3a', 'fg3_pct', 'fta', 'ft_pct',
        'off_rebs', 'def_rebs',
        'assists', 'turnovers',
        'steals', 'blocks',
        'plus_minus', 'off_rtg', 'def_rtg', 'possessions',
        'ts_pct',
        'rim_fga', 'rim_fg_pct', 'uast_rim_fga',
        'mr_fga', 'mr_fg_pct', 'uast_mr_fga',
        'uast_3fga', 'cont_3pa', 'cont_3p_pct', 'open_3pa', 'open_3p_pct',
        'pot_assists', 'avg_sec_touch',
        'oreb_pct', 'off_distance',
        'charges_drawn', 'deflections', 'contests', 'def_efg_pct',
        'dreb_pct', 'def_distance',
        'opp_points', 'opp_fg2a', 'opp_fg2_pct', 'opp_fg3a', 'opp_fg3_pct',
        'opp_fta', 'opp_ft_pct', 'opp_off_rebs', 'opp_def_rebs',
        'opp_assists', 'opp_turnovers', 'opp_steals', 'opp_blocks',
        'opp_off_rtg', 'opp_def_rtg', 'opp_possessions', 'opp_ts_pct',
        'opp_rim_fga', 'opp_rim_fg_pct', 'opp_uast_rim_fga',
        'opp_mr_fga', 'opp_mr_fg_pct', 'opp_uast_mr_fga',
        'opp_uast_3fga', 'opp_cont_3pa', 'opp_cont_3p_pct', 'opp_open_3pa', 'opp_open_3p_pct',
        'opp_pot_assists', 'opp_avg_sec_touch',
        'opp_oreb_pct', 'opp_off_distance',
        'opp_charges_drawn', 'opp_deflections', 'opp_contests', 'opp_def_efg_pct',
        'opp_dreb_pct', 'opp_def_distance'
    ]
    
    # Prepare values (using nba_records instead of records)
    values = []
    for record in nba_records:
        values.append(tuple(record.get(col) for col in columns))
    
    # Build upsert query
    columns_str = ', '.join(columns)
    
    # Update clause (all columns except keys)
    update_cols = [col for col in columns if col not in ['game_id', 'team_id']]
    update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    
    query = f"""
        INSERT INTO team_game_stats ({columns_str})
        VALUES %s
        ON CONFLICT (game_id, team_id)
        DO UPDATE SET {update_str}, updated_at = CURRENT_TIMESTAMP
    """
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
        log_success(f"✓ Upserted {len(nba_records)} team game stats")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to upsert team game stats: {e}")
        raise

def upsert_game(conn, game_id: str, game_date: str, season_type: str, box_scores: Dict[str, pd.DataFrame]):
    """Insert or update game record"""
    
    trad_team = box_scores.get('traditional_team')
    if trad_team is None or trad_team.empty:
        log_error("No team stats available for game")
        return
    
    # Extract game info
    home_team = trad_team[trad_team['GAME_ID'] == game_id].iloc[0]
    away_team = trad_team[trad_team['GAME_ID'] == game_id].iloc[1]
    
    home_team_id = safe_int(home_team['TEAM_ID'])
    away_team_id = safe_int(away_team['TEAM_ID'])
    
    # Get valid NBA team IDs to check if teams exist
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_id FROM teams WHERE team_id IN (%s, %s)", (home_team_id, away_team_id))
            valid_team_ids = {row[0] for row in cur.fetchall()}
    except Exception as e:
        log_error(f"Failed to validate team IDs: {e}")
        return
    
    # If neither team is in our database, skip this game entirely
    if not valid_team_ids:
        log_info(f"Skipping game {game_id}: No NBA teams found")
        return
    
    # Set non-NBA team IDs to NULL (for preseason exhibition games)
    if home_team_id not in valid_team_ids:
        log_info(f"  Home team {home_team_id} not in database (exhibition opponent)")
        home_team_id = None
    
    if away_team_id not in valid_team_ids:
        log_info(f"  Away team {away_team_id} not in database (exhibition opponent)")
        away_team_id = None
    
    query = """
        INSERT INTO games (game_id, game_date, season, season_type, home_team_id, away_team_id, home_score, away_score, game_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET
            season_type = EXCLUDED.season_type,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            game_status = EXCLUDED.game_status
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (
                game_id,
                game_date,
                Config.CURRENT_SEASON,
                season_type,
                home_team_id,
                away_team_id,
                safe_int(home_team['PTS']),
                safe_int(away_team['PTS']),
                'Final'
            ))
        conn.commit()
        log_success(f"✓ Upserted game {game_id} ({season_type})")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to upsert game: {e}")

def populate_upcoming_games(conn, season: str = None):
    """Fetch and insert upcoming games for the season"""
    if season is None:
        season = Config.CURRENT_SEASON
    
    log_info(f"\nPopulating upcoming games for {season}...")
    
    all_upcoming = []
    
    # Fetch for each season type
    for season_type in Config.SEASON_TYPES:
        try:
            upcoming = fetch_upcoming_games(season, season_type)
            all_upcoming.extend(upcoming)
            rate_limit()
        except Exception as e:
            log_error(f"Failed to fetch upcoming games for {season_type}: {e}")
    
    if not all_upcoming:
        log_info("No upcoming games found")
        return
    
    log_info(f"Inserting {len(all_upcoming)} upcoming games...")
    
    # Get valid NBA team IDs from database
    valid_team_ids = set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_id FROM teams")
            valid_team_ids = {row[0] for row in cur.fetchall()}
    except Exception as e:
        log_error(f"Failed to fetch valid team IDs: {e}")
        return
    
    # Filter games: keep if at least ONE team is an NBA team (preseason vs non-NBA teams is OK)
    valid_games = []
    for g in all_upcoming:
        if g['home_team_id'] in valid_team_ids or g['away_team_id'] in valid_team_ids:
            valid_games.append(g)
        else:
            # Skip only if BOTH teams are non-NBA (shouldn't happen, but just in case)
            log_info(f"Skipping non-NBA game: {g['game_id']} ({g['home_team_id']} vs {g['away_team_id']})")
    
    if not valid_games:
        log_info("No valid upcoming games to insert")
        return
    
    # Insert games (only if they don't exist)
    query = """
        INSERT INTO games (game_id, game_date, season, season_type, home_team_id, away_team_id, home_score, away_score, game_status)
        VALUES %s
        ON CONFLICT (game_id) DO NOTHING
    """
    
    values = [
        (
            g['game_id'],
            g['game_date'],
            g['season'],
            g['season_type'],
            g['home_team_id'],
            g['away_team_id'],
            g['home_score'],
            g['away_score'],
            g['game_status']
        )
        for g in valid_games
    ]
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
        log_success(f"✓ Populated {len(valid_games)} upcoming games")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to populate upcoming games: {e}")
        raise

def update_game_statuses(conn):
    """Update game statuses from 'Scheduled' to 'Final' for completed games"""
    log_info("\nUpdating game statuses...")
    
    # Query to find scheduled games in the past that should be marked as Final
    query = """
        UPDATE games
        SET game_status = 'Final'
        WHERE game_status = 'Scheduled'
        AND game_date < CURRENT_DATE
        AND home_score IS NOT NULL
        AND away_score IS NOT NULL
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows_updated = cur.rowcount
        conn.commit()
        if rows_updated > 0:
            log_success(f"✓ Updated {rows_updated} game statuses to Final")
        else:
            log_info("No game statuses to update")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to update game statuses: {e}")
        raise

def calculate_player_season_stats(conn, season: str = None):
    """Calculate and upsert player season stats (aggregated from player_game_stats)"""
    if season is None:
        season = Config.CURRENT_SEASON
    
    log_info(f"Calculating player season stats for {season}...")
    
    # Query to aggregate player stats by season and season_type
    # NOTE: Season stats now store TOTALS (not averages) with scaled values
    # - Minutes: already scaled x10 in player_game_stats, SUM for total
    # - Counts (points, rebounds, etc.): SUM for totals
    # - Percentages: already scaled x1000 in player_game_stats, AVG for season avg
    # - Ratings: already scaled x100 in player_game_stats, AVG for season avg
    query = """
        WITH game_data AS (
            SELECT 
                pgs.player_id,
                pgs.team_id,
                g.season,
                g.season_type,
                -- Only count games where player actually played (minutes > 0)
                COUNT(DISTINCT CASE WHEN pgs.minutes > 0 THEN pgs.game_id END) as games_played,
                -- Assume games_started if minutes >= 240 (24.0 minutes * 10)
                COUNT(DISTINCT CASE WHEN pgs.minutes >= 240 THEN pgs.game_id END) as games_started,
                -- TOTALS (already scaled in player_game_stats)
                SUM(pgs.minutes) as minutes,  -- Total minutes (x10)
                SUM(pgs.points) as points,  -- Total points
                SUM(pgs.fg2a) as fg2a,  -- Total 2PA
                SUM(pgs.fg3a) as fg3a,  -- Total 3PA
                SUM(pgs.fta) as fta,  -- Total FTA
                SUM(pgs.off_rebs) as off_rebs,  -- Total offensive rebounds
                SUM(pgs.def_rebs) as def_rebs,  -- Total defensive rebounds
                SUM(pgs.assists) as assists,  -- Total assists
                SUM(pgs.turnovers) as turnovers,  -- Total turnovers
                SUM(pgs.steals) as steals,  -- Total steals
                SUM(pgs.blocks) as blocks,  -- Total blocks
                SUM(pgs.possessions) as possessions,  -- Total possessions
                SUM(pgs.charges_drawn) as charges_drawn,  -- Total charges drawn
                SUM(pgs.deflections) as deflections,  -- Total deflections
                SUM(pgs.contests) as contests,  -- Total contests
                -- AVERAGES (percentages and ratings, already scaled)
                ROUND(AVG(pgs.fg2_pct))::smallint as fg2_pct,  -- x1000
                ROUND(AVG(pgs.fg3_pct))::smallint as fg3_pct,  -- x1000
                ROUND(AVG(pgs.ft_pct))::smallint as ft_pct,  -- x1000
                ROUND(AVG(pgs.ts_pct))::smallint as ts_pct,  -- x1000
                ROUND(AVG(pgs.rim_fg_pct))::smallint as rim_fg_pct,  -- x1000
                ROUND(AVG(pgs.mr_fg_pct))::smallint as mr_fg_pct,  -- x1000
                ROUND(AVG(pgs.cont_3p_pct))::smallint as cont_3p_pct,  -- x1000
                ROUND(AVG(pgs.open_3p_pct))::smallint as open_3p_pct,  -- x1000
                ROUND(AVG(pgs.oreb_pct))::smallint as oreb_pct,  -- x1000
                ROUND(AVG(pgs.def_efg_pct))::smallint as def_efg_pct,  -- x1000
                ROUND(AVG(pgs.dreb_pct))::smallint as dreb_pct,  -- x1000
                ROUND(AVG(pgs.on_ball_pct))::smallint as on_ball_pct,  -- x1000
                ROUND(AVG(pgs.on_off))::smallint as on_off,  -- x100
                ROUND(AVG(pgs.off_rtg))::smallint as off_rtg,  -- x100
                ROUND(AVG(pgs.def_rtg))::smallint as def_rtg,  -- x100
                -- PLUS/MINUS as total (x100 for storage)
                (ROUND(SUM(pgs.plus_minus) * 100))::smallint as plus_minus,  -- x100
                -- Nulls (cast to proper types)
                NULL::smallint as rim_fga,
                NULL::smallint as uast_rim_fga,
                NULL::smallint as mr_fga,
                NULL::smallint as uast_mr_fga,
                NULL::smallint as uast_3fga,
                NULL::smallint as cont_3pa,
                NULL::smallint as open_3pa,
                NULL::numeric as pot_assists,
                NULL::numeric as avg_sec_touch,
                NULL::numeric as off_distance,
                NULL::numeric as def_distance
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            WHERE g.season = %s AND g.game_status = 'Final'
            GROUP BY pgs.player_id, pgs.team_id, g.season, g.season_type
        )
        INSERT INTO player_season_stats (
            player_id, team_id, season, season_type,
            games_played, games_started,
            minutes, points, fg2a, fg2_pct, fg3a, fg3_pct, fta, ft_pct,
            off_rebs, def_rebs, assists, turnovers, steals, blocks,
            plus_minus, on_off, off_rtg, def_rtg, possessions, ts_pct,
            rim_fga, rim_fg_pct, uast_rim_fga,
            mr_fga, mr_fg_pct, uast_mr_fga,
            uast_3fga, cont_3pa, cont_3p_pct, open_3pa, open_3p_pct,
            pot_assists, on_ball_pct, avg_sec_touch,
            oreb_pct, off_distance,
            charges_drawn, deflections, contests, def_efg_pct,
            dreb_pct, def_distance,
            last_calculated
        )
        SELECT 
            player_id, team_id, season, season_type,
            games_played, games_started,
            minutes, points, fg2a, fg2_pct, fg3a, fg3_pct, fta, ft_pct,
            off_rebs, def_rebs, assists, turnovers, steals, blocks,
            plus_minus, on_off, off_rtg, def_rtg, possessions, ts_pct,
            rim_fga, rim_fg_pct, uast_rim_fga,
            mr_fga, mr_fg_pct, uast_mr_fga,
            uast_3fga, cont_3pa, cont_3p_pct, open_3pa, open_3p_pct,
            pot_assists, on_ball_pct, avg_sec_touch,
            oreb_pct, off_distance,
            charges_drawn, deflections, contests, def_efg_pct,
            dreb_pct, def_distance,
            CURRENT_TIMESTAMP
        FROM game_data
        ON CONFLICT (player_id, season, season_type, team_id)
        DO UPDATE SET
            games_played = EXCLUDED.games_played,
            games_started = EXCLUDED.games_started,
            minutes = EXCLUDED.minutes,
            points = EXCLUDED.points,
            fg2a = EXCLUDED.fg2a,
            fg2_pct = EXCLUDED.fg2_pct,
            fg3a = EXCLUDED.fg3a,
            fg3_pct = EXCLUDED.fg3_pct,
            fta = EXCLUDED.fta,
            ft_pct = EXCLUDED.ft_pct,
            off_rebs = EXCLUDED.off_rebs,
            def_rebs = EXCLUDED.def_rebs,
            assists = EXCLUDED.assists,
            turnovers = EXCLUDED.turnovers,
            steals = EXCLUDED.steals,
            blocks = EXCLUDED.blocks,
            plus_minus = EXCLUDED.plus_minus,
            on_off = EXCLUDED.on_off,
            off_rtg = EXCLUDED.off_rtg,
            def_rtg = EXCLUDED.def_rtg,
            possessions = EXCLUDED.possessions,
            ts_pct = EXCLUDED.ts_pct,
            rim_fga = EXCLUDED.rim_fga,
            rim_fg_pct = EXCLUDED.rim_fg_pct,
            uast_rim_fga = EXCLUDED.uast_rim_fga,
            mr_fga = EXCLUDED.mr_fga,
            mr_fg_pct = EXCLUDED.mr_fg_pct,
            uast_mr_fga = EXCLUDED.uast_mr_fga,
            uast_3fga = EXCLUDED.uast_3fga,
            cont_3pa = EXCLUDED.cont_3pa,
            cont_3p_pct = EXCLUDED.cont_3p_pct,
            open_3pa = EXCLUDED.open_3pa,
            open_3p_pct = EXCLUDED.open_3p_pct,
            pot_assists = EXCLUDED.pot_assists,
            on_ball_pct = EXCLUDED.on_ball_pct,
            avg_sec_touch = EXCLUDED.avg_sec_touch,
            oreb_pct = EXCLUDED.oreb_pct,
            off_distance = EXCLUDED.off_distance,
            charges_drawn = EXCLUDED.charges_drawn,
            deflections = EXCLUDED.deflections,
            contests = EXCLUDED.contests,
            def_efg_pct = EXCLUDED.def_efg_pct,
            dreb_pct = EXCLUDED.dreb_pct,
            def_distance = EXCLUDED.def_distance,
            last_calculated = CURRENT_TIMESTAMP
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (season,))
            rows_affected = cur.rowcount
        conn.commit()
        log_success(f"✓ Calculated player season stats: {rows_affected} records")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to calculate player season stats: {e}")
        raise

def calculate_team_season_stats(conn, season: str = None):
    """Calculate and upsert team season stats (aggregated from team_game_stats)"""
    if season is None:
        season = Config.CURRENT_SEASON
    
    log_info(f"Calculating team season stats for {season}...")
    
    # Query to aggregate team stats by season and season_type
    # NOTE: Season stats now store TOTALS (not averages) with scaled values
    # - Minutes: already scaled x10 in team_game_stats, SUM for total
    # - Counts (points, rebounds, etc.): SUM for totals
    # - Percentages: already scaled x1000 in team_game_stats, AVG for season avg
    # - Ratings: already scaled x100 in team_game_stats, AVG for season avg
    query = """
        WITH game_data AS (
            SELECT 
                tgs.team_id,
                g.season,
                g.season_type,
                COUNT(DISTINCT tgs.game_id) as games_played,
                -- Count wins (when team scored more than opponent)
                COUNT(DISTINCT CASE WHEN tgs.points > tgs.opp_points THEN tgs.game_id END) as wins,
                COUNT(DISTINCT CASE WHEN tgs.points < tgs.opp_points THEN tgs.game_id END) as losses,
                -- TOTALS (already scaled in team_game_stats)
                SUM(tgs.minutes) as minutes,  -- Total minutes (x10)
                SUM(tgs.points) as points,  -- Total points
                SUM(tgs.fg2a) as fg2a,  -- Total 2PA
                SUM(tgs.fg3a) as fg3a,  -- Total 3PA
                SUM(tgs.fta) as fta,  -- Total FTA
                SUM(tgs.off_rebs) as off_rebs,  -- Total offensive rebounds
                SUM(tgs.def_rebs) as def_rebs,  -- Total defensive rebounds
                SUM(tgs.assists) as assists,  -- Total assists
                SUM(tgs.turnovers) as turnovers,  -- Total turnovers
                SUM(tgs.steals) as steals,  -- Total steals
                SUM(tgs.blocks) as blocks,  -- Total blocks
                SUM(tgs.possessions) as possessions,  -- Total possessions
                -- AVERAGES (percentages and ratings, already scaled)
                ROUND(AVG(tgs.fg2_pct))::smallint as fg2_pct,  -- x1000
                ROUND(AVG(tgs.fg3_pct))::smallint as fg3_pct,  -- x1000
                ROUND(AVG(tgs.ft_pct))::smallint as ft_pct,  -- x1000
                ROUND(AVG(tgs.ts_pct))::smallint as ts_pct,  -- x1000
                ROUND(AVG(tgs.rim_fg_pct))::smallint as rim_fg_pct,  -- x1000
                ROUND(AVG(tgs.mr_fg_pct))::smallint as mr_fg_pct,  -- x1000
                ROUND(AVG(tgs.cont_3p_pct))::smallint as cont_3p_pct,  -- x1000
                ROUND(AVG(tgs.open_3p_pct))::smallint as open_3p_pct,  -- x1000
                ROUND(AVG(tgs.oreb_pct))::smallint as oreb_pct,  -- x1000
                ROUND(AVG(tgs.def_efg_pct))::smallint as def_efg_pct,  -- x1000
                ROUND(AVG(tgs.dreb_pct))::smallint as dreb_pct,  -- x1000
                ROUND(AVG(tgs.off_rtg))::smallint as off_rtg,  -- x100
                ROUND(AVG(tgs.def_rtg))::smallint as def_rtg,  -- x100
                -- PLUS/MINUS as total
                SUM(tgs.plus_minus) as plus_minus,  -- Total plus/minus
                -- Nulls for unused fields (cast to proper types)
                NULL::smallint as rim_fga,
                NULL::smallint as uast_rim_fga,
                NULL::smallint as mr_fga,
                NULL::smallint as uast_mr_fga,
                NULL::smallint as uast_3fga,
                NULL::smallint as cont_3pa,
                NULL::smallint as open_3pa,
                NULL::numeric as pot_assists,
                NULL::numeric as avg_sec_touch,
                NULL::numeric as off_distance,
                NULL::smallint as charges_drawn,
                NULL::smallint as deflections,
                NULL::smallint as contests,
                NULL::numeric as def_distance,
                -- OPPONENT TOTALS
                SUM(tgs.opp_points) as opp_points,
                SUM(tgs.opp_fg2a) as opp_fg2a,
                SUM(tgs.opp_fg3a) as opp_fg3a,
                SUM(tgs.opp_fta) as opp_fta,
                SUM(tgs.opp_off_rebs) as opp_off_rebs,
                SUM(tgs.opp_def_rebs) as opp_def_rebs,
                SUM(tgs.opp_assists) as opp_assists,
                SUM(tgs.opp_turnovers) as opp_turnovers,
                SUM(tgs.opp_steals) as opp_steals,
                SUM(tgs.opp_blocks) as opp_blocks,
                SUM(tgs.opp_possessions) as opp_possessions,
                -- OPPONENT AVERAGES (percentages and ratings, already scaled)
                ROUND(AVG(tgs.opp_fg2_pct))::smallint as opp_fg2_pct,  -- x1000
                ROUND(AVG(tgs.opp_fg3_pct))::smallint as opp_fg3_pct,  -- x1000
                ROUND(AVG(tgs.opp_ft_pct))::smallint as opp_ft_pct,  -- x1000
                ROUND(AVG(tgs.opp_ts_pct))::smallint as opp_ts_pct,  -- x1000
                ROUND(AVG(tgs.opp_rim_fg_pct))::smallint as opp_rim_fg_pct,  -- x1000
                ROUND(AVG(tgs.opp_mr_fg_pct))::smallint as opp_mr_fg_pct,  -- x1000
                ROUND(AVG(tgs.opp_cont_3p_pct))::smallint as opp_cont_3p_pct,  -- x1000
                ROUND(AVG(tgs.opp_open_3p_pct))::smallint as opp_open_3p_pct,  -- x1000
                ROUND(AVG(tgs.opp_oreb_pct))::smallint as opp_oreb_pct,  -- x1000
                ROUND(AVG(tgs.opp_def_efg_pct))::smallint as opp_def_efg_pct,  -- x1000
                ROUND(AVG(tgs.opp_dreb_pct))::smallint as opp_dreb_pct,  -- x1000
                ROUND(AVG(tgs.opp_off_rtg))::smallint as opp_off_rtg,  -- x100
                ROUND(AVG(tgs.opp_def_rtg))::smallint as opp_def_rtg,  -- x100
                -- OPPONENT Nulls for unused fields (cast to proper types)
                NULL::smallint as opp_rim_fga,
                NULL::smallint as opp_uast_rim_fga,
                NULL::smallint as opp_mr_fga,
                NULL::smallint as opp_uast_mr_fga,
                NULL::smallint as opp_uast_3fga,
                NULL::smallint as opp_cont_3pa,
                NULL::smallint as opp_open_3pa,
                NULL::numeric as opp_pot_assists,
                NULL::numeric as opp_avg_sec_touch,
                NULL::numeric as opp_off_distance,
                NULL::smallint as opp_charges_drawn,
                NULL::smallint as opp_deflections,
                NULL::smallint as opp_contests,
                NULL::numeric as opp_def_distance
            FROM team_game_stats tgs
            JOIN games g ON tgs.game_id = g.game_id
            WHERE g.season = %s AND g.game_status = 'Final'
            GROUP BY tgs.team_id, g.season, g.season_type
        )
        INSERT INTO team_season_stats (
            team_id, season, season_type,
            games_played, wins, losses,
            minutes, points, fg2a, fg2_pct, fg3a, fg3_pct, fta, ft_pct,
            off_rebs, def_rebs, assists, turnovers, steals, blocks,
            plus_minus, off_rtg, def_rtg, possessions, ts_pct,
            rim_fga, rim_fg_pct, uast_rim_fga,
            mr_fga, mr_fg_pct, uast_mr_fga,
            uast_3fga, cont_3pa, cont_3p_pct, open_3pa, open_3p_pct,
            pot_assists, avg_sec_touch,
            oreb_pct, off_distance,
            charges_drawn, deflections, contests, def_efg_pct,
            dreb_pct, def_distance,
            opp_points, opp_fg2a, opp_fg2_pct, opp_fg3a, opp_fg3_pct,
            opp_fta, opp_ft_pct, opp_off_rebs, opp_def_rebs,
            opp_assists, opp_turnovers, opp_steals, opp_blocks,
            opp_off_rtg, opp_def_rtg, opp_possessions, opp_ts_pct,
            opp_rim_fga, opp_rim_fg_pct, opp_uast_rim_fga,
            opp_mr_fga, opp_mr_fg_pct, opp_uast_mr_fga,
            opp_uast_3fga, opp_cont_3pa, opp_cont_3p_pct, opp_open_3pa, opp_open_3p_pct,
            opp_pot_assists, opp_avg_sec_touch,
            opp_oreb_pct, opp_off_distance,
            opp_charges_drawn, opp_deflections, opp_contests, opp_def_efg_pct,
            opp_dreb_pct, opp_def_distance,
            last_calculated
        )
        SELECT 
            team_id, season, season_type,
            games_played, wins, losses,
            minutes, points, fg2a, fg2_pct, fg3a, fg3_pct, fta, ft_pct,
            off_rebs, def_rebs, assists, turnovers, steals, blocks,
            plus_minus, off_rtg, def_rtg, possessions, ts_pct,
            rim_fga, rim_fg_pct, uast_rim_fga,
            mr_fga, mr_fg_pct, uast_mr_fga,
            uast_3fga, cont_3pa, cont_3p_pct, open_3pa, open_3p_pct,
            pot_assists, avg_sec_touch,
            oreb_pct, off_distance,
            charges_drawn, deflections, contests, def_efg_pct,
            dreb_pct, def_distance,
            opp_points, opp_fg2a, opp_fg2_pct, opp_fg3a, opp_fg3_pct,
            opp_fta, opp_ft_pct, opp_off_rebs, opp_def_rebs,
            opp_assists, opp_turnovers, opp_steals, opp_blocks,
            opp_off_rtg, opp_def_rtg, opp_possessions, opp_ts_pct,
            opp_rim_fga, opp_rim_fg_pct, opp_uast_rim_fga,
            opp_mr_fga, opp_mr_fg_pct, opp_uast_mr_fga,
            opp_uast_3fga, opp_cont_3pa, opp_cont_3p_pct, opp_open_3pa, opp_open_3p_pct,
            opp_pot_assists, opp_avg_sec_touch,
            opp_oreb_pct, opp_off_distance,
            opp_charges_drawn, opp_deflections, opp_contests, opp_def_efg_pct,
            opp_dreb_pct, opp_def_distance,
            CURRENT_TIMESTAMP
        FROM game_data
        ON CONFLICT (team_id, season, season_type)
        DO UPDATE SET
            games_played = EXCLUDED.games_played,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            minutes = EXCLUDED.minutes,
            points = EXCLUDED.points,
            fg2a = EXCLUDED.fg2a,
            fg2_pct = EXCLUDED.fg2_pct,
            fg3a = EXCLUDED.fg3a,
            fg3_pct = EXCLUDED.fg3_pct,
            fta = EXCLUDED.fta,
            ft_pct = EXCLUDED.ft_pct,
            off_rebs = EXCLUDED.off_rebs,
            def_rebs = EXCLUDED.def_rebs,
            assists = EXCLUDED.assists,
            turnovers = EXCLUDED.turnovers,
            steals = EXCLUDED.steals,
            blocks = EXCLUDED.blocks,
            plus_minus = EXCLUDED.plus_minus,
            off_rtg = EXCLUDED.off_rtg,
            def_rtg = EXCLUDED.def_rtg,
            possessions = EXCLUDED.possessions,
            ts_pct = EXCLUDED.ts_pct,
            rim_fga = EXCLUDED.rim_fga,
            rim_fg_pct = EXCLUDED.rim_fg_pct,
            uast_rim_fga = EXCLUDED.uast_rim_fga,
            mr_fga = EXCLUDED.mr_fga,
            mr_fg_pct = EXCLUDED.mr_fg_pct,
            uast_mr_fga = EXCLUDED.uast_mr_fga,
            uast_3fga = EXCLUDED.uast_3fga,
            cont_3pa = EXCLUDED.cont_3pa,
            cont_3p_pct = EXCLUDED.cont_3p_pct,
            open_3pa = EXCLUDED.open_3pa,
            open_3p_pct = EXCLUDED.open_3p_pct,
            pot_assists = EXCLUDED.pot_assists,
            avg_sec_touch = EXCLUDED.avg_sec_touch,
            oreb_pct = EXCLUDED.oreb_pct,
            off_distance = EXCLUDED.off_distance,
            charges_drawn = EXCLUDED.charges_drawn,
            deflections = EXCLUDED.deflections,
            contests = EXCLUDED.contests,
            def_efg_pct = EXCLUDED.def_efg_pct,
            dreb_pct = EXCLUDED.dreb_pct,
            def_distance = EXCLUDED.def_distance,
            opp_points = EXCLUDED.opp_points,
            opp_fg2a = EXCLUDED.opp_fg2a,
            opp_fg2_pct = EXCLUDED.opp_fg2_pct,
            opp_fg3a = EXCLUDED.opp_fg3a,
            opp_fg3_pct = EXCLUDED.opp_fg3_pct,
            opp_fta = EXCLUDED.opp_fta,
            opp_ft_pct = EXCLUDED.opp_ft_pct,
            opp_off_rebs = EXCLUDED.opp_off_rebs,
            opp_def_rebs = EXCLUDED.opp_def_rebs,
            opp_assists = EXCLUDED.opp_assists,
            opp_turnovers = EXCLUDED.opp_turnovers,
            opp_steals = EXCLUDED.opp_steals,
            opp_blocks = EXCLUDED.opp_blocks,
            opp_off_rtg = EXCLUDED.opp_off_rtg,
            opp_def_rtg = EXCLUDED.opp_def_rtg,
            opp_possessions = EXCLUDED.opp_possessions,
            opp_ts_pct = EXCLUDED.opp_ts_pct,
            opp_rim_fga = EXCLUDED.opp_rim_fga,
            opp_rim_fg_pct = EXCLUDED.opp_rim_fg_pct,
            opp_uast_rim_fga = EXCLUDED.opp_uast_rim_fga,
            opp_mr_fga = EXCLUDED.opp_mr_fga,
            opp_mr_fg_pct = EXCLUDED.opp_mr_fg_pct,
            opp_uast_mr_fga = EXCLUDED.opp_uast_mr_fga,
            opp_uast_3fga = EXCLUDED.opp_uast_3fga,
            opp_cont_3pa = EXCLUDED.opp_cont_3pa,
            opp_cont_3p_pct = EXCLUDED.opp_cont_3p_pct,
            opp_open_3pa = EXCLUDED.opp_open_3pa,
            opp_open_3p_pct = EXCLUDED.opp_open_3p_pct,
            opp_pot_assists = EXCLUDED.opp_pot_assists,
            opp_avg_sec_touch = EXCLUDED.opp_avg_sec_touch,
            opp_oreb_pct = EXCLUDED.opp_oreb_pct,
            opp_off_distance = EXCLUDED.opp_off_distance,
            opp_charges_drawn = EXCLUDED.opp_charges_drawn,
            opp_deflections = EXCLUDED.opp_deflections,
            opp_contests = EXCLUDED.opp_contests,
            opp_def_efg_pct = EXCLUDED.opp_def_efg_pct,
            opp_dreb_pct = EXCLUDED.opp_dreb_pct,
            opp_def_distance = EXCLUDED.opp_def_distance,
            last_calculated = CURRENT_TIMESTAMP
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, (season,))
            rows_affected = cur.rowcount
        conn.commit()
        log_success(f"✓ Calculated team season stats: {rows_affected} records")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to calculate team season stats: {e}")
        raise

# ============================================
# PIPELINE ORCHESTRATION
# ============================================

def process_game(conn, game_id: str, game_date: str, season_type: str):
    """Process a single game"""
    log_info(f"\n{'='*60}")
    log_info(f"Processing game: {game_id} ({season_type})")
    log_info(f"{'='*60}")
    
    try:
        # Extract box scores
        box_scores = fetch_box_scores(game_id)
        
        # Get player IDs for shot chart extraction
        player_ids = []
        if not box_scores['traditional_player'].empty:
            player_ids = box_scores['traditional_player']['PLAYER_ID'].tolist()
        
        # Extract shot chart data (if players exist)
        # Note: Shot charts may not be available for all game types (especially Summer League)
        shot_charts = {}
        if player_ids:
            shot_charts = fetch_shot_chart_data(game_id, player_ids, season_type)
        
        # Extract matchup data
        matchup_data = fetch_matchup_data(game_id)
        
        # Transform
        player_records = transform_player_game_stats(
            game_id, 
            game_date, 
            box_scores,
            shot_charts=shot_charts,
            matchup_data=matchup_data
        )
        
        team_records = transform_team_game_stats(
            game_id,
            game_date,
            box_scores,
            shot_charts=shot_charts,
            matchup_data=matchup_data
        )
        
        # Load
        upsert_game(conn, game_id, game_date, season_type, box_scores)
        upsert_player_game_stats(conn, player_records)
        upsert_team_game_stats(conn, team_records)
        
        log_success(f"✓ Successfully processed game {game_id}")
        
    except Exception as e:
        log_error(f"Failed to process game {game_id}: {e}")
        import traceback
        traceback.print_exc()

def run_etl_for_date(date_str: str, check_upcoming: bool = True):
    """Run ETL for a specific date"""
    log_info(f"\n{'='*60}")
    log_info(f"ETL Pipeline - {date_str}")
    log_info(f"{'='*60}")
    
    conn = get_db_connection()
    
    try:
        # Check for upcoming games (only on first run of day or when requested)
        if check_upcoming:
            try:
                populate_upcoming_games(conn, Config.CURRENT_SEASON)
                update_game_statuses(conn)
            except Exception as e:
                log_error(f"Failed to update upcoming games: {e}")
                # Don't fail the entire ETL
        
        # Fetch games for date (returns list of (game_id, season_type) tuples)
        games = fetch_games_for_date(date_str)
        
        if not games:
            log_info("No games to process for this date")
            return
        
        # Process each game
        games_processed = 0
        games_failed = 0
        
        for game_id, season_type in games:
            try:
                process_game(conn, game_id, date_str, season_type)
                games_processed += 1
            except Exception as e:
                games_failed += 1
                log_error(f"Failed to process game {game_id}: {e}")
        
        # Report results
        if games_failed > 0:
            log_error(f"⚠ ETL complete for {date_str}: {games_processed} succeeded, {games_failed} failed")
        else:
            log_success(f"✓ ETL complete for {date_str}: {games_processed} games processed successfully")
        
        # Update season stats after processing all games
        if games_processed > 0:
            log_info("\nUpdating season statistics...")
            try:
                calculate_player_season_stats(conn, Config.CURRENT_SEASON)
                calculate_team_season_stats(conn, Config.CURRENT_SEASON)
                log_success("✓ Season statistics updated")
            except Exception as e:
                log_error(f"Failed to update season statistics: {e}")
                # Don't fail the entire ETL if season stats fail
        
    except Exception as e:
        log_error(f"ETL failed for {date_str}: {e}")
        raise  # Re-raise to trigger exit code
    finally:
        conn.close()

def run_etl_pipeline():
    """Main ETL pipeline entry point"""
    log_info("\n" + "="*60)
    log_info("THE GLASS - ETL PIPELINE")
    log_info("="*60)
    
    # Note: Teams and Players tables must be populated before running this pipeline
    # Run populate_teams.py once, then populate_players.py monthly
    
    # Determine date range
    if Config.START_DATE and Config.END_DATE:
        # Backfill mode
        start = datetime.strptime(Config.START_DATE, "%Y-%m-%d")
        end = datetime.strptime(Config.END_DATE, "%Y-%m-%d")
        dates = [(start + timedelta(days=x)).strftime("%Y-%m-%d") 
                 for x in range((end - start).days + 1)]
        log_info(f"Backfill mode: {len(dates)} dates")
    else:
        # Yesterday only (games finish late, so we process previous day)
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        dates = [yesterday]
        log_info(f"Daily mode: {yesterday} (yesterday's games)")
    
    # Process each date
    total_success = 0
    total_failed = 0
    
    for date_str in dates:
        try:
            run_etl_for_date(date_str)
            total_success += 1
        except Exception as e:
            total_failed += 1
            log_error(f"Failed to process date {date_str}: {e}")
    
    # Final report
    log_info("\n" + "="*60)
    if total_failed > 0:
        log_error(f"ETL PIPELINE COMPLETE: {total_success} dates succeeded, {total_failed} failed")
        sys.exit(1)  # Exit with error code
    else:
        log_success(f"ETL PIPELINE COMPLETE: {total_success} dates processed successfully")
        log_success("="*60)

# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    # Check for required environment variables
    if not Config.DB_PASSWORD:
        print("ERROR: DB_PASSWORD environment variable must be set")
        print("Usage: DB_PASSWORD='your_password' python etl_pipeline.py")
        sys.exit(1)
    
    run_etl_pipeline()
