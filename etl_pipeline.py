"""
THE GLASS - ETL Pipeline
Extracts NBA data and loads it into PostgreSQL
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
    """Aggregate shot chart data into location-based stats"""
    
    stats = {
        'rim_fga': None,
        'rim_fg_pct': None,
        'uast_rim_fga': None,
        'mr_fga': None,
        'mr_fg_pct': None,
        'uast_mr_fga': None,
        'open_3pa': None,
        'open_3p_pct': None,
    }
    
    if shot_df.empty:
        return stats
    
    # Rim shots (Restricted Area)
    rim_shots = shot_df[shot_df['SHOT_ZONE_BASIC'] == 'Restricted Area']
    if not rim_shots.empty:
        stats['rim_fga'] = len(rim_shots)
        rim_made = rim_shots[rim_shots['SHOT_MADE_FLAG'] == 1]
        stats['rim_fg_pct'] = len(rim_made) / len(rim_shots) if len(rim_shots) > 0 else None
        
        # Unassisted rim shots (ACTION_TYPE contains specific keywords)
        unassisted_keywords = ['Driving', 'Running', 'Layup', 'Dunk']
        unassisted_rim = rim_shots[
            rim_shots['ACTION_TYPE'].str.contains('|'.join(unassisted_keywords), case=False, na=False)
        ]
        stats['uast_rim_fga'] = len(unassisted_rim)
    
    # Mid-range shots (8-16 ft and 16-24 ft)
    mr_shots = shot_df[
        shot_df['SHOT_ZONE_RANGE'].isin(['8-16 ft.', '16-24 ft.'])
    ]
    if not mr_shots.empty:
        stats['mr_fga'] = len(mr_shots)
        mr_made = mr_shots[mr_shots['SHOT_MADE_FLAG'] == 1]
        stats['mr_fg_pct'] = len(mr_made) / len(mr_shots) if len(mr_shots) > 0 else None
        
        # Unassisted mid-range (pull-ups, step-backs, etc.)
        unassisted_mr_keywords = ['Pullup', 'Pull-Up', 'Step Back', 'Fadeaway', 'Turnaround']
        unassisted_mr = mr_shots[
            mr_shots['ACTION_TYPE'].str.contains('|'.join(unassisted_mr_keywords), case=False, na=False)
        ]
        stats['uast_mr_fga'] = len(unassisted_mr)
    
    # Open 3-point shots (closest defender > 4 feet, approximated by shot type)
    three_shots = shot_df[shot_df['SHOT_TYPE'] == '3PT Field Goal']
    if not three_shots.empty:
        # Use ACTION_TYPE as proxy: "Open" or wide open attempts
        open_keywords = ['Open']
        open_threes = three_shots[
            three_shots['ACTION_TYPE'].str.contains('|'.join(open_keywords), case=False, na=False)
        ]
        if not open_threes.empty:
            stats['open_3pa'] = len(open_threes)
            open_made = open_threes[open_threes['SHOT_MADE_FLAG'] == 1]
            stats['open_3p_pct'] = len(open_made) / len(open_threes) if len(open_threes) > 0 else None
    
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
        
        for matchup in player_matchups:
            # Contested 3-pointers
            cont_3pa = matchup.get('contestedShots3pt', 0)
            cont_3pm = matchup.get('contestedShots3ptMade', 0)
            total_cont_3pa += cont_3pa if cont_3pa else 0
            total_cont_3pm += cont_3pm if cont_3pm else 0
            
            # Defensive eFG%
            def_fga = matchup.get('defensiveFga', 0)
            def_fgm = matchup.get('defensiveFgm', 0)
            def_3pm = matchup.get('defensive3pm', 0)
            total_def_fga += def_fga if def_fga else 0
            total_def_fgm += def_fgm if def_fgm else 0
        
        # Calculate stats
        if total_cont_3pa > 0:
            stats['cont_3pa'] = total_cont_3pa
            stats['cont_3p_pct'] = total_cont_3pm / total_cont_3pa
        
        if total_def_fga > 0:
            # eFG% = (FGM + 0.5 * 3PM) / FGA
            # For defense, we use opponent's makes
            stats['def_efg_pct'] = total_def_fgm / total_def_fga
    
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
        
        record = {
            'game_id': game_id,
            'player_id': player_id,
            'team_id': safe_int(row.get('TEAM_ID')),
            'game_date': game_date,
            
            # Basic
            'minutes': parse_minutes(row.get('MIN')),
            'points': safe_int(row.get('PTS')),
            
            # Shooting
            'fg2a': fg2a,
            'fg2_pct': fg2_pct,
            'fg3a': fg3a,
            'fg3_pct': safe_float(row.get('FG3_PCT')),
            'fta': safe_int(row.get('FTA')),
            'ft_pct': safe_float(row.get('FT_PCT')),
            
            # Rebounding
            'off_rebs': safe_int(row.get('OREB')),
            'def_rebs': safe_int(row.get('DREB')),
            
            # Playmaking
            'assists': safe_int(row.get('AST')),
            'turnovers': safe_int(row.get('TO')),
            
            # Defense
            'steals': safe_int(row.get('STL')),
            'blocks': safe_int(row.get('BLK')),
            
            # Impact
            'plus_minus': safe_int(row.get('PLUS_MINUS')),
            'on_off': safe_float(row.get('NET_RATING')),  # Approximation
            'off_rtg': safe_float(row.get('OFF_RATING')),
            'def_rtg': safe_float(row.get('DEF_RATING')),
            'possessions': safe_int(row.get('POSS')),
            
            # Advanced Shooting
            'ts_pct': safe_float(row.get('TS_PCT')),
            
            # Shot location (from shot chart aggregation)
            'rim_fga': shot_stats.get('rim_fga'),
            'rim_fg_pct': shot_stats.get('rim_fg_pct'),
            'uast_rim_fga': shot_stats.get('uast_rim_fga'),
            'mr_fga': shot_stats.get('mr_fga'),
            'mr_fg_pct': shot_stats.get('mr_fg_pct'),
            'uast_mr_fga': shot_stats.get('uast_mr_fga'),
            'uast_3fga': uast_3fga,
            'cont_3pa': matchup_stats.get('cont_3pa'),
            'cont_3p_pct': matchup_stats.get('cont_3p_pct'),
            'open_3pa': shot_stats.get('open_3pa'),
            'open_3p_pct': shot_stats.get('open_3p_pct'),
            
            # Playmaking Advanced (tracking not available - dropped)
            'pot_assists': None,
            'on_ball_pct': None,
            'avg_sec_touch': None,
            
            # Rebounding Advanced
            'oreb_pct': safe_float(row.get('OREB_PCT')),
            'dreb_pct': safe_float(row.get('DREB_PCT')),
            
            # Hustle
            'off_distance': None,  # Tracking not available - dropped
            'charges_drawn': safe_int(row.get('chargesDrawn')),
            'deflections': safe_int(row.get('deflections')),
            'contests': safe_int(row.get('contestedShots')),
            'def_efg_pct': matchup_stats.get('def_efg_pct'),
            'def_distance': None,  # Tracking not available - dropped
        }
        
        records.append(record)
    
    log_info(f"Transformed {len(records)} player records")
    return records

# ============================================
# DATA LOADING
# ============================================

def upsert_player_game_stats(conn, records: List[Dict]):
    """Insert or update player game stats in PostgreSQL"""
    if not records:
        return
    
    log_info(f"Upserting {len(records)} player game stats...")
    
    columns = [
        'game_id', 'player_id', 'team_id', 'game_date',
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
    
    # Prepare values
    values = []
    for record in records:
        values.append(tuple(record.get(col) for col in columns))
    
    # Build upsert query
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    
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
        log_success(f"✓ Upserted {len(records)} player game stats")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to upsert player game stats: {e}")
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
    
    query = """
        INSERT INTO games (game_id, game_date, season, season_type, home_team_id, away_team_id, home_score, away_score, game_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE SET
            season_type = EXCLUDED.season_type,
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
                safe_int(home_team['TEAM_ID']),
                safe_int(away_team['TEAM_ID']),
                safe_int(home_team['PTS']),
                safe_int(away_team['PTS']),
                'Final'
            ))
        conn.commit()
        log_success(f"✓ Upserted game {game_id} ({season_type})")
    except Exception as e:
        conn.rollback()
        log_error(f"Failed to upsert game: {e}")

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
        
        # Load
        upsert_game(conn, game_id, game_date, season_type, box_scores)
        upsert_player_game_stats(conn, player_records)
        
        log_success(f"✓ Successfully processed game {game_id}")
        
    except Exception as e:
        log_error(f"Failed to process game {game_id}: {e}")
        import traceback
        traceback.print_exc()

def run_etl_for_date(date_str: str):
    """Run ETL for a specific date"""
    log_info(f"\n{'='*60}")
    log_info(f"ETL Pipeline - {date_str}")
    log_info(f"{'='*60}")
    
    conn = get_db_connection()
    
    try:
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
