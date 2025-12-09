"""
THE GLASS - Daily ETL (Runs nightly)
Fast update that handles:
1. Player season statistics (current + last season) - FAST (2 API calls)
2. Team rosters + jersey numbers - FAST (30 API calls, ~30 seconds)
3. Team season statistics (current season) - FAST (6 API calls)
4. New player details (height, weight, birthdate) - RARE (only for new players)
5. Optional: Historical backfill

This runs DAILY. Height/weight/birthdate for existing players updated ANNUALLY on August 1st.

Usage:
    python src/etl.py                          # Run daily update (fast, ~2-3 minutes)
    python src/etl.py --backfill 2020          # Backfill from 2020 to present
    python src/etl.py --backfill 2015 --end 2020  # Backfill 2015-2020 only
"""

import os
import sys
import time
import argparse
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
from tqdm import tqdm
from nba_api.stats.endpoints import (
    commonplayerinfo,
    leaguedashplayerstats, leaguedashteamstats,
    leaguedashptstats,
    leaguehustlestatsplayer, leaguehustlestatsteam, 
    leaguedashptdefend, leaguedashptteamdefend,
    playerdashboardbyshootingsplits, teamplayeronoffdetails
)

# Load environment variables FIRST (before importing config)
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import config modules (works both with -m and direct execution)
try:
    from src.config import NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA
    from src.backend_config import (
        ETL_GROUPS,
        ETL_STAT_MAPPING,
        get_stats_by_group,
        get_stats_by_entity
    )
except ImportError:
    from config import NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA
    from backend_config import (
        ETL_GROUPS,
        ETL_STAT_MAPPING,
        get_stats_by_group,
        get_stats_by_entity
    )


RATE_LIMIT_DELAY = NBA_CONFIG['api_rate_limit_delay']

# Global progress bars (accessed by all ETL functions)
_overall_pbar = None
_group_pbar = None

# Global progress bars (accessed by all ETL functions)
_overall_pbar = None
_group_pbar = None


def resilient_api_call(endpoint_func, call_description, max_retries=3, timeout=20):
    """
    Execute NBA API call with retry logic and timeout protection.
    
    Args:
        endpoint_func: Lambda that creates and calls the NBA API endpoint
        call_description: Human-readable description for logging
        max_retries: Number of attempts (default 3)
        timeout: Timeout in seconds (default 20)
    
    Returns:
        API response dict
    
    Raises:
        Exception if all retries fail
    """
    result = None
    for attempt in range(1, max_retries + 1):
        try:
            result = endpoint_func(timeout)
            return result  # Success
        except Exception as e:
            if attempt < max_retries:
                wait_time = attempt * 3  # Exponential: 3s, 6s, 9s
                log(f"  WARNING - {call_description} attempt {attempt}/{max_retries} failed: {str(e)[:80]}")
                log(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log(f"  ERROR - {call_description} failed after {max_retries} attempts")
                raise
    
    if result is None:
        raise Exception(f"{call_description} returned None after {max_retries} attempts")


def log(message, level="INFO"):
    """Centralized logging - uses tqdm.write to avoid interfering with progress bars"""
    tqdm.write(message)


def update_group_progress(n=1, description=None):
    """Update the group progress bar (called from individual ETL functions)"""
    global _group_pbar, _overall_pbar
    if _group_pbar is not None:
        if description:
            _group_pbar.set_description(description)
        _group_pbar.update(n)
    # Also update overall progress with each transaction
    if _overall_pbar is not None:
        _overall_pbar.update(n)


def get_db_connection():
    """Create database connection with timeout protection"""
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        application_name='the_glass_etl',
        options='-c statement_timeout=30000'  # 30 second timeout per statement
    )
    return conn


def ensure_schema_exists():
    """Create database schema if it doesn't exist (first-time setup)"""
    log("Checking database schema...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if tables exist
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'players'
        )
    """)
    
    if cursor.fetchone()[0]:
        log("Schema already exists")
        cursor.close()
        conn.close()
        return
    
    log("Creating database schema...")
    
    # Use centralized schema DDL from config
    cursor.execute(DB_SCHEMA['create_schema_sql'])
    conn.commit()
    
    log("Schema created successfully")
    
    cursor.close()
    conn.close()


def safe_int(value, scale=1):
    """Convert value to scaled integer, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return 0
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return 0


def safe_float(value, scale=1):
    """Convert value to scaled float (as integer), handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return None
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return None


def safe_str(value):
    """Safely convert to string"""
    if value is None or value == '' or (hasattr(value, '__len__') and len(value) == 0):
        return None
    return str(value)


def parse_height(height_str):
    """
    Parse height from NBA API format to inches.
    NBA API returns height as: "6-8", "7-0", etc. (feet-inches)
    Returns: total inches as integer, or 0 if invalid
    """
    if not height_str or height_str == '' or height_str == 'None':
        return 0
    
    try:
        # Handle "6-8" format
        if '-' in str(height_str):
            feet, inches = str(height_str).split('-')
            return int(feet) * 12 + int(inches)
        # Handle already numeric values
        else:
            return int(float(height_str))
    except (ValueError, AttributeError):
        return 0


def parse_birthdate(date_str):
    """Parse birthdate string to date"""
    if not date_str or date_str == '' or str(date_str).lower() == 'nan':
        return None
    try:
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                return datetime.strptime(str(date_str), fmt).date()
            except ValueError:
                continue
    except:
        pass
    return None


def get_stat_value_from_row(row, stat_config):
    """
    Extract and scale a stat value from API response using config.
    
    Args:
        row: DataFrame row from NBA API
        stat_config: Config dict from ETL_STAT_MAPPING
        
    Returns:
        Scaled value ready for database insert
    """
    nba_field = stat_config.get('af')  # API field name
    scale = stat_config.get('sc', 1)
    
    if nba_field is None:
        return None
    
    value = row.get(nba_field, 0)
    
    # Apply scaling based on stat type
    if scale == 1:
        return safe_int(value)
    elif scale == 10:
        return safe_int(value, 10)
    elif scale == 1000:
        return safe_float(value, 1000)
    else:
        return safe_float(value, scale)


def update_player_rosters():
    """
    FAST daily roster update:
    1. Fetch player stats (current + last season) - 2 API calls, very fast
    2. Fetch team rosters to get team_id + jersey_number - 30 API calls, ~30 seconds
    3. Only fetch height/weight/birthdate for NEW players (rare)
    
    This completes in ~2-3 minutes instead of 20 minutes.
    Height/weight/birthdate for existing players updated annually on August 1st.
    """
    log("=" * 70)
    log("STEP 1: Updating Player Rosters")
    log("="* 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    current_season = NBA_CONFIG['current_season']
    
    log(f"Fetching ALL players with stats from current season ({current_season})...")
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW
    # This is the SOURCE OF TRUTH for current team assignments
    log("Fetching current team rosters from NBA API...")
    try:
        from nba_api.stats.static import teams
        from nba_api.stats.endpoints import commonteamroster
        nba_teams = teams.get_teams()
        
        for team in nba_teams:
            time.sleep(1)
            team_id = team['id']
            # Retry logic for roster fetching
            for attempt in range(3):
                try:
                    time.sleep(RATE_LIMIT_DELAY)
                    roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=current_season, timeout=30)
                    roster_df = roster.get_data_frames()[0]
                    
                    for _, player_row in roster_df.iterrows():
                        player_id = player_row['PLAYER_ID']
                        player_name = player_row['PLAYER']
                        
                        # Add player from roster (SOURCE OF TRUTH)
                        all_players[player_id] = {
                            'player_id': player_id,
                            'team_id': team_id,  # Use team from roster
                            'name': player_name,
                            'jersey': safe_str(player_row.get('NUM')),
                            'weight': None,  # Will get from annual ETL or commonplayerinfo for new players
                            'age': None
                        }
                        
                    update_group_progress(1)  # One team completed
                    break
                except Exception as e:
                    if attempt < 2:
                        wait_time = 5 * (attempt + 1)
                        log(f"  WARNING - Retry {attempt + 1}/3 for {team['abbreviation']} (waiting {wait_time}s)", "WARN")
                        time.sleep(wait_time)
                    else:
                        log(f"  WARNING - Failed to fetch roster for {team['abbreviation']} after 3 attempts: {e}", "WARN")
                        continue
        
        log(f"Fetched current rosters: {len(all_players)} players")
    except Exception as e:
        log(f"WARNING - Failed to fetch current rosters: {e}", "WARN")
    
    log(f"Total players found from rosters: {len(all_players)}")
    
    # Get existing players from database to identify NEW players
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT player_id FROM players")
    existing_player_ids = {row[0] for row in cursor.fetchall()}
    
    # Identify NEW players (not in database)
    new_player_ids = [pid for pid in all_players.keys() if pid not in existing_player_ids]
    
    if new_player_ids:
        log(f"Found {len(new_player_ids)} new players - fetching height/weight/birthdate...")
        
        failed_count = 0
        consecutive_failures = 0
        
        for idx, player_id in enumerate(new_player_ids):
            player_name = all_players[player_id].get('name', 'Unknown')
            
            # Take breaks if seeing failures
            if consecutive_failures >= 3:
                log("⚠ Detected API issues (3 consecutive failures), taking 2-minute emergency break...", "WARN")
                time.sleep(120)
                consecutive_failures = 0
            
            # Try to fetch details with exponential backoff
            for attempt in range(3):
                try:
                    info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=20)
                    time.sleep(1.5)
                    
                    player_df = info.get_data_frames()[0]
                    if not player_df.empty:
                        pd = player_df.iloc[0]
                        all_players[player_id].update({
                            'birthdate': parse_birthdate(pd.get('BIRTHDATE')),
                            'height': parse_height(pd.get('HEIGHT')),
                            'weight': safe_int(pd.get('WEIGHT')),
                            'jersey': safe_str(pd.get('JERSEY')),
                            'years_experience': safe_int(pd.get('SEASON_EXP')),
                            'pre_nba_team': safe_str(pd.get('SCHOOL'))
                        })
                    consecutive_failures = 0
                    break
                    
                except Exception as e:
                    if attempt < 2:
                        wait_time = 5 * (attempt + 1)
                        log(f"  WARNING - Retry {attempt + 1}/3 for {player_name} (waiting {wait_time}s)", "WARN")
                        time.sleep(wait_time)
                    else:
                        failed_count += 1
                        consecutive_failures += 1
                        log(f"  ERROR - Failed to fetch {player_name}: {e}", "ERROR")
            
            update_group_progress(1)  # One new player processed
            
            # Log progress every 5 new players
            if (idx + 1) % 5 == 0 or (idx + 1) == len(new_player_ids):
                status = f"(OK {idx + 1 - failed_count} success, ERROR {failed_count} failed)" if failed_count > 0 else ""
                log(f"Progress: {idx + 1}/{len(new_player_ids)} new players {status}")
        
        if failed_count > 0:
            log(f"WARNING - Could not fetch details for {failed_count}/{len(new_player_ids)} new players", "WARN")
            log("  These players will still be added with basic info (name, team, jersey from roster)", "WARN")
    else:
        log("No new players found - all players already in database")
    
    # First, clear team_id for all players (they'll be re-assigned if still on roster)
    log("Clearing team assignments for all players...")
    cursor.execute("UPDATE players SET team_id = NULL, updated_at = NOW()")
    conn.commit()
    log("Cleared all team assignments")
    
    # Update database with all players
    log(f"Updating database with {len(all_players)} players currently on rosters...")
    
    for player_id, player_data in all_players.items():
        try:
            cursor.execute("SELECT team_id FROM players WHERE player_id = %s", (player_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing player (jersey and team_id only - height/weight/birthdate updated annually)
                if 'birthdate' in player_data:
                    cursor.execute("""
                        UPDATE players SET
                            team_id = %s, jersey_number = %s, 
                            weight_lbs = %s, height_inches = %s,
                            pre_nba_team = %s, birthdate = %s, 
                            updated_at = NOW()
                        WHERE player_id = %s
                    """, (
                        player_data['team_id'], player_data['jersey'],
                        player_data.get('weight'), player_data.get('height'),
                        player_data.get('pre_nba_team'),
                        player_data.get('birthdate'),
                        player_id
                    ))
                else:
                    cursor.execute("""
                        UPDATE players SET
                            team_id = %s, jersey_number = %s, updated_at = NOW()
                        WHERE player_id = %s
                    """, (player_data['team_id'], player_data['jersey'], player_id))
                
                if existing[0] != player_data['team_id']:
                    players_updated += 1
            else:
                # Insert new player (with height/weight/birthdate if fetched)
                if 'birthdate' in player_data:
                    cursor.execute("""
                        INSERT INTO players (
                            player_id, name, team_id, jersey_number,
                            weight_lbs, height_inches,
                            pre_nba_team, birthdate
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        player_id, player_data['name'], player_data['team_id'],
                        player_data['jersey'], player_data.get('weight'),
                        player_data.get('height'),
                        player_data.get('pre_nba_team'),
                        player_data.get('birthdate')
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO players (player_id, name, team_id, jersey_number)
                        VALUES (%s, %s, %s, %s)
                    """, (player_id, player_data['name'], player_data['team_id'],
                          player_data['jersey']))
                
                players_added += 1
        
        except Exception as e:
            log(f"ERROR - Failed to update player {player_id}: {e}", "ERROR")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"Roster update complete: {players_added} added, {players_updated} updated")
    
    return True


def update_player_stats():
    """Update season statistics for all players (GROUP 1: Basic Stats)"""
    log("=" * 70)
    log("STEP 2: Updating Player Stats (GROUP 1: Basic Stats)")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    
    # Get GROUP 1 stats from config
    group1_stats = get_stats_by_group(1)
    log(f" Processing {len(group1_stats)} GROUP 1 stats")
    
    # Get valid player IDs from database (all players on rosters)
    cursor.execute("SELECT player_id, team_id FROM players")
    all_players = cursor.fetchall()
    valid_player_ids = {row[0] for row in all_players}
    log(f"Found {len(valid_player_ids)} players in database")
    
    # Process all season types
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
    ]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            # Fetch basic stats (with retry)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    stats = leaguedashplayerstats.LeagueDashPlayerStats(
                        season=current_season,
                        season_type_all_star=season_type_name,
                        per_mode_detailed='Totals',
                        timeout=120  # Increased timeout
                    )
                    time.sleep(RATE_LIMIT_DELAY)
                    df = stats.get_data_frames()[0]
                    break
                except Exception as retry_error:
                    if attempt < max_retries - 1:
                        wait_time = 10 * (attempt + 1)
                        log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {season_type_name} basic stats, retrying in {wait_time}s...", "WARN")
                        time.sleep(wait_time)
                    else:
                        raise retry_error
            
            if df.empty:
                log(f"No {season_type_name} data for {current_season}")
                continue
            
            # Fetch advanced stats (with retry)
            try:
                for attempt in range(max_retries):
                    try:
                        adv_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed='Totals',
                            timeout=120
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        adv_df = adv_stats.get_data_frames()[0]
                        break
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            wait_time = 10 * (attempt + 1)
                            log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {season_type_name} advanced stats, retrying in {wait_time}s...", "WARN")
                            time.sleep(wait_time)
                        else:
                            raise retry_error
                
                if not adv_df.empty:
                    df = df.merge(
                        adv_df[['PLAYER_ID', 'TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'OREB_PCT', 'DREB_PCT']], 
                        on=['PLAYER_ID', 'TEAM_ID'], 
                        how='left'
                    )
            except Exception as e:
                log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
            
            log(f"Fetched {season_type_name} stats for {len(df)} players")
            
            # Track which players have stats from API
            players_with_stats = set()
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                player_id = row['PLAYER_ID']
                
                # Skip if not in our database
                if player_id not in valid_player_ids:
                    continue
                
                players_with_stats.add(player_id)
                
                # Build record using config - start with fixed fields
                record_values = [
                    player_id,
                    current_year,
                    safe_int(row.get('TEAM_ID', 0)),
                    season_type_code,
                ]
                
                # Add GROUP 1 stats from config in order
                for stat_key in group1_stats:
                    stat_cfg = ETL_STAT_MAPPING[stat_key]
                    
                    # Handle calculated fields (fg2m, fg2a)
                    if stat_cfg.get('calc'):
                        if stat_key == 'fg2m':
                            fgm = safe_int(row.get('FGM', 0))
                            fg3m = safe_int(row.get('FG3M', 0))
                            value = max(0, fgm - fg3m)
                        elif stat_key == 'fg2a':
                            fga = safe_int(row.get('FGA', 0))
                            fg3a = safe_int(row.get('FG3A', 0))
                            value = max(0, fga - fg3a)
                        else:
                            value = None  # Other calculated fields handled later
                    else:
                        # Extract from API response using config
                        value = get_stat_value_from_row(row, stat_cfg)
                    
                    record_values.append(value)
                
                records.append(tuple(record_values))
            
            # Add zero-stat records for players on rosters who didn't play (Regular Season only)
            if season_type_code == 1:  # Regular Season
                players_without_stats = valid_player_ids - players_with_stats
                if players_without_stats:
                    log(f"  Adding {len(players_without_stats)} roster players with no stats yet")
                    skipped = 0
                    for player_id in players_without_stats:
                        # Get team_id from players table
                        team_id = next((t for p, t in all_players if p == player_id), None)
                        # Skip if no valid team_id (shouldn't happen, but safety check)
                        if not team_id:
                            skipped += 1
                            continue
                        # Build zero-stat record using config
                        zero_values = [player_id, current_year, team_id, season_type_code]
                        
                        # Add zeros/Nones for all GROUP 1 stats
                        for stat_key in group1_stats:
                            stat_cfg = ETL_STAT_MAPPING[stat_key]
                            # Use None for percentage stats, 0 for counts
                            if stat_cfg.get('sc', 1) >= 1000:  # Percentage stats
                                zero_values.append(None)
                            elif 'rating' in stat_key:  # Rating stats
                                zero_values.append(None)
                            else:  # Count stats
                                # Explicitly ensure games_played gets 0 to satisfy NOT NULL constraint
                                zero_values.append(0)
                        
                        records.append(tuple(zero_values))
            
            # Bulk insert using config-driven column names
            if records:
                # Build column list from config
                db_columns = ['player_id', 'year', 'team_id', 'season_type'] + list(group1_stats.keys())
                columns_str = ', '.join(db_columns)
                
                # Build UPDATE SET clause from config (exclude keys)
                update_clauses = ['team_id = EXCLUDED.team_id'] + [
                    f"{col} = EXCLUDED.{col}" for col in group1_stats
                ]
                update_str = ',\n                        '.join(update_clauses)
                
                # Execute bulk insert
                sql = f"""
                    INSERT INTO player_season_stats (
                        {columns_str}
                    ) VALUES %s
                    ON CONFLICT (player_id, year, season_type) DO UPDATE SET
                        {update_str},
                        updated_at = NOW()
                """
                
                execute_values(
                    cursor,
                    sql,
                    records
                )
                conn.commit()
                total_updated += len(records)
                log(f"Inserted/Updated {len(records)} {season_type_name} player records")
        
        except Exception as e:
            log(f"ERROR - Error fetching {season_type_name} stats: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    log(f"Player stats complete: {total_updated} total records")
    return True


def update_team_stats():
    """Update season statistics for all teams (GROUP 1: Basic + GROUP 7: Opponent)"""
    log("=" * 70)
    log("STEP 3: Updating Team Stats (GROUP 1: Basic + GROUP 7: Opponent)")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    
    # Get valid team IDs from config (numeric IDs, not abbreviations)
    valid_team_ids = set(TEAM_IDS.values())
    
    # Get GROUP 1 and GROUP 7 stats from config
    group1_stats = get_stats_by_group(1)
    group7_stats = get_stats_by_group(7)
    log(f" Processing {len(group1_stats)} GROUP 1 stats + {len(group7_stats)} GROUP 7 defense stats")
    
    # Process all season types
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
        ('PlayIn', 3),
    ]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            # Fetch basic stats (with retry)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    stats = leaguedashteamstats.LeagueDashTeamStats(
                        season=current_season,
                        season_type_all_star=season_type_name,
                        per_mode_detailed='Totals',
                        timeout=120  # Increased timeout
                    )
                    time.sleep(RATE_LIMIT_DELAY)
                    df = stats.get_data_frames()[0]
                    break
                except Exception as retry_error:
                    if attempt < max_retries - 1:
                        wait_time = 10 * (attempt + 1)
                        log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {season_type_name} team stats, retrying in {wait_time}s...", "WARN")
                        time.sleep(wait_time)
                    else:
                        raise retry_error
            
            if df.empty:
                log(f"No {season_type_name} data for {current_season}")
                continue
            
            # Fetch advanced stats (with retry)
            try:
                for attempt in range(max_retries):
                    try:
                        adv_stats = leaguedashteamstats.LeagueDashTeamStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed='Totals',
                            timeout=120
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        adv_df = adv_stats.get_data_frames()[0]
                        break
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            wait_time = 10 * (attempt + 1)
                            log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {season_type_name} team advanced stats, retrying in {wait_time}s...", "WARN")
                            time.sleep(wait_time)
                        else:
                            raise retry_error
                
                if not adv_df.empty:
                    df = df.merge(
                        adv_df[['TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'OREB_PCT', 'DREB_PCT']], 
                        on='TEAM_ID', 
                        how='left'
                    )
            except Exception as e:
                log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
            
            # Fetch opponent stats (what opponents did against each team)
            try:
                for attempt in range(max_retries):
                    try:
                        opp_stats = leaguedashteamstats.LeagueDashTeamStats(
                            season=current_season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Opponent',
                            per_mode_detailed='Totals',
                            timeout=120
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        opp_df = opp_stats.get_data_frames()[0]
                        break
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            wait_time = 10 * (attempt + 1)
                            log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for {season_type_name} team opponent stats, retrying in {wait_time}s...", "WARN")
                            time.sleep(wait_time)
                        else:
                            raise retry_error
                
                if not opp_df.empty:
                    # Build opponent column list from config (13 available stats)
                    opp_api_columns = ['TEAM_ID']
                    for stat_key in group7_stats:
                        nba_field = ETL_STAT_MAPPING[stat_key].get('nba')
                        if nba_field:
                            opp_api_columns.append(nba_field)
                    
                    opp_df = opp_df[opp_api_columns]
                    df = df.merge(opp_df, on='TEAM_ID', how='left')
                    log(f"Fetched {len(group7_stats)} opponent stats for {len(opp_df)} teams")
            except Exception as e:
                log(f"Warning: Could not fetch opponent stats: {e}", "WARN")
            
            # Remove duplicates (some seasons return duplicate team entries)
            df = df.drop_duplicates(subset=['TEAM_ID'], keep='first')
            
            log(f"Fetched {season_type_name} stats for {len(df)} teams")
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                team_id = row['TEAM_ID']
                
                # Skip if not valid team
                if team_id not in valid_team_ids:
                    continue
                
                # Build record using config - start with fixed fields
                record_values = [
                    team_id,
                    current_year,
                    season_type_code,
                ]
                
                # Add GROUP 1 stats from config
                for stat_key in group1_stats:
                    stat_cfg = ETL_STAT_MAPPING[stat_key]
                    
                    # Handle calculated fields (fg2m, fg2a)
                    if stat_cfg.get('calc'):
                        if stat_key == 'fg2m':
                            fgm = safe_int(row.get('FGM', 0))
                            fg3m = safe_int(row.get('FG3M', 0))
                            value = max(0, fgm - fg3m)
                        elif stat_key == 'fg2a':
                            fga = safe_int(row.get('FGA', 0))
                            fg3a = safe_int(row.get('FG3A', 0))
                            value = max(0, fga - fg3a)
                        else:
                            value = None
                    else:
                        value = get_stat_value_from_row(row, stat_cfg)
                    
                    record_values.append(value)
                
                # Add GROUP 7 opponent stats from config
                for stat_key in group7_stats:
                    stat_cfg = ETL_STAT_MAPPING[stat_key]
                    
                    # Handle calculated opponent fields (opp_fg2m, opp_fg2a)
                    if stat_cfg.get('calc'):
                        if stat_key == 'opp_fg2m':
                            opp_fgm = safe_int(row.get('OPP_FGM', 0))
                            opp_fg3m = safe_int(row.get('OPP_FG3M', 0))
                            value = max(0, opp_fgm - opp_fg3m)
                        elif stat_key == 'opp_fg2a':
                            opp_fga = safe_int(row.get('OPP_FGA', 0))
                            opp_fg3a = safe_int(row.get('OPP_FG3A', 0))
                            value = max(0, opp_fga - opp_fg3a)
                        else:
                            value = None
                    else:
                        value = get_stat_value_from_row(row, stat_cfg)
                    
                    record_values.append(value)
                
                records.append(tuple(record_values))
            
            # Bulk insert using config-driven column names
            if records:
                # Build column list from config
                db_columns = ['team_id', 'year', 'season_type'] + list(group1_stats.keys()) + list(group7_stats.keys())
                columns_str = ', '.join(db_columns)
                
                # Build UPDATE SET clause from config (exclude keys)
                update_clauses = [f"{col} = EXCLUDED.{col}" for col in list(group1_stats.keys()) + list(group7_stats.keys())]
                update_str = ',\n                        '.join(update_clauses)
                
                # Execute bulk insert
                sql = f"""
                    INSERT INTO team_season_stats (
                        {columns_str}
                    ) VALUES %s
                    ON CONFLICT (team_id, year, season_type) DO UPDATE SET
                        {update_str},
                        updated_at = NOW()
                """
                
                execute_values(
                    cursor,
                    sql,
                    records
                )
                conn.commit()
                total_updated += len(records)
                log(f"Inserted/Updated {len(records)} {season_type_name} team records")
        
        except Exception as e:
            log(f"ERROR - Error fetching {season_type_name} stats: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    log(f"Team stats complete: {total_updated} total records")
    return True


def update_shooting_tracking_bulk(season, season_year):
    """
    GROUP 2: Shooting Tracking - Per-player API calls
    
    Uses backend_config to map shooting columns:
    - Rim: cont_rim_fgm/fga, open_rim_fgm/fga
    - 2PT (all): cont_fg2m/fg2a, open_fg2m/fg2a  
    - 3PT: cont_fg3m/fga, open_fg3m/fga
    - Mid-Range: Calculated as cont_fg2 - cont_rim, open_fg2 - open_rim
    
    Uses playerdashptshots endpoint with zone + close_def_dist_range filters
    """
    log(f"Fetching GROUP 2: Shooting Tracking (per-player) for {season}...")
    
    # Get GROUP 2 stats from config
    group2_stats = get_stats_by_group(2)
    log(f" Processing {len(group2_stats)} GROUP 2 shooting stats")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get active players
    cursor.execute("""
        SELECT DISTINCT player_id 
        FROM player_season_stats
        WHERE year = %s AND season_type = 1 AND games_played > 0
        ORDER BY player_id
    """, (season_year,))
    
    players = [row[0] for row in cursor.fetchall()]
    log(f"Found {len(players)} active players")
    
    # Store player shooting data
    player_data = {}
    failed = 0
    
    try:
        from nba_api.stats.endpoints import playerdashptshots
        
        for idx, player_id in enumerate(players):
            if idx % 100 == 0 and idx > 0:
                log(f"  Progress: {idx}/{len(players)} players processed...")
            
            try:
                stats = {}
                
                # 1. Contested rim (Restricted Area, 0-4 ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='0-2 Feet - Very Tight, 2-4 Feet - Tight',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'ClosestDefenderShooting':
                            for row in rs['rowSet']:
                                if 'Restricted Area' in str(row):
                                    stats['cont_rim_fgm'] = stats.get('cont_rim_fgm', 0) + (row[rs['headers'].index('FGM')] or 0)
                                    stats['cont_rim_fga'] = stats.get('cont_rim_fga', 0) + (row[rs['headers'].index('FGA')] or 0)
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 2. Open rim (Restricted Area, 4+ ft defender)  
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='4-6 Feet - Open, 6+ Feet - Wide Open',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'ClosestDefenderShooting':
                            for row in rs['rowSet']:
                                if 'Restricted Area' in str(row):
                                    stats['open_rim_fgm'] = stats.get('open_rim_fgm', 0) + (row[rs['headers'].index('FGM')] or 0)
                                    stats['open_rim_fga'] = stats.get('open_rim_fga', 0) + (row[rs['headers'].index('FGA')] or 0)
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 3. Contested 2PT (all zones, 0-4 ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='0-2 Feet - Very Tight, 2-4 Feet - Tight',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['cont_fg2m'] = row[rs['headers'].index('FG2M')] or 0
                                stats['cont_fg2a'] = row[rs['headers'].index('FG2A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 4. Open 2PT (all zones, 4+ ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='2PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='4-6 Feet - Open, 6+ Feet - Wide Open',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['open_fg2m'] = row[rs['headers'].index('FG2M')] or 0
                                stats['open_fg2a'] = row[rs['headers'].index('FG2A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 5. Contested 3PT (0-4 ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='3PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='0-2 Feet - Very Tight, 2-4 Feet - Tight',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['cont_fg3m'] = row[rs['headers'].index('FG3M')] or 0
                                stats['cont_fg3a'] = row[rs['headers'].index('FG3A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                # 6. Open 3PT (4+ ft defender)
                try:
                    result = playerdashptshots.PlayerDashPtShots(
                        player_id=player_id,
                        season=season,
                        season_type_all_star='Regular Season',
                        per_mode_simple='Totals',
                        general_range='3PT Field Goals',
                        shot_clock_range='',
                        shot_dist_range='',
                        touch_time_range='',
                        closest_def_dist_range='4-6 Feet - Open, 6+ Feet - Wide Open',
                        timeout=20
                    ).get_dict()
                    
                    for rs in result['resultSets']:
                        if rs['name'] == 'GeneralShooting':
                            for row in rs['rowSet']:
                                stats['open_fg3m'] = row[rs['headers'].index('FG3M')] or 0
                                stats['open_fg3a'] = row[rs['headers'].index('FG3A')] or 0
                                break
                except:
                    pass
                
                time.sleep(RATE_LIMIT_DELAY)
                
                if stats:
                    player_data[player_id] = stats
                    
            except Exception as e:
                failed += 1
                if failed <= 5:  # Only log first 5 failures
                    log(f"  Failed player {player_id}: {e}", "WARN")
                continue
        
        # Update database
        log("Updating database with shooting data...")
        updated = 0
        
        for player_id, stats in player_data.items():
            cursor.execute("""
                UPDATE player_season_stats
                SET cont_rim_fgm = %s, cont_rim_fga = %s,
                    open_rim_fgm = %s, open_rim_fga = %s,
                    cont_fg2m = %s, cont_fg2a = %s,
                    open_fg2m = %s, open_fg2a = %s,
                    cont_fg3m = %s, cont_fg3a = %s,
                    open_fg3m = %s, open_fg3a = %s,
                    updated_at = NOW()
                WHERE player_id = %s AND year = %s AND season_type = 1
            """, (
                stats.get('cont_rim_fgm', 0),
                stats.get('cont_rim_fga', 0),
                stats.get('open_rim_fgm', 0),
                stats.get('open_rim_fga', 0),
                stats.get('cont_fg2m', 0),
                stats.get('cont_fg2a', 0),
                stats.get('open_fg2m', 0),
                stats.get('open_fg2a', 0),
                stats.get('cont_fg3m', 0),
                stats.get('cont_fg3a', 0),
                stats.get('open_fg3m', 0),
                stats.get('open_fg3a', 0),
                player_id,
                season_year
            ))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"GROUP 2 Shooting: {updated} players updated, {failed} failed")
        
    except Exception as e:
        log(f"Failed GROUP 2 shooting tracking: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_playmaking_bulk(season, season_year):
    """
    GROUP 3: Playmaking - Config-driven
    
    Uses backend_config to map 2 playmaking columns:
    - pot_ast: Potential assists
    - touches: Touches (passes received)
    
    Endpoint: LeagueDashPtStats with pt_measure_type='Passing'
    """
    log(f"Fetching GROUP 3: Playmaking (league-wide) for {season}...")
    
    # Get GROUP 3 stats from config
    group3_stats = get_stats_by_group(3)
    log(f" Processing {len(group3_stats)} GROUP 3 playmaking stats")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        endpoint = leaguedashptstats.LeagueDashPtStats(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            player_or_team='Player',
            pt_measure_type='Passing'
        )
        
        result = endpoint.get_dict()
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        # Extract values using config
        update_records = []
        for row in rs['rowSet']:
            player_id = row[0]  # PLAYER_ID
            
            # Extract each GROUP 3 stat from API response using config
            values = []
            for stat_name, stat_cfg in group3_stats.items():
                nba_field = stat_cfg.get('nba')
                scale = stat_cfg.get('sc', 1)
                raw_value = row[headers.index(nba_field)] if nba_field else 0
                value = safe_int(raw_value, scale) if scale < 1000 else safe_float(raw_value, scale)
                values.append(value)
            
            # Build record: (stat1, stat2, ..., player_id, season_year)
            values.extend([player_id, season_year])
            update_records.append(tuple(values))
        
        # Bulk update using config-driven column names
        if update_records:
            set_clause = ', '.join([f"{col} = %s" for col in group3_stats.keys()])
            
            updated = 0
            for record in update_records:
                cursor.execute(f"""
                    UPDATE player_season_stats
                    SET {set_clause}, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, record)
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            log(f"GROUP 3 Playmaking: {updated} players updated (2 columns)")
        else:
            log("WARNING - No playmaking data to update", "WARN")
        
    except Exception as e:
        log(f"Failed GROUP 3 playmaking: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_rebounding_bulk(season, season_year):
    """
    GROUP 4: Rebounding - Config-driven
    
    Uses backend_config to map 2 contested rebound columns:
    - cont_oreb: Contested offensive rebounds
    - cont_dreb: Contested defensive rebounds
    
    Endpoint: LeagueDashPtStats with pt_measure_type='Rebounding'
    """
    log(f"Fetching GROUP 4: Rebounding (league-wide) for {season}...")
    
    # Get GROUP 4 stats from config
    group4_stats = get_stats_by_group(4)
    log(f" Processing {len(group4_stats)} GROUP 4 rebounding stats")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        endpoint = leaguedashptstats.LeagueDashPtStats(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            player_or_team='Player',
            pt_measure_type='Rebounding'
        )
        
        result = endpoint.get_dict()
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        # Extract values using config
        update_records = []
        for row in rs['rowSet']:
            player_id = row[0]  # PLAYER_ID
            
            # Extract each GROUP 4 stat from API response using config
            values = []
            for stat_name, stat_cfg in group4_stats.items():
                nba_field = stat_cfg.get('nba')
                scale = stat_cfg.get('sc', 1)
                raw_value = row[headers.index(nba_field)] if nba_field else 0
                value = safe_int(raw_value, scale) if scale < 1000 else safe_float(raw_value, scale)
                values.append(value)
            
            # Build record: (stat1, stat2, ..., player_id, season_year)
            values.extend([player_id, season_year])
            update_records.append(tuple(values))
        
        # Bulk update using config-driven column names
        if update_records:
            set_clause = ', '.join([f"{col} = %s" for col in group4_stats.keys()])
            
            updated = 0
            for record in update_records:
                cursor.execute(f"""
                    UPDATE player_season_stats
                    SET {set_clause}, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, record)
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            log(f"GROUP 4 Rebounding: {updated} players updated (2 columns)")
        else:
            log("WARNING - No rebounding data to update", "WARN")
        
    except Exception as e:
        log(f"Failed GROUP 4 rebounding: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_hustle_stats_bulk(season, season_year):
    """
    GROUP 6: Hustle Stats - Config-driven
    
    Uses backend_config to map 3 hustle columns:
    - charges_drawn: Charges drawn
    - deflections: Deflections
    - contests: Contested shots
    
    Endpoint: LeagueHustleStatsPlayer
    """
    log(f"Fetching GROUP 6: Hustle stats (league-wide) for {season}...")
    
    # Get GROUP 6 stats from config
    group6_stats = get_stats_by_group(6)
    log(f" Processing {len(group6_stats)} GROUP 6 hustle stats")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        hustle = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            per_mode_time='Totals',
            season_type_all_star='Regular Season'
        )
        
        result = hustle.get_dict()
        
        # Extract values using config
        update_records = []
        for rs in result['resultSets']:
            if rs['name'] == 'HustleStatsPlayer':
                headers = rs['headers']
                
                for row in rs['rowSet']:
                    player_id = row[headers.index('PLAYER_ID')]
                    
                    # Extract each GROUP 6 stat from API response using config
                    values = []
                    for stat_name, stat_cfg in group6_stats.items():
                        nba_field = stat_cfg.get('nba')
                        scale = stat_cfg.get('sc', 1)
                        raw_value = row[headers.index(nba_field)] if nba_field else 0
                        value = safe_int(raw_value, scale) if scale < 1000 else safe_float(raw_value, scale)
                        values.append(value)
                    
                    # Build record: (stat1, stat2, stat3, player_id, season_year)
                    values.extend([player_id, season_year])
                    update_records.append(tuple(values))
        
        # Bulk update using config-driven column names
        if update_records:
            set_clause = ', '.join([f"{col} = %s" for col in group6_stats.keys()])
            
            updated = 0
            for record in update_records:
                cursor.execute(f"""
                    UPDATE player_season_stats
                    SET {set_clause}, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, record)
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            log(f"GROUP 6 Hustle stats: {updated} players updated (3 columns)")
        else:
            log("WARNING - No hustle stats to update", "WARN")
        
    except Exception as e:
        log(f"Failed GROUP 6 hustle stats: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_defense_stats_bulk(season, season_year):
    """
    GROUP 7: Defense Tracking - Config-driven
    
    Uses backend_config to map 7 defense columns:
    - def_rim_fgm/fga: Rim defense (<6 ft)
    - def_fg2m/fga: Overall 2PT defense (calculated: total - fg3)
    - def_fg3m/fga: 3PT defense
    - real_def_fg_pct_x1000: Defensive FG% +/- vs expected
    
    Endpoint: LeagueDashPtDefend with 3 dribble_range calls:
    1. Overall (for FG2 and real_def_fg_pct)
    2. Less Than 6 Ft (for rim defense)
    3. 3 Pointers (for FG3 defense)
    """
    log(f"Fetching GROUP 7: Defense tracking (league-wide) for {season}...")
    
    # Get GROUP 7 stats from config
    group7_stats = get_stats_by_group(7)
    log(f" Processing {len(group7_stats)} GROUP 7 defense stats")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Store defense data: player_id -> {stat: value}
        player_data = {}
        
        # Call 1: Overall defense (for FG totals and real_def_fg_pct)
        log("  Call 1/3: Overall defense...")
        defense = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Overall'
        )
        
        result = defense.get_dict()
        
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]  # CLOSE_DEF_PERSON_ID
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                # Total D_FGM/A (will subtract FG3 to get FG2)
                player_data[player_id]['total_def_fgm'] = row[headers.index('D_FGM')] or 0
                player_data[player_id]['total_def_fga'] = row[headers.index('D_FGA')] or 0
                
                # Real defensive FG% (PCT_PLUSMINUS)
                pct_plusminus = row[headers.index('PCT_PLUSMINUS')]
                player_data[player_id]['real_def_fg_pct_x1000'] = safe_int(pct_plusminus, 1000)
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # Call 2: Rim defense (Less Than 6 Ft)
        log("  Call 2/3: Rim defense (<6 ft)...")
        defense_rim = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Less Than 6Ft'
        )
        
        result_rim = defense_rim.get_dict()
        
        for rs in result_rim['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                player_data[player_id]['def_rim_fgm'] = row[headers.index('FGM_LT_06')] or 0
                player_data[player_id]['def_rim_fga'] = row[headers.index('FGA_LT_06')] or 0
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # Call 3: 3PT defense
        log("  Call 3/3: 3PT defense...")
        defense_3pt = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='3 Pointers'
        )
        
        result_3pt = defense_3pt.get_dict()
        
        for rs in result_3pt['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                player_data[player_id]['def_fg3m'] = row[headers.index('FG3M')] or 0
                player_data[player_id]['def_fg3a'] = row[headers.index('FG3A')] or 0
        
        # Calculate def_fg2 = total - fg3 (per config calculation)
        log("  Calculating FG2 defense and updating database...")
        update_records = []
        for player_id, stats in player_data.items():
            total_fgm = stats.get('total_def_fgm', 0)
            total_fga = stats.get('total_def_fga', 0)
            def_fg3m = stats.get('def_fg3m', 0)
            def_fg3a = stats.get('def_fg3a', 0)
            
            # Calculate FG2 = Total - FG3
            def_fg2m = max(0, total_fgm - def_fg3m)
            def_fg2a = max(0, total_fga - def_fg3a)
            
            # Build record: (rim_m, rim_a, fg2m, fg2a, fg3m, fg3a, pct, player_id, year)
            record = (
                stats.get('def_rim_fgm', 0),
                stats.get('def_rim_fga', 0),
                def_fg2m,
                def_fg2a,
                def_fg3m,
                def_fg3a,
                stats.get('real_def_fg_pct_x1000', 0),
                player_id,
                season_year
            )
            update_records.append(record)
        
        # Bulk update using config-driven column names
        if update_records:
            updated = 0
            for record in update_records:
                cursor.execute("""
                    UPDATE player_season_stats
                    SET def_rim_fgm = %s, def_rim_fga = %s,
                        def_fg2m = %s, def_fg2a = %s,
                        def_fg3m = %s, def_fg3a = %s,
                        real_def_fg_pct_x1000 = %s,
                        updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, record)
                
                if cursor.rowcount > 0:
                    updated += 1
            
            conn.commit()
            log(f"GROUP 7 Defense: {updated} players updated (7 columns)")
        else:
            log("WARNING - No defense data to update", "WARN")
        
    except Exception as e:
        log(f"Failed GROUP 7 defense: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_putbacks_per_player(season, season_year):
    """
    GROUP 5: Putbacks - Per-player (no league endpoint available)
    
    Maps to: putbacks (sum of Putback + Tip shot FGM)
    Endpoint: PlayerDashboardByShootingSplits (per player, ~480 calls)
    
    RESILIENT: Implements retry logic for API instability
    - 2 attempts per player with backoff (2s, 5s)
    - Shorter timeout (20s) to fail fast on hangs
    - 1.5s delay between all requests to avoid rate limits
    - Logs each player attempt for visibility
    - Continues on failure to complete ETL
    """
    log(f"Fetching GROUP 5: Putbacks (per-player) for {season}...")
    
    # Get GROUP 5 stat from config
    group5_stats = get_stats_by_group(5)
    log(f" Processing {len(group5_stats)} GROUP 5 putback stat")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Only get active players with games played (players with 0 GP have no shot data)
    cursor.execute("""
        SELECT DISTINCT pss.player_id, p.name 
        FROM player_season_stats pss
        JOIN players p ON pss.player_id = p.player_id
        WHERE pss.year = %s AND pss.season_type = 1 AND pss.games_played > 0
        ORDER BY pss.player_id
    """, (season_year,))
    
    players = cursor.fetchall()
    
    # Get total count for context
    cursor.execute("""
        SELECT COUNT(*) FROM player_season_stats
        WHERE year = %s AND season_type = 1
    """, (season_year,))
    total_players = cursor.fetchone()[0]
    
    log(f"Found {len(players)} active players (out of {total_players} total)")
    
    updated = 0
    failed = 0
    consecutive_failures = 0
    
    for idx, (player_id, player_name) in enumerate(players):
        success = False
        putbacks_value = 0
        
        # Emergency brake: if too many consecutive failures, take a longer break
        if consecutive_failures >= 5:
            log(f"  WARNING - Taking 30s break after {consecutive_failures} consecutive failures...", "WARN")
            time.sleep(30)
            consecutive_failures = 0
        
        # Try up to 2 times with backoff (reduces total retry time)
        for attempt in range(1, 3):
            try:
                splits = playerdashboardbyshootingsplits.PlayerDashboardByShootingSplits(
                    player_id=player_id,
                    season=season,
                    measure_type_detailed='Base',
                    per_mode_detailed='Totals',
                    timeout=20  # Shorter timeout - fail fast instead of hanging
                )
                
                result = splits.get_dict()
                
                # Find ShotTypePlayerDashboard
                for rs in result['resultSets']:
                    if rs['name'] == 'ShotTypePlayerDashboard':
                        headers = rs['headers']
                        
                        putbacks_value = 0
                        for row in rs['rowSet']:
                            shot_type = row[1]  # GROUP_VALUE
                            if any(x in shot_type for x in ['Putback', 'Tip']):
                                putbacks_value += row[headers.index('FGM')]
                        
                        cursor.execute("""
                            UPDATE player_season_stats
                            SET putbacks = %s, updated_at = NOW()
                            WHERE player_id = %s AND year = %s AND season_type = 1
                        """, (putbacks_value, player_id, season_year))
                        
                        updated += 1
                        success = True
                        consecutive_failures = 0  # Reset on success
                        break
                
                if success:
                    break  # Success - exit retry loop
                else:
                    # No putback data found
                    consecutive_failures = 0  # Reset - this isn't a failure
                    break
            
            except Exception as e:
                error_msg = str(e)[:50]
                
                if attempt < 2:
                    # Retry with longer backoff (2s, 5s)
                    backoff = 2 if attempt == 1 else 5
                    time.sleep(backoff)
                else:
                    # Final attempt failed
                    failed += 1
                    consecutive_failures += 1
        
        # Rate limiting between ALL players (even successful ones)
        time.sleep(max(1.5, RATE_LIMIT_DELAY))  # At least 1.5s between requests
        update_group_progress(1)  # One player completed
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"GROUP 5 Putbacks: {updated} players updated, {failed} failed")
    
    if failed > 0:
        log(f"  WARNING - {failed} players failed after retries - continuing ETL", "WARN")


def update_onoff_stats(season, season_year):
    """
    GROUP 8: On-Off Ratings - Config-driven
    
    Uses backend_config to map 2 on-off columns:
    - tm_off_off_rating_x10: Team offensive rating when player OFF court
    - tm_off_def_rating_x10: Team defensive rating when player OFF court
    
    Endpoint: TeamPlayerOnOffDetails (per team, 30 API calls)
    Note: Calculates ratings from team stats when player is off court
    Formula: ORtg = (PTS / Poss) * 100, DRtg = (Opp PTS / Poss) * 100
    """
    log(f"Fetching GROUP 8: On-Off ratings for {season}...")
    
    # Get GROUP 8 stats from config
    group8_stats = get_stats_by_group(8)
    log(f" Processing {len(group8_stats)} GROUP 8 on-off stats")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    updated = 0
    failed = 0
    
    for team_id in TEAM_IDS.values():
        try:
            onoff = teamplayeronoffdetails.TeamPlayerOnOffDetails(
                team_id=team_id,
                season=season,
                per_mode_detailed='Totals',
                season_type_all_star='Regular Season'
            )
            
            result = onoff.get_dict()
            
            # Find the OFF court result set (player is off court)
            for rs in result['resultSets']:
                if rs['name'] == 'PlayersOffCourtTeamPlayerOnOffDetails':
                    headers = rs['headers']
                    
                    for row in rs['rowSet']:
                        player_id = row[headers.index('VS_PLAYER_ID')]
                        
                        # Get team stats when player is OFF court
                        pts = row[headers.index('PTS')] or 0
                        fga = row[headers.index('FGA')] or 0
                        fta = row[headers.index('FTA')] or 0
                        oreb = row[headers.index('OREB')] or 0
                        tov = row[headers.index('TOV')] or 0
                        plus_minus = row[headers.index('PLUS_MINUS')] or 0
                        
                        # Calculate possessions: FGA - OREB + TOV + 0.44*FTA
                        poss = fga - oreb + tov + (0.44 * fta)
                        
                        if poss > 0:
                            # Offensive rating: points per 100 possessions
                            off_rating = (pts / poss) * 100
                            
                            # Defensive rating: opponent points per 100 possessions
                            # Approximate: if team scores X and has +/- Y, opponents scored (X - Y)
                            opp_pts = pts - plus_minus
                            def_rating = (opp_pts / poss) * 100 if poss > 0 else 0
                            
                            # Scale by 10 for storage (per config)
                            tm_off_off_rating_x10 = int(off_rating * 10)
                            tm_off_def_rating_x10 = int(def_rating * 10)
                            
                            # Update using config column names
                            cursor.execute("""
                                UPDATE player_season_stats
                                SET tm_off_off_rating_x10 = %s, tm_off_def_rating_x10 = %s, updated_at = NOW()
                                WHERE player_id = %s AND year = %s AND season_type = 1
                            """, (tm_off_off_rating_x10, tm_off_def_rating_x10, player_id, season_year))
                            
                            if cursor.rowcount > 0:
                                updated += 1
            
            time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            log(f"  Failed team {team_id}: {e}", "WARN")
            failed += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"GROUP 8 On-Off: {updated} players updated, {failed} teams failed")


def update_team_shooting_tracking(season, season_year, conn=None, cursor=None):
    """
    Get team shooting tracking in 6 league-wide calls
    Maps to: cont_rim_fgm/fga, open_rim_fgm/fga, cont_fg2m/fga, open_fg2m/fga, 
             cont_fg3m/fg3a, open_fg3m/fg3a
    
    Note: Mid-range stats are calculated in frontend as fg2 - rim
    
    Args:
        conn: Optional database connection to reuse (prevents deadlocks)
        cursor: Optional cursor to reuse
    """
    log(f"Fetching team shooting tracking (league-wide) for {season}...")
    
    # Use provided connection or create new one
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        cursor = conn.cursor()
        close_conn = True
    elif cursor is None:
        cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashteamptshot
        
        # Store team shooting data: team_id -> {stat: value}
        team_data = {}
        
        # 1-2: Contested rim (0-2 ft + 2-4 ft)
        log("  Fetching contested rim shots (teams)...")
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                general_range_nullable='Less Than 10 ft',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fgm_idx = headers.index('FGM')
            fga_idx = headers.index('FGA')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                team_data[team_id]['cont_rim_fgm'] = team_data[team_id].get('cont_rim_fgm', 0) + (row[fgm_idx] or 0)
                team_data[team_id]['cont_rim_fga'] = team_data[team_id].get('cont_rim_fga', 0) + (row[fga_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # 3-4: Open rim (4-6 ft + 6+ ft)
        log("  Fetching open rim shots (teams)...")
        for def_dist in ['4-6 Feet - Open', '6+ Feet - Wide Open']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                general_range_nullable='Less Than 10 ft',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fgm_idx = headers.index('FGM')
            fga_idx = headers.index('FGA')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                team_data[team_id]['open_rim_fgm'] = team_data[team_id].get('open_rim_fgm', 0) + (row[fgm_idx] or 0)
                team_data[team_id]['open_rim_fga'] = team_data[team_id].get('open_rim_fga', 0) + (row[fga_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # 5-8: All shots contested (0-4 ft total) and open (4+ ft total) to get FG2/FG3 splits
        # Need 4 separate calls because API doesn't support comma-separated values
        log("  Fetching contested all shots (teams)...")
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fg2m_idx = headers.index('FG2M')
            fg2a_idx = headers.index('FG2A')
            fg3m_idx = headers.index('FG3M')
            fg3a_idx = headers.index('FG3A')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                # Accumulate contested totals
                team_data[team_id]['cont_fg2m_total'] = team_data[team_id].get('cont_fg2m_total', 0) + (row[fg2m_idx] or 0)
                team_data[team_id]['cont_fg2a_total'] = team_data[team_id].get('cont_fg2a_total', 0) + (row[fg2a_idx] or 0)
                team_data[team_id]['cont_fg3m'] = team_data[team_id].get('cont_fg3m', 0) + (row[fg3m_idx] or 0)
                team_data[team_id]['cont_fg3a'] = team_data[team_id].get('cont_fg3a', 0) + (row[fg3a_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        log("  Fetching open all shots (teams)...")
        for def_dist in ['4-6 Feet - Open', '6+ Feet - Wide Open']:
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                close_def_dist_range_nullable=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fg2m_idx = headers.index('FG2M')
            fg2a_idx = headers.index('FG2A')
            fg3m_idx = headers.index('FG3M')
            fg3a_idx = headers.index('FG3A')
            
            for row in rs['rowSet']:
                team_id = row[0]
                if team_id not in team_data:
                    team_data[team_id] = {}
                
                # Accumulate open totals
                team_data[team_id]['open_fg2m_total'] = team_data[team_id].get('open_fg2m_total', 0) + (row[fg2m_idx] or 0)
                team_data[team_id]['open_fg2a_total'] = team_data[team_id].get('open_fg2a_total', 0) + (row[fg2a_idx] or 0)
                team_data[team_id]['open_fg3m'] = team_data[team_id].get('open_fg3m', 0) + (row[fg3m_idx] or 0)
                team_data[team_id]['open_fg3a'] = team_data[team_id].get('open_fg3a', 0) + (row[fg3a_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Calculate mid-range: MR = All 2PT - Rim
        log(f"  Calculating mid-range and updating database (teams) - {len(team_data)} teams to process...")
        log(f"  DEBUG: close_conn={close_conn}, conn={conn is not None}, cursor={cursor is not None}")
        updated = 0
        for idx, (team_id, stats) in enumerate(team_data.items()):
            log(f"    [{idx+1}/{len(team_data)}] Processing team {team_id}...")
            cont_rim_fgm = stats.get('cont_rim_fgm', 0)
            cont_rim_fga = stats.get('cont_rim_fga', 0)
            open_rim_fgm = stats.get('open_rim_fgm', 0)
            open_rim_fga = stats.get('open_rim_fga', 0)
            
            # Store fg2 totals directly (mr is calculated in frontend as fg2 - rim)
            cont_fg2m = stats.get('cont_fg2m_total', 0)
            cont_fg2a = stats.get('cont_fg2a_total', 0)
            open_fg2m = stats.get('open_fg2m_total', 0)
            open_fg2a = stats.get('open_fg2a_total', 0)
            
            cont_fg3m = stats.get('cont_fg3m', 0)
            cont_fg3a = stats.get('cont_fg3a', 0)
            open_fg3m = stats.get('open_fg3m', 0)
            open_fg3a = stats.get('open_fg3a', 0)
            
            cursor.execute("""
                UPDATE team_season_stats
                SET cont_rim_fgm = %s, cont_rim_fga = %s,
                    open_rim_fgm = %s, open_rim_fga = %s,
                    cont_fg2m = %s, cont_fg2a = %s,
                    open_fg2m = %s, open_fg2a = %s,
                    cont_fg3m = %s, cont_fg3a = %s,
                    open_fg3m = %s, open_fg3a = %s,
                    updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (
                cont_rim_fgm, cont_rim_fga, open_rim_fgm, open_rim_fga,
                cont_fg2m, cont_fg2a, open_fg2m, open_fg2a,
                cont_fg3m, cont_fg3a, open_fg3m, open_fg3a,
                team_id, season_year
            ))
            log(f"      UPDATE completed, rowcount={cursor.rowcount}")
            
            if cursor.rowcount > 0:
                updated += 1
        
        # Only commit if we created our own connection
        if close_conn:
            conn.commit()
        
        log(f"Team shooting tracking: {updated} teams updated")
        
    except Exception as e:
        log(f"Failed team shooting tracking: {e}", "ERROR")
    finally:
        # Only close if we created our own connection
        if close_conn:
            cursor.close()
            conn.close()


def update_team_defense_stats(season, season_year, conn=None, cursor=None):
    """
    Get team defensive stats in 3 league-wide calls
    Maps to: def_rim_fgm, def_rim_fga, def_fg2m, def_fg2a, def_fg3m, def_fg3a, real_def_fg_pct_x1000
    
    RESILIENT: Implements retry logic for API instability
    - 3 attempts per call with exponential backoff
    - 20s timeout to fail fast on hangs
    - Logs each attempt for visibility
    
    Args:
        conn: Optional database connection to reuse (prevents deadlocks)
        cursor: Optional cursor to reuse
    """
    log(f"Fetching team defensive tracking (league-wide) for {season}...")
    
    # Use provided connection or create new one
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        cursor = conn.cursor()
        close_conn = True
    elif cursor is None:
        cursor = conn.cursor()
        close_conn = False
    
    try:
        from nba_api.stats.endpoints import leaguedashptteamdefend
        
        # Store data from all 3 calls, then calculate def_fg2m/fg2a
        team_data = {}
        
        # 1. Overall defense - get total FGM/FGA and real_def_fg_pct_x1000
        log("  Call 1/3: Overall defense (with retry protection)...")
        result = resilient_api_call(
            lambda timeout: leaguedashptteamdefend.LeagueDashPtTeamDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                defense_category='Overall',
                timeout=timeout
            ).get_dict(),
            "Overall defense"
        )
        
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_fgm_total = row[headers.index('D_FGM')] or 0
                def_fga_total = row[headers.index('D_FGA')] or 0
                pct_plusminus = row[headers.index('PCT_PLUSMINUS')]
                
                real_def_fg_pct = safe_int(pct_plusminus, 1000)
                
                team_data[team_id] = {
                    'def_fgm_total': def_fgm_total,
                    'def_fga_total': def_fga_total,
                    'real_def_fg_pct': real_def_fg_pct
                }
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 2. Rim defense
        log("  Call 2/3: Rim defense (with retry protection)...")
        result_rim = resilient_api_call(
            lambda timeout: leaguedashptteamdefend.LeagueDashPtTeamDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                defense_category='Less Than 10Ft',
                timeout=timeout
            ).get_dict(),
            "Rim defense"
        )
        
        for rs in result_rim['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_rim_fgm = row[headers.index('FGM_LT_10')] or 0
                def_rim_fga = row[headers.index('FGA_LT_10')] or 0
                
                if team_id in team_data:
                    team_data[team_id]['def_rim_fgm'] = def_rim_fgm
                    team_data[team_id]['def_rim_fga'] = def_rim_fga
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 3. 3PT defense
        log("  Call 3/3: 3PT defense (with retry protection)...")
        result_3pt = resilient_api_call(
            lambda timeout: leaguedashptteamdefend.LeagueDashPtTeamDefend(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                defense_category='3 Pointers',
                timeout=timeout
            ).get_dict(),
            "3PT defense"
        )
        
        for rs in result_3pt['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_fg3m = row[headers.index('FG3M')] or 0
                def_fg3a = row[headers.index('FG3A')] or 0
                
                if team_id in team_data:
                    team_data[team_id]['def_fg3m'] = def_fg3m
                    team_data[team_id]['def_fg3a'] = def_fg3a
        
        # Now calculate def_fg2m/fg2a and update all defense stats
        log("  3PT defense API call completed successfully")
        log("  Calculating FG2 defense and preparing database updates...")
        updated = 0
        for idx, (team_id, stats) in enumerate(team_data.items()):
            log(f"    Updating team {idx+1}/{len(team_data)} (ID: {team_id})...")
            def_fgm_total = stats.get('def_fgm_total', 0)
            def_fga_total = stats.get('def_fga_total', 0)
            def_fg3m = stats.get('def_fg3m', 0)
            def_fg3a = stats.get('def_fg3a', 0)
            
            # Calculate 2PT defense as total - 3PT
            def_fg2m = def_fgm_total - def_fg3m
            def_fg2a = def_fga_total - def_fg3a

            cursor.execute("""
                UPDATE team_season_stats
                SET def_rim_fgm = %s, def_rim_fga = %s,
                    def_fg2m = %s, def_fg2a = %s,
                    def_fg3m = %s, def_fg3a = %s,
                    real_def_fg_pct_x1000 = %s,
                    updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (
                stats.get('def_rim_fgm', 0), stats.get('def_rim_fga', 0),
                def_fg2m, def_fg2a,
                def_fg3m, def_fg3a,
                stats.get('real_def_fg_pct', 0),
                team_id, season_year
            ))
            
            if cursor.rowcount > 0:
                updated += 1
        
        log(f"  Committing {updated} team defense updates to database...")
        
        # Only commit if we created our own connection
        if close_conn:
            conn.commit()
        
        log(f"Team defense stats: {updated} teams updated")
        
    except Exception as e:
        log(f"Failed team defense stats: {e}", "ERROR")
    finally:
        # Only close if we created our own connection
        if close_conn:
            cursor.close()
            conn.close()


def update_team_putbacks(season, season_year):
    """
    Get team putback stats using TeamDashboardByShootingSplits
    Maps to: putbacks (sum of Putback + Tip shot FGM)
    
    Uses ShotTypeTeamDashboard result set to get:
    - Putback Dunk Shot
    - Putback Layup Shot
    - Tip Dunk Shot
    - Tip Layup Shot
    
    Note: Requires 30 API calls (one per team)
    """
    log(f"Fetching team putbacks for {season} (per-team)...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import teamdashboardbyshootingsplits
        
        updated = 0
        failed = 0
        
        # Fetch for all 30 teams
        for team_id in TEAM_IDS.values():
            try:
                endpoint = teamdashboardbyshootingsplits.TeamDashboardByShootingSplits(
                    team_id=team_id,
                    season=season,
                    per_mode_detailed='Totals',
                    season_type_all_star='Regular Season'
                )
                
                result = endpoint.get_dict()
                
                # Find ShotTypeTeamDashboard result set
                putback_total = 0
                for rs in result['resultSets']:
                    if rs['name'] == 'ShotTypeTeamDashboard':
                        headers = rs['headers']
                        fgm_idx = headers.index('FGM')
                        
                        # Sum up all putback and tip shots
                        for row in rs['rowSet']:
                            shot_type = row[1]  # GROUP_VALUE
                            fgm = row[fgm_idx] or 0
                            
                            if any(keyword in shot_type for keyword in ['Putback', 'Tip']):
                                putback_total += fgm
                        break
                
                # Update database
                cursor.execute("""
                    UPDATE team_season_stats 
                    SET putbacks = %s, updated_at = NOW()
                    WHERE team_id = %s AND year = %s AND season_type = 1
                """, (putback_total, team_id, season_year))
                
                if cursor.rowcount > 0:
                    updated += 1
                
                time.sleep(RATE_LIMIT_DELAY)
                update_group_progress(1)  # One team completed
                
            except Exception as e:
                log(f"  Failed team {team_id}: {e}", "ERROR")
                failed += 1
        
        conn.commit()
        log(f"Team putbacks: {updated} teams updated, {failed} failed (30 API calls)")
        
    except Exception as e:
        log(f"Failed team putbacks: {e}", "ERROR")
        log(f"  Error details: {str(e)}", "ERROR")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_team_advanced_stats(season=None, season_year=None):
    """
    Update advanced tracking stats for teams
    Uses league-wide endpoints for team tracking data:
    - Shooting tracking (contested/open by zone)
    - Playmaking (pot_ast, touches)
    - Rebounding (contested rebounds)
    - Hustle stats (charges, deflections, contests)
    - Defense stats
    - Putbacks
    """
    if season is None:
        season = NBA_CONFIG['current_season']
        season_year = NBA_CONFIG['current_season_year']
    
    if season_year < 2013:
        log("SKIP - Team tracking data not available before 2013-14 season")
        return
    
    log("=" * 70)
    log("STEP 5: Updating Team Advanced Stats")
    log("=" * 70)
    
    # Check for competing ETL processes before starting
    log("Checking for competing ETL processes...")
    check_conn = get_db_connection()
    check_cursor = check_conn.cursor()
    check_cursor.execute("""
        SELECT COUNT(*) FROM pg_stat_activity 
        WHERE datname = 'the_glass_db' 
          AND application_name = 'the_glass_etl'
          AND state IN ('active', 'idle in transaction')
          AND pid != pg_backend_pid()
    """)
    competing = check_cursor.fetchone()[0]
    check_cursor.close()
    check_conn.close()
    
    if competing > 0:
        log(f"WARNING - Found {competing} other ETL process(es) running!", "WARN")
        log("  This may cause deadlocks. Consider waiting for them to finish.", "WARN")
        log("  Continuing anyway with statement timeout protection...", "WARN")
    else:
        log("No competing ETL processes found")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. SHOOTING TRACKING (6 calls) - pass connection to avoid deadlock
        update_team_shooting_tracking(season, season_year, conn=conn, cursor=cursor)
        conn.commit()  # Commit shooting tracking before next section
        log("  Shooting tracking committed")
        
        # 2. PLAYMAKING
        log(f"Fetching team playmaking data for {season}...")
        result = resilient_api_call(
            lambda timeout: leaguedashptstats.LeagueDashPtStats(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                player_or_team='Team',
                pt_measure_type='Passing',
                timeout=timeout
            ).get_dict(),
            "Team playmaking"
        )
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        pot_ast_idx = headers.index('POTENTIAL_AST')
        touches_idx = headers.index('PASSES_RECEIVED')
        
        for row in rs['rowSet']:
            team_id = row[0]
            pot_ast = row[pot_ast_idx] or 0
            touches = row[touches_idx] or 0
            
            cursor.execute("""
                UPDATE team_season_stats
                SET pot_ast = %s, touches = %s, updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (pot_ast, touches, team_id, season_year))
        
        conn.commit()  # Commit playmaking before next section
        log("  Playmaking committed")
        time.sleep(RATE_LIMIT_DELAY)
        
        # 3. REBOUNDING
        log(f"Fetching team rebounding data for {season}...")
        result = resilient_api_call(
            lambda timeout: leaguedashptstats.LeagueDashPtStats(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                player_or_team='Team',
                pt_measure_type='Rebounding',
                timeout=timeout
            ).get_dict(),
            "Team rebounding"
        )
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        cont_oreb_idx = headers.index('OREB_CONTEST')
        cont_dreb_idx = headers.index('DREB_CONTEST')
        
        for row in rs['rowSet']:
            team_id = row[0]
            cont_oreb = row[cont_oreb_idx] or 0
            cont_dreb = row[cont_dreb_idx] or 0
            
            cursor.execute("""
                UPDATE team_season_stats
                SET cont_oreb = %s, cont_dreb = %s, updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (cont_oreb, cont_dreb, team_id, season_year))
        
        conn.commit()  # Commit rebounding before next section
        log("  Rebounding committed")
        time.sleep(RATE_LIMIT_DELAY)
        
        # 4. HUSTLE STATS
        log(f"Fetching team hustle stats for {season}...")
        result = resilient_api_call(
            lambda timeout: leaguehustlestatsteam.LeagueHustleStatsTeam(
                season=season,
                per_mode_time='Totals',
                season_type_all_star='Regular Season',
                timeout=timeout
            ).get_dict(),
            "Team hustle stats"
        )
        
        for rs in result['resultSets']:
            if rs['name'] == 'HustleStatsTeam':
                headers = rs['headers']
                
                for row in rs['rowSet']:
                    team_id = row[headers.index('TEAM_ID')]
                    charges = row[headers.index('CHARGES_DRAWN')] or 0
                    deflections = row[headers.index('DEFLECTIONS')] or 0
                    contests = row[headers.index('CONTESTED_SHOTS')] or 0
                    
                    cursor.execute("""
                        UPDATE team_season_stats
                        SET charges_drawn = %s, deflections = %s, contests = %s, updated_at = NOW()
                        WHERE team_id = %s AND year = %s AND season_type = 1
                    """, (charges, deflections, contests, team_id, season_year))
        
        conn.commit()  # Commit hustle stats before next section
        log("  Hustle stats committed")
        time.sleep(RATE_LIMIT_DELAY)
        
        # 5. DEFENSE STATS (3 calls) - pass connection to avoid deadlock
        update_team_defense_stats(season, season_year, conn=conn, cursor=cursor)
        conn.commit()  # Commit defense stats before next section
        log("  Defense stats committed")
        
        # 6. PUTBACKS (30 calls - one per team)
        update_team_putbacks(season, season_year)
        
        log("Team advanced stats updated successfully")
        
    except Exception as e:
        log(f"Failed team advanced stats: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_team_opponent_tracking(season=None, season_year=None):
    """
    Fetch opponent tracking stats for teams (opp_* columns)
    This mirrors team advanced stats but for opponent performance
    
    Maps to: opp_open_rim_fgm/fga, opp_cont_rim_fgm/fga, opp_touches, etc.
    """
    if season is None:
        season = NBA_CONFIG['current_season']
        season_year = NBA_CONFIG['current_season_year']
    
    if season_year < 2013:
        log("SKIP - Opponent tracking data not available before 2013-14 season")
        return
    
    log(f"Fetching team opponent tracking stats for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashplayerptshot
        
        # Note: NBA API doesn't have dedicated "opponent" endpoints for teams
        # Opponent stats are typically derived from defensive matchup data
        # For now, we'll fetch league-wide opponent stats and aggregate them
        
        # 1. Opponent shooting tracking (6 calls, same as player shooting)
        log("  Fetching opponent shooting tracking...")
        player_data = {}
        
        # Contested rim (0-2 ft + 2-4 ft)
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashplayerptshot.LeagueDashPlayerPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                general_range='Restricted Area',
                close_def_dist_range=def_dist
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[headers.index('TEAM_ID')]
                fgm = row[headers.index('FGM')] or 0
                fga = row[headers.index('FGA')] or 0
                
                if team_id not in player_data:
                    player_data[team_id] = {}
                player_data[team_id]['opp_cont_rim_fgm'] = player_data[team_id].get('opp_cont_rim_fgm', 0) + fgm
                player_data[team_id]['opp_cont_rim_fga'] = player_data[team_id].get('opp_cont_rim_fga', 0) + fga
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Note: Full implementation would require all 6 calls like player shooting
        # For now, we're demonstrating the pattern
        # This is commented out to avoid excessive API calls in this demonstration
        
        log("  SKIP - Opponent tracking stats require defensive matchup data not available in current endpoints")
        log("  Skipping opponent advanced tracking for now")
        
    except Exception as e:
        log(f"Failed team opponent tracking: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_player_advanced_stats(season=None, season_year=None):
    """
    OPTIMIZED ADVANCED STATS ETL
    Uses league-wide endpoints wherever possible (75% faster!)
    
    Total time: ~8-10 minutes per season (down from ~30 min)
    """
    if season is None:
        season = NBA_CONFIG['current_season']
        season_year = NBA_CONFIG['current_season_year']
    
    # Skip if before 2013-14 (tracking data not available)
    if season_year < 2013:
        log("SKIP - Tracking data not available before 2013-14 season")
        return
    
    log("=" * 70)
    log("STEP 4: Updating Player Advanced Stats (OPTIMIZED)")
    log("=" * 70)
    start_time = time.time()
    
    try:
        # PHASE 1: LEAGUE-WIDE CALLS (SUPER FAST - ~5 seconds)
        update_playmaking_bulk(season, season_year)           # 1 call, 1 sec
        update_rebounding_bulk(season, season_year)           # 1 call, 1 sec
        update_hustle_stats_bulk(season, season_year)         # 1 call, 1 sec
        update_defense_stats_bulk(season, season_year)        # 2 calls, 2 sec
        
        # PHASE 2: PER-PLAYER CALLS (required - no league endpoints available)
        update_shooting_tracking_bulk(season, season_year)    # ~480 calls, ~5 min
        update_putbacks_per_player(season, season_year)       # ~480 calls, ~5 min
        
        # PHASE 3: TEAM-BASED CALLS
        update_onoff_stats(season, season_year)               # 30 calls, 30 sec
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"ADVANCED STATS COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log(f"Advanced stats failed after {elapsed:.1f}s: {e}", "ERROR")
        raise


def run_nightly_etl(backfill_start=None, backfill_end=None, check_missing=True):
    """
    Main daily ETL orchestrator.
    Now includes advanced stats (~10 minutes total).
    
    Args:
        backfill_start: Start year for historical backfill (None = no backfill)
        backfill_end: End year for backfill (None = current season)
        check_missing: Check for missing data after update
    """
    log("=" * 70)
    log("THE GLASS - DAILY ETL STARTED")
    log("=" * 70)
    start_time = time.time()
    
    global _overall_pbar, _group_pbar
    
    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()
        
        # Calculate total transactions across all steps for accurate progress
        # These numbers are approximate based on typical workload
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get player count for accurate progress calculation
        cursor.execute("SELECT COUNT(*) FROM players")
        player_count = cursor.fetchone()[0] or 480  # Default if empty
        
        cursor.close()
        conn.close()
        
        # Transaction estimates per step:
        # 1. Rosters: 30 teams + new players (variable, use 0-20) + player updates (1 bulk)
        # 2. Player stats: 2 season types (2 API calls)
        # 3. Team stats: 3 season types × 3 API calls = 9 calls
        # 4. Player advanced: playmaking(1) + rebounding(1) + hustle(1) + defense(3) + shooting(4×players) + putbacks(players) + onoff(30)
        # 5. Team advanced: shooting(8) + playmaking(1) + rebounding(1) + hustle(1) + defense(3) + putbacks(2)
        
        rosters_tx = 30 + 10  # 30 teams + avg 10 new players
        player_stats_tx = 2
        team_stats_tx = 9
        # Player advanced: playmaking(1) + rebounding(1) + hustle(1) + defense(3) + shooting(480 players) + putbacks(480 players) + onoff(30)
        player_advanced_tx = 1 + 1 + 1 + 3 + player_count + player_count + 30  # ~1000 for 480 players (NOT 5x players!)
        team_advanced_tx = 8 + 1 + 1 + 1 + 3 + 30  # 44 operations (8 shooting + 3 defense + playmaking + rebounding + hustle + 30 putbacks)
        
        total_transactions = rosters_tx + player_stats_tx + team_stats_tx + player_advanced_tx + team_advanced_tx
        
        # Create two progress bars that stay at the bottom
        # position=0 for step (top), position=1 for overall (bottom)
        _overall_pbar = tqdm(total=total_transactions, desc="Overall ETL Progress", 
                            position=1, leave=True, unit="tx", 
                            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        _group_pbar = tqdm(total=0, desc="Initializing...", 
                          position=0, leave=True, unit="op",
                          bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        
        # STEP 1: Player Rosters
        _group_pbar.reset(total=rosters_tx)
        _group_pbar.set_description("STEP 1: Player Rosters")
        update_player_rosters()
        
        # STEP 2: Player Stats
        _group_pbar.reset(total=player_stats_tx)
        _group_pbar.set_description("STEP 2: Player Stats")
        update_player_stats()
        
        # STEP 3: Team Stats
        _group_pbar.reset(total=team_stats_tx)
        _group_pbar.set_description("STEP 3: Team Stats")
        update_team_stats()
        
        # STEP 4: Player Advanced Stats
        _group_pbar.reset(total=player_advanced_tx)
        _group_pbar.set_description("STEP 4: Player Advanced Stats")
        update_player_advanced_stats()
        
        # STEP 5: Team Advanced Stats
        _group_pbar.reset(total=team_advanced_tx)
        _group_pbar.set_description("STEP 5: Team Advanced Stats")
        update_team_advanced_stats()
        
        # Close progress bars
        _group_pbar.close()
        _overall_pbar.close()
        _overall_pbar = None
        _group_pbar = None
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"DAILY ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"DAILY ETL FAILED - {elapsed:.1f}s", "ERROR")
        log(f"Error: {e}", "ERROR")
        log("=" * 70)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='The Glass Daily ETL - Fast nightly update')
    parser.add_argument('--backfill', type=int, help='Backfill from this year (e.g., 2020 for 2019-20 season)')
    parser.add_argument('--end', type=int, help='Backfill end year (defaults to current season)')
    parser.add_argument('--no-check', action='store_true', help='Skip missing data check')
    
    args = parser.parse_args()
    
    # Check for environment variables if args not provided (for GitHub Actions)
    backfill_start = args.backfill
    backfill_end = args.end
    
    if not backfill_start and os.getenv('BACKFILL_START_YEAR'):
        try:
            backfill_start = int(os.getenv('BACKFILL_START_YEAR'))
        except (ValueError, TypeError):
            pass
    
    if not backfill_end and os.getenv('BACKFILL_END_YEAR'):
        try:
            backfill_end = int(os.getenv('BACKFILL_END_YEAR'))
        except (ValueError, TypeError):
            pass
    
    run_nightly_etl(
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        check_missing=not args.no_check
    )
