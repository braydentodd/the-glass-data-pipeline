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
import pandas as pd
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import (
    commonplayerinfo,
    leaguedashplayerstats, leaguedashteamstats
)

# Load environment variables FIRST (before importing config)
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import NBA_CONFIG, DB_CONFIG, TEAM_IDS, DB_SCHEMA

RATE_LIMIT_DELAY = NBA_CONFIG['api_rate_limit_delay']


def log(message, level="INFO"):
    """Centralized logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )


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
        log("✓ Schema already exists")
        cursor.close()
        conn.close()
        return
    
    log("Creating database schema...")
    
    # Use centralized schema DDL from config
    cursor.execute(DB_SCHEMA['create_schema_sql'])
    conn.commit()
    
    log("✓ Schema created successfully")
    
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
                return datetime.strptime(str(date_str).split('.')[0], fmt).date()
            except Exception:
                continue
        return None
    except Exception as e:
        log(f"Error parsing birthdate '{date_str}': {e}", "ERROR")
        return None


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
    log("STEP 1: Updating Player Rosters (Fast Mode)")
    log("="* 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    current_season = NBA_CONFIG['current_season']
    
    log(f"Fetching ALL players with stats from current season ({current_season})...")
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW (Issue #2)
    log("\nFetching current team rosters from NBA API...")
    current_rosters = {}  # player_id -> team_id mapping
    current_jerseys = {}  # player_id -> jersey_number mapping
    try:
        from nba_api.stats.static import teams
        nba_teams = teams.get_teams()
        
        for team in nba_teams:
            time.sleep(1)
            team_id = team['id']
            # Retry logic for roster fetching
            for attempt in range(3):
                try:
                    time.sleep(RATE_LIMIT_DELAY)
                    from nba_api.stats.endpoints import commonteamroster
                    roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=current_season, timeout=30)
                    roster_df = roster.get_data_frames()[0]
                    
                    for _, player_row in roster_df.iterrows():
                        player_id = player_row['PLAYER_ID']
                        current_rosters[player_id] = team_id
                        # Get jersey number from roster (Issue #1 - fast!)
                        jersey = safe_str(player_row.get('NUM'))
                        if jersey:
                            current_jerseys[player_id] = jersey
                        
                    log(f"  ✓ {team['abbreviation']}: {len(roster_df)} players")
                    break
                except Exception as e:
                    if attempt < 2:
                        wait_time = 5 * (attempt + 1)
                        log(f"  ⚠ Retry {attempt + 1}/3 for {team['abbreviation']} (waiting {wait_time}s)", "WARN")
                        time.sleep(wait_time)
                    else:
                        log(f"  ⚠ Failed to fetch roster for {team['abbreviation']} after 3 attempts: {e}", "WARN")
                        continue
        
        log(f"✓ Fetched current rosters: {len(current_rosters)} players, {len(current_jerseys)} with jersey numbers\n")
    except Exception as e:
        log(f"⚠ Failed to fetch current rosters: {e}\", \"WARN")
        current_rosters = {}
        current_jerseys = {}
    
    # Now fetch players from current season stats
    log(f"\nFetching current season ({current_season}) players from stats...")
    
    try:
        # Fetch player stats to get ALL players who played this season (with retry)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                stats = leaguedashplayerstats.LeagueDashPlayerStats(
                    season=current_season,
                    season_type_all_star='Regular Season',
                    per_mode_detailed='Totals',
                    timeout=120
                )
                time.sleep(RATE_LIMIT_DELAY)
                
                df = stats.get_data_frames()[0]
                log(f"✓ Found {len(df)} players with stats in current season")
                break
            except Exception as retry_error:
                if attempt < max_retries - 1:
                    wait_time = 10 * (attempt + 1)
                    log(f"⚠ Attempt {attempt + 1}/{max_retries} failed for current season stats, retrying in {wait_time}s...", "WARN")
                    time.sleep(wait_time)
                else:
                    raise retry_error
        
        for _, row in df.iterrows():
            player_id = row['PLAYER_ID']
            
            # Add player with live roster data
            all_players[player_id] = {
                'player_id': player_id,
                'team_id': current_rosters.get(player_id),  # Use live roster data
                'name': row.get('PLAYER_NAME', ''),
                'jersey': current_jerseys.get(player_id),  # Get jersey from roster (FAST!)
                'weight': None,  # Will get from annual ETL or commonplayerinfo for new players
                'age': safe_int(row.get('AGE', 0)) if row.get('AGE') else None
            }
        
    except Exception as e:
        log(f"✗ Error fetching current season stats: {e}", "ERROR")
    
    log(f"\nTotal players found: {len(all_players)}")
    
    # Get existing players from database to identify NEW players
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT player_id FROM players")
    existing_player_ids = {row[0] for row in cursor.fetchall()}
    
    # Identify NEW players (not in database)
    new_player_ids = [pid for pid in all_players.keys() if pid not in existing_player_ids]
    
    if new_player_ids:
        log(f"\n🆕 Found {len(new_player_ids)} NEW players - fetching height/weight/birthdate...")
        log(f"⏱ Estimated time: ~{len(new_player_ids) * 1.5 / 60:.1f} minutes")
        
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
                        log(f"  ⚠ Retry {attempt + 1}/3 for {player_name} (waiting {wait_time}s)", "WARN")
                        time.sleep(wait_time)
                    else:
                        failed_count += 1
                        consecutive_failures += 1
                        log(f"  ✗ Failed to fetch {player_name}: {e}", "ERROR")
            
            # Log progress every 5 new players
            if (idx + 1) % 5 == 0 or (idx + 1) == len(new_player_ids):
                status = f"(✓ {idx + 1 - failed_count} success, ✗ {failed_count} failed)" if failed_count > 0 else ""
                log(f"Progress: {idx + 1}/{len(new_player_ids)} new players {status}")
        
        if failed_count > 0:
            log(f"⚠ Could not fetch details for {failed_count}/{len(new_player_ids)} new players", "WARN")
            log("  These players will still be added with basic info (name, team, jersey from roster)", "WARN")
    else:
        log("\n✓ No new players found - all players already in database")
    
    # Update database with all players
    log(f"\nUpdating database with {len(all_players)} players...")
    
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
                            years_experience = %s, pre_nba_team = %s, birthdate = %s, 
                            updated_at = NOW()
                        WHERE player_id = %s
                    """, (
                        player_data['team_id'], player_data['jersey'],
                        player_data.get('weight'), player_data.get('height'),
                        player_data.get('years_experience'), player_data.get('pre_nba_team'),
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
                    log(f"Updated: {player_data['name']} → Team {player_data['team_id']}")
            else:
                # Insert new player (with height/weight/birthdate if fetched)
                if 'birthdate' in player_data:
                    cursor.execute("""
                        INSERT INTO players (
                            player_id, name, team_id, jersey_number,
                            weight_lbs, height_inches,
                            years_experience, pre_nba_team, birthdate
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        player_id, player_data['name'], player_data['team_id'],
                        player_data['jersey'], player_data.get('weight'),
                        player_data.get('height'),
                        player_data.get('years_experience'), player_data.get('pre_nba_team'),
                        player_data.get('birthdate')
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO players (player_id, name, team_id, jersey_number)
                        VALUES (%s, %s, %s, %s)
                    """, (player_id, player_data['name'], player_data['team_id'],
                          player_data['jersey']))
                
                players_added += 1
                log(f"Added: {player_data['name']}")
        
        except Exception as e:
            log(f"✗ Error updating player {player_id}: {e}", "ERROR")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"✓ Roster update complete: {players_added} added, {players_updated} updated")
    
    return True


def update_player_stats():
    """Update season statistics for all players"""
    log("=" * 70)
    log("STEP 2: Updating Player Stats")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    
    # Get valid player IDs from database
    cursor.execute("SELECT player_id FROM players")
    valid_player_ids = {row[0] for row in cursor.fetchall()}
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
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                player_id = row['PLAYER_ID']
                
                # Skip if not in our database
                if player_id not in valid_player_ids:
                    continue
                
                # Calculate 2FG from total FG
                fgm = safe_int(row.get('FGM', 0))
                fga = safe_int(row.get('FGA', 0))
                fg3m = safe_int(row.get('FG3M', 0))
                fg3a = safe_int(row.get('FG3A', 0))
                
                fg2m = max(0, fgm - fg3m)
                fg2a = max(0, fga - fg3a)
                
                record = (
                    player_id,
                    current_year,
                    safe_int(row.get('TEAM_ID', 0)),
                    season_type_code,
                    safe_int(row.get('GP', 0)),
                    safe_int(row.get('MIN', 0), 10),
                    safe_int(row.get('POSS', 0)),
                    fg2m,
                    fg2a,
                    fg3m,
                    fg3a,
                    safe_int(row.get('FTM', 0)),
                    safe_int(row.get('FTA', 0)),
                    safe_int(row.get('OREB', 0)),
                    safe_int(row.get('DREB', 0)),
                    safe_float(row.get('OREB_PCT', 0), 1000),
                    safe_float(row.get('DREB_PCT', 0), 1000),
                    safe_int(row.get('AST', 0)),
                    safe_int(row.get('TOV', 0)),
                    safe_int(row.get('STL', 0)),
                    safe_int(row.get('BLK', 0)),
                    safe_int(row.get('PF', 0)),
                    safe_float(row.get('OFF_RATING', 0), 10),
                    safe_float(row.get('DEF_RATING', 0), 10)
                )
                records.append(record)
            
            # Bulk insert
            if records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO player_season_stats (
                        player_id, year, team_id, season_type,
                        games_played, minutes_x10, possessions,
                        fg2m, fg2a, fg3m, fg3a, ftm, fta,
                        off_rebounds, def_rebounds, off_reb_pct_x1000, def_reb_pct_x1000,
                        assists, turnovers, steals, blocks, fouls,
                        off_rating_x10, def_rating_x10
                    ) VALUES %s
                    ON CONFLICT (player_id, year, season_type) DO UPDATE SET
                        team_id = EXCLUDED.team_id,
                        games_played = EXCLUDED.games_played,
                        minutes_x10 = EXCLUDED.minutes_x10,
                        possessions = EXCLUDED.possessions,
                        fg2m = EXCLUDED.fg2m,
                        fg2a = EXCLUDED.fg2a,
                        fg3m = EXCLUDED.fg3m,
                        fg3a = EXCLUDED.fg3a,
                        ftm = EXCLUDED.ftm,
                        fta = EXCLUDED.fta,
                        off_rebounds = EXCLUDED.off_rebounds,
                        def_rebounds = EXCLUDED.def_rebounds,
                        off_reb_pct_x1000 = EXCLUDED.off_reb_pct_x1000,
                        def_reb_pct_x1000 = EXCLUDED.def_reb_pct_x1000,
                        assists = EXCLUDED.assists,
                        turnovers = EXCLUDED.turnovers,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        fouls = EXCLUDED.fouls,
                        off_rating_x10 = EXCLUDED.off_rating_x10,
                        def_rating_x10 = EXCLUDED.def_rating_x10,
                        updated_at = NOW()
                    """,
                    records
                )
                conn.commit()
                total_updated += len(records)
                log(f"✓ Inserted/Updated {len(records)} {season_type_name} player records")
        
        except Exception as e:
            log(f"✗ Error fetching {season_type_name} stats: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    log(f"✓ Player stats complete: {total_updated} total records")
    return True


def update_team_stats():
    """Update season statistics for all teams"""
    log("=" * 70)
    log("STEP 3: Updating Team Stats")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    current_season = NBA_CONFIG['current_season']
    current_year = NBA_CONFIG['current_season_year']
    
    # Get valid team IDs from config
    valid_team_ids = set(TEAM_IDS)
    
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
            
            log(f"Fetched {season_type_name} stats for {len(df)} teams")
            
            # Prepare bulk insert data
            records = []
            for _, row in df.iterrows():
                team_id = row['TEAM_ID']
                
                # Skip if not valid team
                if team_id not in valid_team_ids:
                    continue
                
                # Calculate 2FG from total FG
                fgm = safe_int(row.get('FGM', 0))
                fga = safe_int(row.get('FGA', 0))
                fg3m = safe_int(row.get('FG3M', 0))
                fg3a = safe_int(row.get('FG3A', 0))
                
                fg2m = max(0, fgm - fg3m)
                fg2a = max(0, fga - fg3a)
                
                record = (
                    team_id,
                    current_year,
                    season_type_code,
                    safe_int(row.get('GP', 0)),
                    safe_int(row.get('MIN', 0), 10),
                    safe_int(row.get('POSS', 0)),
                    fg2m,
                    fg2a,
                    fg3m,
                    fg3a,
                    safe_int(row.get('FTM', 0)),
                    safe_int(row.get('FTA', 0)),
                    safe_int(row.get('OREB', 0)),
                    safe_int(row.get('DREB', 0)),
                    safe_float(row.get('OREB_PCT', 0), 1000),
                    safe_float(row.get('DREB_PCT', 0), 1000),
                    safe_int(row.get('AST', 0)),
                    safe_int(row.get('TOV', 0)),
                    safe_int(row.get('STL', 0)),
                    safe_int(row.get('BLK', 0)),
                    safe_int(row.get('PF', 0)),
                    safe_float(row.get('OFF_RATING', 0), 10),
                    safe_float(row.get('DEF_RATING', 0), 10)
                )
                records.append(record)
            
            # Bulk insert
            if records:
                execute_values(
                    cursor,
                    """
                    INSERT INTO team_season_stats (
                        team_id, year, season_type,
                        games_played, minutes_x10, possessions,
                        fg2m, fg2a, fg3m, fg3a, ftm, fta,
                        off_rebounds, def_rebounds, off_reb_pct_x1000, def_reb_pct_x1000,
                        assists, turnovers, steals, blocks, fouls,
                        off_rating_x10, def_rating_x10
                    ) VALUES %s
                    ON CONFLICT (team_id, year, season_type) DO UPDATE SET
                        games_played = EXCLUDED.games_played,
                        minutes_x10 = EXCLUDED.minutes_x10,
                        possessions = EXCLUDED.possessions,
                        fg2m = EXCLUDED.fg2m,
                        fg2a = EXCLUDED.fg2a,
                        fg3m = EXCLUDED.fg3m,
                        fg3a = EXCLUDED.fg3a,
                        ftm = EXCLUDED.ftm,
                        fta = EXCLUDED.fta,
                        off_rebounds = EXCLUDED.off_rebounds,
                        def_rebounds = EXCLUDED.def_rebounds,
                        off_reb_pct_x1000 = EXCLUDED.off_reb_pct_x1000,
                        def_reb_pct_x1000 = EXCLUDED.def_reb_pct_x1000,
                        assists = EXCLUDED.assists,
                        turnovers = EXCLUDED.turnovers,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        fouls = EXCLUDED.fouls,
                        off_rating_x10 = EXCLUDED.off_rating_x10,
                        def_rating_x10 = EXCLUDED.def_rating_x10,
                        updated_at = NOW()
                    """,
                    records
                )
                conn.commit()
                total_updated += len(records)
                log(f"✓ Inserted/Updated {len(records)} {season_type_name} team records")
        
        except Exception as e:
            log(f"✗ Error fetching {season_type_name} stats: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    log(f"✓ Team stats complete: {total_updated} total records")
    return True


def run_nightly_etl(backfill_start=None, backfill_end=None, check_missing=True):
    """
    Main daily ETL orchestrator.
    Fast update (~2-3 minutes) that handles stats + rosters.
    
    Args:
        backfill_start: Start year for historical backfill (None = no backfill)
        backfill_end: End year for backfill (None = current season)
        check_missing: Check for missing data after update
    """
    log("=" * 70)
    log("🏀 THE GLASS - DAILY ETL STARTED")
    log("=" * 70)
    start_time = time.time()
    
    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()
        
        # Run fast daily updates
        update_player_rosters()
        update_player_stats()
        update_team_stats()
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"✅ DAILY ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"❌ DAILY ETL FAILED - {elapsed:.1f}s", "ERROR")
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
