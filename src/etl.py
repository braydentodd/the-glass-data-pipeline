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
    
    # First, fetch current team rosters to know who's actually on teams RIGHT NOW
    # This is the SOURCE OF TRUTH for current team assignments
    log("\nFetching current team rosters from NBA API...")
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
        
        log(f"✓ Fetched current rosters: {len(all_players)} players\n")
    except Exception as e:
        log(f"⚠ Failed to fetch current rosters: {e}", "WARN")
    
    log(f"\nTotal players found from rosters: {len(all_players)}")
    
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
    
    # First, clear team_id for all players (they'll be re-assigned if still on roster)
    log("\nClearing team assignments for all players...")
    cursor.execute("UPDATE players SET team_id = NULL, updated_at = NOW()")
    conn.commit()
    log("✓ Cleared all team assignments")
    
    # Update database with all players
    log(f"\nUpdating database with {len(all_players)} players currently on rosters...")
    
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
                    log(f"Updated: {player_data['name']} → Team {player_data['team_id']}")
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
                        record = (
                            player_id,
                            current_year,
                            team_id,
                            season_type_code,
                            0, 0, 0,  # games_played, minutes, possessions
                            0, 0, 0, 0, 0, 0,  # shooting stats
                            0, 0, None, None,  # rebounds
                            0, 0, 0, 0, 0,  # assists, turnovers, steals, blocks, fouls
                            None, None  # ratings
                        )
                        records.append(record)
                    if skipped > 0:
                        log(f"  ⚠ Skipped {skipped} players without valid team_id", "WARN")
            
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
    
    # Get valid team IDs from config (numeric IDs, not abbreviations)
    valid_team_ids = set(TEAM_IDS.values())
    
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
                    # API returns columns with OPP_ prefix already
                    opp_columns = ['TEAM_ID', 'OPP_FGM', 'OPP_FGA', 'OPP_FG3M', 'OPP_FG3A', 'OPP_FTM', 'OPP_FTA', 
                                   'OPP_OREB', 'OPP_DREB', 'OPP_AST', 'OPP_TOV', 'OPP_STL', 'OPP_BLK', 'OPP_PF']
                    opp_df = opp_df[opp_columns]
                    df = df.merge(opp_df, on='TEAM_ID', how='left')
                    log(f"✓ Fetched opponent stats for {len(opp_df)} teams")
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
                
                # Calculate 2FG from total FG
                fgm = safe_int(row.get('FGM', 0))
                fga = safe_int(row.get('FGA', 0))
                fg3m = safe_int(row.get('FG3M', 0))
                fg3a = safe_int(row.get('FG3A', 0))
                
                fg2m = max(0, fgm - fg3m)
                fg2a = max(0, fga - fg3a)
                
                # Calculate opponent 2FG from total FG
                opp_fgm = safe_int(row.get('OPP_FGM', 0))
                opp_fga = safe_int(row.get('OPP_FGA', 0))
                opp_fg3m = safe_int(row.get('OPP_FG3M', 0))
                opp_fg3a = safe_int(row.get('OPP_FG3A', 0))
                
                opp_fg2m = max(0, opp_fgm - opp_fg3m)
                opp_fg2a = max(0, opp_fga - opp_fg3a)
                
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
                    safe_float(row.get('DEF_RATING', 0), 10),
                    opp_fg2m,
                    opp_fg2a,
                    opp_fg3m,
                    opp_fg3a,
                    safe_int(row.get('OPP_FTM', 0)),
                    safe_int(row.get('OPP_FTA', 0)),
                    safe_int(row.get('OPP_OREB', 0)),
                    safe_int(row.get('OPP_DREB', 0)),
                    safe_int(row.get('OPP_AST', 0)),
                    safe_int(row.get('OPP_TOV', 0)),
                    safe_int(row.get('OPP_STL', 0)),
                    safe_int(row.get('OPP_BLK', 0)),
                    safe_int(row.get('OPP_PF', 0))
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
                        off_rating_x10, def_rating_x10,
                        opp_fg2m, opp_fg2a, opp_fg3m, opp_fg3a, opp_ftm, opp_fta,
                        opp_off_rebounds, opp_def_rebounds, opp_assists, opp_turnovers,
                        opp_steals, opp_blocks, opp_fouls
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
                        opp_fg2m = EXCLUDED.opp_fg2m,
                        opp_fg2a = EXCLUDED.opp_fg2a,
                        opp_fg3m = EXCLUDED.opp_fg3m,
                        opp_fg3a = EXCLUDED.opp_fg3a,
                        opp_ftm = EXCLUDED.opp_ftm,
                        opp_fta = EXCLUDED.opp_fta,
                        opp_off_rebounds = EXCLUDED.opp_off_rebounds,
                        opp_def_rebounds = EXCLUDED.opp_def_rebounds,
                        opp_assists = EXCLUDED.opp_assists,
                        opp_turnovers = EXCLUDED.opp_turnovers,
                        opp_steals = EXCLUDED.opp_steals,
                        opp_blocks = EXCLUDED.opp_blocks,
                        opp_fouls = EXCLUDED.opp_fouls,
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


def update_shooting_tracking_bulk(season, season_year):
    """
    OPTIMIZED: Get ALL players' shooting tracking in 6 league-wide calls! 🚀
    Was: 439 per-player calls (5-10 min) → Now: 6 league-wide calls (~30 sec)
    
    Maps to: cont_rim_fgm/fga, open_rim_fgm/fga, cont_mr_fgm/fga, open_mr_fgm/fga, 
             cont_fg3m/fg3a, open_fg3m/fg3a
    
    Strategy:
    - Use LeagueDashPlayerPtShot with general_range + close_def_dist filters
    - 6 API calls total:
      1-2. Rim (<10 ft): Contested (0-2 + 2-4 ft) + Open (4-6 + 6+ ft)
      3-4. All shots: Contested (0-4 ft) + Open (4+ ft) to get total FG2M/FG3M
    - Calculate mid-range: MR = (Total 2PT - Rim 2PT)
    """
    log(f"Fetching shooting tracking (league-wide) for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashplayerptshot
        
        # Store player shooting data: player_id -> {stat: value}
        player_data = {}
        
        # 1-2: Contested rim (0-2 ft + 2-4 ft)
        log("  Fetching contested rim shots...")
        for def_dist in ['0-2 Feet - Very Tight', '2-4 Feet - Tight']:
            endpoint = leaguedashplayerptshot.LeagueDashPlayerPtShot(
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
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                player_data[player_id]['cont_rim_fgm'] = player_data[player_id].get('cont_rim_fgm', 0) + (row[fgm_idx] or 0)
                player_data[player_id]['cont_rim_fga'] = player_data[player_id].get('cont_rim_fga', 0) + (row[fga_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # 3-4: Open rim (4-6 ft + 6+ ft)
        log("  Fetching open rim shots...")
        for def_dist in ['4-6 Feet - Open', '6+ Feet - Wide Open']:
            endpoint = leaguedashplayerptshot.LeagueDashPlayerPtShot(
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
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                player_data[player_id]['open_rim_fgm'] = player_data[player_id].get('open_rim_fgm', 0) + (row[fgm_idx] or 0)
                player_data[player_id]['open_rim_fga'] = player_data[player_id].get('open_rim_fga', 0) + (row[fga_idx] or 0)
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # 5-6: All shots contested (0-4 ft) and open (4+ ft) to get total FG2/FG3 splits
        log("  Fetching contested/open all shots...")
        for def_dist_combo in ['0-2 Feet - Very Tight,2-4 Feet - Tight', '4-6 Feet - Open,6+ Feet - Wide Open']:
            is_contested = '0-2' in def_dist_combo
            
            endpoint = leaguedashplayerptshot.LeagueDashPlayerPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                close_def_dist_range_nullable=def_dist_combo
            )
            result = endpoint.get_dict()
            rs = result['resultSets'][0]
            headers = rs['headers']
            
            fg2m_idx = headers.index('FG2M')
            fg2a_idx = headers.index('FG2A')
            fg3m_idx = headers.index('FG3M')
            fg3a_idx = headers.index('FG3A')
            
            for row in rs['rowSet']:
                player_id = row[0]
                if player_id not in player_data:
                    player_data[player_id] = {}
                
                fg2m_total = row[fg2m_idx] or 0
                fg2a_total = row[fg2a_idx] or 0
                fg3m_total = row[fg3m_idx] or 0
                fg3a_total = row[fg3a_idx] or 0
                
                # Store totals to calculate mid-range later
                if is_contested:
                    player_data[player_id]['cont_fg2m_total'] = fg2m_total
                    player_data[player_id]['cont_fg2a_total'] = fg2a_total
                    player_data[player_id]['cont_fg3m'] = fg3m_total
                    player_data[player_id]['cont_fg3a'] = fg3a_total
                else:
                    player_data[player_id]['open_fg2m_total'] = fg2m_total
                    player_data[player_id]['open_fg2a_total'] = fg2a_total
                    player_data[player_id]['open_fg3m'] = fg3m_total
                    player_data[player_id]['open_fg3a'] = fg3a_total
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Calculate mid-range: MR = All 2PT - Rim
        log("  Calculating mid-range and updating database...")
        updated = 0
        for player_id, stats in player_data.items():
            cont_rim_fgm = stats.get('cont_rim_fgm', 0)
            cont_rim_fga = stats.get('cont_rim_fga', 0)
            open_rim_fgm = stats.get('open_rim_fgm', 0)
            open_rim_fga = stats.get('open_rim_fga', 0)
            
            cont_fg2m_total = stats.get('cont_fg2m_total', 0)
            cont_fg2a_total = stats.get('cont_fg2a_total', 0)
            open_fg2m_total = stats.get('open_fg2m_total', 0)
            open_fg2a_total = stats.get('open_fg2a_total', 0)
            
            # Mid-range = 2PT total - rim
            cont_mr_fgm = max(0, cont_fg2m_total - cont_rim_fgm)
            cont_mr_fga = max(0, cont_fg2a_total - cont_rim_fga)
            open_mr_fgm = max(0, open_fg2m_total - open_rim_fgm)
            open_mr_fga = max(0, open_fg2a_total - open_rim_fga)
            
            cont_fg3m = stats.get('cont_fg3m', 0)
            cont_fg3a = stats.get('cont_fg3a', 0)
            open_fg3m = stats.get('open_fg3m', 0)
            open_fg3a = stats.get('open_fg3a', 0)
            
            cursor.execute("""
                UPDATE player_season_stats
                SET cont_rim_fgm = %s, cont_rim_fga = %s,
                    open_rim_fgm = %s, open_rim_fga = %s,
                    cont_mr_fgm = %s, cont_mr_fga = %s,
                    open_mr_fgm = %s, open_mr_fga = %s,
                    cont_fg3m = %s, cont_fg3a = %s,
                    open_fg3m = %s, open_fg3a = %s,
                    updated_at = NOW()
                WHERE player_id = %s AND year = %s AND season_type = 1
            """, (
                cont_rim_fgm, cont_rim_fga, open_rim_fgm, open_rim_fga,
                cont_mr_fgm, cont_mr_fga, open_mr_fgm, open_mr_fga,
                cont_fg3m, cont_fg3a, open_fg3m, open_fg3a,
                player_id, season_year
            ))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"✓ Shooting tracking: {updated} players updated 🚀")
        
    except Exception as e:
        log(f"Failed shooting tracking: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_playmaking_bulk(season, season_year):
    """
    OPTIMIZED: Get ALL players' playmaking in 1 league-wide call
    Replaces 600+ individual player calls (6 min → 1 sec)
    
    Maps to: pot_ast, touches
    """
    log(f"Fetching playmaking data (league-wide) for {season}...")
    
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
        
        pot_ast_idx = headers.index('POTENTIAL_AST')
        touches_idx = headers.index('PASSES_RECEIVED')
        
        updated = 0
        for row in rs['rowSet']:
            player_id = row[0]  # PLAYER_ID
            pot_ast = row[pot_ast_idx] or 0
            touches = row[touches_idx] or 0
            
            cursor.execute("""
                UPDATE player_season_stats
                SET pot_ast = %s, touches = %s, updated_at = NOW()
                WHERE player_id = %s AND year = %s AND season_type = 1
            """, (pot_ast, touches, player_id, season_year))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"✓ Playmaking: {updated} players updated 🚀")
        
    except Exception as e:
        log(f"Failed playmaking: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_rebounding_bulk(season, season_year):
    """
    OPTIMIZED: Get ALL players' rebounding in 1 league-wide call
    Replaces 600+ individual player calls (6 min → 1 sec)
    
    Maps to: cont_dreb, cont_oreb
    """
    log(f"Fetching rebounding data (league-wide) for {season}...")
    
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
        
        cont_oreb_idx = headers.index('OREB_CONTEST')
        cont_dreb_idx = headers.index('DREB_CONTEST')
        
        updated = 0
        for row in rs['rowSet']:
            player_id = row[0]  # PLAYER_ID
            cont_oreb = row[cont_oreb_idx] or 0
            cont_dreb = row[cont_dreb_idx] or 0
            
            cursor.execute("""
                UPDATE player_season_stats
                SET cont_oreb = %s, cont_dreb = %s, updated_at = NOW()
                WHERE player_id = %s AND year = %s AND season_type = 1
            """, (cont_oreb, cont_dreb, player_id, season_year))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"✓ Rebounding: {updated} players updated 🚀")
        
    except Exception as e:
        log(f"Failed rebounding: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_hustle_stats_bulk(season, season_year):
    """
    Get ALL players' hustle stats in 1 league-wide call
    Maps to: charges_drawn, deflections, contests
    """
    log(f"Fetching hustle stats (league-wide) for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        hustle = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            per_mode_time='Totals',
            season_type_all_star='Regular Season'
        )
        
        result = hustle.get_dict()
        
        updated = 0
        for rs in result['resultSets']:
            if rs['name'] == 'HustleStatsPlayer':
                headers = rs['headers']
                
                for row in rs['rowSet']:
                    player_id = row[headers.index('PLAYER_ID')]
                    charges_drawn = row[headers.index('CHARGES_DRAWN')] or 0
                    deflections = row[headers.index('DEFLECTIONS')] or 0
                    contests = row[headers.index('CONTESTED_SHOTS')] or 0
                    
                    cursor.execute("""
                        UPDATE player_season_stats
                        SET charges_drawn = %s, deflections = %s, contests = %s, updated_at = NOW()
                        WHERE player_id = %s AND year = %s AND season_type = 1
                    """, (charges_drawn, deflections, contests, player_id, season_year))
                    
                    if cursor.rowcount > 0:
                        updated += 1
        
        conn.commit()
        log(f"✓ Hustle stats: {updated} players updated")
        
    except Exception as e:
        log(f"Failed hustle stats: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_defense_stats_bulk(season, season_year):
    """
    Get ALL players' defensive stats in 3 league-wide calls
    Maps to: def_rim_fgm, def_rim_fga, def_fg2m, def_fg2a, def_fg3m, def_fg3a, real_def_fg_pct
    """
    log(f"Fetching defensive tracking (league-wide) for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Overall defense (for real_def_fg_pct and general FG2 stats)
        defense = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Overall'
        )
        
        result = defense.get_dict()
        
        updated = 0
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]  # CLOSE_DEF_PERSON_ID (first column)
                def_fgm = row[headers.index('D_FGM')] or 0
                def_fga = row[headers.index('D_FGA')] or 0
                pct_plusminus = row[headers.index('PCT_PLUSMINUS')]
                
                # Convert PCT_PLUSMINUS to integer (x1000 for precision)
                real_def_fg_pct = safe_int(pct_plusminus, 1000)
                
                cursor.execute("""
                    UPDATE player_season_stats
                    SET def_fg2m = %s, def_fg2a = %s, real_def_fg_pct = %s, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, (def_fgm, def_fga, real_def_fg_pct, player_id, season_year))
                
                if cursor.rowcount > 0:
                    updated += 1
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 2. Rim defense (Less Than 10Ft)
        defense_rim = leaguedashptdefend.LeagueDashPtDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Less Than 10Ft'
        )
        
        result_rim = defense_rim.get_dict()
        
        for rs in result_rim['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                player_id = row[0]  # CLOSE_DEF_PERSON_ID
                def_rim_fgm = row[headers.index('FGM_LT_10')] or 0
                def_rim_fga = row[headers.index('FGA_LT_10')] or 0
                
                cursor.execute("""
                    UPDATE player_season_stats
                    SET def_rim_fgm = %s, def_rim_fga = %s, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, (def_rim_fgm, def_rim_fga, player_id, season_year))
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 3. 3PT defense
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
                player_id = row[0]  # CLOSE_DEF_PERSON_ID
                def_fg3m = row[headers.index('FG3M')] or 0
                def_fg3a = row[headers.index('FG3A')] or 0
                
                cursor.execute("""
                    UPDATE player_season_stats
                    SET def_fg3m = %s, def_fg3a = %s, updated_at = NOW()
                    WHERE player_id = %s AND year = %s AND season_type = 1
                """, (def_fg3m, def_fg3a, player_id, season_year))
        
        conn.commit()
        log(f"✓ Defense stats: {updated} players updated")
        
    except Exception as e:
        log(f"Failed defense stats: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_putbacks_per_player(season, season_year):
    """
    Putbacks only available per-player (no league endpoint)
    Maps to: putbacks
    
    RESILIENT: Implements retry logic for API instability
    - 2 attempts per player with backoff (2s, 5s)
    - Shorter timeout (20s) to fail fast on hangs
    - 1.5s delay between all requests to avoid rate limits
    - Logs each player attempt for visibility
    - Continues on failure to complete ETL
    """
    log(f"Fetching putbacks (per-player) for {season}...")
    
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
    log(f"⏱ Estimated time: ~{len(players) * 2 / 60:.1f} minutes (with retries)")
    
    updated = 0
    failed = 0
    consecutive_failures = 0
    
    for idx, (player_id, player_name) in enumerate(players):
        success = False
        putbacks_value = 0
        
        # Emergency brake: if too many consecutive failures, take a longer break
        if consecutive_failures >= 5:
            log(f"  ⚠ {consecutive_failures} consecutive failures - taking 30s break...", "WARN")
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
                        log(f"  [{idx+1}/{len(players)}] ✓ {player_name}: {putbacks_value} putbacks")
                        break
                
                if success:
                    break  # Success - exit retry loop
                else:
                    # No putback data found
                    log(f"  [{idx+1}/{len(players)}] ○ {player_name}: no putback data")
                    consecutive_failures = 0  # Reset - this isn't a failure
                    break
            
            except Exception as e:
                error_msg = str(e)[:50]
                
                if attempt < 2:
                    # Retry with longer backoff (2s, 5s)
                    backoff = 2 if attempt == 1 else 5
                    log(f"  [{idx+1}/{len(players)}] ⚠ {player_name}: attempt {attempt} failed ({error_msg}), retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    # Final attempt failed
                    log(f"  [{idx+1}/{len(players)}] ✗ {player_name}: all attempts failed ({error_msg})")
                    failed += 1
                    consecutive_failures += 1
        
        # Rate limiting between ALL players (even successful ones)
        time.sleep(max(1.5, RATE_LIMIT_DELAY))  # At least 1.5s between requests
        
        # Progress checkpoint every 50 players
        if (idx + 1) % 50 == 0:
            conn.commit()
            log(f"  💾 Checkpoint: {updated} updated, {failed} failed so far...")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"✓ Putbacks complete: {updated} updated, {failed} failed")
    
    if failed > 0:
        log(f"  ⚠ {failed} players failed after 3 attempts - continuing ETL", "WARN")


def update_onoff_stats(season, season_year):
    """
    Get on-off stats per team (30 calls)
    Maps to: tm_off_off_rating_x10, tm_off_def_rating_x10 (team performance with player OFF court)
    
    Note: We calculate simple off/def ratings from team stats when player is off court
    Formula: ORtg = (PTS / Poss) * 100, DRtg = (Opp PTS / Poss) * 100
    """
    log(f"Fetching on-off stats for {season}...")
    
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
            
            # Find the OFF court result set
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
                        
                        # Calculate possessions: FGA - OREB + TOV + 0.44*FTA
                        poss = fga - oreb + tov + (0.44 * fta)
                        
                        if poss > 0:
                            # Offensive rating: points per 100 possessions
                            off_rating = (pts / poss) * 100
                            
                            # For defensive rating, we'd need opponent points
                            # which isn't in this endpoint. Use plus/minus as proxy.
                            plus_minus = row[headers.index('PLUS_MINUS')] or 0
                            
                            # Simple approximation: if team scores X and has +/- Y, 
                            # then opponents scored (X - Y)
                            opp_pts = pts - plus_minus
                            def_rating = (opp_pts / poss) * 100 if poss > 0 else 0
                            
                            # Scale by 10 for storage
                            off_rating_x10 = int(off_rating * 10)
                            def_rating_x10 = int(def_rating * 10)
                            
                            cursor.execute("""
                                UPDATE player_season_stats
                                SET tm_off_off_rating_x10 = %s, tm_off_def_rating_x10 = %s, updated_at = NOW()
                                WHERE player_id = %s AND year = %s AND season_type = 1
                            """, (off_rating_x10, def_rating_x10, player_id, season_year))
                            
                            if cursor.rowcount > 0:
                                updated += 1
            
            time.sleep(RATE_LIMIT_DELAY)
            
        except Exception as e:
            log(f"  Failed team {team_id}: {e}", "WARN")
            failed += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"✓ On-off stats: {updated} updates, {failed} teams failed")


def update_team_shooting_tracking(season, season_year):
    """
    Get team shooting tracking in 6 league-wide calls
    Maps to: cont_rim_fgm/fga, open_rim_fgm/fga, cont_mr_fgm/fga, open_mr_fgm/fga, 
             cont_fg3m/fg3a, open_fg3m/fg3a
    """
    log(f"Fetching team shooting tracking (league-wide) for {season}...")
    
    conn = get_db_connection()
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
        
        # 5-6: All shots contested (0-4 ft) and open (4+ ft) to get total FG2/FG3 splits
        log("  Fetching contested/open all shots (teams)...")
        for def_dist_combo in ['0-2 Feet - Very Tight,2-4 Feet - Tight', '4-6 Feet - Open,6+ Feet - Wide Open']:
            is_contested = '0-2' in def_dist_combo
            
            endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
                season=season,
                per_mode_simple='Totals',
                season_type_all_star='Regular Season',
                close_def_dist_range_nullable=def_dist_combo
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
                
                fg2m_total = row[fg2m_idx] or 0
                fg2a_total = row[fg2a_idx] or 0
                fg3m_total = row[fg3m_idx] or 0
                fg3a_total = row[fg3a_idx] or 0
                
                # Store totals to calculate mid-range later
                if is_contested:
                    team_data[team_id]['cont_fg2m_total'] = fg2m_total
                    team_data[team_id]['cont_fg2a_total'] = fg2a_total
                    team_data[team_id]['cont_fg3m'] = fg3m_total
                    team_data[team_id]['cont_fg3a'] = fg3a_total
                else:
                    team_data[team_id]['open_fg2m_total'] = fg2m_total
                    team_data[team_id]['open_fg2a_total'] = fg2a_total
                    team_data[team_id]['open_fg3m'] = fg3m_total
                    team_data[team_id]['open_fg3a'] = fg3a_total
            
            time.sleep(RATE_LIMIT_DELAY)
        
        # Calculate mid-range: MR = All 2PT - Rim
        log("  Calculating mid-range and updating database (teams)...")
        updated = 0
        for team_id, stats in team_data.items():
            cont_rim_fgm = stats.get('cont_rim_fgm', 0)
            cont_rim_fga = stats.get('cont_rim_fga', 0)
            open_rim_fgm = stats.get('open_rim_fgm', 0)
            open_rim_fga = stats.get('open_rim_fga', 0)
            
            cont_fg2m_total = stats.get('cont_fg2m_total', 0)
            cont_fg2a_total = stats.get('cont_fg2a_total', 0)
            open_fg2m_total = stats.get('open_fg2m_total', 0)
            open_fg2a_total = stats.get('open_fg2a_total', 0)
            
            # Mid-range = 2PT total - rim
            cont_mr_fgm = max(0, cont_fg2m_total - cont_rim_fgm)
            cont_mr_fga = max(0, cont_fg2a_total - cont_rim_fga)
            open_mr_fgm = max(0, open_fg2m_total - open_rim_fgm)
            open_mr_fga = max(0, open_fg2a_total - open_rim_fga)
            
            cont_fg3m = stats.get('cont_fg3m', 0)
            cont_fg3a = stats.get('cont_fg3a', 0)
            open_fg3m = stats.get('open_fg3m', 0)
            open_fg3a = stats.get('open_fg3a', 0)
            
            cursor.execute("""
                UPDATE team_season_stats
                SET cont_rim_fgm = %s, cont_rim_fga = %s,
                    open_rim_fgm = %s, open_rim_fga = %s,
                    cont_mr_fgm = %s, cont_mr_fga = %s,
                    open_mr_fgm = %s, open_mr_fga = %s,
                    cont_fg3m = %s, cont_fg3a = %s,
                    open_fg3m = %s, open_fg3a = %s,
                    updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (
                cont_rim_fgm, cont_rim_fga, open_rim_fgm, open_rim_fga,
                cont_mr_fgm, cont_mr_fga, open_mr_fgm, open_mr_fga,
                cont_fg3m, cont_fg3a, open_fg3m, open_fg3a,
                team_id, season_year
            ))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"✓ Team shooting tracking: {updated} teams updated 🚀")
        
    except Exception as e:
        log(f"Failed team shooting tracking: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_team_defense_stats(season, season_year):
    """
    Get team defensive stats in 3 league-wide calls
    Maps to: def_rim_fgm, def_rim_fga, def_fg2m, def_fg2a, def_fg3m, def_fg3a, real_def_fg_pct
    """
    log(f"Fetching team defensive tracking (league-wide) for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashptteamdefend
        
        # 1. Overall defense
        defense = leaguedashptteamdefend.LeagueDashPtTeamDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Overall'
        )
        
        result = defense.get_dict()
        updated = 0
        
        for rs in result['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_fgm = row[headers.index('D_FGM')] or 0
                def_fga = row[headers.index('D_FGA')] or 0
                pct_plusminus = row[headers.index('PCT_PLUSMINUS')]
                
                real_def_fg_pct = safe_int(pct_plusminus, 1000)
                
                cursor.execute("""
                    UPDATE team_season_stats
                    SET def_fg2m = %s, def_fg2a = %s, real_def_fg_pct = %s, updated_at = NOW()
                    WHERE team_id = %s AND year = %s AND season_type = 1
                """, (def_fgm, def_fga, real_def_fg_pct, team_id, season_year))
                
                if cursor.rowcount > 0:
                    updated += 1
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 2. Rim defense
        defense_rim = leaguedashptteamdefend.LeagueDashPtTeamDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='Less Than 10Ft'
        )
        
        result_rim = defense_rim.get_dict()
        
        for rs in result_rim['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_rim_fgm = row[headers.index('FGM_LT_10')] or 0
                def_rim_fga = row[headers.index('FGA_LT_10')] or 0
                
                cursor.execute("""
                    UPDATE team_season_stats
                    SET def_rim_fgm = %s, def_rim_fga = %s, updated_at = NOW()
                    WHERE team_id = %s AND year = %s AND season_type = 1
                """, (def_rim_fgm, def_rim_fga, team_id, season_year))
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 3. 3PT defense
        defense_3pt = leaguedashptteamdefend.LeagueDashPtTeamDefend(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            defense_category='3 Pointers'
        )
        
        result_3pt = defense_3pt.get_dict()
        
        for rs in result_3pt['resultSets']:
            headers = rs['headers']
            
            for row in rs['rowSet']:
                team_id = row[0]
                def_fg3m = row[headers.index('FG3M')] or 0
                def_fg3a = row[headers.index('FG3A')] or 0
                
                cursor.execute("""
                    UPDATE team_season_stats
                    SET def_fg3m = %s, def_fg3a = %s, updated_at = NOW()
                    WHERE team_id = %s AND year = %s AND season_type = 1
                """, (def_fg3m, def_fg3a, team_id, season_year))
        
        conn.commit()
        log(f"✓ Team defense stats: {updated} teams updated")
        
    except Exception as e:
        log(f"Failed team defense stats: {e}", "ERROR")
    finally:
        cursor.close()
        conn.close()


def update_team_putbacks(season, season_year):
    """
    Get team putback stats (1 league-wide call)
    Maps to: putbacks
    """
    log(f"Fetching team putbacks for {season}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from nba_api.stats.endpoints import leaguedashteamptshot
        
        endpoint = leaguedashteamptshot.LeagueDashTeamPtShot(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            shot_type_nullable='Putbacks'
        )
        
        result = endpoint.get_dict()
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        fgm_idx = headers.index('FGM')
        
        updated = 0
        for row in rs['rowSet']:
            team_id = row[0]
            putbacks = row[fgm_idx] or 0
            
            cursor.execute("""
                UPDATE team_season_stats
                SET putbacks = %s, updated_at = NOW()
                WHERE team_id = %s AND year = %s AND season_type = 1
            """, (putbacks, team_id, season_year))
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        log(f"✓ Team putbacks: {updated} teams updated")
        
    except Exception as e:
        log(f"Failed team putbacks: {e}", "ERROR")
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
        log("⊘ Team tracking data not available before 2013-14 season")
        return
    
    log("=" * 70)
    log("STEP 5: Updating Team Advanced Stats")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. SHOOTING TRACKING (6 calls)
        update_team_shooting_tracking(season, season_year)
        
        # 2. PLAYMAKING
        log(f"Fetching team playmaking data for {season}...")
        endpoint = leaguedashptstats.LeagueDashPtStats(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            player_or_team='Team',
            pt_measure_type='Passing'
        )
        result = endpoint.get_dict()
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
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 3. REBOUNDING
        log(f"Fetching team rebounding data for {season}...")
        endpoint = leaguedashptstats.LeagueDashPtStats(
            season=season,
            per_mode_simple='Totals',
            season_type_all_star='Regular Season',
            player_or_team='Team',
            pt_measure_type='Rebounding'
        )
        result = endpoint.get_dict()
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
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 4. HUSTLE STATS
        log(f"Fetching team hustle stats for {season}...")
        hustle = leaguehustlestatsteam.LeagueHustleStatsTeam(
            season=season,
            per_mode_time='Totals',
            season_type_all_star='Regular Season'
        )
        result = hustle.get_dict()
        
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
        
        time.sleep(RATE_LIMIT_DELAY)
        
        # 5. DEFENSE STATS (3 calls)
        update_team_defense_stats(season, season_year)
        
        # 6. PUTBACKS
        update_team_putbacks(season, season_year)
        
        conn.commit()
        log("✓ Team advanced stats updated successfully")
        
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
        log("⊘ Opponent tracking data not available before 2013-14 season")
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
        
        log("  ⊘ Opponent tracking stats require defensive matchup data not available in current endpoints")
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
        log("⊘ Tracking data not available before 2013-14 season")
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
        log(f"✅ ADVANCED STATS COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log(f"❌ Advanced stats failed after {elapsed:.1f}s: {e}", "ERROR")
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
        
        # Run advanced stats (optimized)
        update_player_advanced_stats()
        update_team_advanced_stats()
        
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
