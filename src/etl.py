"""
THE GLASS - Comprehensive Nightly ETL
Handles all data updates in one consolidated script:
1. Player roster updates (with deep details: birthdate, draft, school)
2. Player season statistics (current + historical)
3. Team season statistics (current + historical)
4. Optional: Historical backfill to specific date
5. Auto schema creation on first run
6. Duplicate prevention

Usage:
    python src/etl/nightly.py                    # Run current season update
    python src/etl/nightly.py --backfill 2020    # Backfill from 2020 to present
    python src/etl/nightly.py --backfill 2015 --end 2020  # Backfill 2015-2020 only
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
    commonteamroster, commonplayerinfo,
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


def parse_birthdate(birth_date_str):
    """Parse birthdate string to date"""
    if not birth_date_str or birth_date_str == '' or str(birth_date_str).lower() == 'nan':
        return None
    try:
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                return datetime.strptime(str(birth_date_str).split('.')[0], fmt).date()
            except Exception:
                continue
        return None
    except Exception as e:
        log(f"Error parsing birthdate '{birth_date_str}': {e}", "ERROR")
        return None


def season_exists(cursor, year, season_type=1):
    """Check if data already exists for a season (prevent duplicates)"""
    cursor.execute("""
        SELECT COUNT(*) FROM player_season_stats 
        WHERE year = %s AND season_type = %s
    """, (year, season_type))
    count = cursor.fetchone()[0]
    return count > 0


def update_player_rosters(include_deep_details=True):
    """
    Update player rosters from NBA API.
    Always fetches detailed info (birthdate, draft, school) for complete data.
    
    Args:
        include_deep_details: Fetch detailed info (birthdate, draft, school)
    """
    log("=" * 70)
    log("STEP 1: Updating Rosters")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    log(f"Fetching rosters for {len(TEAM_IDS)} teams...")
    
    for team_id in TEAM_IDS:
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=team_id, timeout=60)
            time.sleep(RATE_LIMIT_DELAY)
            
            df = roster.get_data_frames()[0]
            
            for _, row in df.iterrows():
                player_id = row['PLAYER_ID']
                all_players[player_id] = {
                    'player_id': player_id,
                    'team_id': team_id,
                    'name': row.get('PLAYER', ''),
                    'jersey': safe_str(row.get('NUM', '')),
                    'weight': safe_int(row.get('WEIGHT', ''))
                }
            
            log(f"✓ Team {team_id}: {len(df)} players")
            
        except Exception as e:
            log(f"✗ Error fetching team {team_id}: {e}", "ERROR")
            continue
    
    log(f"Total players: {len(all_players)}")
    
    # Fetch deep details for all players
    if include_deep_details:
        total_players = len(all_players)
        log(f"Fetching deep player details for {total_players} players...")
        log(f"This will take approximately {int(total_players * RATE_LIMIT_DELAY / 60)} minutes")
        
        for idx, player_id in enumerate(all_players.keys()):
            player_name = all_players[player_id].get('name', 'Unknown')
            
            try:
                info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=30)
                time.sleep(RATE_LIMIT_DELAY)
                
                player_df = info.get_data_frames()[0]
                if not player_df.empty:
                    pd = player_df.iloc[0]
                    all_players[player_id].update({
                        'birthdate': parse_birthdate(pd.get('BIRTHDATE')),
                        'country': safe_str(pd.get('COUNTRY')),
                        'height': safe_int(pd.get('HEIGHT')),
                        'draft_year': safe_int(pd.get('DRAFT_YEAR')),
                        'draft_round': safe_int(pd.get('DRAFT_ROUND')),
                        'draft_number': safe_int(pd.get('DRAFT_NUMBER')),
                        'school': safe_str(pd.get('SCHOOL'))
                    })
                
                # Log every 10 players to show progress
                if (idx + 1) % 10 == 0:
                    log(f"Progress: {idx + 1}/{total_players} players - Last: {player_name}")
                    
            except Exception as e:
                log(f"✗ Error fetching details for {player_name} (ID {player_id}): {e}", "ERROR")
    
    # Update database
    for player_id, player_data in all_players.items():
        try:
            cursor.execute("SELECT team_id FROM players WHERE player_id = %s", (player_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing player
                if include_deep_details and 'birthdate' in player_data:
                    cursor.execute("""
                        UPDATE players SET
                            team_id = %s, jersey_number = %s, weight_pounds = %s,
                            birthdate = %s, country = %s, draft_year = %s,
                            draft_round = %s, draft_number = %s, school = %s,
                            updated_at = NOW()
                        WHERE player_id = %s
                    """, (
                        player_data['team_id'], player_data['jersey'],
                        player_data.get('weight'),
                        player_data.get('birthdate'), player_data.get('country'),
                        player_data.get('draft_year'), player_data.get('draft_round'),
                        player_data.get('draft_number'), player_data.get('school'),
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
                # Insert new player
                if include_deep_details and 'birthdate' in player_data:
                    cursor.execute("""
                        INSERT INTO players (
                            player_id, name, team_id, team_abbreviation, jersey_number,
                            weight_pounds, birthdate, country,
                            draft_year, draft_round, draft_number, school
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        player_id, player_data['name'], player_data['team_id'], None,
                        player_data['jersey'], player_data.get('weight'),
                        player_data.get('birthdate'), player_data.get('country'),
                        player_data.get('draft_year'), player_data.get('draft_round'),
                        player_data.get('draft_number'), player_data.get('school')
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
    
    log(f"✓ Roster complete: {players_added} added, {players_updated} updated")
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
        # Check if data already exists
        if season_exists(cursor, current_year, season_type_code):
            log(f"⚠ {season_type_name} data already exists for {current_year}, skipping to avoid duplicates")
            continue
        
        try:
            # Fetch basic stats
            stats = leaguedashplayerstats.LeagueDashPlayerStats(
                season=current_season,
                season_type_all_star=season_type_name,
                per_mode_detailed='Totals',
                timeout=60
            )
            time.sleep(RATE_LIMIT_DELAY)
            
            df = stats.get_data_frames()[0]
            
            if df.empty:
                log(f"No {season_type_name} data for {current_season}")
                continue
            
            # Fetch advanced stats
            try:
                adv_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                    season=current_season,
                    season_type_all_star=season_type_name,
                    measure_type_detailed_defense='Advanced',
                    per_mode_detailed='Totals',
                    timeout=60
                )
                time.sleep(RATE_LIMIT_DELAY)
                adv_df = adv_stats.get_data_frames()[0]
                
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
    ]
    
    total_updated = 0
    
    for season_type_name, season_type_code in season_types:
        try:
            # Fetch basic stats
            stats = leaguedashteamstats.LeagueDashTeamStats(
                season=current_season,
                season_type_all_star=season_type_name,
                per_mode_detailed='Totals',
                timeout=60
            )
            time.sleep(RATE_LIMIT_DELAY)
            
            df = stats.get_data_frames()[0]
            
            if df.empty:
                log(f"No {season_type_name} data for {current_season}")
                continue
            
            # Fetch advanced stats
            try:
                adv_stats = leaguedashteamstats.LeagueDashTeamStats(
                    season=current_season,
                    season_type_all_star=season_type_name,
                    measure_type_detailed_defense='Advanced',
                    per_mode_detailed='Totals',
                    timeout=60
                )
                time.sleep(RATE_LIMIT_DELAY)
                adv_df = adv_stats.get_data_frames()[0]
                
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


def check_missing_data():
    """Check for missing historical seasons"""
    log("=" * 70)
    log("STEP 4: Checking for Missing Data")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    current_year = NBA_CONFIG['current_season_year']
    missing_seasons = []
    
    # Check last 5 seasons
    for year in range(current_year - 5, current_year + 1):
        season = f"{year-1}-{str(year)[2:]}"
        cursor.execute("SELECT COUNT(*) FROM player_season_stats WHERE year = %s", (year,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            missing_seasons.append(season)
            log(f"⚠ Missing: {season}")
        else:
            log(f"✓ {season}: {count} records")
    
    cursor.close()
    conn.close()
    
    if missing_seasons:
        log(f"⚠ Found {len(missing_seasons)} missing seasons: {missing_seasons}")
        log("💡 Run with --backfill <start_year> to fill historical data")
    else:
        log("✓ All recent seasons have data")
    
    return missing_seasons


def backfill_historical_stats(start_year, end_year=None):
    """
    Backfill historical statistics from start_year to end_year.
    Includes duplicate prevention.
    
    Args:
        start_year: Starting year (e.g., 2020 for 2019-20 season)
        end_year: Ending year (defaults to current season)
    """
    if end_year is None:
        end_year = NBA_CONFIG['current_season_year']
    
    log("=" * 70)
    log(f"BACKFILL: {start_year} to {end_year}")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get valid IDs
    cursor.execute("SELECT player_id FROM players")
    valid_player_ids = {row[0] for row in cursor.fetchall()}
    log(f"Found {len(valid_player_ids)} players in database")
    
    valid_team_ids = set(TEAM_IDS)
    log(f"Processing {len(valid_team_ids)} teams")
    
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
    ]
    
    for year in range(start_year, end_year + 1):
        season = f"{year-1}-{str(year)[2:]}"
        
        log("")
        log("=" * 70)
        log(f"Processing {season} (year={year})")
        log("=" * 70)
        
        for season_type_name, season_type_code in season_types:
            # Check if data already exists (prevent duplicates)
            if season_exists(cursor, year, season_type_code):
                log(f"✓ {season_type_name} data already exists for {year}, skipping")
                continue
            
            # Fetch player stats
            try:
                log(f"Fetching {season_type_name} player stats...")
                stats = leaguedashplayerstats.LeagueDashPlayerStats(
                    season=season,
                    season_type_all_star=season_type_name,
                    per_mode_detailed='Totals',
                    timeout=60
                )
                time.sleep(RATE_LIMIT_DELAY)
                
                df = stats.get_data_frames()[0]
                
                if df.empty:
                    log(f"No {season_type_name} player data for {season}")
                else:
                    # Fetch advanced stats
                    try:
                        adv_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                            season=season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed='Totals',
                            timeout=60
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        adv_df = adv_stats.get_data_frames()[0]
                        
                        if not adv_df.empty:
                            df = df.merge(
                                adv_df[['PLAYER_ID', 'TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'OREB_PCT', 'DREB_PCT']], 
                                on=['PLAYER_ID', 'TEAM_ID'], 
                                how='left'
                            )
                    except Exception as e:
                        log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
                    
                    # Prepare records
                    records = []
                    for _, row in df.iterrows():
                        player_id = row['PLAYER_ID']
                        
                        if player_id not in valid_player_ids:
                            continue
                        
                        fgm = safe_int(row.get('FGM', 0))
                        fga = safe_int(row.get('FGA', 0))
                        fg3m = safe_int(row.get('FG3M', 0))
                        fg3a = safe_int(row.get('FG3A', 0))
                        
                        fg2m = max(0, fgm - fg3m)
                        fg2a = max(0, fga - fg3a)
                        
                        record = (
                            player_id, year, safe_int(row.get('TEAM_ID', 0)), season_type_code,
                            safe_int(row.get('GP', 0)), safe_int(row.get('MIN', 0), 10),
                            safe_int(row.get('POSS', 0)), fg2m, fg2a, fg3m, fg3a,
                            safe_int(row.get('FTM', 0)), safe_int(row.get('FTA', 0)),
                            safe_int(row.get('OREB', 0)), safe_int(row.get('DREB', 0)),
                            safe_float(row.get('OREB_PCT', 0), 1000), safe_float(row.get('DREB_PCT', 0), 1000),
                            safe_int(row.get('AST', 0)), safe_int(row.get('TOV', 0)),
                            safe_int(row.get('STL', 0)), safe_int(row.get('BLK', 0)),
                            safe_int(row.get('PF', 0)),
                            safe_float(row.get('OFF_RATING', 0), 10), safe_float(row.get('DEF_RATING', 0), 10)
                        )
                        records.append(record)
                    
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
                            ON CONFLICT (player_id, year, season_type) DO NOTHING
                            """,
                            records
                        )
                        conn.commit()
                        log(f"✓ Inserted {len(records)} {season_type_name} player records")
            
            except Exception as e:
                log(f"✗ Error fetching {season_type_name} player stats: {e}", "ERROR")
            
            # Fetch team stats
            try:
                log(f"Fetching {season_type_name} team stats...")
                stats = leaguedashteamstats.LeagueDashTeamStats(
                    season=season,
                    season_type_all_star=season_type_name,
                    per_mode_detailed='Totals',
                    timeout=60
                )
                time.sleep(RATE_LIMIT_DELAY)
                
                df = stats.get_data_frames()[0]
                
                if df.empty:
                    log(f"No {season_type_name} team data for {season}")
                else:
                    # Fetch advanced stats
                    try:
                        adv_stats = leaguedashteamstats.LeagueDashTeamStats(
                            season=season,
                            season_type_all_star=season_type_name,
                            measure_type_detailed_defense='Advanced',
                            per_mode_detailed='Totals',
                            timeout=60
                        )
                        time.sleep(RATE_LIMIT_DELAY)
                        adv_df = adv_stats.get_data_frames()[0]
                        
                        if not adv_df.empty:
                            df = df.merge(
                                adv_df[['TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'OREB_PCT', 'DREB_PCT']], 
                                on='TEAM_ID', 
                                how='left'
                            )
                    except Exception as e:
                        log(f"Warning: Could not fetch advanced stats: {e}", "WARN")
                    
                    # Prepare records
                    records = []
                    for _, row in df.iterrows():
                        team_id = row['TEAM_ID']
                        
                        if team_id not in valid_team_ids:
                            continue
                        
                        fgm = safe_int(row.get('FGM', 0))
                        fga = safe_int(row.get('FGA', 0))
                        fg3m = safe_int(row.get('FG3M', 0))
                        fg3a = safe_int(row.get('FG3A', 0))
                        
                        fg2m = max(0, fgm - fg3m)
                        fg2a = max(0, fga - fg3a)
                        
                        record = (
                            team_id, year, season_type_code,
                            safe_int(row.get('GP', 0)), safe_int(row.get('MIN', 0), 10),
                            safe_int(row.get('POSS', 0)), fg2m, fg2a, fg3m, fg3a,
                            safe_int(row.get('FTM', 0)), safe_int(row.get('FTA', 0)),
                            safe_int(row.get('OREB', 0)), safe_int(row.get('DREB', 0)),
                            safe_float(row.get('OREB_PCT', 0), 1000), safe_float(row.get('DREB_PCT', 0), 1000),
                            safe_int(row.get('AST', 0)), safe_int(row.get('TOV', 0)),
                            safe_int(row.get('STL', 0)), safe_int(row.get('BLK', 0)),
                            safe_int(row.get('PF', 0)),
                            safe_float(row.get('OFF_RATING', 0), 10), safe_float(row.get('DEF_RATING', 0), 10)
                        )
                        records.append(record)
                    
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
                            ON CONFLICT (team_id, year, season_type) DO NOTHING
                            """,
                            records
                        )
                        conn.commit()
                        log(f"✓ Inserted {len(records)} {season_type_name} team records")
            
            except Exception as e:
                log(f"✗ Error fetching {season_type_name} team stats: {e}", "ERROR")
    
    cursor.close()
    conn.close()
    
    log("")
    log("=" * 70)
    log("✅ BACKFILL COMPLETE")
    log("=" * 70)


def run_nightly_etl(backfill_start=None, backfill_end=None, check_missing=True):
    """
    Main ETL orchestrator.
    
    Args:
        backfill_start: Start year for historical backfill (None = no backfill)
        backfill_end: End year for backfill (None = current season)
        check_missing: Check for missing data after update
    """
    log("=" * 70)
    log("🏀 THE GLASS - ETL STARTED")
    log("=" * 70)
    start_time = time.time()
    
    try:
        # Ensure schema exists (first-time setup)
        ensure_schema_exists()
        
        # Backfill historical data if requested
        if backfill_start:
            backfill_historical_stats(backfill_start, backfill_end)
        else:
            # Normal nightly update (current season only)
            update_player_rosters(include_deep_details=True)
            update_player_stats()
            update_team_stats()
        
        # Check for missing data
        if check_missing and not backfill_start:
            check_missing_data()
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"✅ ETL COMPLETE - {elapsed:.1f}s")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"❌ ETL FAILED - {elapsed:.1f}s", "ERROR")
        log(f"Error: {e}", "ERROR")
        log("=" * 70)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='The Glass ETL - Update NBA player and team data')
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
