"""
THE GLASS - Annual ETL (Runs August 1st)
Handles annual maintenance tasks:
1. Delete players with no stats in current or previous season (cleanup)
2. Update height, weight, birthdate for ALL remaining players
3. This is the SLOW operation (~16 minutes for 640 players)

This script runs ONCE per year on August 1st, 1 hour before the daily ETL.
The daily ETL handles new players as they appear during the season.

Usage:
    python src/etl_annual.py
"""
import os
import sys
import time
import psycopg2
from datetime import datetime
from nba_api.stats.endpoints import commonplayerinfo, draftcombinestats

# Load environment variables FIRST (before importing config)
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import NBA_CONFIG, DB_CONFIG


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


def safe_int(value, scale=1):
    """Convert value to scaled integer, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return 0
    try:
        return int(float(value) * scale)
    except (ValueError, TypeError):
        return 0


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


def update_wingspan_from_combine():
    """
    Fetch wingspan data from NBA Draft Combine for all available years.
    Uses most recent measurement for each player.
    """
    log("=" * 70)
    log("STEP 1: Updating wingspan from Draft Combine data")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Dictionary to store wingspan by player_id (will keep most recent)
    player_wingspans = {}
    
    # Fetch combine data for all years (API supports 'all' parameter)
    try:
        log("Fetching all-time draft combine data...")
        combine = draftcombinestats.DraftCombineStats(season_all_time='all')
        result = combine.get_dict()
        
        rs = result['resultSets'][0]
        headers = rs['headers']
        
        season_idx = headers.index('SEASON')
        player_id_idx = headers.index('PLAYER_ID')
        wingspan_idx = headers.index('WINGSPAN')
        
        # Process all combine measurements
        for row in rs['rowSet']:
            season = row[season_idx]
            player_id = row[player_id_idx]
            wingspan = row[wingspan_idx]
            
            if wingspan and player_id:
                # Keep most recent measurement (later seasons)
                if player_id not in player_wingspans or season > player_wingspans[player_id]['season']:
                    player_wingspans[player_id] = {
                        'wingspan': wingspan,
                        'season': season
                    }
        
        log(f"Found wingspan measurements for {len(player_wingspans)} players")
        
        # Update database
        updated_count = 0
        for player_id, data in player_wingspans.items():
            cursor.execute("""
                UPDATE players
                SET wingspan_inches = %s, updated_at = NOW()
                WHERE player_id = %s
            """, (data['wingspan'], player_id))
            
            if cursor.rowcount > 0:
                updated_count += 1
        
        conn.commit()
        log(f"✓ Updated wingspan for {updated_count} players")
        
    except Exception as e:
        log(f"Failed to fetch combine data: {e}", "ERROR")
        updated_count = 0
    
    cursor.close()
    conn.close()
    
    return updated_count


def cleanup_inactive_players():
    """
    Delete players who have NO stats in the current or previous season.
    This cascades to delete their historical stats as well.
    """
    log("=" * 70)
    log("STEP 2: Cleaning up inactive players")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    current_year = NBA_CONFIG['current_season_year']
    
    # Find players with NO stats in the last 2 seasons (current + previous)
    cursor.execute("""
        SELECT p.player_id, p.name 
        FROM players p
        WHERE NOT EXISTS (
            SELECT 1 FROM player_season_stats s
            WHERE s.player_id = p.player_id
            AND s.year >= %s
        )
    """, (current_year - 1,))  # Last 2 seasons: current_year and current_year-1
    
    players_to_delete = cursor.fetchall()
    
    if players_to_delete:
        log(f"Found {len(players_to_delete)} inactive players to remove:")
        for player_id, name in players_to_delete[:10]:  # Show first 10
            log(f"  - {name} (ID: {player_id})")
        if len(players_to_delete) > 10:
            log(f"  ... and {len(players_to_delete) - 10} more")
        
        # Delete them (will cascade to player_season_stats)
        player_ids_to_delete = tuple(p[0] for p in players_to_delete)
        cursor.execute("""
            DELETE FROM players 
            WHERE player_id IN %s
        """, (player_ids_to_delete,))
        
        deleted_count = cursor.rowcount
        log(f"✓ Deleted {deleted_count} players and their stats (cascaded)")
    else:
        log("✓ No inactive players to remove")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return len(players_to_delete) if players_to_delete else 0


def update_all_player_details(name_range=None):
    """
    Fetch height, weight, birthdate for ALL players in the database.
    This is the SLOW operation (~16 minutes for 640 players).
    Only runs once per year on August 1st.
    
    Args:
        name_range: Optional tuple ('A', 'J') or ('K', 'Z') to split into batches
    """
    log("=" * 70)
    if name_range:
        log(f"STEP 3: Updating height, weight, birthdate for players {name_range[0]}-{name_range[1]}")
    else:
        log("STEP 3: Updating height, weight, birthdate for all players")
    log("=" * 70)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all players in database (optionally filtered by name range)
    if name_range:
        start_letter, end_letter = name_range
        cursor.execute("""
            SELECT player_id, name FROM players 
            WHERE UPPER(SUBSTRING(name, 1, 1)) >= %s 
            AND UPPER(SUBSTRING(name, 1, 1)) <= %s
            ORDER BY name
        """, (start_letter.upper(), end_letter.upper()))
    else:
        cursor.execute("SELECT player_id, name FROM players ORDER BY player_id")
    
    all_players = cursor.fetchall()
    
    total_players = len(all_players)
    log(f"Found {total_players} players to update")
    
    updated_count = 0
    failed_count = 0
    consecutive_failures = 0
    
    for idx, (player_id, player_name) in enumerate(all_players):
        # Take regular breaks every 50 players
        if idx > 0 and idx % 50 == 0:
            consecutive_failures = 0
        
        # If we're seeing failures, take emergency break
        if consecutive_failures >= 3:
            log("⚠ Detected API issues (3 consecutive failures), taking 2-minute emergency break...", "WARN")
            time.sleep(120)
            consecutive_failures = 0
        
        # Try to fetch details with exponential backoff
        for attempt in range(3):
            try:
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=20)
                info_df = player_info.get_data_frames()[0]
                
                if not info_df.empty:
                    info_row = info_df.iloc[0]
                    
                    # Extract height, weight, birthdate
                    height_str = safe_str(info_row.get('HEIGHT'))
                    weight = safe_int(info_row.get('WEIGHT', 0))
                    birthdate = parse_birthdate(info_row.get('BIRTHDATE'))
                    
                    height_inches = parse_height(height_str)
                    
                    # Update database
                    cursor.execute("""
                        UPDATE players
                        SET height_inches = %s,
                            weight_lbs = %s,
                            birthdate = %s,
                            updated_at = NOW()
                        WHERE player_id = %s
                    """, (height_inches, weight, birthdate, player_id))
                    
                    updated_count += 1
                    consecutive_failures = 0
                    time.sleep(0.6)
                break
                
            except Exception as e:
                consecutive_failures += 1
                if attempt >= 2:
                    log(f"  ✗ Failed to fetch details for {player_name}: {e}", "ERROR")
                    failed_count += 1
        
        # Log progress every 10 players
        if (idx + 1) % 10 == 0:
            log(f"Progress: {idx + 1}/{total_players}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"✓ Updated {updated_count}/{total_players} players")
    if failed_count > 0:
        log(f"⚠ Failed to update {failed_count} players", "WARN")
    
    return updated_count, failed_count


def run_annual_etl(name_range=None):
    """
    Main annual ETL orchestrator.
    Runs once per year on August 1st.
    
    Args:
        name_range: Optional tuple ('A', 'J') or ('K', 'Z') to split into batches
    """
    log("=" * 70)
    if name_range:
        log(f"🏀 THE GLASS - ANNUAL ETL (Players {name_range[0]}-{name_range[1]})")
    else:
        log("🏀 THE GLASS - ANNUAL ETL STARTED (August 1st)")
    log("=" * 70)
    start_time = time.time()
    
    try:
        # Step 1: Update wingspan from combine data (only on first run, not for name ranges)
        if not name_range:
            wingspan_count = update_wingspan_from_combine()
        else:
            wingspan_count = 0
            log("Skipping wingspan update (only runs on first batch)")
        
        # Step 2: Cleanup inactive players (only on first run, not for name ranges)
        if not name_range:
            deleted_count = cleanup_inactive_players()
        else:
            deleted_count = 0
            log("Skipping cleanup (only runs on first batch)")
        
        # Step 3: Update height, weight, birthdate for all remaining players
        updated_count, failed_count = update_all_player_details(name_range)
        
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"✅ ANNUAL ETL COMPLETE - {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        if not name_range:
            log(f"   Wingspan: {wingspan_count} players updated")
            log(f"   Deleted: {deleted_count} inactive players")
        log(f"   Updated: {updated_count} players")
        log(f"   Failed: {failed_count} players")
        log("=" * 70)
        
    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 70)
        log(f"❌ ANNUAL ETL FAILED - {elapsed:.1f}s", "ERROR")
        log(f"Error: {e}", "ERROR")
        log("=" * 70)
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='The Glass Annual ETL - Update player details')
    parser.add_argument('--name-range', choices=['A-J', 'K-Z'], help='Process only players in this name range')
    
    args = parser.parse_args()
    
    # Convert name range to tuple
    name_range = None
    if args.name_range == 'A-J':
        name_range = ('A', 'J')
    elif args.name_range == 'K-Z':
        name_range = ('K', 'Z')
    
    run_annual_etl(name_range)
