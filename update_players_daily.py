"""
THE GLASS - Daily Player Updates (Lightweight)
Run this script daily to update only team_id and age_decimal for all players.
This is much faster than the full monthly refresh.
"""

import os
import sys
import time
from datetime import datetime, date
import psycopg2
from nba_api.stats.endpoints import commonplayerinfo

# Load environment variables from .env file if it exists
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

# Database configuration
DB_HOST = os.getenv('DB_HOST', '150.136.255.23')
DB_NAME = os.getenv('DB_NAME', 'the_glass_db')
DB_USER = os.getenv('DB_USER', 'the_glass_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# API rate limiting
RATE_LIMIT_DELAY = 0.6  # 600ms between requests

def log(message):
    """Simple logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def calculate_age_decimal(birth_date_str):
    """Calculate decimal age from birth date string"""
    if not birth_date_str or birth_date_str == '' or str(birth_date_str).lower() == 'nan':
        return None
    try:
        # Try multiple date formats
        birth_date = None
        birth_str = str(birth_date_str)
        
        # Handle ISO format with time (e.g., '1984-12-30T00:00:00')
        if 'T' in birth_str:
            birth_str = birth_str.split('T')[0]
        
        for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
            try:
                birth_date = datetime.strptime(birth_str, fmt).date()
                break
            except ValueError:
                continue
        
        if not birth_date:
            return None
        
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        # Calculate decimal portion (days into current year / 365)
        # Handle leap day birthdays (Feb 29) in non-leap years
        try:
            year_start = date(today.year, birth_date.month, birth_date.day)
        except ValueError:
            # Leap day doesn't exist in non-leap years, use Feb 28
            if birth_date.month == 2 and birth_date.day == 29:
                year_start = date(today.year, 2, 28)
            else:
                return None
        
        if year_start > today:
            try:
                year_start = date(today.year - 1, birth_date.month, birth_date.day)
            except ValueError:
                # Leap day in previous year
                if birth_date.month == 2 and birth_date.day == 29:
                    year_start = date(today.year - 1, 2, 28)
                else:
                    return None
        
        days_into_year = (today - year_start).days
        decimal_age = age + (days_into_year / 365.0)
        
        return round(decimal_age, 1)
    except Exception:
        return None

def fetch_player_team_and_age(player_id):
    """Fetch only team_id and age from NBA API (lightweight)"""
    try:
        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        info_df = player_info.get_data_frames()[0]
        
        if info_df.empty:
            return None, None
        
        row = info_df.iloc[0]
        
        # Get team_id (current team) - convert to Python int
        team_id = safe_int(row.get('TEAM_ID'))
        if team_id == 0:
            team_id = None  # Free agent
        
        # Calculate age from birthdate
        birth_date = safe_str(row.get('BIRTHDATE'))
        age_decimal = calculate_age_decimal(birth_date)
        
        return team_id, age_decimal
        
    except Exception as e:
        log(f"  ✗ Error fetching data for player {player_id}: {e}")
        return None, None

def safe_int(value):
    """Safely convert value to int, handling numpy types and None"""
    if value is None or (hasattr(value, '__len__') and len(value) == 0):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_str(value):
    """Safely convert value to str, handling None"""
    if value is None or value == '' or (hasattr(value, '__len__') and len(value) == 0):
        return None
    return str(value)

def update_players_daily():
    """Update team_id and age_decimal for all players"""
    
    # Get list of all players from database
    log("Fetching players from database...")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT player_id FROM players")
            db_player_ids = [row[0] for row in cur.fetchall()]
        
        conn.close()
        
        log(f"Found {len(db_player_ids)} players in database")
        
        if not db_player_ids:
            log("No players to update!")
            return
        
    except Exception as e:
        log(f"✗ Database error: {e}")
        sys.exit(1)
    
    # Fetch updated team_id and age_decimal for each player
    log("Fetching updated team_id and age_decimal from NBA API...")
    log("This will take several minutes due to API rate limiting...")
    
    updates = []
    successful = 0
    failed = 0
    
    for i, player_id in enumerate(db_player_ids, 1):
        if i % 50 == 0:
            log(f"Progress: {i}/{len(db_player_ids)} ({successful} successful, {failed} failed)")
        
        team_id, age_decimal = fetch_player_team_and_age(player_id)
        
        if team_id is not None or age_decimal is not None:
            updates.append((team_id, age_decimal, player_id))
            successful += 1
        else:
            failed += 1
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
    
    log(f"\nFetched updates for {successful} players ({failed} failed)")
    
    if not updates:
        log("No updates to apply!")
        return
    
    # Update database
    query = """
        UPDATE players
        SET team_id = %s,
            age_decimal = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE player_id = %s
    """
    
    try:
        log("\nConnecting to database...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        log(f"Updating {len(updates)} player records...")
        with conn.cursor() as cur:
            cur.executemany(query, updates)
        
        conn.commit()
        log(f"✓ Successfully updated {len(updates)} players!")
        
        conn.close()
        
    except Exception as e:
        log(f"✗ Database error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if not DB_PASSWORD:
        print("ERROR: DB_PASSWORD environment variable must be set")
        print("Usage: DB_PASSWORD='your_password' python update_players_daily.py")
        sys.exit(1)
    
    log("="*60)
    log("THE GLASS - Daily Player Updates")
    log("="*60)
    update_players_daily()
    log("="*60)
