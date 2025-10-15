"""
THE GLASS - Monthly Full Players Population
Run this script on the 1st of each month to fully refresh all player data.
Fetches complete player information from NBA API including physical stats, contracts, etc.
"""

import os
import sys
import time
from datetime import datetime, date
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.static import players as static_players
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
        year_start = date(today.year, birth_date.month, birth_date.day)
        if year_start > today:
            year_start = date(today.year - 1, birth_date.month, birth_date.day)
        days_into_year = (today - year_start).days
        decimal_age = age + (days_into_year / 365.0)
        
        return round(decimal_age, 1)
    except Exception as e:
        return None

def fetch_player_details(player_id):
    """Fetch detailed player information from NBA API"""
    try:
        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        info_df = player_info.get_data_frames()[0]
        
        if info_df.empty:
            return None
        
        row = info_df.iloc[0]
        
        # Parse height (e.g., "6-7" -> 79 inches)
        height_inches = None
        height_str = row.get('HEIGHT')
        if height_str and '-' in str(height_str):
            parts = str(height_str).split('-')
            if len(parts) == 2:
                feet = int(parts[0])
                inches = int(parts[1])
                height_inches = (feet * 12) + inches
        
        # Parse weight
        weight_lbs = None
        weight_str = row.get('WEIGHT')
        if weight_str:
            try:
                weight_lbs = int(weight_str)
            except:
                pass
        
        # Get team_id (current team) - convert to Python int
        team_id = safe_int(row.get('TEAM_ID'))
        if team_id == 0:
            team_id = None  # Free agent
        
        # Calculate age from birthdate
        birth_date = row.get('BIRTHDATE')
        birth_date_str = safe_str(birth_date)
        age_decimal = calculate_age_decimal(birth_date_str)
        
        # Debug: log first birthdate to see format
        if player_id == list(static_players.get_players())[0]['id']:
            log(f"  DEBUG: First player birthdate format: '{birth_date_str}'")
        
        details = {
            'team_id': team_id,
            'first_name': safe_str(row.get('FIRST_NAME')),
            'last_name': safe_str(row.get('LAST_NAME')),
            'full_name': safe_str(row.get('DISPLAY_FIRST_LAST')),
            'height_inches': height_inches,
            'weight_lbs': weight_lbs,
            'age_decimal': age_decimal,
            'years_experience': safe_int(row.get('SEASON_EXP')),
            'jersey_number': safe_str(row.get('JERSEY')),
            'pre_nba_team': safe_str(row.get('SCHOOL')),
            'birthplace': safe_str(row.get('COUNTRY')),
            'position': safe_str(row.get('POSITION')),
            'headshot_url': f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png",
            'is_active': True,  # All players from active_players list are active
        }
        
        return details
        
    except Exception as e:
        log(f"  ✗ Error fetching details for player {player_id}: {e}")
        return None

def populate_players():
    """Populate players table with all NBA players"""
    
    log("Fetching all players from NBA API...")
    all_players = static_players.get_players()
    log(f"Found {len(all_players)} total players in NBA history")
    
    # Filter to only active players (optional - you can remove this to get all historical players)
    active_players = [p for p in all_players if p.get('is_active', False)]
    log(f"Found {len(active_players)} active players")
    
    # You can choose: process all players or just active ones
    players_to_process = active_players  # Change to all_players for full history
    
    log(f"Fetching detailed information for {len(players_to_process)} players...")
    log("This will take several minutes due to API rate limiting...")
    
    player_records = []
    successful = 0
    failed = 0
    
    for i, player in enumerate(players_to_process, 1):
        player_id = player['id']
        
        if i % 50 == 0:
            log(f"Progress: {i}/{len(players_to_process)} ({successful} successful, {failed} failed)")
        
        details = fetch_player_details(player_id)
        
        if details:
            # Combine static data with detailed data
            record = (
                player_id,  # player_id (nba_api_id becomes player_id)
                details['team_id'],
                details['first_name'],
                details['last_name'],
                details['full_name'],
                details['height_inches'],
                details['weight_lbs'],
                None,  # wingspan_inches (not available via API)
                details['age_decimal'],
                details['years_experience'],
                details['jersey_number'],
                details['pre_nba_team'],
                details['birthplace'],
                None,  # contract_summary (would need separate scraping)
                details['is_active'],
                details['position'],
                player_id,  # nba_api_id
                details['headshot_url'],
            )
            player_records.append(record)
            successful += 1
        else:
            failed += 1
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
    
    log(f"\nFetched details for {successful} players ({failed} failed)")
    
    if not player_records:
        log("No player records to insert!")
        return
    
    # Insert into database
    query = """
        INSERT INTO players (
            player_id, team_id, first_name, last_name, full_name,
            height_inches, weight_lbs, wingspan_inches, age_decimal,
            years_experience, jersey_number, pre_nba_team, birthplace,
            contract_summary, is_active, position, nba_api_id, headshot_url
        )
        VALUES %s
        ON CONFLICT (player_id) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            height_inches = EXCLUDED.height_inches,
            weight_lbs = EXCLUDED.weight_lbs,
            age_decimal = EXCLUDED.age_decimal,
            years_experience = EXCLUDED.years_experience,
            jersey_number = EXCLUDED.jersey_number,
            pre_nba_team = EXCLUDED.pre_nba_team,
            birthplace = EXCLUDED.birthplace,
            is_active = EXCLUDED.is_active,
            position = EXCLUDED.position,
            nba_api_id = EXCLUDED.nba_api_id,
            headshot_url = EXCLUDED.headshot_url,
            updated_at = CURRENT_TIMESTAMP
    """
    
    try:
        log("\nConnecting to database...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        log(f"Inserting {len(player_records)} player records...")
        with conn.cursor() as cur:
            execute_values(cur, query, player_records)
        
        conn.commit()
        log(f"✓ Successfully populated {len(player_records)} players!")
        
        conn.close()
        
    except Exception as e:
        log(f"✗ Database error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if not DB_PASSWORD:
        print("ERROR: DB_PASSWORD environment variable must be set")
        print("Usage: DB_PASSWORD='your_password' python populate_players.py")
        sys.exit(1)
    
    log("="*60)
    log("THE GLASS - Monthly Players Population")
    log("="*60)
    populate_players()
    log("="*60)
