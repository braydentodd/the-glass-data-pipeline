"""
THE GLASS - Monthly Player Details Update
Updates player information (team_id, position, height, weight, etc.) monthly.
Captures roster changes, trades, and updated player details.
"""

import os
import sys
import time
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import commonplayerinfo, commonteamroster, leaguedashplayerstats
from nba_api.stats.static import teams

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

DB_HOST = os.getenv('DB_HOST', '150.136.255.23')
DB_NAME = os.getenv('DB_NAME', 'the_glass_db')
DB_USER = os.getenv('DB_USER', 'the_glass_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

RATE_LIMIT_DELAY = 0.6

def log(message):
    """Log with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [MONTHLY-PLAYER-UPDATE] {message}")

def safe_int(value):
    """Safely convert to integer"""
    if value is None or (hasattr(value, '__len__') and len(value) == 0):
        return None
    try:
        return int(value)
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
            except:
                continue
        return None
    except Exception as e:
        log(f"Error parsing birthdate '{birth_date_str}': {e}")
        return None

def get_all_active_players():
    """Get all active players from current rosters and season stats"""
    
    all_players = set()
    
    # Get players from current season stats
    log("Fetching players from current season stats...")
    try:
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season='2024-25',
            season_type_all_star='Regular Season',
            per_mode_detailed='Totals',
            timeout=30
        )
        time.sleep(RATE_LIMIT_DELAY)
        df = stats.get_data_frames()[0]
        season_players = set(df['PLAYER_ID'].unique())
        all_players.update(season_players)
        log(f"Found {len(season_players)} players from season stats")
    except Exception as e:
        log(f"Error fetching season stats: {e}")
    
    # Get players from team rosters
    log("Fetching players from team rosters...")
    nba_teams = teams.get_teams()
    roster_players = set()
    
    for team in nba_teams:
        try:
            roster = commonteamroster.CommonTeamRoster(
                team_id=team['id'],
                season='2024-25',
                timeout=30
            )
            time.sleep(RATE_LIMIT_DELAY)
            roster_df = roster.get_data_frames()[0]
            team_players = set(roster_df['PLAYER_ID'].unique())
            roster_players.update(team_players)
        except Exception as e:
            log(f"Error fetching roster for {team['full_name']}: {e}")
            continue
    
    all_players.update(roster_players)
    log(f"Found {len(roster_players)} players from team rosters")
    
    log(f"Total unique active players: {len(all_players)}")
    return all_players

def fetch_player_details(player_id):
    """Fetch detailed player information"""
    try:
        player_info = commonplayerinfo.CommonPlayerInfo(
            player_id=player_id,
            timeout=30
        )
        time.sleep(RATE_LIMIT_DELAY)
        
        info_df = player_info.get_data_frames()[0]
        
        if info_df.empty:
            return None
        
        row = info_df.iloc[0]
        
        # Parse height (e.g., "6-7" to inches)
        height_str = str(row.get('HEIGHT', ''))
        height_inches = None
        if height_str and '-' in height_str:
            try:
                feet, inches = height_str.split('-')
                height_inches = int(feet) * 12 + int(inches)
            except:
                pass
        
        # Parse weight
        weight_str = str(row.get('WEIGHT', ''))
        weight_lbs = None
        if weight_str and weight_str.lower() != 'nan':
            try:
                weight_lbs = int(weight_str)
            except:
                pass
        
        player_data = {
            'player_id': player_id,
            'first_name': safe_str(row.get('FIRST_NAME')),
            'last_name': safe_str(row.get('LAST_NAME')),
            'display_name': safe_str(row.get('DISPLAY_FIRST_LAST')),
            'birthdate': parse_birthdate(row.get('BIRTHDATE')),
            'school': safe_str(row.get('SCHOOL')),
            'country': safe_str(row.get('COUNTRY')),
            'draft_year': safe_int(row.get('DRAFT_YEAR')),
            'draft_round': safe_int(row.get('DRAFT_ROUND')),
            'draft_number': safe_int(row.get('DRAFT_NUMBER')),
            'team_id': safe_int(row.get('TEAM_ID')),
            'jersey': safe_str(row.get('JERSEY')),
            'position': safe_str(row.get('POSITION')),
            'height_inches': height_inches,
            'weight_lbs': weight_lbs,
            'is_active': True
        }
        
        return player_data
        
    except Exception as e:
        log(f"Error fetching details for player {player_id}: {e}")
        return None

def monthly_player_update():
    """Main monthly player update process"""
    
    log("=" * 60)
    log("Starting Monthly Player Details Update")
    log("=" * 60)
    
    start_time = datetime.now()
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        log("Database connection established")
    except Exception as e:
        log(f"Failed to connect to database: {e}")
        sys.exit(1)
    
    cur = conn.cursor()
    
    # Get all active players
    active_player_ids = get_all_active_players()
    
    if not active_player_ids:
        log("No active players found")
        conn.close()
        return
    
    # Fetch and update player details
    log(f"Fetching details for {len(active_player_ids)} players...")
    
    updated_count = 0
    error_count = 0
    
    for idx, player_id in enumerate(active_player_ids, 1):
        if idx % 50 == 0:
            log(f"Progress: {idx}/{len(active_player_ids)} players processed...")
        
        player_data = fetch_player_details(player_id)
        
        if not player_data:
            error_count += 1
            continue
        
        try:
            # Update or insert player
            cur.execute("""
                INSERT INTO players (
                    player_id, first_name, last_name, display_name, birthdate,
                    school, country, draft_year, draft_round, draft_number,
                    team_id, jersey, position, height_inches, weight_lbs, is_active
                ) VALUES (
                    %(player_id)s, %(first_name)s, %(last_name)s, %(display_name)s, %(birthdate)s,
                    %(school)s, %(country)s, %(draft_year)s, %(draft_round)s, %(draft_number)s,
                    %(team_id)s, %(jersey)s, %(position)s, %(height_inches)s, %(weight_lbs)s, %(is_active)s
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    display_name = EXCLUDED.display_name,
                    birthdate = EXCLUDED.birthdate,
                    school = EXCLUDED.school,
                    country = EXCLUDED.country,
                    draft_year = EXCLUDED.draft_year,
                    draft_round = EXCLUDED.draft_round,
                    draft_number = EXCLUDED.draft_number,
                    team_id = EXCLUDED.team_id,
                    jersey = EXCLUDED.jersey,
                    position = EXCLUDED.position,
                    height_inches = EXCLUDED.height_inches,
                    weight_lbs = EXCLUDED.weight_lbs,
                    is_active = EXCLUDED.is_active,
                    updated_at = CURRENT_TIMESTAMP
            """, player_data)
            
            updated_count += 1
            
        except Exception as e:
            log(f"Error updating player {player_id}: {e}")
            error_count += 1
            continue
    
    # Commit changes
    conn.commit()
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    log("")
    log("=" * 60)
    log("Monthly Player Update Complete")
    log("=" * 60)
    log(f"Duration: {duration}")
    log(f"Players processed: {len(active_player_ids)}")
    log(f"Successfully updated: {updated_count}")
    log(f"Errors: {error_count}")
    log("=" * 60)
    
    conn.close()

if __name__ == "__main__":
    monthly_player_update()
