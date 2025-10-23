"""
THE GLASS - Nightly Player Roster Update
Adds new players and updates team_id for players who switched teams.
Runs before season stats updates to ensure all players exist.
"""

import os
import sys
import time
import psycopg2
from datetime import datetime
from nba_api.stats.static import players as nba_players
from nba_api.stats.endpoints import commonteamroster

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

def log(message):
    """Log with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [ROSTER-UPDATE] {message}")

def get_current_rosters():
    """Fetch current rosters for all 30 NBA teams"""
    
    # NBA team IDs
    team_ids = [
        1610612737,  # ATL
        1610612738,  # BOS
        1610612751,  # BKN
        1610612766,  # CHA
        1610612741,  # CHI
        1610612739,  # CLE
        1610612742,  # DAL
        1610612743,  # DEN
        1610612765,  # DET
        1610612744,  # GSW
        1610612745,  # HOU
        1610612754,  # IND
        1610612746,  # LAC
        1610612747,  # LAL
        1610612763,  # MEM
        1610612748,  # MIA
        1610612749,  # MIL
        1610612750,  # MIN
        1610612740,  # NOP
        1610612752,  # NYK
        1610612760,  # OKC
        1610612753,  # ORL
        1610612755,  # PHI
        1610612756,  # PHX
        1610612757,  # POR
        1610612758,  # SAC
        1610612759,  # SAS
        1610612761,  # TOR
        1610612762,  # UTA
        1610612764,  # WAS
    ]
    
    all_players = {}
    
    log(f"Fetching rosters for 30 NBA teams...")
    
    for team_id in team_ids:
        try:
            roster = commonteamroster.CommonTeamRoster(team_id=team_id, timeout=30)
            time.sleep(0.6)  # Rate limiting
            
            df = roster.get_data_frames()[0]
            
            for _, row in df.iterrows():
                player_id = row['PLAYER_ID']
                all_players[player_id] = {
                    'player_id': player_id,
                    'team_id': team_id,
                    'name': row.get('PLAYER', ''),
                    'jersey': row.get('NUM', ''),
                    'position': row.get('POSITION', ''),
                    'height': row.get('HEIGHT', ''),
                    'weight': row.get('WEIGHT', ''),
                    'birth_date': row.get('BIRTH_DATE', ''),
                    'age': row.get('AGE', 0),
                    'exp': row.get('EXP', ''),
                    'school': row.get('SCHOOL', '')
                }
            
            log(f"✓ Fetched {len(df)} players from team {team_id}")
            
        except Exception as e:
            log(f"Warning: Failed to fetch roster for team {team_id}: {e}")
            continue
    
    log(f"Total unique players found: {len(all_players)}")
    return all_players

def parse_height(height_str):
    """Convert height string like '6-7' to inches (79)"""
    if not height_str or height_str == '':
        return None
    try:
        parts = height_str.split('-')
        if len(parts) == 2:
            feet = int(parts[0])
            inches = int(parts[1])
            return (feet * 12) + inches
    except:
        pass
    return None

def parse_weight(weight_str):
    """Convert weight string to integer"""
    if not weight_str or weight_str == '':
        return None
    try:
        return int(float(str(weight_str).strip()))
    except:
        return None

def update_player_rosters():
    """Main function to update player rosters"""
    
    log("=" * 80)
    log("NIGHTLY PLAYER ROSTER UPDATE")
    log("=" * 80)
    
    try:
        # Get current rosters from NBA API
        current_roster = get_current_rosters()
        
        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = conn.cursor()
        log("Database connection established")
        
        # Get existing players
        cursor.execute("SELECT player_id, team_id FROM players")
        existing_players = {row[0]: row[1] for row in cursor.fetchall()}
        log(f"Found {len(existing_players)} existing players in database")
        
        new_players = []
        team_changes = []
        
        # Process each player from current rosters
        for player_id, player_data in current_roster.items():
            if player_id in existing_players:
                # Check if team changed
                old_team_id = existing_players[player_id]
                new_team_id = player_data['team_id']
                
                if old_team_id != new_team_id:
                    team_changes.append((player_id, player_data['name'], old_team_id, new_team_id))
            else:
                # New player - add to insert list
                new_players.append(player_data)
        
        # Insert new players
        if new_players:
            log(f"\nInserting {len(new_players)} new players...")
            
            insert_query = """
                INSERT INTO players (
                    player_id, team_id, name, height_inches, weight_lbs,
                    jersey_number, years_experience, pre_nba_team
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO NOTHING
            """
            
            for player in new_players:
                try:
                    cursor.execute(insert_query, (
                        player['player_id'],
                        player['team_id'],
                        player['name'],
                        parse_height(player['height']),
                        parse_weight(player['weight']),
                        player['jersey'],
                        player['exp'],
                        player['school']
                    ))
                    log(f"  + Added: {player['name']} (ID: {player['player_id']})")
                except Exception as e:
                    log(f"  ! Error adding {player['name']}: {e}")
            
            conn.commit()
            log(f"✓ Inserted {len(new_players)} new players")
        else:
            log("No new players to add")
        
        # Update team changes
        if team_changes:
            log(f"\nUpdating {len(team_changes)} team changes...")
            
            update_query = """
                UPDATE players 
                SET team_id = %s, updated_at = CURRENT_TIMESTAMP 
                WHERE player_id = %s
            """
            
            for player_id, name, old_team, new_team in team_changes:
                cursor.execute(update_query, (new_team, player_id))
                log(f"  → {name}: Team {old_team} → {new_team}")
            
            conn.commit()
            log(f"✓ Updated {len(team_changes)} team assignments")
        else:
            log("No team changes detected")
        
        cursor.close()
        conn.close()
        
        log("")
        log("=" * 80)
        log("ROSTER UPDATE COMPLETE")
        log(f"New players added: {len(new_players)}")
        log(f"Team changes: {len(team_changes)}")
        log("=" * 80)
        
    except Exception as e:
        log(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    update_player_rosters()
