"""
Quick script to update player heights from NBA API.
This fetches height data for all players already in the database.
"""

import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from nba_api.stats.endpoints import commonplayerinfo
from datetime import datetime

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def parse_height(height_str):
    """Parse height from NBA API format (6-8) to inches"""
    if not height_str or height_str == '' or height_str == 'None':
        return 0
    try:
        if '-' in str(height_str):
            feet, inches = str(height_str).split('-')
            return int(feet) * 12 + int(inches)
        else:
            return int(float(height_str))
    except (ValueError, AttributeError):
        return 0

def main():
    log("Starting height update for all players...")
    
    # Connect to database
    conn = psycopg2.connect(
        host='150.136.255.23',
        port=5432,
        database='the_glass_db',
        user='the_glass_user',
        password=os.getenv('DB_PASSWORD', '')
    )
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get all players
    cursor.execute("SELECT player_id, name FROM players ORDER BY name")
    players = cursor.fetchall()
    
    log(f"Found {len(players)} players in database")
    
    updated_count = 0
    error_count = 0
    
    for idx, player in enumerate(players):
        player_id = player['player_id']
        player_name = player['name']
        
        try:
            # Fetch player info from NBA API
            info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=30)
            time.sleep(0.6)  # Rate limiting
            
            df = info.get_data_frames()[0]
            if not df.empty:
                player_data = df.iloc[0]
                height_str = player_data.get('HEIGHT')
                height_inches = parse_height(height_str)
                
                if height_inches > 0:
                    # Update database
                    cursor.execute(
                        "UPDATE players SET height_inches = %s, updated_at = NOW() WHERE player_id = %s",
                        (height_inches, player_id)
                    )
                    conn.commit()
                    updated_count += 1
                    
                    if (idx + 1) % 10 == 0:
                        log(f"Progress: {idx + 1}/{len(players)} - Updated {player_name}: {height_str} ({height_inches} inches)")
        
        except Exception as e:
            error_count += 1
            if error_count % 10 == 0:
                log(f"Error fetching {player_name}: {e}")
    
    log(f"✅ Complete! Updated {updated_count} players, {error_count} errors")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
