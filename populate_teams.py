"""
THE GLASS - One-Time Teams Population
Run this script once to populate the teams table with all 30 NBA teams.
Only needs to be run again if teams change (expansions, relocations, etc.)
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

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

def log(message):
    """Simple logging"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def populate_teams():
    """Populate teams table with all 30 NBA teams"""
    
    # All 30 NBA teams with complete information
    teams = [
        (1610612737, 'Atlanta Hawks', 'ATL', 'Atlanta', 'East', 'Southeast', 
         'https://cdn.nba.com/logos/nba/1610612737/primary/L/logo.svg'),
        (1610612738, 'Boston Celtics', 'BOS', 'Boston', 'East', 'Atlantic',
         'https://cdn.nba.com/logos/nba/1610612738/primary/L/logo.svg'),
        (1610612751, 'Brooklyn Nets', 'BKN', 'Brooklyn', 'East', 'Atlantic',
         'https://cdn.nba.com/logos/nba/1610612751/primary/L/logo.svg'),
        (1610612766, 'Charlotte Hornets', 'CHA', 'Charlotte', 'East', 'Southeast',
         'https://cdn.nba.com/logos/nba/1610612766/primary/L/logo.svg'),
        (1610612741, 'Chicago Bulls', 'CHI', 'Chicago', 'East', 'Central',
         'https://cdn.nba.com/logos/nba/1610612741/primary/L/logo.svg'),
        (1610612739, 'Cleveland Cavaliers', 'CLE', 'Cleveland', 'East', 'Central',
         'https://cdn.nba.com/logos/nba/1610612739/primary/L/logo.svg'),
        (1610612742, 'Dallas Mavericks', 'DAL', 'Dallas', 'West', 'Southwest',
         'https://cdn.nba.com/logos/nba/1610612742/primary/L/logo.svg'),
        (1610612743, 'Denver Nuggets', 'DEN', 'Denver', 'West', 'Northwest',
         'https://cdn.nba.com/logos/nba/1610612743/primary/L/logo.svg'),
        (1610612765, 'Detroit Pistons', 'DET', 'Detroit', 'East', 'Central',
         'https://cdn.nba.com/logos/nba/1610612765/primary/L/logo.svg'),
        (1610612744, 'Golden State Warriors', 'GSW', 'Golden State', 'West', 'Pacific',
         'https://cdn.nba.com/logos/nba/1610612744/primary/L/logo.svg'),
        (1610612745, 'Houston Rockets', 'HOU', 'Houston', 'West', 'Southwest',
         'https://cdn.nba.com/logos/nba/1610612745/primary/L/logo.svg'),
        (1610612754, 'Indiana Pacers', 'IND', 'Indiana', 'East', 'Central',
         'https://cdn.nba.com/logos/nba/1610612754/primary/L/logo.svg'),
        (1610612746, 'LA Clippers', 'LAC', 'Los Angeles', 'West', 'Pacific',
         'https://cdn.nba.com/logos/nba/1610612746/primary/L/logo.svg'),
        (1610612747, 'Los Angeles Lakers', 'LAL', 'Los Angeles', 'West', 'Pacific',
         'https://cdn.nba.com/logos/nba/1610612747/primary/L/logo.svg'),
        (1610612763, 'Memphis Grizzlies', 'MEM', 'Memphis', 'West', 'Southwest',
         'https://cdn.nba.com/logos/nba/1610612763/primary/L/logo.svg'),
        (1610612748, 'Miami Heat', 'MIA', 'Miami', 'East', 'Southeast',
         'https://cdn.nba.com/logos/nba/1610612748/primary/L/logo.svg'),
        (1610612749, 'Milwaukee Bucks', 'MIL', 'Milwaukee', 'East', 'Central',
         'https://cdn.nba.com/logos/nba/1610612749/primary/L/logo.svg'),
        (1610612750, 'Minnesota Timberwolves', 'MIN', 'Minneapolis', 'West', 'Northwest',
         'https://cdn.nba.com/logos/nba/1610612750/primary/L/logo.svg'),
        (1610612740, 'New Orleans Pelicans', 'NOP', 'New Orleans', 'West', 'Southwest',
         'https://cdn.nba.com/logos/nba/1610612740/primary/L/logo.svg'),
        (1610612752, 'New York Knicks', 'NYK', 'New York', 'East', 'Atlantic',
         'https://cdn.nba.com/logos/nba/1610612752/primary/L/logo.svg'),
        (1610612760, 'Oklahoma City Thunder', 'OKC', 'Oklahoma City', 'West', 'Northwest',
         'https://cdn.nba.com/logos/nba/1610612760/primary/L/logo.svg'),
        (1610612753, 'Orlando Magic', 'ORL', 'Orlando', 'East', 'Southeast',
         'https://cdn.nba.com/logos/nba/1610612753/primary/L/logo.svg'),
        (1610612755, 'Philadelphia 76ers', 'PHI', 'Philadelphia', 'East', 'Atlantic',
         'https://cdn.nba.com/logos/nba/1610612755/primary/L/logo.svg'),
        (1610612756, 'Phoenix Suns', 'PHX', 'Phoenix', 'West', 'Pacific',
         'https://cdn.nba.com/logos/nba/1610612756/primary/L/logo.svg'),
        (1610612757, 'Portland Trail Blazers', 'POR', 'Portland', 'West', 'Northwest',
         'https://cdn.nba.com/logos/nba/1610612757/primary/L/logo.svg'),
        (1610612758, 'Sacramento Kings', 'SAC', 'Sacramento', 'West', 'Pacific',
         'https://cdn.nba.com/logos/nba/1610612758/primary/L/logo.svg'),
        (1610612759, 'San Antonio Spurs', 'SAS', 'San Antonio', 'West', 'Southwest',
         'https://cdn.nba.com/logos/nba/1610612759/primary/L/logo.svg'),
        (1610612761, 'Toronto Raptors', 'TOR', 'Toronto', 'East', 'Atlantic',
         'https://cdn.nba.com/logos/nba/1610612761/primary/L/logo.svg'),
        (1610612762, 'Utah Jazz', 'UTA', 'Utah', 'West', 'Northwest',
         'https://cdn.nba.com/logos/nba/1610612762/primary/L/logo.svg'),
        (1610612764, 'Washington Wizards', 'WAS', 'Washington', 'East', 'Southeast',
         'https://cdn.nba.com/logos/nba/1610612764/primary/L/logo.svg'),
    ]
    
    query = """
        INSERT INTO teams (team_id, team_name, team_abbr, city, conference, division, logo_url)
        VALUES %s
        ON CONFLICT (team_id) DO UPDATE SET
            team_name = EXCLUDED.team_name,
            team_abbr = EXCLUDED.team_abbr,
            city = EXCLUDED.city,
            conference = EXCLUDED.conference,
            division = EXCLUDED.division,
            logo_url = EXCLUDED.logo_url,
            updated_at = CURRENT_TIMESTAMP
    """
    
    try:
        log("Connecting to database...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        log(f"Populating teams table with {len(teams)} teams...")
        with conn.cursor() as cur:
            execute_values(cur, query, teams)
        
        conn.commit()
        log(f"✓ Successfully populated {len(teams)} teams!")
        
        conn.close()
        
    except Exception as e:
        log(f"✗ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if not DB_PASSWORD:
        print("ERROR: DB_PASSWORD environment variable must be set")
        print("Usage: DB_PASSWORD='your_password' python populate_teams.py")
        sys.exit(1)
    
    log("="*60)
    log("THE GLASS - Teams Population")
    log("="*60)
    populate_teams()
    log("="*60)
