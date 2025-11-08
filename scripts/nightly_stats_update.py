"""
THE GLASS - Nightly Stats Update Job
Streamlined production version for daily player season stats updates.
Updates current season statistics for all active players.
"""

import os
import sys
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import leaguedashplayerstats

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

# Automatically determine current NBA season based on date
# NBA season starts in October, so:
# - Oct-Dec: use current year as start (e.g., Oct 2025 → 2025-26)
# - Jan-Sep: use previous year as start (e.g., Jan 2026 → 2025-26)
def get_current_season():
    now = datetime.now()
    if now.month >= 10:  # October or later
        start_year = now.year
    else:  # January-September
        start_year = now.year - 1
    
    end_year = start_year + 1
    season_str = f"{start_year}-{str(end_year)[2:]}"  # e.g., "2025-26"
    year_int = end_year  # Database uses end year (2026 for 2025-26 season)
    
    return season_str, year_int

CURRENT_SEASON, CURRENT_YEAR = get_current_season()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [NIGHTLY-STATS] {message}")

def safe_float(value, multiplier=1):
    """Safely convert to float and apply multiplier for scaled storage"""
    if value is None or value == '' or str(value).lower() in ['nan', 'none']:
        return None
    try:
        result = float(value) * multiplier
        return int(result) if multiplier > 1 else result
    except (ValueError, TypeError):
        return None

def safe_int(value):
    """Safely convert to integer"""
    if value is None or value == '' or str(value).lower() in ['nan', 'none']:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def fetch_season_stats(season_type='Regular Season'):
    """Fetch player statistics from NBA API for a specific season type"""
    
    log(f"Fetching {CURRENT_SEASON} {season_type} statistics...")
    
    try:
        # Get basic stats
        basic_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=CURRENT_SEASON,
            season_type_all_star=season_type
        )
        basic_df = basic_stats.get_data_frames()[0]
        
        if basic_df.empty:
            log(f"No {season_type} stats available yet")
            return None
        
        time.sleep(0.6)  # Rate limiting
        
        # Get advanced stats (includes possessions, ratings, rebound percentages)
        advanced_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=CURRENT_SEASON,
            season_type_all_star=season_type,
            measure_type_detailed_defense='Advanced'
        )
        advanced_df = advanced_stats.get_data_frames()[0]
        
        # Merge on PLAYER_ID and TEAM_ID
        merged_df = basic_df.merge(
            advanced_df[['PLAYER_ID', 'TEAM_ID', 'POSS', 'OREB_PCT', 'DREB_PCT', 'OFF_RATING', 'DEF_RATING']], 
            on=['PLAYER_ID', 'TEAM_ID'], 
            how='left'
        )
        
        log(f"Retrieved {season_type} stats for {len(merged_df)} player-team combinations")
        return merged_df
        
    except Exception as e:
        log(f"Error fetching {season_type} stats: {e}")
        return None

def process_stats_data(df, season_type_code):
    """Process raw stats data into database format matching the schema"""
    
    log("Processing stats data for database insertion...")
    
    processed_records = []
    
    for _, row in df.iterrows():
        try:
            # Basic info
            player_id = safe_int(row.get('PLAYER_ID'))
            team_id = safe_int(row.get('TEAM_ID'))
            
            if not player_id:
                continue
                
            # Games and minutes (scaled by 10 for decimal precision)
            games_played = safe_int(row.get('GP'))
            minutes_x10 = safe_float(row.get('MIN'), 10)  # 25.5 minutes -> 255
            
            # Possessions (from advanced stats)
            possessions = safe_int(row.get('POSS')) or 0
            
            # Shooting stats - NBA API gives total FG, we need to calculate 2PT
            fgm_total = safe_int(row.get('FGM')) or 0
            fga_total = safe_int(row.get('FGA')) or 0
            fg3m = safe_int(row.get('FG3M')) or 0
            fg3a = safe_int(row.get('FG3A')) or 0
            
            # Calculate 2-point stats (Total FG - 3PT FG)
            fg2m = fgm_total - fg3m
            fg2a = fga_total - fg3a
            
            # Free throws
            ftm = safe_int(row.get('FTM'))
            fta = safe_int(row.get('FTA'))
            
            # Rebound percentages (scaled by 1000: 0.156 -> 156)
            off_reb_pct_x1000 = safe_float(row.get('OREB_PCT'), 1000)
            def_reb_pct_x1000 = safe_float(row.get('DREB_PCT'), 1000)
            
            # Other stats
            assists = safe_int(row.get('AST'))
            turnovers = safe_int(row.get('TOV'))
            steals = safe_int(row.get('STL'))
            blocks = safe_int(row.get('BLK'))
            fouls = safe_int(row.get('PF'))
            
            # Advanced ratings (scaled by 10: 112.3 -> 1123)
            off_rating_x10 = safe_float(row.get('OFF_RATING'), 10)
            def_rating_x10 = safe_float(row.get('DEF_RATING'), 10)
            
            record = (
                player_id,
                CURRENT_YEAR,
                team_id,
                season_type_code,
                games_played,
                minutes_x10,
                possessions,
                fg2m, fg2a,
                fg3m, fg3a,
                ftm, fta,
                off_reb_pct_x1000,
                def_reb_pct_x1000,
                assists,
                turnovers,
                steals,
                blocks,
                fouls,
                off_rating_x10,
                def_rating_x10
            )
            
            processed_records.append(record)
            
        except Exception as e:
            log(f"Error processing player {row.get('PLAYER_ID', 'unknown')}: {e}")
            continue
    
    log(f"Successfully processed {len(processed_records)} player records")
    return processed_records

def nightly_stats_update():
    """Main nightly stats update process"""
    
    log("=" * 50)
    log("Starting Nightly Stats Update")
    log("=" * 50)
    
    start_time = datetime.now()
    
    # Season types to fetch: 1=Regular Season, 2=Playoffs, 3=Play-In
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
        ('PlayIn', 3)
    ]
    
    all_processed_records = []
    
    for season_type_name, season_type_code in season_types:
        # Fetch stats for this season type
        stats_df = fetch_season_stats(season_type_name)
        if stats_df is None or stats_df.empty:
            log(f"No {season_type_name} data available (this is normal if season hasn't started yet)")
            continue
        
        # Process the data
        processed_records = process_stats_data(stats_df, season_type_code)
        if processed_records:
            all_processed_records.extend(processed_records)
            log(f"Processed {len(processed_records)} {season_type_name} records")
    
    if not all_processed_records:
        log("No valid records to insert. Exiting.")
        return False
    
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
        log(f"Database connection error: {e}")
        return False
    
    try:
        # Clear existing current season data
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM player_season_stats 
                WHERE year = %s
            """, (CURRENT_YEAR,))
            
            deleted_count = cur.rowcount
            log(f"Cleared {deleted_count} existing {CURRENT_SEASON} records")
        
        # Insert new stats data
        insert_query = """
            INSERT INTO player_season_stats (
                player_id, year, team_id, season_type, games_played, minutes_x10, possessions,
                fg2m, fg2a, fg3m, fg3a, ftm, fta,
                off_reb_pct_x1000, def_reb_pct_x1000,
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
                off_reb_pct_x1000 = EXCLUDED.off_reb_pct_x1000,
                def_reb_pct_x1000 = EXCLUDED.def_reb_pct_x1000,
                assists = EXCLUDED.assists,
                turnovers = EXCLUDED.turnovers,
                steals = EXCLUDED.steals,
                blocks = EXCLUDED.blocks,
                fouls = EXCLUDED.fouls,
                off_rating_x10 = EXCLUDED.off_rating_x10,
                def_rating_x10 = EXCLUDED.def_rating_x10,
                updated_at = CURRENT_TIMESTAMP
        """
        
        with conn.cursor() as cur:
            execute_values(cur, insert_query, all_processed_records)
        
        conn.commit()
        log(f"Successfully inserted {len(all_processed_records)} current season stat records")
        
        # Verify insertion
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM player_season_stats 
                WHERE year = %s
            """, (CURRENT_YEAR,))
            
            final_count = cur.fetchone()[0]
        
        conn.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        log("=" * 50)
        log(f"Nightly Stats Update Completed Successfully")
        log(f"Duration: {duration}")
        log(f"Season: {CURRENT_SEASON}")
        log(f"Records inserted: {len(all_processed_records)}")
        log(f"Total {CURRENT_SEASON} records in database: {final_count}")
        log("=" * 50)
        
        return True
        
    except Exception as e:
        log(f"Error during stats update: {e}")
        conn.rollback()
        conn.close()
        return False

if __name__ == "__main__":
    if not DB_PASSWORD:
        log("ERROR: DB_PASSWORD environment variable must be set")
        sys.exit(1)
    
    success = nightly_stats_update()
    sys.exit(0 if success else 1)