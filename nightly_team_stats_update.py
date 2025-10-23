"""
THE GLASS - Nightly Team Stats Update Job
Updates current season team statistics for all NBA teams.
"""

import os
import sys
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import leaguedashteamstats

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

# Current season configuration
CURRENT_SEASON = '2024-25'
CURRENT_YEAR = 2025

def log(message):
    """Log with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [TEAM-STATS] {message}")

def safe_int(value, scale=1):
    """Convert value to scaled integer, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return None
    try:
        if isinstance(value, str):
            value = float(value)
        return int(round(float(value) * scale))
    except (ValueError, TypeError):
        return None

def safe_float(value, scale=1):
    """Convert value to scaled float, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return None
    try:
        if isinstance(value, str):
            value = float(value)
        return int(round(float(value) * scale))
    except (ValueError, TypeError):
        return None

def fetch_season_team_stats(season_type='Regular Season'):
    """Fetch team statistics from NBA API for a specific season type"""
    
    log(f"Fetching {CURRENT_SEASON} {season_type} team statistics...")
    
    try:
        # Get basic stats
        basic_stats = leaguedashteamstats.LeagueDashTeamStats(
            season=CURRENT_SEASON,
            season_type_all_star=season_type
        )
        basic_df = basic_stats.get_data_frames()[0]
        
        if basic_df.empty:
            log(f"No {season_type} team stats available yet")
            return None
        
        time.sleep(0.6)  # Rate limiting
        
        # Get advanced stats (includes possessions, ratings, rebound percentages)
        advanced_stats = leaguedashteamstats.LeagueDashTeamStats(
            season=CURRENT_SEASON,
            season_type_all_star=season_type,
            measure_type_detailed_defense='Advanced'
        )
        advanced_df = advanced_stats.get_data_frames()[0]
        
        # Merge on TEAM_ID
        merged_df = basic_df.merge(
            advanced_df[['TEAM_ID', 'POSS', 'OREB_PCT', 'DREB_PCT', 'OFF_RATING', 'DEF_RATING']], 
            on='TEAM_ID', 
            how='left'
        )
        
        log(f"Retrieved {season_type} stats for {len(merged_df)} teams")
        return merged_df
        
    except Exception as e:
        log(f"Error fetching {season_type} team stats: {e}")
        return None

def process_team_stats_data(df, season_type_code, valid_team_ids):
    """Process raw team stats data into database format matching the schema"""
    
    log("Processing team stats data for database insertion...")
    
    processed_records = []
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            # Basic info
            team_id = safe_int(row.get('TEAM_ID'))
            
            if not team_id:
                continue
            
            # Skip teams not in our database (filters out WNBA teams)
            if team_id not in valid_team_ids:
                skipped_count += 1
                continue
                
            # Games and minutes (scaled by 10 for decimal precision)
            games_played = safe_int(row.get('GP'))
            minutes_x10 = safe_float(row.get('MIN'), 10)  # 1615.0 minutes -> 16150
            
            # Possessions (from advanced stats)
            possessions = safe_int(row.get('POSS'))
            
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
            
            # Rebound percentages (scaled by 1000: 0.308 -> 308)
            off_reb_pct_x1000 = safe_float(row.get('OREB_PCT'), 1000)
            def_reb_pct_x1000 = safe_float(row.get('DREB_PCT'), 1000)
            
            # Other stats
            assists = safe_int(row.get('AST'))
            turnovers = safe_int(row.get('TOV'))
            steals = safe_int(row.get('STL'))
            blocks = safe_int(row.get('BLK'))
            fouls = safe_int(row.get('PF'))
            
            # Advanced ratings (scaled by 10: 96.6 -> 966)
            off_rating_x10 = safe_float(row.get('OFF_RATING'), 10)
            def_rating_x10 = safe_float(row.get('DEF_RATING'), 10)
            
            record = (
                team_id,
                CURRENT_YEAR,
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
            log(f"Error processing team {row.get('TEAM_ID', 'unknown')}: {e}")
            continue
    
    if skipped_count > 0:
        log(f"Skipped {skipped_count} non-NBA teams (WNBA, etc.)")
    log(f"Successfully processed {len(processed_records)} NBA team records")
    return processed_records

def nightly_team_stats_update():
    """Main nightly team stats update process"""
    
    log("=" * 50)
    log("Starting Nightly Team Stats Update")
    log("=" * 50)
    
    start_time = datetime.now()
    
    # Season types to fetch: 1=Regular Season, 2=Playoffs, 3=Play-In
    season_types = [
        ('Regular Season', 1),
        ('Playoffs', 2),
        ('PlayIn', 3)
    ]
    
    # Get valid NBA team IDs from database
    try:
        conn_check = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        with conn_check.cursor() as cur:
            cur.execute("SELECT team_id FROM teams")
            valid_team_ids = set(row[0] for row in cur.fetchall())
        conn_check.close()
        log(f"Found {len(valid_team_ids)} NBA teams in database")
    except Exception as e:
        log(f"Error fetching valid team IDs: {e}")
        return False
    
    all_processed_records = []
    
    for season_type_name, season_type_code in season_types:
        # Fetch stats for this season type
        stats_df = fetch_season_team_stats(season_type_name)
        if stats_df is None or stats_df.empty:
            log(f"No {season_type_name} data available (this is normal if season hasn't started yet)")
            continue
        
        # Process the data
        processed_records = process_team_stats_data(stats_df, season_type_code, valid_team_ids)
        if processed_records:
            all_processed_records.extend(processed_records)
            log(f"Processed {len(processed_records)} {season_type_name} team records")
    
    if not all_processed_records:
        log("No valid team records to insert. Exiting.")
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
                DELETE FROM team_season_stats 
                WHERE year = %s
            """, (CURRENT_YEAR,))
            
            deleted_count = cur.rowcount
            log(f"Cleared {deleted_count} existing {CURRENT_SEASON} records")
        
        # Insert new team stats data
        insert_query = """
            INSERT INTO team_season_stats (
                team_id, year, season_type, games_played, minutes_x10, possessions,
                fg2m, fg2a, fg3m, fg3a, ftm, fta,
                off_reb_pct_x1000, def_reb_pct_x1000,
                assists, turnovers, steals, blocks, fouls,
                off_rating_x10, def_rating_x10
            ) VALUES %s
        """
        
        with conn.cursor() as cur:
            execute_values(cur, insert_query, all_processed_records)
        
        conn.commit()
        log(f"Successfully inserted {len(all_processed_records)} current season team stat records")
        
        # Verify insertion
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM team_season_stats 
                WHERE year = %s
            """, (CURRENT_YEAR,))
            
            final_count = cur.fetchone()[0]
        
        conn.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        log("=" * 50)
        log(f"Nightly Team Stats Update Completed Successfully")
        log(f"Duration: {duration}")
        log(f"Season: {CURRENT_SEASON}")
        log(f"Records inserted: {len(all_processed_records)}")
        log(f"Total {CURRENT_SEASON} records in database: {final_count}")
        log("=" * 50)
        
        return True
        
    except Exception as e:
        log(f"Database error: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    success = nightly_team_stats_update()
    sys.exit(0 if success else 1)
