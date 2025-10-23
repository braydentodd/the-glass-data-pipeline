"""
THE GLASS - Historical Stats Backfill (2003-2025)
One-time script to populate player and team season stats for historical seasons.
"""

import os
import sys
import time
import psycopg2
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashteamstats

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

# Historical range
START_YEAR = 2003
END_YEAR = 2026  # Current season

def log(message):
    """Log with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [BACKFILL] {message}")

def reconnect_db(conn):
    """Reconnect to database if connection is lost"""
    try:
        conn.close()
    except:
        pass
    
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
    except Exception as e:
        log(f"Failed to reconnect: {e}")
        raise

def safe_int(value, scale=1):
    """Convert value to scaled integer, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return 0
    try:
        result = int(float(value) * scale)
        return result
    except (ValueError, TypeError):
        return 0

def safe_float(value, scale=1):
    """Convert value to scaled float, handling None/NaN"""
    if value is None or (hasattr(value, '__iter__') and len(str(value).strip()) == 0):
        return None
    try:
        result = int(float(value) * scale)
        return result
    except (ValueError, TypeError):
        return None

def fetch_player_stats(season, season_type='Regular Season'):
    """Fetch player statistics for a specific season and type"""
    
    log(f"Fetching {season} {season_type} player stats...")
    
    try:
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed='Totals',
            timeout=30
        )
        time.sleep(0.6)
        
        df = stats.get_data_frames()[0]
        
        if df.empty:
            log(f"No {season_type} player stats for {season}")
            return pd.DataFrame()
        
        # Get advanced stats (includes rebounding percentages)
        try:
            adv_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season,
                season_type_all_star=season_type,
                measure_type_detailed_defense='Advanced',
                per_mode_detailed='Totals',
                timeout=30
            )
            time.sleep(0.6)
            adv_df = adv_stats.get_data_frames()[0]
            
            if not adv_df.empty:
                df = df.merge(adv_df[['PLAYER_ID', 'TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'OREB_PCT', 'DREB_PCT']], 
                             on=['PLAYER_ID', 'TEAM_ID'], how='left')
        except:
            pass
        
        log(f"Retrieved {len(df)} player-team combinations for {season} {season_type}")
        return df
        
    except Exception as e:
        log(f"Error fetching {season} {season_type} player stats: {e}")
        return pd.DataFrame()

def fetch_team_stats(season, season_type='Regular Season'):
    """Fetch team statistics for a specific season and type"""
    
    log(f"Fetching {season} {season_type} team stats...")
    
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed='Totals',
            timeout=30
        )
        time.sleep(0.6)
        
        df = stats.get_data_frames()[0]
        
        if df.empty:
            log(f"No {season_type} team stats for {season}")
            return pd.DataFrame()
        
        # Get advanced stats (includes rebounding percentages)
        try:
            adv_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                season_type_all_star=season_type,
                measure_type_detailed_defense='Advanced',
                per_mode_detailed='Totals',
                timeout=30
            )
            time.sleep(0.6)
            adv_df = adv_stats.get_data_frames()[0]
            
            if not adv_df.empty:
                df = df.merge(adv_df[['TEAM_ID', 'OFF_RATING', 'DEF_RATING', 'POSS', 'OREB_PCT', 'DREB_PCT']], 
                             on='TEAM_ID', how='left')
        except:
            pass
        
        log(f"Retrieved {len(df)} teams for {season} {season_type}")
        return df
        
    except Exception as e:
        log(f"Error fetching {season} {season_type} team stats: {e}")
        return pd.DataFrame()

def process_player_stats(df, year, season_type_code, valid_player_ids):
    """Process player stats DataFrame into database format"""
    
    processed = []
    seen_player_ids = set()
    
    for _, row in df.iterrows():
        player_id = row['PLAYER_ID']
        
        # Skip if not in valid players or already processed
        if player_id not in valid_player_ids:
            continue
        if player_id in seen_player_ids:
            continue
        seen_player_ids.add(player_id)
        
        # Calculate 2FG from total FG
        fgm = safe_int(row.get('FGM', 0))
        fga = safe_int(row.get('FGA', 0))
        fg3m = safe_int(row.get('FG3M', 0))
        fg3a = safe_int(row.get('FG3A', 0))
        
        fg2m = max(0, fgm - fg3m)
        fg2a = max(0, fga - fg3a)
        
        record = (
            player_id,
            year,
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
        
        processed.append(record)
    
    log(f"Processed {len(processed)} {['Regular Season', 'Playoffs', 'Play-In'][season_type_code-1]} player records")
    return processed

def process_team_stats(df, year, season_type_code, valid_team_ids):
    """Process team stats DataFrame into database format"""
    
    processed = []
    seen_team_ids = set()
    
    for _, row in df.iterrows():
        team_id = row['TEAM_ID']
        
        # Skip if not in valid teams or already processed
        if team_id not in valid_team_ids:
            continue
        if team_id in seen_team_ids:
            continue
        seen_team_ids.add(team_id)
        
        # Calculate 2FG from total FG
        fgm = safe_int(row.get('FGM', 0))
        fga = safe_int(row.get('FGA', 0))
        fg3m = safe_int(row.get('FG3M', 0))
        fg3a = safe_int(row.get('FG3A', 0))
        
        fg2m = max(0, fgm - fg3m)
        fg2a = max(0, fga - fg3a)
        
        record = (
            team_id,
            year,
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
        
        processed.append(record)
    
    log(f"Processed {len(processed)} {['Regular Season', 'Playoffs', 'Play-In'][season_type_code-1]} team records")
    return processed

def backfill_historical_data():
    """Main function to backfill historical data from 2004-2026 (2003-04 through 2025-26 seasons)"""
    
    log("=" * 80)
    log("HISTORICAL DATA BACKFILL: 2004-2026")
    log("=" * 80)
    
    conn = None
    
    try:
        # Database connection
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        log("Database connection established")
        
        # Get valid team and player IDs
        cursor = conn.cursor()
        
        # Get all NBA teams (30 teams)
        cursor.execute("SELECT team_id FROM teams")
        valid_team_ids = {row[0] for row in cursor.fetchall()}
        log(f"Found {len(valid_team_ids)} NBA teams in database")
        
        # Get all players from database (filter to currently active players in database)
        cursor.execute("SELECT player_id FROM players")
        valid_player_ids = {row[0] for row in cursor.fetchall()}
        log(f"Found {len(valid_player_ids)} players in database")
        
        cursor.close()
        
        # Process seasons from 2004 to 2026 (2003-04 through 2025-26)
        for year in range(2004, 2027):
            season = f"{year-1}-{str(year)[2:]}"
            
            log("")
            log("=" * 80)
            log(f"Processing {season} season (year={year})")
            log("=" * 80)
            
            all_player_records = []
            all_team_records = []
            
            # Process each season type
            season_types = [
                ('Regular Season', 1),
                ('Playoffs', 2),
                ('PlayIn', 3)
            ]
            
            for season_type_name, season_type_code in season_types:
                # Fetch player stats
                player_df = fetch_player_stats(season, season_type_name)
                if not player_df.empty:
                    player_records = process_player_stats(player_df, year, season_type_code, valid_player_ids)
                    all_player_records.extend(player_records)
                
                # Fetch team stats
                team_df = fetch_team_stats(season, season_type_name)
                if not team_df.empty:
                    team_records = process_team_stats(team_df, year, season_type_code, valid_team_ids)
                    all_team_records.extend(team_records)
            
            # Bulk insert player records
            if all_player_records:
                cursor = conn.cursor()
                execute_values(
                    cursor,
                    """
                    INSERT INTO player_season_stats (
                        player_id, year, team_id, season_type,
                        games_played, minutes_x10, possessions,
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
                        def_rating_x10 = EXCLUDED.def_rating_x10
                    """,
                    all_player_records
                )
                conn.commit()
                cursor.close()
                log(f"✓ Inserted {len(all_player_records)} player records for {season}")
            
            # Bulk insert team records
            if all_team_records:
                cursor = conn.cursor()
                execute_values(
                    cursor,
                    """
                    INSERT INTO team_season_stats (
                        team_id, year, season_type,
                        games_played, minutes_x10, possessions,
                        fg2m, fg2a, fg3m, fg3a, ftm, fta,
                        off_reb_pct_x1000, def_reb_pct_x1000,
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
                        off_reb_pct_x1000 = EXCLUDED.off_reb_pct_x1000,
                        def_reb_pct_x1000 = EXCLUDED.def_reb_pct_x1000,
                        assists = EXCLUDED.assists,
                        turnovers = EXCLUDED.turnovers,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        fouls = EXCLUDED.fouls,
                        off_rating_x10 = EXCLUDED.off_rating_x10,
                        def_rating_x10 = EXCLUDED.def_rating_x10
                    """,
                    all_team_records
                )
                conn.commit()
                cursor.close()
                log(f"✓ Inserted {len(all_team_records)} team records for {season}")
        
        log("")
        log("=" * 80)
        log("BACKFILL COMPLETE!")
        log("=" * 80)
        
    except Exception as e:
        log(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if conn:
            conn.close()
            log("Database connection closed")

if __name__ == "__main__":
    backfill_historical_data()
