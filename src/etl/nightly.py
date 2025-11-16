"""
THE GLASS - Consolidated Nightly ETL
Single module that handles all nightly data updates:
1. Player roster updates (new players, team changes)
2. Player season statistics
3. Team season statistics
"""

import os
import sys
import time
import psycopg2
from datetime import datetime
from nba_api.stats.static import players as nba_players
from nba_api.stats.endpoints import commonteamroster, playergamelog, leaguedashteamstats

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import NBA_CONFIG, DB_CONFIG, TEAM_IDS

# Load environment variables from .env if it exists
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)


def log(message, level="INFO"):
    """Centralized logging with timestamp"""
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


# ============================================================================
# STEP 1: UPDATE PLAYER ROSTERS
# ============================================================================

def update_player_rosters():
    """
    Fetch current NBA rosters and update database.
    Adds new players and updates team assignments for traded players.
    """
    log("=" * 60)
    log("STEP 1: Updating Player Rosters")
    log("=" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_players = {}
    players_added = 0
    players_updated = 0
    
    log(f"Fetching rosters for {len(TEAM_IDS)} NBA teams...")
    
    for team_id in TEAM_IDS:
        try:
            roster = commonteamroster.CommonTeamRoster(
                team_id=team_id,
                timeout=60,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Referer': 'https://www.nba.com/'
                }
            )
            time.sleep(1.0)  # Rate limiting
            
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
            
            log(f"✓ Team {team_id}: {len(df)} players")
            
        except Exception as e:
            log(f"✗ Error fetching team {team_id}: {e}", "ERROR")
            continue
    
    log(f"Total players fetched: {len(all_players)}")
    
    # Update database
    for player_id, player_data in all_players.items():
        try:
            # Check if player exists
            cursor.execute("SELECT team_id FROM players WHERE player_id = %s", (player_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update team_id if changed
                if existing[0] != player_data['team_id']:
                    cursor.execute(
                        "UPDATE players SET team_id = %s WHERE player_id = %s",
                        (player_data['team_id'], player_id)
                    )
                    players_updated += 1
                    log(f"Updated: {player_data['name']} → Team {player_data['team_id']}")
            else:
                # Insert new player
                cursor.execute("""
                    INSERT INTO players (player_id, name, team_id, position, jersey_number)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    player_id,
                    player_data['name'],
                    player_data['team_id'],
                    player_data['position'],
                    player_data['jersey']
                ))
                players_added += 1
                log(f"Added: {player_data['name']} (Team {player_data['team_id']})")
        
        except Exception as e:
            log(f"✗ Error updating player {player_id}: {e}", "ERROR")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"✓ Roster update complete: {players_added} added, {players_updated} updated")
    return True


# ============================================================================
# STEP 2: UPDATE PLAYER SEASON STATISTICS
# ============================================================================

def update_player_stats():
    """
    Update season statistics for all active players.
    """
    log("=" * 60)
    log("STEP 2: Updating Player Season Statistics")
    log("=" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all players
    cursor.execute("SELECT player_id, name FROM players WHERE team_id IS NOT NULL")
    players = cursor.fetchall()
    
    log(f"Updating stats for {len(players)} players...")
    
    current_season = NBA_CONFIG['current_season']
    updated_count = 0
    
    for player_id, player_name in players:
        try:
            # Fetch player game log
            gamelog = playergamelog.PlayerGameLog(
                player_id=player_id,
                season=current_season,
                season_type_all_star='Regular Season'
            )
            time.sleep(0.6)  # Rate limiting
            
            df = gamelog.get_data_frames()[0]
            
            if df.empty:
                continue
            
            # Aggregate stats
            stats = {
                'games_played': len(df),
                'minutes': df['MIN'].sum() if 'MIN' in df else 0,
                'points': df['PTS'].sum() if 'PTS' in df else 0,
                'rebounds': df['REB'].sum() if 'REB' in df else 0,
                'assists': df['AST'].sum() if 'AST' in df else 0,
                # Add more stats as needed
            }
            
            # Upsert into database
            cursor.execute("""
                INSERT INTO player_season_stats 
                (player_id, year, games_played, minutes_x10, points, rebounds, assists)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, year) 
                DO UPDATE SET
                    games_played = EXCLUDED.games_played,
                    minutes_x10 = EXCLUDED.minutes_x10,
                    points = EXCLUDED.points,
                    rebounds = EXCLUDED.rebounds,
                    assists = EXCLUDED.assists
            """, (
                player_id,
                NBA_CONFIG['current_season_year'],
                stats['games_played'],
                int(stats['minutes'] * 10),  # Store as minutes * 10
                stats['points'],
                stats['rebounds'],
                stats['assists']
            ))
            
            updated_count += 1
            
            if updated_count % 50 == 0:
                log(f"Progress: {updated_count}/{len(players)} players")
                conn.commit()
        
        except Exception as e:
            log(f"✗ Error updating {player_name}: {e}", "ERROR")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    log(f"✓ Player stats updated: {updated_count} players")
    return True


# ============================================================================
# STEP 3: UPDATE TEAM SEASON STATISTICS
# ============================================================================

def update_team_stats():
    """
    Update season statistics for all NBA teams.
    """
    log("=" * 60)
    log("STEP 3: Updating Team Season Statistics")
    log("=" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Fetch league-wide team stats
        team_stats = leaguedashteamstats.LeagueDashTeamStats(
            season=NBA_CONFIG['current_season'],
            season_type_all_star='Regular Season'
        )
        time.sleep(1.0)
        
        df = team_stats.get_data_frames()[0]
        
        updated_count = 0
        
        for _, row in df.iterrows():
            team_id = row['TEAM_ID']
            
            cursor.execute("""
                INSERT INTO team_season_stats
                (team_id, year, games_played, wins, losses, win_pct, 
                 points, fg_pct, fg3_pct, ft_pct, rebounds, assists)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, year)
                DO UPDATE SET
                    games_played = EXCLUDED.games_played,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    win_pct = EXCLUDED.win_pct,
                    points = EXCLUDED.points,
                    fg_pct = EXCLUDED.fg_pct,
                    fg3_pct = EXCLUDED.fg3_pct,
                    ft_pct = EXCLUDED.ft_pct,
                    rebounds = EXCLUDED.rebounds,
                    assists = EXCLUDED.assists
            """, (
                team_id,
                NBA_CONFIG['current_season_year'],
                row.get('GP', 0),
                row.get('W', 0),
                row.get('L', 0),
                row.get('W_PCT', 0),
                row.get('PTS', 0),
                row.get('FG_PCT', 0),
                row.get('FG3_PCT', 0),
                row.get('FT_PCT', 0),
                row.get('REB', 0),
                row.get('AST', 0)
            ))
            
            updated_count += 1
        
        conn.commit()
        log(f"✓ Team stats updated: {updated_count} teams")
        
    except Exception as e:
        log(f"✗ Error updating team stats: {e}", "ERROR")
        return False
    finally:
        cursor.close()
        conn.close()
    
    return True


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def run_nightly_etl():
    """
    Main orchestrator for nightly ETL pipeline.
    Runs all three steps in sequence.
    """
    log("=" * 60)
    log("THE GLASS - NIGHTLY ETL STARTING")
    log("=" * 60)
    
    start_time = datetime.now()
    
    steps = [
        ("Player Rosters", update_player_rosters),
        ("Player Statistics", update_player_stats),
        ("Team Statistics", update_team_stats)
    ]
    
    results = []
    
    for step_name, step_func in steps:
        log(f"\n>>> {step_name}")
        step_start = datetime.now()
        
        try:
            success = step_func()
            duration = datetime.now() - step_start
            results.append((step_name, success, duration))
            
            if not success:
                log(f"✗ {step_name} failed. Stopping ETL.", "ERROR")
                break
                
        except Exception as e:
            log(f"✗ {step_name} encountered error: {e}", "ERROR")
            duration = datetime.now() - step_start
            results.append((step_name, False, duration))
            break
    
    # Summary
    total_duration = datetime.now() - start_time
    
    log("\n" + "=" * 60)
    log("ETL SUMMARY")
    log("=" * 60)
    
    all_success = True
    for step_name, success, duration in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        log(f"{step_name:20s} {status:12s} ({duration})")
        if not success:
            all_success = False
    
    log(f"\nTotal Duration: {total_duration}")
    log("=" * 60)
    
    return all_success


if __name__ == "__main__":
    # Check for required environment variables
    if not os.getenv('DB_PASSWORD'):
        log("ERROR: DB_PASSWORD environment variable must be set", "ERROR")
        sys.exit(1)
    
    # Run the ETL
    success = run_nightly_etl()
    sys.exit(0 if success else 1)
