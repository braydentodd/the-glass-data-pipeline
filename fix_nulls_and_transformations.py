"""
One-time fix for NULL values in existing data.

This script fixes NULLs that were created before the backfill was updated
to automatically convert NULL→0. Future backfills will do this automatically.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from lib.etl import get_db_connection, quote_column, get_columns_for_null_cleanup
from config.etl import DB_COLUMNS

def fix_nulls_to_zeros():
    """Convert NULL values to 0 for all stat columns where GP != 0, respecting min_season constraints."""
    print("=" * 70)
    print("FIXING NULL VALUES (ONE-TIME CLEANUP)")
    print("=" * 70)
    print()
    print("NOTE: Future backfills will automatically convert NULL→0")
    print("This script only fixes existing data.")
    print()
    print("IMPORTANT: Only updates columns for seasons where data is available")
    print("           (respects min_season constraints from endpoints)")
    print()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all distinct seasons from player_season_stats
    print("Finding all seasons in database...")
    cursor.execute("SELECT DISTINCT year FROM player_season_stats ORDER BY year")
    player_seasons = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT year FROM team_season_stats ORDER BY year")
    team_seasons = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {len(player_seasons)} player seasons, {len(team_seasons)} team seasons\n")
    
    # Process player seasons
    print("Updating player_season_stats...")
    print("=" * 70)
    player_total_updates = 0
    
    for season in player_seasons:
        # Get columns available for this specific season
        available_cols = get_columns_for_null_cleanup(season, entity='player')
        
        if not available_cols:
            continue
        
        print(f"\nSeason {season}: {len(available_cols)} columns available")
        season_updates = 0
        
        for col in available_cols:
            try:
                quoted_col = quote_column(col)
                cursor.execute(f"""
                    UPDATE player_season_stats 
                    SET {quoted_col} = 0 
                    WHERE {quoted_col} IS NULL 
                    AND games > 0
                    AND year = %s
                """, (season,))
                updated = cursor.rowcount
                if updated > 0:
                    print(f"  {col}: {updated} rows")
                    season_updates += updated
            except Exception as e:
                print(f"  {col}: ERROR - {e}")
                conn.rollback()
        
        if season_updates > 0:
            player_total_updates += season_updates
            print(f"  → Season total: {season_updates} rows")
    
    print(f"\n{'=' * 70}")
    print(f"Player total: {player_total_updates} rows updated")
    
    # Process team seasons
    print("\n\nUpdating team_season_stats...")
    print("=" * 70)
    team_total_updates = 0
    
    for season in team_seasons:
        # Convert year int to season string for team stats
        # team_season_stats stores year as '2024', '2025', etc.
        # get_columns_for_null_cleanup expects '2023-24', '2024-25', etc.
        try:
            year_int = int(season)
            season_str = f"{year_int-1}-{str(year_int)[2:]}"
        except:
            season_str = season
        
        available_cols = get_columns_for_null_cleanup(season_str, entity='team')
        
        if not available_cols:
            continue
        
        print(f"\nYear {season}: {len(available_cols)} columns available")
        season_updates = 0
        
        for col in available_cols:
            try:
                quoted_col = quote_column(col)
                cursor.execute(f"""
                    UPDATE team_season_stats 
                    SET {quoted_col} = 0 
                    WHERE {quoted_col} IS NULL 
                    AND games > 0
                    AND year = %s
                """, (season,))
                updated = cursor.rowcount
                if updated > 0:
                    print(f"  {col}: {updated} rows")
                    season_updates += updated
            except Exception as e:
                print(f"  {col}: ERROR - {e}")
                conn.rollback()
        
        if season_updates > 0:
            team_total_updates += season_updates
            print(f"  → Year total: {season_updates} rows")
    
    print(f"\n{'=' * 70}")
    print(f"Team total: {team_total_updates} rows updated")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("\n✅ NULL values fixed!\n")


if __name__ == '__main__':
    print("\n")
    
    try:
        # Fix existing NULLs to 0s
        fix_nulls_to_zeros()
        
        print("=" * 70)
        print("NEXT STEPS")
        print("=" * 70)
        print("""
1. Reset transformation endpoints:
   python3 reset_transformation_endpoints.py --execute

2. Re-run player backfill (will populate transformations):
   python3 -m src.etl

3. Re-run team backfill (will populate transformations):
   python3 backfill_team_stats.py

The backfill now automatically:
  ✅ Populates direct API columns
  ✅ Executes transformations to compute derived columns  
  ✅ Converts NULL→0 for rows where games > 0
""")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
