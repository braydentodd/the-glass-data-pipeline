#!/usr/bin/env python3
"""
Test each transformation type individually before running full ETL.
Validates that all transformation functions work correctly.
"""

import os
import sys

# Set test mode
os.environ['ETL_TEST_MODE'] = '1'

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.etl import DB_COLUMNS, TEST_MODE_CONFIG
from src.etl import apply_transformation, get_db_connection

def test_arithmetic_subtract_player():
    """Test arithmetic_subtract for player close shot columns."""
    print("\n" + "="*80)
    print("TEST: arithmetic_subtract (player execution)")
    print("="*80)
    print(f"Testing: cont_close_2fgm player_source")
    print(f"Player: {TEST_MODE_CONFIG['player_name']} (ID: {TEST_MODE_CONFIG['player_id']})")
    
    try:
        source_config = DB_COLUMNS['cont_close_2fgm']['player_source']
        transform = source_config['transformation']
        
        result = apply_transformation(
            'cont_close_2fgm',
            transform,
            TEST_MODE_CONFIG['season'],
            entity='player',
            table='player_season_stats',
            season_type=1,
            season_type_name='Regular Season',
            source_config=source_config
        )
        
        test_player_id = TEST_MODE_CONFIG['player_id']
        if test_player_id in result:
            print(f"✅ SUCCESS: Got value {result[test_player_id]} for player {test_player_id}")
            return True
        else:
            print(f"❌ FAILED: No data for test player {test_player_id}")
            print(f"   Result keys: {list(result.keys())[:5]}")
            return False
            
    except Exception as e:
        print(f"❌ FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_arithmetic_subtract_team():
    """Test arithmetic_subtract for team close shot columns."""
    print("\n" + "="*80)
    print("TEST: arithmetic_subtract (league execution for teams)")
    print("="*80)
    print(f"Testing: cont_close_2fgm team_source")
    print(f"Team: {TEST_MODE_CONFIG['team_name']} (ID: {TEST_MODE_CONFIG['team_id']})")
    
    try:
        source_config = DB_COLUMNS['cont_close_2fgm']['team_source']
        transform = source_config['transformation']
        
        result = apply_transformation(
            'cont_close_2fgm',
            transform,
            TEST_MODE_CONFIG['season'],
            entity='team',
            table='team_season_stats',
            season_type=1,
            season_type_name='Regular Season',
            source_config=source_config
        )
        
        test_team_id = TEST_MODE_CONFIG['team_id']
        if test_team_id in result:
            print(f"✅ SUCCESS: Got value {result[test_team_id]} for team {test_team_id}")
            return True
        else:
            print(f"❌ FAILED: No data for test team {test_team_id}")
            print(f"   Result keys: {list(result.keys())[:5]}")
            return False
            
    except Exception as e:
        print(f"❌ FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_filter_aggregate_player():
    """Test filter_aggregate for player total shot columns."""
    print("\n" + "="*80)
    print("TEST: filter_aggregate (player execution)")
    print("="*80)
    print(f"Testing: cont_2fgm player_source")
    print(f"Player: {TEST_MODE_CONFIG['player_name']} (ID: {TEST_MODE_CONFIG['player_id']})")
    
    try:
        source_config = DB_COLUMNS['cont_2fgm']['player_source']
        transform = source_config['transformation']
        
        result = apply_transformation(
            'cont_2fgm',
            transform,
            TEST_MODE_CONFIG['season'],
            entity='player',
            table='player_season_stats',
            season_type=1,
            season_type_name='Regular Season',
            source_config=source_config
        )
        
        test_player_id = TEST_MODE_CONFIG['player_id']
        if test_player_id in result:
            print(f"✅ SUCCESS: Got value {result[test_player_id]} for player {test_player_id}")
            return True
        else:
            print(f"❌ FAILED: No data for test player {test_player_id}")
            print(f"   Result keys: {list(result.keys())[:5]}")
            return False
            
    except Exception as e:
        print(f"❌ FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_filter_aggregate_team():
    """Test filter_aggregate for team total shot columns."""
    print("\n" + "="*80)
    print("TEST: filter_aggregate (league execution for teams)")
    print("="*80)
    print(f"Testing: cont_2fgm team_source")
    print(f"Team: {TEST_MODE_CONFIG['team_name']} (ID: {TEST_MODE_CONFIG['team_id']})")
    
    try:
        source_config = DB_COLUMNS['cont_2fgm']['team_source']
        transform = source_config['transformation']
        
        result = apply_transformation(
            'cont_2fgm',
            transform,
            TEST_MODE_CONFIG['season'],
            entity='team',
            table='team_season_stats',
            season_type=1,
            season_type_name='Regular Season',
            source_config=source_config
        )
        
        test_team_id = TEST_MODE_CONFIG['team_id']
        if test_team_id in result:
            print(f"✅ SUCCESS: Got value {result[test_team_id]} for team {test_team_id}")
            return True
        else:
            print(f"❌ FAILED: No data for test team {test_team_id}")
            print(f"   Result keys: {list(result.keys())[:5]}")
            return False
            
    except Exception as e:
        print(f"❌ FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_database_values():
    """Verify that transformation results were written to database correctly."""
    print("\n" + "="*80)
    print("DATABASE VERIFICATION")
    print("="*80)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    test_player_id = TEST_MODE_CONFIG['player_id']
    test_team_id = TEST_MODE_CONFIG['team_id']
    
    # Check player data
    print(f"\nPlayer data for {TEST_MODE_CONFIG['player_name']}:")
    cursor.execute("""
        SELECT cont_close_2fgm, cont_close_2fga, cont_2fgm, cont_2fga,
               open_close_2fgm, open_close_2fga, open_2fgm, open_2fga
        FROM player_season_stats
        WHERE player_id = %s AND year = %s AND season_type = 1
    """, (test_player_id, TEST_MODE_CONFIG['season']))
    
    row = cursor.fetchone()
    if row:
        print(f"  cont_close_2fgm: {row[0]}, cont_2fgm: {row[2]} (close <= total: {row[0] <= row[2] if row[0] and row[2] else 'N/A'})")
        print(f"  open_close_2fgm: {row[4]}, open_2fgm: {row[6]} (close <= total: {row[4] <= row[6] if row[4] and row[6] else 'N/A'})")
        
        all_populated = all(x is not None for x in row)
        if all_populated:
            print("  ✅ All player shot columns populated")
        else:
            print(f"  ⚠️  Some columns NULL: {[i for i, x in enumerate(row) if x is None]}")
    else:
        print("  ❌ No player data found")
    
    # Check team data
    print(f"\nTeam data for {TEST_MODE_CONFIG['team_name']}:")
    cursor.execute("""
        SELECT cont_close_2fgm, cont_close_2fga, cont_2fgm, cont_2fga,
               open_close_2fgm, open_close_2fga, open_2fgm, open_2fga
        FROM team_season_stats
        WHERE team_id = %s AND year = %s AND season_type = 1
    """, (test_team_id, str(2025)))
    
    row = cursor.fetchone()
    if row:
        print(f"  cont_close_2fgm: {row[0]}, cont_2fgm: {row[2]} (close <= total: {row[0] <= row[2] if row[0] and row[2] else 'N/A'})")
        print(f"  open_close_2fgm: {row[4]}, open_2fgm: {row[6]} (close <= total: {row[4] <= row[6] if row[4] and row[6] else 'N/A'})")
        
        all_populated = all(x is not None for x in row)
        if all_populated:
            print("  ✅ All team shot columns populated")
        else:
            print(f"  ⚠️  Some columns NULL: {[i for i, x in enumerate(row) if x is None]}")
    else:
        print("  ❌ No team data found")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    print("\n" + "="*80)
    print("TRANSFORMATION SYSTEM TEST SUITE")
    print("="*80)
    print(f"Test Subject: {TEST_MODE_CONFIG['player_name']} / {TEST_MODE_CONFIG['team_name']}")
    print(f"Season: {TEST_MODE_CONFIG['season']}")
    
    results = {
        'arithmetic_subtract (player)': test_arithmetic_subtract_player(),
        'arithmetic_subtract (team)': test_arithmetic_subtract_team(),
        'filter_aggregate (player)': test_filter_aggregate_player(),
        'filter_aggregate (team)': test_filter_aggregate_team(),
    }
    
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n✅ All transformation tests PASSED!")
        print("\nNow verifying database values...")
        verify_database_values()
        sys.exit(0)
    else:
        print("\n❌ Some transformation tests FAILED!")
        print("Fix issues before running full ETL")
        sys.exit(1)
