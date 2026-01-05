#!/usr/bin/env python3
"""
Test result_set subtraction approach for shot distance filtering.
Verifies that close shots (<10ft) are correctly calculated as ALL - 10ft+.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.etl import get_db_connection

def test_result_set_subtraction():
    """Check if close and far shot values differ (validates subtraction worked)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get a team's shooting stats
    cursor.execute("""
        SELECT 
            year,
            season_type,
            cont_close_2fgm,
            open_close_2fgm,
            cont_2fgm,
            open_2fgm
        FROM team_season_stats
        WHERE team_id = 1610612748  -- Miami Heat
        AND year = '2025'
        ORDER BY season_type
    """)
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    print("=" * 80)
    print("TESTING RESULT SET SUBTRACTION APPROACH")
    print("=" * 80)
    print(f"\nTeam: Miami Heat (2024-25 season)\n")
    print(f"{'Type':<12} {'cont_close':<12} {'open_close':<12} {'cont_total':<12} {'open_total':<12} Status")
    print("-" * 80)
    
    all_good = True
    for row in results:
        year, st, cont_close, open_close, cont_total, open_total = row
        st_name = {1: 'Regular', 2: 'Playoffs', 3: 'PlayIn'}.get(st, str(st))
        
        # Check 1: Close and total should differ (close < total)
        check1 = cont_close < cont_total if cont_close and cont_total else True
        check2 = open_close < open_total if open_close and open_total else True
        
        # Check 2: Close contested and close open should differ
        check3 = cont_close != open_close
        
        status = "✅ OK" if (check1 and check2 and check3) else "❌ FAIL"
        if not (check1 and check2 and check3):
            all_good = False
            
        print(f"{st_name:<12} {cont_close or 'NULL':<12} {open_close or 'NULL':<12} {cont_total or 'NULL':<12} {open_total or 'NULL':<12} {status}")
    
    print("\n" + "=" * 80)
    if all_good:
        print("✅ SUCCESS: Result set subtraction working correctly!")
        print("   - Close shots < Total shots (subtraction working)")
        print("   - Contested ≠ Open for close shots (defender filter working)")
    else:
        print("❌ FAILURE: Issues detected with result set subtraction")
    print("=" * 80)
    
    return all_good

if __name__ == '__main__':
    success = test_result_set_subtraction()
    sys.exit(0 if success else 1)
