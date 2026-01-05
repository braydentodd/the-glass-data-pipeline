#!/usr/bin/env python3
"""
Quick test for Issue 3 fix - Run ONLY team transformation columns
This avoids waiting for the full ETL to complete
"""
import os, sys
sys.path.insert(0, '.')

from config.etl import DB_CONFIG, get_season, SEASON_TYPE_MAP
from src.etl import update_transformation_columns
import psycopg2

print("="*80)
print("TESTING ISSUE 3 FIX - Team Transformation Columns")
print("="*80)

season = get_season()

try:
    print(f"\n📊 Running transformation columns for team (season={season})...")
    print("This will test the shot_distance filtering fix\n")
    
    # Run transformation columns for each season type
    for season_type_name, season_type_code in SEASON_TYPE_MAP.items():
        print(f"  • {season_type_name}...")
        update_transformation_columns(
            season=season,
            entity='team',
            table='team_season_stats',
            season_type=season_type_code,
            season_type_name=season_type_name
        )
    
    print("\n✅ Transformation columns updated!")
    print("\n" + "="*80)
    print("CHECKING RESULTS...")
    print("="*80)
    
    # Check results
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            season_type,
            games_played,
            open_close_2fgm, cont_close_2fgm,
            open_3fgm, cont_3fgm,
            open_2fgm, cont_2fgm
        FROM team_season_stats
        WHERE team_id = 1610612748 AND year = '2025'
        ORDER BY season_type
    """)
    
    print("\nTeam Stats (Miami Heat):")
    print("-"*80)
    print("ST | GP | oCl2M | cCl2M | MATCH? | o3M | c3M | MATCH? | o2M | c2M | MATCH?")
    print("-"*80)
    
    all_good = True
    for row in cursor.fetchall():
        st = {1: "RS", 2: "PO", 3: "PI"}[row[0]]
        gp, ocl2m, ccl2m, o3m, c3m, o2m, c2m = row[1:]
        
        cl2_match = "❌ DUP" if ocl2m == ccl2m else "✅ OK"
        c3_match = "❌ DUP" if o3m == c3m else "✅ OK"
        c2_match = "❌ DUP" if o2m == c2m else "✅ OK"
        
        if ocl2m == ccl2m or o3m == c3m or o2m == c2m:
            all_good = False
        
        print(f"{st} | {gp:2} | {ocl2m:5} | {ccl2m:5} | {cl2_match:6} | {o3m:3} | {c3m:3} | {c3_match:6} | {o2m:4} | {c2m:4} | {c2_match:6}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*80)
    if all_good:
        print("✅ ISSUE 3 FIXED! Contested and open shots now have different values!")
    else:
        print("❌ ISSUE 3 STILL BROKEN - Values still duplicated")
    print("="*80)
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
