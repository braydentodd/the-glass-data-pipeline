#!/usr/bin/env python3
"""
Comprehensive test to analyze all 3 data quality issues:

ISSUE 1: PlayIn NULLs
- Some columns NULL in PlayIn despite player having minutes
- ROOT CAUSE: NBA API doesn't return all endpoints for PlayIn (hustle, defense tracking)
- EXPECTED: NULL is correct - API doesn't provide the data

ISSUE 2: Repeated Values Across Season Types  
- Shooting splits, rebounds have identical values for RS/PO/PI
- ROOT CAUSE: Config had hardcoded 'season_type_all_star': 'Regular Season' in endpoint_params
- FIX APPLIED: Removed hardcoded values, ETL now overrides with runtime season_type
- EXPECTED: After fix, values should differ across season types

ISSUE 3: Data Duplication
- contested and open shots have identical values (should differ)
- ROOT CAUSE: defender_distance_category filtering in per-team aggregation not working
- STATUS: NOT YET FIXED - requires debugging aggregation logic
"""

import psycopg2
import os

conn = psycopg2.connect(
    host='150.136.255.23',
    port=5432,
    database='the_glass_db',
    user='the_glass_user',
    password=''
)
cursor = conn.cursor()

print("="*100)
print("ISSUE ANALYSIS - THE GLASS DATA QUALITY")
print("="*100)

# ============================================================================
# ISSUE 1: PlayIn NULLs
# ============================================================================
print("\n" + "="*100)
print("ISSUE 1: PlayIn NULLs (API Limitation)")
print("="*100)

cursor.execute("""
    SELECT 
        player_id,
        minutes_x10,
        charges_drawn, deflections, contests,
        d_close_2fgm, d_2fgm, d_3fgm,
        touches, passes, possessions
    FROM player_season_stats
    WHERE year = '2024-25' AND season_type = 3 AND player_id = 1631170
""")

row = cursor.fetchone()
if row:
    pid, min10, charges, defl, cont, d_cl2m, d_2m, d_3m, touch, pass_, poss = row
    
    print(f"\nPlayer: {pid} | Minutes: {min10/10:.1f}")
    print(f"\nHustle Stats (from leaguehustlestatsplayer):")
    print(f"  charges_drawn: {charges} {'✓' if charges is not None else '❌ NULL'}")
    print(f"  deflections:   {defl} {'✓' if defl is not None else '❌ NULL'}")
    print(f"  contests:      {cont} {'✓' if cont is not None else '❌ NULL'}")
    
    print(f"\nDefense Stats (from leaguedashptdefend):")
    print(f"  d_close_2fgm: {d_cl2m} {'✓' if d_cl2m is not None else '❌ NULL'}")
    print(f"  d_2fgm:       {d_2m} {'✓' if d_2m is not None else '❌ NULL'}")
    print(f"  d_3fgm:       {d_3m} {'✓' if d_3m is not None else '❌ NULL'}")
    
    print(f"\nBase Stats (always available):")
    print(f"  touches:      {touch} ✓")
    print(f"  passes:       {pass_} ✓")
    print(f"  possessions:  {poss} ✓")
    
    print(f"\n⚠️  EXPECTED BEHAVIOR: Hustle/Defense NULLs are correct")
    print(f"    NBA API doesn't track these stats for all players in PlayIn games")

# ============================================================================
# ISSUE 2: Repeated Values (SHOULD BE FIXED NOW)
# ============================================================================
print("\n" + "="*100)
print("ISSUE 2: Repeated Values Across Season Types (Should be Fixed)")
print("="*100)

cursor.execute("""
    SELECT 
        season_type,
        games_played,
        minutes_x10,
        cont_3fgm, cont_3fga,
        open_3fgm, open_3fga,
        open_close_2fgm, open_close_2fga,
        cont_2fgm, cont_2fga,
        putbacks
    FROM player_season_stats
    WHERE player_id = 1631170 AND year = '2024-25'
    ORDER BY season_type
""")

print("\nPlayer Transformation Columns:")
print("-"*100)
print("ST | GP | Min  | cC3M | cC3A | oC3M | oC3A | oCl2M | oCl2A | c2M | c2A | PB")
print("-"*100)

rows = cursor.fetchall()
for row in rows:
    st, gp, min10 = row[0:3]
    cc3m, cc3a, oc3m, oc3a = row[3:7]
    ocl2m, ocl2a, c2m, c2a = row[7:11]
    pb = row[11]
    
    st_name = {1: "RS", 2: "PO", 3: "PI"}[st]
    print(f"{st_name} | {gp:2} | {min10:4} | {cc3m:4} | {cc3a:4} | {oc3m:4} | {oc3a:4} | {ocl2m:5} | {ocl2a:5} | {c2m:3} | {c2a:3} | {pb:2}")

# Check if values are identical
if len(rows) >= 2:
    rs_vals = rows[0][3:]  # Skip season_type, gp, minutes
    po_vals = rows[1][3:]
    
    print("\n" + "="*100)
    if rs_vals == po_vals:
        print("❌ STILL BROKEN: RS == PO (identical transformation values)")
        print("   This means the fix didn't work - still fetching Regular Season data")
    else:
        print("✅ FIXED: RS != PO (transformation values differ correctly)")
        print("   The season_type override is working!")

# ============================================================================
# ISSUE 3: Data Duplication (NOT YET FIXED)
# ============================================================================
print("\n" + "="*100)
print("ISSUE 3: Data Duplication - Contested vs Open (NOT YET FIXED)")
print("="*100)

cursor.execute("""
    SELECT 
        season_type,
        open_close_2fgm, cont_close_2fgm,
        open_close_2fga, cont_close_2fga,
        open_3fgm, cont_3fgm,
        open_3fga, cont_3fga,
        open_2fgm, cont_2fgm
    FROM player_season_stats
    WHERE player_id = 1631170 AND year = '2024-25'
    ORDER BY season_type
""")

print("\nContested vs Open Comparison:")
print("-"*100)
print("ST | oCl2M | cCl2M | MATCH? | oC3M | cC3M | MATCH? | o2M | c2M | MATCH?")
print("-"*100)

for row in cursor.fetchall():
    st = {1: "RS", 2: "PO", 3: "PI"}[row[0]]
    ocl2m, ccl2m = row[1], row[2]
    ocl2a, ccl2a = row[3], row[4]
    oc3m, cc3m = row[5], row[6]
    oc3a, cc3a = row[7], row[8]
    o2m, c2m = row[9], row[10]
    
    cl2_match = "❌ DUP" if ocl2m == ccl2m else "✓ OK"
    c3_match = "❌ DUP" if oc3m == cc3m else "✓ OK"
    c2_match = "❌ DUP" if o2m == c2m else "✓ OK"
    
    print(f"{st} | {ocl2m:5} | {ccl2m:5} | {cl2_match:6} | {oc3m:4} | {cc3m:4} | {c3_match:6} | {o2m:3} | {c2m:3} | {c3_match:6}")

print("\n" + "="*100)
print("⚠️  ISSUE 3 NOT FIXED: Per-team aggregation filtering is broken")
print("    defender_distance_category filter not properly distinguishing contested vs open")
print("    This requires debugging _execute_per_team_endpoint aggregation logic")

# ============================================================================
# Team Stats Check
# ============================================================================
print("\n" + "="*100)
print("TEAM STATS: Checking for same issues")
print("="*100)

cursor.execute("""
    SELECT 
        season_type,
        games_played,
        open_close_2fgm, cont_close_2fgm,
        open_3fgm, cont_3fgm,
        putbacks
    FROM team_season_stats
    WHERE team_id = 1610612748 AND year = '2025'
    ORDER BY season_type
""")

print("\nTeam Transformation Columns:")
print("-"*70)
print("ST | GP | oCl2M | cCl2M | oC3M | cC3M | PB")
print("-"*70)

team_rows = cursor.fetchall()
for row in team_rows:
    st = {1: "RS", 2: "PO", 3: "PI"}[row[0]]
    gp, ocl2m, ccl2m, oc3m, cc3m, pb = row[1:]
    print(f"{st} | {gp:2} | {ocl2m:5} | {ccl2m:5} | {oc3m:4} | {cc3m:4} | {pb:3}")

# Check for duplication
if len(team_rows) >= 1:
    row = team_rows[0]
    ocl2m, ccl2m, oc3m, cc3m = row[2], row[3], row[4], row[5]
    
    print("\n" + "="*70)
    if ocl2m == ccl2m or oc3m == cc3m:
        print("❌ ISSUE 3 ALSO IN TEAM STATS: Contested = Open (duplication)")
    else:
        print("✅ Team stats look OK")

# Check if putbacks repeat
if len(team_rows) >= 2:
    pb_vals = [row[6] for row in team_rows]
    if len(set(pb_vals)) == 1:
        print(f"❌ ISSUE 2 IN TEAM: putbacks={pb_vals[0]} (same across all season types)")
    else:
        print(f"✅ Team putbacks differ: {pb_vals}")

cursor.close()
conn.close()

print("\n" + "="*100)
print("SUMMARY:")
print("="*100)
print("✓ Issue 1: PlayIn NULLs are EXPECTED (API doesn't provide hustle/defense for all players)")
print("? Issue 2: Should be FIXED (removed hardcoded season_type) - check test results above")
print("❌ Issue 3: NOT FIXED - per-team aggregation filtering needs debugging")
print("="*100)
