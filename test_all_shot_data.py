"""
Comprehensive test for ALL shot-related data (offensive + defensive).
Tests both player and team data to ensure transformations work correctly.
"""

import sys
import os
sys.path.insert(0, '/Users/br288608/repos/personal-stuff/The Glass/the-glass-data-pipeline')
os.environ['TEST_MODE'] = '1'

from config.etl import DB_COLUMNS, TEST_MODE_CONFIG
import psycopg2
from config.etl import DB_CONFIG

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def test_database_values():
    """Check actual database values after ETL runs"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    player_id = TEST_MODE_CONFIG['player_id']
    team_id = TEST_MODE_CONFIG['team_id']
    season = TEST_MODE_CONFIG['season']
    
    print("\n" + "="*80)
    print("DATABASE VALUES - PLAYER SHOOTING DATA")
    print("="*80)
    
    # Player shooting columns
    shot_cols = [
        'cont_close_2fgm', 'cont_close_2fga', 'open_close_2fgm', 'open_close_2fga',
        'cont_2fgm', 'cont_2fga', 'open_2fgm', 'open_2fga',
        'cont_3fgm', 'cont_3fga', 'open_3fgm', 'open_3fga'
    ]
    
    cursor.execute(f"""
        SELECT season_type, {', '.join(shot_cols)}
        FROM player_season_stats
        WHERE player_id = %s AND year = %s
        ORDER BY season_type
    """, (player_id, season))
    
    rows = cursor.fetchall()
    season_types = {1: 'Regular', 2: 'Playoffs', 3: 'PlayIn'}
    
    for row in rows:
        st = season_types.get(row[0], f'Unknown({row[0]})')
        print(f"\n{st} Season:")
        for i, col in enumerate(shot_cols, 1):
            print(f"  {col:20s}: {row[i]}")
    
    print("\n" + "="*80)
    print("DATABASE VALUES - TEAM SHOOTING DATA")
    print("="*80)
    
    cursor.execute(f"""
        SELECT season_type, {', '.join(shot_cols)}
        FROM team_season_stats
        WHERE team_id = %s AND year = %s
        ORDER BY season_type
    """, (team_id, season))
    
    rows = cursor.fetchall()
    
    for row in rows:
        st = season_types.get(row[0], f'Unknown({row[0]})')
        print(f"\n{st} Season:")
        for i, col in enumerate(shot_cols, 1):
            print(f"  {col:20s}: {row[i]}")
    
    print("\n" + "="*80)
    print("DATABASE VALUES - DEFENSIVE SHOT DATA")
    print("="*80)
    
    def_cols = ['d_close_2fgm', 'd_close_2fga', 'd_2fgm', 'd_2fga', 'd_3fgm', 'd_3fga']
    
    # Player defensive
    cursor.execute(f"""
        SELECT season_type, {', '.join(def_cols)}
        FROM player_season_stats
        WHERE player_id = %s AND year = %s
        ORDER BY season_type
    """, (player_id, season))
    
    rows = cursor.fetchall()
    print("\nPlayer Defensive Stats:")
    for row in rows:
        st = season_types.get(row[0], f'Unknown({row[0]})')
        print(f"\n{st} Season:")
        for i, col in enumerate(def_cols, 1):
            print(f"  {col:20s}: {row[i]}")
    
    # Team defensive
    cursor.execute(f"""
        SELECT season_type, {', '.join(def_cols)}
        FROM team_season_stats
        WHERE team_id = %s AND year = %s
        ORDER BY season_type
    """, (team_id, season))
    
    rows = cursor.fetchall()
    print("\nTeam Defensive Stats:")
    for row in rows:
        st = season_types.get(row[0], f'Unknown({row[0]})')
        print(f"\n{st} Season:")
        for i, col in enumerate(def_cols, 1):
            print(f"  {col:20s}: {row[i]}")
    
    cursor.close()
    conn.close()

def validate_data_logic():
    """Validate the data makes logical sense"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    player_id = TEST_MODE_CONFIG['player_id']
    team_id = TEST_MODE_CONFIG['team_id']
    season = TEST_MODE_CONFIG['season']
    
    print("\n" + "="*80)
    print("DATA VALIDATION CHECKS")
    print("="*80)
    
    season_types = {1: 'Regular', 2: 'Playoffs', 3: 'PlayIn'}
    
    for entity, entity_id, table in [('Player', player_id, 'player_season_stats'), 
                                      ('Team', team_id, 'team_season_stats')]:
        print(f"\n{entity} Data:")
        
        cursor.execute(f"""
            SELECT season_type, 
                   cont_close_2fgm, open_close_2fgm, cont_2fgm, open_2fgm,
                   cont_close_2fga, open_close_2fga, cont_2fga, open_2fga
            FROM {table}
            WHERE {entity.lower()}_id = %s AND year = %s
            ORDER BY season_type
        """, (entity_id, season))
        
        for row in cursor.fetchall():
            st_name = season_types.get(row[0], f'Unknown({row[0]})')
            cont_close_m, open_close_m, cont_m, open_m = row[1:5]
            cont_close_a, open_close_a, cont_a, open_a = row[5:9]
            
            checks = []
            
            # Close shots should be <= Total shots
            if cont_close_m is not None and cont_m is not None:
                if cont_close_m <= cont_m:
                    checks.append(f"✓ Contested: Close({cont_close_m}) <= Total({cont_m})")
                else:
                    checks.append(f"✗ Contested: Close({cont_close_m}) > Total({cont_m}) INVALID!")
            
            if open_close_m is not None and open_m is not None:
                if open_close_m <= open_m:
                    checks.append(f"✓ Open: Close({open_close_m}) <= Total({open_m})")
                else:
                    checks.append(f"✗ Open: Close({open_close_m}) > Total({open_m}) INVALID!")
            
            # Contested should differ from Open
            if cont_m is not None and open_m is not None and cont_m != open_m:
                checks.append(f"✓ Contested({cont_m}) ≠ Open({open_m})")
            elif cont_m == open_m:
                checks.append(f"✗ Contested({cont_m}) = Open({open_m}) SUSPICIOUS!")
            
            print(f"\n  {st_name} Season:")
            for check in checks:
                print(f"    {check}")
    
    cursor.close()
    conn.close()

def analyze_config_inconsistencies():
    """Find all transformation inconsistencies in config"""
    print("\n" + "="*80)
    print("CONFIG ANALYSIS - TRANSFORMATION INCONSISTENCIES")
    print("="*80)
    
    shot_columns = [
        'cont_close_2fgm', 'cont_close_2fga', 'open_close_2fgm', 'open_close_2fga',
        'cont_2fgm', 'cont_2fga', 'open_2fgm', 'open_2fga',
        'cont_3fgm', 'cont_3fga', 'open_3fgm', 'open_3fga',
        'd_close_2fgm', 'd_close_2fga', 'd_2fgm', 'd_2fga', 'd_3fgm', 'd_3fga'
    ]
    
    issues = []
    
    for col_name in shot_columns:
        col_cfg = DB_COLUMNS.get(col_name, {})
        
        for source_type in ['player_source', 'team_source']:
            source = col_cfg.get(source_type)
            if not source:
                continue
            
            outer_endpoint = source.get('endpoint')
            transformation = source.get('transformation', {})
            inner_endpoint = transformation.get('endpoint')
            
            # Check for duplication
            if outer_endpoint and inner_endpoint:
                if outer_endpoint != inner_endpoint:
                    issues.append(f"{col_name}.{source_type}: MISMATCH - outer='{outer_endpoint}' vs inner='{inner_endpoint}'")
                else:
                    issues.append(f"{col_name}.{source_type}: DUPLICATE - endpoint defined twice: '{outer_endpoint}'")
            
            # Check if transformation exists but missing endpoint
            if transformation and not inner_endpoint and not outer_endpoint:
                issues.append(f"{col_name}.{source_type}: MISSING - transformation exists but no endpoint defined")
    
    if issues:
        print("\nISSUES FOUND:")
        for issue in issues:
            print(f"  ✗ {issue}")
    else:
        print("\n✓ No inconsistencies found in shot column configs")
    
    return issues

if __name__ == '__main__':
    print("\n" + "="*80)
    print(f"COMPREHENSIVE SHOT DATA TEST")
    print(f"Testing: {TEST_MODE_CONFIG['player_name']} / {TEST_MODE_CONFIG['team_name']}")
    print(f"Season: {TEST_MODE_CONFIG['season']}")
    print("="*80)
    
    # First analyze config
    issues = analyze_config_inconsistencies()
    
    # Then test database values
    test_database_values()
    
    # Finally validate logic
    validate_data_logic()
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
    
    if issues:
        print(f"\n⚠️  Found {len(issues)} config issues that need to be fixed!")
        sys.exit(1)
