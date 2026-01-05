"""
Test script to validate shot column data for players and teams.
Tests the transformation logic directly without running full ETL.
"""

import sys
sys.path.insert(0, '/Users/br288608/repos/personal-stuff/The Glass/the-glass-data-pipeline')

from config.etl import DB_COLUMNS, TEST_MODE_CONFIG
from src.etl import _apply_arithmetic_subtract_per_player, _get_endpoint_class
import os
os.environ['TEST_MODE'] = '1'

def test_player_close_shots():
    """Test player close shot columns using arithmetic_subtract transformation"""
    print("\n" + "="*80)
    print("TESTING PLAYER CLOSE SHOT COLUMNS (arithmetic_subtract)")
    print("="*80)
    
    player_id = TEST_MODE_CONFIG['player_id']
    season = TEST_MODE_CONFIG['season']
    
    columns_to_test = ['cont_close_2fgm', 'cont_close_2fga', 'open_close_2fgm', 'open_close_2fga']
    
    for col_name in columns_to_test:
        col_cfg = DB_COLUMNS[col_name]
        transform = col_cfg['player_source']['transformation']
        
        print(f"\n{col_name}:")
        print(f"  Transformation type: {transform['type']}")
        print(f"  Formula: {transform['formula']}")
        
        # Test for Regular Season
        try:
            result = _apply_arithmetic_subtract_per_player(transform, season, season_type=1, season_type_name='Regular Season')
            value = result.get(player_id, 0)
            print(f"  Regular Season: {value}")
        except Exception as e:
            print(f"  Regular Season: ERROR - {e}")

def test_team_close_shots():
    """Test team close shot columns using arithmetic_subtract transformation"""
    print("\n" + "="*80)
    print("TESTING TEAM CLOSE SHOT COLUMNS (arithmetic_subtract)")
    print("="*80)
    
    team_id = TEST_MODE_CONFIG['team_id']
    season = TEST_MODE_CONFIG['season']
    
    columns_to_test = ['cont_close_2fgm', 'cont_close_2fga', 'open_close_2fgm', 'open_close_2fga']
    
    for col_name in columns_to_test:
        col_cfg = DB_COLUMNS[col_name]
        transform = col_cfg['team_source']['transformation']
        
        print(f"\n{col_name}:")
        print(f"  Transformation type: {transform['type']}")
        print(f"  Formula: {transform['formula']}")
        print(f"  Group: {transform['group']}")
        
        # For team transformations, we need to call the endpoint directly
        try:
            # Import the function for league-wide arithmetic subtract
            from src.etl import _apply_arithmetic_subtract_league_wide
            result = _apply_arithmetic_subtract_league_wide(transform, season, 'team', season_type=1, season_type_name='Regular Season')
            value = result.get(team_id, 0)
            print(f"  Regular Season: {value}")
        except Exception as e:
            print(f"  Regular Season: ERROR - {e}")

def test_player_total_shots():
    """Test player total shot columns using filter_aggregate transformation"""
    print("\n" + "="*80)
    print("TESTING PLAYER TOTAL SHOT COLUMNS (filter_aggregate)")
    print("="*80)
    
    player_id = TEST_MODE_CONFIG['player_id']
    
    columns_to_test = ['cont_2fgm', 'cont_2fga', 'open_2fgm', 'open_2fga']
    
    for col_name in columns_to_test:
        col_cfg = DB_COLUMNS[col_name]
        transform = col_cfg['player_source']['transformation']
        
        print(f"\n{col_name}:")
        print(f"  Transformation type: {transform['type']}")
        print(f"  Result set: {transform['result_set']}")
        print(f"  Filter: {transform['filter_values']}")

def test_team_total_shots():
    """Test team total shot columns using per-team execution"""
    print("\n" + "="*80)
    print("TESTING TEAM TOTAL SHOT COLUMNS (per-team execution)")
    print("="*80)
    
    team_id = TEST_MODE_CONFIG['team_id']
    
    columns_to_test = ['cont_2fgm', 'cont_2fga', 'open_2fgm', 'open_2fga']
    
    for col_name in columns_to_test:
        col_cfg = DB_COLUMNS[col_name]
        team_source = col_cfg['team_source']
        
        print(f"\n{col_name}:")
        print(f"  Endpoint: {team_source['endpoint']}")
        print(f"  Execution tier: {team_source['execution_tier']}")
        print(f"  Result set: {team_source['result_set']}")
        print(f"  Defender category: {team_source.get('defender_distance_category', 'N/A')}")
        print(f"  Field: {team_source['field']}")

def compare_results():
    """Compare close vs total shots to verify subtraction worked"""
    print("\n" + "="*80)
    print("COMPARISON: Close vs Total Shots")
    print("="*80)
    print("\nFor correct implementation:")
    print("  ✓ Close shots should be < Total shots")
    print("  ✓ Contested should ≠ Open")
    print("\nRun full ETL to get database values, then check with test_result_set_subtraction.py")

if __name__ == '__main__':
    print("\n" + "="*80)
    print(f"Testing for: {TEST_MODE_CONFIG['player_name']} / {TEST_MODE_CONFIG['team_name']}")
    print(f"Player ID: {TEST_MODE_CONFIG['player_id']}")
    print(f"Team ID: {TEST_MODE_CONFIG['team_id']}")
    print(f"Season: {TEST_MODE_CONFIG['season']}")
    print("="*80)
    
    # Test each category
    test_player_close_shots()
    test_team_close_shots()
    test_player_total_shots()
    test_team_total_shots()
    compare_results()
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
