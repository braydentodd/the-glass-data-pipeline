#!/usr/bin/env python3
"""
Test script to isolate the leaguehustlestatsplayer endpoint issue.
"""
from nba_api.stats.endpoints import leaguehustlestatsplayer
import time

def test_hustle_endpoint():
    """Test different parameter combinations for leaguehustlestatsplayer"""
    
    season = '2023-24'
    
    print("=" * 70)
    print("Testing leaguehustlestatsplayer endpoint")
    print("=" * 70)
    
    # Test 1: With season_type_all_star (what we might be using)
    print("\nTest 1: Using season_type_all_star='Regular Season'")
    try:
        endpoint = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            season_type_all_star='Regular Season',
            timeout=30
        )
        result = endpoint.get_dict()
        print(f"✓ SUCCESS - Got {len(result['resultSets'])} result sets")
        if result['resultSets']:
            print(f"  First result set: {result['resultSets'][0]['name']}")
            print(f"  Row count: {len(result['resultSets'][0]['rowSet'])}")
    except Exception as e:
        print(f"✗ FAILED: {e}")
    
    time.sleep(2)
    
    # Test 2: With per_mode_time (the correct parameter for hustle stats)
    print("\nTest 2: Using per_mode_time='PerGame'")
    try:
        endpoint = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            per_mode_time='PerGame',
            timeout=30
        )
        result = endpoint.get_dict()
        print(f"✓ SUCCESS - Got {len(result['resultSets'])} result sets")
        if result['resultSets']:
            print(f"  First result set: {result['resultSets'][0]['name']}")
            print(f"  Row count: {len(result['resultSets'][0]['rowSet'])}")
    except Exception as e:
        print(f"✗ FAILED: {e}")
    
    time.sleep(2)
    
    # Test 3: No optional parameters (defaults)
    print("\nTest 3: Using only required parameters (season)")
    try:
        endpoint = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            timeout=30
        )
        result = endpoint.get_dict()
        print(f"✓ SUCCESS - Got {len(result['resultSets'])} result sets")
        if result['resultSets']:
            print(f"  First result set: {result['resultSets'][0]['name']}")
            print(f"  Row count: {len(result['resultSets'][0]['rowSet'])}")
    except Exception as e:
        print(f"✗ FAILED: {e}")
    
    time.sleep(2)
    
    # Test 4: Check what parameters this endpoint actually accepts
    print("\nTest 4: Checking endpoint signature")
    import inspect
    sig = inspect.signature(leaguehustlestatsplayer.LeagueHustleStatsPlayer.__init__)
    print("Available parameters:")
    for param_name, param in sig.parameters.items():
        if param_name not in ['self', 'proxy', 'headers', 'timeout', 'get_request']:
            default = param.default if param.default != inspect.Parameter.empty else "REQUIRED"
            print(f"  - {param_name}: {default}")
    
    print("\n" + "=" * 70)
    print("Test complete!")
    print("=" * 70)

if __name__ == '__main__':
    test_hustle_endpoint()
