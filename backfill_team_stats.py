"""
One-time script to backfill team_season_stats using endpoint-by-endpoint strategy.

This uses the same 4-retry, 1-failure restart strategy as the player backfill.
Processes all team endpoints systematically for all seasons.

Usage:
    python3 backfill_team_stats.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.etl import ETLContext
from lib.etl import (
    get_endpoint_processing_order,
    calculate_current_season,
    get_backfill_status,
    get_endpoint_parameter_combinations,
    get_columns_for_endpoint_params,
    infer_execution_tier_from_endpoint
)
from config.etl import ENDPOINTS_CONFIG, SEASON_TYPE_CONFIG, NBA_CONFIG
from src.etl import run_endpoint_backfill

def backfill_team_stats(start_season: str = None):
    """
    Backfill team_season_stats using endpoint-by-endpoint strategy.
    
    Same strategy as player backfill:
    - Process each endpoint completely before moving to next
    - For each endpoint: all seasons, all season types
    - Track progress in backfill_endpoint_tracker
    - Auto-resume on restart
    """
    if start_season is None:
        start_season = NBA_CONFIG['backfill_start_season']
    
    current_season = calculate_current_season()
    
    # Parse season years
    start_year = int('20' + start_season.split('-')[1])
    current_year = int('20' + current_season.split('-')[1])
    
    # Generate all seasons
    all_seasons = []
    for year in range(start_year, current_year + 1):
        season_str = f"{year-1}-{str(year)[-2:]}"
        all_seasons.append(season_str)
    
    # Get ordered endpoint list and filter to TEAM endpoints only
    all_endpoints = get_endpoint_processing_order()
    
    # Filter endpoints to only those that have 'team' in entity_types
    team_endpoints = []
    for endpoint_name in all_endpoints:
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint_name, {})
        entity_types = endpoint_config.get('entity_types', [])
        if 'team' in entity_types:
            team_endpoints.append(endpoint_name)
    
    print("=" * 70)
    print("TEAM STATS BACKFILL")
    print("=" * 70)
    print(f"Endpoints: {len(team_endpoints)}")
    print(f"Seasons: {start_season} to {current_season}")
    print(f"Total combinations: {len(team_endpoints) * len(all_seasons) * 3}")  # 3 season types
    print()
    print("Team endpoints to process:")
    for ep in team_endpoints:
        print(f"  - {ep}")
    print()
    
    ctx = ETLContext()
    total_processed = 0
    total_failed = 0
    
    # Process each endpoint completely before moving to next
    for endpoint_name in team_endpoints:
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint_name, {})
        min_season = endpoint_config.get('min_season')
        
        # Infer scope from endpoint name
        scope = infer_execution_tier_from_endpoint(endpoint_name)
        
        # Get all parameter combinations for this endpoint
        param_combinations = get_endpoint_parameter_combinations(endpoint_name, 'team')
        
        print("=" * 70)
        print(f"ENDPOINT: {endpoint_name} (scope: {scope}, {len(param_combinations)} param combinations)")
        print("=" * 70)
        
        # Process each parameter combination
        for params in param_combinations:
            # Build param description for logging
            param_desc = ""
            if params:
                param_parts = []
                for key, value in sorted(params.items()):
                    if not key.startswith('_'):
                        param_parts.append(f"{key}={value}")
                if param_parts:
                    param_desc = f" [{', '.join(param_parts)}]"
            
            # Process all seasons for this endpoint+params combination
            for season in all_seasons:
                # Skip if season is before endpoint's minimum season
                if min_season and season < min_season:
                    continue
                
                # Process all season types
                prev_processed = False
                for season_type_idx, (season_type_name, config) in enumerate(SEASON_TYPE_CONFIG.items()):
                    season_type = config['season_code']
                    season_type_min = config.get('minimum_season')
                    
                    # Skip if season is before this season type's minimum
                    if season_type_min and season < season_type_min:
                        continue
                    
                    # Add empty line between season types if previous was processed
                    if season_type_idx > 0 and prev_processed:
                        print()
                    
                    # Check if already complete
                    status = get_backfill_status(endpoint_name, season, season_type, params)
                    if status and status['status'] == 'complete':
                        columns = get_columns_for_endpoint_params(endpoint_name, params, 'team')
                        columns_str = f" ({', '.join(columns)})" if columns else ""
                        
                        # Get count from status
                        team_count = status.get('teams_total', 0)
                        print(f"Already complete: {season} {season_type_name} {endpoint_name}{param_desc}{columns_str} - {team_count} teams")
                        prev_processed = True
                        continue
                    
                    # Process this season/type/params combination
                    success = run_endpoint_backfill(
                        ctx=ctx,
                        endpoint=endpoint_name,
                        season=season,
                        season_type=season_type,
                        scope=scope,
                        params=params
                    )
                    
                    if success:
                        total_processed += 1
                        prev_processed = True
                    else:
                        total_failed += 1
                        print(f"  FAILED: {season} {season_type_name}")
                        # Continue to next season type
    
    print()
    print("=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"Processed: {total_processed} combinations")
    print(f"Failed: {total_failed} combinations")
    print()

if __name__ == '__main__':
    print()
    print("Starting team stats backfill...")
    print("This will process ALL team endpoints for ALL seasons")
    print("The backfill will automatically resume if interrupted")
    print()
    
    try:
        backfill_team_stats()
    except KeyboardInterrupt:
        print("\n\nBackfill interrupted by user")
        print("Run again to resume where it left off")
    except Exception as e:
        print(f"\n\nBackfill failed: {e}")
        print("Run again to resume where it left off")
        import traceback
        traceback.print_exc()
