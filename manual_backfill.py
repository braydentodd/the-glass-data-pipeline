#!/usr/bin/env python3
"""
Manual backfill script for specific players and seasons.
"""
import sys
sys.path.insert(0, '.')

from src.etl import ETLContext, update_basic_stats, update_advanced_stats, update_transformation_columns
from config.etl import NBA_CONFIG, SEASON_TYPE_CONFIG

def main():
    ctx = ETLContext()
    ctx.init_parallel_executor(max_workers=10, endpoint_tier='league')
    
    player_ids = [1627780, 1630202]
    seasons = ['2023-24', '2024-25', '2025-26']
    
    # Next Batch
    # player_ids = [1629636, 1629631, 1628386, 1628378, 1626204]
    # seasons = ['2019-20', '2021-22', '2022-23', '2023-24']
    
    # Next Batch
    # player_ids = [1627832]
    # seasons = ['2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25', '2025-26']
    
    for season in seasons:
        print(f'\n{"="*70}')
        print(f'Processing season {season}')
        print("="*70)
        
        # Temporarily update config for this season
        year = int('20' + season.split('-')[1])
        NBA_CONFIG['current_season'] = season
        NBA_CONFIG['current_season_year'] = year
        
        # Update basic stats for these players
        print(f'\nBasic stats for {season}...')
        update_basic_stats(ctx, 'player', player_ids=player_ids)
        
        # Update advanced stats for these players
        print(f'\nAdvanced stats for {season}...')
        update_advanced_stats(ctx, 'player', season=season)
        
        # Update transformations for all season types
        for season_type_name, season_type_config in SEASON_TYPE_CONFIG.items():
            season_type = season_type_config['season_code']
            print(f'\nTransformations for {season} - {season_type_name}...')
            update_transformation_columns(ctx, season, 'player', season_type=season_type, season_type_name=season_type_name)
    
    print('\n' + '='*70)
    print('MANUAL BACKFILL COMPLETE')
    print('='*70)

if __name__ == '__main__':
    main()
