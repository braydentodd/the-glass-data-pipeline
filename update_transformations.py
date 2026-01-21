"""
Update transformation columns for all seasons.

Transformation columns are computed from already-fetched API data.
This script processes all seasons to populate these computed columns.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.etl import ETLContext, update_transformation_columns
from lib.etl import calculate_current_season
from config.etl import NBA_CONFIG, SEASON_TYPE_CONFIG

def update_all_transformations():
    """Update transformation columns for all seasons and season types."""
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
    
    print("=" * 70)
    print("TRANSFORMATION COLUMNS UPDATE")
    print("=" * 70)
    print(f"Seasons: {start_season} to {current_season}")
    print(f"Total seasons: {len(all_seasons)}")
    print(f"Season types: {list(SEASON_TYPE_CONFIG.keys())}")
    print()
    print("This will update transformation columns for:")
    print("  - Player stats (playerdashptshots, playerdashptreb, etc.)")
    print("  - Team stats (teamdashptshots, teamdashptreb, etc.)")
    print()
    
    ctx = ETLContext()
    
    total_updated = 0
    total_seasons_processed = 0
    
    # Process each entity type
    for entity in ['player', 'team']:
        print(f"\n{'=' * 70}")
        print(f"PROCESSING {entity.upper()} TRANSFORMATIONS")
        print(f"{'=' * 70}\n")
        
        for season in all_seasons:
            for season_type_name, config in SEASON_TYPE_CONFIG.items():
                season_type = config['season_code']
                season_type_min = config.get('minimum_season')
                
                # Skip if season is before this season type's minimum
                if season_type_min:
                    season_year = int('20' + season.split('-')[1])
                    min_year = int('20' + season_type_min.split('-')[1])
                    if season_year < min_year:
                        continue
                
                print(f"Processing {season} {season_type_name} ({entity})...")
                
                try:
                    updated = update_transformation_columns(
                        ctx=ctx,
                        season=season,
                        entity=entity,
                        season_type=season_type,
                        season_type_name=season_type_name
                    )
                    
                    if updated > 0:
                        print(f"  ✅ Updated {updated} {entity}s")
                        total_updated += updated
                    else:
                        print(f"  ⏭️  No updates needed")
                    
                    total_seasons_processed += 1
                    
                except Exception as e:
                    print(f"  ❌ ERROR: {e}")
                    import traceback
                    traceback.print_exc()
    
    print("\n" + "=" * 70)
    print("TRANSFORMATION UPDATE COMPLETE")
    print("=" * 70)
    print(f"Seasons processed: {total_seasons_processed}")
    print(f"Total {entity}s updated: {total_updated}")
    print()


if __name__ == '__main__':
    print("\n")
    
    try:
        update_all_transformations()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        print("Progress has been saved. Run again to resume.")
    except Exception as e:
        print(f"\n\nFailed: {e}")
        import traceback
        traceback.print_exc()
