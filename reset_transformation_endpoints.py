"""
Reset backfill tracker for endpoints with transformation columns.

This allows re-running those endpoints to populate the missing transformation data.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from lib.etl import get_db_connection
from config.etl import DB_COLUMNS, ENDPOINTS_CONFIG

def find_endpoints_with_transformations():
    """Find all endpoints that have transformation columns."""
    endpoints_with_transforms = set()
    
    for col_name, col_config in DB_COLUMNS.items():
        if not isinstance(col_config, dict):
            continue
        if col_config.get('table') != 'stats':
            continue
        
        # Check player source for transformations
        player_source = col_config.get('player_source', {})
        if isinstance(player_source, dict) and player_source.get('transformation'):
            transformation = player_source['transformation']
            endpoint = transformation.get('endpoint')
            if endpoint:
                endpoints_with_transforms.add((endpoint, 'player'))
        
        # Check team source for transformations
        team_source = col_config.get('team_source', {})
        if isinstance(team_source, dict) and team_source.get('transformation'):
            transformation = team_source['transformation']
            endpoint = transformation.get('endpoint')
            if endpoint:
                endpoints_with_transforms.add((endpoint, 'team'))
    
    return sorted(endpoints_with_transforms)


def reset_endpoints(endpoints_to_reset, dry_run=True):
    """Reset the backfill tracker for specific endpoints."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("RESETTING TRANSFORMATION ENDPOINTS")
    print("=" * 70)
    print()
    
    for endpoint, entity in endpoints_to_reset:
        # Get endpoint config to check minimum season
        endpoint_config = ENDPOINTS_CONFIG.get(endpoint, {})
        min_season = endpoint_config.get('min_season', 'N/A')
        
        # Count how many records will be affected
        cursor.execute("""
            SELECT COUNT(*) 
            FROM backfill_endpoint_tracker 
            WHERE endpoint = %s AND entity = %s
        """, (endpoint, entity))
        count = cursor.fetchone()[0]
        
        print(f"{endpoint} (entity: {entity})")
        print(f"  Min season: {min_season}")
        print(f"  Records to reset: {count}")
        
        if not dry_run:
            cursor.execute("""
                DELETE FROM backfill_endpoint_tracker 
                WHERE endpoint = %s AND entity = %s
            """, (endpoint, entity))
            deleted = cursor.rowcount
            print(f"  ✅ Deleted {deleted} records")
        else:
            print(f"  🔍 DRY RUN - would delete {count} records")
        print()
    
    if not dry_run:
        conn.commit()
        print("✅ Tracker reset complete! Re-run the backfill to populate transformations.\n")
    else:
        print("🔍 DRY RUN complete. Run with --execute to actually reset.\n")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Reset backfill tracker for transformation endpoints')
    parser.add_argument('--execute', action='store_true', 
                       help='Actually reset the tracker (default is dry run)')
    args = parser.parse_args()
    
    print("\n")
    
    endpoints = find_endpoints_with_transformations()
    
    print("Endpoints with transformation columns:\n")
    for endpoint, entity in endpoints:
        print(f"  - {endpoint} ({entity})")
    print()
    
    if args.execute:
        confirm = input("Are you sure you want to reset these endpoints? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)
    
    try:
        reset_endpoints(endpoints, dry_run=not args.execute)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
