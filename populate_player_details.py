"""
One-time script to populate player entity details (height, weight, wingspan, birthdate, pre_nba_team).

This script fetches details for ALL players in the players table and populates:
- height_inches
- weight_lbs
- wingspan_inches
- birthdate
- pre_nba_team

Usage:
    python3 populate_player_details.py
"""
import sys
import os
import time
from nba_api.stats.endpoints import commonplayerinfo, DraftCombinePlayerAnthro

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from lib.etl import (
    get_db_connection, get_table_name,
    safe_int, parse_height, parse_birthdate, safe_str, get_season_year
)
from config.etl import API_CONFIG, DB_COLUMNS

def populate_player_details():
    """
    Fetch and populate entity details for all players in the database.
    """
    print("=" * 70)
    print("POPULATE PLAYER ENTITY DETAILS")
    print("=" * 70)
    print("This will fetch details for ALL players in the players table")
    print()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    players_table = get_table_name('player', 'entity')
    
    # Get detail field mappings from config first
    detail_fields = {}
    for col_name, col_config in DB_COLUMNS.items():
        if not isinstance(col_config, dict):
            continue
        if (col_config.get('table') == 'entity' and 
            col_config.get('player_source')):
            player_source = col_config['player_source']
            if player_source.get('endpoint') == 'commonplayerinfo':
                detail_fields[col_name] = {
                    'api_field': player_source.get('field'),
                    'transform': player_source.get('transform', 'safe_str')
                }
    
    print(f"Will populate fields: {', '.join(sorted(detail_fields.keys()))}")
    print()
    
    # Get only players that have at least one missing detail field or missing wingspan
    field_names = list(detail_fields.keys()) + ['wingspan_inches']
    null_conditions = ' OR '.join([f"{field} IS NULL" for field in field_names])
    
    cursor.execute(f"""
        SELECT player_id, name, {', '.join(field_names)}
        FROM {players_table}
        WHERE {null_conditions}
        ORDER BY player_id
    """)
    all_players = cursor.fetchall()
    
    print(f"Found {len(all_players)} players with missing data to process")
    print()
    
    # Track stats
    processed = 0
    updated = 0
    failed = 0
    wingspans_found = 0
    
    RATE_LIMIT = 0.6
    BATCH_SIZE = 50
    
    # Map field names to their position in the result tuple (after player_id and name)
    field_positions = {field: idx + 2 for idx, field in enumerate(field_names)}
    
    for player_row in all_players:
        processed += 1
        player_id = player_row[0]
        player_name = player_row[1]
        
        # Determine which detail fields are missing
        missing_detail_fields = []
        for field_name in detail_fields.keys():
            if player_row[field_positions[field_name]] is None:
                missing_detail_fields.append(field_name)
        
        # Check if wingspan is missing
        wingspan_missing = player_row[field_positions['wingspan_inches']] is None
        
        # Skip if nothing to update
        if not missing_detail_fields and not wingspan_missing:
            continue
        
        if processed % BATCH_SIZE == 0:
            print(f"Progress: {processed}/{len(all_players)} players ({updated} updated, {failed} failed, {wingspans_found} wingspans)")
        
        try:
            # Only fetch commonplayerinfo if we have missing detail fields
            if missing_detail_fields:
                info_endpoint = commonplayerinfo.CommonPlayerInfo(
                    player_id=player_id,
                    timeout=API_CONFIG['timeout_default']
                )
                player_df = info_endpoint.get_data_frames()[0]
                
                if player_df.empty:
                    failed += 1
                    continue
                
                row = player_df.iloc[0]
                
                # Extract rookie year for wingspan search
                rookie_year = None
                from_year_raw = row.get('FROM_YEAR')
                if from_year_raw:
                    try:
                        rookie_year = int(from_year_raw) + 1
                    except (ValueError, TypeError):
                        pass
                
                # Extract and transform ONLY missing detail fields
                update_data = {}
                for db_col_name in missing_detail_fields:
                    field_config = detail_fields[db_col_name]
                    api_field = field_config['api_field']
                    transform_name = field_config['transform']
                    raw_value = row.get(api_field)
                    
                    # Convert numpy types to Python native types for psycopg2
                    if hasattr(raw_value, 'item'):  # numpy scalar
                        raw_value = raw_value.item()
                    
                    if transform_name == 'safe_int':
                        update_data[db_col_name] = safe_int(raw_value)
                    elif transform_name == 'parse_height':
                        update_data[db_col_name] = parse_height(raw_value)
                    elif transform_name == 'parse_birthdate':
                        update_data[db_col_name] = parse_birthdate(raw_value)
                    elif transform_name == 'safe_str':
                        update_data[db_col_name] = safe_str(raw_value)
                    else:
                        update_data[db_col_name] = raw_value
                
                # Build UPDATE statement
                set_clauses = [f"{col} = %s" for col in update_data.keys()]
                set_values = list(update_data.values())
                
                if set_clauses:
                    cursor.execute(f"""
                        UPDATE {players_table}
                        SET {', '.join(set_clauses)}, updated_at = NOW()
                        WHERE player_id = %s
                    """, set_values + [player_id])
                    
                    if cursor.rowcount > 0:
                        updated += 1
                
                time.sleep(RATE_LIMIT)
            else:
                # If no detail fields missing, still need rookie_year for wingspan search
                rookie_year = None
            
            # Only fetch wingspan if it's missing
            if wingspan_missing:
                try:
                    wingspan_found = False
                    
                    if rookie_year:
                        first_combine_year = rookie_year - 1
                    else:
                        first_combine_year = get_season_year() - 1
                    
                    # Check 5 combines
                    for year_offset in range(5):
                        combine_year = first_combine_year - year_offset
                        combine_season = f"{combine_year}-{str(combine_year + 1)[-2:]}"
                        
                        try:
                            endpoint = DraftCombinePlayerAnthro(season_year=combine_season, timeout=10)
                            time.sleep(RATE_LIMIT)
                            result = endpoint.get_dict()
                            
                            for rs in result['resultSets']:
                                if 'PLAYER_ID' in rs['headers'] and 'WINGSPAN' in rs['headers']:
                                    player_id_idx = rs['headers'].index('PLAYER_ID')
                                    wingspan_idx = rs['headers'].index('WINGSPAN')
                                    
                                    for row_data in rs['rowSet']:
                                        if row_data[player_id_idx] == player_id and row_data[wingspan_idx] is not None:
                                            wingspan_inches = round(row_data[wingspan_idx])
                                            cursor.execute(f"""
                                                UPDATE {players_table}
                                                SET wingspan_inches = %s, updated_at = NOW()
                                                WHERE player_id = %s
                                            """, (wingspan_inches, player_id))
                                            wingspans_found += 1
                                            wingspan_found = True
                                            break
                                
                                if wingspan_found:
                                    break
                            
                            if wingspan_found:
                                break
                        
                        except Exception:
                            continue
                
                except Exception:
                    pass  # Wingspan is optional
            
            conn.commit()
            time.sleep(RATE_LIMIT)
            
        except Exception as e:
            failed += 1
            if failed <= 10:  # Only print first 10 errors
                print(f"  Failed to fetch {player_name} (ID: {player_id}): {e}")
            conn.rollback()
            time.sleep(RATE_LIMIT)
    
    cursor.close()
    conn.close()
    
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Processed: {processed} players")
    print(f"Updated: {updated} players")
    print(f"Failed: {failed} players")
    print(f"Wingspans found: {wingspans_found} players")
    print()

if __name__ == '__main__':
    populate_player_details()
