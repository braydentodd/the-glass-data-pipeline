"""
Flask API for interactive NBA stat calculations
Provides endpoints for switching between stat modes (totals, per-game, per-100, etc.)
"""

import sys
import os
import subprocess
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

from config.etl import DB_CONFIG, NBA_CONFIG
from config.sheets import API_CONFIG, SERVER_CONFIG
from lib.etl import get_table_name, get_teams_from_db
from lib.sheets import (
    calculate_entity_stats,
    get_reverse_stats,
    get_editable_fields,
    get_config_for_export,
    fetch_players_for_team,
    fetch_all_players,
    fetch_team_stats,
    calculate_all_percentiles,
    build_entity_row,
    build_sheet_columns,
    get_db_connection as _get_db_conn,
    SHEETS_COLUMNS,
)

# Load NBA teams from DB at startup (team_id -> (abbr, name))
_teams_db = get_teams_from_db()
NBA_TEAMS_BY_ID = {tid: name for tid, (abbr, name) in _teams_db.items()}
NBA_TEAMS_BY_ABBR = {abbr: tid for tid, (abbr, name) in _teams_db.items()}

# Get reverse stats and editable fields from config
REVERSE_STATS = get_reverse_stats()
EDITABLE_FIELDS = get_editable_fields()

# Stat columns for percentile calculation
STAT_COLUMNS = [k for k, v in SHEETS_COLUMNS.items() if v.get('is_stat', False)]

app = Flask(__name__)

# Enable CORS if configured
if API_CONFIG['cors_enabled']:
    CORS(app)


def get_db_connection():
    """Create database connection."""
    return _get_db_conn()


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'service': 'nba-stats-api'})


@app.route('/api/sync-historical-stats', methods=['POST'])
def sync_historical_stats():
    """
    Trigger historical stats sync with configuration from Apps Script.
    
    Request body:
    {
        "mode": "years"|"seasons"|"career",
        "years": 3,  // for years mode
        "seasons": ["2024-25", "2023-24"],  // for seasons mode
        "include_current": true|false
    }
    """
    try:
        data = request.json
        mode = data.get('mode', 'years')
        years = data.get('years', 3)
        seasons = data.get('seasons', [])
        include_current = data.get('include_current', False)
        stats_mode = data.get('stats_mode', 'per_100')  # Get stats mode from request
        stats_custom_value = data.get('stats_custom_value')  # Get custom value if present
        toggle_percentiles = data.get('toggle_percentiles', False)  # Toggle flag from Apps Script
        # Note: show_percentiles is parsed from sheet header, not passed as parameter
        priority_team = data.get('priority_team')  # Optional: team to process first
        sync_section = data.get('sync_section')  # Optional: 'historical', 'postseason', or None for full sync (default: None)
        
        # Build environment variables for sync script
        env = os.environ.copy()
        env['HISTORICAL_MODE'] = mode
        env['INCLUDE_CURRENT_YEAR'] = 'true' if include_current else 'false'
        env['STATS_MODE'] = stats_mode  # Pass stats mode to sync script
        # Note: SHOW_PERCENTILES removed - Python will parse from sheet header
        
        # Pass toggle flag if requested
        if toggle_percentiles:
            env['TOGGLE_PERCENTILES'] = 'true'
        
        # Only set SYNC_SECTION if explicitly requested (for partial syncs)
        # If not set, Python script will do a FULL sync (current + historical + postseason)
        if sync_section:
            env['SYNC_SECTION'] = sync_section
        
        if stats_custom_value:
            env['STATS_CUSTOM_VALUE'] = str(stats_custom_value)
        
        if priority_team:
            env['PRIORITY_TEAM_ABBR'] = priority_team.upper()
        
        # Handle both 'season' (singular) and 'seasons' (plural) for compatibility
        if mode == 'season' or mode == 'seasons':
            env['HISTORICAL_MODE'] = 'seasons'  # Normalize to plural
            env['HISTORICAL_SEASONS'] = ','.join(str(s) for s in seasons)  # Convert all to strings
        else:
            env['HISTORICAL_YEARS'] = str(years)
        
        # Get the project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Build command arguments - run as module to ensure imports work
        cmd = [sys.executable, '-m', 'src.sheets']
        
        # Add priority team as CLI argument if specified
        if priority_team:
            cmd += ['--team', priority_team.upper()]
        
        # Ensure DB_PASSWORD is in environment (required by sync script)
        if 'DB_PASSWORD' not in env:
            env['DB_PASSWORD'] = os.environ.get('DB_PASSWORD', '')
        
        # Run the sheets_sync module
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_root,
            env=env,
            timeout=600  # Increased to 10 minutes for all 30 teams
        )
        
        # Log subprocess output for debugging
        if result.stdout:
            print(f"[SYNC OUTPUT] {result.stdout}", file=sys.stderr, flush=True)
        if result.stderr:
            print(f"[SYNC STDERR] {result.stderr}", file=sys.stderr, flush=True)
        
        if result.returncode == 0:
            return jsonify({
                'success': True,
                'message': 'Historical stats synced successfully'
            })
        else:
            # Get detailed error information
            error_msg = f"Sync failed (exit code {result.returncode})"
            if result.stderr:
                error_msg += f": {result.stderr[:1000]}"  # First 1000 chars
            
            return jsonify({
                'success': False,
                'error': error_msg,
                'stderr': result.stderr[:3000] if result.stderr else '',
                'stdout': result.stdout[:3000] if result.stdout else ''
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Sync timed out after 10 minutes'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/sync-postseason-stats', methods=['POST'])
@app.route('/api/sync-playoff-stats', methods=['POST'])  # Backward compatibility alias
def sync_postseason_stats():
    """
    Trigger postseason stats sync (playoffs + play-in) with configuration from Apps Script.
    
    Request body:
    {
        "mode": "years"|"seasons"|"career",
        "years": 3,  // for years mode
        "seasons": ["2024-25", "2023-24"],  // for seasons mode
        "stats_mode": "per_36",
        "stats_custom_value": 75,  // optional
        "show_percentiles": true|false
    }
    """
    try:
        data = request.json
        mode = data.get('mode', 'career')
        years = data.get('years', 25)
        seasons = data.get('seasons', [])
        stats_mode = data.get('stats_mode', 'per_100')
        stats_custom_value = data.get('stats_custom_value')
        # Note: show_percentiles is parsed from sheet header, not passed as parameter
        priority_team = data.get('priority_team')
        
        # Build environment variables for sync script
        env = os.environ.copy()
        env['HISTORICAL_MODE'] = mode
        env['INCLUDE_CURRENT_YEAR'] = 'false'  # Postseason never includes current
        env['STATS_MODE'] = stats_mode
        env['SEASON_TYPE'] = '2,3'  # 2 = Playoffs, 3 = Play-in
        # Note: SHOW_PERCENTILES removed - Python will parse from sheet header
        env['SYNC_SECTION'] = 'postseason'  # Tell sync script to write to postseason columns
        
        if stats_custom_value:
            env['STATS_CUSTOM_VALUE'] = str(stats_custom_value)
        
        if priority_team:
            env['PRIORITY_TEAM_ABBR'] = priority_team.upper()
        
        # Handle both 'season' (singular) and 'seasons' (plural)
        if mode == 'season' or mode == 'seasons':
            env['HISTORICAL_MODE'] = 'seasons'
            env['HISTORICAL_SEASONS'] = ','.join(str(s) for s in seasons)  # Convert all to strings
        else:
            env['HISTORICAL_YEARS'] = str(years)
        
        # Get the project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Build command arguments
        cmd = [sys.executable, '-m', 'src.sheets']
        
        if priority_team:
            cmd += ['--team', priority_team.upper()]
        
        # Ensure DB_PASSWORD is in environment
        if 'DB_PASSWORD' not in env:
            env['DB_PASSWORD'] = os.environ.get('DB_PASSWORD', '')
        
        # Run the sheets_sync module
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=project_root,
            env=env,
            timeout=600
        )
        
        if result.returncode == 0:
            return jsonify({
                'success': True,
                'message': 'Postseason stats synced successfully'
            })
        else:
            error_msg = f"Sync failed (exit code {result.returncode})"
            if result.stderr:
                error_msg += f": {result.stderr[:1000]}"
            
            return jsonify({
                'success': False,
                'error': error_msg,
                'stderr': result.stderr[:3000] if result.stderr else '',
                'stdout': result.stdout[:3000] if result.stdout else ''
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Sync timed out after 10 minutes'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/teams', methods=['GET'])
def get_teams():
    """Get list of all NBA teams."""
    teams_list = [{'id': team_id, 'name': team_name} 
                  for team_id, team_name in NBA_TEAMS_BY_ID.items()]
    return jsonify({'teams': teams_list})


@app.route('/api/stats', methods=['POST'])
def calculate_stats():
    """
    Calculate stats for a team in specified mode.
    
    Request body:
    {
        "team_id": 1610612738,
        "mode": "per_100",  # totals, per_game, per_100, per_36, per_minutes, per_possessions
        "custom_value": 75,  # Optional: for per_minutes or per_possessions
        "season": "2024-25"  # Optional: defaults to current season
    }
    
    Response:
    {
        "team_id": 1610612738,
        "team_name": "Boston Celtics",
        "mode": "per_100",
        "players": [
            {
                "player_name": "Jayson Tatum",
                "calculated_stats": {...},
                "percentiles": {...}
            },
            ...
        ]
    }
    """
    data = request.get_json()
    
    # Validate request
    team_id = data.get('team_id')
    mode = data.get('mode', 'per_100')
    custom_value = data.get('custom_value')
    season = data.get('season', NBA_CONFIG['current_season'])
    
    if not team_id:
        return jsonify({'error': 'team_id is required'}), 400
    
    if team_id not in NBA_TEAMS_BY_ID:
        return jsonify({'error': 'Invalid team_id'}), 400
    
    valid_modes = ['totals', 'per_game', 'per_100', 'per_36', 'per_minutes', 'per_possessions']
    if mode not in valid_modes:
        return jsonify({'error': f'Invalid mode. Must be one of: {valid_modes}'}), 400
    
    if mode in ['per_minutes', 'per_possessions'] and custom_value is None:
        return jsonify({'error': f'{mode} requires custom_value parameter'}), 400

    # Resolve team_id -> team_abbr
    team_abbr = None
    for abbr, tid in NBA_TEAMS_BY_ABBR.items():
        if tid == team_id:
            team_abbr = abbr
            break
    if not team_abbr:
        return jsonify({'error': 'Team not found in database'}), 404

    try:
        conn = get_db_connection()

        # Use lib.sheets fetch + calculate (config-driven, no hardcoded SQL)
        players = fetch_players_for_team(conn, team_abbr, 'current_stats')
        all_players = fetch_all_players(conn, 'current_stats')
        conn.close()

        # Calculate percentile populations
        percentile_pops = calculate_all_percentiles(all_players, 'player', mode)

        # Build column list (for context_section blanking)
        columns = build_sheet_columns(entity='player', stat_mode='both', show_percentiles=False)

        # Build rows using lib.sheets (same as sync path)
        player_rows = []
        for p in players:
            stats = calculate_entity_stats(p, 'player', mode, custom_value)
            player_rows.append({
                'player_id': p.get('player_id'),
                'name': p.get('name'),
                'team_abbr': p.get('team_abbr'),
                'calculated_stats': stats,
            })

        return jsonify({
            'team_id': team_id,
            'team_abbr': team_abbr,
            'team_name': NBA_TEAMS_BY_ID.get(team_id, team_abbr),
            'mode': mode,
            'season': season,
            'players': player_rows,
            'custom_value': custom_value,
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/team/<int:team_id>/players', methods=['GET'])
def get_team_players(team_id):
    """
    Get list of players for a team.
    
    Response:
    {
        "team_id": 1610612738,
        "team_name": "Boston Celtics",
        "players": [
            {
                "player_id": 1627759,
                "name": "Jayson Tatum"
            },
            ...
        ]
    }
    """
    if team_id not in NBA_TEAMS_BY_ID:
        return jsonify({'error': 'Invalid team_id'}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT player_id, name
            FROM players
            WHERE team_id = %s
              AND team_id IS NOT NULL
            ORDER BY name
        """
        
        cursor.execute(query, (team_id,))
        players = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'team_id': team_id,
            'team_name': NBA_TEAMS_BY_ID[team_id],
            'players': [dict(p) for p in players]
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<int:player_id>/stats', methods=['GET'])
def get_player_stats(player_id):
    """
    Get stats for a specific player in all modes.
    
    Query params:
    - season: Season (default: 2024-25)
    
    Response includes stats calculated in all modes for comparison.
    """
    season = request.args.get('season', NBA_CONFIG['current_season'])
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        player_stats_table = get_table_name('player', 'stats')
        query = f"""
            SELECT *
            FROM {player_stats_table}
            WHERE player_id = %s AND season = %s
        """
        
        cursor.execute(query, (player_id, season))
        player = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not player:
            return jsonify({'error': 'Player not found'}), 404
        
        player_dict = dict(player)

        # Calculate stats in all modes using config-driven engine
        modes = {
            'totals': calculate_entity_stats(player_dict, 'player', mode='totals'),
            'per_game': calculate_entity_stats(player_dict, 'player', mode='per_game'),
            'per_100': calculate_entity_stats(player_dict, 'player', mode='per_100'),
            'per_36': calculate_entity_stats(player_dict, 'player', mode='per_36'),
        }
        
        return jsonify({
            'player_id': player_id,
            'player_name': player_dict['player_name'],
            'team_name': NBA_TEAMS_BY_ID.get(player_dict['team_id'], 'Unknown'),
            'season': season,
            'modes': modes
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/player/<int:player_id>', methods=['PATCH'])
def update_player(player_id):
    """
    Update player information (wingspan, notes, etc.)
    
    Request body:
    {
        "wingspan_inches": 84,  // Optional
        "notes": "Great defender"  // Optional
    }
    """
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    # Build dynamic update query based on provided fields
    allowed_fields = EDITABLE_FIELDS
    updates = []
    values = []
    
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            values.append(data[field])
    
    if not updates:
        return jsonify({'error': 'No valid fields to update'}), 400
    
    # Add updated_at timestamp
    updates.append("updated_at = CURRENT_TIMESTAMP")
    
    # Add player_id for WHERE clause
    values.append(player_id)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        returning_fields = ['player_id', 'name'] + allowed_fields
        query = f"""
            UPDATE players 
            SET {', '.join(updates)}
            WHERE player_id = %s
            RETURNING {', '.join(returning_fields)}
        """
        
        cursor.execute(query, values)
        updated_player = cursor.fetchone()
        
        if not updated_player:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Player not found'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        result = {'success': True}
        for i, field in enumerate(returning_fields):
            result[field] = updated_player[i]
        return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/teams/<int:team_id>', methods=['PUT'])
def update_team(team_id):
    """
    Update team information (notes, etc.)
    
    Request body:
    {
        "notes": "Championship contenders"  // Optional
    }
    """
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    # Build dynamic update query based on provided fields
    allowed_fields = ['notes']  # Only notes is editable for teams
    updates = []
    values = []
    
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            values.append(data[field])
    
    if not updates:
        return jsonify({'error': 'No valid fields to update'}), 400
    
    # Add updated_at timestamp
    updates.append("updated_at = CURRENT_TIMESTAMP")
    
    # Add team_id for WHERE clause
    values.append(team_id)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = f"""
            UPDATE teams 
            SET {', '.join(updates)}
            WHERE team_id = %s
            RETURNING team_id, abbreviation, full_name, notes
        """
        
        cursor.execute(query, values)
        updated_team = cursor.fetchone()
        
        if not updated_team:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Team not found'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'team_id': updated_team[0],
            'abbreviation': updated_team[1],
            'full_name': updated_team[2],
            'notes': updated_team[3]
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config', methods=['GET'])
def get_config():
    """
    Single source of truth for Apps Script.
    Returns all config Apps Script needs: teams, column ranges, stat lists,
    colors, editable fields, API base URL. Zero hardcoding in Apps Script.
    """
    mode = request.args.get('mode', 'per_100')
    try:
        return jsonify(get_config_for_export(mode=mode))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/column-config', methods=['GET'])
def get_column_config():
    """Alias for /api/config for backward compatibility."""
    return get_config()


if __name__ == '__main__':
    app.run(
        host=API_CONFIG.get('host', '0.0.0.0'),
        port=API_CONFIG.get('port', 5000),
        debug=API_CONFIG.get('debug', False),
    )
