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
import numpy as np

from config.database import DB_CONFIG
from config.sheets import (
    API_CONFIG, NBA_TEAMS, COLUMN_DEFINITIONS,
    get_reverse_stats, get_editable_fields, get_config_for_export,
    STAT_ORDER
)
from src.stat_engine import calculate_entity_stats

# Build lookup dicts from NBA_TEAMS list
NBA_TEAMS_BY_ID = {}
NBA_TEAMS_BY_ABBR = {}
for abbr, name in NBA_TEAMS:
    # NBA team IDs start at 1610612737 (ATL) and increment
    # This is a simplified mapping - ideally should be in database
    team_id = 1610612737 + NBA_TEAMS.index((abbr, name))
    NBA_TEAMS_BY_ID[team_id] = name
    NBA_TEAMS_BY_ABBR[abbr] = team_id

# Get reverse stats and editable fields from config
REVERSE_STATS = get_reverse_stats()
EDITABLE_FIELDS = get_editable_fields()

# Build STAT_COLUMNS list from COLUMN_DEFINITIONS
STAT_COLUMNS = [col for col, defn in COLUMN_DEFINITIONS.items() if defn.get('is_stat', False)]

app = Flask(__name__)

# Enable CORS if configured
if API_CONFIG['cors_enabled']:
    CORS(app)


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )


def calculate_percentiles(players_data, stat_columns):
    """
    Calculate percentiles for all stats across all players.
    
    Args:
        players_data (list): List of player dictionaries with calculated stats
        stat_columns (list): List of stat column names to calculate percentiles for
    
    Returns:
        list: Players with percentiles added
    """
    # Extract calculated stats for all players
    all_stats = {stat: [] for stat in stat_columns}
    
    for player in players_data:
        calculated = player.get('calculated_stats', {})
        for stat in stat_columns:
            value = calculated.get(stat, 0)
            if value is not None and value > 0:  # Only include players with valid stats
                all_stats[stat].append(value)
    
    # Calculate percentiles for each player
    for player in players_data:
        calculated = player['calculated_stats']
        percentiles = {}
        
        for stat in stat_columns:
            value = calculated.get(stat, 0)
            stat_values = all_stats[stat]
            
            if not stat_values or value is None or value == 0:
                percentiles[f'{stat}_percentile'] = 0
            else:
                # Calculate percentile (what percentage of players this player beats)
                if stat in REVERSE_STATS:
                    # For reverse stats (turnovers, fouls), lower is better
                    percentiles[f'{stat}_percentile'] = 100 - np.percentile(stat_values, 
                                                         np.searchsorted(sorted(stat_values), value, side='right') / len(stat_values) * 100)
                else:
                    # For normal stats, higher is better
                    percentiles[f'{stat}_percentile'] = np.searchsorted(sorted(stat_values), value, side='right') / len(stat_values) * 100
        
        player['percentiles'] = percentiles
    
    return players_data


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
        stats_mode = data.get('stats_mode', 'per_36')  # Get stats mode from request
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
        cmd = [sys.executable, '-m', 'src.sheets_sync']
        
        # Add priority team as first argument if specified
        if priority_team:
            cmd.append(priority_team.upper())
        
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
            'error': 'Sync timed out after 5 minutes'
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
        stats_mode = data.get('stats_mode', 'per_36')
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
        cmd = [sys.executable, '-m', 'src.sheets_sync']
        
        if priority_team:
            cmd.append(priority_team.upper())
        
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
    season = data.get('season', '2024-25')
    
    if not team_id:
        return jsonify({'error': 'team_id is required'}), 400
    
    if team_id not in NBA_TEAMS_BY_ID:
        return jsonify({'error': 'Invalid team_id'}), 400
    
    valid_modes = ['totals', 'per_game', 'per_100', 'per_36', 'per_minutes', 'per_possessions']
    if mode not in valid_modes:
        return jsonify({'error': f'Invalid mode. Must be one of: {valid_modes}'}), 400
    
    if mode in ['per_minutes', 'per_possessions'] and custom_value is None:
        return jsonify({'error': f'{mode} requires custom_value parameter'}), 400
    
    # Fetch players from database
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Parse season year from season string (e.g., "2024-25" -> 2025)
        season_year = int('20' + season.split('-')[1])
        
        query = """
            SELECT 
                p.player_id,
                p.name AS player_name,
                s.games_played AS games,
                s.minutes_x10::float / 10 AS minutes,
                s.possessions,
                s.2fgm,
                s.2fga,
                s.3fgm,
                s.3fga,
                s.ftm,
                s.fta,
                s.off_rebounds,
                s.def_rebounds,
                s.off_reb_pct_x1000::float / 1000 AS oreb_pct,
                s.def_reb_pct_x1000::float / 1000 AS dreb_pct,
                s.assists,
                s.turnovers,
                s.steals,
                s.blocks,
                s.fouls
            FROM players p
            LEFT JOIN player_season_stats s 
                ON s.player_id = p.player_id
                AND s.year = %s
                AND s.season_type = 1
            WHERE p.team_id = %s
              AND p.team_id IS NOT NULL
            ORDER BY COALESCE(s.minutes_x10, 0) DESC, p.name
        """
        
        cursor.execute(query, (season_year, team_id))
        players = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert to list of dicts
        players_list = [dict(player) for player in players]
        
        # Separate players with stats from those without
        players_with_stats = [p for p in players_list if p.get('games')]
        players_without_stats = [p for p in players_list if not p.get('games')]
        
        # Calculate stats for players with data using generic engine
        calculated_players = []
        if players_with_stats:
            for player in players_with_stats:
                calculated_stats = calculate_entity_stats(player, STAT_ORDER, mode=mode, custom_value=custom_value)
                player['calculated_stats'] = calculated_stats
                calculated_players.append(player)
        
        # Add players without stats with empty calculated_stats
        for player in players_without_stats:
            calculated_players.append({
                'player_name': player['player_name'],
                'calculated_stats': {},
                'percentiles': {}
            })
        
        # Calculate percentiles only for players with stats
        if calculated_players:
            stat_cols = [col for col in STAT_COLUMNS if col not in ['games']]
            players_with_percentiles = calculate_percentiles(calculated_players, stat_cols)
        
        # Format response
        response = {
            'team_id': team_id,
            'team_name': NBA_TEAMS_BY_ID[team_id],
            'mode': mode,
            'season': season,
            'players': players_with_percentiles
        }
        
        if custom_value:
            response['custom_value'] = custom_value
        
        return jsonify(response)
    
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
    season = request.args.get('season', '2024-25')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT *
            FROM player_season_stats
            WHERE player_id = %s AND season = %s
        """
        
        cursor.execute(query, (player_id, season))
        player = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not player:
            return jsonify({'error': 'Player not found'}), 404
        
        player_dict = dict(player)
        
        # Calculate stats in all modes using generic engine
        modes = {
            'totals': calculate_entity_stats(player_dict, STAT_ORDER, mode='totals'),
            'per_game': calculate_entity_stats(player_dict, STAT_ORDER, mode='per_game'),
            'per_100': calculate_entity_stats(player_dict, STAT_ORDER, mode='per_100_poss'),
            'per_36': calculate_entity_stats(player_dict, STAT_ORDER, mode='per_36'),
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
        
        query = f"""
            UPDATE players 
            SET {', '.join(updates)}
            WHERE player_id = %s
            RETURNING player_id, name, wingspan_inches, notes
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
        
        return jsonify({
            'success': True,
            'player_id': updated_player[0],
            'name': updated_player[1],
            'wingspan_inches': updated_player[2],
            'notes': updated_player[3]
        })
    
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
    Provide client configuration for Apps Script.
    Returns configuration values from centralized config.py
    """
    return jsonify(get_config_for_export())


@app.route('/api/column-config', methods=['GET'])
def get_column_config():
    """
    Expose complete column configuration for Apps Script.
    This is the single source of truth for all column definitions,
    sections, and formatting rules.
    
    Response includes:
    - column_definitions: Complete metadata for every column
    - stat_order: Left-to-right order of stats
    - sections: Section configuration with column ranges
    - sections_nba: Section configuration for NBA sheet (includes TEAM column)
    - reverse_stats: Stats where lower is better
    - editable_fields: Fields that can be edited by user
    - colors: Color scheme for conditional formatting
    - color_thresholds: Percentile thresholds for each color
    """
    return jsonify(get_config_for_export())


if __name__ == '__main__':
    # Run with configuration from config.py
    app.run(
        host=API_CONFIG['host'], 
        port=API_CONFIG['port'], 
        debug=API_CONFIG['debug']
    )
