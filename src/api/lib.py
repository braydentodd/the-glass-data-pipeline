"""
The Glass - Flask API

Serves interactive NBA stat calculations and triggers sheet sync.
Provides endpoints for switching stat modes, fetching player/team data,
and triggering background Google Sheets syncs.

Run with:
    python -m lib.api
"""

import sys
import os
import subprocess
import threading

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

from src.db import get_db_connection as _get_db_conn
from src.sheets.config import SHEETS_COLUMNS
from src.sheets.lib.calculations import (
    calculate_entity_stats,
    calculate_all_percentiles,
)
from src.sheets.lib.db import (
    fetch_players_for_team,
    fetch_all_players,
    fetch_team_stats,
)
from src.sheets.lib.formatting import (
    get_reverse_stats,
    get_editable_fields,
    get_config_for_export,
)
from src.sheets.lib.layout import (
    build_entity_row,
    build_sheet_columns,
)

def get_league_lib(league):
    if league == 'ncaa':
        import etl.ncaa.lib as lib
    else:
        import etl.nba.lib as lib
    return lib

def get_league_config(league):
    if league == 'ncaa':
        from etl.ncaa.config import NCAA_CONFIG as config
    else:
        from etl.nba.config import NBA_CONFIG as config
    return config

def load_teams(league):
    lib = get_league_lib(league)
    db = lib.get_teams_from_db()
    return {tid: name for tid, (abbr, name) in db.items()}, {abbr: tid for tid, (abbr, name) in db.items()}

NBA_TEAMS_BY_ID, NBA_TEAMS_BY_ABBR = load_teams('nba')
NCAA_TEAMS_BY_ID, NCAA_TEAMS_BY_ABBR = load_teams('ncaa')

def get_teams_dicts(league):
    if league == 'ncaa':
        return NCAA_TEAMS_BY_ID, NCAA_TEAMS_BY_ABBR
    return NBA_TEAMS_BY_ID, NBA_TEAMS_BY_ABBR

# Get reverse stats and editable fields from config
REVERSE_STATS = get_reverse_stats()
EDITABLE_FIELDS = get_editable_fields()

# Stat columns for percentile calculation
STAT_COLUMNS = [k for k, v in SHEETS_COLUMNS.items() if v.get('stat_category', 'none') != 'none']

app = Flask(__name__)

# Enable CORS if configured
if os.getenv('API_CORS_ENABLED', 'True').lower() == 'true':
    CORS(app)


def get_db_connection():
    """Create database connection."""
    return _get_db_conn()


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'service': 'nba-stats-api'})


@app.route('/api/update-sheets', methods=['POST'])
def update_sheets():
    """
    Trigger historical stats sync with configuration from Apps Script.
    
    Request body:
    {
        "mode": "seasons"|"career",
        "seasons": ["2024-25", "2023-24"],  // for seasons mode
        "seasons_count": 3,  // alternative: number of recent seasons
        "include_current": true|false
    }
    """
    try:
        data = request.json
        mode = data.get('mode', 'seasons')
        seasons = data.get('seasons', [])
        seasons_count = data.get('seasons_count', 3)
        include_current = data.get('include_current', False)
        stats_mode = data.get('stats_mode', 'per_100')
        show_advanced = data.get('show_advanced', False)
        priority_tab = data.get('priority_tab')
        sync_section = data.get('sync_section')
        partial_update_sync = data.get('partial_update', True)
        
        # Build environment variables for sync script
        env = os.environ.copy()
        env['HISTORICAL_MODE'] = mode
        env['INCLUDE_CURRENT_SEASON'] = 'true' if include_current else 'false'
        env['STATS_MODE'] = stats_mode
        env['SHOW_ADVANCED'] = 'true' if show_advanced else 'false'
        env['PARTIAL_UPDATE'] = 'true' if partial_update_sync else 'false'
        
        if sync_section:
            env['SYNC_SECTION'] = sync_section
        
        if priority_tab:
            env['PRIORITY_TAB'] = priority_tab
        
        # Handle seasons mode
        if mode == 'season' or mode == 'seasons':
            env['HISTORICAL_MODE'] = 'seasons'
            env['HISTORICAL_SEASONS'] = ','.join(str(s) for s in seasons)
        else:
            env['HISTORICAL_SEASONS_COUNT'] = str(seasons_count)
        
        # Get the project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Determine the league from the payload
        league = data.get('league', 'nba')
        
        cmd = [sys.executable, '-m', 'src.sheets.runner', '--league', league]
        
        if priority_tab:
            cmd += ['--tab', priority_tab]
        
        if 'DB_PASSWORD' not in env:
            env['DB_PASSWORD'] = os.environ.get('DB_PASSWORD', '')
        
        def _run_sync_bg(bg_cmd, bg_env, bg_cwd):
            try:
                result = subprocess.run(
                    bg_cmd, capture_output=True, text=True,
                    cwd=bg_cwd, env=bg_env, timeout=600
                )
                if result.stdout:
                    print(f"[SYNC OUTPUT] {result.stdout}", file=sys.stderr, flush=True)
                if result.stderr:
                    print(f"[SYNC STDERR] {result.stderr}", file=sys.stderr, flush=True)
                if result.returncode != 0:
                    print(f"[SYNC FAILED] exit code {result.returncode}", file=sys.stderr, flush=True)
            except subprocess.TimeoutExpired:
                print("[SYNC TIMEOUT] Sync exceeded 10 minute limit", file=sys.stderr, flush=True)
            except Exception as exc:
                print(f"[SYNC ERROR] {exc}", file=sys.stderr, flush=True)

        thread = threading.Thread(
            target=_run_sync_bg, args=(cmd, env, project_root), daemon=True
        )
        thread.start()

        return jsonify({
            'success': True,
            'message': 'Sync started — sheets will update shortly'
        }), 202
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
        "mode": "seasons"|"career",
        "seasons": ["2024-25", "2023-24"],
        "seasons_count": 25,
        "include_current": true|false,
        "stats_mode": "per_36"
    }
    """
    try:
        data = request.json
        mode = data.get('mode', 'career')
        seasons_count = data.get('seasons_count', 25)
        seasons = data.get('seasons', [])
        include_current = data.get('include_current', False)
        stats_mode = data.get('stats_mode', 'per_100')
        priority_tab = data.get('priority_tab')
        
        env = os.environ.copy()
        env['HISTORICAL_MODE'] = mode
        env['INCLUDE_CURRENT_SEASON'] = 'true' if include_current else 'false'
        env['STATS_MODE'] = stats_mode
        env['SEASON_TYPE'] = '2,3'
        env['SHOW_ADVANCED'] = 'true' if data.get('show_advanced', False) else 'false'
        env['SYNC_SECTION'] = 'postseason_stats'
        
        if priority_tab:
            env['PRIORITY_TAB'] = priority_tab
        
        if mode == 'season' or mode == 'seasons':
            env['HISTORICAL_MODE'] = 'seasons'
            env['HISTORICAL_SEASONS'] = ','.join(str(s) for s in seasons)
        else:
            env['HISTORICAL_SEASONS_COUNT'] = str(seasons_count)
        
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        league = data.get('league', 'nba')
        cmd = [sys.executable, '-m', 'src.sheets.runner', '--league', league]
        
        if priority_tab:
            cmd += ['--tab', priority_tab]
        
        if 'DB_PASSWORD' not in env:
            env['DB_PASSWORD'] = os.environ.get('DB_PASSWORD', '')
        
        def _run_post_sync(bg_cmd, bg_env, bg_cwd):
            try:
                result = subprocess.run(
                    bg_cmd, capture_output=True, text=True,
                    cwd=bg_cwd, env=bg_env, timeout=600
                )
                if result.stdout:
                    print(f"[POST SYNC OUTPUT] {result.stdout}", file=sys.stderr, flush=True)
                if result.stderr:
                    print(f"[POST SYNC STDERR] {result.stderr}", file=sys.stderr, flush=True)
            except Exception as exc:
                print(f"[POST SYNC ERROR] {exc}", file=sys.stderr, flush=True)

        thread = threading.Thread(
            target=_run_post_sync, args=(cmd, env, project_root), daemon=True
        )
        thread.start()

        return jsonify({
            'success': True,
            'message': 'Postseason sync started — sheets will update shortly'
        }), 202
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/teams', methods=['GET'])
def get_teams():
    """Get list of all teams."""
    league = request.args.get('league', 'nba').lower()
    teams_dict, _ = get_teams_dicts(league)
    teams_list = [{'id': team_id, 'name': team_name} 
                  for team_id, team_name in teams_dict.items()]
    return jsonify({'teams': teams_list})


@app.route('/api/stats', methods=['POST'])
def calculate_stats():
    """
    Calculate stats for a team in specified mode.
    
    Request body:
    {
        "team_id": 1610612738,
        "mode": "per_100",  # per_game, per_100, per_36
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
    season = data.get('season', NBA_CONFIG['current_season'])
    
    if not team_id:
        return jsonify({'error': 'team_id is required'}), 400
    
    if team_id not in NBA_TEAMS_BY_ID:
        return jsonify({'error': 'Invalid team_id'}), 400
    
    valid_modes = ['per_game', 'per_100', 'per_36']
    if mode not in valid_modes:
        return jsonify({'error': f'Invalid mode. Must be one of: {valid_modes}'}), 400

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

        # Build mock context for sheets lib function calls
        class Context: pass
        ctx = Context()
        ctx.league = 'nba'
        import etl.nba.lib as etl_lib
        ctx.player_entity_table = etl_lib.get_table_name('player', 'entity')
        ctx.team_entity_table = etl_lib.get_table_name('team', 'entity')
        ctx.player_stats_table = etl_lib.get_table_name('player', 'stats')
        ctx.team_stats_table = etl_lib.get_table_name('team', 'stats')
        from etl.nba.config import NBA_CONFIG as league_config
        ctx.league_config = league_config
        ctx.season_key = 'current_season'
        ctx.league_id = '00' # Required by NCAA/NBA configs sometimes? Actually no

        current_season = league_config['current_season']
        current_season_year = int(current_season[:4]) + 1
        season_type_val = 1

        # Use lib.sheets fetch + calculate (config-driven, no hardcoded SQL)
        players = fetch_players_for_team(
            conn=conn, 
            team_abbr=team_abbr, 
            section='current_stats', 
            historical_config=None, 
            ctx=ctx,
            current_season=current_season, 
            current_season_year=current_season_year, 
            season_type_val=season_type_val
        )
        all_players = fetch_all_players(
            conn=conn, 
            section='current_stats', 
            historical_config=None, 
            ctx=ctx,
            current_season=current_season, 
            current_season_year=current_season_year, 
            season_type_val=season_type_val
        )
        conn.close()

        # Calculate percentile populations
        percentile_pops = calculate_all_percentiles(all_players, 'player', mode)

        # Build column list (for context_section blanking)
        columns = build_sheet_columns(entity='player', stat_mode='both')

        # Build rows using lib.sheets (same as sync path)
        player_rows = []
        for p in players:
            stats = calculate_entity_stats(p, 'player', mode)
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
        
        query = f"""
            SELECT player_id, name
            FROM {get_table_name('player', 'entity')}
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
            RETURNING team_id, team_abbr, team_name, notes
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
            'team_abbr': updated_team[1],
            'team_name': updated_team[2],
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
    league = request.args.get('league', 'nba').lower()
    try:
        return jsonify(get_config_for_export(league=league, mode=mode))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/column-config', methods=['GET'])
def get_column_config():
    """Alias for /api/config for backward compatibility."""
    return get_config()