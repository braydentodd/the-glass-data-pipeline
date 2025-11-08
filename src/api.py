"""
Flask API for interactive NBA stat calculations
Provides endpoints for switching between stat modes (totals, per-game, per-100, etc.)
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np

from src.config import DB_CONFIG, NBA_TEAMS_BY_ID, STAT_COLUMNS, REVERSE_STATS, API_CONFIG
from src.stat_calculator import calculate_stats_for_team

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
                s.fg2m,
                s.fg2a,
                s.fg3m,
                s.fg3a,
                s.ftm,
                s.fta,
                s.off_reb_pct_x1000::float / 1000 AS oreb_pct,
                s.def_reb_pct_x1000::float / 1000 AS dreb_pct,
                s.assists,
                s.turnovers,
                s.steals,
                s.blocks,
                s.fouls
            FROM players p
            INNER JOIN player_season_stats s 
                ON s.player_id = p.player_id
            WHERE p.team_id = %s 
              AND s.year = %s
              AND s.season_type = 1
              AND s.games_played > 0
            ORDER BY s.minutes_x10 DESC
        """
        
        cursor.execute(query, (team_id, season_year))
        players = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not players:
            return jsonify({
                'error': f'No players found for team {NBA_TEAMS_BY_ID[team_id]} in {season} season'
            }), 404
        
        # Calculate stats in requested mode
        calculated_players = calculate_stats_for_team(
            [dict(player) for player in players],
            mode=mode,
            custom_value=custom_value
        )
        
        # Calculate percentiles
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
        
        # Calculate stats in all modes
        from src.stat_calculator import StatCalculator
        calc = StatCalculator(player_dict)
        
        modes = {
            'totals': calc.calculate_totals(),
            'per_game': calc.calculate_per_game(),
            'per_100': calc.calculate_per_100(),
            'per_36': calc.calculate_per_36(),
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


if __name__ == '__main__':
    # Run with configuration from config.py
    app.run(
        host=API_CONFIG['host'], 
        port=API_CONFIG['port'], 
        debug=API_CONFIG['debug']
    )
