"""
THE GLASS - ETL Configuration
Single source of truth for ETL execution strategy, rate limiting, and runtime behavior.

This module defines how the ETL runs, not what data it collects (that's in db_config.py).
"""

import os

# ============================================================================
# PARALLEL EXECUTION STRATEGY
# ============================================================================
# Three-tier execution strategy based on API endpoint patterns

PARALLEL_EXECUTION = {
    'league': {
        'max_workers': 10,              # League-wide endpoints: fast, reliable, max parallelism
        'timeout': 30,
        'description': 'Single API call returns ALL entities (never fails)'
    },
    'team': {
        'max_workers': 10,              # Per-team endpoints: 30 calls, high parallelism OK
        'timeout': 30,
        'description': 'One API call per team (30 total, very reliable)'
    },
    'player': {
        'max_workers': 1,               # Per-player endpoints: MUST BE 1! Concurrency causes failures
        'description': 'One API call per player (536 total) - NEEDS RATE LIMITING'
    }
}

# ============================================================================
# SUBPROCESS EXECUTION - Per-Player Endpoints ONLY
# ============================================================================
# NBA API enforces a hard ~600 call limit per connection/process.
# Solution: Run each per-player endpoint in a SEPARATE OS subprocess.
# Each subprocess gets a fresh 500-call quota, bypassing the limit entirely.
#
# Strategy:
# - League-wide endpoints (1 call): Run in main process
# - Per-team endpoints (30 calls): Run in main process  
# - Per-player endpoints (500+ calls): SPAWN SUBPROCESS with batch of 500 players
#
# Proven in production: 1500/1500 API calls (100% success) across 3 subprocesses

SUBPROCESS_CONFIG = {
    # Subprocess batching (each subprocess handles this many players)
    'players_per_subprocess': 600,      # Proven: 500 players per subprocess = 100% success
    
    # Per-request timing (within each subprocess)
    'delay_between_calls': 1.5,         # Seconds between API calls (conservative)
    'timeout': 20,                      # Request timeout
    
    # Subprocess management
    'max_retries': 3,                   # Retries for failed subprocess
    'subprocess_timeout': 1000,         # Max seconds per subprocess (60 min for 600 players - allows for API slowness)
}

# ============================================================================
# API CONFIGURATION
# ============================================================================

API_CONFIG = {
    'rate_limit_delay': float(os.getenv('API_RATE_LIMIT_DELAY', '0.6')),
    'timeout_default': 20,
    'timeout_bulk': 120,
    'max_retries': 3,
    
    # Standard NBA API parameters (single source of truth)
    'league_id': '00',  # NBA league
    'per_mode_simple': 'Totals',
    'per_mode_time': 'Totals',
    'per_mode_detailed': 'Totals',
    'last_n_games': '0',
    'month': '0',
    'opponent_team_id': '0',
    'period': '0',
    'player_or_team_player': 'Player',
    'player_or_team_team': 'Team',
}

# ============================================================================
# RETRY & ERROR HANDLING
# ============================================================================

RETRY_CONFIG = {
    'max_retries': 3,
    'backoff_base': 10,
}

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

DB_OPERATIONS = {
    'bulk_insert_batch_size': 1000,
    'statement_timeout_ms': 120000,  # 2 minutes
}

# ============================================================================
# RESULT SET DEFAULTS
# ============================================================================
# Default result set names by endpoint (eliminates hardcoded fallbacks in etl.py)

RESULT_SET_DEFAULTS = {
    'teamdashptreb': 'OverallRebounding',
    'teamdashptshots': 'GeneralShooting',
    'playerdashptshots': 'ShotTypePlayerDashboard',
    'default': 'General'  # Fallback for unknown endpoints
}

# ============================================================================
# PROGRESS TRACKING
# ============================================================================

PROGRESS_CONFIG = {
    'show_overall_bar': True,
    'log_level': 'INFO',                # INFO, DEBUG, ERROR
    'batch_check_interval': 50,         # Check for consecutive failures every N players
    'consecutive_failure_threshold': 3, # Trigger emergency break after N failures
    'emergency_break_seconds': 120,     # Emergency break duration (2 minutes)
}

# ============================================================================
# ANNUAL ETL CONFIGURATION
# ============================================================================
# Configuration for annual maintenance operations (run August 1st each year)
# This makes annual ETL config-driven like daily ETL - no hardcoding!

ANNUAL_ETL_CONFIG = {
    'wingspan': {
        'description': 'Wingspan from NBA Draft Combine',
        'source_endpoint': 'draftcombineplayeranthro',
        'endpoint_class': 'DraftCombinePlayerAnthro',
        'field_mapping': {
            'WINGSPAN': 'wingspan_inches'  # API field -> DB column
        },
        'id_field': 'PLAYER_ID',
        'start_year': 2002,  # First year with combine data
        'rate_limit': 1.2,
        'timeout': 10,
        'keep_most_recent': True,  # If player has multiple years, use newest
        'transform': 'round',  # Function to apply to value
        'applies_to': ['player']
    },
    'player_bio': {
        'description': 'Height, weight, birthdate from CommonPlayerInfo',
        'source_endpoint': 'commonplayerinfo',
        'endpoint_class': 'CommonPlayerInfo',
        'field_mapping': {
            'HEIGHT': 'height_inches',
            'WEIGHT': 'weight_lbs',
            'BIRTHDATE': 'birthdate'
        },
        'id_field': 'PERSON_ID',
        'rate_limit': 0.6,
        'timeout': 20,
        'batch_size': 50,  # Log every N players
        'consecutive_failure_threshold': 3,
        'emergency_break_seconds': 120,  # Break on repeated failures
        'max_retries': 3,
        'applies_to': ['player']
    }
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def estimate_player_endpoint_time(player_count):
    """
    Estimate execution time for a per-player endpoint using subprocess strategy.
    
    Args:
        player_count: Number of players to process
        
    Returns:
        Estimated seconds to complete
    """
    players_per_subprocess = SUBPROCESS_CONFIG['players_per_subprocess']
    delay = SUBPROCESS_CONFIG['delay_between_calls']
    avg_response_time = 0.1  # Fast response times observed
    
    # Each subprocess runs in parallel (effectively), so time = longest subprocess
    time_per_subprocess = players_per_subprocess * (avg_response_time + delay)
    
    return time_per_subprocess  # All subprocesses can run sequentially

# ============================================================================
# TRANSFORMATION DEFINITIONS
# ============================================================================
# Post-processing transformations for complex multi-resultSet endpoints
# This enables 100% config-driven ETL - even complex stats that require:
# - Arithmetic operations (subtraction, division)
# - Filtering by shot type, defender distance, etc.
# - Aggregation across multiple API calls
# - Formula-based calculations (ratings, percentages)

TRANSFORMATIONS = {
    # Shooting tracking - Arithmetic transformations (close = contested 0-4ft MINUS 10ft+)
    # This gives us contested shots UNDER 10 feet (close range)
    'cont_close_2fgm': {
        'type': 'arithmetic_subtract',
        'endpoint': 'playerdashptshots',
        'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'},
        'subtract': [
            # Sum of 0-2ft and 2-4ft contested shots
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'},
            # Subtract 10ft+ contested
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2M'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2M'}
        ],
        'formula': '(a + b) - (c + d)'  # Sum first two, subtract sum of last two
    },
    'cont_close_2fga': {
        'type': 'arithmetic_subtract',
        'endpoint': 'playerdashptshots',
        'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'},
        'subtract': [
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '0-2 Feet - Very Tight'}, 'field': 'FG2A'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '2-4 Feet - Tight'}, 'field': 'FG2A'}
        ],
        'formula': '(a + b) - (c + d)'
    },
    'open_close_2fgm': {
        'type': 'arithmetic_subtract',
        'endpoint': 'playerdashptshots',
        'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'},
        'subtract': [
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2M'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2M'}
        ],
        'formula': '(a + b) - (c + d)'
    },
    'open_close_2fga': {
        'type': 'arithmetic_subtract',
        'endpoint': 'playerdashptshots',
        'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'},
        'subtract': [
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
            {'result_set': 'ClosestDefenderShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '4-6 Feet - Open'}, 'field': 'FG2A'},
            {'result_set': 'ClosestDefender10ftPlusShooting', 'filter': {'CLOSE_DEF_DIST_RANGE': '6+ Feet - Wide Open'}, 'field': 'FG2A'}
        ],
        'formula': '(a + b) - (c + d)'
    },
    
    # Aggregated contested/open stats (0-4 ft = contested, 4+ ft = open)
    'cont_2fgm': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting', 
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
        'aggregate': 'sum',
        'field': 'FG2M',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'cont_2fga': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
        'aggregate': 'sum',
        'field': 'FG2A',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'cont_3fgm': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
        'aggregate': 'sum',
        'field': 'FG3M',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'cont_3fga': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'],
        'aggregate': 'sum',
        'field': 'FG3A',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'open_2fgm': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
        'aggregate': 'sum',
        'field': 'FG2M',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'open_2fga': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
        'aggregate': 'sum',
        'field': 'FG2A',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'open_3fgm': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
        'aggregate': 'sum',
        'field': 'FG3M',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'open_3fga': {
        'type': 'filter_aggregate', 'endpoint': 'playerdashptshots', 'execution_tier': 'player',
        'group': 'playerdashptshots_player',
        'result_set': 'ClosestDefenderShooting',
        'filter_field': 'CLOSE_DEF_DIST_RANGE',
        'filter_values': ['4-6 Feet - Open', '6+ Feet - Wide Open'],
        'aggregate': 'sum',
        'field': 'FG3A',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    
    # Putbacks - Filter & aggregate transformation
    'putbacks': {
        'type': 'filter_aggregate',
        'endpoint': 'playerdashboardbyshootingsplits',
        'team_endpoint': 'teamdashboardbyshootingsplits',
        'execution_tier': 'player',
        'group': 'playerdashboardbyshootingsplits_player',
        'season_type_param': 'season_type_playoffs',  # This endpoint uses season_type_playoffs, not season_type_all_star
        'result_set': 'ShotTypePlayerDashboard',
        'team_result_set': 'ShotTypeTeamDashboard',
        'filter_field': 'GROUP_VALUE',
        'filter_values': ['Putback Dunk Shot', 'Putback Layup Shot', 'Tip Dunk Shot', 'Tip Layup Shot'],
        'aggregate': 'sum',
        'field': 'FGM',
        'endpoint_params': {
            'measure_type_detailed': 'Base',
            'per_mode_detailed': 'Totals',
            'season_type_playoffs': 'Regular Season'
        }
    },
    
    # On/off ratings - Calculated formula transformations
    'tm_off_o_rating_x10': {
        'type': 'calculated_rating',
        'endpoint': 'teamplayeronoffdetails',
        'execution_tier': 'team',
        'result_set': 'PlayersOffCourtTeamPlayerOnOffDetails',
        'formula': '(PTS / POSS) * 1000',
        'possession_formula': 'FGA - OREB + TOV + (0.44 * FTA)'
    },
    'tm_off_d_rating_x10': {
        'type': 'calculated_rating',
        'endpoint': 'teamplayeronoffdetails',
        'execution_tier': 'team',
        'result_set': 'PlayersOffCourtTeamPlayerOnOffDetails',
        'formula': '((PTS - PLUS_MINUS) / POSS) * 1000',
        'possession_formula': 'FGA - OREB + TOV + (0.44 * FTA)'
    },
    
    # Team shooting tracking - Multi-call aggregate transformations
    'cont_rim_fgm': {
        'type': 'multi_call_aggregate',
        'endpoint': 'leaguedashteamptshot',
        'execution_tier': 'league',
        'entity': 'team',
        'calls': [
            {'close_def_dist_range': '0-2 Feet - Very Tight', 'shot_dist_range': 'Less Than 8 ft'},
            {'close_def_dist_range': '2-4 Feet - Tight', 'shot_dist_range': 'Less Than 8 ft'}
        ],
        'aggregate': 'sum',
        'field': 'FGM'
    },
    'cont_rim_fga': {
        'type': 'multi_call_aggregate',
        'endpoint': 'leaguedashteamptshot',
        'execution_tier': 'league',
        'entity': 'team',
        'calls': [
            {'close_def_dist_range': '0-2 Feet - Very Tight', 'shot_dist_range': 'Less Than 8 ft'},
            {'close_def_dist_range': '2-4 Feet - Tight', 'shot_dist_range': 'Less Than 8 ft'}
        ],
        'aggregate': 'sum',
        'field': 'FGA'
    },
    'open_rim_fgm': {
        'type': 'multi_call_aggregate',
        'endpoint': 'leaguedashteamptshot',
        'execution_tier': 'league',
        'entity': 'team',
        'calls': [
            {'close_def_dist_range': '4-6 Feet - Open', 'shot_dist_range': 'Less Than 8 ft'},
            {'close_def_dist_range': '6+ Feet - Wide Open', 'shot_dist_range': 'Less Than 8 ft'}
        ],
        'aggregate': 'sum',
        'field': 'FGM'
    },
    'open_rim_fga': {
        'type': 'multi_call_aggregate',
        'endpoint': 'leaguedashteamptshot',
        'execution_tier': 'league',
        'entity': 'team',
        'calls': [
            {'close_def_dist_range': '4-6 Feet - Open', 'shot_dist_range': 'Less Than 8 ft'},
            {'close_def_dist_range': '6+ Feet - Wide Open', 'shot_dist_range': 'Less Than 8 ft'}
        ],
        'aggregate': 'sum',
        'field': 'FGA'
    },
    
    # Total contested/open 2PT and 3PT (abbreviated for brevity)
    'cont_2fgm_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '0-2 Feet - Very Tight', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '2-4 Feet - Tight', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG2M'
    },
    'cont_2fga_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '0-2 Feet - Very Tight', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '2-4 Feet - Tight', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG2A'
    },
    'cont_3fgm_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '0-2 Feet - Very Tight', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '2-4 Feet - Tight', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG3M'
    },
    'cont_3fga_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '0-2 Feet - Very Tight', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '2-4 Feet - Tight', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG3A'
    },
    'open_2fgm_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '4-6 Feet - Open', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '6+ Feet - Wide Open', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG2M'
    },
    'open_2fga_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '4-6 Feet - Open', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '6+ Feet - Wide Open', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG2A'
    },
    'open_3fgm_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '4-6 Feet - Open', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '6+ Feet - Wide Open', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG3M'
    },
    'open_3fga_team': {
        'type': 'multi_call_aggregate', 'endpoint': 'leaguedashteamptshot', 'execution_tier': 'league', 'entity': 'team',
        'calls': [{'close_def_dist_range': '4-6 Feet - Open', 'shot_dist_range': ''}, 
                  {'close_def_dist_range': '6+ Feet - Wide Open', 'shot_dist_range': ''}],
        'aggregate': 'sum', 'field': 'FG3A'
    },
    
    # Team defense - Simple extracts and arithmetic
    'd_rim_fgm': {'type': 'simple_extract', 'endpoint': 'leaguedashptteamdefend', 'execution_tier': 'league', 
                  'entity': 'team', 'params': {'defense_category': 'Less Than 10Ft'}, 'field': 'FGM_LT_10'},
    'd_rim_fga': {'type': 'simple_extract', 'endpoint': 'leaguedashptteamdefend', 'execution_tier': 'league', 
                  'entity': 'team', 'params': {'defense_category': 'Less Than 10Ft'}, 'field': 'FGA_LT_10'},
    'd_3fgm': {'type': 'simple_extract', 'endpoint': 'leaguedashptteamdefend', 'execution_tier': 'league', 
               'entity': 'team', 'params': {'defense_category': '3 Pointers'}, 'field': 'FG3M'},
    'd_3fga': {'type': 'simple_extract', 'endpoint': 'leaguedashptteamdefend', 'execution_tier': 'league', 
               'entity': 'team', 'params': {'defense_category': '3 Pointers'}, 'field': 'FG3A'},
    'd_2fgm': {
        'type': 'arithmetic_subtract',
        'endpoint': 'leaguedashptteamdefend',
        'execution_tier': 'league',
        'entity': 'team',
        'subtract': [
            {'params': {'defense_category': 'Overall'}, 'field': 'D_FGM'},
            {'params': {'defense_category': '3 Pointers'}, 'field': 'FG3M'}
        ]
    },
    'd_2fga': {
        'type': 'arithmetic_subtract',
        'endpoint': 'leaguedashptteamdefend',
        'execution_tier': 'league',
        'entity': 'team',
        'subtract': [
            {'params': {'defense_category': 'Overall'}, 'field': 'D_FGA'},
            {'params': {'defense_category': '3 Pointers'}, 'field': 'FG3A'}
        ]
    },
    'real_d_fg_pct_x1000': {'type': 'simple_extract', 'endpoint': 'leaguedashptteamdefend', 'execution_tier': 'league', 
                             'entity': 'team', 'params': {'defense_category': 'Overall'}, 'field': 'PCT_PLUSMINUS', 
                             'transform': 'scale_1000'},
    
    # ========== CONTESTED REBOUNDS (per-player endpoint via subprocess) ==========
    'cont_o_rebs': {
        'type': 'simple_extract',
        'endpoint': 'playerdashptreb',
        'execution_tier': 'player',
        'entity': 'player',
        'group': 'playerdashptreb_player',
        'result_set': 'OverallRebounding',
        'field': 'C_OREB',
        'description': 'Contested offensive rebounds',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    'cont_d_rebs': {
        'type': 'simple_extract',
        'endpoint': 'playerdashptreb',
        'execution_tier': 'player',
        'entity': 'player',
        'group': 'playerdashptreb_player',
        'result_set': 'OverallRebounding',
        'field': 'C_DREB',
        'description': 'Contested defensive rebounds',
        'endpoint_params': {'team_id': 0, 'season_type_all_star': 'Regular Season'}
    },
    
    # ========== ON/OFF RATINGS (per-team endpoint using teamplayeronoffsummary) ==========
    'tm_off_o_rating_x10': {
        'type': 'simple_extract',
        'endpoint': 'teamplayeronoffsummary',
        'execution_tier': 'team',
        'entity': 'player',
        'group': 'teamplayeronoffsummary_team',
        'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
        'field': 'OFF_RATING',
        'player_id_field': 'VS_PLAYER_ID',  # Specify the field containing player IDs
        'description': 'Team offensive rating when player off court',
        'transform': 'safe_float',
        'scale': 10,
        'endpoint_params': {}  # Uses season_type_all_star from base params
    },
    'tm_off_d_rating_x10': {
        'type': 'simple_extract',
        'endpoint': 'teamplayeronoffsummary',
        'execution_tier': 'team',
        'entity': 'player',
        'group': 'teamplayeronoffsummary_team',
        'result_set': 'PlayersOffCourtTeamPlayerOnOffSummary',
        'field': 'DEF_RATING',
        'player_id_field': 'VS_PLAYER_ID',  # Specify the field containing player IDs
        'description': 'Team defensive rating when player off court',
        'transform': 'safe_float',
        'scale': 10,
        'endpoint_params': {}  # Uses season_type_all_star from base params
    },
    
    # Putbacks and tip shots - Filter/aggregate transformation
    # NO GROUP - different handling for players vs teams (different endpoints)
    'putbacks': {
        'type': 'filter_aggregate',
        'endpoint': 'playerdashboardbyshootingsplits',
        'team_endpoint': 'teamdashboardbyshootingsplits',  # Config-driven team endpoint
        'execution_tier': 'player',
        'result_set': 'ShotTypePlayerDashboard',
        'team_result_set': 'ShotTypeTeamDashboard',  # Config-driven team result set
        'season_type_param': 'season_type_playoffs',  # This endpoint uses season_type_playoffs, not season_type_all_star
        'filter': {
            'field': 'GROUP_VALUE',
            'operator': 'startswith',
            'values': ['Putback', 'Tip']  # Match "Putback Dunk Shot", "Putback Layup Shot", "Tip Dunk Shot", "Tip Layup Shot"
        },
        'aggregate': {
            'field': 'FGM',
            'function': 'sum'
        },
        'transform': 'safe_int',
        'description': 'Putback and tip shots made',
        'endpoint_params': {
            'measure_type_detailed': 'Base',
            'per_mode_detailed': 'Totals'
        },
        'team_endpoint_params': {
            'measure_type_detailed_defense': 'Base',
            'per_mode_detailed': 'Totals'
        }
    },
}
