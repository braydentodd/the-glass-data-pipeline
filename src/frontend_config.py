"""
THE GLASS - Frontend Configuration
Defines how stats are displayed in Google Sheets (Basic and Advanced views).

BIDIRECTIONAL REFERENCES:
- Each frontend stat has 'backend_sources' listing backend columns + calculation
- Backend config has 'frontend_stats' (fs) field listing which display stats it populates

ARCHITECTURE:
- Basic View: 20 stats, simple percentages
- Advanced View: 66 stats, organized in 6 subsections
- Column sizes optimized for display
- Reverse stats flagged (lower is better)
- Percentage stats identified for conditional formatting
"""

# ============================================================================
# BASIC VIEW CONFIGURATION
# ============================================================================

BASIC_VIEW = {
    'GMS': {
        'display_name': 'Games',
        'backend_sources': ['games_played'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'MIN': {
        'display_name': 'Minutes',
        'backend_sources': ['minutes_x10'],
        'calculation': 'minutes_x10 / 10',
        'col_size': 50,
        'applies_to': ['player', 'team'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 1
    },
    'POS': {
        'display_name': 'Possessions',
        'backend_sources': ['possessions'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'PTS': {
        'display_name': 'Points',
        'backend_sources': ['fg2m', 'fg3m', 'ftm'],
        'calculation': '(fg2m * 2) + (fg3m * 3) + ftm',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'TS%': {
        'display_name': 'True Shooting %',
        'backend_sources': ['fg2m', 'fg3m', 'ftm', 'fg2a', 'fg3a', 'fta'],
        'calculation': 'PTS / (2 * (fg2a + fg3a + 0.44*fta)) * 100',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': True,
        'is_reverse': False,
        'decimals': 1
    },
    '2PA': {
        'display_name': '2PT Attempts',
        'backend_sources': ['fg2a'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    '2P%': {
        'display_name': '2PT %',
        'backend_sources': ['fg2m', 'fg2a'],
        'calculation': 'fg2m / fg2a * 100',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': True,
        'is_reverse': False,
        'decimals': 1
    },
    '3PA': {
        'display_name': '3PT Attempts',
        'backend_sources': ['fg3a'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    '3P%': {
        'display_name': '3PT %',
        'backend_sources': ['fg3m', 'fg3a'],
        'calculation': 'fg3m / fg3a * 100',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': True,
        'is_reverse': False,
        'decimals': 1
    },
    'FTA': {
        'display_name': 'Free Throw Attempts',
        'backend_sources': ['fta'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'FT%': {
        'display_name': 'Free Throw %',
        'backend_sources': ['ftm', 'fta'],
        'calculation': 'ftm / fta * 100',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': True,
        'is_reverse': False,
        'decimals': 1
    },
    'AST': {
        'display_name': 'Assists',
        'backend_sources': ['assists'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'TOV': {
        'display_name': 'Turnovers',
        'backend_sources': ['turnovers'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': True,  # Lower is better
        'decimals': 0
    },
    'ORB%': {
        'display_name': 'Off Rebound %',
        'backend_sources': ['off_reb_pct_x1000'],
        'calculation': 'off_reb_pct_x1000 / 10',
        'col_size': 60,
        'totals_display': 'ORB',
        'totals_backend': ['off_rebounds'],
        'totals_calculation': 'direct',
        'applies_to': ['player', 'team'],
        'is_percentage': True,
        'is_reverse': False,
        'decimals': 1
    },
    'DRB%': {
        'display_name': 'Def Rebound %',
        'backend_sources': ['def_reb_pct_x1000'],
        'calculation': 'def_reb_pct_x1000 / 10',
        'col_size': 60,
        'totals_display': 'DRB',
        'totals_backend': ['def_rebounds'],
        'totals_calculation': 'direct',
        'applies_to': ['player', 'team'],
        'is_percentage': True,
        'is_reverse': False,
        'decimals': 1
    },
    'STL': {
        'display_name': 'Steals',
        'backend_sources': ['steals'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'BLK': {
        'display_name': 'Blocks',
        'backend_sources': ['blocks'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 0
    },
    'FLS': {
        'display_name': 'Fouls',
        'backend_sources': ['fouls'],
        'calculation': 'direct',
        'col_size': 50,
        'applies_to': ['player', 'team', 'opponent'],
        'is_percentage': False,
        'is_reverse': True,  # Lower is better
        'decimals': 0
    },
    'ORTG': {
        'display_name': 'Offensive Rating',
        'backend_sources': ['off_rating_x10'],
        'calculation': 'off_rating_x10 / 10',
        'col_size': 60,
        'applies_to': ['player', 'team'],
        'is_percentage': False,
        'is_reverse': False,
        'decimals': 1
    },
    'DRTG': {
        'display_name': 'Defensive Rating',
        'backend_sources': ['def_rating_x10'],
        'calculation': 'def_rating_x10 / 10',
        'col_size': 60,
        'applies_to': ['player', 'team'],
        'is_percentage': False,
        'is_reverse': True,  # Lower is better
        'decimals': 1
    },
}

# ============================================================================
# ADVANCED VIEW CONFIGURATION
# ============================================================================

ADVANCED_VIEW = {
    
    # ========================================================================
    # TIME SUBSECTION
    # ========================================================================
    'Time': {
        'GMS': BASIC_VIEW['GMS'],
        'MIN': BASIC_VIEW['MIN'],
        'POS': BASIC_VIEW['POS'],
    },
    
    # ========================================================================
    # SCORING SUBSECTION
    # ========================================================================
    'Scoring': {
        'PTS': BASIC_VIEW['PTS'],
        'TS%': BASIC_VIEW['TS%'],
        
        'CRMA': {
            'display_name': 'Contested Rim Attempts',
            'backend_sources': ['cont_rim_fga'],
            'calculation': 'direct',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'CRM%': {
            'display_name': 'Contested Rim %',
            'backend_sources': ['cont_rim_fgm', 'cont_rim_fga'],
            'calculation': 'cont_rim_fgm / cont_rim_fga * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        'ORMA': {
            'display_name': 'Open Rim Attempts',
            'backend_sources': ['open_rim_fga'],
            'calculation': 'direct',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'ORM%': {
            'display_name': 'Open Rim %',
            'backend_sources': ['open_rim_fgm', 'open_rim_fga'],
            'calculation': 'open_rim_fgm / open_rim_fga * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        
        'CMRA': {
            'display_name': 'Contested Mid-Range Attempts',
            'backend_sources': ['cont_fg2a', 'cont_rim_fga'],
            'calculation': 'cont_fg2a - cont_rim_fga',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'CMR%': {
            'display_name': 'Contested Mid-Range %',
            'backend_sources': ['cont_fg2m', 'cont_fg2a', 'cont_rim_fgm', 'cont_rim_fga'],
            'calculation': '(cont_fg2m - cont_rim_fgm) / (cont_fg2a - cont_rim_fga) * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        'OMRA': {
            'display_name': 'Open Mid-Range Attempts',
            'backend_sources': ['open_fg2a', 'open_rim_fga'],
            'calculation': 'open_fg2a - open_rim_fga',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'OMR%': {
            'display_name': 'Open Mid-Range %',
            'backend_sources': ['open_fg2m', 'open_fg2a', 'open_rim_fgm', 'open_rim_fga'],
            'calculation': '(open_fg2m - open_rim_fgm) / (open_fg2a - open_rim_fga) * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        
        'C3PA': {
            'display_name': 'Contested 3PT Attempts',
            'backend_sources': ['cont_fg3a'],
            'calculation': 'direct',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'C3P%': {
            'display_name': 'Contested 3PT %',
            'backend_sources': ['cont_fg3m', 'cont_fg3a'],
            'calculation': 'cont_fg3m / cont_fg3a * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        'O3PA': {
            'display_name': 'Open 3PT Attempts',
            'backend_sources': ['open_fg3a'],
            'calculation': 'direct',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'O3P%': {
            'display_name': 'Open 3PT %',
            'backend_sources': ['open_fg3m', 'open_fg3a'],
            'calculation': 'open_fg3m / open_fg3a * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        
        'FTA': BASIC_VIEW['FTA'],
        'FT%': BASIC_VIEW['FT%'],
    },
    
    # ========================================================================
    # PLAYMAKING SUBSECTION
    # ========================================================================
    'Playmaking': {
        'TOU': {
            'display_name': 'Touches',
            'backend_sources': ['touches'],
            'calculation': 'direct',
            'col_size': 60,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'AST': BASIC_VIEW['AST'],
        'PAST': {
            'display_name': 'Potential Assists',
            'backend_sources': ['pot_ast'],
            'calculation': 'direct',
            'col_size': 60,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'TOV': BASIC_VIEW['TOV'],
    },
    
    # ========================================================================
    # REBOUNDING SUBSECTION
    # ========================================================================
    'Rebounding': {
        'ORB%': BASIC_VIEW['ORB%'],
        'DRB%': BASIC_VIEW['DRB%'],
        
        'COR%': {
            'display_name': 'Contested Off Rebound %',
            'backend_sources': ['cont_oreb', 'off_rebounds'],
            'calculation': 'cont_oreb / off_rebounds * 100',
            'col_size': 70,
            'totals_display': 'COR',
            'totals_backend': ['cont_oreb'],
            'totals_calculation': 'direct',
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        'CDR%': {
            'display_name': 'Contested Def Rebound %',
            'backend_sources': ['cont_dreb', 'def_rebounds'],
            'calculation': 'cont_dreb / def_rebounds * 100',
            'col_size': 70,
            'totals_display': 'CDR',
            'totals_backend': ['cont_dreb'],
            'totals_calculation': 'direct',
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': False,
            'decimals': 1
        },
        'PBS': {
            'display_name': 'Putback Makes',
            'backend_sources': ['putbacks'],
            'calculation': 'direct',
            'col_size': 60,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
    },
    
    # ========================================================================
    # DEFENSE SUBSECTION
    # ========================================================================
    'Defense': {
        'DRA': {
            'display_name': 'Defended Rim Attempts',
            'backend_sources': ['def_rim_fga'],
            'calculation': 'direct',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'DR%': {
            'display_name': 'Defended Rim %',
            'backend_sources': ['def_rim_fgm', 'def_rim_fga'],
            'calculation': 'def_rim_fgm / def_rim_fga * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': True,  # Lower is better for defense
            'decimals': 1
        },
        'DMRA': {
            'display_name': 'Defended Mid-Range Attempts',
            'backend_sources': ['def_fg2a', 'def_rim_fga'],
            'calculation': 'def_fg2a - def_rim_fga',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'DMR%': {
            'display_name': 'Defended Mid-Range %',
            'backend_sources': ['def_fg2m', 'def_rim_fgm', 'def_fg2a', 'def_rim_fga'],
            'calculation': '(def_fg2m - def_rim_fgm) / (def_fg2a - def_rim_fga) * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': True,  # Lower is better
            'decimals': 1
        },
        'D3PA': {
            'display_name': 'Defended 3PT Attempts',
            'backend_sources': ['def_fg3a'],
            'calculation': 'direct',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'D3P%': {
            'display_name': 'Defended 3PT %',
            'backend_sources': ['def_fg3m', 'def_fg3a'],
            'calculation': 'def_fg3m / def_fg3a * 100',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': True,  # Lower is better
            'decimals': 1
        },
        'RD%': {
            'display_name': 'Real Def FG %',
            'backend_sources': ['real_def_fg_pct_x1000'],
            'calculation': 'real_def_fg_pct_x1000 / 10',
            'col_size': 70,
            'applies_to': ['player', 'team'],
            'is_percentage': True,
            'is_reverse': True,  # Lower is better
            'decimals': 1
        },
        
        'STL': BASIC_VIEW['STL'],
        'DEF': {
            'display_name': 'Deflections',
            'backend_sources': ['deflections'],
            'calculation': 'direct',
            'col_size': 60,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'BLK': BASIC_VIEW['BLK'],
        'CON': {
            'display_name': 'Contests',
            'backend_sources': ['contests'],
            'calculation': 'direct',
            'col_size': 60,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'CHG': {
            'display_name': 'Charges Drawn',
            'backend_sources': ['charges_drawn'],
            'calculation': 'direct',
            'col_size': 60,
            'applies_to': ['player', 'team'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 0
        },
        'FLS': BASIC_VIEW['FLS'],
    },
    
    # ========================================================================
    # ON-OFF SUBSECTION
    # ========================================================================
    'On-Off': {
        'ORTG': BASIC_VIEW['ORTG'],
        'DRTG': BASIC_VIEW['DRTG'],
        'TM_OFF_ORTG': {
            'display_name': 'Team Off-Court ORTG',
            'backend_sources': ['tm_off_off_rating_x10'],
            'calculation': 'tm_off_off_rating_x10 / 10',
            'col_size': 80,
            'applies_to': ['player'],
            'is_percentage': False,
            'is_reverse': False,
            'decimals': 1,
            'notes': 'Team offensive rating when player is OFF court'
        },
        'TM_OFF_DRTG': {
            'display_name': 'Team Off-Court DRTG',
            'backend_sources': ['tm_off_def_rating_x10'],
            'calculation': 'tm_off_def_rating_x10 / 10',
            'col_size': 80,
            'applies_to': ['player'],
            'is_percentage': False,
            'is_reverse': True,  # Lower is better
            'decimals': 1,
            'notes': 'Team defensive rating when player is OFF court'
        },
    },
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_all_basic_stats():
    """Get all 20 basic view stats"""
    return BASIC_VIEW

def get_all_advanced_stats():
    """Get all 66 advanced view stats organized by subsection"""
    return ADVANCED_VIEW

def get_stat_by_name(stat_name):
    """Get config for a specific stat (searches both views)"""
    if stat_name in BASIC_VIEW:
        return BASIC_VIEW[stat_name]
    
    for subsection, stats in ADVANCED_VIEW.items():
        if stat_name in stats:
            return stats[stat_name]
    
    return None

def get_stats_by_entity_type(entity):
    """Get all stats applicable to 'player', 'team', or 'opponent'"""
    result = {}
    
    # Check basic view
    for stat, config in BASIC_VIEW.items():
        if entity in config['applies_to']:
            result[stat] = config
    
    # Check advanced view
    for subsection, stats in ADVANCED_VIEW.items():
        if subsection == 'description':
            continue
        for stat, config in stats.items():
            if entity in config['applies_to']:
                result[stat] = config
    
    return result

def get_reverse_stats():
    """Get all stats where lower is better (for conditional formatting)"""
    result = []
    
    for stat, config in BASIC_VIEW.items():
        if config['is_reverse']:
            result.append(stat)
    
    for subsection, stats in ADVANCED_VIEW.items():
        for stat, config in stats.items():
            if config.get('is_reverse', False):
                result.append(stat)
    
    return result

def get_percentage_stats():
    """Get all percentage stats (for formatting)"""
    result = []
    
    for stat, config in BASIC_VIEW.items():
        if config['is_percentage']:
            result.append(stat)
    
    for subsection, stats in ADVANCED_VIEW.items():
        for stat, config in stats.items():
            if config.get('is_percentage', False):
                result.append(stat)
    
    return result
