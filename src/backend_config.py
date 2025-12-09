"""
THE GLASS - Backend Configuration
Single source of truth for ALL ETL operations.

BIDIRECTIONAL REFERENCES:
- Each backend stat has 'frontend_stats' listing which frontend display stats it feeds
- Frontend config has 'backend_sources' listing which backend columns + calculations it needs

ARCHITECTURE:
- Team stats fetched separately via team endpoints (NEVER aggregated from players)
- On-Off stats fetched for all 30 teams via teamplayeronoffdetails
- Opponent stats: Only 13 basic box score stats available (FG2M/A, FG3M/A, FTM/A, REB, AST, TOV, STL, BLK, PF)
"""

# ============================================================================
# ETL GROUPS - API endpoint organization for efficiency
# ============================================================================

ETL_GROUPS = {
    'GROUP_1_BASIC': {
        'player_endpoint': 'leaguedashplayerstats',
        'team_endpoint': 'leaguedashteamstats',
        'opponent_available': True,  # Use measure_type='Opponent' 
        'endpoint_type': 'league_wide',
        'time_est_sec': 3
    },
    'GROUP_2_SHOOTING': {
        'player_endpoint': 'playerdashptshots',
        'team_endpoint': 'teamdashptshots',
        'opponent_available': False,
        'endpoint_type': 'per_entity',
        'time_est_sec': 380
    },
    'GROUP_3_PLAYMAKING': {
        'player_endpoint': 'playerdashptpass',
        'team_endpoint': 'teamdashptpass',
        'opponent_available': False,
        'endpoint_type': 'per_entity',
        'time_est_sec': 380
    },
    'GROUP_4_REBOUNDING': {
        'player_endpoint': 'playerdashptreb',
        'team_endpoint': 'teamdashptreb',
        'opponent_available': False,
        'endpoint_type': 'per_entity',
        'time_est_sec': 380
    },
    'GROUP_5_PUTBACKS': {
        'player_endpoint': 'playerdashboardbyshootingsplits',
        'team_endpoint': 'teamdashboardbyshootingsplits',
        'opponent_available': False,
        'endpoint_type': 'per_entity',
        'time_est_sec': 380
    },
    'GROUP_6_HUSTLE': {
        'player_endpoint': 'leaguehustlestatsplayer',
        'team_endpoint': 'leaguehustlestatsteam',
        'opponent_available': False,
        'endpoint_type': 'league_wide',
        'time_est_sec': 2
    },
    'GROUP_7_DEFENSE': {
        'player_endpoint': 'leaguedashptdefend',
        'team_endpoint': 'leaguedashptteamdefend',
        'opponent_available': False,
        'endpoint_type': 'league_wide_multi_call',  # 3 calls with DribbleRange filter
        'time_est_sec': 6
    },
    'GROUP_8_ONOFF': {
        'player_endpoint': 'teamplayeronoffdetails',
        'team_endpoint': None,
        'opponent_available': False,
        'endpoint_type': 'per_team',  # All 30 teams
        'time_est_sec': 30
    },
}

# ============================================================================
# COMPLETE ETL STAT MAPPING
# Format: 'db_column': {config_dict}
# ============================================================================

ETL_STAT_MAPPING = {
    
    # ========================================================================
    # GROUP 1: BASIC STATS (leaguedashplayerstats / leaguedashteamstats)
    # ========================================================================
    
    'games_played': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'GP', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['GMS']},
    'minutes_x10': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'MIN', 'dt': 'integer', 'sc': 10, 'calc': None, 'ent': ['player', 'team'], 'fs': ['MIN']},
    'possessions': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Advanced', 'rs': 'LeagueDashPlayerStats', 'af': 'POSS', 'dt': 'integer', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['POS']},
    
    'fg2m': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': None, 'dt': 'smallint', 'sc': 1, 'calc': 'FGM - FG3M', 'ent': ['player', 'team', 'opponent'], 'fs': ['2PM', '2P%', 'PTS', 'TS%']},
    'fg2a': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': None, 'dt': 'smallint', 'sc': 1, 'calc': 'FGA - FG3A', 'ent': ['player', 'team', 'opponent'], 'fs': ['2PA', '2P%', 'TS%']},
    'fg3m': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'FG3M', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['3PM', '3P%', 'PTS', 'TS%']},
    'fg3a': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'FG3A', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['3PA', '3P%', 'TS%']},
    'ftm': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'FTM', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['FTM', 'FT%', 'PTS', 'TS%']},
    'fta': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'FTA', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['FTA', 'FT%', 'TS%']},
    
    'off_reb_pct_x1000': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Advanced', 'rs': 'LeagueDashPlayerStats', 'af': 'OREB_PCT', 'dt': 'smallint', 'sc': 1000, 'calc': None, 'ent': ['player', 'team'], 'fs': ['ORB%']},
    'def_reb_pct_x1000': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Advanced', 'rs': 'LeagueDashPlayerStats', 'af': 'DREB_PCT', 'dt': 'smallint', 'sc': 1000, 'calc': None, 'ent': ['player', 'team'], 'fs': ['DRB%']},
    'off_rebounds': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'OREB', 'dt': 'integer', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['ORB']},
    'def_rebounds': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'DREB', 'dt': 'integer', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['DRB']},
    
    'assists': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'AST', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['AST']},
    'turnovers': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'TOV', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['TOV']},
    'steals': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'STL', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['STL']},
    'blocks': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'BLK', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['BLK']},
    'fouls': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats', 'o': 'leaguedashteamstats'}, 'mt': 'Base', 'rs': 'LeagueDashPlayerStats', 'af': 'PF', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team', 'opponent'], 'fs': ['FLS']},
    
    'off_rating_x10': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Advanced', 'rs': 'LeagueDashPlayerStats', 'af': 'OFF_RATING', 'dt': 'smallint', 'sc': 10, 'calc': None, 'ent': ['player', 'team'], 'fs': ['ORTG']},
    'def_rating_x10': {'g': 1, 'ep': {'p': 'leaguedashplayerstats', 't': 'leaguedashteamstats'}, 'mt': 'Advanced', 'rs': 'LeagueDashPlayerStats', 'af': 'DEF_RATING', 'dt': 'smallint', 'sc': 10, 'calc': None, 'ent': ['player', 'team'], 'fs': ['DRTG']},
    
    # ========================================================================
    # GROUP 2: SHOOTING TRACKING (playerdashptshots / teamdashptshots)
    # CRITICAL: 4 separate API calls required (comma-separated params broken)
    # ========================================================================
    
    'open_rim_fgm': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': 'Restricted Area', 'def_dist': ['4-6 Feet - Open', '6+ Feet - Wide Open'], 'af': 'FGM', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(4-6ft + 6+ft)', 'ent': ['player', 'team'], 'fs': ['ORMA', 'ORM%']},
    'open_rim_fga': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': 'Restricted Area', 'def_dist': ['4-6 Feet - Open', '6+ Feet - Wide Open'], 'af': 'FGA', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(4-6ft + 6+ft)', 'ent': ['player', 'team'], 'fs': ['ORMA', 'ORM%']},
    'cont_rim_fgm': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': 'Restricted Area', 'def_dist': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'], 'af': 'FGM', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(0-2ft + 2-4ft)', 'ent': ['player', 'team'], 'fs': ['CRMA', 'CRM%']},
    'cont_rim_fga': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': 'Restricted Area', 'def_dist': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'], 'af': 'FGA', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(0-2ft + 2-4ft)', 'ent': ['player', 'team'], 'fs': ['CRMA', 'CRM%']},
    
    'open_fg2m': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Restricted Area', 'In The Paint (Non-RA)', 'Mid-Range'], 'def_dist': ['4-6 Feet - Open', '6+ Feet - Wide Open'], 'af': 'FG2M', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(all 2pt zones)', 'ent': ['player', 'team'], 'fs': ['OMRA', 'OMR%']},
    'open_fg2a': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Restricted Area', 'In The Paint (Non-RA)', 'Mid-Range'], 'def_dist': ['4-6 Feet - Open', '6+ Feet - Wide Open'], 'af': 'FG2A', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(all 2pt zones)', 'ent': ['player', 'team'], 'fs': ['OMRA', 'OMR%']},
    'cont_fg2m': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Restricted Area', 'In The Paint (Non-RA)', 'Mid-Range'], 'def_dist': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'], 'af': 'FG2M', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(all 2pt zones)', 'ent': ['player', 'team'], 'fs': ['CMRA', 'CMR%']},
    'cont_fg2a': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Restricted Area', 'In The Paint (Non-RA)', 'Mid-Range'], 'def_dist': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'], 'af': 'FG2A', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(all 2pt zones)', 'ent': ['player', 'team'], 'fs': ['CMRA', 'CMR%']},
    
    'cont_fg3m': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Above the Break 3', 'Corner 3'], 'def_dist': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'], 'af': 'FG3M', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(zones)', 'ent': ['player', 'team'], 'fs': ['C3PA', 'C3P%']},
    'cont_fg3a': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Above the Break 3', 'Corner 3'], 'def_dist': ['0-2 Feet - Very Tight', '2-4 Feet - Tight'], 'af': 'FG3A', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(zones)', 'ent': ['player', 'team'], 'fs': ['C3PA', 'C3P%']},
    'open_fg3m': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Above the Break 3', 'Corner 3'], 'def_dist': ['4-6 Feet - Open', '6+ Feet - Wide Open'], 'af': 'FG3M', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(zones)', 'ent': ['player', 'team'], 'fs': ['O3PA', 'O3P%']},
    'open_fg3a': {'g': 2, 'ep': {'p': 'playerdashptshots', 't': 'teamdashptshots'}, 'rs': 'ClosestDefenderShooting', 'zone': ['Above the Break 3', 'Corner 3'], 'def_dist': ['4-6 Feet - Open', '6+ Feet - Wide Open'], 'af': 'FG3A', 'dt': 'smallint', 'sc': 1, 'calc': 'sum(zones)', 'ent': ['player', 'team'], 'fs': ['O3PA', 'O3P%']},
    
    # ========================================================================
    # GROUP 3: PLAYMAKING (playerdashptpass / teamdashptpass)
    # ========================================================================
    
    'touches': {'g': 3, 'ep': {'p': 'playerdashptpass', 't': 'teamdashptpass'}, 'rs': 'PassesReceived', 'af': 'TOUCHES', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['TOU']},
    'pot_ast': {'g': 3, 'ep': {'p': 'playerdashptpass', 't': 'teamdashptpass'}, 'rs': 'PassesMade', 'af': 'POTENTIAL_AST', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['PAST']},
    
    # ========================================================================
    # GROUP 4: REBOUNDING (playerdashptreb / teamdashptreb)
    # ========================================================================
    
    'cont_oreb': {'g': 4, 'ep': {'p': 'playerdashptreb', 't': 'teamdashptreb'}, 'rs': 'OverallRebounding', 'af': 'REB_CONTEST_OREB', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['COR', 'COR%']},
    'cont_dreb': {'g': 4, 'ep': {'p': 'playerdashptreb', 't': 'teamdashptreb'}, 'rs': 'OverallRebounding', 'af': 'REB_CONTEST_DREB', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['CDR', 'CDR%']},
    
    # ========================================================================
    # GROUP 5: PUTBACKS (playerdashboardbyshootingsplits / teamdashboardbyshootingsplits)
    # ========================================================================
    
    'putbacks': {'g': 5, 'ep': {'p': 'playerdashboardbyshootingsplits', 't': 'teamdashboardbyshootingsplits'}, 'rs': 'ShotTypePlayerDashboard', 'af': None, 'dt': 'smallint', 'sc': 1, 'calc': 'sum(Putback + Tip shots FGM)', 'ent': ['player', 'team'], 'fs': ['PBS']},
    
    # ========================================================================
    # GROUP 6: HUSTLE (leaguehustlestatsplayer / leaguehustlestatsteam)
    # ========================================================================
    
    'charges_drawn': {'g': 6, 'ep': {'p': 'leaguehustlestatsplayer', 't': 'leaguehustlestatsteam'}, 'rs': 'HustleStatsPlayer', 'af': 'CHARGES_DRAWN', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['CHG']},
    'deflections': {'g': 6, 'ep': {'p': 'leaguehustlestatsplayer', 't': 'leaguehustlestatsteam'}, 'rs': 'HustleStatsPlayer', 'af': 'DEFLECTIONS', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['DEF']},
    'contests': {'g': 6, 'ep': {'p': 'leaguehustlestatsplayer', 't': 'leaguehustlestatsteam'}, 'rs': 'HustleStatsPlayer', 'af': 'CONTESTED_SHOTS', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['CON']},
    
    # ========================================================================
    # GROUP 7: DEFENSE (leaguedashptdefend / leaguedashptteamdefend)
    # 3 separate API calls with DribbleRange filter: Overall, Less Than 6 Ft, 3 Pointers
    # ========================================================================
    
    'def_fg2m': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': 'Overall', 'af': None, 'dt': 'smallint', 'sc': 1, 'calc': 'D_FGM - def_fg3m', 'ent': ['player', 'team'], 'fs': ['DFGA (2PT)', 'DFG% (2PT)']},
    'def_fg2a': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': 'Overall', 'af': None, 'dt': 'smallint', 'sc': 1, 'calc': 'D_FGA - def_fg3a', 'ent': ['player', 'team'], 'fs': ['DFGA (2PT)', 'DFG% (2PT)']},
    'def_rim_fgm': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': 'Less Than 6 Ft', 'af': 'D_FGM', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['DRA', 'DR%']},
    'def_rim_fga': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': 'Less Than 6 Ft', 'af': 'D_FGA', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['DRA', 'DR%']},
    'def_fg3m': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': '3 Pointers', 'af': 'D_FGM', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['D3PA', 'D3P%']},
    'def_fg3a': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': '3 Pointers', 'af': 'D_FGA', 'dt': 'smallint', 'sc': 1, 'calc': None, 'ent': ['player', 'team'], 'fs': ['D3PA', 'D3P%']},
    'real_def_fg_pct_x1000': {'g': 7, 'ep': {'p': 'leaguedashptdefend', 't': 'leaguedashptteamdefend'}, 'rs': 'LeagueDashPtDefend', 'dribble_range': 'Overall', 'af': 'D_FG_PCT', 'dt': 'integer', 'sc': 1000, 'calc': None, 'ent': ['player', 'team'], 'fs': ['RD%']},
    
    # ========================================================================
    # GROUP 8: ON-OFF (teamplayeronoffdetails)
    # Per team, filter GROUP_VALUE="Off"
    # ========================================================================
    
    'tm_off_off_rating_x10': {'g': 8, 'ep': {'p': 'teamplayeronoffdetails'}, 'rs': 'TeamPlayerOnOffDetails', 'filter': 'GROUP_VALUE="Off"', 'af': 'OFF_RATING', 'dt': 'smallint', 'sc': 10, 'calc': None, 'ent': ['player'], 'fs': ['ORTG (Team Off-Court)']},
    'tm_off_def_rating_x10': {'g': 8, 'ep': {'p': 'teamplayeronoffdetails'}, 'rs': 'TeamPlayerOnOffDetails', 'filter': 'GROUP_VALUE="Off"', 'af': 'DEF_RATING', 'dt': 'smallint', 'sc': 10, 'calc': None, 'ent': ['player'], 'fs': ['DRTG (Team Off-Court)']},
}

# ============================================================================
# LEGEND for abbreviated keys (keeps config compact)
# ============================================================================
LEGEND = {
    'g': 'group (1-8)',
    'ep': 'endpoints {p: player, t: team, o: opponent}',
    'mt': 'measure_type (Base/Advanced/Opponent)',
    'rs': 'result_set name',
    'af': 'api_field (None if calculated)',
    'dt': 'datatype',
    'sc': 'scale (1, 10, 1000)',
    'calc': 'calculation formula if derived',
    'ent': 'entities (player/team/opponent)',
    'fs': 'frontend_stats (display stats this populates)',
    'zone': 'shot_zone filter',
    'def_dist': 'defense_distance filter',
    'dribble_range': 'dribble_range filter',
    'filter': 'additional filter criteria'
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_stats_by_group(group_num):
    """Get all stats in GROUP 1-8"""
    return {k: v for k, v in ETL_STAT_MAPPING.items() if v['g'] == group_num}

def get_stats_by_entity(entity):
    """Get stats for 'player', 'team', or 'opponent'"""
    return {k: v for k, v in ETL_STAT_MAPPING.items() if entity in v['ent']}

def get_opponent_stats():
    """Get the 13 available opponent stats (basic box score only)"""
    return get_stats_by_entity('opponent')

def get_calculated_stats():
    """Get all stats that require calculation (not direct from API)"""
    return {k: v for k, v in ETL_STAT_MAPPING.items() if v['calc'] is not None}
