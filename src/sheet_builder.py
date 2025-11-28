"""
Config-driven sheet building module.
All sheet creation logic unified and driven by SECTIONS configuration.
"""


def build_headers(sections_config, headers_config, stats_mode, stats_custom_value, 
                  past_years, specific_seasons, include_current, current_season, sheet_name):
    """
    Build header rows dynamically from configuration.
    
    Args:
        sections_config: SECTIONS or SECTIONS_NBA
        headers_config: HEADERS or HEADERS_NBA
        stats_mode: 'per_36', 'totals', etc.
        stats_custom_value: Custom value for per_minutes mode
        past_years: Number of past years for historical
        specific_seasons: List of specific seasons or None
        include_current: Whether to include current season
        current_season: Current season string (e.g., '2025-26')
        sheet_name: Name to display (team name or 'NBA')
    
    Returns:
        tuple: (header_row_1, header_row_2, filter_row)
    """
    mode_display = {
        'totals': 'Totals',
        'per_game': 'Per Game',
        'per_36': 'Per 36 Mins',
        'per_100_poss': 'Per 100 Poss',
        'per_minutes': f'Per {stats_custom_value} Mins' if stats_custom_value else 'Per Minute'
    }
    mode_text = mode_display.get(stats_mode, 'Per 36 Mins')
    
    header_row_1 = []
    for h in headers_config['row_1']:
        # Replace placeholders
        if '{historical_years}' in h:
            if specific_seasons:
                start_year = min(specific_seasons)
                start_season_text = f"{start_year-1}-{str(start_year)[2:]}"
                historical_text = f'{"" if include_current else "Prev "}Stats since {start_season_text} {mode_text}'
            elif past_years >= 25:
                historical_text = f'Career {"" if include_current else "Prev Season "}Stats {mode_text}'
            else:
                historical_text = f'{"Last" if include_current else "Prev"} {past_years} Seasons {mode_text}'
            header_row_1.append(h.replace('{historical_years}', historical_text))
        elif '{postseason_years}' in h:
            if specific_seasons:
                start_year_ps = min(specific_seasons)
                start_season_text = f"{start_year_ps-1}-{str(start_year_ps)[2:]}"
                postseason_text = f'{"" if include_current else "Prev "}Postseason Stats since {start_season_text} {mode_text}'
            elif past_years >= 25:
                postseason_text = f'Career {"" if include_current else "Prev Season "}Postseason Stats {mode_text}'
            else:
                postseason_text = f'{"Last" if include_current else "Prev"} {past_years} Postseason Seasons {mode_text}'
            header_row_1.append(h.replace('{postseason_years}', postseason_text))
        elif '{season}' in h:
            header_row_1.append(h.replace('{season}', f'{current_season} Stats {mode_text}'))
        elif '{team_name}' in h:
            header_row_1.append(sheet_name)
        else:
            header_row_1.append(h)
    
    # Header row 2 - replace OR%/DR% with ORS/DRS for totals mode
    header_row_2 = list(headers_config['row_2'])
    if stats_mode == 'totals':
        header_row_2 = [h.replace('OR%', 'ORS').replace('DR%', 'DRS') for h in header_row_2]
    else:
        # For non-totals, adjust GMS headers for historical sections
        hist_section = sections_config.get('historical')
        if hist_section:
            hist_start = hist_section['columns']['start']
            header_row_2[hist_start + 1] = 'GMS'  # Games column in historical section
    
    # Filter row
    total_cols = sum(s['columns']['count'] for s in sections_config.values())
    filter_row = [""] * total_cols
    
    return header_row_1, header_row_2, filter_row


def build_player_row(player, sections_config, calculated_stats, historical_calculated_stats,
                     playoff_calculated_stats, player_percentiles, has_stats, has_historical_minutes,
                     has_playoff_minutes, seasons_played, playoff_seasons_played, stats_mode,
                     show_percentiles, sync_section):
    """
    Build a single player row dynamically from SECTIONS configuration.
    
    Args:
        player: Player dict with all info
        sections_config: SECTIONS or SECTIONS_NBA
        calculated_stats: Current season calculated stats
        historical_calculated_stats: Historical calculated stats
        playoff_calculated_stats: Playoff calculated stats
        player_percentiles: Percentile dict for player
        has_stats: Whether player has current season stats
        has_historical_minutes: Whether player has historical stats
        has_playoff_minutes: Whether player has playoff stats
        seasons_played: Number of seasons played (historical)
        playoff_seasons_played: Number of playoff seasons played
        stats_mode: Current stats mode
        show_percentiles: Whether to show percentiles instead of values
        sync_section: Which section to sync (None, 'historical', 'postseason')
    
    Returns:
        list: Row data
    """
    def format_stat(value, decimals=1):
        if value is None or value == 0:
            return 0
        rounded = round(value, decimals)
        return int(rounded) if rounded == int(rounded) else rounded
    
    def format_pct(value, decimals=1, allow_zero=False):
        if value is None:
            return ''
        if value == 0:
            return 0 if allow_zero else ''
        result = value * 100
        rounded = round(result, decimals)
        return int(rounded) if rounded == int(rounded) else rounded
    
    def get_display_value(stat_value, percentile_value, is_pct=False, allow_zero=False):
        if show_percentiles and percentile_value is not None:
            return int(round(percentile_value))
        elif is_pct:
            return format_pct(stat_value, allow_zero=allow_zero)
        else:
            return format_stat(stat_value)
    
    def format_height(inches):
        if not inches:
            return ''
        feet = int(inches) // 12
        remaining_inches = int(inches) % 12
        return f"{feet}'{remaining_inches}\""
    
    # Build row section by section
    row = []
    
    for section_name, section in sections_config.items():
        if section_name == 'player_info':
            # Player info section
            exp = player.get('years_experience')
            exp_display = 0 if exp == 0 else (exp if exp else '')
            
            # Check if this is NBA sheet (has 'team' field)
            if 'team' in section['fields']:
                # NBA sheet - includes team column
                row.extend([
                    player.get('player_name', ''),
                    player.get('team_abbr', 'FA'),
                    player.get('jersey_number', ''),
                    exp_display,
                    round(float(player.get('age', 0)), 1) if player.get('age') else '',
                    format_height(player.get('height_inches')),
                    format_height(player.get('wingspan_inches')),
                    int(player.get('weight_lbs', 0)) if player.get('weight_lbs') else '',
                    player.get('notes', ''),
                ])
            else:
                # Team sheet - no team column
                row.extend([
                    player['player_name'],
                    player.get('jersey_number', ''),
                    exp_display,
                    round(float(player.get('age', 0)), 1) if player.get('age') else '',
                    format_height(player.get('height_inches')),
                    format_height(player.get('wingspan_inches')),
                    player.get('weight_lbs', ''),
                    player.get('notes', ''),
                ])
        
        elif section_name == 'current':
            # Current season stats
            if has_stats and not sync_section:
                fg2a = calculated_stats.get('fg2a', 0)
                fg3a = calculated_stats.get('fg3a', 0)
                fta = calculated_stats.get('fta', 0)
                
                # Build stats based on section config
                stat_values = []
                for idx, stat_name in enumerate(section['stats']):
                    col_idx = section['columns']['start'] + idx
                    
                    if stat_name == 'games':
                        stat_values.append(calculated_stats.get('games', 0) if calculated_stats.get('games', 0) else '')
                    elif stat_name == 'minutes':
                        minutes = calculated_stats.get('minutes', 0)
                        stat_values.append(get_display_value(minutes, player_percentiles.get(col_idx)))
                    elif stat_name == 'possessions':
                        stat_values.append(get_display_value(calculated_stats.get('possessions', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'points':
                        stat_values.append(get_display_value(calculated_stats.get('points', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'ts_pct':
                        stat_values.append(get_display_value(calculated_stats.get('ts_pct'), player_percentiles.get(col_idx), is_pct=True))
                    elif stat_name == 'fg2a':
                        stat_values.append(get_display_value(fg2a, player_percentiles.get(col_idx)))
                    elif stat_name == 'fg2_pct':
                        stat_values.append(get_display_value(calculated_stats.get('fg2_pct', 0) if fg2a else 0, player_percentiles.get(col_idx), is_pct=True, allow_zero=(fg2a > 0)))
                    elif stat_name == 'fg3a':
                        stat_values.append(get_display_value(fg3a, player_percentiles.get(col_idx)))
                    elif stat_name == 'fg3_pct':
                        stat_values.append(get_display_value(calculated_stats.get('fg3_pct', 0) if fg3a else 0, player_percentiles.get(col_idx), is_pct=True, allow_zero=(fg3a > 0)))
                    elif stat_name == 'fta':
                        stat_values.append(get_display_value(fta, player_percentiles.get(col_idx)))
                    elif stat_name == 'ft_pct':
                        stat_values.append(get_display_value(calculated_stats.get('ft_pct', 0) if fta else 0, player_percentiles.get(col_idx), is_pct=True, allow_zero=(fta > 0)))
                    elif stat_name == 'assists':
                        stat_values.append(get_display_value(calculated_stats.get('assists', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'turnovers':
                        stat_values.append(get_display_value(calculated_stats.get('turnovers', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'oreb_pct':
                        stat_values.append(get_display_value(calculated_stats.get('oreb_pct'), player_percentiles.get(col_idx), is_pct=(stats_mode != 'totals'), allow_zero=True))
                    elif stat_name == 'dreb_pct':
                        stat_values.append(get_display_value(calculated_stats.get('dreb_pct'), player_percentiles.get(col_idx), is_pct=(stats_mode != 'totals'), allow_zero=True))
                    elif stat_name == 'steals':
                        stat_values.append(get_display_value(calculated_stats.get('steals', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'blocks':
                        stat_values.append(get_display_value(calculated_stats.get('blocks', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'fouls':
                        stat_values.append(get_display_value(calculated_stats.get('fouls', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'off_rating':
                        stat_values.append(get_display_value(calculated_stats.get('off_rating', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'def_rating':
                        stat_values.append(get_display_value(calculated_stats.get('def_rating', 0), player_percentiles.get(col_idx)))
                
                row.extend(stat_values)
            else:
                row.extend([''] * section['columns']['count'])
        
        elif section_name == 'historical':
            # Historical stats
            if has_historical_minutes:
                hist_fg2a = historical_calculated_stats.get('fg2a', 0)
                hist_fg3a = historical_calculated_stats.get('fg3a', 0)
                hist_fta = historical_calculated_stats.get('fta', 0)
                historical_minutes = historical_calculated_stats.get('minutes', 0)
                
                stat_values = []
                for idx, stat_name in enumerate(section['stats']):
                    col_idx = section['columns']['start'] + idx
                    
                    if stat_name == 'years':
                        stat_values.append(seasons_played)
                    elif stat_name == 'games':
                        games_val = historical_calculated_stats.get('games', 0)
                        if stats_mode != 'totals' and seasons_played > 0:
                            games_val = games_val / seasons_played
                        stat_values.append(get_display_value(games_val, player_percentiles.get(col_idx)))
                    elif stat_name == 'minutes':
                        stat_values.append(get_display_value(historical_minutes, player_percentiles.get(col_idx)))
                    elif stat_name == 'possessions':
                        stat_values.append(get_display_value(historical_calculated_stats.get('possessions', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'points':
                        stat_values.append(get_display_value(historical_calculated_stats.get('points', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'ts_pct':
                        stat_values.append(get_display_value(historical_calculated_stats.get('ts_pct'), player_percentiles.get(col_idx), is_pct=True))
                    elif stat_name == 'fg2a':
                        stat_values.append(get_display_value(hist_fg2a, player_percentiles.get(col_idx)))
                    elif stat_name == 'fg2_pct':
                        stat_values.append(get_display_value(historical_calculated_stats.get('fg2_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=(hist_fg2a > 0)))
                    elif stat_name == 'fg3a':
                        stat_values.append(get_display_value(hist_fg3a, player_percentiles.get(col_idx)))
                    elif stat_name == 'fg3_pct':
                        stat_values.append(get_display_value(historical_calculated_stats.get('fg3_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=(hist_fg3a > 0)))
                    elif stat_name == 'fta':
                        stat_values.append(get_display_value(hist_fta, player_percentiles.get(col_idx)))
                    elif stat_name == 'ft_pct':
                        stat_values.append(get_display_value(historical_calculated_stats.get('ft_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=(hist_fta > 0)))
                    elif stat_name == 'assists':
                        stat_values.append(get_display_value(historical_calculated_stats.get('assists', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'turnovers':
                        stat_values.append(get_display_value(historical_calculated_stats.get('turnovers', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'oreb_pct':
                        stat_values.append(get_display_value(historical_calculated_stats.get('oreb_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=True))
                    elif stat_name == 'dreb_pct':
                        stat_values.append(get_display_value(historical_calculated_stats.get('dreb_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=True))
                    elif stat_name == 'steals':
                        stat_values.append(get_display_value(historical_calculated_stats.get('steals', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'blocks':
                        stat_values.append(get_display_value(historical_calculated_stats.get('blocks', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'fouls':
                        stat_values.append(get_display_value(historical_calculated_stats.get('fouls', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'off_rating':
                        stat_values.append(get_display_value(historical_calculated_stats.get('off_rating', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'def_rating':
                        stat_values.append(get_display_value(historical_calculated_stats.get('def_rating', 0), player_percentiles.get(col_idx)))
                
                row.extend(stat_values)
            else:
                row.extend([''] * section['columns']['count'])
        
        elif section_name == 'postseason':
            # Postseason stats
            if has_playoff_minutes:
                playoff_fg2a = playoff_calculated_stats.get('fg2a', 0)
                playoff_fg3a = playoff_calculated_stats.get('fg3a', 0)
                playoff_fta = playoff_calculated_stats.get('fta', 0)
                playoff_minutes = playoff_calculated_stats.get('minutes', 0)
                
                stat_values = []
                for idx, stat_name in enumerate(section['stats']):
                    col_idx = section['columns']['start'] + idx
                    
                    if stat_name == 'years':
                        stat_values.append(playoff_seasons_played)
                    elif stat_name == 'games':
                        games_val = playoff_calculated_stats.get('games', 0)
                        if stats_mode != 'totals' and playoff_seasons_played > 0:
                            games_val = games_val / playoff_seasons_played
                        stat_values.append(get_display_value(games_val, player_percentiles.get(col_idx)))
                    elif stat_name == 'minutes':
                        stat_values.append(get_display_value(playoff_minutes, player_percentiles.get(col_idx)))
                    elif stat_name == 'possessions':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('possessions', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'points':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('points', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'ts_pct':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('ts_pct'), player_percentiles.get(col_idx), is_pct=True))
                    elif stat_name == 'fg2a':
                        stat_values.append(get_display_value(playoff_fg2a, player_percentiles.get(col_idx)))
                    elif stat_name == 'fg2_pct':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('fg2_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=(playoff_fg2a > 0)))
                    elif stat_name == 'fg3a':
                        stat_values.append(get_display_value(playoff_fg3a, player_percentiles.get(col_idx)))
                    elif stat_name == 'fg3_pct':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('fg3_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=(playoff_fg3a > 0)))
                    elif stat_name == 'fta':
                        stat_values.append(get_display_value(playoff_fta, player_percentiles.get(col_idx)))
                    elif stat_name == 'ft_pct':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('ft_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=(playoff_fta > 0)))
                    elif stat_name == 'assists':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('assists', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'turnovers':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('turnovers', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'oreb_pct':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('oreb_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=True))
                    elif stat_name == 'dreb_pct':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('dreb_pct'), player_percentiles.get(col_idx), is_pct=True, allow_zero=True))
                    elif stat_name == 'steals':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('steals', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'blocks':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('blocks', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'fouls':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('fouls', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'off_rating':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('off_rating', 0), player_percentiles.get(col_idx)))
                    elif stat_name == 'def_rating':
                        stat_values.append(get_display_value(playoff_calculated_stats.get('def_rating', 0), player_percentiles.get(col_idx)))
                
                row.extend(stat_values)
            else:
                row.extend([''] * section['columns']['count'])
        
        elif section_name == 'hidden':
            # Hidden player ID
            row.append(str(player['player_id']))
    
    return row


def build_formatting_requests(sections_config, sheet_format, sheet_id, total_rows, total_cols, show_percentiles):
    """
    Build all formatting requests dynamically from SECTIONS configuration.
    
    Args:
        sections_config: SECTIONS or SECTIONS_NBA
        sheet_format: SHEET_FORMAT or SHEET_FORMAT_NBA
        sheet_id: Google Sheets sheet ID
        total_rows: Total number of rows
        total_cols: Total number of columns
        show_percentiles: Whether percentiles are shown
    
    Returns:
        list: List of batch update requests
    """
    requests = []
    
    # Freeze rows and columns
    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {
                    "frozenRowCount": sheet_format['frozen']['rows'],
                    "frozenColumnCount": sheet_format['frozen']['columns']
                }
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
        }
    })
    
    # Set column widths
    for section_name, section in sections_config.items():
        if 'resize_rules' in section:
            start_col = section['columns']['start']
            for field_idx, (field_name, rules) in enumerate(section['resize_rules'].items()):
                if rules.get('fixed'):
                    col_idx = start_col + field_idx
                    requests.append({
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": col_idx,
                                "endIndex": col_idx + 1
                            },
                            "properties": {"pixelSize": rules['width']},
                            "fields": "pixelSize"
                        }
                    })
        
        # Auto-resize columns
        if section.get('auto_resize'):
            requests.append({
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": section['auto_resize_start'],
                        "endIndex": section['auto_resize_end']
                    }
                }
            })
    
    # Add borders for stat sections
    for section_name in ['current', 'historical', 'postseason']:
        if section_name in sections_config:
            section = sections_config[section_name]
            if section.get('has_border'):
                border_config = section['border_config']
                start_col = section['columns']['start']
                end_col = section['columns']['end']
                
                # Left border on first column
                if border_config.get('first_column_left'):
                    requests.append({
                        "updateBorders": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 0,
                                "endRowIndex": total_rows,
                                "startColumnIndex": start_col,
                                "endColumnIndex": start_col + 1
                            },
                            "left": {
                                "style": "SOLID",
                                "width": border_config['weight'],
                                "color": {"red": 1, "green": 1, "blue": 1} if start_col < sheet_format['header_rows'] else {"red": 0, "green": 0, "blue": 0}
                            }
                        }
                    })
                
                # Right border on last column
                if border_config.get('last_column_right'):
                    requests.append({
                        "updateBorders": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 0,
                                "endRowIndex": total_rows,
                                "startColumnIndex": end_col - 1,
                                "endColumnIndex": end_col
                            },
                            "right": {
                                "style": "SOLID",
                                "width": border_config['weight'],
                                "color": {"red": 1, "green": 1, "blue": 1} if end_col < sheet_format['header_rows'] else {"red": 0, "green": 0, "blue": 0}
                            }
                        }
                    })
    
    # Apply fonts
    fonts = sheet_format['fonts']
    
    # Header fonts
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": total_cols
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "fontFamily": fonts['header_large']['family'],
                        "fontSize": fonts['header_large']['size'],
                        "bold": fonts['header_large']['bold']
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat"
        }
    })
    
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 2,
                "startColumnIndex": 0,
                "endColumnIndex": total_cols
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "fontFamily": fonts['header_small']['family'],
                        "fontSize": fonts['header_small']['size'],
                        "bold": fonts['header_small']['bold']
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat"
        }
    })
    
    # Data fonts
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 3,
                "endRowIndex": total_rows,
                "startColumnIndex": 0,
                "endColumnIndex": total_cols
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "fontFamily": fonts['data']['family'],
                        "fontSize": fonts['data']['size'],
                        "bold": fonts['data']['bold']
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat"
        }
    })
    
    return requests
