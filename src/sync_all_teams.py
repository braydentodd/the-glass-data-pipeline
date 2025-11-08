"""
Sync all 30 NBA teams to Google Sheets with percentile-based color coding.
"""

import os
import sys
import psycopg2
import gspread
import numpy as np
import time
from google.oauth2.service_account import Credentials
from psycopg2.extras import RealDictCursor

# Import centralized configuration
from src.config import (
    DB_CONFIG,
    GOOGLE_SHEETS_CONFIG,
    NBA_CONFIG,
    NBA_TEAMS,
    STAT_COLUMNS,
    REVERSE_STATS,
    PERCENTILE_CONFIG,
    COLORS,
    COLOR_THRESHOLDS,
    SHEET_FORMAT,
    HEADERS,
)

# Load environment variables
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

def log(message):
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_google_sheets_client():
    """Initialize Google Sheets API client"""
    credentials = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['credentials_file'], 
        scopes=GOOGLE_SHEETS_CONFIG['scopes']
    )
    client = gspread.authorize(credentials)
    return client

def fetch_all_players_data(conn):
    """Fetch all players with stats for percentile calculation"""
    query = """
    SELECT 
        p.player_id,
        p.name AS player_name,
        p.team_id,
        t.team_abbr,
        p.jersey_number,
        p.years_experience,
        EXTRACT(YEAR FROM AGE(p.birthdate)) + 
            (EXTRACT(MONTH FROM AGE(p.birthdate)) / 12.0) + 
            (EXTRACT(DAY FROM AGE(p.birthdate)) / 365.25) AS age,
        p.height_inches,
        p.weight_lbs,
        p.wingspan_inches,
        s.games_played,
        s.minutes_x10::float / 10 AS minutes_total,
        s.possessions,
        s.fg2m, s.fg2a,
        s.fg3m, s.fg3a,
        s.ftm, s.fta,
        s.off_reb_pct_x1000::float / 1000 AS oreb_pct,
        s.def_reb_pct_x1000::float / 1000 AS dreb_pct,
        s.assists,
        s.turnovers,
        s.steals,
        s.blocks,
        s.fouls
    FROM teams t
    INNER JOIN players p ON p.team_id = t.team_id
    LEFT JOIN player_season_stats s 
        ON s.player_id = p.player_id 
        AND s.year = %s 
        AND s.season_type = %s
    WHERE s.games_played IS NOT NULL
    ORDER BY t.team_abbr, s.minutes_x10 DESC
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (NBA_CONFIG['current_season_year'], NBA_CONFIG['season_type']))
        rows = cur.fetchall()
    
    return [dict(row) for row in rows]

def calculate_per_100_poss_stats(player):
    """Calculate per-100 possession stats"""
    possessions = player.get('possessions', 0)
    if not possessions or possessions == 0:
        return {}
    
    factor = 100.0 / possessions
    
    points = ((player.get('fg2m', 0) or 0) * 2 + 
              (player.get('fg3m', 0) or 0) * 3 + 
              (player.get('ftm', 0) or 0))
    
    fga = (player.get('fg2a', 0) or 0) + (player.get('fg3a', 0) or 0)
    fta = player.get('fta', 0) or 0
    ts_attempts = 2 * (fga + 0.44 * fta)
    ts_pct = (points / ts_attempts * 100) if ts_attempts > 0 else 0
    
    fg2_pct = ((player.get('fg2m', 0) or 0) / (player.get('fg2a', 0) or 1) * 100) if player.get('fg2a', 0) else 0
    fg3_pct = ((player.get('fg3m', 0) or 0) / (player.get('fg3a', 0) or 1) * 100) if player.get('fg3a', 0) else 0
    ft_pct = ((player.get('ftm', 0) or 0) / (player.get('fta', 0) or 1) * 100) if player.get('fta', 0) else 0
    
    return {
        'games': player.get('games_played', 0),
        'minutes': player.get('minutes_total', 0) / player.get('games_played', 1),
        'points': points * factor,
        'ts_pct': ts_pct,
        'fg2a': (player.get('fg2a', 0) or 0) * factor,
        'fg2_pct': fg2_pct,
        'fg3a': (player.get('fg3a', 0) or 0) * factor,
        'fg3_pct': fg3_pct,
        'fta': fta * factor,
        'ft_pct': ft_pct,
        'assists': (player.get('assists', 0) or 0) * factor,
        'turnovers': (player.get('turnovers', 0) or 0) * factor,
        'oreb_pct': (player.get('oreb_pct', 0) or 0) * 100,
        'dreb_pct': (player.get('dreb_pct', 0) or 0) * 100,
        'steals': (player.get('steals', 0) or 0) * factor,
        'blocks': (player.get('blocks', 0) or 0) * factor,
        'fouls': (player.get('fouls', 0) or 0) * factor,
    }

def calculate_percentiles(all_players_data):
    """Calculate weighted percentiles for each stat across all players (weighted by total minutes)"""
    # Calculate per-100 stats for all players
    players_with_stats = []
    for player in all_players_data:
        per100 = calculate_per_100_poss_stats(player)
        if per100:
            player['per100'] = per100
            players_with_stats.append(player)
    
    # Calculate weighted percentiles for each stat
    percentiles = {}
    for stat_name in STAT_COLUMNS.keys():
        # Create weighted samples where each player's stat appears proportional to their minutes
        weighted_values = []
        for p in players_with_stats:
            stat_value = p['per100'].get(stat_name, 0)
            if stat_value and stat_value != 0:
                # Weight by total minutes played
                minutes_weight = p.get('minutes_total', 0)
                if minutes_weight > 0:
                    # Add the value multiple times based on minutes (rounded to avoid too many samples)
                    # Scale minutes to reasonable sample size (e.g., 1 sample per 10 minutes)
                    weight_count = max(1, int(round(minutes_weight / PERCENTILE_CONFIG['minutes_weight_factor'])))
                    weighted_values.extend([stat_value] * weight_count)
        
        if weighted_values:
            percentiles[stat_name] = np.percentile(weighted_values, range(101))
        else:
            percentiles[stat_name] = None
    
    return percentiles, players_with_stats

def get_percentile_rank(value, percentiles_array, reverse=False):
    """Get the percentile rank for a value"""
    if percentiles_array is None or value == 0 or value == '':
        return None
    
    # Find which percentile this value falls into
    rank = np.searchsorted(percentiles_array, value)
    
    if reverse:
        rank = 100 - rank
    
    return min(max(rank, 0), 100)

def get_color_for_percentile(percentile):
    """
    Get RGB color for a percentile value using custom color scale.
    Uses colors from config: red, yellow, green.
    """
    if percentile is None:
        return None
    
    red_rgb = COLORS['red']['rgb']
    yellow_rgb = COLORS['yellow']['rgb']
    green_rgb = COLORS['green']['rgb']
    
    low_threshold = COLOR_THRESHOLDS['low']
    mid_threshold = COLOR_THRESHOLDS['mid']
    
    if percentile <= low_threshold:
        # Red to Yellow gradient (0-33%)
        ratio = percentile / low_threshold
        return {
            'red': red_rgb['red'] + (yellow_rgb['red'] - red_rgb['red']) * ratio,
            'green': red_rgb['green'] + (yellow_rgb['green'] - red_rgb['green']) * ratio,
            'blue': red_rgb['blue'] + (yellow_rgb['blue'] - red_rgb['blue']) * ratio
        }
    elif percentile <= mid_threshold:
        # Yellow plateau (33-66%)
        return yellow_rgb.copy()
    else:
        # Yellow to Green gradient (66-100%)
        ratio = (percentile - mid_threshold) / (COLOR_THRESHOLDS['high'] - mid_threshold)
        return {
            'red': yellow_rgb['red'] + (green_rgb['red'] - yellow_rgb['red']) * ratio,
            'green': yellow_rgb['green'] + (green_rgb['green'] - yellow_rgb['green']) * ratio,
            'blue': yellow_rgb['blue'] + (green_rgb['blue'] - yellow_rgb['blue']) * ratio
        }

def format_height(inches):
    """Convert inches to feet-inches format"""
    if not inches:
        return ""
    feet = inches // 12
    remaining_inches = inches % 12
    return f'{feet}\'{remaining_inches}"'

def create_team_sheet(worksheet, team_abbr, team_name, team_players, percentiles):
    """Create/update a team sheet with formatting and color coding"""
    log(f"Creating {team_name} sheet...")
    
    # Header row 1 - replace {team_name} placeholder
    header_row_1 = [h.format(team_name=team_name) if '{team_name}' in h else h 
                    for h in HEADERS['row_1']]
    
    # Header row 2
    header_row_2 = HEADERS['row_2']
    
    # Filter row
    filter_row = [""] * SHEET_FORMAT['total_columns']
    
    # Prepare data rows with percentile tracking
    data_rows = []
    percentile_data = []  # Track percentiles for color coding
    
    for player in team_players:
        per100 = player.get('per100', {})
        
        exp = player.get('years_experience')
        exp_display = 0 if exp == 0 else (exp if exp else '')
        
        row = [
            player['player_name'],
            player.get('jersey_number', ''),
            exp_display,
            round(float(player.get('age', 0)), 1) if player.get('age') else '',
            format_height(player.get('height_inches')),
            format_height(player.get('wingspan_inches')),
            player.get('weight_lbs', ''),
            '',
            per100.get('games', 0) if per100.get('games', 0) != 0 else '',
            round(per100.get('minutes', 0), 1) if per100.get('minutes', 0) != 0 else '',
            round(per100.get('points', 0), 1) if per100.get('points', 0) != 0 else '',
            round(per100.get('ts_pct', 0), 1) if per100.get('ts_pct') and per100.get('ts_pct', 0) != 0 else '',
            round(per100.get('fg2a', 0), 1) if per100.get('fg2a', 0) != 0 else '',
            round(per100.get('fg2_pct', 0), 1) if per100.get('fg2_pct', 0) != 0 else '',
            round(per100.get('fg3a', 0), 1) if per100.get('fg3a', 0) != 0 else '',
            round(per100.get('fg3_pct', 0), 1) if per100.get('fg3_pct', 0) != 0 else '',
            round(per100.get('fta', 0), 1) if per100.get('fta', 0) != 0 else '',
            round(per100.get('ft_pct', 0), 1) if per100.get('ft_pct') and per100.get('ft_pct', 0) != 0 else '',
            round(per100.get('assists', 0), 1) if per100.get('assists', 0) != 0 else '',
            round(per100.get('turnovers', 0), 1) if per100.get('turnovers', 0) != 0 else '',
            round(per100.get('oreb_pct', 0), 1) if per100.get('oreb_pct', 0) != 0 else '',
            round(per100.get('dreb_pct', 0), 1) if per100.get('dreb_pct', 0) != 0 else '',
            round(per100.get('steals', 0), 1) if per100.get('steals', 0) != 0 else '',
            round(per100.get('blocks', 0), 1) if per100.get('blocks', 0) != 0 else '',
            round(per100.get('fouls', 0), 1) if per100.get('fouls', 0) != 0 else '',
        ]
        
        # Calculate percentiles for this player
        player_percentiles = {}
        for stat_name, col_idx in STAT_COLUMNS.items():
            value = per100.get(stat_name, 0)
            if value and value != 0:
                reverse = stat_name in REVERSE_STATS
                pct = get_percentile_rank(value, percentiles.get(stat_name), reverse=reverse)
                player_percentiles[col_idx] = pct
            else:
                player_percentiles[col_idx] = None
        
        data_rows.append(row)
        percentile_data.append(player_percentiles)
    
    # Combine all data
    all_data = [header_row_1, header_row_2, filter_row] + data_rows
    
    # Clear and update
    worksheet.clear()
    
    spreadsheet = worksheet.spreadsheet
    
    # Remove existing banding
    sheet_metadata = spreadsheet.fetch_sheet_metadata({'includeGridData': False})
    for sheet in sheet_metadata.get('sheets', []):
        if sheet['properties']['sheetId'] == worksheet.id:
            banded_ranges = sheet.get('bandedRanges', [])
            if banded_ranges:
                delete_requests = [
                    {'deleteBanding': {'bandedRangeId': br['bandedRangeId']}}
                    for br in banded_ranges
                ]
                spreadsheet.batch_update({'requests': delete_requests})
                break
    
    worksheet.update(values=all_data, range_name='A1')
    
    total_rows = 3 + len(data_rows)
    total_cols = SHEET_FORMAT['total_columns']
    
    requests = []
    
    # Delete extra rows/columns
    sheet_properties = spreadsheet.fetch_sheet_metadata({'includeGridData': False})
    current_row_count = 1000
    current_col_count = 26
    for sheet in sheet_properties.get('sheets', []):
        if sheet['properties']['sheetId'] == worksheet.id:
            current_row_count = sheet['properties']['gridProperties'].get('rowCount', 1000)
            current_col_count = sheet['properties']['gridProperties'].get('columnCount', 26)
            break
    
    if current_row_count > total_rows:
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'ROWS',
                    'startIndex': total_rows,
                    'endIndex': current_row_count
                }
            }
        })
    
    if current_col_count > total_cols:
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'COLUMNS',
                    'startIndex': total_cols,
                    'endIndex': current_col_count
                }
            }
        })
    
    # Merge cells
    requests.append({
        'mergeCells': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 1,
                'endColumnIndex': 7,
            },
            'mergeType': 'MERGE_ALL'
        }
    })
    
    requests.append({
        'mergeCells': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 8,   # I
                'endColumnIndex': 25,    # Y
            },
            'mergeType': 'MERGE_ALL'
        }
    })
    
    # Format row 1
    black = COLORS['black']['rgb']
    white = COLORS['white']['rgb']
    light_gray = COLORS['light_gray']['rgb']
    
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT['fonts']['header_primary']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['header_primary']['size'],
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format row 2
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 1,
                'endRowIndex': 2,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT['fonts']['header_secondary']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['header_secondary']['size'],
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format A1
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 1,
                'startColumnIndex': 0,
                'endColumnIndex': 1
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT['fonts']['team_name']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['team_name']['size'],
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format filter row
    requests.append({
        'repeatCell': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 2,
                'endRowIndex': 3,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': black,
                    'textFormat': {
                        'foregroundColor': white,
                        'fontFamily': SHEET_FORMAT['fonts']['header_primary']['family'],
                        'fontSize': SHEET_FORMAT['fonts']['header_primary']['size'],
                        'bold': True
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                    'wrapStrategy': 'CLIP'
                }
            },
            'fields': 'userEnteredFormat'
        }
    })
    
    # Format data rows (centered by default)
    # IMPORTANT: Use specific fields to avoid overwriting background colors set by percentile logic
    if len(data_rows) > 0:
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontFamily': SHEET_FORMAT['fonts']['data']['family'],
                            'fontSize': SHEET_FORMAT['fonts']['data']['size']
                        },
                        'wrapStrategy': 'CLIP',
                        'verticalAlignment': 'TOP',
                        'horizontalAlignment': 'CENTER'
                    }
                },
                'fields': 'userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment,userEnteredFormat.horizontalAlignment'
            }
        })
        
        # Left-align column A
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'LEFT'
                    }
                },
                'fields': 'userEnteredFormat.horizontalAlignment'
            }
        })
        
        # Left-align column H
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 3,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 7,
                    'endColumnIndex': 8
                },
                'cell': {
                    'userEnteredFormat': {
                        'horizontalAlignment': 'LEFT'
                    }
                },
                'fields': 'userEnteredFormat.horizontalAlignment'
            }
        })
    
    # Bold column A
    if len(data_rows) > 0:
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 2,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': 1,
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'bold': True
                        }
                    }
                },
                'fields': 'userEnteredFormat.textFormat.bold'
            }
        })
    
    # Apply banding to ALL data columns FIRST (so empty cells get alternating colors)
    if len(data_rows) > 0:
        requests.append({
            'addBanding': {
                'bandedRange': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': 3,
                        'endRowIndex': 3 + len(data_rows),
                        'startColumnIndex': 0,
                        'endColumnIndex': SHEET_FORMAT['total_columns']
                    },
                    'rowProperties': {
                        'firstBandColor': white,
                        'secondBandColor': light_gray
                    }
                }
            }
        })
        
        # Apply percentile colors to stat cells AFTER banding
        # This way, empty cells keep the alternating row colors, but cells with data get percentile colors
        for row_idx, player_percentiles in enumerate(percentile_data):
            for col_idx, percentile in player_percentiles.items():
                if percentile is not None:
                    color = get_color_for_percentile(percentile)
                    if color:
                        requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': worksheet.id,
                                    'startRowIndex': 3 + row_idx,
                                    'endRowIndex': 3 + row_idx + 1,
                                    'startColumnIndex': col_idx,
                                    'endColumnIndex': col_idx + 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'backgroundColor': color
                                    }
                                },
                                'fields': 'userEnteredFormat.backgroundColor'
                            }
                        })
    
    # Column widths
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
                'dimension': 'COLUMNS',
                'startIndex': 1,
                'endIndex': 2
            },
            'properties': {
                'pixelSize': SHEET_FORMAT['column_widths']['jersey_number']
            },
            'fields': 'pixelSize'
        }
    })
    
    requests.append({
        'updateDimensionProperties': {
            'range': {
                'sheetId': worksheet.id,
                'dimension': 'COLUMNS',
                'startIndex': 8,
                'endIndex': 9
            },
            'properties': {
                'pixelSize': SHEET_FORMAT['column_widths']['games']
            },
            'fields': 'pixelSize'
        }
    })
    
    # Borders
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 3,
                'endRowIndex': 3 + len(data_rows),
                'startColumnIndex': 6,
                'endColumnIndex': 7,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': black
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 3,
                'endRowIndex': 3 + len(data_rows),
                'startColumnIndex': 7,
                'endColumnIndex': 8,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': black
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 6,
                'endColumnIndex': 7,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 7,
                'endColumnIndex': 8,
            },
            'left': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 0,
                'endRowIndex': 3,
                'startColumnIndex': 7,
                'endColumnIndex': 8,
            },
            'right': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    requests.append({
        'updateBorders': {
            'range': {
                'sheetId': worksheet.id,
                'startRowIndex': 1,
                'endRowIndex': 2,
                'startColumnIndex': 0,
                'endColumnIndex': SHEET_FORMAT['total_columns']
            },
            'top': {
                'style': 'SOLID',
                'width': 2,
                'color': white
            }
        }
    })
    
    # WHITE border to the right of each column B-X in rows 2-3 (exclude Y, column 24)
    for col_idx in range(1, SHEET_FORMAT['total_columns'] - 1):
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 1,
                    'endRowIndex': 3,
                    'startColumnIndex': col_idx,
                    'endColumnIndex': col_idx + 1,
                },
                'right': {
                    'style': 'SOLID',
                    'width': 2,
                    'color': white
                }
            }
        })
    
    # Freeze panes
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': worksheet.id,
                'gridProperties': {
                    'frozenRowCount': SHEET_FORMAT['frozen']['rows'],
                    'frozenColumnCount': SHEET_FORMAT['frozen']['columns']
                }
            },
            'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'
        }
    })
    
    # Hide gridlines
    requests.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': worksheet.id,
                'gridProperties': {
                    'hideGridlines': True
                }
            },
            'fields': 'gridProperties.hideGridlines'
        }
    })
    
    # Add filter
    requests.append({
        'setBasicFilter': {
            'filter': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 2,
                    'endRowIndex': 3 + len(data_rows),
                    'startColumnIndex': 0,
                    'endColumnIndex': SHEET_FORMAT['total_columns']
                }
            }
        }
    })
    
    # Auto-resize columns
    for col_idx in range(SHEET_FORMAT['total_columns']):
        if col_idx not in [1, 8]:
            requests.append({
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': col_idx,
                        'endIndex': col_idx + 1
                    }
                }
            })
    
    # Execute all requests
    spreadsheet.batch_update({'requests': requests})
    
    log(f"✅ {team_name} sheet created with {len(data_rows)} players")

def main():
    log("=" * 60)
    log("SYNCING ALL 30 NBA TEAMS TO GOOGLE SHEETS")
    log("=" * 60)
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        log("✅ Database connection established")
    except Exception as e:
        log(f"❌ Database connection error: {e}")
        return False
    
    # Fetch all players data
    log("Fetching all players data for percentile calculation...")
    all_players = fetch_all_players_data(conn)
    log(f"✅ Fetched {len(all_players)} total players")
    
    # Calculate percentiles
    log("Calculating percentiles across all players...")
    percentiles, players_with_stats = calculate_percentiles(all_players)
    log("✅ Percentiles calculated")
    
    conn.close()
    
    # Group players by team
    teams_data = {}
    for player in players_with_stats:
        team_abbr = player['team_abbr']
        if team_abbr not in teams_data:
            teams_data[team_abbr] = []
        teams_data[team_abbr].append(player)
    
    # Connect to Google Sheets
    spreadsheet_name = GOOGLE_SHEETS_CONFIG['spreadsheet_name']
    try:
        gc = get_google_sheets_client()
        spreadsheet = gc.open(spreadsheet_name)
        log(f"✅ Opened spreadsheet: {spreadsheet_name}")
    except gspread.SpreadsheetNotFound:
        log(f"❌ Spreadsheet '{spreadsheet_name}' not found")
        return False
    except Exception as e:
        log(f"❌ Error connecting to Google Sheets: {e}")
        return False
    
    # Create/update sheets for each team
    for idx, (team_abbr, team_name) in enumerate(NBA_TEAMS):
        team_players = teams_data.get(team_abbr, [])
        if not team_players:
            log(f"⚠️  No data found for {team_name}, skipping...")
            continue
        
        try:
            worksheet = spreadsheet.worksheet(team_abbr)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=team_abbr, rows=100, cols=30)
        
        create_team_sheet(worksheet, team_abbr, team_name, team_players, percentiles)
        
        # Add a 3-second delay after each team to avoid rate limits
        if idx < len(NBA_TEAMS) - 1:
            time.sleep(3)
    
    log("=" * 60)
    log("✅ SUCCESS! All teams synced to Google Sheets")
    log(f"   View it here: {spreadsheet.url}")
    log("=" * 60)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
