"""
Shared formatting utilities for The Glass Data Pipeline.
Used by both sheets_sync.py and Apps Script to ensure consistent formatting.
"""

from config.sheets import COLORS, SHEET_FORMAT


def get_color_dict(color_name):
    """Get color dict from COLORS constant"""
    return COLORS.get(color_name, COLORS['white'])


def create_text_format(font_family=None, font_size=None, bold=False, foreground_color='white'):
    """Create a text format dict for Google Sheets API"""
    format_dict = {
        'foregroundColor': get_color_dict(foreground_color),
        'bold': bold
    }
    if font_family:
        format_dict['fontFamily'] = font_family
    if font_size:
        format_dict['fontSize'] = font_size
    return format_dict


def create_cell_format(background_color='white', text_format=None, h_align='CENTER', v_align='MIDDLE', wrap='CLIP'):
    """Create a complete cell format dict"""
    cell_format = {
        'backgroundColor': get_color_dict(background_color),
        'horizontalAlignment': h_align,
        'verticalAlignment': v_align,
        'wrapStrategy': wrap
    }
    if text_format:
        cell_format['textFormat'] = text_format
    return cell_format


def create_repeat_cell_request(sheet_id, start_row, end_row, start_col=None, end_col=None, 
                                 cell_format=None, fields='userEnteredFormat'):
    """Create a repeatCell request for formatting a range"""
    range_dict = {
        'sheetId': sheet_id,
        'startRowIndex': start_row,
        'endRowIndex': end_row
    }
    if start_col is not None:
        range_dict['startColumnIndex'] = start_col
    if end_col is not None:
        range_dict['endColumnIndex'] = end_col
        
    return {
        'repeatCell': {
            'range': range_dict,
            'cell': {'userEnteredFormat': cell_format},
            'fields': fields
        }
    }


def create_border_style(style='SOLID', width=1, color='black'):
    """Create a border style dict"""
    return {
        'style': style,
        'width': width,
        'color': get_color_dict(color)
    }


def create_border_request(sheet_id, start_row, end_row, start_col, end_col, 
                          top=None, bottom=None, left=None, right=None):
    """Create an updateBorders request"""
    borders = {}
    if top:
        borders['top'] = top
    if bottom:
        borders['bottom'] = bottom
    if left:
        borders['left'] = left
    if right:
        borders['right'] = right
        
    return {
        'updateBorders': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': start_col,
                'endColumnIndex': end_col
            },
            **borders
        }
    }


def create_merge_request(sheet_id, start_row, end_row, start_col, end_col, merge_type='MERGE_ALL'):
    """Create a mergeCells request"""
    return {
        'mergeCells': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row,
                'endRowIndex': end_row,
                'startColumnIndex': start_col,
                'endColumnIndex': end_col
            },
            'mergeType': merge_type
        }
    }


def create_auto_resize_request(sheet_id, start_col, end_col):
    """Create an autoResizeDimensions request for columns"""
    return {
        'autoResizeDimensions': {
            'dimensions': {
                'sheetId': sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': start_col,
                'endIndex': end_col
            }
        }
    }


def get_section_boundaries(sections):
    """Get list of column indices where section boundaries occur"""
    boundaries = []
    for section_name, section_info in sections.items():
        if section_name != 'hidden':  # Don't add border before hidden column
            boundaries.append(section_info['start_col'])
    return sorted(set(boundaries))


def create_section_border_requests(sheet_id, sections, total_rows, header_rows=3):
    """Create border requests for all section boundaries"""
    requests = []
    boundaries = get_section_boundaries(sections)
    
    white_border = create_border_style('SOLID', 1, 'white')
    black_border = create_border_style('SOLID', 1, 'black')
    
    for boundary_col in boundaries:
        if boundary_col == 0:  # Skip first column
            continue
            
        # White borders in header rows (rows 2-3, indices 1-3)
        requests.append(create_border_request(
            sheet_id, 1, 3, boundary_col, boundary_col,
            left=white_border
        ))
        
        # Black borders in data rows (row 4+, index 3+)
        if total_rows > header_rows:
            requests.append(create_border_request(
                sheet_id, header_rows, total_rows, boundary_col, boundary_col,
                left=black_border
            ))
    
    return requests


def create_header_format_requests(sheet_id, sections, header_rows=3):
    """Create formatting requests for header rows"""
    requests = []
    
    # Row 1 - Primary header (font 12)
    row1_format = create_cell_format(
        background_color='black',
        text_format=create_text_format(
            font_family=SHEET_FORMAT['fonts']['header_primary']['family'],
            font_size=12,  # Row 1 is always 12
            bold=True,
            foreground_color='white'
        )
    )
    requests.append(create_repeat_cell_request(sheet_id, 0, 1, cell_format=row1_format))
    
    # Row 2 - Secondary header (font 10)
    row2_format = create_cell_format(
        background_color='black',
        text_format=create_text_format(
            font_family=SHEET_FORMAT['fonts']['header_secondary']['family'],
            font_size=10,
            bold=True,
            foreground_color='white'
        )
    )
    requests.append(create_repeat_cell_request(sheet_id, 1, 2, cell_format=row2_format))
    
    # Row 3 - Filter row (font 10)
    row3_format = create_cell_format(
        background_color='black',
        text_format=create_text_format(
            font_family=SHEET_FORMAT['fonts']['header_primary']['family'],
            font_size=10,
            bold=True,
            foreground_color='white'
        )
    )
    requests.append(create_repeat_cell_request(sheet_id, 2, 3, cell_format=row3_format))
    
    # A1 special formatting (font 12)
    a1_format = create_cell_format(
        background_color='black',
        text_format=create_text_format(
            font_family=SHEET_FORMAT['fonts']['team_name']['family'],
            font_size=12,
            bold=True,
            foreground_color='white'
        )
    )
    requests.append(create_repeat_cell_request(sheet_id, 0, 1, 0, 1, cell_format=a1_format))
    
    return requests


def create_data_format_requests(sheet_id, total_rows, total_cols, header_rows=3):
    """Create formatting requests for data rows"""
    requests = []
    
    if total_rows <= header_rows:
        return requests
    
    # Format all data rows (font 10, Sofia Sans)
    data_format = create_cell_format(
        text_format=create_text_format(
            font_family=SHEET_FORMAT['fonts']['data']['family'],
            font_size=10
        ),
        h_align='CENTER',
        v_align='MIDDLE'
    )
    requests.append(create_repeat_cell_request(
        sheet_id, header_rows, total_rows, 
        cell_format=data_format,
        fields='userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment)'
    ))
    
    # Format column A (font 12, Sofia Sans, bold)
    col_a_format = create_cell_format(
        text_format=create_text_format(
            font_family=SHEET_FORMAT['fonts']['data']['family'],
            font_size=12,
            bold=True
        )
    )
    requests.append(create_repeat_cell_request(
        sheet_id, header_rows, total_rows, 0, 1,
        cell_format=col_a_format,
        fields='userEnteredFormat.textFormat'
    ))
    
    return requests


def create_section_merge_requests(sheet_id, sections):
    """Create merge requests for section headers in row 1"""
    requests = []
    
    for section_name, section_info in sections.items():
        if section_name == 'hidden':  # Don't merge hidden column
            continue
            
        start_col = section_info['start_col']
        col_count = section_info['column_count']
        
        # Only merge if section has more than 1 column
        if col_count > 1:
            requests.append(create_merge_request(
                sheet_id, 0, 1, start_col, start_col + col_count
            ))
    
    return requests


def create_column_resize_requests(sheet_id, total_cols):
    """Create auto-resize requests for all columns"""
    requests = []
    
    # Resize all columns to fit content
    for col_idx in range(total_cols):
        requests.append(create_auto_resize_request(sheet_id, col_idx, col_idx + 1))
    
    return requests


def build_row_from_sections(player, sections, stats_mode='per_36', 
                            current_stats=None, historical_stats=None, 
                            postseason_stats=None, percentiles=None):
    """
    Build a player data row by iterating through sections.
    This ensures row structure matches section definitions.
    """
    from config.sheets import COLUMN_DEFINITIONS
    
    row = []
    percentile_data = {}
    
    def format_height(inches):
        if not inches:
            return ''
        feet = inches // 12
        remaining_inches = inches % 12
        return f"{feet}'{remaining_inches}\""
    
    def get_stat_value(stat_name, stats_dict, section_type='current'):
        """Get stat value with proper formatting"""
        if not stats_dict:
            return ''
            
        col_def = COLUMN_DEFINITIONS.get(stat_name, {})
        value = stats_dict.get(stat_name, 0)
        
        if value == 0 or value is None:
            return ''
        
        # Handle percentages
        if col_def.get('format_as_percentage'):
            value = value * 100
            
        # Round to decimal places
        decimals = col_def.get('decimal_places', 1)
        rounded = round(value, decimals)
        
        # Return int if whole number
        if rounded == int(rounded):
            return int(rounded)
        return rounded
    
    # Iterate through sections in order
    for section_name, section_info in sections.items():
        columns = section_info['columns']
        
        for col_name in columns:
            col_def = COLUMN_DEFINITIONS.get(col_name, {})
            
            # Name section
            if col_name == 'name':
                row.append(player.get('player_name', ''))
            elif col_name == 'team':
                row.append(player.get('team_abbr', ''))
            
            # Player info section
            elif col_name == 'jersey':
                row.append(player.get('jersey_number', ''))
            elif col_name == 'age':
                age = player.get('age', 0)
                row.append(round(float(age), 1) if age else '')
            elif col_name == 'experience':
                exp = player.get('years_experience')
                row.append(0 if exp == 0 else (exp if exp else ''))
            elif col_name == 'height':
                row.append(format_height(player.get('height_inches')))
            elif col_name == 'weight':
                row.append(player.get('weight_lbs', ''))
            elif col_name == 'wingspan':
                row.append(format_height(player.get('wingspan_inches')))
            
            # Notes section
            elif col_name == 'notes':
                row.append(player.get('notes', ''))
            
            # Stats sections
            elif col_def.get('is_stat'):
                # Determine which stats dict to use
                if section_name == 'current':
                    stats = current_stats
                elif section_name == 'historical':
                    stats = historical_stats
                elif section_name == 'postseason':
                    stats = postseason_stats
                else:
                    stats = None
                
                row.append(get_stat_value(col_name, stats, section_name))
            
            # Hidden section
            elif col_name == 'player_id':
                row.append(str(player.get('player_id', '')))
            
            else:
                row.append('')  # Unknown column
    
    return row, percentile_data


def format_stat_value(value, col_def, stats_mode='per_36'):
    """Format a stat value according to column definition"""
    if value is None or value == 0:
        return ''
    
    # Handle percentages
    if col_def.get('format_as_percentage'):
        # Check if there's a mode-specific override
        if stats_mode == 'totals' and col_def.get('format_as_percentage_totals') is False:
            pass  # Don't format as percentage for totals mode
        else:
            value = value * 100
    
    # Handle division
    if col_def.get('divide_by_10'):
        value = value / 10
    
    # Round to decimal places
    decimals = col_def.get('decimal_places', 1)
    rounded = round(value, decimals)
    
    # Return int if whole number
    if rounded == int(rounded):
        return int(rounded)
    return rounded
