from typing import Dict
from src.publish.definitions.config import COLORS, COLOR_THRESHOLDS

def get_color_for_percentile(percentile: float, reverse: bool = False) -> Dict:
    """Get RGB color dict (values 0-1) for a percentile using red->yellow->green gradient."""
    if reverse:
        percentile = 100 - percentile
    percentile = max(0, min(100, percentile))

    red, yellow, green = COLORS['red'], COLORS['yellow'], COLORS['green']
    mid = COLOR_THRESHOLDS['mid']

    if percentile < mid:
        ratio = percentile / mid
        return {
            'red': red['red'] + (yellow['red'] - red['red']) * ratio,
            'green': red['green'] + (yellow['green'] - red['green']) * ratio,
            'blue': red['blue'] + (yellow['blue'] - red['blue']) * ratio,
        }
    else:
        ratio = (percentile - mid) / (COLOR_THRESHOLDS['high'] - mid)
        return {
            'red': yellow['red'] + (green['red'] - yellow['red']) * ratio,
            'green': yellow['green'] + (green['green'] - yellow['green']) * ratio,
            'blue': yellow['blue'] + (green['blue'] - yellow['blue']) * ratio,
        }

def get_color_dict(color_name: str) -> Dict:
    """Get color dict from COLORS constant."""
    return COLORS.get(color_name, COLORS['white'])

def get_color_for_raw(color_dict: Dict) -> Dict:
    """Ensure a color dict has the right keys for Sheets API."""
    return {
        'red': color_dict.get('red', 0),
        'green': color_dict.get('green', 0),
        'blue': color_dict.get('blue', 0),
    }

def create_text_format(font_family=None, font_size=None, bold=False,
                       foreground_color='white') -> dict:
    """Helper to create a Sheets API textFormat dict."""
    fmt = {
        'foregroundColor': get_color_for_raw(COLORS[foreground_color]),
        'bold': bold,
    }
    if font_family:
        fmt['fontFamily'] = font_family
    if font_size:
        fmt['fontSize'] = font_size
    return fmt

def create_cell_format(background_color='white', text_format=None,
                       h_align='CENTER', v_align='MIDDLE', wrap='CLIP') -> dict:
    """Helper to create a Sheets API cellFormat dict."""
    fmt = {
        'backgroundColor': get_color_for_raw(COLORS[background_color]),
        'horizontalAlignment': h_align,
        'verticalAlignment': v_align,
        'wrapStrategy': wrap,
    }
    if text_format:
        fmt['textFormat'] = text_format
    return fmt

def get_border_style(weight: int, color: dict) -> dict:
    """Create a Sheets API border description."""
    if weight == 1:
        return {'style': 'SOLID', 'color': color}
    elif weight == 2:
        return {'style': 'SOLID_MEDIUM', 'color': color}
    elif weight >= 3:
        return {'style': 'SOLID_THICK', 'color': color}
    return {'style': 'NONE'}
