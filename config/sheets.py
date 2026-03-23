"""
The Glass - Shared Sheets Configuration

Constants shared identically between NBA and NCAA display layers.
League-specific configs import from here to stay DRY.
"""

import os

# ============================================================================
# COLORS & PERCENTILES (identical across leagues)
# ============================================================================

COLORS = {
    'red': {'red': 0.933, 'green': 0.294, 'blue': 0.169},
    'yellow': {'red': 0.988, 'green': 0.961, 'blue': 0.373},
    'green': {'red': 0.298, 'green': 0.733, 'blue': 0.090},
    'black': {'red': 0, 'green': 0, 'blue': 0},
    'white': {'red': 1, 'green': 1, 'blue': 1},
    'light_gray': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
    'dark_gray': {'red': 67/255, 'green': 67/255, 'blue': 67/255},
    'row_alt': {'red': 0.94, 'green': 0.94, 'blue': 0.94},
}

COLOR_THRESHOLDS = {
    'low': 0,    # 0% = pure red
    'mid': 50,   # 50% = pure yellow
    'high': 100, # 100% = pure green
}


# ============================================================================
# SHEET FORMATTING CONFIG (identical across leagues)
# ============================================================================

SHEET_FORMATTING = {
    # Fonts
    'header_font': 'Staatliches',
    'data_font': 'Sofia Sans',

    # Font sizes
    'section_header_size': 12,
    'team_name_size': 15,
    'subsection_header_size': 11,
    'column_header_size': 10,
    'data_size': 10,

    # Header styling
    'header_bg': 'black',
    'header_fg': 'white',

    # Data row alternating colors (uses addBanding so colors survive sorting)
    'row_even_bg': 'white',
    'row_odd_bg': 'row_alt',

    # Borders
    'border_weight': 2,
    'header_border_color': 'white',
    'data_border_color': 'black',

    # Alignment
    'default_h_align': 'CENTER',
    'default_v_align': 'MIDDLE',
    'left_align_columns': ['names', 'notes'],
    'bold_columns': ['names'],

    # Overflow handling
    'wrap_strategy': 'CLIP',

    # Default visibility
    'hide_advanced_columns': True,
    'hide_subsection_row': True,
    'hide_identity_section': True,

    # Percentile companion column formatting
    'percentile_companion_width': 10,      # pixels
    'percentile_companion_font_size': 5,   # pt

    # Layout — 4 header rows
    # Row 0: Section headers (team name merged into entities section)
    # Row 1: Subsection headers (hidden by default)
    # Row 2: Column names
    # Row 3: Filter row (auto-filter dropdowns)
    'section_header_row': 0,
    'subsection_header_row': 1,
    'column_header_row': 2,
    'filter_row': 3,
    'data_start_row': 4,
    'header_row_count': 4,

    # Freeze
    'frozen_rows': 4,
    'frozen_cols': 1,

    # Row sections
    'row_sections': ['current_players', 'team_opponent'],

    # Rate limiting
    'sync_delay_seconds': 3,
}


# ============================================================================
# SECTION CONFIG (identical across leagues)
# ============================================================================

SECTION_CONFIG = {
    'entities': {
        'display_name': 'Names',
    },
    'player_info': {
        'display_name': 'Player Info',
    },
    'analysis': {
        'display_name': 'Analysis',
    },
    'current_stats': {
        'display_name': 'Current Stats',
        'is_stats_section': True,
    },
    'historical_stats': {
        'display_name': 'Historical Stats',
        'is_stats_section': True,
    },
    'postseason_stats': {
        'display_name': 'Postseason Stats',
        'is_stats_section': True,
    },
    'identity': {
        'display_name': 'Identity',
    },
}

# Section order — left-to-right column layout (identical across leagues)
SECTIONS = [
    'entities',
    'player_info',
    'analysis',
    'current_stats',
    'historical_stats',
    'postseason_stats',
    'identity',
]


# ============================================================================
# COLUMN DEFINITION DEFAULTS
# ============================================================================

# Default values for SHEETS_COLUMNS entries — league configs only need to
# override fields that differ from these defaults.
COLUMN_DEFAULTS = {
    'stat_category': 'none',
    'section': ['current_stats'],
    'subsection': None,
    'sheets': ['all_teams', 'all_players', 'teams'],
    'stat_mode': 'both',
    'has_percentile': False,
    'is_stat': False,
    'editable': False,
    'reverse_percentile': False,
    'scale_with_mode': False,
    'format': 'number',
    'decimal_places': 0,
    'player_formula': None,
    'team_formula': None,
    'opponents_formula': None,
    'minimum_width': 'auto',
}


# ============================================================================
# API & SERVER CONFIGURATION (identical across leagues)
# ============================================================================

API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', '5000')),
    'debug': os.getenv('API_DEBUG', 'False').lower() == 'true',
    'cors_enabled': True,
}

SERVER_CONFIG = {
    'production_host': os.getenv('PRODUCTION_HOST', ''),
    'production_port': int(os.getenv('PRODUCTION_PORT', '5000')),
    'ssh_user': os.getenv('SSH_USER', ''),
    'remote_dir': os.getenv('REMOTE_DIR', ''),
    'systemd_service': os.getenv('SYSTEMD_SERVICE', 'flask-api'),
}
