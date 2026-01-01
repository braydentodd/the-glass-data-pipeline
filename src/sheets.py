"""
THE GLASS - Google Sheets Sync
Config-driven orchestrator for syncing NBA data to Google Sheets.

Architecture:
1. Fetch data from database (players, teams, stats)
2. Calculate stats based on view mode (per_game, per_100, per_36, totals)
3. Calculate percentiles for all stats
4. Build rows dynamically from DISPLAY_COLUMNS config
5. Format and sync to Google Sheets

NO HARDCODING. Everything driven by config/sheets.py and config/db.py.
"""

import os
import sys
import psycopg2
from google.oauth2.service_account import Credentials
import gspread
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import configuration
from config.db import DB_CONFIG, NBA_CONFIG, get_nba_teams
from config.sheets import (
    GOOGLE_SHEETS_CONFIG,
    SECTIONS,
    SECTION_CONFIG,
    SUBSECTIONS,
    DISPLAY_COLUMNS,
    COLORS,
    STAT_CONSTANTS,
    SHEET_FORMAT,
    # Helper functions
    get_columns_by_filters,
    get_columns_for_section_and_entity,
    build_sheet_columns,
    build_headers,
    get_column_index,
    calculate_stat_value,
    format_stat_value,
    generate_percentile_columns,
    get_all_columns_with_percentiles,
    get_percentile_rank,
    get_color_for_percentile,
    get_color_dict,
    create_text_format,
    create_cell_format,
    format_height,
)


def log(message):
    """Log message with timestamp."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def get_sheets_client():
    """Initialize Google Sheets API client."""
    credentials = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['credentials_file'],
        scopes=GOOGLE_SHEETS_CONFIG['scopes']
    )
    return gspread.authorize(credentials)


# ============================================================================
# DATA FETCHING - 100% CONFIG-DRIVEN (ZERO HARDCODING)
# ============================================================================
# All SQL queries are dynamically generated from DISPLAY_COLUMNS config.
# NO column names are hardcoded anywhere - everything comes from the config.
# This ensures the system remains flexible and maintainable.
# ============================================================================

def _get_columns_for_query(section, entity_type='player', include_stats=True):
    """
    Get all columns needed for a database query - 100% config-driven.
    
    Args:
        section: Section to fetch ('current_stats', 'historical_stats', 'postseason_stats')
        entity_type: 'player', 'team', or 'opponent'
        include_stats: Whether to include stat columns
    
    Returns:
        Dict with 'player_cols', 'team_cols', and 'stat_cols' lists
    """
    result = {
        'player_cols': [],
        'team_cols': [],
        'stat_cols': []
    }
    
    # Get all columns that apply to this entity and section
    for col_key, col_def in DISPLAY_COLUMNS.items():
        if entity_type not in col_def.get('applies_to_entities', []):
            continue
        
        # Check if column appears in this section or related sections
        col_sections = col_def.get('section', [])
        is_relevant = (
            section in col_sections or
            'player_info' in col_sections or  # Always include player_info
            'identity' in col_sections  # Always include identity
        )
        
        if not is_relevant:
            continue
        
        db_field = col_def.get('db_field')
        if not db_field:
            continue
        
        # Skip calculated columns - they don't exist in database
        if col_def.get('calculated', False):
            continue
        
        is_stat = col_def.get('is_stat', False)
        
        if is_stat and include_stats:
            result['stat_cols'].append(db_field)
        elif not is_stat:
            # Determine which table the field comes from
            # Player fields: player_id, name, height, wingspan, birthdate, notes, etc.
            # Team fields: team_abbr, team_name, team_id
            if entity_type == 'player':
                if db_field in ['team_abbr', 'team_name', 'team_id']:
                    result['team_cols'].append(db_field)
                else:
                    result['player_cols'].append(db_field)
            else:  # team or opponent
                result['team_cols'].append(db_field)
    
    # Remove duplicates while preserving order
    result['player_cols'] = list(dict.fromkeys(result['player_cols']))
    result['team_cols'] = list(dict.fromkeys(result['team_cols']))
    result['stat_cols'] = list(dict.fromkeys(result['stat_cols']))
    
    return result


def fetch_players_for_team(conn, team_abbr, section='current_stats', years_config=None):
    """
    Fetch player data for a team based on section.
    100% CONFIG-DRIVEN - NO HARDCODING
    
    Args:
        conn: Database connection
        team_abbr: Team abbreviation (e.g., 'LAL')
        section: 'current_stats', 'historical_stats', or 'postseason_stats'
        years_config: Dict with 'mode', 'value', 'include_current' for historical/postseason
    
    Returns:
        List of player dicts with stats
    """
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    # Get columns from config - NO HARDCODING
    cols = _get_columns_for_query(section, 'player', include_stats=True)
    
    if section == 'current_stats':
        # Build SELECT clause dynamically
        player_fields = [f'p."{col}"' if not col.isidentifier() else f'p.{col}' for col in cols['player_cols']]
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f's."{col}"' for col in cols['stat_cols']]
        
        all_fields = player_fields + team_fields + stat_fields
        
        query = f"""
        SELECT 
            {', '.join(all_fields)}
        FROM players p
        INNER JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN player_season_stats s 
            ON s.player_id = p.player_id 
            AND s.year = %s
            AND s.season_type = %s
        WHERE t.team_abbr = %s
        ORDER BY COALESCE(s.minutes_x10, 0) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_year, season_type, team_abbr))
            rows = cur.fetchall()
            
    elif section == 'historical_stats':
        year_filter, params = _build_year_filter(years_config, current_year, season_type=1)
        
        # For aggregated stats, we need SUM() for stat columns
        player_fields = [f'p."{col}"' if not col.isidentifier() else f'p.{col}' for col in cols['player_cols']]
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f'SUM(s."{col}") as "{col}"' for col in cols['stat_cols']]
        
        # For GROUP BY, only include player and team fields
        group_by_fields = player_fields + team_fields
        
        query = f"""
        SELECT 
            {', '.join(player_fields + team_fields + stat_fields)}
        FROM players p
        INNER JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN player_season_stats s 
            ON s.player_id = p.player_id 
            {year_filter}
            AND s.season_type = 1
        WHERE t.team_abbr = %s
        GROUP BY {', '.join(group_by_fields)}
        ORDER BY SUM(COALESCE(s.minutes_x10, 0)) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            rows = cur.fetchall()
            
    elif section == 'postseason_stats':
        year_filter, params = _build_year_filter(years_config, current_year, season_type='2,3')
        
        player_fields = [f'p."{col}"' if not col.isidentifier() else f'p.{col}' for col in cols['player_cols']]
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f'SUM(s."{col}") as "{col}"' for col in cols['stat_cols']]
        
        group_by_fields = player_fields + team_fields
        
        query = f"""
        SELECT 
            {', '.join(player_fields + team_fields + stat_fields)}
        FROM players p
        INNER JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN player_season_stats s 
            ON s.player_id = p.player_id 
            {year_filter}
            AND s.season_type IN (2, 3)
        WHERE t.team_abbr = %s
        GROUP BY {', '.join(group_by_fields)}
        ORDER BY SUM(COALESCE(s.minutes_x10, 0)) DESC, p.name
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            rows = cur.fetchall()
    
    return [dict(row) for row in rows]


def fetch_all_players(conn, section='current_stats', years_config=None):
    """
    Fetch all players for percentile calculations.
    100% CONFIG-DRIVEN - NO HARDCODING
    """
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    cols = _get_columns_for_query(section, 'player', include_stats=True)
    
    if section == 'current_stats':
        player_fields = [f'p."{col}"' if not col.isidentifier() else f'p.{col}' for col in cols['player_cols']]
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f's."{col}"' for col in cols['stat_cols']]
        
        all_fields = player_fields + team_fields + stat_fields
        
        query = f"""
        SELECT 
            {', '.join(all_fields)}
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN player_season_stats s 
            ON s.player_id = p.player_id 
            AND s.year = %s
            AND s.season_type = %s
        WHERE s.player_id IS NOT NULL
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_year, season_type))
            rows = cur.fetchall()
            
    elif section in ['historical_stats', 'postseason_stats']:
        season_filter = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, season_type=season_filter)
        
        player_fields = [f'p."{col}"' if not col.isidentifier() else f'p.{col}' for col in cols['player_cols']]
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f'SUM(s."{col}") as "{col}"' for col in cols['stat_cols']]
        
        group_by_fields = player_fields + team_fields
        
        query = f"""
        SELECT 
            {', '.join(player_fields + team_fields + stat_fields)}
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN player_season_stats s 
            ON s.player_id = p.player_id 
            {year_filter}
            AND s.season_type IN ({season_filter})
        WHERE s.player_id IS NOT NULL
        GROUP BY {', '.join(group_by_fields)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    
    return [dict(row) for row in rows]


def fetch_team_stats(conn, team_abbr, section='current_stats', years_config=None):
    """
    Fetch team and opponent stats for a team.
    100% CONFIG-DRIVEN - NO HARDCODING
    
    Returns:
        Dict with 'team' and 'opponent' stats
    """
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    # Get columns for team entity
    cols = _get_columns_for_query(section, 'team', include_stats=True)
    
    if section == 'current_stats':
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f's."{col}"' for col in cols['stat_cols']]
        
        # Get opponent stat columns
        opp_stat_fields = []
        for col_key, col_def in DISPLAY_COLUMNS.items():
            if col_def.get('is_stat') and section in col_def.get('section', []) and 'opponent' in col_def.get('applies_to_entities', []):
                db_field = col_def.get('db_field')
                if db_field and f'opp_{db_field}' in _get_all_db_fields():
                    opp_stat_fields.append(f's."opp_{db_field}"')
        
        all_fields = team_fields + stat_fields + opp_stat_fields
        
        query = f"""
        SELECT 
            {', '.join(all_fields)}
        FROM teams t
        LEFT JOIN team_season_stats s 
            ON s.team_id = t.team_id 
            AND s.year = %s
            AND s.season_type = %s
        WHERE t.team_abbr = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_year, season_type, team_abbr))
            row = cur.fetchone()
            
    elif section in ['historical_stats', 'postseason_stats']:
        season_filter = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, season_type=season_filter)
        
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f'SUM(s."{col}") as "{col}"' for col in cols['stat_cols']]
        
        # Get opponent stat columns
        opp_stat_fields = []
        for col_key, col_def in DISPLAY_COLUMNS.items():
            if col_def.get('is_stat') and section in col_def.get('section', []) and 'opponent' in col_def.get('applies_to_entities', []):
                db_field = col_def.get('db_field')
                if db_field and f'opp_{db_field}' in _get_all_db_fields():
                    opp_stat_fields.append(f'SUM(s."opp_{db_field}") as "opp_{db_field}"')
        
        all_fields = team_fields + stat_fields + opp_stat_fields
        group_by_fields = team_fields
        
        query = f"""
        SELECT 
            {', '.join(all_fields)}
        FROM teams t
        LEFT JOIN team_season_stats s 
            ON s.team_id = t.team_id 
            {year_filter}
            AND s.season_type IN ({season_filter})
        WHERE t.team_abbr = %s
        GROUP BY {', '.join(group_by_fields)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params + (team_abbr,))
            row = cur.fetchone()
    
    if not row:
        return {'team': {}, 'opponent': {}}
    
    result = dict(row)
    # Separate team and opponent stats
    team_data = {k: v for k, v in result.items() if not k.startswith('opp_')}
    opponent_data = {k.replace('opp_', ''): v for k, v in result.items() if k.startswith('opp_')}
    
    return {'team': team_data, 'opponent': opponent_data}


def fetch_all_teams(conn, section='current_stats', years_config=None):
    """
    Fetch all teams for percentile calculations.
    100% CONFIG-DRIVEN - NO HARDCODING
    """
    current_year = NBA_CONFIG['current_season_year']
    season_type = NBA_CONFIG['season_type']
    
    cols = _get_columns_for_query(section, 'team', include_stats=True)
    
    if section == 'current_stats':
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f's."{col}"' for col in cols['stat_cols']]
        
        # Get opponent stat columns
        opp_stat_fields = []
        for col_key, col_def in DISPLAY_COLUMNS.items():
            if col_def.get('is_stat') and section in col_def.get('section', []) and 'opponent' in col_def.get('applies_to_entities', []):
                db_field = col_def.get('db_field')
                if db_field and f'opp_{db_field}' in _get_all_db_fields():
                    opp_stat_fields.append(f's."opp_{db_field}"')
        
        all_fields = team_fields + stat_fields + opp_stat_fields
        
        query = f"""
        SELECT 
            {', '.join(all_fields)}
        FROM teams t
        LEFT JOIN team_season_stats s 
            ON s.team_id = t.team_id 
            AND s.year = %s
            AND s.season_type = %s
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (current_year, season_type))
            rows = cur.fetchall()
            
    elif section in ['historical_stats', 'postseason_stats']:
        season_filter = 1 if section == 'historical_stats' else '2, 3'
        year_filter, params = _build_year_filter(years_config, current_year, season_type=season_filter)
        
        team_fields = [f't."{col}"' if not col.isidentifier() else f't.{col}' for col in cols['team_cols']]
        stat_fields = [f'SUM(s."{col}") as "{col}"' for col in cols['stat_cols']]
        
        # Get opponent stat columns
        opp_stat_fields = []
        for col_key, col_def in DISPLAY_COLUMNS.items():
            if col_def.get('is_stat') and section in col_def.get('section', []) and 'opponent' in col_def.get('applies_to_entities', []):
                db_field = col_def.get('db_field')
                if db_field and f'opp_{db_field}' in _get_all_db_fields():
                    opp_stat_fields.append(f'SUM(s."opp_{db_field}") as "opp_{db_field}"')
        
        all_fields = team_fields + stat_fields + opp_stat_fields
        group_by_fields = team_fields
        
        query = f"""
        SELECT 
            {', '.join(all_fields)}
        FROM teams t
        LEFT JOIN team_season_stats s 
            ON s.team_id = t.team_id 
            {year_filter}
            AND s.season_type IN ({season_filter})
        GROUP BY {', '.join(group_by_fields)}
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    
    # Separate into teams and opponents lists
    teams = []
    opponents = []
    for row in rows:
        result = dict(row)
        team_data = {k: v for k, v in result.items() if not k.startswith('opp_')}
        opponent_data = {k.replace('opp_', ''): v for k, v in result.items() if k.startswith('opp_')}
        teams.append(team_data)
        opponents.append(opponent_data)
    
    return {'teams': teams, 'opponents': opponents}


def _build_year_filter(years_config, current_year, season_type):
    """
    Build SQL year filter based on years configuration.
    
    Args:
        years_config: Dict with 'mode', 'value', 'include_current'
        current_year: Current season year
        season_type: Season type filter value
    
    Returns:
        Tuple of (filter_string, params_tuple)
    """
    if not years_config:
        # Default: last 3 years excluding current
        years = [current_year - i for i in range(1, 4)]
        return "AND s.year IN %s", (tuple(years),)
    
    mode = years_config.get('mode', 'years')
    value = years_config.get('value', 3)
    include_current = years_config.get('include_current', False)
    
    if mode == 'career':
        # All years
        return "AND s.year > 0", ()
    
    elif mode == 'years':
        # Last N years
        start_offset = 0 if include_current else 1
        years = [current_year - i for i in range(start_offset, start_offset + value)]
        return "AND s.year IN %s", (tuple(years),)
    
    elif mode == 'seasons':
        # Specific seasons list
        return "AND s.year IN %s", (tuple(value),)
    
    else:
        # Default fallback
        return "AND s.year > 0", ()


def _get_all_db_fields():
    """Get set of all db_field values from DISPLAY_COLUMNS."""
    return {col.get('db_field') for col in DISPLAY_COLUMNS.values() if col.get('db_field')}


# ============================================================================
# PERCENTILE CALCULATIONS
# ============================================================================

def calculate_all_percentiles(all_players, all_teams_data, section='current_stats'):
    """
    Calculate percentiles for all entities (players, teams, opponents).
    
    Args:
        all_players: List of player dicts
        all_teams_data: Dict with 'teams' and 'opponents' lists
        section: Which section to calculate for
    
    Returns:
        Dict with percentile arrays for each stat by entity type
    """
    percentiles = {
        'player': {},
        'team': {},
        'opponent': {}
    }
    
    # Get columns that need percentiles for this section
    cols_with_percentiles = {
        k: v for k, v in DISPLAY_COLUMNS.items()
        if v.get('has_percentile') and section in v.get('section', [])
    }
    
    # Calculate player percentiles
    for col_key, col_def in cols_with_percentiles.items():
        if 'player' in col_def.get('applies_to_entities', []):
            db_field = col_def['db_field']
            values = []
            for player in all_players:
                val = player.get(db_field)
                if val is not None:
                    # Apply transformations
                    if col_def.get('divide_by_10'):
                        val = val / 10.0
                    elif col_def.get('divide_by_1000'):
                        val = val / 1000.0
                    values.append(val)
            
            if values:
                percentiles['player'][col_key] = sorted(values)
    
    # Calculate team percentiles
    for col_key, col_def in cols_with_percentiles.items():
        if 'team' in col_def.get('applies_to_entities', []):
            db_field = col_def['db_field']
            values = []
            for team in all_teams_data.get('teams', []):
                val = team.get(db_field)
                if val is not None:
                    if col_def.get('divide_by_10'):
                        val = val / 10.0
                    elif col_def.get('divide_by_1000'):
                        val = val / 1000.0
                    values.append(val)
            
            if values:
                percentiles['team'][col_key] = sorted(values)
    
    # Calculate opponent percentiles
    for col_key, col_def in cols_with_percentiles.items():
        if 'opponent' in col_def.get('applies_to_entities', []):
            db_field = col_def['db_field']
            values = []
            for opponent in all_teams_data.get('opponents', []):
                val = opponent.get(db_field)
                if val is not None:
                    if col_def.get('divide_by_10'):
                        val = val / 10.0
                    elif col_def.get('divide_by_1000'):
                        val = val / 1000.0
                    values.append(val)
            
            if values:
                percentiles['opponent'][col_key] = sorted(values)
    
    return percentiles


# ============================================================================
# ROW BUILDING
# ============================================================================

def build_player_row(player_data, columns, percentiles, entity_type='player', years_str=None):
    """
    Build a row for a player based on column definitions.
    
    Args:
        player_data: Player dict with stats
        columns: List of column defs from build_sheet_columns()
        percentiles: Percentile arrays dict
        entity_type: 'player'
        years_str: String showing year range (for historical/postseason)
    
    Returns:
        List of cell values
    """
    row = []
    
    for col_def in columns:
        col_key = col_def['key']
        
        # Handle special columns
        if col_key == 'years':
            row.append(years_str or '')
            continue
        
        # Check if this is a percentile column
        if col_key.endswith('_pct'):
            base_key = col_key[:-4]  # Remove _pct suffix
            base_col = DISPLAY_COLUMNS.get(base_key)
            
            if base_col and entity_type in percentiles:
                db_field = base_col['db_field']
                value = player_data.get(db_field)
                
                if value is not None:
                    # Apply transformations
                    if base_col.get('divide_by_10'):
                        value = value / 10.0
                    elif base_col.get('divide_by_1000'):
                        value = value / 1000.0
                    
                    # Get percentile rank
                    if base_key in percentiles[entity_type]:
                        percentile_array = percentiles[entity_type][base_key]
                        reverse = base_col.get('reverse_percentile', False)
                        percentile = get_percentile_rank(value, percentile_array, reverse)
                        row.append(f"{percentile}%")
                    else:
                        row.append('')
                else:
                    row.append('')
            else:
                row.append('')
        
        # Regular stat or info column
        else:
            db_field = col_def.get('db_field')
            value = player_data.get(db_field)
            
            # Format the value
            formatted = format_stat_value(value, col_def)
            row.append(formatted if formatted is not None else '')
    
    return row


def build_team_row(team_data, columns, percentiles, entity_type='team', years_str=None):
    """
    Build a row for team or opponent based on column definitions.
    
    Args:
        team_data: Team/opponent dict with stats
        columns: List of column defs from build_sheet_columns()
        percentiles: Percentile arrays dict
        entity_type: 'team' or 'opponent'
        years_str: String showing year range (for historical/postseason)
    
    Returns:
        List of cell values
    """
    row = []
    
    for col_def in columns:
        col_key = col_def['key']
        
        # Handle special columns
        if col_key == 'name':
            # First column is entity type label
            label = 'TEAM' if entity_type == 'team' else 'OPPONENT'
            row.append(label)
            continue
        
        if col_key == 'years':
            row.append(years_str or '')
            continue
        
        # Check if this is a percentile column
        if col_key.endswith('_pct'):
            base_key = col_key[:-4]
            base_col = DISPLAY_COLUMNS.get(base_key)
            
            if base_col and entity_type in percentiles:
                db_field = base_col['db_field']
                value = team_data.get(db_field)
                
                if value is not None:
                    if base_col.get('divide_by_10'):
                        value = value / 10.0
                    elif base_col.get('divide_by_1000'):
                        value = value / 1000.0
                    
                    if base_key in percentiles[entity_type]:
                        percentile_array = percentiles[entity_type][base_key]
                        reverse = base_col.get('reverse_percentile', False)
                        percentile = get_percentile_rank(value, percentile_array, reverse)
                        row.append(f"{percentile}%")
                    else:
                        row.append('')
                else:
                    row.append('')
            else:
                row.append('')
        
        # Regular stat column
        else:
            db_field = col_def.get('db_field')
            value = team_data.get(db_field)
            
            formatted = format_stat_value(value, col_def)
            row.append(formatted if formatted is not None else '')
    
    return row


def format_years_range(years_config, current_year):
    """
    Format year range string for display.
    
    Returns: String like "22-23, 23-24, 24-25" or "Career"
    """
    if not years_config:
        # Default: last 3 years
        years = [current_year - i for i in range(1, 4)]
        return ', '.join([f"{str(y)[-2:]}-{str(y+1)[-2:]}" for y in sorted(years)])
    
    mode = years_config.get('mode', 'years')
    
    if mode == 'career':
        return 'Career'
    
    elif mode == 'years':
        value = years_config.get('value', 3)
        include_current = years_config.get('include_current', False)
        start_offset = 0 if include_current else 1
        years = [current_year - i for i in range(start_offset, start_offset + value)]
        return ', '.join([f"{str(y)[-2:]}-{str(y+1)[-2:]}" for y in sorted(years)])
    
    elif mode == 'seasons':
        years = years_config.get('value', [])
        return ', '.join([f"{str(y)[-2:]}-{str(y+1)[-2:]}" for y in sorted(years)])
    
    return ''


# ============================================================================
# SHEET SYNCING
# ============================================================================

def sync_team_sheet(client, spreadsheet, team_abbr, view_mode='basic', show_percentiles=False,
                    historical_config=None, postseason_config=None):
    """
    Sync a single team sheet with all sections.
    
    Args:
        client: Google Sheets client
        spreadsheet: Spreadsheet object
        team_abbr: Team abbreviation
        view_mode: 'basic' or 'advanced'
        show_percentiles: Whether to show percentile columns
        historical_config: Config for historical stats section
        postseason_config: Config for postseason stats section
    """
    log(f"📊 Syncing sheet for {team_abbr}...")
    
    # Get or create worksheet
    try:
        worksheet = spreadsheet.worksheet(team_abbr)
        worksheet.clear()
    except:
        worksheet = spreadsheet.add_worksheet(title=team_abbr, rows=100, cols=100)
    
    conn = get_db_connection()
    
    try:
        # Fetch data for all sections
        current_players = fetch_players_for_team(conn, team_abbr, 'current_stats')
        historical_players = fetch_players_for_team(conn, team_abbr, 'historical_stats', historical_config)
        postseason_players = fetch_players_for_team(conn, team_abbr, 'postseason_stats', postseason_config)
        
        team_stats = fetch_team_stats(conn, team_abbr, 'current_stats')
        historical_team_stats = fetch_team_stats(conn, team_abbr, 'historical_stats', historical_config)
        postseason_team_stats = fetch_team_stats(conn, team_abbr, 'postseason_stats', postseason_config)
        
        # Fetch all data for percentiles (pre-calculate)
        all_players_current = fetch_all_players(conn, 'current_stats')
        all_players_historical = fetch_all_players(conn, 'historical_stats', historical_config)
        all_players_postseason = fetch_all_players(conn, 'postseason_stats', postseason_config)
        
        all_teams_current = fetch_all_teams(conn, 'current_stats')
        all_teams_historical = fetch_all_teams(conn, 'historical_stats', historical_config)
        all_teams_postseason = fetch_all_teams(conn, 'postseason_stats', postseason_config)
        
        # Calculate percentiles
        percentiles_current = calculate_all_percentiles(all_players_current, all_teams_current, 'current_stats')
        percentiles_historical = calculate_all_percentiles(all_players_historical, all_teams_historical, 'historical_stats')
        percentiles_postseason = calculate_all_percentiles(all_players_postseason, all_teams_postseason, 'postseason_stats')
        
        # Build column structure
        columns = build_sheet_columns(entity_type='player', view_mode=view_mode, show_percentiles=show_percentiles)
        
        # Build headers
        header_rows = build_headers(columns)
        
        # Build data rows
        data_rows = []
        
        # Current stats players
        for player in current_players:
            row = build_player_row(player, columns, percentiles_current, 'player')
            data_rows.append(row)
        
        # Historical stats players
        years_str = format_years_range(historical_config, NBA_CONFIG['current_season_year'])
        for player in historical_players:
            row = build_player_row(player, columns, percentiles_historical, 'player', years_str)
            data_rows.append(row)
        
        # Postseason stats players
        years_str_post = format_years_range(postseason_config, NBA_CONFIG['current_season_year'])
        for player in postseason_players:
            row = build_player_row(player, columns, percentiles_postseason, 'player', years_str_post)
            data_rows.append(row)
        
        # Team rows - ALWAYS LAST 2 ROWS (dynamic positioning)
        team_row = build_team_row(team_stats['team'], columns, percentiles_current, 'team')
        opponent_row = build_team_row(team_stats['opponent'], columns, percentiles_current, 'opponent')
        
        # Combine all rows
        all_rows = header_rows + data_rows + [team_row, opponent_row]
        
        # Write to sheet
        worksheet.update('A1', all_rows, value_input_option='USER_ENTERED')
        
        # Apply formatting
        _apply_sheet_formatting(worksheet, len(header_rows), len(data_rows) + 2, len(columns))
        
        log(f"✅ {team_abbr} synced successfully")
        
    finally:
        conn.close()


def sync_all_teams(view_mode='basic', show_percentiles=False, historical_config=None, 
                   postseason_config=None, priority_team=None):
    """
    Sync all team sheets in the spreadsheet.
    
    Args:
        view_mode: 'basic' or 'advanced'
        show_percentiles: Whether to show percentile columns
        historical_config: Config for historical stats
        postseason_config: Config for postseason stats
        priority_team: Team to sync first (optional)
    """
    log("🚀 Starting full sync...")
    
    client = get_sheets_client()
    spreadsheet = client.open(GOOGLE_SHEETS_CONFIG['spreadsheet_name'])
    
    teams = get_nba_teams()
    
    # Sort teams: priority first, then alphabetical
    if priority_team:
        teams = sorted(teams, key=lambda t: (t['abbr'] != priority_team, t['abbr']))
    else:
        teams = sorted(teams, key=lambda t: t['abbr'])
    
    for team in teams:
        try:
            sync_team_sheet(
                client, 
                spreadsheet, 
                team['abbr'],
                view_mode=view_mode,
                show_percentiles=show_percentiles,
                historical_config=historical_config,
                postseason_config=postseason_config
            )
        except Exception as e:
            log(f"❌ Error syncing {team['abbr']}: {e}")
            continue
    
    log("✅ All teams synced!")


def _apply_sheet_formatting(worksheet, header_row_count, data_row_count, col_count):
    """
    Apply formatting to worksheet.
    
    Args:
        worksheet: Worksheet object
        header_row_count: Number of header rows
        data_row_count: Number of data rows (including team/opponent)
        col_count: Number of columns
    """
    # Freeze header rows and first column
    worksheet.freeze(rows=header_row_count, cols=1)
    
    # TODO: Add more formatting (colors, fonts, borders) using batch API requests
    # This will be implemented in future iterations
    pass


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main(priority_team=None, view_mode='basic', show_percentiles=False, 
         historical_config=None, postseason_config=None):
    """
    Main entry point for sheets sync.
    
    Args:
        priority_team: Team abbreviation to sync first
        view_mode: 'basic' or 'advanced'
        show_percentiles: Whether to show percentiles
        historical_config: Dict with historical stats config
        postseason_config: Dict with postseason stats config
    """
    sync_all_teams(
        view_mode=view_mode,
        show_percentiles=show_percentiles,
        historical_config=historical_config,
        postseason_config=postseason_config,
        priority_team=priority_team
    )


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync NBA data to Google Sheets')
    parser.add_argument('--team', help='Priority team to sync first')
    parser.add_argument('--view', choices=['basic', 'advanced'], default='basic', help='View mode')
    parser.add_argument('--percentiles', action='store_true', help='Show percentiles instead of values')
    
    args = parser.parse_args()
    
    main(
        priority_team=args.team,
        view_mode=args.view,
        show_percentiles=args.percentiles
    )
