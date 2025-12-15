"""
Generic stat calculation and percentile engine.
Everything driven by COLUMN_DEFINITIONS - zero hardcoding.
"""

import numpy as np
from src.config import COLUMN_DEFINITIONS, PERCENTILE_CONFIG, STAT_CONSTANTS


# ============================================================================
# HELPER FUNCTIONS - Centralized logic used across multiple functions
# ============================================================================

def get_field_alias(db_field):
    """
    Calculate the alias name for a database field.
    Handles _x10, _x1000, and other naming conventions.
    
    Examples:
        'minutes_x10' -> 'minutes_total'
        'off_rating_x10' -> 'off_rating'
        'off_reb_pct_x1000' -> 'oreb_pct'
    """
    if db_field == 'minutes_x10':
        return 'minutes_total'
    elif db_field == 'off_reb_pct_x1000':
        return 'oreb_pct'
    elif db_field == 'def_reb_pct_x1000':
        return 'dreb_pct'
    elif db_field.endswith('_x10'):
        return db_field.replace('_x10', '')
    elif db_field.endswith('_x1000'):
        return db_field.replace('_x1000', '')
    else:
        return db_field

def get_opponent_stat_name(regular_stat_name):
    """
    Map a regular stat name to its opponent stat equivalent.
    Returns None if there's no opponent stat for this stat.
    
    Examples:
        'points' -> 'opp_points'
        'fg2_pct' -> 'opp_fg2_pct'
        'oreb_pct' -> 'opp_ors'
        'games' -> None (no opponent equivalent)
    """
    # Special mappings for rebounds (percentages become counts)
    if regular_stat_name == 'oreb_pct':
        return 'opp_ors'
    elif regular_stat_name == 'dreb_pct':
        return 'opp_drs'
    
    # Stats that have direct opponent equivalents
    opponent_stats = ['fg2_pct', 'fg3_pct', 'ft_pct', 'ts_pct', 'fg2a', 'fg3a', 'fta',
                     'points', 'assists', 'turnovers', 'steals', 'blocks', 'fouls']
    
    if regular_stat_name in opponent_stats:
        return f'opp_{regular_stat_name}'
    
    return None

def get_physical_attribute_columns():
    """
    Get list of physical attribute column keys from COLUMN_DEFINITIONS.
    Returns keys where is_physical_attribute=True.
    """
    return [col_key for col_key, col_def in COLUMN_DEFINITIONS.items() 
            if col_def.get('is_physical_attribute')]


# ============================================================================
# SQL QUERY BUILDER - Auto-generates SELECT fields from config
# ============================================================================

def build_select_fields(entity_type='player', include_opponent=False, context='current'):
    """
    Build SQL SELECT fields dynamically from COLUMN_DEFINITIONS.
    
    Args:
        entity_type: 'player' or 'team'
        include_opponent: Whether to include opponent stats (team only)
        context: 'current', 'historical', or 'postseason' - determines which fields to include
    
    Returns:
        List of SQL field expressions (with aliases)
    """
    fields = []
    
    # Always include entity identifiers
    if entity_type == 'player':
        fields.extend([
            'p.player_id',
            'p.name AS player_name',
            'p.team_id',
            'p.jersey_number',
            'p.height_inches',
            'p.weight_lbs',
        ])
    else:  # team
        fields.extend([
            't.team_id',
            't.team_abbr',
            't.team_name',
        ])
    
    # Iterate through column definitions to build stat fields
    for col_key, col_def in COLUMN_DEFINITIONS.items():
        # Skip non-stat columns and opponent stats (handled separately)
        if not col_def.get('is_stat'):
            continue
        if col_def.get('is_opponent_stat'):
            continue
        # Skip physical attributes - these come from players table, not stats table
        if col_def.get('is_physical_attribute'):
            continue
        # Skip player-only fields when fetching team stats
        if entity_type == 'team' and col_def.get('player_only'):
            continue
        
        # Filter by context - only include fields appropriate for this query type
        if context == 'current' and not col_def.get('in_current'):
            continue
        elif context == 'historical' and not col_def.get('in_historical'):
            continue
        elif context == 'postseason' and not col_def.get('in_postseason'):
            continue
        
        db_field = col_def.get('db_field')
        if not db_field:
            continue  # Calculated fields handled later
        
        # Handle fields that need division (e.g., minutes_x10 / 10, off_rating_x10 / 10)
        if col_def.get('divide_by_10'):
            alias = get_field_alias(db_field)
            if entity_type == 'player':
                fields.append(f's.{db_field}::float / 10 AS {alias}')
            else:
                fields.append(f's.{db_field}::float / 10 AS {alias}')
        # Handle fields that need division by 1000 (e.g., off_reb_pct_x1000 / 1000)
        elif col_def.get('divide_by_1000'):
            alias = get_field_alias(db_field)
            if entity_type == 'player':
                fields.append(f's.{db_field}::float / 1000 AS {alias}')
            else:
                fields.append(f's.{db_field}::float / 1000 AS {alias}')
            # Also add totals field if specified (e.g., off_rebounds for totals mode)
            if col_def.get('db_field_totals'):
                totals_field = col_def['db_field_totals']
                fields.append(f's.{totals_field}')
        # Handle fields with special totals mode (e.g., oreb_pct vs off_rebounds)
        elif col_def.get('db_field_totals'):
            if entity_type == 'player':
                fields.append(f's.{db_field}')
                fields.append(f's.{col_def["db_field_totals"]}')
            else:
                fields.append(f's.{db_field}')
                fields.append(f's.{col_def["db_field_totals"]}')
        else:
            if entity_type == 'player':
                fields.append(f's.{db_field}')
            else:
                fields.append(f's.{db_field}')
    
    # Add raw fields needed for calculated stats
    # Note: possessions is calculated from fg2a + fg3a - off_rebounds + turnovers + 0.44*fta
    # Note: oreb_pct and dreb_pct are added via additional_fields in fetch functions
    raw_fields_needed = ['fg2m', 'fg2a', 'fg3m', 'fg3a', 'ftm', 'fta', 
                         'off_rebounds', 'def_rebounds', 'games_played', 
                         'minutes_x10', 'assists', 'turnovers',
                         'steals', 'blocks', 'fouls', 'off_rating_x10', 'def_rating_x10']
    
    for field in raw_fields_needed:
        field_str = f's.{field}'
        # Don't add if already present (e.g., minutes_x10 if minutes added above)
        if field_str not in fields and not any(f' AS {field}' in str(f) or f.startswith(f's.{field}::') for f in fields):
            fields.append(field_str)
    
    # Add opponent stats if requested (team only)
    if include_opponent and entity_type == 'team':
        opponent_raw_fields = [
            's.opp_fg2m', 's.opp_fg2a', 's.opp_fg3m', 's.opp_fg3a',
            's.opp_ftm', 's.opp_fta', 's.opp_off_rebounds', 's.opp_def_rebounds',
            's.opp_assists', 's.opp_turnovers', 's.opp_steals', 's.opp_blocks', 's.opp_fouls'
        ]
        fields.extend(opponent_raw_fields)
    
    return fields


def build_aggregated_select_fields(entity_type='player', include_opponent=False):
    """
    Build SQL SELECT fields for aggregated queries (SUM, AVG, etc).
    Used for historical and postseason queries.
    
    Returns:
        List of SQL field expressions with aggregations
    """
    fields = []
    
    # Identifiers (no aggregation)
    if entity_type == 'player':
        fields.extend([
            'p.player_id',
            'p.name AS player_name',
            'p.team_id',
            'p.jersey_number',
            'p.height_inches',
            'p.weight_lbs',
        ])
    else:  # team
        fields.extend([
            't.team_id',
            't.team_abbr',
            't.team_name',
        ])
    
    # Aggregated stats
    fields.extend([
        'COUNT(DISTINCT s.year) AS seasons_played',
        'SUM(s.games_played) AS games_played',
        'SUM(s.minutes_x10::float) / 10 AS minutes_total',
        # Possessions calculated: FGA - OREB + TOV + 0.44*FTA
        'SUM(s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)) AS possessions',
        'SUM(s.fg2m) AS fg2m',
        'SUM(s.fg2a) AS fg2a',
        'SUM(s.fg3m) AS fg3m',
        'SUM(s.fg3a) AS fg3a',
        'SUM(s.ftm) AS ftm',
        'SUM(s.fta) AS fta',
        'SUM(s.off_rebounds) AS off_rebounds',
        'SUM(s.def_rebounds) AS def_rebounds',
        # Use calculated possessions for weighted averages
        'SUM(s.off_reb_pct_x1000 * (s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)))::float / NULLIF(SUM(s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)), 0) / 1000 AS oreb_pct',
        'SUM(s.def_reb_pct_x1000 * (s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)))::float / NULLIF(SUM(s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)), 0) / 1000 AS dreb_pct',
        'SUM(s.off_rating_x10 * (s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)))::float / NULLIF(SUM(s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)), 0) / 10 AS off_rating',
        'SUM(s.def_rating_x10 * (s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)))::float / NULLIF(SUM(s.fg2a + s.fg3a - s.off_rebounds + s.turnovers + (0.44 * s.fta)), 0) / 10 AS def_rating',
        'SUM(s.assists) AS assists',
        'SUM(s.turnovers) AS turnovers',
        'SUM(s.steals) AS steals',
        'SUM(s.blocks) AS blocks',
        'SUM(s.fouls) AS fouls',
    ])
    
    # Add opponent stats aggregation if requested
    if include_opponent and entity_type == 'team':
        fields.extend([
            'SUM(s.opp_fg2m) AS opp_fg2m',
            'SUM(s.opp_fg2a) AS opp_fg2a',
            'SUM(s.opp_fg3m) AS opp_fg3m',
            'SUM(s.opp_fg3a) AS opp_fg3a',
            'SUM(s.opp_ftm) AS opp_ftm',
            'SUM(s.opp_fta) AS opp_fta',
            'SUM(s.opp_off_rebounds) AS opp_off_rebounds',
            'SUM(s.opp_def_rebounds) AS opp_def_rebounds',
            'SUM(s.opp_assists) AS opp_assists',
            'SUM(s.opp_turnovers) AS opp_turnovers',
            'SUM(s.opp_steals) AS opp_steals',
            'SUM(s.opp_blocks) AS opp_blocks',
            'SUM(s.opp_fouls) AS opp_fouls',
        ])
    
    return fields


# ============================================================================
# STAT CALCULATION ENGINE - Generic calculation from config
# ============================================================================

def calculate_stat_value(entity_data, col_key, mode='per_36', custom_value=None):
    """
    Calculate a single stat value based on column definition and mode.
    
    Args:
        entity_data: Dict with raw data from database
        col_key: Column key from COLUMN_DEFINITIONS
        mode: 'totals', 'per_36', 'per_game', 'per_minutes', 'per_100_poss'
        custom_value: Custom value for per_minutes or per_100_poss modes
    
    Returns:
        Calculated stat value
    """
    col_def = COLUMN_DEFINITIONS[col_key]
    
    # Get scaling factor based on mode
    if mode == 'totals':
        factor = 1.0
    elif mode == 'per_game':
        games = entity_data.get('games_played', 0) or 0
        factor = 1.0 / games if games > 0 else 0
    elif mode == 'per_36' or mode == 'per_minutes':
        minutes_total = entity_data.get('minutes_total', 0) or 0
        target_minutes = float(custom_value) if mode == 'per_minutes' and custom_value else STAT_CONSTANTS['default_per_minutes']
        factor = target_minutes / minutes_total if minutes_total > 0 else 0
    elif mode == 'per_100_poss':
        possessions = entity_data.get('possessions', 0) or 0
        possessions = float(possessions) if possessions else 0  # Convert Decimal to float
        target_poss = float(custom_value) if custom_value else STAT_CONSTANTS['default_per_possessions']
        factor = target_poss / possessions if possessions > 0 else 0
    else:
        factor = 1.0
    
    # Handle calculated fields
    if col_def.get('calculated'):
        if col_key == 'points':
            fg2m = entity_data.get('fg2m', 0) or 0
            fg3m = entity_data.get('fg3m', 0) or 0
            ftm = entity_data.get('ftm', 0) or 0
            return (fg2m * 2 + fg3m * 3 + ftm) * factor
        
        elif col_key == 'opp_points':
            opp_fg2m = entity_data.get('opp_fg2m', 0) or 0
            opp_fg3m = entity_data.get('opp_fg3m', 0) or 0
            opp_ftm = entity_data.get('opp_ftm', 0) or 0
            return (opp_fg2m * 2 + opp_fg3m * 3 + opp_ftm) * factor
        
        elif col_key == 'ts_pct':
            fg2m = entity_data.get('fg2m', 0) or 0
            fg3m = entity_data.get('fg3m', 0) or 0
            ftm = entity_data.get('ftm', 0) or 0
            fg2a = entity_data.get('fg2a', 0) or 0
            fg3a = entity_data.get('fg3a', 0) or 0
            fta = entity_data.get('fta', 0) or 0
            
            points = fg2m * 2 + fg3m * 3 + ftm
            fga = fg2a + fg3a
            ts_attempts = 2 * (fga + STAT_CONSTANTS['ts_fta_multiplier'] * fta)
            return (points / ts_attempts) if ts_attempts > 0 else 0
        
        elif col_key == 'opp_ts_pct':
            opp_fg2m = entity_data.get('opp_fg2m', 0) or 0
            opp_fg3m = entity_data.get('opp_fg3m', 0) or 0
            opp_ftm = entity_data.get('opp_ftm', 0) or 0
            opp_fg2a = entity_data.get('opp_fg2a', 0) or 0
            opp_fg3a = entity_data.get('opp_fg3a', 0) or 0
            opp_fta = entity_data.get('opp_fta', 0) or 0
            
            opp_points = opp_fg2m * 2 + opp_fg3m * 3 + opp_ftm
            opp_fga = opp_fg2a + opp_fg3a
            opp_ts_attempts = 2 * (opp_fga + STAT_CONSTANTS['ts_fta_multiplier'] * opp_fta)
            return (opp_points / opp_ts_attempts) if opp_ts_attempts > 0 else 0
        
        elif col_key == 'fg2_pct':
            fg2m = entity_data.get('fg2m', 0) or 0
            fg2a = entity_data.get('fg2a', 0) or 0
            return (fg2m / fg2a) if fg2a > 0 else 0
        
        elif col_key == 'fg3_pct':
            fg3m = entity_data.get('fg3m', 0) or 0
            fg3a = entity_data.get('fg3a', 0) or 0
            return (fg3m / fg3a) if fg3a > 0 else 0
        
        elif col_key == 'ft_pct':
            ftm = entity_data.get('ftm', 0) or 0
            fta = entity_data.get('fta', 0) or 0
            return (ftm / fta) if fta > 0 else 0
    
    # Handle fields with denominator (percentages)
    elif col_def.get('db_field_denominator'):
        numerator = entity_data.get(col_def['db_field'], 0) or 0
        denominator = entity_data.get(col_def['db_field_denominator'], 0) or 0
        return (numerator / denominator) if denominator > 0 else 0
    
    # Handle special totals mode fields (oreb_pct -> off_rebounds in totals)
    elif mode == 'totals' and col_def.get('db_field_totals'):
        value = entity_data.get(col_def['db_field_totals'], 0) or 0
        return value * factor
    
    # Handle fields that need division (already divided in SQL, so just scale by factor)
    elif col_def.get('divide_by_10'):
        # The SQL already divided by 10 and aliased, so use the alias (e.g., 'off_rating' not 'off_rating_x10')
        alias = get_field_alias(col_def['db_field'])
        value = entity_data.get(alias, 0) or 0
        # Ratings are already per-game/per-possession metrics - don't scale them
        if col_key in ['off_rating', 'def_rating']:
            return value
        return value * factor
    
    # Standard fields
    else:
        db_field = col_def.get('db_field')
        if not db_field:
            return 0
        
        # Special handling for non-scaled fields in certain modes
        if col_key in ['oreb_pct', 'dreb_pct']:
            # Percentages already divided in SQL and aliased (oreb_pct, dreb_pct), don't scale with factor
            return entity_data.get(col_key, 0) or 0
        
        value = entity_data.get(db_field, 0) or 0
        return value * factor


def calculate_entity_stats(entity_data, stat_columns, mode='per_36', custom_value=None):
    """
    Calculate all stats for an entity (player/team) based on mode.
    
    Args:
        entity_data: Dict with raw data from database
        stat_columns: List of stat column keys to calculate
        mode: Calculation mode
        custom_value: Custom value for per_minutes/per_100_poss
    
    Returns:
        Dict of calculated stats
    """
    calculated = {}
    
    for col_key in stat_columns:
        if col_key not in COLUMN_DEFINITIONS:
            continue
        
        # Special handling for minutes display
        if col_key == 'minutes':
            if mode == 'totals':
                calculated[col_key] = entity_data.get('minutes_total', 0)
            elif mode == 'per_game':
                games = entity_data.get('games_played', 0) or 0
                calculated[col_key] = entity_data.get('minutes_total', 0) / games if games > 0 else 0
            else:  # per_36, per_minutes, per_100_poss
                games = entity_data.get('games_played', 0) or 0
                calculated[col_key] = entity_data.get('minutes_total', 0) / games if games > 0 else 0
        
        # Special handling for possessions display in per_100_poss mode
        elif col_key == 'possessions' and mode == 'per_100_poss':
            games = entity_data.get('games_played', 0) or 0
            calculated[col_key] = entity_data.get('possessions', 0) / games if games > 0 else 0
        
        # Special handling for games (never scaled)
        elif col_key == 'games':
            calculated[col_key] = entity_data.get('games_played', 0)
        
        # Special handling for years (never scaled)
        elif col_key == 'years':
            calculated[col_key] = entity_data.get('seasons_played', 0)
        
        # Calculate all other stats
        else:
            calculated[col_key] = calculate_stat_value(entity_data, col_key, mode, custom_value)
    
    return calculated


# ============================================================================
# PERCENTILE CALCULATION ENGINE - Generic percentile calculation
# ============================================================================

def calculate_percentiles_generic(entities_data, stat_columns, mode='per_36', custom_value=None, 
                                  entity_type='player', use_minutes_weighting=True):
    """
    Generic percentile calculation for any entity type and stat set.
    
    Args:
        entities_data: List of entity dicts with raw data
        stat_columns: List of stat column keys to calculate percentiles for
        mode: Calculation mode
        custom_value: Custom value for per_minutes/per_100_poss
        entity_type: 'player', 'team', or 'opponent'
        use_minutes_weighting: Whether to weight by minutes (True for players, False for teams)
    
    Returns:
        (percentiles_dict, entities_with_calculated_stats)
    """
    # Calculate stats for all entities
    entities_with_stats = []
    for entity in entities_data:
        stats = calculate_entity_stats(entity, stat_columns, mode, custom_value)
        if stats:
            entity['calculated_stats'] = stats
            entities_with_stats.append(entity)
    
    # Calculate percentiles for each stat
    percentiles = {}
    
    for stat_name in stat_columns:
        if stat_name == 'years':  # Skip years, has custom logic
            continue
        
        if stat_name not in COLUMN_DEFINITIONS:
            continue
        
        col_def = COLUMN_DEFINITIONS[stat_name]
        
        # Physical attributes - no weighting, use all entities
        if col_def.get('is_physical_attribute'):
            values = []
            for entity in entities_data:
                if stat_name == 'age':
                    value = entity.get('age', 0)
                elif stat_name == 'height':
                    value = entity.get('height_inches', 0)
                elif stat_name == 'weight':
                    value = entity.get('weight_lbs', 0)
                elif stat_name == 'wingspan':
                    value = entity.get('wingspan_inches', 0)
                else:
                    value = 0
                
                if value and value > 0:
                    values.append(float(value))
            
            if values:
                percentiles[stat_name] = np.percentile(values, range(101))
            else:
                percentiles[stat_name] = None
        
        # Regular stats - with optional minutes weighting
        else:
            if use_minutes_weighting:
                # Weighted samples (for players)
                weighted_values = []
                for entity in entities_with_stats:
                    stat_value = entity['calculated_stats'].get(stat_name, 0)
                    if stat_value and stat_value != 0:
                        minutes_weight = entity.get('minutes_total', 0)
                        if minutes_weight > 0:
                            weight_count = max(1, int(round(minutes_weight / PERCENTILE_CONFIG['minutes_weight_factor'])))
                            weighted_values.extend([float(stat_value)] * weight_count)
                
                if weighted_values:
                    percentiles[stat_name] = np.percentile(weighted_values, range(101))
                else:
                    percentiles[stat_name] = None
            else:
                # No weighting (for teams/opponents)
                values = []
                for entity in entities_with_stats:
                    stat_value = entity['calculated_stats'].get(stat_name, 0)
                    if stat_value and stat_value != 0:
                        values.append(float(stat_value))
                
                if values:
                    percentiles[stat_name] = np.percentile(values, range(101))
                else:
                    percentiles[stat_name] = None
    
    return percentiles, entities_with_stats


def get_percentile_for_value(percentiles_array, value):
    """
    Get percentile rank for a value given a percentile array.
    
    Args:
        percentiles_array: numpy array from np.percentile(values, range(101))
        value: The value to find percentile for
    
    Returns:
        Percentile rank (0-100)
    """
    if percentiles_array is None or value is None:
        return None
    
    # Find where value falls in percentile array
    percentile = np.searchsorted(percentiles_array, value)
    return min(100, max(0, percentile))
