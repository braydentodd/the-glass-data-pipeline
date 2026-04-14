"""
The Glass - Core Configuration & Constants

Shared values, standards, and conventions used across both ETL and Publish processes.
"""

# ============================================================================
# SEASON FORMATTING
# ============================================================================

def format_season_label(season_year: int) -> str:
    """
    Convert end-year integer to a descriptive season string.
    Example: 2026 -> '2025-26'
    """
    return f"{season_year - 1}-{str(season_year)[2:]}"

# ============================================================================
# SCHEMA VALIDATION DEFINITIONS
# ============================================================================

CORE_CONFIG_SCHEMA = {
    'SEASON_TYPE_GROUPS': {
        'regular_season': {'required': True, 'types': (tuple, list)},
        'postseason': {'required': True, 'types': (tuple, list)},
    },
    'SEASON_TYPE_LABELS': { # Using dict string match conceptually
    }
}

# ============================================================================
# SEASON TYPE CLASSIFICATION
# Season type codes are grouped into "regular_season" vs "postseason".
# Queries use these groups to decide which season_type values to aggregate.
# ============================================================================

SEASON_TYPE_GROUPS = {
    'regular_season': ('rs',),
    'postseason': ('po', 'pi', 'ct'),
}

SEASON_TYPE_LABELS = {
    'rs': 'Regular Season',
    'po': 'Playoffs',
    'pi': 'Play-In',
    'ct': 'Conference Tournament',
}
