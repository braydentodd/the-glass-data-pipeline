"""
The Glass - Source Registry

Declares available data sources, the leagues they feed, and
which column in the entity tables holds each source's external ID.

Leagues double as PostgreSQL schema names (e.g. league 'nba' → schema 'nba').
Source module paths follow convention and are not stored here:
    config  → src.etl.sources.{source_key}.config
    client  → src.etl.sources.{source_key}.client
"""

SOURCES = {
    'nba_api': {
        'leagues': ['nba'],
        'source_id_column': 'nba_api_id',
    },
}


def get_source_for_league(league: str) -> str:
    """Return the source key that provides data for a league."""
    for source_key, meta in SOURCES.items():
        if league in meta['leagues']:
            return source_key
    raise ValueError(f"No source registered for league: {league!r}")


def get_source_id_column(league: str) -> str:
    """Return the source_id column name for a league's entity tables."""
    source_key = get_source_for_league(league)
    return SOURCES[source_key]['source_id_column']
