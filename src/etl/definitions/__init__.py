"""
The Glass - ETL Definitions Package

Re-exports all definition symbols and provides source-registry helpers.
"""

from src.etl.definitions.config import (                           # noqa: F401
    DB_COLUMNS_SCHEMA,
    ETL_CONFIG,
    ETL_CONFIG_SCHEMA,
    ETL_TABLES,
    SOURCES,
    TABLES,
    TABLES_SCHEMA,
    TYPE_TRANSFORMS,
    VALID_ENTITY_TYPES,
    VALID_PG_TYPES,
    VALID_SCOPES,
    VALID_UPDATE_FREQUENCIES,
)
from src.etl.definitions.columns import DB_COLUMNS                   # noqa: F401


def get_source_for_league(league: str) -> str:
    """Return the source key that provides data for a league."""
    for source_key, meta in SOURCES.items():
        if league in meta['leagues']:
            return source_key
    raise ValueError(f"No source registered for league: {league!r}")


def get_source_id_column(league: str) -> str:
    """Return the source_id column name for a league's entity tables.

    Convention: ``{source_key}_id`` (e.g. ``nba_api_id``).
    """
    return f'{get_source_for_league(league)}_id'
