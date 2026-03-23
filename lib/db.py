"""
The Glass - Shared Database Module

Provides a single source of truth for database connections, shared across
both NBA and NCAA pipelines. Eliminates duplicated connection code.

Both leagues share the same PostgreSQL database with separate schemas:
  - nba.*   (players, teams, player_season_stats, team_season_stats, endpoint_tracker)
  - ncaa.*  (players, teams, player_season_stats, team_season_stats)

Note: lib/nba_etl.py maintains its own ThreadedConnectionPool for the
heavy NBA pipeline. This module is the canonical connection source for
NCAA and lightweight shared callers.
"""
import logging
from contextlib import contextmanager
from datetime import datetime

import psycopg2

from config.db import DB_CONFIG

logger = logging.getLogger(__name__)


# ============================================================================
# SHARED UTILITIES
# ============================================================================

def quote_col(col: str) -> str:
    """Quote a column name for PostgreSQL. Handles digit-starting names like 2fgm."""
    return f'"{col}"'


def get_db_connection():
    """
    Create a new database connection.

    Caller is responsible for calling conn.close().
    Prefer using db_connection() context manager for short operations.
    """
    return psycopg2.connect(**DB_CONFIG)


@contextmanager
def db_connection():
    """
    Context manager for a database connection.

    Automatically commits on success, rolls back on exception,
    and closes the connection.

    Usage:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ...")
    """
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================================
# SHARED SEASON UTILITIES
# ============================================================================

def get_current_season_year() -> int:
    """Current season end-year (calendar year of spring semester).

    After August we're already in the next season (e.g. September 2024 → 2025).
    Both NBA and NCAA share this convention.
    """
    now = datetime.now()
    return now.year + 1 if now.month > 8 else now.year


def get_current_season() -> str:
    """Current season as a display string, e.g. '2024-25'."""
    year = get_current_season_year()
    return f"{year - 1}-{str(year)[-2:]}"
