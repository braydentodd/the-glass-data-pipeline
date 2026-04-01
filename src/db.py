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
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Dict

import psycopg2

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
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        database=os.getenv('DB_NAME', ''),
        user=os.getenv('DB_USER', ''),
        password=os.getenv('DB_PASSWORD', '')
    )


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


# ============================================================================
# SCHEMA AUTO-SYNC
# ============================================================================

def ensure_schema(db_schema: str, tables_config: dict, db_columns: dict,
                  conn=None) -> Dict[str, list]:
    """Ensure all tables exist and have every column defined in config.

    Compares config-driven column definitions against ``information_schema``
    and issues ``ALTER TABLE … ADD COLUMN IF NOT EXISTS`` for anything
    missing.  Designed to run **once per ETL invocation** (not per-write)
    so there is zero per-row overhead.

    Args:
        db_schema:     Schema name (``'nba'`` or ``'ncaa'``).
        tables_config: ``TABLES_CONFIG`` dict mapping table names →
                       ``{'entity': …, 'contents': …}``.
        db_columns:    ``DB_COLUMNS`` dict mapping column names →
                       ``{'table': 'entity'|'stats'|'both', 'type': …, …}``.
        conn:          Optional existing connection.  If *None*, a new
                       connection is created (and closed on exit).

    Returns:
        Dict mapping schema-qualified table name → list of columns added.
        Empty lists mean the table already had all columns.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()

    try:
        added: Dict[str, list] = {}

        # --- Build {table_name: set_of_column_names} from TABLES_CONFIG ------
        table_cols: Dict[str, Dict[str, str]] = {}  # table -> {col: pg_type}
        stats_tables = {
            f"{db_schema}.{name}"
            for name, meta in tables_config.items()
            if meta['contents'] == 'stats'
        }
        entity_tables = {
            f"{db_schema}.{name}"
            for name, meta in tables_config.items()
            if meta['contents'] == 'entity'
        }

        for col_name, col_meta in db_columns.items():
            if not isinstance(col_meta, dict):
                continue
            col_scope = col_meta.get('table', '')
            col_type = col_meta.get('type', 'INTEGER')

            target_tables = set()
            if col_scope == 'entity':
                target_tables = entity_tables
            elif col_scope == 'stats':
                target_tables = stats_tables
            elif col_scope == 'both':
                target_tables = entity_tables | stats_tables

            for tbl in target_tables:
                table_cols.setdefault(tbl, {})[col_name] = col_type

        # --- Query information_schema for existing columns -------------------
        with conn.cursor() as cur:
            # Ensure the schema itself exists
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")

            for qual_table, expected_cols in table_cols.items():
                bare_table = qual_table.split('.', 1)[1]

                # Check if table exists at all
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (db_schema, bare_table),
                )
                if cur.fetchone() is None:
                    # Table doesn't exist yet — skip (CREATE TABLE is the
                    # ETL's responsibility on first run; we only add columns)
                    logger.info(
                        'ensure_schema: table %s does not exist yet, skipping',
                        qual_table,
                    )
                    added[qual_table] = []
                    continue

                # Fetch existing columns
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (db_schema, bare_table),
                )
                existing = {row[0] for row in cur.fetchall()}

                missing = []
                for col, pg_type in expected_cols.items():
                    if col not in existing:
                        cur.execute(
                            f'ALTER TABLE {qual_table} '
                            f'ADD COLUMN IF NOT EXISTS {quote_col(col)} {pg_type}'
                        )
                        missing.append(col)

                added[qual_table] = missing
                if missing:
                    logger.info(
                        'ensure_schema: added %d column(s) to %s: %s',
                        len(missing), qual_table, ', '.join(missing),
                    )

        conn.commit()
        return added

    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()
