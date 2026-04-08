"""
The Glass - ETL Database Management

Dynamic table creation and schema synchronization driven by the column
registry.  Creates tables that don't exist and adds missing columns to
existing tables -- all from the single source of truth in config.py.
"""

import logging
from typing import Any, Dict, List, Tuple

from src.db import get_db_connection, quote_col
from src.etl.definitions import DB_COLUMNS, ETL_TABLES, TABLES
from src.etl.definitions.sources import get_source_id_column

logger = logging.getLogger(__name__)


# ============================================================================
# COLUMN RESOLUTION
# ============================================================================

def get_table_columns(
    table_name: str,
    table_meta: Dict[str, Any],
) -> List[Tuple[str, Dict[str, Any]]]:
    """Determine which columns belong in a table.

    Walks DB_COLUMNS and includes every column whose scope and entity_types
    match the table.  For tables with ``has_opponent_columns``, also emits
    ``opp_<col>`` entries for columns with 'opponent' in entity_types.
    """
    scope = table_meta['scope']
    entity = table_meta['entity']
    has_opp = table_meta.get('has_opponent_columns', False)

    columns: List[Tuple[str, Dict[str, Any]]] = []

    for col_name, col_meta in DB_COLUMNS.items():
        col_scope = col_meta.get('scope', [])
        if isinstance(col_scope, str):
            col_scope = [col_scope]

        if scope not in col_scope:
            continue

        entity_types = col_meta.get('entity_types', [])
        if entity not in entity_types:
            continue

        columns.append((col_name, col_meta))

        if has_opp and 'opponent' in entity_types:
            columns.append((f'opp_{col_name}', col_meta))

    return columns


# ============================================================================
# DDL GENERATION
# ============================================================================

def _col_ddl(col_name: str, col_meta: Dict[str, Any]) -> str:
    """Generate a single column DDL fragment."""
    col_type = col_meta['type']
    nullable = col_meta.get('nullable', True)
    default = col_meta.get('default')
    pk = col_meta.get('primary_key', False)

    parts = [quote_col(col_name), col_type]
    if pk:
        parts.append('PRIMARY KEY')
    elif not nullable:
        parts.append('NOT NULL')
    if default is not None and not pk:
        parts.append(f'DEFAULT {default}')

    return ' '.join(parts)


# ============================================================================
# TABLE CREATION & SYNC
# ============================================================================

def ensure_tables(db_schema: str, conn=None) -> Dict[str, List[str]]:
    """Create missing tables and add missing columns from configuration.

    Safe to call on every ETL run -- only issues DDL when something is
    actually missing.

    Args:
        db_schema: PostgreSQL schema name (e.g. ``'nba'``).
        conn:      Optional existing connection.

    Returns:
        Dict mapping qualified table name to list of actions taken
        (empty list means nothing was needed).
    """
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()

    try:
        actions: Dict[str, List[str]] = {}
        source_id_col = get_source_id_column(db_schema)

        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")

            for table_name, table_meta in TABLES.items():
                qual_table = f"{db_schema}.{table_name}"
                table_actions: List[str] = []
                columns = get_table_columns(table_name, table_meta)

                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (db_schema, table_name),
                )

                if cur.fetchone() is None:
                    col_defs = [_col_ddl(cn, cm) for cn, cm in columns]

                    unique_key = table_meta.get('unique_key')
                    if unique_key is None and table_meta['scope'] == 'entity':
                        unique_key = [source_id_col]
                    unique_key = unique_key or []
                    if unique_key:
                        uk_cols = ', '.join(quote_col(c) for c in unique_key)
                        col_defs.append(f'UNIQUE ({uk_cols})')

                    create_sql = (
                        f"CREATE TABLE {qual_table} (\n  "
                        + ",\n  ".join(col_defs)
                        + "\n)"
                    )
                    cur.execute(create_sql)
                    table_actions.append(f'created ({len(columns)} columns)')
                    logger.info(
                        'Created table %s with %d columns', qual_table, len(columns),
                    )
                else:
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s",
                        (db_schema, table_name),
                    )
                    existing = {row[0] for row in cur.fetchall()}

                    for col_name, col_meta in columns:
                        if col_name not in existing:
                            cur.execute(
                                f'ALTER TABLE {qual_table} '
                                f'ADD COLUMN IF NOT EXISTS '
                                f'{quote_col(col_name)} {col_meta["type"]}'
                            )
                            table_actions.append(f'added {col_name}')

                    if table_actions:
                        logger.info(
                            'Updated %s: %s',
                            qual_table, ', '.join(table_actions),
                        )

                actions[qual_table] = table_actions

            # ---- ETL operational tables (inline column definitions) ----
            for table_name, table_meta in ETL_TABLES.items():
                qual_table = f"{db_schema}.{table_name}"
                table_actions: List[str] = []
                inline_cols = table_meta.get('columns', {})

                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (db_schema, table_name),
                )

                if cur.fetchone() is None:
                    col_defs = [
                        _col_ddl(cn, cm) for cn, cm in inline_cols.items()
                    ]
                    unique_key = table_meta.get('unique_key', [])
                    if unique_key:
                        uk_cols = ', '.join(quote_col(c) for c in unique_key)
                        col_defs.append(f'UNIQUE ({uk_cols})')

                    create_sql = (
                        f"CREATE TABLE {qual_table} (\n  "
                        + ",\n  ".join(col_defs)
                        + "\n)"
                    )
                    cur.execute(create_sql)
                    table_actions.append(
                        f'created ({len(inline_cols)} columns)'
                    )
                    logger.info(
                        'Created ETL table %s with %d columns',
                        qual_table, len(inline_cols),
                    )
                else:
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s",
                        (db_schema, table_name),
                    )
                    existing = {row[0] for row in cur.fetchall()}

                    for col_name, col_meta in inline_cols.items():
                        if col_name not in existing:
                            cur.execute(
                                f'ALTER TABLE {qual_table} '
                                f'ADD COLUMN IF NOT EXISTS '
                                f'{quote_col(col_name)} {col_meta["type"]}'
                            )
                            table_actions.append(f'added {col_name}')

                    if table_actions:
                        logger.info(
                            'Updated ETL table %s: %s',
                            qual_table, ', '.join(table_actions),
                        )

                actions[qual_table] = table_actions

        conn.commit()
        return actions

    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


# ============================================================================
# STALE DATA PRUNING
# ============================================================================

def prune_stale(entities: List[str], oldest_season: str, db_schema: str) -> int:
    """Delete stats rows older than the retention window, then remove orphaned entities.

    Orphaned entities are entity rows (players/teams) that have no remaining
    stats rows after the prune -- e.g. a player who only appeared in seasons
    that are now outside the retention window.

    Returns total rows deleted.
    """
    logger.info('Phase: prune_stale (before %s)', oldest_season)
    conn = get_db_connection()
    pruned = 0
    try:
        with conn.cursor() as cur:
            for table_name, meta in TABLES.items():
                if meta['scope'] != 'stats':
                    continue
                qualified = f"{db_schema}.{table_name}"
                cur.execute(
                    f"DELETE FROM {qualified} WHERE season < %s",
                    (oldest_season,),
                )
                count = cur.rowcount
                if count:
                    logger.info('Pruned %d rows from %s', count, qualified)
                    pruned += count

            for table_name, meta in TABLES.items():
                if meta['scope'] != 'entity':
                    continue
                entity_type = meta['entity']
                if entity_type not in entities:
                    continue
                stats_table = None
                for st_name, st_meta in TABLES.items():
                    if st_meta['scope'] == 'stats' and st_meta['entity'] == entity_type:
                        stats_table = f"{db_schema}.{st_name}"
                        break
                if not stats_table:
                    continue
                entity_qualified = f"{db_schema}.{table_name}"
                cur.execute(
                    f"DELETE FROM {entity_qualified} e "
                    f"WHERE NOT EXISTS ("
                    f"  SELECT 1 FROM {stats_table} s WHERE s.entity_id = e.id"
                    f")",
                )
                count = cur.rowcount
                if count:
                    logger.info(
                        'Pruned %d orphaned entities from %s', count, entity_qualified,
                    )
                    pruned += count

        conn.commit()
        return pruned
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
