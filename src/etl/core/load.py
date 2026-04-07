"""
The Glass - ETL Database Loader

Bulk database write functions for the ETL pipeline.  Provides ``bulk_upsert``
(using ``execute_values`` with ``ON CONFLICT``) and ``bulk_copy``
(using PostgreSQL ``COPY FROM``).  Both honour the column-quoting
required by digit-starting column names (e.g. ``fg2m``).

All functions are stateless and operate on a caller-provided connection.
"""

import logging
from io import StringIO
from typing import Any, Dict, List, Optional

from psycopg2.extras import execute_values

from src.db import db_connection, quote_col

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500


# ============================================================================
# BULK OPERATIONS
# ============================================================================

def bulk_upsert(
    conn: Any,
    table: str,
    columns: List[str],
    data: List[tuple],
    conflict_columns: List[str],
    update_columns: Optional[List[str]] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """INSERT ... ON CONFLICT DO UPDATE SET for a batch of rows.

    Args:
        conn:             psycopg2 connection.
        table:            Schema-qualified table name.
        columns:          Ordered column names matching *data* tuples.
        data:             List of tuples -- one per row.
        conflict_columns: Unique-constraint columns for conflict detection.
        update_columns:   Columns to overwrite on conflict.
                          *None* -> all non-conflict columns.
        batch_size:       Rows per execute_values call.

    Returns:
        Number of rows written.
    """
    if not data:
        return 0

    if update_columns is None:
        conflict_set = set(conflict_columns)
        update_columns = [c for c in columns if c not in conflict_set]

    cols_sql = ', '.join(quote_col(c) for c in columns)
    conflict_sql = ', '.join(quote_col(c) for c in conflict_columns)
    update_sql = ', '.join(
        f'{quote_col(c)} = EXCLUDED.{quote_col(c)}' for c in update_columns
    )

    query = (
        f'INSERT INTO {table} ({cols_sql}) VALUES %s '
        f'ON CONFLICT ({conflict_sql}) '
        f'DO UPDATE SET {update_sql}, updated_at = NOW()'
    )

    cursor = conn.cursor()
    written = 0

    for offset in range(0, len(data), batch_size):
        batch = data[offset : offset + batch_size]
        try:
            execute_values(cursor, query, batch, page_size=batch_size)
            written += len(batch)
        except Exception:
            logger.error('Batch failed at offset %d in %s', offset, table)
            conn.rollback()
            raise

    conn.commit()
    return written


def bulk_copy(
    conn: Any,
    table: str,
    columns: List[str],
    data: List[tuple],
) -> int:
    """Ultra-fast initial load via PostgreSQL COPY FROM.

    Does **not** handle conflicts -- use for initial loads only.

    Returns:
        Number of rows copied.
    """
    if not data:
        return 0

    buf = StringIO()
    for row in data:
        line = '\t'.join(
            str(v) if v is not None else '\\N' for v in row
        )
        buf.write(line + '\n')
    buf.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buf, table, columns=columns, null='\\N')
        conn.commit()
        return len(data)
    except Exception:
        logger.error('COPY into %s failed', table)
        conn.rollback()
        raise


# ============================================================================
# HIGH-LEVEL WRITE HELPERS
# ============================================================================

def upsert_entity_rows(
    conn: Any,
    table: str,
    rows: Dict[int, Dict[str, Any]],
    conflict_columns: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Convert ``{entity_id: {col: val}}`` dicts to tuples and upsert.

    Collects the union of all column names across every entity, fills
    missing values with ``None``, and delegates to ``bulk_upsert``.

    Args:
        conn:             psycopg2 connection.
        table:            Schema-qualified table name.
        rows:             Extraction result from ``extract_columns_from_result``.
        conflict_columns: PK / unique columns (e.g. ``['nba_api_id', 'season']``).
        batch_size:       Rows per batch.

    Returns:
        Number of rows written.
    """
    if not rows:
        return 0

    # Collect the union of all columns present in the data
    all_cols: set = set()
    for entity_vals in rows.values():
        all_cols.update(entity_vals.keys())

    columns = sorted(all_cols)
    data = [
        tuple(entity_vals.get(c) for c in columns)
        for entity_vals in rows.values()
    ]

    return bulk_upsert(conn, table, columns, data, conflict_columns, batch_size=batch_size)


def write_entity_rows(
    entity: str,
    scope: str,
    rows: Dict[Any, Dict[str, Any]],
    season: str,
    season_type: int,
    db_schema: str,
) -> int:
    """Write extracted entity rows to the database via upsert.

    For entity scope: upserts directly with nba_api_id as conflict key.
    For stats scope: looks up the entity's serial id from the entity
    table, then upserts with (entity_id, season, season_type) conflict key.

    Args:
        entity:      ``'player'`` or ``'team'``.
        scope:       ``'stats'`` or ``'entity'``.
        rows:        ``{source_entity_id: {col_name: value, ...}, ...}``
        season:      Season string (e.g. ``'2024-25'``).
        season_type: Season type integer (1=Regular, 2=Playoffs, 3=PlayIn).
        db_schema:   Database schema name (e.g. ``'nba'``).

    Returns:
        Number of rows written.
    """
    if not rows:
        return 0

    from src.etl.config import TABLES
    from src.db import get_table_name

    table = get_table_name(entity, scope, db_schema)
    table_name = table.split('.', 1)[1]
    table_meta = TABLES[table_name]
    conflict_columns = table_meta['unique_key']

    all_cols: set = set()
    for vals in rows.values():
        all_cols.update(vals.keys())

    data_cols = sorted(all_cols)

    with db_connection() as conn:
        if scope == 'stats':
            entity_table = get_table_name(entity, 'entity', db_schema)
            id_map = _load_entity_id_map(conn, entity_table)

            columns = list(conflict_columns) + data_cols
            data = []
            for source_id, vals in rows.items():
                serial_id = id_map.get(str(source_id))
                if serial_id is None:
                    logger.warning(
                        'No entity row for source_id=%s in %s, skipping stats',
                        source_id, entity_table,
                    )
                    continue

                identity_values = []
                for ck in conflict_columns:
                    if ck == 'entity_id':
                        identity_values.append(serial_id)
                    elif ck == 'season':
                        identity_values.append(season)
                    elif ck == 'season_type':
                        identity_values.append(str(season_type))
                    else:
                        identity_values.append(None)

                row_values = [vals.get(c) for c in data_cols]
                data.append(tuple(identity_values + row_values))

            if not data:
                return 0
            return bulk_upsert(conn, table, columns, data, conflict_columns)
        else:
            columns = list(conflict_columns) + data_cols
            data = []
            for source_id, vals in rows.items():
                identity_values = []
                for ck in conflict_columns:
                    if ck == 'nba_api_id':
                        identity_values.append(str(source_id))
                    else:
                        identity_values.append(None)

                row_values = [vals.get(c) for c in data_cols]
                data.append(tuple(identity_values + row_values))

            return bulk_upsert(conn, table, columns, data, conflict_columns)


def _load_entity_id_map(conn: Any, entity_table: str) -> Dict[str, int]:
    """Load nba_api_id -> serial id mapping from an entity table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT nba_api_id, id FROM {entity_table}")
        return {str(row[0]): row[1] for row in cur.fetchall()}
